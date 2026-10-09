"""Immutable Advisory stages: one shared fit and a separately resumable risk head."""
import hashlib
import json
import os
from functools import wraps
from pathlib import Path
import sys
import tempfile

import pandas as pd

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.parent_score_price_5td_pipeline_v1 import _exclusive_json, _journal, _node, _qe
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import (
    ARMS, CALIBRATION, PARAMETERS, ROSTER_KEY, SCHEMA_SHA256, RiskTailStudyPlanV1, node_path,
)
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import (
    CAL_FINANCE, EVAL_FINANCE, checked_source, prepare_risk_tail_inputs_v1, read_projection_v1, sessions_v1,
)
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_model_v1 import (
    bundle_from_payload_v1, bundle_payload_v1, core_from_payload_v1, core_payload_v1,
    fit_shared_core_v1, fit_tail_residual_calibration_v1, query_risk_tail_nodes_v1, validate_card_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def implementation_sha256():
    names = [f"risk_tail_calibrated_price_5td_{n}_v1.py" for n in ("contracts", "inputs", "model", "pipeline", "evaluation", "cli")]
    names += ["generic_daily_price_input_v1.py", "generic_price_5td_contracts_v1.py", "generic_price_5td_models_v1.py",
        "generic_price_5td_valuation_v2.py", "generic_population_price_5td_model_v1.py", "generic_population_price_5td_cli_v1.py",
        "parent_score_price_5td_contracts_v1.py", "parent_score_price_5td_inputs_v1.py", "parent_score_price_5td_model_v1.py",
        "parent_score_price_5td_pipeline_v1.py", "parent_score_price_5td_evaluation_v1.py", "economic_entry_pipeline.py",
        "economic_value_anchor_contracts_v1.py", "research_control.py", "research_control_contracts.py"]
    return sha({n: hashlib.sha256((Path(__file__).parent/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})


def _checked(plan, output_root):
    plan = RiskTailStudyPlanV1.model_validate(plan)
    if plan.implementation_sha256 != implementation_sha256():
        raise ValueError("risk study implementation changed after preregistration")
    root, source = node_path(output_root)/plan.experiment_id, node_path(plan.source_prepared.artifact_uri)
    if root == source or root.is_relative_to(source.parent) or source.is_relative_to(root):
        raise ValueError("new risk artifacts cannot overwrite or enter the original source store")
    _temporary_root(plan)
    checked_source(plan)
    if plan.exact_retry_of is not None:
        previous = node_path(plan.exact_retry_of)
        if previous == root or (previous/"trained").exists() or (previous/"evaluated").exists():
            raise ValueError("exact technical retry cannot replace a completed study")
        old = RiskTailStudyPlanV1.model_validate_json((previous/"preregistered"/"plan.json").read_bytes())
        _stage(old, previous, "preregistered")
        if old.attempt_id == plan.attempt_id or old.economic_contract_sha256 != plan.economic_contract_sha256:
            raise ValueError("exact technical retry needs a new identity and unchanged economic contract")
    return plan, root


def _temporary_root(plan):
    temporary = node_path(plan.temporary_root_uri)
    if not str(temporary).replace("\\", "/").lower().startswith(("x:/", "/mnt/x/")):
        raise ValueError("risk study temporary files must use the explicitly named X drive")
    return temporary


def _x_temporary_files(operation):
    """Offline operation scope; never leak environment changes into other consumers."""
    @wraps(operation)
    def scoped(**kwargs):
        plan = RiskTailStudyPlanV1.model_validate(kwargs["plan"])
        temporary = _temporary_root(plan)
        keys = ("TEMP", "TMP", "TMPDIR", "JOBLIB_TEMP_FOLDER")
        prior = {n: os.environ.get(n) for n in keys}
        prior_temp, prior_bytecode = tempfile.tempdir, sys.dont_write_bytecode
        temporary.mkdir(parents=True, exist_ok=True)
        try:
            os.environ.update({n: str(temporary) for n in keys})
            tempfile.tempdir, sys.dont_write_bytecode = str(temporary), True
            return operation(**kwargs)
        finally:
            for n, value in prior.items():
                if value is None:
                    os.environ.pop(n, None)
                else:
                    os.environ[n] = value
            tempfile.tempdir, sys.dont_write_bytecode = prior_temp, prior_bytecode
    return scoped


def _stage(plan, root, until):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        header = read_stage(root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = header["stage_sha256"]
        if stage == until:
            return header
    raise ValueError("unknown risk study stage")


def _register(plan, root, stage, *, generated=0, evaluated=0, evidence=None, result_class="EXPLORATORY"):
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id=plan.attempt_id, research_stage=stage.upper(),
        study_type=plan.study_type, objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        hypothesis_family_id="GP5-RISK-TAIL-CALIBRATION-1", unique_variable="TRAIN_ONLY_GLOBAL_Q90_RISK_RESIDUAL_CALIBRATION",
        parent_lineage=tuple(s.run_id for s in plan.sources)+((plan.exact_retry_of,) if plan.exact_retry_of else ()),
        dataset_identity=plan.source_prepared.stage_sha256, schema_identity=SCHEMA_SHA256,
        policy_identity=VALUATION_POLICY_SHA256, planned_trial_count=2, generated_trial_count=generated,
        evaluated_trial_count=evaluated, selected_trial_count=0, result_class=result_class,
        consumed_windows=(ConsumedWindowV1(window_id="CONSUMED_DEVELOPMENT_ONLY", dataset_identity=plan.source_prepared.stage_sha256,
            start_date=plan.train_start, end_date=plan.evaluation_end),),
        evidence_refs=(evidence_reference_for_file(evidence or root/stage/"manifest.json", role="risk_calibration_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _publish(plan, root, stage, parent, artifacts, *, generated=0, evaluated=0, result_class="EXPLORATORY"):
    if sum(len(v) for v in artifacts.values()) > 8*1024**3:
        raise ValueError("risk study artifact budget exceeds 8GiB")
    target = publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256,
        parent_sha256=parent, artifacts=artifacts)
    _register(plan, root, stage, generated=generated, evaluated=evaluated, result_class=result_class)
    return target


@_x_temporary_files
def preregister_risk_tail_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    if (root/"preregistered").exists():
        _stage(plan, root, "preregistered")
        _register(plan, root, "preregistered")
        return root/"preregistered"
    return _publish(plan, root, "preregistered", None, {"plan.json": _json_bytes(plan.model_dump(mode="json")),
        "policy.json": _json_bytes(dict(execution_policy=POLICY, calibration=CALIBRATION, parameters=dict(PARAMETERS),
            physical_fit_budget=1, calibration_estimation_budget=1, planned_decision_arms=2,
            economic_contract_sha256=plan.economic_contract_sha256, deployable=False))})


@_x_temporary_files
def prepare_risk_tail_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    initial = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        _register(plan, root, "prepared")
        return root/"prepared"
    prepared = prepare_risk_tail_inputs_v1(plan=plan)
    receipt = dict(status="PREPARED_THREE_POOLS_NO_FIT", original_rows=len(prepared["roster"]),
        original_package_days=len(prepared["days"]), base_supervision_rows=len(prepared["base"]),
        calibration_original_rows=len(prepared["calibration_roster"]),
        calibration_finite_original_mass=float(prepared["calibration_roster"].original_weight.sum()),
        source_stage_sha256=prepared["source_stage_sha256"], physical_fit_count=0, calibration_estimation_attempts=0,
        parent_scores_consumed=False, calibration_finance_consumed=False, evaluation_finance_consumed=False,
        test_or_sealed_finance_consumed=False, database_written=False, deployable=False)
    _checked(plan, output_root)
    return _publish(plan, root, "prepared", initial["stage_sha256"], {
        "roster.parquet": parquet_bytes(prepared["roster"]), "days.parquet": parquet_bytes(prepared["days"]),
        "base.parquet": parquet_bytes(prepared["base"]), "calibration_roster.parquet": parquet_bytes(prepared["calibration_roster"]),
        "encoding.json": _json_bytes(prepared["encoding"]), "calendar.json": _json_bytes(prepared["calendar"]),
        "receipt.json": _json_bytes(receipt)})


def _components(plan, root, prepared):
    core_root = root/"fits"/"shared_core"/"trained"
    header = read_stage(core_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
    core = core_from_payload_v1(json.loads((core_root/"model.json").read_bytes()))
    encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
    if core.recipe["plan_sha256"] != plan.plan_sha256 or core.recipe["encoding_sha256"] != sha(encoding):
        raise ValueError("shared core does not belong to this prepared plan")
    return core, header


@_x_temporary_files
def train_risk_tail_study_v1(*, plan, output_root, qe_idle_probe):
    plan, root = _checked(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        bundle = _validated_bundle(plan, root, prepared)
        _register(plan, root, "trained", generated=1+int(bundle["tail_calibration"]["calibration_status"] == "AVAILABLE"))
        return root/"trained"
    core_path = root/"fits"/"shared_core"/"trained"
    if not core_path.exists():
        if (root/"fit_attempt.json").exists():
            raise ValueError("partial physical fit retained; no implicit refit")
        encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
        rows = pd.read_parquet(root/"prepared"/"base.parquet")
        node = _node(plan)
        def before_fit(arm):
            if arm != ARMS[0]:
                raise ValueError("physical fit is the shared core only")
            observation = _qe(qe_idle_probe, require_idle=True)
            _exclusive_json(root/"fit_attempt.json", dict(kind="PHYSICAL_FIT_STARTED", physical_fit_count=1,
                attempt_id=plan.attempt_id, plan_sha256=plan.plan_sha256, node=node, qe_observation=observation))
            _journal(root, dict(kind="PHYSICAL_FIT_STARTED", physical_fit_count=1, qe_observation=observation))
            _register(plan, root, "physical_fit_started", generated=1, evidence=root/"fit_attempt.json")
        def after_fit(arm):
            if arm != ARMS[0]:
                raise ValueError("post-fit shared arm differs")
            observation = _qe(qe_idle_probe, require_idle=False)
            _journal(root, dict(kind="POST_FIT_QE_OBSERVATION", qe_observation=observation))
            if any(observation["active_counts"].values()):
                raise ValueError("QE became active during fit; retain attempt, no success or new fit")
        try:
            core = fit_shared_core_v1(rows=rows, encoding=encoding, plan_sha256=plan.plan_sha256,
                before_fit=before_fit, after_fit=after_fit)
            _stage(plan, root, "prepared")
            publish_stage(study_root=core_path.parent, stage="trained", plan_sha256=plan.plan_sha256,
                parent_sha256=prepared["stage_sha256"], artifacts={"model.json": _json_bytes(core_payload_v1(core))})
            _journal(root, dict(kind="SHARED_CORE_COMPLETE", model_sha256=core.model_sha256, physical_fit_count=1))
        except Exception as exc:
            if (root/"fit_attempt.json").exists():
                _journal(root, dict(kind="ATTEMPT_INCOMPLETE_NO_IMPLICIT_REFIT", error_type=type(exc).__name__))
            raise
    core, core_header = _components(plan, root, prepared)
    card_path = root/"fits"/"risk_calibration"/"trained"
    if not card_path.exists():
        if (root/"calibration_attempt.json").exists():
            raise ValueError("partial risk estimation retained; use an explicit exact technical retry")
        calibration_roster = pd.read_parquet(root/"prepared"/"calibration_roster.parquet")
        rows = read_projection_v1(plan=plan, dates=core.recipe["encoding"]["calibration_dates"], columns=CAL_FINANCE, top5=True)
        _exclusive_json(root/"calibration_attempt.json", dict(kind="RISK_CALIBRATION_STARTED", calibration_estimation_attempts=1,
            base_model_sha256=core.model_sha256, plan_sha256=plan.plan_sha256))
        _journal(root, dict(kind="RISK_CALIBRATION_STARTED", calibration_estimation_attempts=1, physical_fit_count=1))
        card = fit_tail_residual_calibration_v1(core=core, calibration_rows=rows,
            original_calibration_roster=calibration_roster, plan_sha256=plan.plan_sha256)
        _stage(plan, root, "prepared")
        checked_source(plan)
        publish_stage(study_root=card_path.parent, stage="trained", plan_sha256=plan.plan_sha256,
            parent_sha256=core_header["stage_sha256"], artifacts={"calibration.json": _json_bytes(card)})
        _journal(root, dict(kind="RISK_CALIBRATION_COMPLETE", calibration_sha256=card["calibration_sha256"]))
    card_header = read_stage(card_path, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=core_header["stage_sha256"])
    card = json.loads((card_path/"calibration.json").read_bytes())
    validate_card_v1(core, card)
    bundle = bundle_payload_v1(core, card)
    _checked(plan, output_root)
    return _publish(plan, root, "trained", prepared["stage_sha256"], {"bundle.json": _json_bytes(bundle),
        "components.json": _json_bytes(dict(core_stage_sha256=core_header["stage_sha256"],
            calibration_stage_sha256=card_header["stage_sha256"], bundle_sha256=bundle["bundle_sha256"])),
        "receipt.json": _json_bytes(dict(status="ONE_FIT_COMPLETE_NO_ACTIVATION", physical_fit_count=1,
            calibration_estimation_attempts=1, calibration_status=card["calibration_status"],
            calibrated_parameter_count=card["calibrated_parameter_count"], test_or_sealed_finance_consumed=False, deployable=False))},
        generated=1+card["calibrated_parameter_count"])


def _validated_bundle(plan, root, prepared):
    core, core_header = _components(plan, root, prepared)
    path = root/"fits"/"risk_calibration"/"trained"
    card_header = read_stage(path, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=core_header["stage_sha256"])
    card = json.loads((path/"calibration.json").read_bytes())
    bundle = json.loads((root/"trained"/"bundle.json").read_bytes())
    bundle_from_payload_v1(bundle)
    refs = json.loads((root/"trained"/"components.json").read_bytes())
    if (bundle["bundle_sha256"] != bundle_payload_v1(core, card)["bundle_sha256"]
            or refs != dict(core_stage_sha256=core_header["stage_sha256"],
            calibration_stage_sha256=card_header["stage_sha256"], bundle_sha256=bundle["bundle_sha256"])):
        raise ValueError("completed risk bundle/component stage chain differs")
    return bundle


@_x_temporary_files
def evaluate_risk_tail_study_v1(*, plan, output_root):
    from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_evaluation_v1 import evaluate_risk_tail_cohorts_v1
    plan, root = _checked(plan, output_root)
    trained, prepared = _stage(plan, root, "trained"), _stage(plan, root, "prepared")
    bundle = _validated_bundle(plan, root, prepared)
    generated = 1+int(bundle["tail_calibration"]["calibration_status"] == "AVAILABLE")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        result = json.loads((root/"evaluated"/"evaluation.json").read_bytes())
        _register(plan, root, "evaluated", generated=generated, evaluated=generated,
            result_class="NEGATIVE" if result["status"].startswith("NEGATIVE") else "EXPLORATORY")
        return root/"evaluated"
    calendar = json.loads((root/"prepared"/"calendar.json").read_bytes())
    dates = [d for d in calendar if str(plan.evaluation_start) <= d[:10] <= str(plan.evaluation_end)]
    # Only D features + the observed T coordinate are decoded for the forecasts.
    sessions = sessions_v1(calendar)
    gap_dates = [d for d in dates if sessions[sessions.get_loc(pd.Timestamp(d))+1] <= pd.Timestamp(plan.evaluation_end)]
    mature_dates = [d for d in dates if sessions[sessions.get_loc(pd.Timestamp(d))+5] <= pd.Timestamp(plan.evaluation_end)]
    inputs = read_projection_v1(plan=plan, dates=dates, columns=FEATURES)
    gaps = read_projection_v1(plan=plan, dates=gap_dates, columns=("observed_gap_bps",))
    inputs = inputs.merge(gaps[[*ROSTER_KEY, "observed_gap_bps"]], on=list(ROSTER_KEY), how="left", validate="one_to_one")
    predictions = {a: query_risk_tail_nodes_v1(bundle=bundle, features=inputs[[*ROSTER_KEY, *FEATURES]],
        scenario_gap_bps=inputs.observed_gap_bps, arm=a) for a in ARMS}
    # Financial evaluation labels are first decoded after both frozen forecasts exist.
    mature_rows = read_projection_v1(plan=plan, dates=mature_dates, columns=EVAL_FINANCE)
    immature = inputs.loc[~inputs[KEY[0]].isin(pd.to_datetime(mature_dates))].copy()
    for n in EVAL_FINANCE:
        if n not in immature:
            immature[n] = float("nan")
    immature["valuation_status"] = immature["label_status"] = "IMMATURE"
    immature["exit_execution_status"] = "IMMATURE"
    immature["valuation_policy_sha256"] = VALUATION_POLICY_SHA256
    immature["label_information_end"] = [sessions[sessions.get_loc(d)+5] for d in immature[KEY[0]]]
    rows = pd.concat([mature_rows, immature], ignore_index=True)
    result = evaluate_risk_tail_cohorts_v1(rows=rows, days=pd.read_parquet(root/"prepared"/"days.parquet"),
        original_roster=pd.read_parquet(root/"prepared"/"roster.parquet"), predictions=predictions, plan=plan, calendar=calendar)
    result.update(base_model_sha256=bundle["core"]["model_sha256"], bundle_sha256=bundle["bundle_sha256"],
        calibration_sha256=bundle["tail_calibration"]["calibration_sha256"],
        risk_calibration_delta=bundle["tail_calibration"]["delta"], effective_raw_cutoff=bundle["tail_calibration"]["effective_raw_cutoff"])
    _checked(plan, output_root)
    _stage(plan, root, "trained")
    return _publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(result),
        "predictions.parquet": parquet_bytes(pd.concat([f.assign(arm=a) for a, f in predictions.items()], ignore_index=True)),
        "receipt.json": _json_bytes(dict(status=result["status"], physical_fit_count=1, calibration_estimation_attempts=1,
            evaluated_decision_arms=generated, independent_oos_evidence=False, activation_evidence=False,
            test_or_sealed_finance_consumed=False, database_written=False, deployable=False))},
        generated=generated, evaluated=generated, result_class="NEGATIVE" if result["status"].startswith("NEGATIVE") else "EXPLORATORY")
