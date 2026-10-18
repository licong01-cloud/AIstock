"""One file-only candidate, immutable stages, six actual fits, no deployment.

CLI: python -m backend.services.advisory_model_first.sector_relative_open_price_5td_pipeline_v1
     --plan <frozen-json> --output-root <non-C-root> --stage run
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import tempfile

import pandas as pd
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_inputs_v1 import clocks_v1
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.parent_score_price_5td_pipeline_v1 import _exclusive_json, _journal, _node, _qe
from backend.services.advisory_model_first.sector_relative_open_price_5td_contracts_v1 import (
    ARMS, COMPONENTS, CONDITION_KINDS, CONTEXT_COLUMNS, FIT_BUDGET, HYPOTHESIS,
    KEY, PARAMETERS, PEER_POLICY, POLICY, POLICY_SHA256, PREVIOUS_FIT_COUNT,
    SCHEMA_SHA256, SectorRelativeOpenStudyPlanV1, node_path, sha,
)
from backend.services.advisory_model_first.sector_relative_open_price_5td_model_v1 import (
    bundle_payload_v1, fit_component_v1, query_sector_relative_open_nodes_v1,
    supervision_v1, validate_bundle_v1, validate_component_v1,
)

STAGES = ("preregistered", "prepared", "trained", "evaluated")


def implementation_sha256():
    root = Path(__file__).parents[3]
    names = ["backend/services/advisory_model_first/sector_relative_open_price_5td_"+n+"_v1.py"
        for n in ("contracts", "inputs", "model", "evaluation", "pipeline")]
    names += ["backend/services/advisory_model_first/"+n for n in (
        "original_slot_hurdle_price_5td_contracts_v1.py", "original_slot_hurdle_price_5td_model_v1.py",
        "original_slot_hurdle_price_5td_inputs_v1.py", "market_open_hurdle_price_5td_model_v1.py",
        "market_open_hurdle_price_5td_contracts_v1.py", "generic_daily_price_input_v1.py",
        "generic_price_5td_models_v1.py", "generic_price_5td_contracts_v1.py", "generic_price_5td_labels_v1.py",
        "generic_price_5td_valuation_v2.py", "cross_package_validation_inputs_v1.py",
        "parent_score_price_5td_contracts_v1.py", "parent_score_price_5td_evaluation_v1.py",
        "parent_score_price_5td_inputs_v1.py", "parent_score_price_5td_pipeline_v1.py",
        "economic_entry_pipeline.py", "research_control.py", "research_control_contracts.py")]
    names += ["backend/services/advisory_model_first/economic_value_anchor_contracts_v1.py",
        "backend/services/advisory_model_first/risk_tail_calibrated_price_5td_inputs_v1.py",
        "backend/services/strategy_package/runtime_variant.py", "backend/data_service/frozen_dataset.py",
        "backend/services/advisory_historical_range/outcome_file_source.py"]
    return sha({n: hashlib.sha256((root/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})


def _checked(plan, output_root):
    plan = SectorRelativeOpenStudyPlanV1.model_validate(plan)
    if plan.implementation_sha256 != implementation_sha256():
        raise ValueError("preregistered implementation identity changed")
    root = node_path(output_root)/plan.experiment_id
    source = node_path(plan.source_prepared.artifact_uri)
    data = node_path(plan.file_source_binding["profile"]["controller_paths"]["candidate_root"])
    if root == source or source in root.parents or root in source.parents or root == data or data in root.parents or root in data.parents:
        raise ValueError("study output cannot overwrite or nest original inputs")
    return plan, root


def _stage(plan, root, until):
    parent = None
    for name in STAGES:
        header = read_stage(root/name, stage=name, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = header["stage_sha256"]
        if name == until:
            return header
    raise ValueError("unknown study stage")


def _register(plan, root, stage, *, generated=0, evaluated=0, evidence=None, result_class="EXPLORATORY"):
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id=plan.attempt_id, research_stage=stage.upper(),
        study_type=plan.study_type, objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        hypothesis_family_id="ADVISORY_GP5_ENTRY_VALUE", unique_variable=HYPOTHESIS,
        parent_lineage=("ADVISORY_GP5_ENTRY_VALUE", *(s.run_id for s in plan.sources)),
        dataset_identity=plan.economic_contract_sha256, schema_identity=SCHEMA_SHA256, policy_identity=POLICY_SHA256,
        planned_trial_count=2, generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
        result_class=result_class, consumed_windows=(ConsumedWindowV1(window_id="CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=plan.economic_contract_sha256, start_date=plan.train_start, end_date=plan.evaluation_end),),
        evidence_refs=(evidence_reference_for_file(evidence or root/stage/"manifest.json", role="sector_open_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _publish(plan, root, stage, parent, artifacts, **counts):
    if sum(map(len, artifacts.values())) > 8*1024**3:
        raise ValueError("study artifact memory budget exceeds 8GiB")
    target = publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256,
        parent_sha256=parent, artifacts=artifacts)
    _register(plan, root, stage, **counts)
    return target


@contextmanager
def x_temporary_files(plan):
    target = node_path(plan.temporary_root_uri)
    target.mkdir(parents=True, exist_ok=True)
    previous, cached = {k: os.environ.get(k) for k in ("TEMP", "TMP", "TMPDIR")}, tempfile.tempdir
    try:
        for k in previous:
            os.environ[k] = str(target)
        tempfile.tempdir = str(target)
        yield
    finally:
        tempfile.tempdir = cached
        for k, v in previous.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def preregister_sector_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    return _publish(plan, root, "preregistered", None, {"plan.json": _json_bytes(plan.model_dump(mode="json")),
        "policy.json": _json_bytes(dict(policy=POLICY, peer_policy=PEER_POLICY, parameters=PARAMETERS,
            physical_fit_budget=FIT_BUDGET, candidate_count=1, previous_physical_fit_count=PREVIOUS_FIT_COUNT,
            old_index_fit_count=1, research_lineage="ADVISORY_GP5_ENTRY_VALUE"))})


def prepare_sector_study_v1(*, plan, output_root, reader=None):
    from backend.services.advisory_model_first.sector_relative_open_price_5td_inputs_v1 import prepare_sector_inputs_v1
    plan, root = _checked(plan, output_root)
    initial = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        _register(plan, root, "prepared")
        return root/"prepared"
    with x_temporary_files(plan):
        prepared = prepare_sector_inputs_v1(plan=plan, reader=reader)
        receipt = dict(status="PREPARED_COMMON_FILES_NO_FIT", original_rows=len(prepared["roster"]),
            original_package_days=len(prepared["days"]), train_top5_rows=len(prepared["domain"]),
            training_valuation_statuses=prepared["domain"].valuation_status.value_counts().to_dict(),
            context_statuses=prepared["domain"].peer_status.value_counts().to_dict(),
            source_stage_sha256=prepared["source_stage_sha256"], source_read=prepared["source_receipt"],
            physical_fit_count=0, test_or_sealed_finance_consumed=False, database_written=False, deployable=False)
        _checked(plan, output_root)
        return _publish(plan, root, "prepared", initial["stage_sha256"], {
            **{n+".parquet": parquet_bytes(prepared[n]) for n in ("roster", "days", "domain", "peer_panel")},
            "calendar.json": _json_bytes(prepared["calendar"]), "encoding.json": _json_bytes(prepared["encoding"]),
            "receipt.json": _json_bytes(receipt)})


def _budget():
    import psutil
    memory = psutil.virtual_memory()
    if psutil.Process().memory_info().rss > 8*1024**3 or memory.available < 256*1024**2:
        raise ValueError("RESOURCE_BUDGET_EXCEEDED: retain progress, no next fit")
    return dict(rss_bytes=psutil.Process().memory_info().rss, available_bytes=memory.available,
        logical_cpus=psutil.cpu_count(), fit_thread_limit=2)


def _reader(plan, reader):
    from backend.services.advisory_model_first.sector_relative_open_price_5td_inputs_v1 import SectorOpenFileProjectionV1
    reader = reader or SectorOpenFileProjectionV1(plan.file_source_binding)
    if reader.binding_hash != plan.file_source_binding["binding_hash"]:
        raise ValueError("study reader binding differs from the frozen common snapshot")
    reader.assert_unchanged()
    return reader


def _component(plan, root, prepared, encoding, arm, component):
    folder = root/"fits"/(arm+"_"+component)
    header = read_stage(folder/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
    attempt = json.loads((folder/"attempt.json").read_bytes())
    if (attempt.get("plan_sha256") != plan.plan_sha256 or attempt.get("arm") != arm
            or attempt.get("component") != component or attempt.get("physical_fit_count") != 1):
        raise ValueError("component attempt identity differs")
    payload = validate_component_v1(json.loads((folder/"trained"/"model.json").read_bytes()),
        encoding=encoding, arm=arm, component=component, plan_sha256=plan.plan_sha256)
    return payload, header["stage_sha256"]


def _prior_attempts(plan):
    count, seen = 0, set()
    while plan.exact_retry_of is not None:
        folder = node_path(plan.exact_retry_of)
        if folder in seen or len(seen) >= 16:
            raise ValueError("technical recovery lineage is cyclic or unbounded")
        seen.add(folder)
        previous = SectorRelativeOpenStudyPlanV1.model_validate_json((folder/"preregistered"/"plan.json").read_bytes())
        _stage(previous, folder, "preregistered")
        if previous.economic_contract_sha256 != plan.economic_contract_sha256:
            raise ValueError("technical recovery changed the economic contract")
        for arm in ARMS:
            for component in COMPONENTS:
                path = folder/"fits"/(arm+"_"+component)/"attempt.json"
                if path.exists():
                    attempt = json.loads(path.read_bytes())
                    if attempt.get("plan_sha256") != previous.plan_sha256 or attempt.get("physical_fit_count") != 1:
                        raise ValueError("prior physical fit identity differs")
                    count += 1
        plan = previous
    return count


def train_sector_study_v1(*, plan, output_root, qe_observation_probe):
    plan, root = _checked(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
    rows = pd.read_parquet(root/"prepared"/"domain.parquet")
    supervision_v1(rows, encoding)  # No fit claim on a missing class/input.
    prior = _prior_attempts(plan)
    if not (root/"trained").exists():
        node = _node(plan)
        for arm in ARMS:
            for component in COMPONENTS:
                folder = root/"fits"/(arm+"_"+component)
                if (folder/"trained").exists():
                    _component(plan, root, prepared, encoding, arm, component)
                    continue
                if (folder/"attempt.json").exists():
                    raise ValueError("TECHNICAL_RECOVERY_REQUIRED: STARTED without an immutable model; no implicit refit")
                def before_fit():
                    current = sum((root/"fits"/(a+"_"+c)/"attempt.json").exists() for a in ARMS for c in COMPONENTS)
                    if current+prior >= FIT_BUDGET:
                        raise ValueError("six physical fits already consumed in this economic lineage")
                    observation = _qe(qe_observation_probe, require_idle=False)
                    resource = _budget()
                    _exclusive_json(folder/"attempt.json", dict(status="STARTED", plan_sha256=plan.plan_sha256,
                        arm=arm, component=component, physical_fit_count=1, ordinal=current+prior+1,
                        node=node, qe_observation=observation, resource=resource,
                        captured_at=datetime.now(timezone.utc).isoformat()))
                    _journal(root, dict(kind="PHYSICAL_FIT_STARTED", arm=arm, component=component, ordinal=current+prior+1))
                    _register(plan, root, "fit_started_"+str(current+1), generated=min(2, current//3+1), evidence=folder/"attempt.json")
                try:
                    with x_temporary_files(plan), threadpool_limits(limits=2):
                        payload = fit_component_v1(rows=rows, encoding=encoding, arm=arm, component=component,
                            plan_sha256=plan.plan_sha256, before_fit=before_fit)
                    _stage(plan, root, "prepared")
                    publish_stage(study_root=folder, stage="trained", plan_sha256=plan.plan_sha256,
                        parent_sha256=prepared["stage_sha256"], artifacts={"model.json": _json_bytes(payload)})
                    _journal(root, dict(kind="PHYSICAL_FIT_COMPLETE", arm=arm, component=component,
                        component_sha256=payload["component_sha256"]))
                except Exception as exc:
                    _journal(root, dict(kind="ATTEMPT_INCOMPLETE_NO_IMPLICIT_RETRY", arm=arm, component=component,
                        fit_started=(folder/"attempt.json").exists(), error_type=type(exc).__name__, reason=str(exc)))
                    raise
    models, refs = {}, {}
    for arm in ARMS:
        models[arm] = {}
        for component in COMPONENTS:
            models[arm][component], refs[arm+"_"+component] = _component(plan, root, prepared, encoding, arm, component)
    bundle = bundle_payload_v1(models=models, encoding=encoding, plan_sha256=plan.plan_sha256)
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        if (json.loads((root/"trained"/"bundle.json").read_bytes()) != bundle
                or json.loads((root/"trained"/"components.json").read_bytes()) != refs):
            raise ValueError("completed bundle and component chain disagree")
        _register(plan, root, "trained", generated=2)
        return root/"trained"
    _checked(plan, output_root)
    return _publish(plan, root, "trained", prepared["stage_sha256"], {"bundle.json": _json_bytes(bundle),
        "components.json": _json_bytes(refs), "receipt.json": _json_bytes(dict(status="SIX_COMPONENTS_COMPLETE_NO_ACTIVATION",
            physical_fit_count=6, technical_lineage_prior_fit_attempts=prior, cumulative_physical_fit_attempts=PREVIOUS_FIT_COUNT+6+prior,
            old_index_fit_count=1, research_hypotheses=1, node=node, deployable=False))}, generated=2)


def freeze_sector_forecasts_v1(*, plan, output_root, reader=None):
    from backend.services.advisory_model_first.sector_relative_open_price_5td_inputs_v1 import build_sector_context_inputs_v1, frame_sha256
    plan, root = _checked(plan, output_root)
    trained = _stage(plan, root, "trained")
    target = root/"forecasts"/"trained"
    if target.exists():
        read_stage(target, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
        return target
    reader = _reader(plan, reader)
    roster = pd.read_parquet(root/"prepared"/"roster.parquet")
    roster = roster.loc[roster[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))
        & roster.selection_effective_rank.le(5)].copy()
    calendar = json.loads((root/"prepared"/"calendar.json").read_bytes())
    contexts, peers = build_sector_context_inputs_v1(roster=roster, reader=reader, calendar=calendar, cutoff=plan.evaluation_end)
    bundle = validate_bundle_v1(json.loads((root/"trained"/"bundle.json").read_bytes()))
    predictions = []
    for arm in ARMS:
        price = contexts.reference_cny*(1+contexts.observed_gap_bps/10000)
        predictions.append(query_sector_relative_open_nodes_v1(bundle=bundle, features=contexts,
            price_cny=price, reference_cny=contexts.reference_cny, market_condition=contexts[CONTEXT_COLUMNS[0]],
            peer_condition=contexts[list(CONTEXT_COLUMNS[1:])], condition_kind=CONDITION_KINDS[1], arm=arm))
    _checked(plan, output_root)
    return publish_stage(study_root=root/"forecasts", stage="trained", plan_sha256=plan.plan_sha256,
        parent_sha256=trained["stage_sha256"], artifacts={"contexts.parquet": parquet_bytes(contexts),
            "peer_panel.parquet": parquet_bytes(peers), "predictions.parquet": parquet_bytes(pd.concat(predictions, ignore_index=True)),
            "receipt.json": _json_bytes(dict(evaluation_finance_consumed=False, source_read=reader.receipt(),
                context_sha256=frame_sha256(contexts), bundle_sha256=bundle["bundle_sha256"], deployable=False))})


def evaluate_sector_study_v1(*, plan, output_root, reader=None):
    from backend.services.advisory_model_first.sector_relative_open_price_5td_evaluation_v1 import evaluate_sector_relative_open_v1
    from backend.services.advisory_model_first.sector_relative_open_price_5td_inputs_v1 import measure_sector_finance_v1
    plan, root = _checked(plan, output_root)
    trained = _stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        frozen = read_stage(root/"forecasts"/"trained", stage="trained", plan_sha256=plan.plan_sha256,
            parent_sha256=trained["stage_sha256"])
        if json.loads((root/"evaluated"/"receipt.json").read_bytes())["forecast_stage_sha256"] != frozen["stage_sha256"]:
            raise ValueError("evaluated stage refers to a different frozen forecast")
        return root/"evaluated"
    forecasts = freeze_sector_forecasts_v1(plan=plan, output_root=output_root, reader=reader)
    frozen = read_stage(forecasts, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
    contexts = pd.read_parquet(forecasts/"contexts.parquet")
    predictions = pd.read_parquet(forecasts/"predictions.parquet")
    calendar = json.loads((root/"prepared"/"calendar.json").read_bytes())
    reader = _reader(plan, reader)
    rows = measure_sector_finance_v1(inputs=contexts, reader=reader, calendar=calendar,
        cutoff=plan.evaluation_end, role="EVALUATION_FINANCE")
    read_stage(forecasts, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
    days = pd.read_parquet(root/"prepared"/"days.parquet")
    days = days.loc[days.decision_date.between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    result = evaluate_sector_relative_open_v1(rows=rows, days=days,
        predictions={a: predictions.loc[predictions.arm.eq(a)].copy() for a in ARMS}, plan=plan, calendar=calendar)
    result["forecast_stage_sha256"] = frozen["stage_sha256"]
    _checked(plan, output_root)
    return _publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(result),
        "measured_rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days),
        "receipt.json": _json_bytes(dict(forecast_stage_sha256=frozen["stage_sha256"], source_read=reader.receipt(),
            clocks=clocks_v1(plan, calendar), physical_fit_count=6, deployable=False))}, generated=2, evaluated=2,
        result_class="NEGATIVE" if result["status"] == "EXACT_CANDIDATE_NOT_SUPPORTED" else "EXPLORATORY")


def _fit_via_wsl(plan, *, plan_uri, output_root):
    """Existing Windows file reader + explicit existing WSL pure-math process.

    No asset copy, service API, installation or persisted remote workspace.
    The child runs only the trained stage; inputs stay immutable in place.
    """
    import subprocess
    if not plan.node_wsl_distribution:
        raise ValueError("Windows controller needs its explicitly registered WSL distribution")
    def mounted(value):
        path = node_path(value)
        windows = PureWindowsPath(str(path))
        if windows.drive:
            return "/mnt/"+windows.drive[0].lower()+"/"+"/".join(windows.parts[1:])
        return path.as_posix()
    command = ["wsl", "-d", plan.node_wsl_distribution, "--cd", mounted(Path(__file__).parents[3]), "--exec", "env",
        "PYTHONDONTWRITEBYTECODE=1", "OMP_NUM_THREADS=2", "OPENBLAS_NUM_THREADS=2", "MKL_NUM_THREADS=2",
        "TMPDIR="+mounted(plan.temporary_root_uri), plan.node_python_uri, "-u", "-m", __package__+".sector_relative_open_price_5td_pipeline_v1",
        "--plan", mounted(plan_uri), "--output-root", mounted(output_root), "--stage", "trained"]
    subprocess.run(command, check=True)
    _, root = _checked(plan, output_root)
    _stage(plan, root, "trained")
    return root/"trained"


def main():
    import argparse
    from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--stage", choices=(*STAGES, "forecasts", "run"), required=True)
    args = parser.parse_args()
    plan = SectorRelativeOpenStudyPlanV1.model_validate_json(node_path(args.plan).read_bytes())
    functions = dict(preregistered=preregister_sector_study_v1, prepared=prepare_sector_study_v1,
        trained=train_sector_study_v1, forecasts=freeze_sector_forecasts_v1, evaluated=evaluate_sector_study_v1)
    reader = None
    for stage in ("preregistered", "prepared", "trained", "forecasts", "evaluated") if args.stage == "run" else (args.stage,):
        kwargs = dict(plan=plan, output_root=args.output_root)
        if stage in ("prepared", "forecasts", "evaluated"):
            reader = _reader(plan, reader)
            kwargs["reader"] = reader
        if stage == "trained":
            kwargs["qe_observation_probe"] = lambda: public_qe_observation_v1(plan.qe_api_base)
        print(json.dumps(dict(stage=stage, status="STARTED", experiment_id=plan.experiment_id)), flush=True)
        with x_temporary_files(plan):
            target = _fit_via_wsl(plan, plan_uri=args.plan, output_root=args.output_root) if stage == "trained" and os.name == "nt" else functions[stage](**kwargs)
        print(json.dumps(dict(stage=stage, status="COMPLETE", path=str(target))), flush=True)


if __name__ == "__main__":
    main()
