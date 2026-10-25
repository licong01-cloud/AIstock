"""Atomic, read-only-source Advisory study; two fits, no deployment or QE mutation."""
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import sys

import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import _read_database
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import (
    ARMS, PARAMETERS, SCHEMA_SHA256, ParentScoreStudyPlanV1, node_path,
)
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import (
    build_inputs, freeze_rosters, original_calendar, parquet_bytes, warmup_calendar,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def implementation_sha256():
    names = [f"parent_score_price_5td_{n}_v1.py" for n in ("contracts", "inputs", "model", "pipeline", "evaluation", "cli")]
    names += ["cross_package_validation_source_v1.py", "cross_package_validation_inputs_v1.py",
        "generic_daily_price_input_v1.py", "generic_price_5td_contracts_v1.py", "generic_price_5td_models_v1.py",
        "generic_price_5td_labels_v1.py", "generic_price_5td_valuation_v2.py", "generic_population_price_5td_model_v1.py",
        "generic_population_price_5td_pipeline_v1.py", "economic_entry_sources.py", "economic_entry_pipeline.py",
        "research_control.py", "research_control_contracts.py"]
    return sha({n: hashlib.sha256((Path(__file__).parent/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})


def _checked(plan, output_root):
    plan = ParentScoreStudyPlanV1.model_validate(plan)
    if plan.implementation_sha256 != implementation_sha256():
        raise ValueError("study implementation changed after preregistration")
    root = node_path(output_root)/plan.experiment_id
    original_calendar(plan.calendar_ref)
    return plan, root


def _stage(plan, root, until):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        header = read_stage(root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = header["stage_sha256"]
        if stage == until:
            return header
    raise ValueError("unknown parent-score study stage")


def _register(plan, root, stage, *, generated=0, evaluated=0, evidence=None, result_class="EXPLORATORY"):
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="exact_attempt_v1", research_stage=stage.upper(),
        study_type=plan.study_type, objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        hypothesis_family_id="GP5-PARENT-SCORE-CONDITION-1", unique_variable="OWN_PARENT_SCORE_TRAIN_ONLY_COORDINATE",
        parent_lineage=tuple(s.run_id for s in plan.sources), dataset_identity=plan.plan_sha256,
        schema_identity=SCHEMA_SHA256, policy_identity=VALUATION_POLICY_SHA256, planned_trial_count=2,
        generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0, result_class=result_class,
        consumed_windows=(ConsumedWindowV1(window_id="CONSUMED_DEVELOPMENT_ONLY", dataset_identity=plan.plan_sha256,
            start_date=plan.train_start, end_date=plan.evaluation_end),),
        evidence_refs=(evidence_reference_for_file(evidence or root/stage/"manifest.json", role="parent_score_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _publish(plan, root, stage, parent, artifacts, *, generated=0, evaluated=0, result_class="EXPLORATORY"):
    if sum(len(v) for v in artifacts.values()) > 8*1024**3:
        raise ValueError("study artifact budget exceeds 8GiB")
    target = publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256,
                           parent_sha256=parent, artifacts=artifacts)
    _register(plan, root, stage, generated=generated, evaluated=evaluated, result_class=result_class)
    return target


def preregister_parent_score_study(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    return _publish(plan, root, "preregistered", None, {"plan.json": _json_bytes(plan.model_dump(mode="json")),
        "policy.json": _json_bytes(dict(legacy_execution_policy=POLICY, valuation_policy_sha256=VALUATION_POLICY_SHA256,
                                        parameters=dict(PARAMETERS), physical_fit_budget=2))})


def prepare_parent_score_study(*, plan, output_root, api, connection_context_factory=None, progress=None):
    plan, root = _checked(plan, output_root)
    initial = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        return root/"prepared"
    original = original_calendar(plan.calendar_ref)
    calendar, warmup = warmup_calendar(calendar=original, train_start=plan.train_start,
                                       connection_context_factory=connection_context_factory)
    roster, days, score_receipts = freeze_rosters(plan=plan, calendar=calendar, api=api)
    if progress is not None:
        progress(dict(phase="score_rosters", original_rows=len(roster), original_package_days=len(days), physical_fit_count=0))
    daily, index_daily, db_receipt = _read_database(symbols=sorted(set(roster.instrument)), calendar=calendar,
        cutoff=plan.evaluation_end, connection_context_factory=connection_context_factory, progress=progress)
    inputs, encoding = build_inputs(plan=plan, roster=roster, calendar=calendar, daily=daily, index_daily=index_daily)
    receipt = dict(status=encoding["contrast_status"], original_rows=len(inputs), original_package_days=len(days),
        unique_label_clusters=int(inputs.label_cluster.nunique()), calendar_sessions=len(calendar),
        original_calendar_ref=plan.calendar_ref.model_dump(mode="json"), extended_calendar_sha256=sha([d.isoformat() for d in calendar]),
        warmup=warmup, database_read=db_receipt, score_sources=score_receipts,
        source_evidence="QE_FROZEN_BACKTEST_SCORE", database_vintage="CURRENT_DATABASE_NON_VINTAGE",
        historical_native_receipt=False, original_pool_membership_claimed=False,
        physical_fit_count=0, test_or_sealed_finance_consumed=False, database_written=False, deployable=False)
    _checked(plan, output_root)
    return _publish(plan, root, "prepared", initial["stage_sha256"], {"rows.parquet": parquet_bytes(inputs),
        "days.parquet": parquet_bytes(days), "encoding.json": _json_bytes(encoding),
        "calendar.json": _json_bytes([d.isoformat() for d in calendar]), "receipt.json": _json_bytes(receipt)})


def _node(plan):
    if os.name == "nt" or Path(sys.executable).resolve() != Path(plan.node_python_uri).resolve():
        raise ValueError("study fit requires its explicit existing WSL Python; no install or Windows fit")
    actual = {name: version(name) for name in plan.library_versions}
    if actual != plan.library_versions:
        raise ValueError("existing-node library identities differ")
    return dict(python_uri=str(Path(sys.executable).resolve()), library_versions=actual, process_owned_by_study=True)


def _qe(probe, *, require_idle):
    observation = probe()
    if not isinstance(observation, dict):
        raise ValueError("fresh read-only QE observation is required")
    captured = datetime.fromisoformat(observation.get("captured_at", ""))
    counts = observation.get("active_counts", {})
    age = (datetime.now(timezone.utc)-captured).total_seconds() if captured.tzinfo is not None else 999.
    if (not 0 <= age <= 60 or set(counts) != {"single", "custom_evo", "multi_alpha"}
            or any(type(v) is not int or v < 0 for v in counts.values())
            or (require_idle and any(counts.values()))):
        raise ValueError("QE running/pending or observation stale; no new fit")
    return observation


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


def train_parent_score_study(*, plan, output_root, qe_idle_probe):
    from backend.services.advisory_model_first.parent_score_price_5td_model_v1 import model_payload, train_parent_score_model
    plan, root = _checked(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        return root/"trained"
    if (root/"fit_attempt.json").exists():
        raise ValueError("partial/STARTED attempt retained; never implicitly refit")
    encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
    if encoding.get("contrast_status") != "PREPARED_IDENTIFIABLE_NO_FIT":
        raise ValueError("parent information or honest training population is unidentifiable; no fit")
    node = _node(plan)
    models, count, claimed = {}, 0, False
    try:
        for arm in ARMS:
            _stage(plan, root, "prepared")
            # Arrow predicate projection before any feature/target decode, including poisoned held rows.
            rows = ds.dataset(root/"prepared"/"rows.parquet", format="parquet").to_table(
                filter=ds.field("pool").isin(["STRUCTURE", "ESTIMATION"])).to_pandas()
            _stage(plan, root, "prepared")
            def before_fit(name):
                nonlocal count, claimed
                if name != arm or count >= 2:
                    raise ValueError("physical fit arm/ordinal differs")
                observation = _qe(qe_idle_probe, require_idle=True)
                if not claimed:
                    _exclusive_json(root/"fit_attempt.json", dict(status="STARTED", plan_sha256=plan.plan_sha256,
                        node=node, physical_fit_budget=2, captured_at=datetime.now(timezone.utc).isoformat()))
                    claimed = True
                count += 1
                attempt = root/"fits"/arm/"attempt.json"
                _exclusive_json(attempt, dict(kind="PHYSICAL_FIT_STARTED", arm=arm, ordinal=count, qe_observation=observation))
                _journal(root, dict(kind="PHYSICAL_FIT_STARTED", arm=arm, ordinal=count, qe_observation=observation))
                _register(plan, root, "fit_started_"+str(count), generated=count, evidence=attempt)
            def after_fit(name):
                if name != arm:
                    raise ValueError("post-fit arm differs")
                observation = _qe(qe_idle_probe, require_idle=False)
                _journal(root, dict(kind="POST_FIT_QE_OBSERVATION", arm=arm, ordinal=count, qe_observation=observation))
                if any(observation["active_counts"].values()):
                    raise ValueError("QE became active during fit; retain attempt, no new fit or success claim")
            fitted = train_parent_score_model(rows=rows, encoding=encoding, arm=arm, plan_sha256=plan.plan_sha256,
                                              before_fit=before_fit, after_fit=after_fit)
            _stage(plan, root, "prepared")
            target = publish_stage(study_root=root/"fits"/arm, stage="trained", plan_sha256=plan.plan_sha256,
                parent_sha256=prepared["stage_sha256"], artifacts={"model.json": _json_bytes(model_payload(fitted))})
            header = read_stage(target, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
            models[arm] = dict(model_sha256=fitted.model_sha256, stage_sha256=header["stage_sha256"])
            _journal(root, dict(kind="PHYSICAL_FIT_COMPLETE", arm=arm, ordinal=count, **models[arm]))
    except Exception as exc:
        if claimed:
            _journal(root, dict(kind="ATTEMPT_INCOMPLETE_NO_IMPLICIT_RETRY", physical_fits_started=count, error_type=type(exc).__name__))
        raise
    _checked(plan, output_root)
    return _publish(plan, root, "trained", prepared["stage_sha256"], {"models.json": _json_bytes(models),
        "receipt.json": _json_bytes(dict(status="TWO_FITS_COMPLETE_NO_ACTIVATION", physical_fit_count=count,
            node=node, test_or_sealed_finance_consumed=False, deployable=False))}, generated=2)


def evaluate_parent_score_study(*, plan, output_root):
    from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import evaluate_parent_score_cohorts
    from backend.services.advisory_model_first.parent_score_price_5td_model_v1 import model_from_payload, query_parent_score_nodes
    plan, root = _checked(plan, output_root)
    trained, prepared = _stage(plan, root, "trained"), _stage(plan, root, "prepared")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        return root/"evaluated"
    predicate = (ds.field(KEY[0]) >= pd.Timestamp(plan.evaluation_start)) & (ds.field(KEY[0]) <= pd.Timestamp(plan.evaluation_end))
    rows = ds.dataset(root/"prepared"/"rows.parquet", format="parquet").to_table(filter=predicate).to_pandas()
    days = pd.read_parquet(root/"prepared"/"days.parquet")
    calendar = json.loads((root/"prepared"/"calendar.json").read_bytes())
    descriptors = json.loads((root/"trained"/"models.json").read_bytes())
    predictions = {}
    for arm in ARMS:
        model_root = root/"fits"/arm/"trained"
        header = read_stage(model_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
        model = model_from_payload(json.loads((model_root/"model.json").read_bytes()))
        if (header["stage_sha256"] != descriptors[arm]["stage_sha256"] or model.model_sha256 != descriptors[arm]["model_sha256"]
                or model.recipe["arm"] != arm or model.recipe["plan_sha256"] != plan.plan_sha256
                or model.recipe["encoding_sha256"] != sha(json.loads((root/"prepared"/"encoding.json").read_bytes()))):
            raise ValueError("fitted arm, source or encoding identity changed")
        features = rows.loc[:, ["package_id", "manifest_sha256", "run_id", *KEY, *FEATURES, "parent_score"]]
        predictions[arm] = query_parent_score_nodes(fitted=model, features=features, scenario_gap_bps=rows.observed_gap_bps)
    result = evaluate_parent_score_cohorts(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    _checked(plan, output_root)
    return _publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(result),
        "predictions.parquet": parquet_bytes(pd.concat([f.assign(arm=a) for a, f in predictions.items()], ignore_index=True))},
        generated=2, evaluated=2, result_class="NEGATIVE" if result["status"].startswith("NEGATIVE") else "EXPLORATORY")
