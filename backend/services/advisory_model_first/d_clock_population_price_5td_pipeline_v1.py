"""Four immutable stages, one shared readonly snapshot, two fits; no production wiring."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, MARK_POLICY, MARK_POLICY_SHA256, PARAMETERS, SCHEMA_SHA256, SOURCE_COLUMNS,
    DClockPopulationStudyPlanV1, node_path,
)
from backend.services.advisory_model_first.d_clock_population_price_5td_inputs_v1 import (
    build_d_clock_inputs_v1, construct_mark_targets_v1, verified_parent_rosters_v1,
)
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import _read_database
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes, warmup_calendar
from backend.services.advisory_model_first.parent_score_price_5td_pipeline_v1 import _exclusive_json, _journal, _node, _qe
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def d_clock_implementation_sha256_v1():
    names = [f"d_clock_population_price_5td_{n}_v1.py" for n in ("contracts", "inputs", "model", "pipeline", "evaluation", "cli")]
    names += ["cross_package_validation_inputs_v1.py", "generic_daily_price_input_v1.py", "generic_price_5td_contracts_v1.py",
        "generic_price_5td_models_v1.py", "generic_price_5td_labels_v1.py", "generic_price_5td_valuation_v2.py",
        "generic_population_price_5td_contracts_v1.py", "generic_population_price_5td_pipeline_v1.py",
        "generic_population_price_5td_cli_v1.py", "generic_population_price_5td_model_v1.py", "parent_score_price_5td_contracts_v1.py",
        "parent_score_price_5td_inputs_v1.py", "parent_score_price_5td_pipeline_v1.py", "parent_score_price_5td_model_v1.py",
        "economic_entry_sources.py", "economic_entry_pipeline.py", "research_control.py", "research_control_contracts.py"]
    return sha({n: hashlib.sha256((Path(__file__).parent/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})


def _checked(plan, output_root):
    plan = DClockPopulationStudyPlanV1.model_validate(plan)
    if plan.implementation_sha256 != d_clock_implementation_sha256_v1():
        raise ValueError("D-clock implementation changed after preregistration")
    return plan, node_path(output_root)/plan.experiment_id


def _stage(plan, root, until):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        header = read_stage(root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = header["stage_sha256"]
        if stage == until:
            return header
    raise ValueError("unknown D-clock stage")


def _register(plan, root, stage, *, generated=0, evaluated=0, evidence=None, result_class="EXPLORATORY"):
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id=plan.attempt_id, research_stage=stage.upper(),
        study_type=plan.study_type, objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        hypothesis_family_id="GP5-D-CLOCK-POPULATION-ALIGNMENT-1", unique_variable="TRAINING_INCLUSION_MASK",
        parent_lineage=tuple(s.run_id for s in plan.sources), dataset_identity=plan.plan_sha256,
        schema_identity=SCHEMA_SHA256, policy_identity=MARK_POLICY_SHA256, planned_trial_count=2,
        generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0, result_class=result_class,
        consumed_windows=(ConsumedWindowV1(window_id="CONSUMED_DEVELOPMENT_ONLY", dataset_identity=plan.plan_sha256,
            start_date=plan.train_start, end_date=plan.evaluation_end),),
        evidence_refs=(evidence_reference_for_file(evidence or root/stage/"manifest.json", role="d_clock_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _publish(plan, root, stage, parent, artifacts, *, generated=0, evaluated=0, result_class="EXPLORATORY"):
    if sum(len(v) for v in artifacts.values()) > 8*1024**3:
        raise ValueError("D-clock study artifact budget exceeds 8GiB")
    target = publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256,
                           parent_sha256=parent, artifacts=artifacts)
    _register(plan, root, stage, generated=generated, evaluated=evaluated, result_class=result_class)
    return target


def preregister_d_clock_population_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    if (root/"preregistered").exists():
        _stage(plan, root, "preregistered")
        return root/"preregistered"
    if plan.exact_retry_of is not None:
        # An explicit prior root, never 'latest'. Only an incomplete technical attempt is retryable.
        prior = node_path(plan.exact_retry_of)
        old = DClockPopulationStudyPlanV1.model_validate_json((prior/"preregistered"/"plan.json").read_bytes())
        if not (prior/"fit_attempt.json").is_file() or (prior/"trained").exists() or (prior/"evaluated").exists():
            raise ValueError("exact retry cannot revive a completed/negative study")
        ignored = {"attempt_id", "exact_retry_of", "source_commit", "implementation_sha256"}
        if {k: v for k, v in old.model_dump(mode="json").items() if k not in ignored} != {
                k: v for k, v in plan.model_dump(mode="json").items() if k not in ignored}:
            raise ValueError("technical retry cannot change the economic contract")
        if old.experiment_id != prior.name:
            raise ValueError("retry root and original run identity differ")
    return _publish(plan, root, "preregistered", None, {
        "plan.json": _json_bytes(plan.model_dump(mode="json")),
        "policy.json": _json_bytes(dict(mark_policy=MARK_POLICY, mark_policy_sha256=MARK_POLICY_SHA256,
            parameters=dict(PARAMETERS), physical_fit_budget=2, independent_confirmation=False))})


def prepare_d_clock_population_inputs_v1(*, plan, output_root, connection_context_factory=None, progress=None):
    plan, root = _checked(plan, output_root)
    initial = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        return root/"prepared"
    roster, days, original = verified_parent_rosters_v1(plan)
    calendar, warmup = warmup_calendar(calendar=original, train_start=plan.train_start,
                                       connection_context_factory=connection_context_factory)
    if progress is not None:
        progress(dict(phase="ORIGINAL_METADATA_ONLY", rows=len(roster), package_days=len(days), physical_fit_count=0))
    daily, index_daily, database = _read_database(symbols=sorted(set(roster.instrument)), calendar=calendar,
        cutoff=plan.evaluation_end, connection_context_factory=connection_context_factory, progress=progress)
    rows, encoding = build_d_clock_inputs_v1(plan=plan, roster=roster, calendar=calendar, daily=daily, index_daily=index_daily)
    receipt = dict(status=encoding["contrast_status"], original_rows=len(rows), original_package_days=len(days),
        unique_economic_clusters=int(rows.label_cluster.nunique()), warmup=warmup, database_read=database,
        original_parent_stage_sha256=plan.parent_stage_sha256, original_calendar_ref=plan.calendar_ref.model_dump(mode="json"),
        mark_policy_sha256=MARK_POLICY_SHA256, evaluation_targets_constructed=False, evaluation_predictions_frozen=False,
        historical_native_receipt=False, original_pool_membership_claimed=False, dataset_vintage="CURRENT_DATABASE_NON_VINTAGE",
        physical_fit_count=0, database_written=False, test_or_sealed_finance_consumed=False, deployable=False)
    return _publish(plan, root, "prepared", initial["stage_sha256"], {
        "rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days), "daily.parquet": parquet_bytes(daily),
        "encoding.json": _json_bytes(encoding), "calendar.json": _json_bytes([d.isoformat() for d in calendar]),
        "receipt.json": _json_bytes(receipt)})


def _public_evaluation_inputs(plan, root):
    predicate = (ds.field(KEY[0]) >= pd.Timestamp(plan.evaluation_start)) & (ds.field(KEY[0]) <= pd.Timestamp(plan.evaluation_end))
    rows = ds.dataset(root/"prepared"/"rows.parquet", format="parquet").to_table(
        columns=[*SOURCE_COLUMNS, *FEATURES, "reference_cny"], filter=predicate).to_pandas()
    if rows.groupby(list(KEY))[[*FEATURES, "reference_cny"]].nunique(dropna=False).gt(1).any().any():
        raise ValueError("shared D evaluation input conflicts between packages")
    return rows


def _freeze_evaluation_distributions(plan, root, models, prepared):
    from backend.services.advisory_model_first.d_clock_population_price_5td_model_v1 import (
        d_clock_model_from_payload_v1, freeze_d_clock_distribution_v1,
    )
    rows = _public_evaluation_inputs(plan, root)
    public = rows.drop_duplicates(list(KEY)).loc[:, [*KEY, *FEATURES]]
    frozen = []
    for arm in ARMS:
        model_root = root/"fits"/arm/"trained"
        header = read_stage(model_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
        model = d_clock_model_from_payload_v1(json.loads((model_root/"model.json").read_bytes()))
        if header["stage_sha256"] != models[arm]["stage_sha256"] or model.model_sha256 != models[arm]["model_sha256"]:
            raise ValueError("fitted distribution/model binding changed")
        frame = freeze_d_clock_distribution_v1(fitted=model, d_features=public.reset_index(drop=True))
        frozen.append(frame.assign(arm=arm))
    return pd.concat(frozen, ignore_index=True)


def train_d_clock_population_study_v1(*, plan, output_root, qe_idle_probe):
    from backend.services.advisory_model_first.d_clock_population_price_5td_model_v1 import d_clock_model_payload_v1, train_d_clock_population_model_v1
    plan, root = _checked(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        return root/"trained"
    encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
    if encoding["contrast_status"] != "PREPARED_IDENTIFIABLE_NO_FIT":
        return _publish(plan, root, "trained", prepared["stage_sha256"], {
            "models.json": _json_bytes({}), "receipt.json": _json_bytes(dict(status=encoding["contrast_status"],
                physical_fit_count=0, evaluation_targets_constructed=False, deployable=False))})
    if (root/"fit_attempt.json").exists():
        raise ValueError("partial STARTED attempt retained; never implicitly refit")
    node, models, count, claimed = _node(plan), {}, 0, False
    try:
        for arm in ARMS:
            column = "pool_"+arm
            rows = ds.dataset(root/"prepared"/"rows.parquet", format="parquet").to_table(
                filter=(ds.field(column).isin(["STRUCTURE", "ESTIMATION"]))
                    & (ds.field(KEY[0]) >= pd.Timestamp(plan.train_start))
                    & (ds.field(KEY[0]) <= pd.Timestamp(plan.train_end))
                    & (ds.field("label_information_end") <= pd.Timestamp(plan.train_end))).to_pandas()
            def before_fit(name):
                nonlocal count, claimed
                if name != arm or count >= 2:
                    raise ValueError("D-clock physical fit arm/ordinal differs")
                observation = _qe(qe_idle_probe, require_idle=True)
                if not claimed:
                    _exclusive_json(root/"fit_attempt.json", dict(status="STARTED", plan_sha256=plan.plan_sha256,
                        node=node, physical_fit_budget=2, captured_at=datetime.now(timezone.utc).isoformat()))
                    claimed = True
                count += 1
                attempt = root/"fits"/arm/"attempt.json"
                event = dict(kind="PHYSICAL_FIT_STARTED", arm=arm, ordinal=count, qe_observation=observation)
                _exclusive_json(attempt, event)
                _journal(root, event)
                _register(plan, root, "fit_started_"+str(count), generated=count, evidence=attempt)
            def after_fit(name):
                if name != arm:
                    raise ValueError("post-fit arm differs")
                observation = _qe(qe_idle_probe, require_idle=False)
                _journal(root, dict(kind="POST_FIT_QE_OBSERVATION", arm=arm, ordinal=count, qe_observation=observation))
                if any(observation["active_counts"].values()):
                    raise ValueError("QE became active during this fit; no next fit or success claim")
            model = train_d_clock_population_model_v1(rows=rows, encoding=encoding, arm=arm,
                plan_sha256=plan.plan_sha256, before_fit=before_fit, after_fit=after_fit)
            target = publish_stage(study_root=root/"fits"/arm, stage="trained", plan_sha256=plan.plan_sha256,
                parent_sha256=prepared["stage_sha256"], artifacts={"model.json": _json_bytes(d_clock_model_payload_v1(model))})
            header = read_stage(target, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
            models[arm] = dict(model_sha256=model.model_sha256, stage_sha256=header["stage_sha256"])
            _journal(root, dict(kind="PHYSICAL_FIT_COMPLETE", arm=arm, ordinal=count, **models[arm]))
        distributions = _freeze_evaluation_distributions(plan, root, models, prepared)
    except Exception as exc:
        if claimed:
            _journal(root, dict(kind="ATTEMPT_INCOMPLETE_NO_IMPLICIT_RETRY", physical_fits_started=count, error_type=type(exc).__name__))
        raise
    return _publish(plan, root, "trained", prepared["stage_sha256"], {
        "models.json": _json_bytes(models), "d_distributions.parquet": parquet_bytes(distributions),
        "receipt.json": _json_bytes(dict(status="TWO_FITS_D_DISTRIBUTIONS_FROZEN_NO_ACTIVATION", physical_fit_count=count,
            public_unique_D_keys=len(distributions)//2, evaluation_targets_constructed=False, node=node, deployable=False))}, generated=count)


def evaluate_d_clock_population_study_v1(*, plan, output_root):
    from backend.services.advisory_model_first.d_clock_population_price_5td_evaluation_v1 import evaluate_d_clock_population_cohorts_v1
    from backend.services.advisory_model_first.d_clock_population_price_5td_model_v1 import d_clock_model_from_payload_v1, query_frozen_d_clock_nodes_v1
    plan, root = _checked(plan, output_root)
    trained = _stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        return root/"evaluated"
    receipt = json.loads((root/"trained"/"receipt.json").read_bytes())
    if receipt["physical_fit_count"] == 0:
        return _publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(dict(
            status=receipt["status"], physical_fit_count=0, financial_evaluation_performed=False,
            evidence_use="NAVIGATION_ONLY", deployable=False, activation_evidence=False))})
    if receipt.get("evaluation_targets_constructed") is not False:
        raise ValueError("evaluation targets were consumed before the D distributions froze")
    # Complete trained-stage digest is checked before any evaluation target is decoded.
    distributions = pd.read_parquet(root/"trained"/"d_distributions.parquet")
    rows = _public_evaluation_inputs(plan, root)
    calendar = json.loads((root/"prepared"/"calendar.json").read_bytes())
    daily = pd.read_parquet(root/"prepared"/"daily.parquet")
    labels = construct_mark_targets_v1(plan=plan, roster=rows, calendar=[pd.Timestamp(v).date() for v in calendar],
                                     daily=daily, cutoff=plan.evaluation_end, legacy=False)
    rows = rows.merge(labels, on=["package_id", *KEY], validate="one_to_one", sort=False)
    days = pd.read_parquet(root/"prepared"/"days.parquet")
    descriptors, predictions = json.loads((root/"trained"/"models.json").read_bytes()), {}
    prepared = json.loads((root/"prepared"/"manifest.json").read_bytes())
    for arm in ARMS:
        model_root = root/"fits"/arm/"trained"
        header = read_stage(model_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
        model = d_clock_model_from_payload_v1(json.loads((model_root/"model.json").read_bytes()))
        if (header["stage_sha256"] != descriptors[arm]["stage_sha256"] or model.model_sha256 != descriptors[arm]["model_sha256"]
                or model.recipe["arm"] != arm or model.recipe["plan_sha256"] != plan.plan_sha256):
            raise ValueError("evaluation fitted arm/plan identity differs")
        frozen = distributions.loc[distributions.arm.eq(arm)].drop(columns="arm")
        if frozen.duplicated(list(KEY)).any():
            raise ValueError("frozen public distribution D keys duplicate")
        mapped = rows.loc[:, [*SOURCE_COLUMNS]].merge(frozen, on=list(KEY), how="left", validate="many_to_one", sort=False)
        if mapped.model_sha256.isna().any():
            raise ValueError("frozen D distribution omitted an original candidate")
        predictions[arm] = query_frozen_d_clock_nodes_v1(fitted=model, frozen=mapped,
            price_cny=rows.entry_price_cny, reference_cny=rows.reference_cny)
    evaluation = evaluate_d_clock_population_cohorts_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    return _publish(plan, root, "evaluated", trained["stage_sha256"], {
        "evaluation.json": _json_bytes(evaluation), "labels.parquet": parquet_bytes(labels),
        "predictions.parquet": parquet_bytes(pd.concat([f.assign(arm=a) for a, f in predictions.items()], ignore_index=True))},
        generated=2, evaluated=2, result_class="NEGATIVE" if evaluation["status"].startswith("NEGATIVE") else "EXPLORATORY")
