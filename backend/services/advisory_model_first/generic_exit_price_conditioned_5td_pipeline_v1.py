"""Immutable S-curve first, label-only U query later; one independent Exit audit."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, _verify_reference, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day
from backend.services.advisory_model_first.generic_exit_price_conditioned_5td_contracts_v1 import (
    HYPOTHESIS, KEY, POLICY_SHA256, QUERY_FIELDS, RECIPE, RECIPE_SHA256, S_FIELDS, TRAIN_FIELDS,
)
from backend.services.advisory_model_first.generic_exit_price_conditioned_5td_models_v1 import (
    fit_price_conditioned_fold_v1, query_sealed_curves_v1, seal_s_curves_v1,
)
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_audit_v1 import (
    _parquet, evaluate_exit_chains_v1, exit_fold_schedule_v1, summarize_exit_v1,
)
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import AUDIT, AUDIT_SHA256, ordered_calendar
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, EvidenceReferenceV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_exit_price_conditioned_5td_{part}_v1.py" for part in ("contracts", "models", "pipeline")]
    names += [f"generic_remaining_value_exit_5td_{part}_v1.py" for part in ("contracts", "labels", "audit")]
    names += ["generic_daily_price_input_v1.py"]
    return sha({name: file_sha256(directory/name) for name in names})


def _load(plan, output_root):
    fields = {"schema_version", "inputs", "implementation_sha256", "recipe_sha256", "label_policy_sha256"}
    roles = {"base_plan", "prepared_rows", "folds", "calendar", "old_predictions", "old_cohorts",
        *[stage+"_manifest" for stage in ("preregistered", "prepared", "trained", "evaluated")]}
    if (not isinstance(plan, dict) or set(plan) != fields or plan["schema_version"] != "exit5_price_conditioned_plan_v1"
            or set(plan["inputs"]) != roles or plan["implementation_sha256"] != implementation_sha256()
            or plan["recipe_sha256"] != RECIPE_SHA256 or plan["label_policy_sha256"] != POLICY_SHA256):
        raise ValueError("Exit conditioned exact code/query/label/input identity differs")
    paths = {key: _verify_reference(EvidenceReferenceV1.model_validate(value)) for key, value in plan["inputs"].items()}
    original = json.loads(paths["base_plan"].read_text(encoding="utf8"))
    if original.get("schema_version") != "exit5_remaining_value_plan_v1":
        raise ValueError("Exit conditioned input is not the original fixed Exit audit")
    base = paths["base_plan"].parent.parent
    previous, original_identity = None, sha(dict(plan=original, policy=POLICY_SHA256, audit=AUDIT_SHA256))
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        if paths[stage+"_manifest"] != base/stage/"manifest.json":
            raise ValueError("Exit original stage cannot be replaced")
        manifest = read_stage(base/stage, stage=stage, plan_sha256=original_identity, parent_sha256=previous)
        previous = manifest["stage_sha256"]
    expected = dict(prepared_rows=base/"prepared"/"rows.parquet", folds=base/"prepared"/"folds.json",
        calendar=base/"prepared"/"calendar.json", old_predictions=base/"trained"/"predictions.parquet",
        old_cohorts=base/"evaluated"/"cohorts.parquet")
    if any(paths[key] != value for key, value in expected.items()):
        raise ValueError("Exit conditioned cannot substitute original population/control")
    declared, root = Path(output_root), Path(output_root).resolve()
    if not declared.is_absolute() or declared != root or root.drive.upper() == "C:":
        raise ValueError("Exit study requires an explicit non-C real artifact path")
    identity = sha(dict(plan=plan, recipe=RECIPE_SHA256))
    return root/("advexitquery_"+identity[:24]), identity, paths, original


def _stage(root, identity, name):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        manifest = read_stage(root/stage, stage=stage, plan_sha256=identity, parent_sha256=parent)
        parent = manifest["stage_sha256"]
        if stage == name:
            return manifest
    raise ValueError("unknown Exit conditioned stage")


def _record(root, original, stage, fits=0):
    record = build_trial_record(experiment_id=root.name, attempt_id="exact_attempt_v1", research_stage=stage.upper(),
        study_type="LEARNABILITY_AUDIT", hypothesis_family_id=HYPOTHESIS,
        parent_lineage=tuple(original["parent_lineage"]), unique_variable=RECIPE["unique_variable"],
        objective_contract="RISK_MANAGED_ADVISORY", dataset_identity=original["dataset_identity"],
        schema_identity=RECIPE_SHA256, policy_identity=POLICY_SHA256, planned_trial_count=4,
        generated_trial_count=fits, evaluated_trial_count=fits if stage == "evaluated" else 0, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=original["dataset_identity"], start_date=_day(original["development_start"]), end_date=_day(original["development_cutoff"])),),
        result_class="EXPLORATORY", decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="exit_conditioned_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def preregister_exit_conditioned_v1(*, plan, output_root):
    root, identity, _, original = _load(plan, output_root)
    publish_stage(study_root=root, stage="preregistered", plan_sha256=identity, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan), "recipe.json": _json_bytes(RECIPE)})
    _record(root, original, "preregistered")
    return root/"preregistered"/"plan.json"


def prepare_exit_conditioned_v1(*, plan, output_root):
    root, identity, paths, original = _load(plan, output_root)
    parent = _stage(root, identity, "preregistered")
    if (root/"prepared").exists():
        _stage(root, identity, "prepared")
        return root/"prepared"
    rows = pd.read_parquet(paths["prepared_rows"])
    if rows.duplicated(list(KEY)).any() or len(rows) > 15000:
        raise ValueError("Exit original decision domain differs")
    cutoff = _day(original["development_cutoff"])
    unmatured = rows.endpoint_date.map(lambda value: _day(value, nullable=True) is None or _day(value) > cutoff)
    if rows.loc[unmatured, ["y_hold_bps", "sell_scenario_bps"]].notna().any().any():
        raise ValueError("Exit unmatured original labels cannot become train queries")
    calendar = ordered_calendar(json.loads(paths["calendar"].read_text(encoding="utf8")))
    folds = json.loads(paths["folds"].read_text(encoding="utf8"))
    if folds != exit_fold_schedule_v1(labels=rows, calendar=calendar):
        raise ValueError("Exit original chronological fold geometry changed")
    publish_stage(study_root=root, stage="prepared", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"rows.parquet": _parquet(rows), "s_inputs.parquet": _parquet(rows.loc[:, S_FIELDS]),
            "folds.json": _json_bytes(folds), "receipt.json": _json_bytes(dict(retained_decisions=len(rows),
                original_episodes=int(rows.episode_id.nunique()), physical_fits=0, old_control_refits=0,
                s_input_fields=list(S_FIELDS), raw_price_product=False, sealed_holdout_read=False))})
    _record(root, original, "prepared")
    return root/"prepared"


def _write(path, content):
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def _train_fold(root, identity, fold, qe_idle_check):
    folder = root/"folds"/str(fold["ordinal"])
    folder.mkdir(parents=True, exist_ok=True)
    marker, model_path, curve_path = folder/"fit_attempt.json", folder/"model.json", folder/"curves.parquet"
    if marker.exists():
        saved = json.loads(marker.read_text(encoding="utf8"))
        if (saved.get("status") != "COMPLETE" or saved.get("plan_sha256") != identity
                or saved.get("model_sha256") != file_sha256(model_path) or saved.get("curves_sha256") != file_sha256(curve_path)):
            raise ValueError("Exit conditioned unresolved fit must not be automatically repeated")
        return json.loads(model_path.read_text(encoding="utf8")), pd.read_parquet(curve_path)
    # Financial supervision is projected/filtered to past training IDs before decoding.
    train = pd.read_parquet(root/"prepared"/"rows.parquet", columns=list(TRAIN_FIELDS),
        filters=[("episode_id", "in", fold["train_episode_ids"])])
    state = pd.read_parquet(root/"prepared"/"s_inputs.parquet", columns=list(S_FIELDS),
        filters=[("episode_id", "in", fold["evaluation_episode_ids"])])
    checks = []
    def before_fit(ordinal):
        receipt, now = qe_idle_check(), datetime.now(timezone.utc)
        if (not 0 <= (now-datetime.fromisoformat(receipt["checked_at_utc"])).total_seconds() <= 60
                or set(receipt["running_counts"]) != {"single", "custom_evo", "multi_alpha"}
                or any(type(value) is not int or value != 0 for value in receipt["running_counts"].values())):
            raise ValueError("Exit conditioned next fit waits for fresh QE three-path idle")
        checks.append(receipt)
        _write(marker, _json_bytes(dict(status="STARTED", plan_sha256=identity, ordinal=ordinal, qe_before=receipt)))
    model = fit_price_conditioned_fold_v1(train_rows=train, ordinal=fold["ordinal"], before_fit=before_fit)
    after = qe_idle_check() if model["physical_fits"] else None
    curves = seal_s_curves_v1(model=model, s_inputs=state)
    _write(model_path, _json_bytes(model))
    _write(curve_path, _parquet(curves))
    done = folder/"fit_complete.json"
    _write(done, _json_bytes(dict(status="COMPLETE", plan_sha256=identity, physical_fits=model["physical_fits"],
        qe_before=checks, qe_after=after, model_sha256=file_sha256(model_path), curves_sha256=file_sha256(curve_path))))
    os.replace(done, marker)
    return model, curves


def train_exit_conditioned_v1(*, plan, output_root, qe_idle_check):
    root, identity, _, original = _load(plan, output_root)
    parent = _stage(root, identity, "prepared")
    if (root/"trained").exists():
        _stage(root, identity, "trained")
        return root/"trained"
    folds = json.loads((root/"prepared"/"folds.json").read_text(encoding="utf8"))
    models, outputs, fits = {}, [], 0
    for fold in folds:
        model, curves = _train_fold(root, identity, fold, qe_idle_check)
        fits += model["physical_fits"]
        if fits > RECIPE["max_physical_fits"]:
            raise ValueError("Exit conditioned candidate fit budget exceeded")
        models[str(fold["ordinal"])] = model
        outputs.append(curves)
    publish_stage(study_root=root, stage="trained", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"models.json": _json_bytes(dict(models=models, physical_fits=fits, control_refits=0)),
            "curves.parquet": _parquet(pd.concat(outputs, ignore_index=True))})
    _record(root, original, "trained", fits)
    return root/"trained"


def _paired_stats(values):
    values = np.asarray(values, dtype=float)
    finite = np.isfinite(values)
    result = dict(mean_bps=float(values[finite].mean()) if finite.any() else None, ci95_bps=None, mde80_bps=None)
    if finite.sum() >= 2*AUDIT["bootstrap_block"]:
        rng = np.random.default_rng(AUDIT["bootstrap_seed"])
        starts = rng.integers(0, len(values), size=(AUDIT["bootstrap_replicates"], int(np.ceil(len(values)/AUDIT["bootstrap_block"]))))
        indexes = (starts[:, :, None]+np.arange(AUDIT["bootstrap_block"])) % len(values)
        samples = values[indexes.reshape(len(starts), -1)[:, :len(values)]]
        means = np.nanmean(samples[np.isfinite(samples).any(axis=1)], axis=1)
        result.update(ci95_bps=[float(value) for value in np.quantile(means, [.025, .975])],
            mde80_bps=float((1.96+.84)*np.std(means, ddof=1)))
    return result


def evaluate_exit_conditioned_v1(*, plan, output_root):
    root, identity, paths, original = _load(plan, output_root)
    parent = _stage(root, identity, "trained")
    if (root/"evaluated").exists():
        _stage(root, identity, "evaluated")
        return root/"evaluated"
    # The S curves have already been immutably published; only now read future query labels.
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    curves = pd.read_parquet(root/"trained"/"curves.parquet")
    evaluation = rows.loc[rows.episode_id.isin(curves.episode_id)]
    predictions = query_sealed_curves_v1(curves=curves, queries=evaluation.loc[:, QUERY_FIELDS])
    predictions["s_date"] = predictions.s_date.map(_day)
    rows["s_date"] = rows.s_date.map(lambda value: _day(value, nullable=True))
    ids = sorted(set(evaluation.episode_id))
    episodes, cohorts, decisions = evaluate_exit_chains_v1(rows=rows, predictions=predictions.loc[:, [*KEY, "predicted_y_hold_bps"]], evaluation_episode_ids=ids)
    old = pd.read_parquet(paths["old_cohorts"])
    paired = cohorts.merge(old.loc[:, ["entry_date", "baseline_bps", "candidate_bps", "complete"]],
        on="entry_date", how="outer", validate="one_to_one", suffixes=("", "_old"), indicator=True)
    if (not paired._merge.eq("both").all() or not paired.complete.eq(paired.complete_old).all()
            or not np.allclose(paired.baseline_bps.to_numpy(dtype=float), paired.baseline_bps_old.to_numpy(dtype=float), equal_nan=True)):
        raise ValueError("Exit conditioned original baseline/control population differs")
    result = summarize_exit_v1(episodes=episodes, cohorts=cohorts)
    comparison = _paired_stats(np.where(paired.complete, paired.candidate_bps-paired.candidate_bps_old, np.nan))
    result.update(hypothesis=HYPOTHESIS, query_role=RECIPE["query_role"], control_refits=0, new_oracle_runs=0,
        original_oracle_only="REFERENCE_NOT_NEW_CANDIDATE_EVIDENCE",
        mean_increment_over_frozen_exit_bps=comparison["mean_bps"], frozen_exit_comparison_ci95_bps=comparison["ci95_bps"],
        frozen_exit_comparison_mde80_bps=comparison["mde80_bps"], query_status_counts=predictions.query_status.value_counts().to_dict(),
        unknown_query_policy="UNKNOWN_MODEL_QUERY_BASELINE_ACTION_NOT_CASH_OR_MODEL_SUCCESS",
        physical_fits=json.loads((root/"trained"/"models.json").read_text(encoding="utf8"))["physical_fits"],
        retained_decisions=len(rows), original_episode_count=int(rows.episode_id.nunique()),
        raw_price_product=False, calibrated_probability=False, tail_head=False)
    publish_stage(study_root=root, stage="evaluated", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"episodes.parquet": _parquet(episodes), "cohorts.parquet": _parquet(cohorts),
            "decisions.parquet": _parquet(decisions), "query_predictions.parquet": _parquet(predictions), "result.json": _json_bytes(result)})
    _record(root, original, "evaluated", result["physical_fits"])
    return root/"evaluated"
