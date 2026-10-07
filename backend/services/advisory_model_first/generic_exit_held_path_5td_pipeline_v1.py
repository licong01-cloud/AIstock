"""One held-path candidate; immutable old labels/predictions, zero old/control fits."""
from datetime import datetime, timezone
import inspect
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _verify_reference, file_sha256, publish_stage, read_stage
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day
from backend.services.advisory_model_first.generic_exit_held_path_5td_contracts_v1 import (
    FEATURE_KEY, FEATURE_SHA256, HYPOTHESIS, LABEL_POLICY_SHA256, MODEL_FEATURES,
    PRICE_FIELDS, RECIPE, RECIPE_SHA256,
)
from backend.services.advisory_model_first.generic_exit_held_path_5td_features_v1 import build_exit_held_path_features_v1
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_audit_v1 import (
    _parquet, _read_development, evaluate_exit_chains_v1, exit_fold_schedule_v1, summarize_exit_v1,
)
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import AUDIT_SHA256, ordered_calendar
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, EvidenceReferenceV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_exit_held_path_5td_{part}_v1.py" for part in ("contracts", "features", "pipeline")]
    names += [f"generic_remaining_value_exit_5td_{part}_v1.py" for part in ("contracts", "labels", "audit")]
    identities = {name: file_sha256(directory/name) for name in names}
    identities["number_and_day_input"] = file_sha256(Path(inspect.getsourcefile(_day)))
    return sha(identities)


def _load(plan, output_root):
    fields = {"schema_version", "inputs", "implementation_sha256", "feature_sha256", "label_policy_sha256"}
    roles = {"base_plan", "prepared_rows", "folds", "calendar", "old_predictions", "old_cohorts", "raw_daily"}
    if (not isinstance(plan, dict) or set(plan) != fields or plan["schema_version"] != "exit5_held_path_plan_v1"
            or set(plan["inputs"]) != roles or plan["implementation_sha256"] != implementation_sha256()
            or plan["feature_sha256"] != FEATURE_SHA256 or plan["label_policy_sha256"] != LABEL_POLICY_SHA256):
        raise ValueError("held-path exact code/feature/label/input identity differs")
    paths = {key: _verify_reference(EvidenceReferenceV1.model_validate(value)) for key, value in plan["inputs"].items()}
    original = json.loads(paths["base_plan"].read_text(encoding="utf8"))
    if original.get("schema_version") != "exit5_remaining_value_plan_v1":
        raise ValueError("held-path label source is not the original fixed Exit study")
    original_identity = sha(dict(plan=original, policy=LABEL_POLICY_SHA256, audit=AUDIT_SHA256))
    base = paths["base_plan"].parent.parent
    previous = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        manifest = read_stage(base/stage, stage=stage, plan_sha256=original_identity, parent_sha256=previous)
        previous = manifest["stage_sha256"]
    expected = {"prepared_rows": base/"prepared"/"rows.parquet", "folds": base/"prepared"/"folds.json",
        "calendar": base/"prepared"/"calendar.json", "old_predictions": base/"trained"/"predictions.parquet",
        "old_cohorts": base/"evaluated"/"cohorts.parquet"}
    if (any(paths[key] != path for key, path in expected.items())
            or paths["raw_daily"] != Path(original["inputs"]["raw_daily"]["artifact_uri"])
            or plan["inputs"]["raw_daily"]["sha256"] != original["inputs"]["raw_daily"]["sha256"]):
        raise ValueError("held-path cannot substitute original episodes/control or raw source")
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:" or declared != root:
        raise ValueError("held-path needs an explicit non-C real artifact path")
    identity = sha(dict(plan=plan, recipe=RECIPE_SHA256))
    return root/("advexitpath_"+identity[:24]), identity, paths, original


def _stage(root, identity, name):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        manifest = read_stage(root/stage, stage=stage, plan_sha256=identity, parent_sha256=parent)
        parent = manifest["stage_sha256"]
        if stage == name:
            return manifest
    raise ValueError("unknown held-path stage")


def _record(root, original, stage, fits=0):
    record = build_trial_record(experiment_id=root.name, attempt_id="exact_attempt_v1", research_stage=stage.upper(),
        study_type="LEARNABILITY_AUDIT", hypothesis_family_id=HYPOTHESIS,
        parent_lineage=tuple(original["parent_lineage"]), unique_variable="THREE_S_KNOWN_ORIGINAL_HELD_PATH_STATES",
        objective_contract="RISK_MANAGED_ADVISORY", dataset_identity=original["dataset_identity"],
        schema_identity=FEATURE_SHA256, policy_identity=LABEL_POLICY_SHA256, planned_trial_count=4,
        generated_trial_count=fits, evaluated_trial_count=fits if stage == "evaluated" else 0, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=original["dataset_identity"], start_date=_day(original["development_start"]), end_date=_day(original["development_cutoff"])),),
        result_class="EXPLORATORY", decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="held_path_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def preregister_exit_held_path_v1(*, plan, output_root):
    root, identity, _, original = _load(plan, output_root)
    publish_stage(study_root=root, stage="preregistered", plan_sha256=identity, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan), "recipe.json": _json_bytes(RECIPE)})
    _record(root, original, "preregistered")
    return root/"preregistered"/"plan.json"


def prepare_exit_held_path_v1(*, plan, output_root):
    root, identity, paths, original = _load(plan, output_root)
    parent = _stage(root, identity, "preregistered")
    if (root/"prepared").exists():
        _stage(root, identity, "prepared")
        return root/"prepared"
    rows = pd.read_parquet(paths["prepared_rows"])
    calendar = ordered_calendar(json.loads(paths["calendar"].read_text(encoding="utf8")))
    cutoff = _day(original["development_cutoff"])
    unmatured = rows.endpoint_date.map(lambda value: _day(value, nullable=True) is None or _day(value) > cutoff)
    if rows.loc[unmatured, "y_hold_bps"].notna().any():
        raise ValueError("held-path source labels include unallowed unmatured future values")
    prices = _read_development(paths["raw_daily"], PRICE_FIELDS, cutoff)
    features, receipt = build_exit_held_path_features_v1(rows=rows, prices=prices, calendar=calendar, development_cutoff=cutoff)
    joined = rows.merge(features, on=list(FEATURE_KEY), how="outer", validate="one_to_one", indicator=True)
    if not joined._merge.eq("both").all() or len(joined) != len(rows):
        raise ValueError("held-path original population changed")
    joined = joined.drop(columns="_merge")
    folds = json.loads(paths["folds"].read_text(encoding="utf8"))
    if folds != exit_fold_schedule_v1(labels=rows, calendar=calendar):
        raise ValueError("held-path original chronological folds changed")
    publish_stage(study_root=root, stage="prepared", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"rows.parquet": _parquet(joined), "folds.json": _json_bytes(folds), "receipt.json": _json_bytes(receipt)})
    _record(root, original, "prepared")
    return root/"prepared"


def _matrix(rows, recipe=None):
    raw = rows.loc[:, MODEL_FEATURES].to_numpy(dtype=float)
    if np.isinf(raw).any():
        raise ValueError("held-path model input is infinite")
    missing = np.isnan(raw)
    if recipe is None:
        median = np.asarray([np.median(col[np.isfinite(col)]) if np.isfinite(col).any() else 0. for col in raw.T])
        filled = np.where(missing, median, raw)
        mean, scale = filled.mean(axis=0), filled.std(axis=0)
        scale[scale == 0] = 1.
        recipe = dict(median=median.tolist(), mean=mean.tolist(), scale=scale.tolist())
    matrix = np.column_stack(((np.where(missing, recipe["median"], raw)-recipe["mean"])/recipe["scale"], missing[:, :-1].astype(float)))
    if matrix.shape[1] != RECIPE["model_inputs"]:
        raise ValueError("held-path fixed 25-input contract differs")
    return matrix, recipe


def fit_held_path_fold_v1(*, rows, fold, before_fit):
    train = rows.loc[rows.episode_id.isin(fold["train_episode_ids"]) & rows.held.eq(True) & rows.y_hold_bps.notna()]
    evaluation = rows.loc[rows.episode_id.isin(fold["evaluation_episode_ids"])]
    if train.empty:
        return dict(status="UNKNOWN_NO_MATURE_TRAIN", physical_fits=0), pd.DataFrame(columns=[*FEATURE_KEY, "predicted_y_hold_bps"])
    x, recipe = _matrix(train)
    future, _ = _matrix(evaluation, recipe)
    y = train.y_hold_bps.to_numpy(dtype=float)
    if not np.isfinite(y).all():
        raise ValueError("held-path mature train target is not finite")
    before_fit(fold["ordinal"])
    model = Ridge(alpha=RECIPE["alpha"]).fit(x, y)
    predictions = evaluation.loc[:, FEATURE_KEY].copy()
    predictions["predicted_y_hold_bps"] = model.predict(future)
    return dict(status="FITTED", physical_fits=1, ordinal=fold["ordinal"], recipe=recipe,
        coefficients=model.coef_.tolist(), intercept=float(model.intercept_), train_decisions=len(train)), predictions


def _write(path, content):
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def train_exit_held_path_v1(*, plan, output_root, qe_idle_check):
    root, identity, _, original = _load(plan, output_root)
    parent = _stage(root, identity, "prepared")
    if (root/"trained").exists():
        _stage(root, identity, "trained")
        return root/"trained"
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    folds = json.loads((root/"prepared"/"folds.json").read_text(encoding="utf8"))
    models, outputs, fits = {}, [], 0
    for fold in folds:
        folder = root/"folds"/str(fold["ordinal"])
        folder.mkdir(parents=True, exist_ok=True)
        marker, model_path, pred_path = folder/"fit_attempt.json", folder/"model.json", folder/"predictions.parquet"
        if marker.exists():
            saved = json.loads(marker.read_text(encoding="utf8"))
            if (saved.get("status") != "COMPLETE" or saved.get("plan_sha256") != identity
                    or saved.get("model_sha256") != file_sha256(model_path) or saved.get("predictions_sha256") != file_sha256(pred_path)):
                raise ValueError("held-path unresolved fit must not be implicitly retrained")
            model, predictions = json.loads(model_path.read_text(encoding="utf8")), pd.read_parquet(pred_path)
        else:
            checks = []
            def before_fit(ordinal):
                receipt = qe_idle_check()
                now = datetime.now(timezone.utc)
                if (not 0 <= (now-datetime.fromisoformat(receipt["checked_at_utc"])).total_seconds() <= 60
                        or set(receipt["running_counts"]) != {"single", "custom_evo", "multi_alpha"}
                        or any(type(v) is not int or v != 0 for v in receipt["running_counts"].values())):
                    raise ValueError("held-path next fit waits for a fresh QE three-path idle read")
                checks.append(receipt)
                _write(marker, _json_bytes(dict(status="STARTED", plan_sha256=identity, ordinal=ordinal, qe_before=receipt)))
            model, predictions = fit_held_path_fold_v1(rows=rows, fold=fold, before_fit=before_fit)
            after = qe_idle_check() if model["physical_fits"] else None
            _write(model_path, _json_bytes(model))
            _write(pred_path, _parquet(predictions))
            done = folder/"fit_complete.json"
            _write(done, _json_bytes(dict(status="COMPLETE", plan_sha256=identity, physical_fits=model["physical_fits"],
                qe_before=checks, qe_after=after, model_sha256=file_sha256(model_path), predictions_sha256=file_sha256(pred_path))))
            os.replace(done, marker)
        fits += model["physical_fits"]
        if fits > RECIPE["max_physical_fits"]:
            raise ValueError("held-path candidate fit budget exceeded")
        models[str(fold["ordinal"])] = model
        outputs.append(predictions)
    publish_stage(study_root=root, stage="trained", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"models.json": _json_bytes(dict(models=models, physical_fits=fits, control_refits=0)),
            "predictions.parquet": _parquet(pd.concat(outputs, ignore_index=True))})
    _record(root, original, "trained", fits)
    return root/"trained"


def _paired_block_stats(values):
    values = np.asarray(values, dtype=float)
    finite = np.isfinite(values)
    result = dict(mean_bps=float(values[finite].mean()) if finite.any() else None, ci95_bps=None, mde80_bps=None)
    if finite.sum() >= 2*RECIPE["bootstrap_block"]:
        rng = np.random.default_rng(RECIPE["bootstrap_seed"])
        starts = rng.integers(0, len(values), size=(RECIPE["bootstrap_replicates"], int(np.ceil(len(values)/RECIPE["bootstrap_block"]))))
        indexes = (starts[:, :, None]+np.arange(RECIPE["bootstrap_block"])) % len(values)
        samples = values[indexes.reshape(len(starts), -1)[:, :len(values)]]
        means = np.nanmean(samples[np.isfinite(samples).any(axis=1)], axis=1)
        result.update(ci95_bps=[float(value) for value in np.quantile(means, [.025, .975])],
            mde80_bps=float((1.96+.84)*np.std(means, ddof=1)))
    return result


def evaluate_exit_held_path_v1(*, plan, output_root):
    root, identity, paths, original = _load(plan, output_root)
    parent = _stage(root, identity, "trained")
    if (root/"evaluated").exists():
        _stage(root, identity, "evaluated")
        return root/"evaluated"
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    predictions = pd.read_parquet(root/"trained"/"predictions.parquet")
    folds = json.loads((root/"prepared"/"folds.json").read_text(encoding="utf8"))
    ids = [episode for fold in folds for episode in fold["evaluation_episode_ids"]]
    episodes, cohorts, decisions = evaluate_exit_chains_v1(rows=rows, predictions=predictions, evaluation_episode_ids=ids)
    old = pd.read_parquet(paths["old_cohorts"])
    paired = cohorts.merge(old.loc[:, ["entry_date", "baseline_bps", "candidate_bps", "complete"]],
        on="entry_date", how="outer", validate="one_to_one", suffixes=("", "_old"), indicator=True)
    if (not paired._merge.eq("both").all() or not paired.complete.eq(paired.complete_old).all()
            or not np.allclose(paired.baseline_bps.to_numpy(dtype=float), paired.baseline_bps_old.to_numpy(dtype=float), equal_nan=True)):
        raise ValueError("held-path same-population frozen baseline/control changed")
    result = summarize_exit_v1(episodes=episodes, cohorts=cohorts)
    old_comparison = _paired_block_stats(np.where(paired.complete, paired.candidate_bps-paired.candidate_bps_old, np.nan))
    result.update(hypothesis=HYPOTHESIS, feature_sha256=FEATURE_SHA256, control_refits=0,
        mean_increment_over_frozen_exit_bps=old_comparison["mean_bps"],
        frozen_exit_comparison_ci95_bps=old_comparison["ci95_bps"], frozen_exit_comparison_mde80_bps=old_comparison["mde80_bps"],
        physical_fits=json.loads((root/"trained"/"models.json").read_text(encoding="utf8"))["physical_fits"],
        new_oracle_runs=0, original_oracle_only="REFERENCE_NOT_NEW_MODEL_EVIDENCE",
        original_episode_count=int(rows.episode_id.nunique()), retained_decisions=len(rows),
        remaining_model="FIXED_ALPHA1_MEAN_ONLY_NO_TAIL_HEAD_OR_DEPLOYED_PRICE_FAMILY")
    publish_stage(study_root=root, stage="evaluated", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"episodes.parquet": _parquet(episodes), "cohorts.parquet": _parquet(cohorts), "decisions.parquet": _parquet(decisions), "result.json": _json_bytes(result)})
    _record(root, original, "evaluated", result["physical_fits"])
    return root/"evaluated"
