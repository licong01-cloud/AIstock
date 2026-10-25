"""Frozen Exit fold transfer; S-only inputs and original T/U/E policy."""
from __future__ import annotations

from datetime import date
import importlib.util
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.cross_package_validation_adapters_v1 import MODULE_PREFIX, _checked_member
from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import (
    canonical_sha, file_sha, publish_bytes, publish_json, readonly_connection,
)
from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import (
    _parquet, build_shared_daily_features, checked_plan,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, KEY, ROSTER, _day


ORIGINAL_MODULES = (
    "generic_remaining_value_exit_5td_contracts_v1", "generic_remaining_value_exit_5td_labels_v1",
    "generic_remaining_value_exit_5td_audit_v1", "generic_exit_held_path_5td_contracts_v1",
    "generic_exit_held_path_5td_features_v1", "generic_exit_held_path_5td_pipeline_v1",
    "generic_exit_price_conditioned_5td_contracts_v1", "generic_exit_price_conditioned_5td_models_v1",
)
FAMILIES = (
    "advisory_exit_remaining_value_5td_v1_20261008", "advisory_exit_held_path_context_5td_v1_20261008",
    "advisory_exit_price_conditioned_5td_v1_20261008",
)


def load_original_exit_modules(descriptor):
    if [member["module"] for member in descriptor["modules"]] != list(ORIGINAL_MODULES):
        raise ValueError("only explicit original Exit source modules may load")
    for member in descriptor["dependencies"]:
        if file_sha(member["path"]) != member["sha256"]:
            raise ValueError("frozen Exit inference dependency changed")
    result = {}
    for member in descriptor["modules"]:
        path = Path(member["path"])
        if (not path.is_absolute() or path.drive.upper() == "C:" or path.suffix != ".py"
                or path.stat().st_size > 65536 or file_sha(path) != member["sha256"]):
            raise ValueError("frozen Exit source path/hash differs")
        name = MODULE_PREFIX+member["module"]
        if name in sys.modules:
            if file_sha(sys.modules[name].__file__) != member["sha256"]:
                raise ValueError("original Exit source collides with another module")
            result[member["module"]] = sys.modules[name]
            continue
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
            if file_sha(path) != member["sha256"]:
                raise ValueError("original Exit source changed during load")
        except BaseException:
            sys.modules.pop(name, None)
            raise
        result[member["module"]] = module
    return result


def choose_frozen_exit_fold(*, schedules, first_s):
    """Metadata-only latest past-trained fold; no fit, averaging or result choice."""
    day = _day(first_s)
    eligible = [(date.fromisoformat(v["latest_training_e"]), int(k)) for k, v in schedules.items()
        if v["status"] == "FITTED" and date.fromisoformat(v["latest_training_e"]) < day
        and date.fromisoformat(v["first_evaluation_s"]) <= day]
    if not eligible:
        raise ValueError("no original past-trained Exit fold for these S dates")
    return str(max(eligible)[1])


def frozen_linear_exit_prediction(*, s_inputs, fitted, held_path=False):
    """Exact original frozen medians/flags/Ridge export, without label columns."""
    path_features = ("held_mark_return_bps", "held_peak_drawdown_fraction", "held_range_fraction") if held_path else ()
    features = (*FEATURES, *path_features, "remaining_session_fraction")
    allowed = {"episode_id", "s_date", *features}
    if (set(s_inputs.columns) != allowed or not s_inputs.columns.is_unique
            or s_inputs.duplicated(["episode_id", "s_date"]).any()):
        raise ValueError("frozen Exit linear inference needs only original S inputs")
    raw = s_inputs.loc[:, features].to_numpy(dtype=float)
    recipe = fitted["recipe"]
    names = ("median", "mean", "scale") if held_path else ("medians", "means", "scales")
    medians, means, scales = (np.asarray(recipe[name], dtype=float) for name in names)
    if (fitted["status"] != "FITTED" or any(v.shape != (len(features),) or not np.isfinite(v).all()
            for v in (medians, means, scales)) or (scales <= 0).any() or np.isinf(raw).any()):
        raise ValueError("frozen Exit numeric recipe differs")
    missing = np.isnan(raw)
    matrix = np.column_stack(((np.where(missing, medians, raw)-means)/scales,
        missing[:, :len(features)-1].astype(float)))
    coefficients = np.asarray(fitted["coefficients"], dtype=float)
    if (coefficients.shape != (matrix.shape[1],) or not np.isfinite(coefficients).all()
            or not np.isfinite(fitted["intercept"])):
        raise ValueError("frozen Exit coefficients differ")
    result = s_inputs.loc[:, ["episode_id", "s_date"]].copy()
    result["predicted_y_hold_bps"] = matrix@coefficients+fitted["intercept"]
    if not np.isfinite(result.predicted_y_hold_bps).all():
        raise ValueError("frozen Exit predictions are nonfinite")
    return result


def verify_original_exit_encoder(*, s_inputs, fitted, original_matrix, held_path=False):
    """Pure export parity against the frozen source; always pass its existing recipe."""
    predicted = frozen_linear_exit_prediction(s_inputs=s_inputs, fitted=fitted, held_path=held_path)
    matrix, recipe = original_matrix(s_inputs, recipe=fitted["recipe"])
    if canonical_sha(recipe) != canonical_sha(fitted["recipe"]):
        raise ValueError("original Exit encoder changed its frozen recipe")
    expected = np.asarray(matrix)@np.asarray(fitted["coefficients"])+fitted["intercept"]
    if not np.allclose(predicted.predicted_y_hold_bps, expected, rtol=0, atol=1e-10):
        raise ValueError("frozen Exit export does not reproduce original encoder")
    return dict(known_rows=len(s_inputs), physical_fit_count=0, original_encoder_parity=True)


def freeze_exit_transfer_plan(root, *, original_source):
    root = Path(root)
    plan = checked_plan(root)
    modules = load_original_exit_modules(original_source)
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    items = [item for item in inventory["inventory"]["items"] if item["family"] in FAMILIES]
    if len(items) != 3:
        raise ValueError("the three preregistered frozen Exit units are not complete")
    units = []
    for item in items:
        body, digest = _checked_member(item, "models.json")
        units.append(dict(model_id=item["model_id"], family=item["family"], models=body,
            weight_sha256=digest, manifest_ref=item["manifest_ref"], manifest_sha256=item["manifest_sha256"]))
    schedule = next(unit for unit in units if unit["family"] == FAMILIES[0])["models"]["folds"]
    first_s = date.fromisoformat(plan["decision_dates"][0])  # Conservative: actual first S is the next session.
    ordinal = choose_frozen_exit_fold(schedules=schedule, first_s=first_s)
    for unit in units:
        frozen = unit["models"]["folds" if unit["family"] == FAMILIES[0] else "models"][ordinal]
        if frozen["status"] != "FITTED":
            raise ValueError("the same original eligible fold is not fitted in all Exit units")
        unit["ordinal"], unit["frozen_fold_sha256"] = ordinal, canonical_sha(frozen)
        unit.pop("models")
    result = dict(plan_sha256=file_sha(root/"plan.json"), original_source=original_source, units=units,
        decision_dates=plan["decision_dates"], settlement_cutoff=plan["settlement_cutoff"],
        policy_sha256=modules[ORIGINAL_MODULES[0]].POLICY_SHA256,
        fold_selection="LATEST_ORIGINAL_PAST_TRAINED_FOLD_BY_METADATA_NO_RESULT_SELECTION",
        evidence_semantics="FROZEN_FOLD_EXTRAPOLATION_NOT_ORIGINAL_OOF_OR_FULL_REFIT",
        objective_contract="RISK_MANAGED_ADVISORY", decision_use="NAVIGATION_ONLY", physical_fit_count=0,
        Entry_window_other_model_results_already_seen=True)
    publish_json(root/"exit_query_spec_v1.json", result)
    return result, modules


def run_exit_transfer(root, *, original_source, progress=None):
    root = Path(root)
    plan = checked_plan(root)
    spec, modules = freeze_exit_transfer_plan(root, original_source=original_source)
    source = root/"settlement_coordinates"
    receipt = json.loads((source/"receipt.json").read_text(encoding="utf-8"))
    if (receipt["until"] != plan["settlement_cutoff"] or file_sha(source/"quotes.parquet") != receipt["quotes_sha256"]):
        raise ValueError("Exit shared readonly quote snapshot changed")
    quotes = pd.read_parquet(source/"quotes.parquet")
    index = pd.read_parquet(root/"shared_daily/index_daily.parquet")
    last_index = index.trade_date.max()
    with readonly_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT trade_date,ts_code,close FROM market.index_daily WHERE ts_code='000300.SH' AND trade_date>%s AND trade_date<=%s ORDER BY trade_date",
                (last_index.date(), date.fromisoformat(plan["settlement_cutoff"])))
            extra = pd.DataFrame(cursor.fetchall(), columns=["trade_date", "instrument", "close"])
    extra["trade_date"] = pd.to_datetime(extra.trade_date)
    index = pd.concat((index, extra), ignore_index=True)
    calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
    rosters = pd.read_parquet(root/"shared_daily/rosters.parquet")
    labels_module, audit_module = modules[ORIGINAL_MODULES[1]], modules[ORIGINAL_MODULES[2]]
    cutoff = date.fromisoformat(plan["settlement_cutoff"])
    results, daily_frames, all_geometries = [], [], []
    for package, candidates in rosters.groupby("package_id", sort=True):
        geometry, _ = labels_module.exit_geometry_v1(candidates=candidates.loc[:, ROSTER],
            decision_dates=[date.fromisoformat(d) for d in plan["decision_dates"]], calendar=calendar, development_cutoff=cutoff)
        geometry["package_id"] = package
        all_geometries.append(geometry)
    geometry = pd.concat(all_geometries, ignore_index=True)
    pseudo = geometry.loc[:, ["s_date", "u_date", "instrument"]].drop_duplicates().reset_index(drop=True).rename(columns={"s_date": KEY[0], "u_date": KEY[1]})
    for name in KEY[:2]:
        pseudo[name] = pseudo[name].map(_day).map(pd.Timestamp)
    pseudo["package_id"] = "SHARED_S_ONLY_ENCODER_NOT_SELECTION"
    pseudo["selection_effective_rank"] = pseudo.groupby(KEY[0]).cumcount()+1
    pseudo["candidate_group_size"] = pseudo.groupby(KEY[0]).instrument.transform("size")
    shared, _ = build_shared_daily_features(rosters=pseudo, calendar=calendar, daily=quotes, index_daily=index)
    shared = shared.loc[:, [KEY[0], "instrument", *FEATURES]].rename(columns={KEY[0]: "s_date"})
    shared["s_date"] = shared.s_date.dt.date
    publish_bytes(root/"exit_transfer_v1/s_inputs.parquet", _parquet(shared))
    quote_fields = modules[ORIGINAL_MODULES[0]].QUOTE_FIELDS
    prices = quotes.rename(columns={"adj_factor": "policy_price_per_raw_cny"}).loc[:, quote_fields].copy()
    for name in ("suspended", "tradability_unknown"):
        if not prices[name].map(lambda value:isinstance(value, (bool, np.bool_))).all():
            raise ValueError("Exit trading flags must be explicit booleans; UNKNOWN cannot become False")
        prices[name] = prices[name].astype(bool)
    for package, original in geometry.groupby("package_id", sort=True):
        original = original.drop(columns="package_id").reset_index(drop=True)
        # S features and sealed price curves are published before this role reads E labels.
        inputs = original.merge(shared, on=["s_date", "instrument"], how="left", validate="many_to_one", sort=False)
        if inputs.loc[:, ["episode_id", "s_date"]].to_dict("records") != original.loc[:, ["episode_id", "s_date"]].to_dict("records"):
            raise ValueError("Exit S feature join changed original decision keys")
        inputs["remaining_session_fraction"] = inputs.remaining_sessions/4
        base_inputs = inputs.loc[:, ["episode_id", "s_date", *FEATURES, "remaining_session_fraction"]]
        predictions, curves = {}, {}
        for unit in spec["units"]:
            item = dict(manifest_ref=unit["manifest_ref"], manifest_sha256=unit["manifest_sha256"])
            body, digest = _checked_member(item, "models.json")
            frozen = body["folds" if unit["family"] == FAMILIES[0] else "models"][unit["ordinal"]]
            if digest != unit["weight_sha256"] or canonical_sha(frozen) != unit["frozen_fold_sha256"]:
                raise ValueError("Exit frozen fold changed")
            if unit["family"] == FAMILIES[2]:
                curves[unit["model_id"]] = modules[ORIGINAL_MODULES[7]].seal_s_curves_v1(model=frozen, s_inputs=base_inputs)
                output = curves[unit["model_id"]]
            else:
                query = base_inputs
                if unit["family"] == FAMILIES[1]:
                    held, _ = modules[ORIGINAL_MODULES[4]].build_exit_held_path_features_v1(rows=original,
                        prices=quotes.loc[:, modules[ORIGINAL_MODULES[3]].PRICE_FIELDS], calendar=calendar, development_cutoff=cutoff)
                    query = query.merge(held, on=["episode_id", "s_date"], how="left", validate="one_to_one", sort=False)
                output = frozen_linear_exit_prediction(s_inputs=query, fitted=frozen, held_path=unit["family"] == FAMILIES[1])
                predictions[unit["model_id"]] = output
            publish_bytes(root/"exit_transfer_v1"/package/unit["model_id"]/"s_predictions.parquet", _parquet(output))
        rows, label_receipt = labels_module.build_exit_remaining_value_labels_v1(geometry=original,
            calendar=calendar, prices=prices, development_cutoff=cutoff)
        for unit in spec["units"]:
            model_id = unit["model_id"]
            if model_id in curves:
                predictions[model_id] = modules[ORIGINAL_MODULES[7]].query_sealed_curves_v1(curves=curves[model_id],
                    queries=rows.loc[:, modules[ORIGINAL_MODULES[6]].QUERY_FIELDS]).loc[:, ["episode_id", "s_date", "predicted_y_hold_bps"]]
                predictions[model_id]["s_date"] = predictions[model_id].s_date.map(_day)
            episodes, cohorts, decisions = audit_module.evaluate_exit_chains_v1(rows=rows,
                predictions=predictions[model_id], evaluation_episode_ids=sorted(set(original.episode_id)))
            cohorts["decision_date"] = [str(calendar[calendar.index(_day(t))-1]) for t in cohorts.entry_date]
            axis = cohorts.set_index("decision_date").reindex(plan["decision_dates"])
            axis["increment_bps"] = axis.candidate_bps-axis.baseline_bps
            axis["package_id"], axis["model_id"] = package, model_id
            daily_frames.append(axis.reset_index())
            known = axis.increment_bps.notna()
            result = dict(model_id=model_id, family=unit["family"], package_id=package,
                frozen_fold=unit["ordinal"], paired_original_days=int(known.sum()), original_days=len(axis),
                baseline_cohort_mean_bps=float(axis.loc[known, "baseline_bps"].mean()) if known.any() else None,
                candidate_cohort_mean_bps=float(axis.loc[known, "candidate_bps"].mean()) if known.any() else None,
                descriptive_increment_bps=float(axis.loc[known, "increment_bps"].mean()) if known.any() else None,
                known_interventions=int(episodes.loc[episodes.paired_known, "intervention"].sum()),
                label_receipt=label_receipt, evidence_semantics=spec["evidence_semantics"], physical_fit_count=0,
                activation_evidence=False)
            base = root/"exit_transfer_v1"/package/model_id
            for name, frame in (("episodes", episodes), ("daily", axis.reset_index()), ("decisions", decisions)):
                publish_bytes(base/(name+".parquet"), _parquet(frame))
            publish_json(base/"result.json", result)
            results.append(result)
            if progress:
                progress(dict(event="FROZEN_EXIT_TRANSFER_EVALUATED", **{k: v for k, v in result.items() if k != "label_receipt"}))
    output = dict(spec_sha256=file_sha(root/"exit_query_spec_v1.json"), comparisons=results,
        policy_sha256=spec["policy_sha256"], physical_fit_count=0, sealed_read=False, database_written=False,
        aggregate_inference_status="PENDING_FULL_ORIGINAL_AXIS_JOINT_REVIEW", activation_evidence=False)
    publish_bytes(root/"exit_transfer_v1/daily.parquet", _parquet(pd.concat(daily_frames, ignore_index=True)))
    publish_json(root/"exit_transfer_v1/results.json", output)
    return output
