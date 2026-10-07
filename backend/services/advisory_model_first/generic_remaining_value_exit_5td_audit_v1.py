"""One frozen past-only Exit audit; not a deployed sell model or QE producer."""
from datetime import datetime, timezone
import io
import inspect
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, _verify_reference, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import (
    CONTEXT, _day, _number, build_generic_daily_price_input_v1,
)
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import (
    AUDIT, AUDIT_SHA256, FEATURES, HYPOTHESIS, KEY, POLICY, POLICY_SHA256,
    QUOTE_FIELDS, ROSTER, ordered_calendar,
)
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_labels_v1 import (
    build_exit_remaining_value_labels_v1, exit_geometry_v1,
)
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1, EvidenceReferenceV1, build_trial_record,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

FEATURE_KEY = ("episode_id", "s_date")
FEATURE_COLUMNS = (*FEATURE_KEY, *FEATURES, "remaining_session_fraction")


def exit_fold_schedule_v1(*, labels, calendar):
    days = ordered_calendar(calendar)
    indexes = {day: i for i, day in enumerate(days)}
    mature = labels.loc[labels.geometry_status.eq("MATURE")]
    cohorts = sorted({_day(value) for value in mature.entry_date})
    if len(cohorts) < AUDIT["blocks"]:
        raise ValueError("Exit audit needs five nonempty original mature cohort blocks")
    blocks = [tuple(block) for block in np.array_split(np.asarray(cohorts, dtype=object), AUDIT["blocks"])]
    folds = []
    for ordinal in range(1, AUDIT["blocks"]):
        evaluation = mature.entry_date.map(_day).isin(blocks[ordinal])
        first_s = min(mature.loc[evaluation, "s_date"].map(_day))
        latest_e = indexes[first_s]-AUDIT["embargo_complete_sessions"]-1
        training = mature.entry_date.map(_day).lt(blocks[ordinal][0]) & mature.endpoint_date.map(_day).map(indexes).le(latest_e)
        folds.append(dict(ordinal=ordinal, train_episode_ids=sorted(set(mature.loc[training, "episode_id"])),
            evaluation_episode_ids=sorted(set(mature.loc[evaluation, "episode_id"])),
            evaluation_cohorts=[day.isoformat() for day in blocks[ordinal]],
            first_evaluation_s=first_s.isoformat(), latest_training_e=days[latest_e].isoformat()))
    return folds


def _join_features(labels, features):
    if (set(features.columns) != set(FEATURE_COLUMNS) or not features.columns.is_unique
            or features.duplicated(list(FEATURE_KEY)).any() or len(features) != len(labels)):
        raise ValueError("Exit S features must exactly match original episode/S keys")
    result = labels.merge(features, on=list(FEATURE_KEY), how="outer", validate="one_to_one", indicator=True)
    if not result._merge.eq("both").all():
        raise ValueError("Exit features contain missing/foreign original keys")
    result = result.drop(columns="_merge")
    remaining = pd.to_numeric(result.remaining_session_fraction, errors="raise")
    if not remaining.eq(pd.to_numeric(result.remaining_sessions, errors="raise")/4).all():
        raise ValueError("Exit remaining feature differs from its known original fixed endpoint")
    return result


def _design_matrix(frame, recipe=None):
    raw = frame.loc[:, [*FEATURES, "remaining_session_fraction"]].to_numpy(dtype=float)
    if np.isinf(raw).any():
        raise ValueError("Exit S inputs cannot be infinite")
    missing = np.isnan(raw)
    if recipe is None:
        medians = np.asarray([np.median(col[np.isfinite(col)]) if np.isfinite(col).any() else 0. for col in raw.T])
        filled = np.where(missing, medians, raw)
        means, scales = filled.mean(axis=0), filled.std(axis=0)
        scales[scales == 0] = 1.
        recipe = dict(medians=medians.tolist(), means=means.tolist(), scales=scales.tolist())
    medians, means, scales = (np.asarray(recipe[name], dtype=float) for name in ("medians", "means", "scales"))
    filled = np.where(missing, medians, raw)
    return np.column_stack(((filled-means)/scales, missing[:, :len(FEATURES)].astype(float))), recipe


def fit_exit_fold_v1(*, rows, fold, before_fit):
    train = rows.loc[rows.episode_id.isin(fold["train_episode_ids"]) & rows.y_hold_bps.notna() & rows.held.eq(True)]
    evaluation = rows.loc[rows.episode_id.isin(fold["evaluation_episode_ids"])]
    if train.empty:
        return dict(status="UNKNOWN_NO_MATURE_TRAIN", ordinal=fold["ordinal"], physical_fits=0), pd.DataFrame(columns=[*FEATURE_KEY, "predicted_y_hold_bps"])
    x, recipe = _design_matrix(train)
    future_x, _ = _design_matrix(evaluation, recipe)
    y = train.y_hold_bps.to_numpy(dtype=float)
    if not np.isfinite(y).all():
        raise ValueError("Exit matured training labels must be finite")
    before_fit(fold["ordinal"])
    model = Ridge(alpha=AUDIT["alpha"]).fit(x, y)
    predictions = evaluation.loc[:, FEATURE_KEY].copy()
    predictions["predicted_y_hold_bps"] = model.predict(future_x)
    artifact = dict(status="FITTED", ordinal=fold["ordinal"], physical_fits=1, recipe=recipe,
        coefficients=model.coef_.tolist(), intercept=float(model.intercept_), train_decisions=len(train),
        train_episode_count=int(train.episode_id.nunique()), first_evaluation_s=fold["first_evaluation_s"],
        latest_training_e=fold["latest_training_e"], evaluation_episode_ids=fold["evaluation_episode_ids"])
    return artifact, predictions


def evaluate_exit_chains_v1(*, rows, predictions, evaluation_episode_ids):
    if (set(predictions.columns) != {*FEATURE_KEY, "predicted_y_hold_bps"}
            or predictions.duplicated(list(FEATURE_KEY)).any()):
        raise ValueError("Exit prediction schema/unique S keys differs")
    selected = rows.loc[rows.episode_id.isin(evaluation_episode_ids)].copy()
    if set(evaluation_episode_ids)-set(selected.episode_id):
        raise ValueError("Exit evaluation references a foreign episode")
    if set(map(tuple, predictions.loc[:, FEATURE_KEY].to_numpy()))-set(map(tuple, selected.loc[:, FEATURE_KEY].to_numpy())):
        raise ValueError("Exit predictions reference a foreign S key")
    decisions = selected.merge(predictions, on=list(FEATURE_KEY), how="left", validate="one_to_one")
    decisions["predicted_advantage_bps"] = decisions.sell_scenario_bps-decisions.predicted_y_hold_bps
    episodes = []
    for episode, chain in decisions.groupby("episode_id", sort=False):
        chain = chain.sort_values("s_date", kind="stable")
        first = chain.iloc[0]
        baseline = first.baseline_return_bps
        known = np.isfinite(baseline) if baseline is not None else False
        if chain.baseline_return_bps.dropna().nunique() > 1:
            raise ValueError("Exit baseline endpoint value changes within one original episode")
        available = chain.sell_executable.eq(True) & chain.sell_return_bps.notna()
        actionable = chain.loc[available & chain.predicted_advantage_bps.gt(0)]
        action = None if actionable.empty else actionable.iloc[0]
        candidate = baseline if action is None else action.sell_return_bps
        oracle = max([baseline, *chain.loc[available, "sell_return_bps"].tolist()]) if known else None
        episodes.append(dict(episode_id=episode, entry_date=_day(first.entry_date),
            selection_effective_rank=int(first.selection_effective_rank), held=first.held,
            baseline_bps=baseline, candidate_bps=candidate if known else None, oracle_bps=oracle,
            paired_known=bool(known and candidate is not None and np.isfinite(candidate)),
            intervention=action is not None, intervention_u=None if action is None else _day(action.u_date),
            remaining_sessions=None if action is None else int(action.remaining_sessions)))
    episodes = pd.DataFrame(episodes)
    cohorts = []
    for day, group in episodes.groupby("entry_date", sort=True):
        complete = bool(group.paired_known.all())
        totals = {arm+"_bps": float(group[arm+"_bps"].sum()/5) if complete else None for arm in ("baseline", "candidate", "oracle")}
        cohorts.append(dict(entry_date=day, original_episode_count=len(group), known_slots=int(group.paired_known.sum()),
            empty_slots=5-len(group), unknown_slots=int((~group.paired_known).sum()),
            original_not_held_slots=int(group.held.eq(False).sum()), complete=complete,
            interventions=int(group.intervention.sum()), **totals))
    cohorts = pd.DataFrame(cohorts)
    return episodes, cohorts, decisions


def summarize_exit_v1(*, episodes, cohorts):
    complete = cohorts.loc[cohorts.complete].copy()
    deltas = (complete.candidate_bps-complete.baseline_bps).to_numpy(dtype=float)
    oracle = (complete.oracle_bps-complete.baseline_bps).to_numpy(dtype=float)
    interval = None
    mde = None
    if len(deltas) >= 2*AUDIT["bootstrap_block"]:
        # Blocks are formed on the original timeline. UNKNOWN days are not compressed away.
        values = np.where(cohorts.complete, cohorts.candidate_bps-cohorts.baseline_bps, np.nan).astype(float)
        rng = np.random.default_rng(AUDIT["bootstrap_seed"])
        starts = rng.integers(0, len(values), size=(AUDIT["bootstrap_replicates"], int(np.ceil(len(values)/AUDIT["bootstrap_block"]))))
        indexes = (starts[:, :, None]+np.arange(AUDIT["bootstrap_block"])) % len(values)
        samples = values[indexes.reshape(len(starts), -1)[:, :len(values)]]
        valid = np.isfinite(samples).any(axis=1)
        means = np.nanmean(samples[valid], axis=1)
        interval = [float(value) for value in np.quantile(means, [.025, .975])]
        mde = float((1.96+.84)*np.std(means, ddof=1))
    interventions = episodes.loc[episodes.intervention & episodes.paired_known & episodes.entry_date.isin(complete.entry_date)]
    counts = dict(episodes=len(interventions), cohorts=int(interventions.entry_date.nunique()),
        cohort_fraction=float(interventions.entry_date.nunique()/len(complete)) if len(complete) else 0.)
    support = counts["episodes"] >= AUDIT["min_intervention_episodes"] and counts["cohorts"] >= AUDIT["min_intervention_cohorts"] and counts["cohort_fraction"] >= AUDIT["min_intervention_coverage"]
    lift = interventions.candidate_bps-interventions.baseline_bps
    baseline_profit, candidate_profit = (interventions[name].clip(lower=0) for name in ("baseline_bps", "candidate_bps"))
    baseline_loss, candidate_loss = ((-interventions[name]).clip(lower=0) for name in ("baseline_bps", "candidate_bps"))
    return dict(hypothesis=HYPOTHESIS, objective_contract="RISK_MANAGED_ADVISORY", decision_use="NAVIGATION_ONLY",
        result_class="EXPLORATORY", complete_cohorts=len(complete), original_cohorts=len(cohorts),
        unknown_cohorts=int((~cohorts.complete).sum()), intervention_support=counts,
        intervention_minima_met=bool(support), regime_support="UNKNOWN_REGIME_SUPPORT",
        mean_candidate_increment_bps=float(deltas.mean()) if len(deltas) else None,
        mean_oracle_increment_bps=float(oracle.mean()) if len(oracle) else None,
        candidate_increment_ci95_bps=interval, mde80_bps=mde,
        known_intervention_increment_bps_sum=float(lift.sum()),
        attribution_population="COMPLETE_PAIRED_COHORT_EPISODES_SUM_NOT_MEAN_NAV",
        avoided_loss_bps_sum=float((baseline_loss-candidate_loss).clip(lower=0).sum()),
        added_loss_bps_sum=float((candidate_loss-baseline_loss).clip(lower=0).sum()),
        improved_profit_bps_sum=float((candidate_profit-baseline_profit).clip(lower=0).sum()),
        missed_profit_bps_sum=float((baseline_profit-candidate_profit).clip(lower=0).sum()),
        original_unknown_cash_increment=None, remaining_intervention_counts={str(k): int(v) for k, v in interventions.remaining_sessions.value_counts().items()},
        accounting="OVERLAPPING_ORIGINAL_FIVE_SLOT_COHORT_NOT_INVESTABLE_NAV",
        oracle="CLAIRVOYANT_LEGAL_U_OPENS_NOT_HIGH_OR_MINUTE_EXTREMA", economic_confirmation=False, deployable=False)


def implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_remaining_value_exit_5td_{part}_v1.py" for part in ("contracts", "labels", "audit")]
    names += ["generic_daily_price_input_v1.py"]
    identities = {name: file_sha256(directory/name) for name in names}
    # A short-process explicit local dependency is not serving activation; record its actual code.
    identities["feature_builder_consumed"] = file_sha256(Path(inspect.getsourcefile(build_generic_daily_price_input_v1)))
    return sha(identities)


def _load(plan, output_root):
    required = {"schema_version", "parent_plan", "inputs", "dataset_identity", "parent_lineage", "decision_dates", "development_start", "development_cutoff", "implementation_sha256", "source_evidence"}
    if (not isinstance(plan, dict) or set(plan) != required or plan["schema_version"] != "exit5_remaining_value_plan_v1"
            or plan["implementation_sha256"] != implementation_sha256()
            or set(plan["inputs"]) != {"calendar", "roster", "raw_daily", "volume_daily", "index_daily"}
            or not plan["dataset_identity"] or not plan["parent_lineage"]):
        raise ValueError("Exit plan source/schema identity differs")
    start, cutoff = _day(plan["development_start"]), _day(plan["development_cutoff"])
    dates = ordered_calendar(plan["decision_dates"])
    if dates[0] < start or dates[-1] > cutoff or start > cutoff:
        raise ValueError("Exit plan cannot consume test/sealed decisions")
    original_plan = json.loads(_verify_reference(EvidenceReferenceV1.model_validate(plan["parent_plan"])).read_text(encoding="utf-8"))
    original = original_plan["configuration"]
    if (start < _day(original["train_start"]) or cutoff > _day(original["validation_end"])
            or cutoff >= _day(original["test_start"]) or plan["dataset_identity"] != original_plan["dataset_identity"]
            or any(day.isoformat() not in original_plan["decision_dates"] for day in dates)):
        raise ValueError("Exit development scope cannot relabel the original test/sealed window")
    if not isinstance(plan["source_evidence"], str) or not plan["source_evidence"].strip():
        raise ValueError("Exit needs an honest source evidence classification")
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:" or declared != root:
        raise ValueError("Exit artifacts require an explicit non-C real path")
    identity = sha(dict(plan=plan, policy=POLICY_SHA256, audit=AUDIT_SHA256))
    root = root/("advexit5_"+identity[:24])
    paths = {name: _verify_reference(EvidenceReferenceV1.model_validate(value)) for name, value in plan["inputs"].items()}
    return root, identity, paths


def _stage(root, identity, name):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        manifest = read_stage(root/stage, stage=stage, plan_sha256=identity, parent_sha256=parent)
        parent = manifest["stage_sha256"]
        if stage == name:
            return manifest
    raise ValueError("unknown Exit stage")


def _record(plan, root, stage, study_type, fits=0):
    record = build_trial_record(experiment_id=root.name+":"+study_type, attempt_id="exact_attempt_v1",
        research_stage=stage.upper(), study_type=study_type, hypothesis_family_id=HYPOTHESIS,
        parent_lineage=tuple(plan["parent_lineage"]), unique_variable="REMAINING_VALUE_NOT_HOLDING_DURATION",
        objective_contract="RISK_MANAGED_ADVISORY", dataset_identity=plan["dataset_identity"],
        schema_identity="exit5_original_episode_s_v1", policy_identity=POLICY_SHA256,
        planned_trial_count=0 if study_type == "ORACLE_DIAGNOSTIC" else 4,
        generated_trial_count=fits, evaluated_trial_count=fits if stage == "evaluated" else 0,
        selected_trial_count=0, consumed_windows=(ConsumedWindowV1(window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=plan["dataset_identity"], start_date=_day(plan["development_start"]), end_date=_day(plan["development_cutoff"])),),
        result_class="EXPLORATORY", decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="exit5_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def preregister_exit5_v1(*, plan, output_root):
    root, identity, _ = _load(plan, output_root)
    publish_stage(study_root=root, stage="preregistered", plan_sha256=identity, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan), "policy.json": _json_bytes(POLICY), "audit.json": _json_bytes(AUDIT)})
    for study_type in ("ORACLE_DIAGNOSTIC", "LEARNABILITY_AUDIT"):
        _record(plan, root, "preregistered", study_type)
    return root/"preregistered"/"plan.json"


def _parquet(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def _read_development(path, columns, cutoff, start=None, *, date_column="trade_date"):
    # Projection and date filter precede financial-value decoding, including poisoned test rows.
    filters = [(date_column, "<=", pd.Timestamp(cutoff))]
    if start is not None:
        filters.append((date_column, ">=", pd.Timestamp(start)))
    return pd.read_parquet(path, columns=list(columns), filters=filters)


def _feature_rows(geometry, calendar, raw, volumes, benchmark, cutoff, source_evidence):
    indexes = {day: i for i, day in enumerate(calendar)}
    bars = {( _day(row["trade_date"]), row["instrument"]): row for row in raw.to_dict("records")}
    volume = {(_day(row["trade_date"]), row["instrument"]): _number(row["volume_hand"], nonnegative=True) for row in volumes.to_dict("records")}
    records = []
    for s, group in geometry.groupby("s_date", sort=True):
        s = _day(s)
        i = indexes[s]
        values = {}
        if s <= cutoff and i >= 19 and i+1 < len(calendar):
            sessions = calendar[i-19:i+1]
            symbols = sorted(set(group.instrument))
            query = pd.DataFrame([{KEY[0]: s, KEY[1]: calendar[i+1], "instrument": symbol,
                "selection_effective_rank": rank, "candidate_group_size": len(symbols)} for rank, symbol in enumerate(symbols, 1)], columns=ROSTER)
            panel = []
            for symbol in symbols:
                anchor = bars.get((s, symbol))
                anchor_factor = None if anchor is None else _number(anchor["adj_factor"], positive=True)
                for day in sessions:
                    bar = bars.get((day, symbol))
                    if bar is None:
                        continue
                    factor = _number(bar["adj_factor"], positive=True)
                    scale = None if factor is None or anchor_factor is None else factor/anchor_factor
                    amounts = {field: None if scale is None or _number(bar["raw_"+field+"_cny"], positive=True) is None else float(bar["raw_"+field+"_cny"])*scale for field in ("open", "high", "low", "close")}
                    vol = volume.get((day, symbol))
                    panel.append(dict(trade_date=day, instrument=symbol, **amounts, volume=None if vol is None else vol*100))
            context = dict.fromkeys(CONTEXT)
            context.update(source_evidence=source_evidence, price_basis="D_ADJUSTED_CNY", volume_basis="RAW_SHARES",
                source_visible_through=s.isoformat(), benchmark_visible_through=s.isoformat())
            frame, _ = build_generic_daily_price_input_v1(candidates=query, calendar=[*sessions, calendar[i+1]],
                panel=pd.DataFrame(panel, columns=["trade_date", "instrument", "open", "high", "low", "close", "volume"]),
                benchmark_daily=benchmark.loc[benchmark.trade_date.map(_day).isin(sessions)],
                market_state=dict(trade_date=s, market_up_ratio=None, market_definition_id=None, visible_through=None), source_context=context)
            values = frame.set_index("instrument").loc[:, FEATURES].to_dict("index")
        for row in group.to_dict("records"):
            records.append({"episode_id": row["episode_id"], "s_date": row["s_date"],
                **values.get(row["instrument"], dict.fromkeys(FEATURES, np.nan)),
                "remaining_session_fraction": row["remaining_sessions"]/4})
    return pd.DataFrame(records, columns=FEATURE_COLUMNS)


def prepare_exit5_v1(*, plan, output_root):
    root, identity, paths = _load(plan, output_root)
    parent = _stage(root, identity, "preregistered")
    if (root/"prepared").exists():
        _stage(root, identity, "prepared")
        return root/"prepared"
    cutoff = _day(plan["development_cutoff"])
    calendar = ordered_calendar(json.loads(paths["calendar"].read_text(encoding="utf-8")))
    import pyarrow.parquet as parquet
    legacy = "is_candidate_decision" in parquet.read_schema(paths["roster"]).names
    roster = _read_development(paths["roster"], (*KEY, "selection_effective_rank", "is_candidate_decision") if legacy else ROSTER,
        cutoff, _day(plan["development_start"]), date_column=KEY[0])
    if legacy:
        if not roster.is_candidate_decision.map(lambda value: type(value) is bool).all():
            raise ValueError("Exit legacy candidate flags are not explicit")
        roster = roster.loc[roster.is_candidate_decision.eq(True) & roster.selection_effective_rank.le(20)].copy()
        roster["candidate_group_size"] = roster.groupby(KEY[0]).instrument.transform("size")
        roster = roster.sort_values([KEY[0], "selection_effective_rank"], kind="stable").loc[:, ROSTER]
    geometry, geometry_receipt = exit_geometry_v1(candidates=roster, decision_dates=plan["decision_dates"], calendar=calendar, development_cutoff=cutoff)
    raw = _read_development(paths["raw_daily"], (*QUOTE_FIELDS, "raw_high_cny", "raw_low_cny", "adj_factor"), cutoff)
    volumes = _read_development(paths["volume_daily"], ("trade_date", "instrument", "volume_hand"), cutoff)
    benchmark = _read_development(paths["index_daily"], ("trade_date", "instrument", "close"), cutoff)
    if any(frame.duplicated(["trade_date", "instrument"]).any() for frame in (raw, volumes, benchmark)):
        raise ValueError("Exit frozen source contains duplicate daily keys")
    labels, label_receipt = build_exit_remaining_value_labels_v1(geometry=geometry, calendar=calendar,
        prices=raw.loc[:, QUOTE_FIELDS], development_cutoff=cutoff)
    features = _feature_rows(geometry, calendar, raw, volumes, benchmark, cutoff, plan["source_evidence"])
    rows = _join_features(labels, features)
    folds = exit_fold_schedule_v1(labels=labels, calendar=calendar)
    publish_stage(study_root=root, stage="prepared", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"rows.parquet": _parquet(rows), "folds.json": _json_bytes(folds),
            "calendar.json": _json_bytes([day.isoformat() for day in calendar]),
            "receipt.json": _json_bytes(dict(geometry=geometry_receipt, labels=label_receipt,
                feature_role="HELD_STOCK_S_QUERY_NOT_SELECTION", source_evidence=plan["source_evidence"],
                feature_known_counts={field: int(features[field].notna().sum()) for field in FEATURES},
                test_values_decoded=False, market_breadth="UNKNOWN_NOT_BACKFILLED", physical_fits=0))})
    _record(plan, root, "prepared", "ORACLE_DIAGNOSTIC")
    _record(plan, root, "prepared", "LEARNABILITY_AUDIT")
    return root/"prepared"


def train_exit5_v1(*, plan, output_root, qe_idle_check):
    root, identity, _ = _load(plan, output_root)
    parent = _stage(root, identity, "prepared")
    if (root/"trained").exists():
        _stage(root, identity, "trained")
        return root/"trained"
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    folds = json.loads((root/"prepared"/"folds.json").read_text(encoding="utf-8"))
    artifacts, outputs, fits = {}, [], 0
    for fold in folds:
        # A completed fold is reused on resume; an unresolved STARTED marker never causes an automatic refit.
        folder = root/"folds"/str(fold["ordinal"])
        folder.mkdir(parents=True, exist_ok=True)
        marker = folder/"fit_attempt.json"
        model_path, prediction_path = folder/"model.json", folder/"predictions.parquet"
        if marker.exists():
            marker_data = json.loads(marker.read_text(encoding="utf-8"))
            if (marker_data.get("status") != "COMPLETE" or marker_data.get("plan_sha256") != identity
                    or file_sha256(model_path) != marker_data.get("model_sha256")
                    or file_sha256(prediction_path) != marker_data.get("predictions_sha256")):
                raise ValueError("Exit unresolved physical fit attempt needs explicit recovery, not a repeat")
            model = json.loads(model_path.read_text(encoding="utf-8"))
            predictions = pd.read_parquet(prediction_path)
        else:
            checks = []
            def before_fit(ordinal):
                receipt = qe_idle_check()
                now = datetime.now(timezone.utc)
                checked = datetime.fromisoformat(receipt["checked_at_utc"])
                if not 0 <= (now-checked).total_seconds() <= 60 or set(receipt["running_counts"]) != {"single", "custom_evo", "multi_alpha"} or any(type(v) is not int or v != 0 for v in receipt["running_counts"].values()):
                    raise ValueError("Exit next fit waits for a fresh three-path QE idle read")
                checks.append(receipt)
                with marker.open("xb") as stream:
                    stream.write(_json_bytes(dict(status="STARTED", plan_sha256=identity, ordinal=ordinal, checked_at_utc=now.isoformat(), qe_before=receipt)))
                    stream.flush()
                    os.fsync(stream.fileno())
            model, predictions = fit_exit_fold_v1(rows=rows, fold=fold, before_fit=before_fit)
            after = qe_idle_check() if model["physical_fits"] else None
            for path, content in ((model_path, _json_bytes(model)), (prediction_path, _parquet(predictions))):
                with path.open("xb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
            complete = dict(status="COMPLETE", plan_sha256=identity, physical_fits=model["physical_fits"], qe_before=checks, qe_after=after,
                model_sha256=file_sha256(model_path), predictions_sha256=file_sha256(prediction_path))
            temporary = folder/"fit_complete.json"
            with temporary.open("xb") as stream:
                stream.write(_json_bytes(complete))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, marker)
        fits += model["physical_fits"]
        if fits > AUDIT["max_physical_fits"]:
            raise ValueError("Exit physical fit budget exceeded")
        artifacts[str(fold["ordinal"])] = model
        outputs.append(predictions)
    predictions = pd.concat(outputs, ignore_index=True)
    publish_stage(study_root=root, stage="trained", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"models.json": _json_bytes(dict(folds=artifacts, physical_fits=fits)), "predictions.parquet": _parquet(predictions)})
    _record(plan, root, "trained", "LEARNABILITY_AUDIT", fits)
    return root/"trained"


def evaluate_exit5_v1(*, plan, output_root):
    root, identity, _ = _load(plan, output_root)
    parent = _stage(root, identity, "trained")
    if (root/"evaluated").exists():
        _stage(root, identity, "evaluated")
        return root/"evaluated"
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    predictions = pd.read_parquet(root/"trained"/"predictions.parquet")
    folds = json.loads((root/"prepared"/"folds.json").read_text(encoding="utf-8"))
    evaluated = [episode for fold in folds for episode in fold["evaluation_episode_ids"]]
    episodes, cohorts, decisions = evaluate_exit_chains_v1(rows=rows, predictions=predictions, evaluation_episode_ids=evaluated)
    result = summarize_exit_v1(episodes=episodes, cohorts=cohorts)
    models = json.loads((root/"trained"/"models.json").read_text(encoding="utf-8"))
    result["physical_fits"] = models["physical_fits"]
    result["fold_statuses"] = {key: value.get("status", "UNKNOWN") for key, value in models["folds"].items()}
    result["predicted_decision_count"] = int(predictions.predicted_y_hold_bps.notna().sum())
    result["unsettled_original_episodes"] = int(rows.loc[rows.geometry_status.ne("MATURE"), "episode_id"].nunique())
    result["warmup_original_episodes"] = int(rows.loc[rows.geometry_status.eq("MATURE") & ~rows.episode_id.isin(evaluated), "episode_id"].nunique())
    publish_stage(study_root=root, stage="evaluated", plan_sha256=identity, parent_sha256=parent["stage_sha256"],
        artifacts={"episodes.parquet": _parquet(episodes), "cohorts.parquet": _parquet(cohorts), "decisions.parquet": _parquet(decisions), "result.json": _json_bytes(result)})
    _record(plan, root, "evaluated", "ORACLE_DIAGNOSTIC")
    _record(plan, root, "evaluated", "LEARNABILITY_AUDIT", result["physical_fits"])
    return root/"evaluated"
