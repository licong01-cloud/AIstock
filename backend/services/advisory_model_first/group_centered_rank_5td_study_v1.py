"""Offline fixed-slot ranking study; never changes Selection or live recommendations."""
import json
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd
import psutil
from sklearn.linear_model import Ridge
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import paired_statistics
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import STATISTICS
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, file_sha256, publish_stage, read_stage
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ARMS = ("baseline", "absolute", "centered")
IDENTITY = ("package_id", "manifest_sha256", "run_id", *KEY)
INPUTS = (*IDENTITY, "selection_effective_rank", "candidate_group_size", *FEATURES)
FINANCE = ("valuation_status", "hypothetical_liquidation_net_bps", "valuation_gross_terminal_ratio",
           "observed_gap_bps", "exit_execution_status", "valuation_policy_sha256", "label_information_end")
CONTRACT = dict(schema="group_centered_rank_5td_v1", top_k=10, features=list(FEATURES),
    target="ORIGINAL_FIXED_FIVE_SESSION_NET_NOMINAL", missing="PIN_ORIGINAL_SLOT_NO_RANK_ADVICE",
    structural_missing="TRAIN_ALL_UNKNOWN_INACTIVE_FOR_BOTH_ARMS_NOT_ZERO_OBSERVATION",
    alpha=10., solver="lsqr", tol=1e-6, max_iter=2000, group="UNIQUE_STOCK_D_T_UNION_DATE",
    objective_contract="ALPHA_RANKING", decision_use="NAVIGATION_ONLY", fit_count=2,
    valuation_policy_sha256=VALUATION_POLICY_SHA256, measurement_policy=POLICY, statistics=STATISTICS)
POLICY_HASH = sha(CONTRACT)


def _numeric(frame):
    x = frame.loc[:, FEATURES].apply(pd.to_numeric, errors="raise").to_numpy(dtype=float)
    if np.isinf(x).any():
        raise ValueError("known D feature is nonfinite")
    return x


def validate_roster(rows, days, calendar, sources, start, end):
    """Validate dates/identities only; never inspect future valuation state for selection."""
    if rows.empty or len(rows) > 100000 or rows.duplicated(["package_id", *KEY]).any():
        raise ValueError("empty, duplicate or oversized original roster")
    sessions = pd.DatetimeIndex(calendar)
    if list(sessions) != sorted(set(sessions)) or sessions.hasnans:
        raise ValueError("calendar must be ordered and unique")
    expected = sessions[(sessions >= pd.Timestamp(start)) & (sessions <= pd.Timestamp(end))]
    source_map = {s["package_id"]: s for s in sources}
    if len(source_map) != len(sources) or set(rows.package_id) != set(source_map) or set(days.package_id) != set(source_map):
        raise ValueError("original package identity differs")
    if days.duplicated(["package_id", "decision_date"]).any():
        raise ValueError("duplicate original day")
    if not rows[KEY[0]].isin(expected).all():
        raise ValueError("foreign date in roster")
    if rows.groupby(list(KEY))[list(FEATURES)].nunique(dropna=False).gt(1).any().any():
        raise ValueError("same stock-day has contradictory D features")
    positions = {d: i for i, d in enumerate(sessions)}
    for package, source in source_map.items():
        rr = rows.loc[rows.package_id.eq(package)]
        dd = days.loc[days.package_id.eq(package)]
        if dd.decision_date.tolist() != list(expected):
            raise ValueError("missing original package/date")
        if not rr.manifest_sha256.eq(source["manifest_sha256"]).all() or not rr.run_id.eq(source["run_id"]).all():
            raise ValueError("manifest/run mismatch")
        for day in dd.itertuples(index=False):
            group = rr.loc[rr[KEY[0]].eq(day.decision_date)]
            pos = positions[day.decision_date]
            t = sessions[pos+1] if pos+1 < len(sessions) else pd.NaT
            h = sessions[pos+5] if pos+5 < len(sessions) else pd.NaT
            if (day.target_date != t or not (day.horizon_end == h or pd.isna(day.horizon_end) and pd.isna(h))
                    or len(group) != day.original_candidates or len(group) > 50
                    or group.selection_effective_rank.tolist() != list(range(1, len(group)+1))
                    or not group.candidate_group_size.eq(len(group)).all() or not group[KEY[1]].eq(t).all()):
                raise ValueError("original ranks, counts or D/T/H differ")
    return rows


def training_rows(rows, *, train_start, train_end):
    # Select by clock before decoding feature or target values: eval poison cannot affect fit.
    mask = rows[KEY[0]].between(train_start, train_end) & rows.label_information_end.le(pd.Timestamp(train_end))
    part = rows.loc[mask].copy()
    compare = [*FEATURES, *FINANCE]
    if part.groupby(list(KEY))[compare].nunique(dropna=False).gt(1).any().any():
        raise ValueError("duplicate stock-day has contradictory inputs/finance")
    part = part.drop_duplicates(list(KEY)).reset_index(drop=True)
    _validate_finance(part)
    known = part.valuation_status.eq("AVAILABLE") & np.isfinite(_numeric(part)).any(axis=1)
    result = part.loc[known].reset_index(drop=True)
    if not 2 <= len(result) <= 30000 or result[KEY[0]].nunique() < 2:
        raise ValueError("insufficient or oversized measurable training population")
    return result


def _validate_finance(rows):
    if not rows.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all():
        raise ValueError("valuation identity mismatch")
    statuses = rows.valuation_status.astype(str)
    if not statuses.map(lambda s: s in {"AVAILABLE", "IMMATURE", "CASH_ENTRY_NOT_EXECUTABLE"} or s.startswith("UNKNOWN")).all():
        raise ValueError("undeclared valuation status")
    available = rows.loc[statuses.eq("AVAILABLE")]
    raw = available.loc[:, ["hypothetical_liquidation_net_bps", "valuation_gross_terminal_ratio", "observed_gap_bps"]].apply(pd.to_numeric, errors="raise").to_numpy(float)
    if not np.isfinite(raw).all() or (raw[:, 1] <= 0).any() or (raw[:, 2] <= -10000).any():
        raise ValueError("known measurement is invalid")
    expected = 10000*(raw[:, 1]*(1-POLICY["sell_bps"]/10000)/((1+raw[:, 2]/10000)*(1+POLICY["buy_bps"]/10000))-1)
    if not np.allclose(raw[:, 0], expected, rtol=0, atol=1e-6):
        raise ValueError("cost/coordinate mismatch")


def _center(x, groups):
    return x - pd.DataFrame(x).groupby(np.asarray(groups), sort=False).transform("mean").to_numpy()


def train_models(rows, *, train_start, train_end, before_fit=lambda arm: None, after_fit=lambda arm: None):
    train = training_rows(rows, train_start=train_start, train_end=train_end)
    raw = _numeric(train)
    active = np.flatnonzero(~np.isnan(raw).all(axis=0))
    raw = raw[:, active]
    median = np.nanmedian(raw, axis=0)
    filled = np.where(np.isnan(raw), median, raw)
    x = np.column_stack((filled, np.isnan(raw).astype(float)))
    center, scale = x.mean(axis=0), x.std(axis=0)
    constant = scale == 0
    scale[constant] = 1.
    x = (x-center)/scale
    y = train.hypothetical_liquidation_net_bps.to_numpy(float)
    groups = train[KEY[0]].to_numpy()
    models = {}
    for arm in ARMS[1:]:
        xx, yy = (x, y) if arm == "absolute" else (_center(x, groups), _center(y[:, None], groups).ravel())
        before_fit(arm)
        model = Ridge(alpha=CONTRACT["alpha"], solver="lsqr", tol=CONTRACT["tol"],
                      max_iter=CONTRACT["max_iter"], fit_intercept=True).fit(xx, yy)
        if (model.n_iter_ is not None and np.max(model.n_iter_) >= CONTRACT["max_iter"]
                or not np.isfinite(model.coef_).all() or not np.isfinite(model.intercept_)):
            raise ValueError("Ridge did not converge to a finite model")
        models[arm] = dict(coef=model.coef_.tolist(), intercept=float(model.intercept_),
                           iterations=None if model.n_iter_ is None else model.n_iter_.tolist())
        after_fit(arm)
    return dict(schema=CONTRACT["schema"], policy_sha256=POLICY_HASH, models=models,
        active_columns=active.tolist(), inactive_source_fields=[name for i,name in enumerate(FEATURES) if i not in active],
        median=median.tolist(), center=center.tolist(), scale=scale.tolist(), constant_columns=np.flatnonzero(constant).tolist(),
        train_start=str(train_start), train_end=str(train_end), train_clusters=len(train), train_days=int(train[KEY[0]].nunique()),
        training_key_sha256=sha([[pd.Timestamp(d).isoformat(), pd.Timestamp(t).isoformat(), s] for d,t,s in train.loc[:, KEY].itertuples(index=False, name=None)]))


def predict_ranks(inputs, fitted):
    """No Y or future-status column is accessed. Unknown rows keep their original slots."""
    if fitted["policy_sha256"] != POLICY_HASH or fitted["schema"] != CONTRACT["schema"]:
        raise ValueError("model policy mismatch")
    frame = inputs.loc[:, INPUTS].copy().reset_index(drop=True)
    if frame.duplicated(["package_id", *KEY]).any():
        raise ValueError("duplicate prediction key")
    raw = _numeric(frame)[:, fitted["active_columns"]]
    known = np.isfinite(raw).any(axis=1)
    x = np.column_stack((np.where(np.isnan(raw), np.array(fitted["median"]), raw), np.isnan(raw).astype(float)))
    x = (x-np.array(fitted["center"]))/np.array(fitted["scale"])
    output = frame.loc[:, [*IDENTITY, "selection_effective_rank"]].copy()
    output["status"] = np.where(known, "SCORED", "NO_RANK_ADVICE")
    output["baseline_rank"] = frame.selection_effective_rank
    output["policy_sha256"] = POLICY_HASH
    for arm in ARMS[1:]:
        model = fitted["models"][arm]
        scores = x @ np.array(model["coef"]) + model["intercept"]
        scores[~known] = np.nan
        for _, idx in frame.groupby(["package_id", KEY[0]], sort=False).groups.items():
            idx = np.array(idx)
            order = frame.loc[idx, "selection_effective_rank"].to_numpy()
            if sorted(order) != list(range(1, len(idx)+1)):
                raise ValueError("non-contiguous original ranking")
            chosen = idx[known[idx]]
            if arm == "centered" and len(chosen):
                scores[chosen] -= scores[chosen].mean()
            sorted_known = sorted(chosen, key=lambda i: (-scores[i], int(frame.loc[i, "selection_effective_rank"])))
            output.loc[idx, arm+"_rank"] = order
            output.loc[sorted_known, arm+"_rank"] = sorted(frame.loc[chosen, "selection_effective_rank"].tolist())
        output[arm+"_rank"] = output[arm+"_rank"].astype(int)
        output[arm+"_score"] = scores
    return output


def _mean(values):
    return None if not values or any(v is None for v in values) else float(np.mean(values))


def evaluate_ranks(rows, days, predictions, *, evaluation_end):
    keys = list(IDENTITY)
    if (rows.duplicated(keys).any() or predictions.duplicated(keys).any()
            or set(rows.loc[:, keys].itertuples(index=False, name=None)) != set(predictions.loc[:, keys].itertuples(index=False, name=None))
            or not predictions.policy_sha256.eq(POLICY_HASH).all()):
        raise ValueError("prediction changed source identity or full roster")
    _validate_finance(rows)
    frame = rows.merge(predictions.drop(columns="selection_effective_rank"), on=keys, validate="one_to_one", sort=False)
    groups, ic, records = [], [], []
    for day in days.itertuples(index=False):
        group = frame.loc[frame.package_id.eq(day.package_id) & frame[KEY[0]].eq(day.decision_date)]
        mature = pd.notna(day.horizon_end) and day.horizon_end <= pd.Timestamp(evaluation_end)
        if len(group) != day.original_candidates or (len(group) and not group.label_information_end.eq(day.horizon_end).all()):
            raise ValueError("measurement date/count differs from original schedule")
        if not group.baseline_rank.equals(group.selection_effective_rank):
            raise ValueError("baseline order changed")
        for arm in ARMS:
            if sorted(group[arm+"_rank"].tolist()) != list(range(1, len(group)+1)):
                raise ValueError("prediction lost a candidate or duplicated a rank")
        contributions = {arm: [] for arm in ARMS}
        for row in group.to_dict("records"):
            state = row["valuation_status"]
            value = (float(row["hypothetical_liquidation_net_bps"]) if state == "AVAILABLE" else
                     0. if state == "CASH_ENTRY_NOT_EXECUTABLE" else None) if mature else None
            item = dict(package_id=day.package_id, date=day.decision_date.date().isoformat(), instrument=row["instrument"],
                mature=mature, value_bps=value, valuation_status=state, exit_execution_status=row["exit_execution_status"],
                selected={arm: row[arm+"_rank"] <= 10 for arm in ARMS}, input_status=row["status"])
            records.append(item)
            for arm in ARMS:
                if item["selected"][arm]:
                    contributions[arm].append(value)
        values = {arm: (None if not mature or any(v is None for v in vals) else float(sum(vals)/10))
                  for arm, vals in contributions.items()}
        groups.append(dict(package_id=day.package_id, date=day.decision_date.date().isoformat(), mature=bool(mature), values=values))
        common = group.loc[group.status.eq("SCORED") & group.valuation_status.eq("AVAILABLE")] if mature else group.iloc[:0]
        target = common.hypothetical_liquidation_net_bps
        scores = dict(baseline=-common.baseline_rank, absolute=common.absolute_score, centered=common.centered_score)
        ic.append(dict(package_id=day.package_id, date=day.decision_date.date().isoformat(), n=len(common),
            values={a: float(s.rank().corr(target.rank())) if len(common) >= 3 and s.nunique() > 1 and target.nunique() > 1 else None
                    for a,s in scores.items()}))
    dates = sorted({g["date"] for g in groups if g["mature"]})
    packages = sorted(days.package_id.unique())
    panels = {arm: [_mean([g["values"][arm] for g in groups if g["date"] == d]) for d in dates] for arm in ARMS}
    diffs = {a: [None if c is None or b is None else c-b for c,b in zip(panels["centered"], panels[a], strict=True)]
             for a in ARMS[:2]}
    stats_native = paired_statistics(dict(baseline=diffs["baseline"], matched_core=diffs["absolute"]))
    statistics = dict(baseline=stats_native["baseline"], absolute=stats_native["matched_core"])
    supports, attribution = {}, {}
    for opponent in ARMS[:2]:
        paired_dates = {d for d, v in zip(dates, diffs[opponent], strict=True) if v is not None}
        changes = [r for r in records if r["date"] in paired_dates and r["selected"]["centered"] != r["selected"][opponent]]
        clusters = {(r["date"],r["instrument"]) for r in changes}
        changed_dates = {r["date"] for r in changes}
        supports[opponent] = dict(unique_stock_days=len(clusters), days=len(changed_dates),
            fraction=len(changed_dates)/len(dates) if dates else 0.)
        scale = len(paired_dates)*len(packages)*10
        incoming = sum(r["value_bps"] for r in changes if r["selected"]["centered"])
        outgoing = sum(r["value_bps"] for r in changes if r["selected"][opponent])
        value = (incoming-outgoing)/scale if scale else None
        if value is not None and not np.isclose(value, statistics[opponent]["mean_increment_bps"], atol=1e-8, rtol=0):
            raise ValueError("membership attribution does not reconcile")
        attribution[opponent] = dict(incoming_bps=incoming/scale if scale else None,
            removed_bps=outgoing/scale if scale else None, net_increment_bps=value)
    risks = {a: None if not p or any(v is None for v in p) else float(np.maximum(-np.array(p)-800,0).mean()) for a,p in panels.items()}
    mean_ic = {a: _mean([r["values"][a] for r in ic if all(r["values"][arm] is not None for arm in ARMS)]) for a in ARMS}
    proceed = (mean_ic["centered"] is not None and mean_ic["absolute"] is not None and mean_ic["centered"] > mean_ic["absolute"]
        and all(statistics[a]["mean_increment_bps"] is not None and statistics[a]["mean_increment_bps"] > 5
            and statistics[a]["simultaneous_95_bonferroni_bps"] is not None and statistics[a]["simultaneous_95_bonferroni_bps"][0] > 0
            and supports[a]["unique_stock_days"] >= 30 and supports[a]["days"] >= 20 and supports[a]["fraction"] >= .25
            and risks["centered"] is not None and risks[a] is not None and risks["centered"] <= risks[a] for a in ARMS[:2]))
    strata = {}
    for kind, labels in (("package",packages), ("month",sorted({r["date"][:7] for r in groups}))):
        strata[kind] = {label: {a: dict(known_groups=sum(g["values"][a] is not None for g in groups if (g["package_id"] if kind == "package" else g["date"][:7]) == label),
            known_group_mean_bps=_mean([g["values"][a] for g in groups if g["mature"] and (g["package_id"] if kind == "package" else g["date"][:7]) == label and g["values"][a] is not None]))
            for a in ARMS} for label in labels}
    slot_metrics = {}
    for a in ARMS:
        selected = [r for r in records if r["selected"][a]]
        valued = [r for r in selected if r["mature"] and r["valuation_status"] == "AVAILABLE"]
        slot_metrics[a] = dict(original_selected=len(selected), mature_selected=sum(r["mature"] for r in selected),
            available_selected=len(valued), available_win_rate=_mean([float(r["value_bps"] > 0) for r in valued]),
            unavailable_entry=sum(r["mature"] and r["valuation_status"] == "CASH_ENTRY_NOT_EXECUTABLE" for r in selected),
            unknown_selected=sum(r["mature"] and r["value_bps"] is None for r in selected),
            exit_execution_status=pd.Series([r["exit_execution_status"] for r in valued], dtype=str).value_counts().to_dict())
    replacements = {}
    for arm in ARMS:
        changes = []
        for package in packages:
            previous = None
            for d in sorted(days.loc[days.package_id.eq(package),"decision_date"]):
                current = {r["instrument"] for r in records if r["package_id"] == package and r["date"] == d.date().isoformat() and r["selected"][arm]}
                if previous is not None:
                    changes.append(len(current.symmetric_difference(previous))/20)
                previous = current
        replacements[arm] = _mean(changes)
    return dict(status="WORTH_CONFIRMING" if proceed else "EXACT_CANDIDATE_NOT_SUPPORTED", statistics=statistics,
        fixed_ten_slot_mean_bps={a:_mean(p) for a,p in panels.items()}, group_tail_bps=risks,
        rank_ic_common=mean_ic, support=supports, attribution=attribution, strata=strata, slots=slot_metrics,
        adjacent_set_replacement_fraction_not_portfolio_turnover=replacements,
        original_rows=len(rows), original_days=int(days.decision_date.nunique()), mature_days=len(dates),
        original_package_days=len(days), panels=panels, dates=dates, groups=groups, rank_ic_by_group=ic,
        physical_fit_count=2, candidate_count=1, objective_contract="ALPHA_RANKING", decision_use="NAVIGATION_ONLY",
        measurement="FIXED_TEN_SLOT_FIVE_SESSION_NET_NOMINAL_NOT_NAV_NOT_FILL_NOT_MARKET_EXCESS",
        independent_oos_evidence=False, deployable=False, policy_sha256=POLICY_HASH)


def load_json(path):
    return json.loads(Path(path).read_bytes())


def run_study(*, source_root, source_plan_sha256, source_prepared_sha256, output_root, source_commit):
    if re.fullmatch(r"[0-9a-f]{40}", source_commit) is None:
        raise ValueError("explicit full producer commit required")
    source, output = Path(source_root).resolve(), Path(output_root).resolve()
    if (not Path(output_root).is_absolute() or output.drive.upper() == "C:" or str(output).startswith("/mnt/c/")
            or output == source or output in source.parents or source in output.parents):
        raise ValueError("study needs a separate non-C immutable output root")
    parent = read_stage(source/"preregistered", stage="preregistered", plan_sha256=source_plan_sha256, parent_sha256=None)
    prepared = read_stage(source/"prepared", stage="prepared", plan_sha256=source_plan_sha256, parent_sha256=parent["stage_sha256"])
    if prepared["stage_sha256"] != source_prepared_sha256:
        raise ValueError("source prepared stage mismatch")
    old = load_json(source/"preregistered/plan.json")
    dates = {k: old[k] for k in ("train_start", "train_end", "evaluation_start", "evaluation_end")}
    if not dates["train_start"] <= dates["train_end"] < dates["evaluation_start"] <= dates["evaluation_end"]:
        raise ValueError("source window order invalid")
    plan = dict(contract=CONTRACT, source_root=source.as_posix(), source_plan_sha256=source_plan_sha256,
        source_prepared_sha256=source_prepared_sha256, source_commit=source_commit,
        implementation_sha256=sha({name:file_sha256(Path(__file__).with_name(name)) for name in (
            Path(__file__).name,"generic_price_5td_contracts_v1.py","generic_price_5td_valuation_v2.py",
            "parent_score_price_5td_evaluation_v1.py","parent_score_price_5td_contracts_v1.py","economic_entry_pipeline.py")}),
        sources=[{k:s[k] for k in ("package_id","manifest_sha256","run_id")} for s in old["sources"]],
        inherited_evidence="QE_FROZEN_BACKTEST_SCORE_CURRENT_DATABASE_NON_VINTAGE_HISTORICAL_UNIVERSE_UNPROVEN",
        cumulative_prior_research_fits=158, prior_index_builds=1, prior_cohort_statistical_estimators=2, **dates)
    digest = sha(plan)
    root = output/("advgroupcentered_"+digest[:24])
    root.mkdir(parents=True, exist_ok=True)
    registry = AdvisoryResearchTrialRegistryV1(output/"trial_registry.jsonl")

    def publish(stage, parent_sha, artifacts, *, generated=0, evaluated=0):
        publish_stage(study_root=root, stage=stage, plan_sha256=digest, parent_sha256=parent_sha, artifacts=artifacts)
        record = build_trial_record(experiment_id=root.name, attempt_id="exact_v1", research_stage=stage.upper(),
            study_type="EXPLORATORY_SCREEN", objective_contract="ALPHA_RANKING", decision_use="NAVIGATION_ONLY",
            hypothesis_family_id="ADVISORY_GP5_ENTRY_VALUE", parent_lineage=("ADVISORY_GP5_ENTRY_VALUE",source.name),
            unique_variable="GROUP_CENTERED_RANK_5TD_1", dataset_identity=source_plan_sha256,
            schema_identity=CONTRACT["schema"], policy_identity=POLICY_HASH, planned_trial_count=2,
            generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
            result_class="EXPLORATORY", consumed_windows=(dict(window_id="CONSUMED_DEVELOPMENT_ONLY",
                dataset_identity=source_plan_sha256,start_date=dates["train_start"],end_date=dates["evaluation_end"]),),
            evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="group_centered_"+stage),))
        registry.append_batch((record,))
        return load_json(root/stage/"manifest.json")["stage_sha256"]

    started = time.monotonic()

    def budget():
        if psutil.Process().memory_info().rss > 8*1024**3 or time.monotonic()-started > 14400:
            raise ValueError("study exceeded its memory/time envelope")

    with _exclusive_file_lock(root/"study.lock"), threadpool_limits(limits=2):
        pre = publish("preregistered",None,{"plan.json":_json_bytes(plan)})
        feature_rows = pd.read_parquet(source/"prepared/rows.parquet", columns=list(INPUTS))
        days = pd.read_parquet(source/"prepared/days.parquet")
        calendar = load_json(source/"prepared/calendar.json")
        validate_roster(feature_rows,days,calendar,plan["sources"],dates["train_start"],dates["evaluation_end"])
        receipt = dict(original_rows=len(feature_rows),original_package_days=len(days),
            original_decision_days=int(days.decision_date.nunique()),source_prepared_sha256=source_prepared_sha256,
            source_sql_count=0, evaluation_finance_read=False, reused_old_models=False)
        prep = publish("prepared",pre,{"receipt.json":_json_bytes(receipt)})
        eval_mask = feature_rows[KEY[0]].between(dates["evaluation_start"],dates["evaluation_end"])
        if (root/"trained").exists():
            trained = read_stage(root/"trained",stage="trained",plan_sha256=digest,parent_sha256=prep)
            prediction = pd.read_parquet(root/"trained/predictions.parquet")
        else:
            journal = root/"fit_journal.jsonl"
            if journal.exists():
                raise ValueError("partial fit exists; no implicit refit")
            training = pd.read_parquet(source/"prepared/rows.parquet", columns=list(INPUTS)+list(FINANCE),
                filters=[(KEY[0],">=",pd.Timestamp(dates["train_start"])),(KEY[0],"<=",pd.Timestamp(dates["train_end"]))])
            def log_fit(arm,state):
                budget()
                mode = "x" if arm == "absolute" and state == "STARTED" else "a"
                with journal.open(mode,encoding="utf-8") as stream:
                    stream.write(json.dumps(dict(arm=arm,state=state,plan_sha256=digest),sort_keys=True)+"\n")
            fitted = train_models(training,train_start=dates["train_start"],train_end=dates["train_end"],
                before_fit=lambda arm:log_fit(arm,"STARTED"),after_fit=lambda arm:log_fit(arm,"COMPLETE"))
            prediction = predict_ranks(feature_rows.loc[eval_mask].reset_index(drop=True),fitted)
            budget()
            publish("trained",prep,{"model.json":_json_bytes(fitted),"predictions.parquet":parquet_bytes(prediction),
                "fit_journal.jsonl":journal.read_bytes()},generated=2)
            trained = load_json(root/"trained/manifest.json")
        if (root/"evaluated").exists():
            read_stage(root/"evaluated",stage="evaluated",plan_sha256=digest,parent_sha256=trained["stage_sha256"])
            return dict(load_json(root/"evaluated/evaluation.json"),artifact_root=root.as_posix(),exact_retry=True)
        # This is the first evaluation-outcome decode; rankings are already immutable.
        evaluated = pd.read_parquet(source/"prepared/rows.parquet", columns=list(INPUTS)+list(FINANCE),
            filters=[(KEY[0],">=",pd.Timestamp(dates["evaluation_start"])),(KEY[0],"<=",pd.Timestamp(dates["evaluation_end"]))])
        eval_days = days.loc[days.decision_date.between(dates["evaluation_start"],dates["evaluation_end"])].copy()
        validate_roster(evaluated,eval_days,calendar,plan["sources"],dates["evaluation_start"],dates["evaluation_end"])
        result = evaluate_ranks(evaluated,eval_days,prediction,evaluation_end=dates["evaluation_end"])
        budget()
        verified = read_stage(source/"prepared",stage="prepared",plan_sha256=source_plan_sha256,parent_sha256=parent["stage_sha256"])
        if verified["stage_sha256"] != prepared["stage_sha256"]:
            raise ValueError("source changed during consumption")
        result.update(experiment_id=root.name,plan_sha256=digest,forecast_stage_sha256=trained["stage_sha256"],
            source_prepared_sha256=source_prepared_sha256,source_evidence=plan["inherited_evidence"],
            elapsed_seconds=time.monotonic()-started, ending_rss_bytes=psutil.Process().memory_info().rss,
            cumulative_research_fits=160,sql_count=0)
        publish("evaluated",trained["stage_sha256"],{"evaluation.json":_json_bytes(result)},generated=2,evaluated=2)
        return dict(result,artifact_root=root.as_posix(),exact_retry=False)
