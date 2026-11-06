"""File-only, fixed-protocol research of matured cohort state; no runtime wiring."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import paired_statistics
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

D = "decision_as_of_trade_date"
T = "target_trade_date"
H = "label_information_end"
POLICY = dict(version="causal_mature_cohort_state_v1", window=20, risk_bps=800,
    information_clock="D_CLOSE_MATURED_ONLY", target="EQUAL_PACKAGE_FIXED_FIVE_SLOT_5TD",
    action="TAKE_IFF_MEAN_AND_MEAN_MINUS_GROUP_TAIL_POSITIVE", missing="NO_ADVICE_BASELINE_IDENTITY")
POLICY_SHA256 = sha(POLICY)
ARMS = ("static", "rolling20")
CRITERIA = dict(min_increment_bps_exclusive=5, min_intervention_days=20,
    min_intervention_fraction=.25, min_unique_stock_days=30,
    simultaneous_lower_bound_exclusive=0, group_tail_not_worse=True)


def implementation_sha256() -> str:
    root = Path(__file__).parent
    names = (Path(__file__).name, "economic_entry_pipeline.py", "parent_score_price_5td_evaluation_v1.py",
        "parent_score_price_5td_contracts_v1.py", "research_control.py", "research_control_contracts.py")
    return sha({n: hashlib.sha256((root/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})


def build_cohorts(rows: pd.DataFrame, days: pd.DataFrame, calendar: list[str]) -> pd.DataFrame:
    """Finance preparation only; predictions receive the separate as-of projection."""
    sessions = pd.DatetimeIndex(pd.to_datetime(calendar))
    packages = sorted(days.package_id.unique())
    if (days.empty or not packages or days[["package_id", "decision_date", "target_date"]].isna().any().any()
            or (days.original_candidates < 0).any() or list(sessions) != sorted(set(sessions)) or rows.duplicated(["package_id", D, T, "instrument"]).any()
            or days.duplicated(["package_id", "decision_date"]).any() or set(rows.package_id)-set(packages)):
        raise ValueError("duplicate or invalid original cohort identity")
    dates = sorted(days.decision_date.unique())
    expected = sessions[(sessions >= dates[0]) & (sessions <= dates[-1])].tolist()
    if dates != expected:
        raise ValueError("original decision calendar has a missing date")
    if set(rows[D])-set(dates) or rows[["package_id", D, T, "instrument"]].isna().any().any():
        raise ValueError("foreign or null original slot")
    grouped = {(p,d): g for (p,d),g in rows.groupby(["package_id", D], sort=False)}
    output = []
    for d in dates:
        i = sessions.get_loc(d)
        target = sessions[i+1] if i+1 < len(sessions) else pd.NaT
        horizon = sessions[i+5] if i+5 < len(sessions) else pd.NaT
        native = days.loc[days.decision_date.eq(d)]
        if set(native.package_id) != set(packages) or not native.target_date.eq(target).all():
            raise ValueError("original package/date or T differs")
        sums, known, slots = [], True, 0
        for item in native.itertuples(index=False):
            g = grouped.get((item.package_id,d), rows.iloc[:0])
            if (len(g) != min(5,item.original_candidates) or g.selection_effective_rank.tolist() != list(range(1,len(g)+1))
                    or not g[T].eq(target).all() or not (g[H].eq(horizon) | (g[H].isna() & pd.isna(horizon))).all()
                    or not (item.horizon_end == horizon or pd.isna(item.horizon_end) and pd.isna(horizon))):
                raise ValueError("original slot count, rank or five-session horizon differs")
            subtotal = 0.
            for row in g.itertuples(index=False):
                status = row.valuation_status
                value = row.hypothetical_liquidation_net_bps
                if status in ("AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"):
                    if value is None or not np.isfinite(float(value)) or status == "CASH_ENTRY_NOT_EXECUTABLE" and float(value) != 0:
                        raise ValueError("known finance missing or cash invented")
                    subtotal += float(value)
                else:
                    known = False
            sums.append(subtotal/5)
            slots += len(g)
        output.append(dict(decision_date=pd.Timestamp(d), horizon_end=horizon, known=known,
            cohort_net_bps=float(np.mean(sums)) if known else np.nan, original_slots=slots,
            packages=len(packages), fixed_slot_budget=5*len(packages)))
    return pd.DataFrame(output)


def predict_state(cohorts: pd.DataFrame, *, as_of, window: int | None) -> dict:
    """H filtering precedes financial value access. Unknown days never shift the window."""
    if window not in (None, POLICY["window"]):
        raise ValueError("this study does not search windows")
    as_of = pd.Timestamp(as_of)
    if cohorts.decision_date.duplicated().any() or not cohorts.decision_date.is_monotonic_increasing:
        raise ValueError("state ledger must retain sorted unique original dates")
    visible = cohorts.loc[cohorts.horizon_end.le(as_of) & cohorts.decision_date.lt(as_of)]
    selected = visible if window is None else visible.tail(window)
    records = [dict(decision_date=r.decision_date.date().isoformat(), horizon_end=r.horizon_end.date().isoformat(),
        known=bool(r.known), value=float(r.cohort_net_bps) if r.known else None) for r in selected.itertuples(index=False)]
    common = dict(as_of=as_of.date().isoformat(), history_days=len(selected), history_sha256=sha(records),
        max_label_end=None if not len(selected) else selected.horizon_end.max().date().isoformat(),
        unknown_history_days=int((~selected.known).sum()), window=window, policy_sha256=POLICY_SHA256)
    if not len(selected) or window is not None and (len(selected) != window or not selected.known.all()):
        return dict(common, status="NO_ADVICE", action="NO_ADVICE", reason="INSUFFICIENT_OR_UNKNOWN_ORIGINAL_HISTORY")
    # Static training statistic may use its known labels; all omissions remain in common.
    values = selected.loc[selected.known, "cohort_net_bps"].to_numpy(dtype=float)
    if not len(values):
        return dict(common, status="NO_ADVICE", action="NO_ADVICE", reason="NO_KNOWN_TRAIN_COHORT")
    if not np.isfinite(values).all():
        raise ValueError("available history contains nonfinite finance")
    mean, tail = float(values.mean()), float(np.maximum(-values-POLICY["risk_bps"],0).mean())
    return dict(common, status="PREDICTED", action="TAKE" if mean > 0 and mean-tail > 0 else "SKIP",
        expected_net_bps=mean, expected_group_excess_loss_bps=tail, expected_utility_bps=mean-tail,
        p_profit=float((values > 0).mean()), known_history_days=len(values), reason=None)


def freeze_forecasts(cohorts: pd.DataFrame, *, train_end, evaluation_dates: list) -> dict:
    if not evaluation_dates or len(set(evaluation_dates)) != len(evaluation_dates):
        raise ValueError("evaluation requires unique original dates")
    static = predict_state(cohorts, as_of=train_end, window=None)
    forecasts = {}
    for d in evaluation_dates:
        d = pd.Timestamp(d)
        if d <= pd.Timestamp(train_end):
            raise ValueError("evaluation overlaps initial training")
        forecasts[d.date().isoformat()] = dict(static=static, rolling20=predict_state(cohorts, as_of=d, window=20))
    return forecasts


def evaluate_states(source: dict, forecasts: dict) -> dict:
    dates = source["aggregate"]["original_mature_days"]
    if (not dates or len(dates) != len(set(dates)) or not set(dates).issubset(forecasts)
            or len(forecasts) != source["original_decision_days"]
            or not {ep["decision_date"] for ep in source["episodes"]}.issubset(forecasts)):
        raise ValueError("prediction changed original dates")
    for d, arms in forecasts.items():
        if set(arms) != set(ARMS):
            raise ValueError("prediction arm mismatch")
        for state in arms.values():
            if (state.get("policy_sha256") != POLICY_SHA256 or state.get("action") not in ("TAKE", "SKIP", "NO_ADVICE")
                    or state["as_of"] > d or state["max_label_end"] is not None and state["max_label_end"] > state["as_of"]):
                raise ValueError("prediction policy, action or observation clock mismatch")
    panels = {a: list(source["aggregate"]["fixed_slot_panels_bps"][a]) for a in ("baseline","rule_300bps")}
    for arm in ARMS:
        panels[arm] = [None if v is None else 0. if forecasts[d][arm]["action"] == "SKIP" else v
            for d,v in zip(dates,panels["baseline"],strict=True)]
    candidate = panels["rolling20"]
    comparisons = {a:[None if x is None or y is None else x-y for x,y in zip(candidate,panels[a],strict=True)]
        for a in ("baseline","static")}
    # Reuse the native two-endpoint statistic; matched_core is its API key only.
    native_stats = paired_statistics(dict(baseline=comparisons["baseline"], matched_core=comparisons["static"]))
    stats = dict(baseline=native_stats["baseline"], static=native_stats["matched_core"])
    skills = {}
    for arm in ARMS:
        pairs = [(forecasts[d][arm], v) for d, v in zip(dates, panels["baseline"], strict=True)
            if v is not None and forecasts[d][arm]["status"] == "PREDICTED"]
        skills[arm] = dict(predicted_known_days=len(pairs), original_mature_days=len(dates),
            rmse_bps=None if not pairs else float(np.sqrt(np.mean([(f["expected_net_bps"]-v)**2 for f,v in pairs]))),
            brier=None if not pairs else float(np.mean([(f["p_profit"]-float(v>0))**2 for f,v in pairs])))
    risks = {a: None if any(v is None for v in p) else float(np.maximum(-np.array(p)-800,0).mean()) for a,p in panels.items()}
    supports, attribution = {}, {}
    for opponent in ("baseline","static"):
        changed_dates, clusters = set(), set()
        parts = dict(avoided_loss_bps=0.,missed_profit_bps=0.,new_take_profit_bps=0.,new_take_loss_bps=0.)
        scale = 1/(len(dates)*5*len(source["aggregate"]["packages"]))
        for ep in source["episodes"]:
            if not ep["mature"] or not ep["financial_known"] or ep["valuation_status"] == "CASH_ENTRY_NOT_EXECUTABLE":
                continue
            d, value = ep["decision_date"], ep["baseline_net_bps"]
            a = forecasts[d]["rolling20"]["action"] != "SKIP"
            b = opponent == "baseline" or forecasts[d][opponent]["action"] != "SKIP"
            if a != b:
                changed_dates.add(d)
                clusters.add(ep["label_cluster"])
                key = ("new_take_profit_bps" if value >= 0 else "new_take_loss_bps") if a else (
                    "missed_profit_bps" if value >= 0 else "avoided_loss_bps")
                parts[key] += abs(value)*scale
        supports[opponent] = dict(days=len(changed_dates),unique_stock_days=len(clusters),fraction=len(changed_dates)/len(dates))
        parts["net_increment_bps"] = parts["avoided_loss_bps"]-parts["missed_profit_bps"]+parts["new_take_profit_bps"]-parts["new_take_loss_bps"]
        point=stats[opponent]["mean_increment_bps"]
        if point is not None and all(v is not None for v in comparisons[opponent]) and not np.isclose(point,parts["net_increment_bps"],atol=1e-9):
            raise ValueError("original-slot economic attribution differs")
        attribution[opponent] = parts
    proceed = all(stats[a]["mean_increment_bps"] is not None and stats[a]["mean_increment_bps"] > CRITERIA["min_increment_bps_exclusive"]
        and stats[a]["simultaneous_95_bonferroni_bps"] is not None
        and stats[a]["simultaneous_95_bonferroni_bps"][0] > CRITERIA["simultaneous_lower_bound_exclusive"]
        and supports[a]["days"] >= CRITERIA["min_intervention_days"]
        and supports[a]["fraction"] >= CRITERIA["min_intervention_fraction"]
        and supports[a]["unique_stock_days"] >= CRITERIA["min_unique_stock_days"]
        and risks["rolling20"] is not None and risks[a] is not None and risks["rolling20"] <= risks[a]
        for a in comparisons)
    return dict(status="WORTH_CONFIRMING" if proceed else "EXACT_CANDIDATE_NOT_SUPPORTED", statistics=stats,
        fixed_slot_mean_bps={a: None if any(v is None for v in p) else float(np.mean(p)) for a,p in panels.items()},
        group_tail_bps=risks, predictive_skill=skills, support=supports, attribution=attribution, original_days=len(forecasts),mature_days=len(dates),
        original_slots=len(source["episodes"]), actions={a:{act:sum(f[a]["action"] == act for f in forecasts.values())
            for act in ("TAKE","SKIP","NO_ADVICE")} for a in ARMS},
        mature_actions={a:{act:sum(forecasts[d][a]["action"] == act for d in dates)
            for act in ("TAKE","SKIP","NO_ADVICE")} for a in ARMS},
        policy_sha256=POLICY_SHA256, decision_use="NAVIGATION_ONLY", independent_oos_evidence=False,deployable=False)


def run_study(*, source_root: str, source_plan_sha256: str, forecast_stage_sha256: str, output_root: str) -> dict:
    source_root, output_root = Path(source_root).resolve(), Path(output_root).resolve()
    if (output_root.drive.upper() == "C:" or output_root == source_root or source_root in output_root.parents
            or output_root in source_root.parents):
        raise ValueError("new study must not overwrite/nest source or use C")
    parent, manifests = None, {}
    for stage in ("preregistered","prepared","trained","evaluated"):
        manifests[stage] = read_stage(source_root/stage,stage=stage,plan_sha256=source_plan_sha256,parent_sha256=parent)
        parent=manifests[stage]["stage_sha256"]
    header=read_stage(source_root/"forecasts/trained",stage="trained",plan_sha256=source_plan_sha256,
        parent_sha256=manifests["trained"]["stage_sha256"])
    if header["stage_sha256"] != forecast_stage_sha256:
        raise ValueError("source frozen forecast identity mismatch")
    original_plan=json.loads((source_root/"preregistered/plan.json").read_bytes())
    plan=dict(schema="causal_cohort_state_study_v1",policy=POLICY,criteria=CRITERIA,source_root=source_root.as_posix(),
        source_plan_sha256=source_plan_sha256,source_stages={s:m["stage_sha256"] for s,m in manifests.items()},
        forecast_stage_sha256=forecast_stage_sha256,implementation_sha256=implementation_sha256(),
        train_start=original_plan["train_start"],train_end=original_plan["train_end"],
        evaluation_start=original_plan["evaluation_start"],evaluation_end=original_plan["evaluation_end"])
    digest=sha(plan)
    root=output_root/("advcohortstate_"+digest[:24])
    registry=AdvisoryResearchTrialRegistryV1(output_root/"trial_registry.jsonl")
    def register(stage, *, generated=0,evaluated=0):
        record=build_trial_record(experiment_id=root.name,attempt_id="exact_v1",research_stage=stage.upper(),
            study_type="EXPLORATORY_SCREEN",objective_contract="RISK_MANAGED_ADVISORY",decision_use="NAVIGATION_ONLY",
            hypothesis_family_id="ADVISORY_GP5_ENTRY_VALUE",parent_lineage=("ADVISORY_GP5_ENTRY_VALUE",source_root.name),
            unique_variable="CAUSAL_MATURE_COHORT_STATE_1",dataset_identity=source_plan_sha256,schema_identity=plan["schema"],
            policy_identity=POLICY_SHA256,planned_trial_count=2,generated_trial_count=generated,evaluated_trial_count=evaluated,
            selected_trial_count=0,result_class="EXPLORATORY",consumed_windows=(dict(window_id="CONSUMED_DEVELOPMENT_ONLY",
                dataset_identity=source_plan_sha256,start_date=plan["train_start"],end_date=plan["evaluation_end"]),),
            evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json",role="cohort_state_"+stage),))
        registry.append_batch((record,))
    def publish(stage,parent,artifacts,**counts):
        path=root/stage
        if path.exists():
            prior=read_stage(path,stage=stage,plan_sha256=digest,parent_sha256=parent)
            for name,content in artifacts.items():
                if prior["files"].get(name,{}).get("sha256") != hashlib.sha256(content).hexdigest():
                    raise ValueError("existing study content changed")
        else:
            publish_stage(study_root=root,stage=stage,plan_sha256=digest,parent_sha256=parent,artifacts=artifacts)
        register(stage,**counts)
        return json.loads((path/"manifest.json").read_bytes())["stage_sha256"]
    prereg=publish("preregistered",None,{"plan.json":_json_bytes(plan)})
    train=pd.read_parquet(source_root/"prepared/domain.parquet")
    measured=pd.read_parquet(source_root/"evaluated/measured_rows.parquet")
    rows=pd.concat((train,measured),ignore_index=True)
    days=pd.read_parquet(source_root/"prepared/days.parquet")
    identities = {s["package_id"]:s for s in original_plan["sources"]}
    if set(days.package_id) != set(identities):
        raise ValueError("source package identity mismatch")
    for package, group in rows.groupby("package_id", sort=False):
        identity = identities.get(package)
        if identity is None or not group.manifest_sha256.eq(identity["manifest_sha256"]).all() or not group.run_id.eq(identity["run_id"]).all():
            raise ValueError("source manifest/run identity mismatch")
    calendar=json.loads((source_root/"prepared/calendar.json").read_bytes())
    cohorts=build_cohorts(rows,days,calendar)
    meta=dict(preparation_original_cohorts=len(cohorts),preparation_original_slots=len(rows),known_cohorts=int(cohorts.known.sum()),
        physical_fit_count=0,statistical_estimators=2,candidate_count=1,sql_count=0)
    prepared=publish("prepared",prereg,{"receipt.json":_json_bytes(meta)})
    dates=cohorts.loc[cohorts.decision_date.between(plan["evaluation_start"],plan["evaluation_end"]),"decision_date"].tolist()
    forecasts=freeze_forecasts(cohorts,train_end=plan["train_end"],evaluation_dates=dates)
    trained=publish("trained",prepared,{"forecasts.json":_json_bytes(forecasts)},generated=2)
    source=json.loads((source_root/"evaluated/evaluation.json").read_bytes())
    if source["forecast_stage_sha256"] != forecast_stage_sha256:
        raise ValueError("finance references another source forecast")
    actual=cohorts.set_index("decision_date")
    for d,v in zip(source["aggregate"]["original_mature_days"],source["aggregate"]["fixed_slot_panels_bps"]["baseline"],strict=True):
        ours=actual.loc[pd.Timestamp(d),"cohort_net_bps"]
        if (v is None) != bool(pd.isna(ours)) or v is not None and not np.isclose(ours,v,atol=1e-9,rtol=0):
            raise ValueError("new ledger changed original economic baseline")
    result=evaluate_states(source,forecasts)
    result["by_package"]={p:evaluate_states(dict(source,aggregate=aggregate,episodes=[e for e in source["episodes"] if e["package_id"] == p]),
        forecasts) for p,aggregate in source["by_package"].items()}
    result.update(meta,experiment_id=root.name,plan_sha256=digest,forecast_stage_sha256=trained,
        criteria=CRITERIA,state_update_count=len(dates),
        source_valuation_policy_sha256=source["policy_sha256"],
        measurement="FIXED_FIVE_ORIGINAL_SLOT_FIVE_SESSION_NOMINAL_NOT_NAV_NOT_FILL_NOT_EXCESS")
    publish("evaluated",trained,{"evaluation.json":_json_bytes(result)},generated=2,evaluated=2)
    return dict(result,artifact_root=root.as_posix())
