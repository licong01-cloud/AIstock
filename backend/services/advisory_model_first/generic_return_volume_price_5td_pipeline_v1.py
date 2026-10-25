"""Own immutable lag-information study, never reruns parents or submits QE."""
from dataclasses import asdict
from datetime import date, datetime, timezone
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict

from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, _verify_reference, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.generic_price_5td_models_v1 import GenericPrice5TDFitV1, validate_fit as validate_control
from backend.services.advisory_model_first.generic_price_5td_inference_v1 import query_price_nodes_v1
from backend.services.advisory_model_first.generic_price_5td_pipeline_v1 import GenericPrice5TDPlanV1
from backend.services.advisory_model_first.generic_return_volume_price_5td_contracts_v1 import (
    KEY, POLICY, POLICY_SHA256, SCHEMA_SHA256, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.generic_return_volume_price_5td_model_v1 import (
    GenericReturnVolumePrice5TDFitV1, query_return_volume_nodes_v1, train_return_volume_price_5td_v1,
)
from backend.services.advisory_model_first.generic_return_volume_price_5td_source_v1 import build_return_volume_lag_features_v1
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, EvidenceReferenceV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_return_volume_price_5td_{v}_v1.py" for v in ("contracts", "source", "model", "pipeline")]
    names += [f"generic_price_5td_{v}_v1.py" for v in ("contracts", "models", "inference", "pipeline")]
    names += ["generic_daily_price_input_v1.py", "economic_price_campaign_models_v2.py", "economic_entry_pipeline.py"]
    return sha({name: file_sha256(directory/name) for name in names})


class GenericReturnVolumePrice5TDPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    configuration: GenericPrice5TDConfigurationV1
    gp5_plan_ref: EvidenceReferenceV1
    gp5_prepared_ref: EvidenceReferenceV1
    gp5_trained_ref: EvidenceReferenceV1
    dataset_identity: str
    parent_lineage: tuple[str, ...]
    decision_dates: tuple[date, ...]
    implementation_sha256: str

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode="json"), "schema_sha256": SCHEMA_SHA256, "policy_sha256": POLICY_SHA256,
                    "study_type": "EXPLORATORY_SCREEN", "decision_use": "NAVIGATION_ONLY", "physical_fit_budget": 2})

    @property
    def experiment_id(self):
        return "advgp5rvlag_"+self.plan_sha256[:24]


def _control(body):
    fitted = GenericPrice5TDFitV1(**{**body, "intervals_bps": tuple(tuple(v) for v in body["intervals_bps"])})
    validate_control(fitted)
    return fitted


def _load(plan, output_root):
    plan = GenericReturnVolumePrice5TDPlanV1.model_validate(plan)
    declared, root = Path(output_root), Path(output_root).resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:" or declared != root:
        raise ValueError("return-volume output requires explicit non-C real root")
    if (plan.implementation_sha256 != implementation_sha256() or not plan.dataset_identity or not plan.parent_lineage
            or not plan.decision_dates or list(plan.decision_dates) != sorted(set(plan.decision_dates))):
        raise ValueError("return-volume code/window/lineage identity differs")
    path = _verify_reference(plan.gp5_plan_ref)
    parent = GenericPrice5TDPlanV1.model_validate_json(path.read_bytes())
    stages, digest = {}, None
    for name, ref in (("preregistered", plan.gp5_plan_ref), ("prepared", plan.gp5_prepared_ref), ("trained", plan.gp5_trained_ref)):
        item = _verify_reference(ref)
        if item.parent != path.parent.parent/name or (name != "preregistered" and item.name != "manifest.json"):
            raise ValueError("return-volume requires unchanged parent stage chain")
        stages[name] = read_stage(item.parent, stage=name, plan_sha256=parent.plan_sha256, parent_sha256=digest)
        digest = stages[name]["stage_sha256"]
    if (plan.configuration != parent.configuration or plan.decision_dates != parent.decision_dates
            or plan.dataset_identity != parent.dataset_identity or parent.experiment_id not in plan.parent_lineage):
        raise ValueError("return-volume original parent population/configuration differs")
    return plan, root/plan.experiment_id, parent, path.parent.parent, stages


def _stage(plan, root, until):
    digest = None
    for name in ("preregistered", "prepared", "trained", "evaluated"):
        result = read_stage(root/name, stage=name, plan_sha256=plan.plan_sha256, parent_sha256=digest)
        digest = result["stage_sha256"]
        if name == until:
            return result
    raise ValueError("unknown return-volume study stage")


def _record(plan, root, stage):
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="exact_attempt_v1",
        research_stage=stage.upper(), study_type="EXPLORATORY_SCREEN", hypothesis_family_id="generic_return_volume_price_5td_v1",
        parent_lineage=plan.parent_lineage, unique_variable="D_19SESSION_VOLUME_RETURN_LAG_INFORMATION",
        objective_contract="RISK_MANAGED_ADVISORY", dataset_identity=plan.dataset_identity,
        schema_identity=SCHEMA_SHA256, policy_identity=POLICY_SHA256, planned_trial_count=1,
        generated_trial_count=int(stage in ("trained", "evaluated")), evaluated_trial_count=int(stage == "evaluated"), selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY", dataset_identity=plan.dataset_identity,
            start_date=plan.configuration.train_start, end_date=plan.configuration.label_cutoff),), result_class="EXPLORATORY", decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="gp5_return_volume_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def preregister_return_volume_price_5td_v1(*, plan, output_root):
    plan, root, _, _, _ = _load(plan, output_root)
    publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json")), "policy.json": _json_bytes(POLICY)})
    _record(plan, root, "preregistered")
    return root/"preregistered"/"plan.json"


def prepare_return_volume_price_5td_v1(*, plan, output_root):
    plan, root, parent, parent_root, stages = _load(plan, output_root)
    first = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        _record(plan, root, "prepared")
        return root/"prepared"
    paths = {name: _verify_reference(parent.inputs[name]) for name in ("calendar", "raw_daily", "volume_daily")}
    rows = pd.read_parquet(parent_root/"prepared"/"rows.parquet")
    original = rows.loc[:, KEY].copy()
    previous = json.loads((parent_root/"prepared"/"receipt.json").read_bytes())
    control_body = json.loads((parent_root/"trained"/"model.json").read_bytes())
    control = _control(control_body)
    if (previous["decision_dates"] != [v.isoformat() for v in plan.decision_dates]
            or not rows.policy_sha256.eq(POLICY_SHA256).all() or not rows.label_contract.eq(POLICY["label_contract"]).all()):
        raise ValueError("return-volume frozen prepared labels/policy/calendar differ")
    prices = pd.read_parquet(paths["raw_daily"], columns=["trade_date", "instrument", "raw_close_cny", "adj_factor"])
    volumes = pd.read_parquet(paths["volume_daily"], columns=["trade_date", "instrument", "volume_hand"])
    calendar = json.loads(paths["calendar"].read_bytes())
    extra, source = build_return_volume_lag_features_v1(roster=original, calendar=calendar, prices=prices, volumes=volumes)
    rows = rows.merge(extra, on=list(KEY), how="left", sort=False, validate="one_to_one")
    # <=7720 unique KEY per side, one-to-one left join: bounded original population.
    if not rows.loc[:, KEY].equals(original):
        raise ValueError("return-volume preparation changed original keys/order")
    for name in paths:
        _verify_reference(parent.inputs[name])
    _load(plan, output_root)
    buffer = io.BytesIO()
    rows.to_parquet(buffer, index=False)
    receipt = dict(original_rows=len(rows), decision_dates=previous["decision_dates"], lag_source=source,
        parent_stage_sha256={k: v["stage_sha256"] for k, v in stages.items()}, frozen_control_arm="candidate_19D",
        frozen_control_model_sha256=control.model_sha256, source_evidence=previous["source_evidence"],
        parent_refit=False, database_written=False, selection_regenerated=False, active_profile_required=False)
    publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=first["stage_sha256"],
        artifacts={"rows.parquet": buffer.getvalue(), "control_model.json": _json_bytes(control_body), "receipt.json": _json_bytes(receipt)})
    _record(plan, root, "prepared")
    return root/"prepared"


def train_return_volume_price_5td_study_v1(*, plan, output_root, qe_training_idle):
    plan, root, _, _, _ = _load(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        _record(plan, root, "trained")
        return root/"trained"
    if qe_training_idle is not True:
        raise ValueError("return-volume fit waits for fresh QE idle check")
    with (root/"fit_attempt.json").open("xb") as stream:
        stream.write(_json_bytes(dict(status="STARTED", physical_fit_budget=2, started_at=datetime.now(timezone.utc).isoformat())))
    count = 0
    def before_fit(head):
        nonlocal count
        count += 1
        if count > 2:
            raise ValueError("return-volume physical fit budget exceeded")
        with (root/"fit_journal.jsonl").open("ab") as stream:
            stream.write((json.dumps(dict(head=head, kind="PHYSICAL_FIT", ordinal=count), allow_nan=False)+"\n").encode())
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    control = _control(json.loads((root/"prepared"/"control_model.json").read_bytes()))
    fitted = train_return_volume_price_5td_v1(rows=rows, frozen_control=control, configuration=plan.configuration, before_fit=before_fit)
    publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"],
                  artifacts={"model.json": _json_bytes(asdict(fitted))})
    _record(plan, root, "trained")
    return root/"trained"


def evaluate_return_volume_price_5td_cohorts_v1(*, rows, fitted, frozen_control, decision_dates):
    dates = pd.DatetimeIndex(pd.to_datetime(decision_dates))
    required = {*KEY, "selection_effective_rank", "candidate_group_size", "label_status", "observed_gap_bps", "gross_terminal_ratio"}
    if (not isinstance(rows, pd.DataFrame) or len(rows) > 7720 or not required.issubset(rows.columns) or not rows.columns.is_unique
            or not rows.index.is_unique or rows.duplicated([KEY[0], KEY[2]]).any() or len(dates) > 386
            or dates.hasnans or dates.has_duplicates or not dates.is_monotonic_increasing or not rows[KEY[0]].isin(dates).all()):
        raise ValueError("return-volume cohort original rows/decision schedule differ")
    for _, group in rows.groupby(KEY[0], sort=False):
        if (len(group) > 50 or group.selection_effective_rank.tolist() != list(range(1, len(group)+1))
                or not group.candidate_group_size.eq(len(group)).all() or group[KEY[1]].nunique() != 1
                or not group[KEY[1]].gt(group[KEY[0]]).all()
                or any(not isinstance(v, (int, np.integer)) or isinstance(v, (bool, np.bool_))
                       for v in [*group.selection_effective_rank, *group.candidate_group_size])):
            raise ValueError("return-volume full original ranks/group/target differ")
    states = dict(candidate=query_return_volume_nodes_v1(fitted=fitted, features=rows, scenario_gap_bps=rows.observed_gap_bps),
                  matched=query_price_nodes_v1(fitted=frozen_control, features=rows, scenario_gap_bps=rows.observed_gap_bps, arm="candidate"))
    panel = []
    for d in dates:
        group = rows.loc[rows[KEY[0]].eq(d) & rows.selection_effective_rank.le(5)]
        settled, item = {}, {KEY[0]: str(d.date())}
        for row in group.loc[group.label_status.eq("AVAILABLE")].itertuples(index=False):
            if not np.isfinite([row.gross_terminal_ratio, row.observed_gap_bps]).all() or row.gross_terminal_ratio <= 0 or row.observed_gap_bps <= -10000:
                raise ValueError("return-volume AVAILABLE settlement price coordinates differ")
            value = 10000*(row.gross_terminal_ratio*(1-POLICY["sell_bps"]/10000)/((1+row.observed_gap_bps/10000)*(1+POLICY["buy_bps"]/10000))-1)
            if not np.isfinite(value):
                raise ValueError("return-volume settlement arithmetic is nonfinite")
            settled[row.instrument] = float(value)
        item["settled_returns_bps"] = settled
        for arm in ("baseline", "rule", "candidate", "matched"):
            values, returns, actions, known, unknown, unsettled, not_executable = [], [], [], {}, 0, 0, 0
            for index, row in group.iterrows():
                if row.label_status == "ENTRY_NOT_EXECUTABLE":
                    values.append(0.)
                    not_executable += 1
                    continue
                state = ("ACCEPTABLE" if arm == "baseline" else ("UNKNOWN_INPUT_OR_SUPPORT" if pd.isna(row.observed_gap_bps)
                         else "ACCEPTABLE" if abs(row.observed_gap_bps) <= 300 else "AVOID") if arm == "rule"
                         else states[arm].loc[index, "status"])
                if state not in {"ACCEPTABLE", "AVOID", "UNKNOWN_INPUT_OR_SUPPORT"}:
                    raise ValueError("return-volume model action state is invalid")
                action = state == "ACCEPTABLE"
                unknown += int(state == "UNKNOWN_INPUT_OR_SUPPORT")
                if state != "UNKNOWN_INPUT_OR_SUPPORT":
                    known[row.instrument] = action
                if action:
                    actions.append(row.instrument)
                if not action:
                    values.append(0.)
                elif row.instrument not in settled:
                    values.append(None)
                    unsettled += 1
                else:
                    values.append(settled[row.instrument])
                    returns.append(settled[row.instrument])
            item[arm] = dict(net_bps=None if any(v is None for v in values) else sum(values)/5,
                true_take=len(returns), unknown=unknown, unsettled=unsettled, not_executable=not_executable,
                known_avoid=sum(not v for v in known.values()), cash_slots=5-len(returns)-unsettled,
                known_decisions=known, take_returns_bps=returns, intervention_actions=actions)
        reject = [k for k, action in item["candidate"]["known_decisions"].items() if not action and item["baseline"]["known_decisions"].get(k) is True]
        missing = item["baseline"]["known_decisions"].keys()-item["candidate"]["known_decisions"].keys()
        avoided = sum(-settled[k] for k in reject if k in settled and settled[k] < 0)/5
        missed = sum(settled[k] for k in reject if k in settled and settled[k] > 0)/5
        item["rejection_attribution"] = dict(avoided_loss_bps=avoided, missed_profit_bps=missed,
            known_rejection_contribution_bps=avoided-missed, unknown_cash_contribution_bps=-sum(settled[k] for k in missing if k in settled)/5,
            unresolved_rejection_count=sum(k not in settled for k in reject), unknown_cash_count=len(missing), denominator_original_slots=5)
        panel.append(item)
    paired = {}
    for control in ("baseline", "matched"):
        values = [v["candidate"]["net_bps"]-v[control]["net_bps"] for v in panel if v["candidate"]["net_bps"] is not None and v[control]["net_bps"] is not None]
        changes = [sum(v["candidate"]["known_decisions"][k] != v[control]["known_decisions"][k]
                       for k in v["candidate"]["known_decisions"].keys() & v[control]["known_decisions"].keys()) for v in panel]
        interval = None
        if len(values) == len(panel) and len(values) >= 5:
            random, vector, boot = np.random.default_rng(20261006), np.array(values), []
            for _ in range(2000):
                starts = random.integers(0, len(vector), size=(len(vector)+4)//5)
                indices = ((starts[:, None]+np.arange(5)) % len(vector)).ravel()[:len(vector)]
                boot.append(float(vector[indices].mean()))
            interval = np.quantile(boot, [.025, .975]).tolist()
        paired[control] = dict(mean_5td_increment_bps=float(np.mean(values)) if values else None,
            complete_paired_days=len(values), unknown_paired_days=len(panel)-len(values), block5_ci95_bps=interval,
            known_intervention_decisions=sum(changes), known_intervention_days=sum(v > 0 for v in changes),
            intervention_days=sum(v["candidate"]["intervention_actions"] != v[control]["intervention_actions"] for v in panel),
            inference_use="DESCRIPTIVE_ONLY_OVERLAPPING_COHORTS")
    common = [v for v in panel if all(v[arm]["net_bps"] is not None for arm in ("candidate", "matched", "baseline", "rule"))]
    summary = {}
    for arm in ("candidate", "matched", "baseline", "rule"):
        outcomes = [y for v in panel for y in v[arm]["take_returns_bps"]]
        wins, losses = [y for y in outcomes if y > 0], [y for y in outcomes if y < 0]
        summary[arm] = dict(true_takes=len(outcomes), hit_rate=len(wins)/len(outcomes) if outcomes else None,
            average_profit_bps=float(np.mean(wins)) if wins else None, average_loss_bps=float(np.mean(losses)) if losses else None,
            mean_common_5td_cohort_bps=float(np.mean([v[arm]["net_bps"] for v in common])) if common else None,
            **{name+"_count": sum(v[arm][name] for v in panel) for name in ("unknown", "unsettled", "not_executable", "known_avoid")})
    complete = [v for v in panel if v["candidate"]["net_bps"] is not None and v["baseline"]["net_bps"] is not None]
    for item in complete:
        a = item["rejection_attribution"]
        if not np.isclose(item["candidate"]["net_bps"]-item["baseline"]["net_bps"], a["known_rejection_contribution_bps"]+a["unknown_cash_contribution_bps"], atol=1e-8):
            raise ValueError("return-volume known/UNKNOWN attribution does not reconcile")
    attribution = {k: float(np.mean([v["rejection_attribution"][k] for v in complete])) if complete else None
                   for k in ("avoided_loss_bps", "missed_profit_bps", "known_rejection_contribution_bps", "unknown_cash_contribution_bps")}
    attribution.update(complete_paired_cohorts=len(complete), unknown_cash_is_model_value=False, measure="MEAN_BPS_PER_5TD_COHORT_NOT_PER_DAY")
    return dict(cohorts=panel, paired_increments=paired, arm_summary=summary, rejection_attribution=attribution,
        common_complete_cohorts=len(common), measure="OVERLAPPING_5TD_COHORT_NOT_INVESTABLE_NAV", cumulative_nav=None,
        policy_sha256=POLICY_SHA256, schema_sha256=SCHEMA_SHA256, label_contract=POLICY["label_contract"], economic_confirmation=False, deployable=False)


def evaluate_return_volume_price_5td_study_v1(*, plan, output_root):
    plan, root, _, _, _ = _load(plan, output_root)
    trained = _stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        _record(plan, root, "evaluated")
        return root/"evaluated"
    body = json.loads((root/"trained"/"model.json").read_bytes())
    fitted = GenericReturnVolumePrice5TDFitV1(**{**body, "intervals_bps": tuple(tuple(v) for v in body["intervals_bps"])})
    control = _control(json.loads((root/"prepared"/"control_model.json").read_bytes()))
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    test = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.configuration.test_start), pd.Timestamp(plan.configuration.test_end))].reset_index(drop=True)
    dates = [v for v in plan.decision_dates if plan.configuration.test_start <= v <= plan.configuration.test_end]
    result = evaluate_return_volume_price_5td_cohorts_v1(rows=test, fitted=fitted, frozen_control=control, decision_dates=dates)
    publish_stage(study_root=root, stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"],
                  artifacts={"evaluation.json": _json_bytes(result)})
    _record(plan, root, "evaluated")
    return root/"evaluated"
