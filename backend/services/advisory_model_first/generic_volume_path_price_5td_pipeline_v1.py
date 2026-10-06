"""New immutable minute-info study; consumes the completed GP5 prepared labels only."""
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
from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import (
    ARMS, KEY, MINUTE_FEATURES, POLICY, POLICY_SHA256, SCHEMA_SHA256, GenericPrice5TDConfigurationV1, MinuteSourceIdentityV1,
)
from backend.services.advisory_model_first.generic_volume_path_price_5td_model_v1 import query_volume_path_nodes_v1
from backend.services.advisory_model_first.generic_volume_path_price_5td_model_v1 import (
    GenericVolumePathPrice5TDFitV1, train_generic_volume_path_price_5td_v1,
)
from backend.services.advisory_model_first.generic_volume_path_price_5td_source_v1 import read_d_volume_path_features_v1
from backend.services.advisory_model_first.generic_price_5td_pipeline_v1 import GenericPrice5TDPlanV1
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, EvidenceReferenceV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def implementation_sha256():
    directory = Path(__file__).parent
    files = [directory/f"generic_volume_path_price_5td_{name}_v1.py"
             for name in ("contracts", "source", "model", "pipeline")]
    files += [directory/f"generic_price_5td_{name}_v1.py" for name in ("contracts", "models", "pipeline")]
    files += [directory/"generic_daily_price_input_v1.py", directory/"economic_price_campaign_models_v2.py",
              directory/"generic_minute_price_5td_contracts_v1.py", directory/"generic_minute_price_5td_source_v1.py"]
    return sha({path.name: file_sha256(path) for path in files})


class GenericVolumePathPrice5TDPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    configuration: GenericPrice5TDConfigurationV1
    gp5_plan_ref: EvidenceReferenceV1
    gp5_prepared_ref: EvidenceReferenceV1
    minute_identity: MinuteSourceIdentityV1
    active_profile_path: str
    dataset_identity: str
    parent_lineage: tuple[str, ...]
    decision_dates: tuple[date, ...]
    implementation_sha256: str

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode="json"), "schema_sha256": SCHEMA_SHA256, "policy_sha256": POLICY_SHA256,
                    "study_type": "EXPLORATORY_SCREEN", "decision_use": "NAVIGATION_ONLY", "physical_fit_budget": 4})

    @property
    def experiment_id(self):
        return "advgp5volume_"+self.plan_sha256[:24]


def _load(plan, output_root):
    plan = GenericVolumePathPrice5TDPlanV1.model_validate(plan)
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:" or declared != root:
        raise ValueError("minute study output requires an explicit non-C real path")
    if (plan.implementation_sha256 != implementation_sha256() or not plan.parent_lineage or not plan.dataset_identity
            or not plan.decision_dates or list(plan.decision_dates) != sorted(set(plan.decision_dates))
            or plan.decision_dates[0] < plan.configuration.train_start or plan.decision_dates[-1] > plan.configuration.test_end):
        raise ValueError("minute study code/window/lineage identity differs")
    path = _verify_reference(plan.gp5_plan_ref)
    manifest = _verify_reference(plan.gp5_prepared_ref)
    parent = GenericPrice5TDPlanV1.model_validate_json(path.read_bytes())
    preregistered = read_stage(path.parent, stage="preregistered", plan_sha256=parent.plan_sha256, parent_sha256=None)
    prepared = read_stage(manifest.parent, stage="prepared", plan_sha256=parent.plan_sha256,
                          parent_sha256=preregistered["stage_sha256"])
    if (plan.configuration != parent.configuration or plan.decision_dates != parent.decision_dates
            or plan.dataset_identity != parent.dataset_identity or parent.experiment_id not in plan.parent_lineage
            or path.parent.name != "preregistered" or manifest.name != "manifest.json"
            or manifest.parent != path.parent.parent/"prepared"):
        raise ValueError("minute study must consume exactly its unchanged GP5 prepared population")
    return plan, root/plan.experiment_id, manifest.parent, prepared


def _stage(plan, root, name):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        result = read_stage(root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = result["stage_sha256"]
        if stage == name:
            return result
    raise ValueError("unknown minute study stage")


def _record(plan, root, stage):
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="exact_attempt_v1",
        research_stage=stage.upper(), study_type="EXPLORATORY_SCREEN", hypothesis_family_id="generic_volume_path_price_5td_v1",
        parent_lineage=plan.parent_lineage, unique_variable="D_OHLC_VOLUME_PATH_BLOCK_ONLY_FIXED_5TD",
        objective_contract="RISK_MANAGED_ADVISORY", dataset_identity=plan.dataset_identity,
        schema_identity=SCHEMA_SHA256, policy_identity=POLICY_SHA256, planned_trial_count=1,
        generated_trial_count=int(stage in ("trained", "evaluated")), evaluated_trial_count=int(stage == "evaluated"),
        selected_trial_count=0, consumed_windows=(ConsumedWindowV1(window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=plan.dataset_identity, start_date=plan.configuration.train_start,
            end_date=plan.configuration.label_cutoff),), result_class="EXPLORATORY", decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="gp5_minute_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def preregister_volume_path_price_5td_v1(*, plan, output_root):
    plan, root, _, _ = _load(plan, output_root)
    publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json")), "policy.json": _json_bytes(POLICY)})
    _record(plan, root, "preregistered")
    return root/"preregistered"/"plan.json"


def prepare_volume_path_price_5td_v1(*, plan, output_root):
    plan, root, parent_root, parent_manifest = _load(plan, output_root)
    first = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        _record(plan, root, "prepared")
        return root/"prepared"
    parent_rows = pd.read_parquet(parent_root/"rows.parquet")
    parent_receipt = json.loads((parent_root/"receipt.json").read_bytes())
    if (parent_receipt["decision_dates"] != [v.isoformat() for v in plan.decision_dates]
            or not parent_rows.policy_sha256.eq(POLICY_SHA256).all()
            or not parent_rows.label_contract.eq(POLICY["label_contract"]).all()):
        raise ValueError("minute study parent GP5 policy/calendar differs")
    minute, source = read_d_volume_path_features_v1(roster=parent_rows.loc[:, KEY], identity=plan.minute_identity,
                                              active_profile_path=plan.active_profile_path)
    rows = parent_rows.merge(minute, on=list(KEY), how="left", sort=False, validate="one_to_one")
    # <=7720 unique KEY per side; one-to-one left join cannot explode the original population.
    if len(rows) != len(parent_rows) or not rows.loc[:, KEY].equals(parent_rows.loc[:, KEY]):
        raise ValueError("minute study changed original roster/order")
    # Verify the parent once more after all reads; do not reload/modify daily data.
    _load(plan, output_root)
    buffer = io.BytesIO()
    rows.to_parquet(buffer, index=False)
    receipt = dict(parent_gp5_stage_sha256=parent_manifest["stage_sha256"], original_rows=len(rows),
        decision_dates=parent_receipt["decision_dates"], minute_source_identity=source["identity"],
        minute_rows_full_ohlc=source["rows_with_full_ohlc"],
        minute_rows_known_features=int(minute.loc[:, MINUTE_FEATURES].notna().all(axis=1).sum()),
        minute_coverage_reasons=minute.minute_reason.value_counts().to_dict(), source_evidence=source["source_evidence"],
        volume_rows_all_known=int(minute.loc[:, list(MINUTE_FEATURES[-3:])].notna().all(axis=1).sum()),
        volume_coverage_reasons=minute.volume_path_reason.value_counts().to_dict(),
        future_price_bars_decoded=0, database_written=False, selection_regenerated=False,
        active_profile_required_after_prepare=False)
    publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=first["stage_sha256"],
        artifacts={"rows.parquet": buffer.getvalue(), "receipt.json": _json_bytes(receipt), "minute_slices.json": _json_bytes(source)})
    _record(plan, root, "prepared")
    return root/"prepared"


def train_volume_path_price_5td_study_v1(*, plan, output_root, qe_training_idle):
    plan, root, _, _ = _load(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        _record(plan, root, "trained")
        return root/"trained"
    if qe_training_idle is not True:
        raise ValueError("minute study fit waits for a fresh QE idle check")
    with (root/"fit_attempt.json").open("xb") as stream:
        stream.write(_json_bytes(dict(status="STARTED", physical_fit_budget=4, started_at=datetime.now(timezone.utc).isoformat())))
    count = 0
    def before_fit(name):
        nonlocal count
        count += 1
        if count > 4:
            raise ValueError("minute study physical fit budget exceeded")
        with (root/"fit_journal.jsonl").open("ab") as stream:
            stream.write((json.dumps(dict(head=name, kind="PHYSICAL_FIT", ordinal=count), allow_nan=False)+"\n").encode())
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    fitted = train_generic_volume_path_price_5td_v1(rows=rows, configuration=plan.configuration, before_fit=before_fit)
    publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"],
                  artifacts={"model.json": _json_bytes(asdict(fitted))})
    _record(plan, root, "trained")
    return root/"trained"


def evaluate_volume_path_price_5td_cohorts_v1(*, rows, fitted, decision_dates):
    dates = pd.DatetimeIndex(pd.to_datetime(decision_dates))
    required = {*KEY, "selection_effective_rank", "candidate_group_size", "label_status", "observed_gap_bps", "gross_terminal_ratio"}
    if (not isinstance(rows, pd.DataFrame) or len(rows) > 7720 or not required.issubset(rows.columns)
            or not rows.index.is_unique or rows.duplicated([KEY[0], "instrument"]).any()
            or len(dates) > 386 or dates.hasnans or dates.duplicated().any() or not dates.is_monotonic_increasing
            or not rows[KEY[0]].isin(dates).all()):
        raise ValueError("minute cohort original roster/full decision schedule differs")
    for _, group in rows.groupby(KEY[0], sort=False):
        if (len(group) > 50 or group.selection_effective_rank.tolist() != list(range(1, len(group)+1))
                or not group.selection_effective_rank.map(lambda v: isinstance(v, (int, np.integer)) and not isinstance(v, (bool, np.bool_))).all()
                or not group.candidate_group_size.map(lambda v: isinstance(v, (int, np.integer)) and not isinstance(v, (bool, np.bool_))).all()
                or not group.candidate_group_size.eq(len(group)).all() or group[KEY[1]].nunique() != 1
                or not group[KEY[1]].gt(group[KEY[0]]).all()):
            raise ValueError("minute cohort full original ranks/group/T identity differs")
    states = {arm: query_volume_path_nodes_v1(fitted=fitted, features=rows, scenario_gap_bps=rows.observed_gap_bps, arm=arm)
              for arm in ARMS}
    panel = []
    for d in dates:
        group = rows.loc[rows[KEY[0]].eq(pd.Timestamp(d)) & rows.selection_effective_rank.le(5)]
        settled = {}
        for _, row in group.loc[group.label_status.eq("AVAILABLE")].iterrows():
            if (not np.isfinite([row.gross_terminal_ratio, row.observed_gap_bps]).all()
                    or row.gross_terminal_ratio <= 0 or row.observed_gap_bps <= -10000):
                raise ValueError("volume study AVAILABLE settlement contradicts price coordinates")
            settled[row.instrument] = float(10000*(row.gross_terminal_ratio*(1-POLICY["sell_bps"]/10000)/
                ((1+row.observed_gap_bps/10000)*(1+POLICY["buy_bps"]/10000))-1))
        item = {KEY[0]: str(pd.Timestamp(d).date()), "settled_returns_bps": settled}
        for arm in ("baseline", "rule", *ARMS):
            values, returns, actions, known, unknown, unsettled, not_executable, avoided = [], [], [], {}, 0, 0, 0, 0
            for index, row in group.iterrows():
                if row.label_status == "ENTRY_NOT_EXECUTABLE":
                    values.append(0.)
                    not_executable += 1
                    continue
                if arm == "baseline":
                    action = True
                    known[row.instrument] = action
                elif arm == "rule":
                    action = pd.notna(row.observed_gap_bps) and abs(row.observed_gap_bps) <= 300
                    if pd.notna(row.observed_gap_bps):
                        known[row.instrument] = bool(action)
                    else:
                        unknown += 1
                else:
                    state = states[arm].loc[index, "status"]
                    unknown += int(state == "UNKNOWN_INPUT_OR_SUPPORT")
                    action = state == "ACCEPTABLE"
                    if state != "UNKNOWN_INPUT_OR_SUPPORT":
                        known[row.instrument] = action
                avoided += int(row.instrument in known and not action)
                if action:
                    actions.append(row.instrument)
                if not action:
                    values.append(0.)
                elif row.label_status != "AVAILABLE":
                    values.append(None)
                    unsettled += 1
                else:
                    value = float(10000*(row.gross_terminal_ratio*(1-POLICY["sell_bps"]/10000)/
                        ((1+row.observed_gap_bps/10000)*(1+POLICY["buy_bps"]/10000))-1))
                    if not np.isfinite(value):
                        raise ValueError("minute study AVAILABLE settlement is nonfinite")
                    values.append(value)
                    returns.append(value)
            item[arm] = dict(net_bps=None if any(value is None for value in values) else sum(values)/5,
                true_take=len(returns), unknown=unknown, unsettled=unsettled, cash_slots=5-len(returns)-unsettled,
                not_executable=not_executable, known_avoid=avoided, known_decisions=known,
                take_returns_bps=returns, intervention_actions=actions)
        rejection = [instrument for instrument, action in item["candidate"]["known_decisions"].items()
                     if not bool(action) and item["baseline"]["known_decisions"].get(instrument) is True]
        unknown_cash = [instrument for instrument in item["baseline"]["known_decisions"]
                        if instrument not in item["candidate"]["known_decisions"]]
        avoided_loss = sum(-settled[instrument] for instrument in rejection
                           if instrument in settled and settled[instrument] < 0)/5
        missed_profit = sum(settled[instrument] for instrument in rejection
                            if instrument in settled and settled[instrument] > 0)/5
        item["rejection_attribution"] = dict(avoided_loss_bps=avoided_loss, missed_profit_bps=missed_profit,
            known_rejection_contribution_bps=avoided_loss-missed_profit,
            unknown_cash_contribution_bps=-sum(settled[instrument] for instrument in unknown_cash if instrument in settled)/5,
            unresolved_rejection_count=sum(instrument not in settled for instrument in rejection),
            unknown_cash_count=len(unknown_cash), denominator_original_slots=5)
        panel.append(item)
    paired = {}
    for control in ("matched", "baseline"):
        differences = [item["candidate"]["net_bps"]-item[control]["net_bps"] for item in panel
                       if item["candidate"]["net_bps"] is not None and item[control]["net_bps"] is not None]
        unknown_days = len(panel)-len(differences)
        paired[control] = dict(mean_5td_increment_bps=float(np.mean(differences)) if differences else None,
            complete_paired_days=len(differences), unknown_paired_days=unknown_days,
            intervention_days=sum(item["candidate"]["intervention_actions"] != item[control]["intervention_actions"] for item in panel),
            block5_ci95_bps=None, inference_use="DESCRIPTIVE_ONLY_OVERLAPPING_COHORTS")
        known_changes = [sum(item["candidate"]["known_decisions"][instrument] != item[control]["known_decisions"][instrument]
                             for instrument in item["candidate"]["known_decisions"].keys() & item[control]["known_decisions"].keys())
                         for item in panel]
        paired[control].update(known_intervention_decisions=sum(known_changes),
            known_intervention_days=sum(count > 0 for count in known_changes),
            unsupported_only_action_difference_days=sum(not count and item["candidate"]["intervention_actions"] != item[control]["intervention_actions"]
                                                        for count, item in zip(known_changes, panel, strict=True)))
        if len(differences) >= 5 and not unknown_days:
            vector, random, boot = np.asarray(differences), np.random.default_rng(20261006), []
            for _ in range(2000):
                starts = random.integers(0, len(vector), size=(len(vector)+4)//5)
                indices = (starts[:, None]+np.arange(5)) % len(vector)
                boot.append(float(vector[indices.ravel()[:len(vector)]].mean()))
            paired[control]["block5_ci95_bps"] = np.quantile(boot, [.025, .975]).tolist()
    summary = {}
    for arm in ("baseline", "rule", *ARMS):
        returns = [value for item in panel for value in item[arm]["take_returns_bps"]]
        positive, negative = [v for v in returns if v > 0], [v for v in returns if v < 0]
        summary[arm] = dict(true_takes=len(returns), hit_rate=len(positive)/len(returns) if returns else None,
            average_profit_bps=float(np.mean(positive)) if positive else None, average_loss_bps=float(np.mean(negative)) if negative else None,
            unknown_count=sum(item[arm]["unknown"] for item in panel), unsettled_count=sum(item[arm]["unsettled"] for item in panel))
        summary[arm].update(not_executable_count=sum(item[arm]["not_executable"] for item in panel),
                            known_avoid_count=sum(item[arm]["known_avoid"] for item in panel))
    complete = [item for item in panel if item["baseline"]["net_bps"] is not None and item["candidate"]["net_bps"] is not None]
    attribution = {name: float(np.mean([item["rejection_attribution"][name] for item in complete])) if complete else None
                   for name in ("avoided_loss_bps", "missed_profit_bps", "known_rejection_contribution_bps",
                                "unknown_cash_contribution_bps")}
    for item in complete:
        total = (item["rejection_attribution"]["known_rejection_contribution_bps"]
                 + item["rejection_attribution"]["unknown_cash_contribution_bps"])
        if not np.isclose(item["candidate"]["net_bps"]-item["baseline"]["net_bps"], total, atol=1e-8):
            raise ValueError("volume study rejected-slot attribution does not reconcile with baseline")
    attribution.update(complete_paired_cohorts=len(complete), measure="MEAN_BPS_PER_5TD_COHORT_NOT_PER_DAY",
                       unknown_cash_is_model_value=False)
    return dict(rejection_attribution=attribution, policy_sha256=POLICY_SHA256, schema_sha256=SCHEMA_SHA256, label_contract=POLICY["label_contract"],
        cohorts=panel, paired_increments=paired, arm_summary=summary, measure="OVERLAPPING_5TD_COHORT_NOT_INVESTABLE_NAV",
        cumulative_nav=None, economic_confirmation=False, deployable=False)


def evaluate_volume_path_price_5td_study_v1(*, plan, output_root):
    plan, root, _, _ = _load(plan, output_root)
    trained = _stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        _record(plan, root, "evaluated")
        return root/"evaluated"
    body = json.loads((root/"trained"/"model.json").read_bytes())
    fitted = GenericVolumePathPrice5TDFitV1(**{**body, "intervals_bps": tuple(tuple(value) for value in body["intervals_bps"])})
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    test = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.configuration.test_start), pd.Timestamp(plan.configuration.test_end))].reset_index(drop=True)
    dates = [value for value in plan.decision_dates if plan.configuration.test_start <= value <= plan.configuration.test_end]
    result = evaluate_volume_path_price_5td_cohorts_v1(rows=test, fitted=fitted, decision_dates=dates)
    publish_stage(study_root=root, stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"],
                  artifacts={"evaluation.json": _json_bytes(result)})
    _record(plan, root, "evaluated")
    return root/"evaluated"
