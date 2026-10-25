"""Small artifact GP5 workflow and non-investable overlapping cohort measurement."""
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
from backend.services.advisory_model_first.generic_daily_price_input_v1 import build_generic_daily_price_input_v1
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, POLICY, POLICY_SHA256, ROSTER, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.generic_price_5td_inference_v1 import query_price_nodes_v1
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import build_generic_price_5td_labels_v1, validate_roster
from backend.services.advisory_model_first.generic_price_5td_models_v1 import (
    GenericPrice5TDFitV1, train_generic_price_5td_v1,
)
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, EvidenceReferenceV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

INPUT_ROLES = {"calendar", "roster", "raw_daily", "volume_daily", "index_daily"}


def implementation_sha256():
    directory = Path(__file__).parent
    files = [directory / f"generic_price_5td_{name}_v1.py"
             for name in ("contracts", "labels", "models", "inference", "pipeline")]
    files += [directory/"generic_daily_price_input_v1.py", directory/"economic_price_campaign_models_v2.py"]
    return sha({path.name: file_sha256(path) for path in files})


class GenericPrice5TDPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    configuration: GenericPrice5TDConfigurationV1
    inputs: dict[str, EvidenceReferenceV1]
    dataset_identity: str
    parent_lineage: tuple[str, ...]
    decision_dates: tuple[date, ...]
    implementation_sha256: str
    source_evidence: str
    universe_identity: dict | str | None = None

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode="json"), "policy_sha256": POLICY_SHA256,
                    "study_type": "EXPLORATORY_SCREEN", "decision_use": "NAVIGATION_ONLY",
                    "data_scope": "PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY", "physical_fit_budget": 4})

    @property
    def experiment_id(self):
        return "advgp5_"+self.plan_sha256[:24]


def _load(plan, output_root):
    plan = GenericPrice5TDPlanV1.model_validate(plan)
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:" or declared != root:
        raise ValueError("GP5 output must be an explicit non-C real path")
    if (set(plan.inputs) != INPUT_ROLES or plan.implementation_sha256 != implementation_sha256()
            or not plan.dataset_identity or not plan.source_evidence or not plan.parent_lineage
            or not plan.decision_dates or list(plan.decision_dates) != sorted(set(plan.decision_dates))
            or plan.decision_dates[0] < plan.configuration.train_start
            or plan.decision_dates[-1] > plan.configuration.test_end):
        raise ValueError("GP5 plan input/code/scope identity differs")
    paths = {name: _verify_reference(ref) for name, ref in plan.inputs.items()}
    return plan, root/plan.experiment_id, paths


def _stage(plan, root, name):
    parent = None
    result = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        result = read_stage(root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = result["stage_sha256"]
        if stage == name:
            return result
    raise ValueError("unknown GP5 stage")


def _record(plan, root, stage):
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="exact_attempt_v1",
        research_stage=stage.upper(), study_type="EXPLORATORY_SCREEN", hypothesis_family_id="generic_price_5td_v1",
        parent_lineage=plan.parent_lineage, unique_variable="FIXED_5TD_PACKAGE_INDEPENDENT_VALUE",
        objective_contract="RISK_MANAGED_ADVISORY", dataset_identity=plan.dataset_identity,
        schema_identity="generic_daily_price_input_v1", policy_identity=POLICY_SHA256,
        planned_trial_count=1, generated_trial_count=int(stage in ("trained", "evaluated")),
        evaluated_trial_count=int(stage == "evaluated"), selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=plan.dataset_identity, start_date=plan.configuration.train_start,
            end_date=plan.configuration.label_cutoff),), result_class="EXPLORATORY",
        decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="gp5_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def preregister_generic_price_5td_v1(*, plan, output_root):
    plan, root, _ = _load(plan, output_root)
    publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json")),
                   "policy.json": _json_bytes(POLICY)})
    _record(plan, root, "preregistered")
    return root/"preregistered"/"plan.json"


def _parquet(frame):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def prepare_generic_price_5td_v1(*, plan, output_root):
    plan, root, paths = _load(plan, output_root)
    parent = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        _record(plan, root, "prepared")
        return root/"prepared"
    config = plan.configuration
    from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
    calendar = pd.DatetimeIndex(pd.to_datetime(json.loads(paths["calendar"].read_text(encoding="utf-8"))))
    if calendar.has_duplicates or calendar.hasnans or not calendar.is_monotonic_increasing:
        raise ValueError("GP5 frozen original calendar differs")
    calendar = pd.DatetimeIndex([pd.Timestamp(_day(value)) for value in calendar])
    calendar = calendar[calendar <= pd.Timestamp(config.label_cutoff)]
    import pyarrow.parquet as parquet
    names = parquet.read_schema(paths["roster"]).names
    legacy = "is_candidate_decision" in names
    fields = [*KEY, "selection_effective_rank"]+["is_candidate_decision"] if legacy else list(ROSTER)
    original = pd.read_parquet(paths["roster"], columns=fields)
    if legacy:
        # Exact historical parent projection, not a new Selection or expanded pool.
        if not original.is_candidate_decision.map(lambda value: type(value) is bool).all():
            raise ValueError("GP5 frozen legacy candidate flags must be explicit")
        original = original.loc[original.is_candidate_decision.eq(True) & original.selection_effective_rank.le(20)]
    roster = original.loc[original[KEY[0]].between(pd.Timestamp(config.train_start), pd.Timestamp(config.test_end))
                          ].copy()
    if legacy:
        roster["candidate_group_size"] = roster.groupby(KEY[0]).instrument.transform("size")
        roster = roster.sort_values([KEY[0], "selection_effective_rank"], kind="stable").reset_index(drop=True)
    roster = roster.loc[:, ROSTER]
    roster, _, _ = validate_roster(roster, plan.decision_dates, [day.date() for day in calendar])
    raw = pd.read_parquet(paths["raw_daily"])
    volumes = pd.read_parquet(paths["volume_daily"], columns=["trade_date", "instrument", "volume_hand"])
    indices = pd.read_parquet(paths["index_daily"], columns=["trade_date", "instrument", "close"])
    keys = ["trade_date", "instrument"]
    if any(frame.duplicated(keys).any() for frame in (raw, volumes, indices)):
        raise ValueError("GP5 frozen source has duplicate stock/date keys")
    if any(not raw[name].map(lambda value: type(value) is bool).all()
           for name in ("suspended", "tradability_unknown")):
        raise ValueError("GP5 frozen tradability states must be explicit")
    if len(raw) > 500000 or len(volumes) > 500000 or len(indices) > 5000:
        raise ValueError("GP5 frozen source exceeds bounded input rows")
    raw["adj_factor"] = raw.adj_factor.map(lambda value: _number(value, positive=True))
    raw = raw.merge(volumes, on=keys, how="left", validate="one_to_one").set_index(keys).sort_index()
    features, price_rows, refs, decisions, cold = [], [], [], list(plan.decision_dates), 0
    for d, group in roster.groupby(KEY[0], sort=True):
        d = pd.Timestamp(d)
        group = group.sort_values("selection_effective_rank").reset_index(drop=True)
        position = calendar.get_indexer([d])[0]
        if position < 0 or position+1 >= len(calendar):
            raise ValueError("GP5 original candidate date has no immediate target")
        panels = []
        for row in group.to_dict("records"):
            symbol = row["instrument"]
            original_d = raw.loc[(d, symbol)] if (d, symbol) in raw.index else None
            factor = None if original_d is None else original_d.adj_factor
            factor = float(factor) if pd.notna(factor) else None
            refs.append({**{k: row[k] for k in KEY},
                         "reference_cny": None if original_d is None else original_d.raw_close_cny,
                         "reference_visible_through": d.date()})
            if position >= 19:
                for day in calendar[position-19:position+1]:
                    if (day, symbol) not in raw.index:
                        continue
                    quote = raw.loc[(day, symbol)]
                    scale = None if factor is None or pd.isna(quote.adj_factor) else float(quote.adj_factor)/factor
                    panels.append(dict(trade_date=day, instrument=symbol,
                        **{name: None if scale is None else quote["raw_"+name+"_cny"]*scale
                           for name in ("open", "high", "low", "close")}, volume=quote.volume_hand*100))
            for day in calendar[position+1:position+6]:
                if (day, symbol) not in raw.index:
                    continue
                quote = raw.loc[(day, symbol)]
                price_rows.append(dict(decision_as_of_trade_date=d, trade_date=day, instrument=symbol,
                    **{name: quote["raw_"+name+"_cny"] for name in ("open", "high", "low", "close")},
                    d_anchor_factor=None if factor is None or pd.isna(quote.adj_factor) else float(quote.adj_factor)/factor,
                    suspended=quote.suspended, tradability_unknown=quote.tradability_unknown,
                    up_limit=quote.up_limit, down_limit=quote.down_limit))
        if position < 19:
            cold += len(group)
            block = group.loc[:, ROSTER].copy()
            for name in FEATURES:
                block[name] = np.nan
        else:
            day_calendar = calendar[position-19:position+2]
            bench = indices.loc[indices.trade_date.isin(day_calendar[:-1])]
            panel = pd.DataFrame(panels, columns=["trade_date", "instrument", "open", "high", "low", "close", "volume"])
            block, _ = build_generic_daily_price_input_v1(candidates=group.loc[:, ROSTER],
                calendar=[day.date() for day in day_calendar], panel=panel, benchmark_daily=bench,
                market_state=dict(trade_date=d.date(), market_up_ratio=None, market_definition_id=None, visible_through=None),
                source_context=dict(package_id=None, run_id=None, list_version_id=None, universe_identity=plan.universe_identity,
                    source_evidence=plan.source_evidence, price_basis="D_ADJUSTED_CNY", volume_basis="RAW_SHARES",
                    source_visible_through=d.date(), benchmark_visible_through=d.date()))
        features.append(block)
    if not features:
        raise ValueError("GP5 study original frozen candidate input is empty")
    from backend.services.advisory_model_first.generic_price_5td_labels_v1 import PRICE_FIELDS, REFERENCE_FIELDS
    labels, receipt = build_generic_price_5td_labels_v1(candidates=roster.loc[:, ROSTER], decision_dates=decisions,
        calendar=[day.date() for day in calendar], prices=pd.DataFrame(price_rows, columns=PRICE_FIELDS),
        references=pd.DataFrame(refs, columns=REFERENCE_FIELDS),
        source_context=dict(calendar_sha256=plan.inputs["calendar"].sha256, prices_sha256=plan.inputs["raw_daily"].sha256,
            references_sha256=plan.inputs["raw_daily"].sha256, label_price_basis="D_REFERENCE_POLICY_RATIO",
            source_evidence=plan.source_evidence))
    feature_frame = pd.concat(features, ignore_index=True)
    rows = labels.merge(feature_frame.loc[:, [*KEY, *FEATURES]], on=list(KEY), how="left", validate="one_to_one", sort=False)
    if len(rows) != len(roster):
        raise ValueError("GP5 preparation changed the original candidate count")
    receipt.update(cold_start_candidates=cold, database_written=False, selection_regenerated=False)
    for ref in plan.inputs.values():
        _verify_reference(ref)
    publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"],
        artifacts={"rows.parquet": _parquet(rows), "receipt.json": _json_bytes(receipt)})
    _record(plan, root, "prepared")
    return root/"prepared"


def train_generic_price_5td_study_v1(*, plan, output_root, qe_training_idle):
    plan, root, _ = _load(plan, output_root)
    parent = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        _record(plan, root, "trained")
        return root/"trained"
    if qe_training_idle is not True:
        raise ValueError("GP5 fit waits for a fresh QE idle check")
    marker = root/"fit_attempt.json"
    with marker.open("xb") as handle:
        handle.write(_json_bytes(dict(status="STARTED", physical_fit_budget=4,
                                      started_at=datetime.now(timezone.utc).isoformat())))
    count = 0
    def before_fit(name):
        nonlocal count
        count += 1
        if count > 4:
            raise ValueError("GP5 physical fit budget exceeded")
        with (root/"fit_journal.jsonl").open("ab") as handle:
            handle.write((json.dumps(dict(head=name, kind="PHYSICAL_FIT", ordinal=count),
                                    sort_keys=True, allow_nan=False)+"\n").encode("utf-8"))
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    fitted = train_generic_price_5td_v1(rows=rows, configuration=plan.configuration, before_fit=before_fit)
    publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"],
        artifacts={"model.json": _json_bytes(dict(recipe=fitted.recipe, models=fitted.models, intervals_bps=fitted.intervals_bps,
                                                 diagnostics=fitted.diagnostics, model_sha256=fitted.model_sha256))})
    _record(plan, root, "trained")
    return root/"trained"


def evaluate_generic_price_5td_cohorts_v1(*, rows, fitted, decision_dates):
    arm_states = {arm: query_price_nodes_v1(fitted=fitted, features=rows,
                 scenario_gap_bps=rows.observed_gap_bps, arm=arm) for arm in ARMS}
    panel = []
    for d in decision_dates:
        group = rows.loc[rows[KEY[0]].eq(pd.Timestamp(d)) & rows.selection_effective_rank.le(5)]
        item = {KEY[0]: str(pd.Timestamp(d).date())}
        for arm in ("baseline", "rule", *ARMS):
            values, take, unknown, unsettled, skipped, actions, returns = [], 0, 0, 0, 0, [], []
            for index, row in group.iterrows():
                if row.label_status == "ENTRY_NOT_EXECUTABLE":
                    values.append(0.)
                    skipped += 1
                    continue
                if arm == "baseline":
                    action = True
                elif arm == "rule":
                    action = pd.notna(row.observed_gap_bps) and abs(row.observed_gap_bps) <= 300
                else:
                    state = arm_states[arm].loc[index, "status"]
                    unknown += int(state == "UNKNOWN_INPUT_OR_SUPPORT")
                    action = state == "ACCEPTABLE"
                if action:
                    actions.append(row.instrument)
                if not action:
                    values.append(0.)
                    skipped += 1
                elif row.label_status != "AVAILABLE":
                    values.append(None)
                    unsettled += 1
                else:
                    value = 10000*(row.gross_terminal_ratio*(1-POLICY["sell_bps"]/10000)/
                                  ((1+row.observed_gap_bps/10000)*(1+POLICY["buy_bps"]/10000))-1)
                    values.append(float(value))
                    returns.append(float(value))
                    take += 1
            item[arm] = dict(net_bps=None if any(v is None for v in values) else sum(values)/5,
                             true_take=take, unknown=unknown, unsettled=unsettled, cash_slots=5-take-unsettled,
                             not_executable_or_skipped=skipped, profitable_takes=sum(v > 0 for v in returns),
                             take_returns_bps=returns, intervention_actions=actions)
        panel.append(item)
    paired = {}
    for control in ("matched", "baseline"):
        differences = [(item["candidate"]["net_bps"]-item[control]["net_bps"])
                       for item in panel if item["candidate"]["net_bps"] is not None and item[control]["net_bps"] is not None]
        unknown_days = len(panel)-len(differences)
        paired[control] = dict(mean_5td_increment_bps=float(np.mean(differences)) if differences else None,
                               complete_paired_days=len(differences), unknown_paired_days=unknown_days,
                               intervention_days=sum(item["candidate"]["intervention_actions"] != item[control]["intervention_actions"]
                                                     for item in panel),
                               block5_ci95_bps=None, inference_use="DESCRIPTIVE_ONLY_OVERLAPPING_COHORTS")
        # Missing dates are not compressed into supposedly consecutive bootstrap blocks.
        if len(differences) >= 5 and not unknown_days:
            vector = np.asarray(differences)
            random = np.random.default_rng(20261006)
            boot = []
            for _ in range(2000):
                starts = random.integers(0, len(vector), size=(len(vector)+4)//5)
                indices = (starts[:, None]+np.arange(5)) % len(vector)
                boot.append(float(vector[indices.ravel()[:len(vector)]].mean()))
            paired[control]["block5_ci95_bps"] = np.quantile(boot, [.025, .975]).tolist()
    arm_summary = {}
    for arm in ("baseline", "rule", *ARMS):
        values = [value for item in panel for value in item[arm]["take_returns_bps"]]
        positive, negative = [v for v in values if v > 0], [v for v in values if v < 0]
        arm_summary[arm] = dict(true_takes=len(values), hit_rate=len(positive)/len(values) if values else None,
            average_profit_bps=float(np.mean(positive)) if positive else None,
            average_loss_bps=float(np.mean(negative)) if negative else None,
            unknown_count=sum(item[arm]["unknown"] for item in panel),
            unsettled_count=sum(item[arm]["unsettled"] for item in panel))
    return dict(policy_sha256=POLICY_SHA256, label_contract=POLICY["label_contract"], cohorts=panel,
                paired_increments=paired, arm_summary=arm_summary,
                measure="OVERLAPPING_5TD_COHORT_NOT_INVESTABLE_NAV", cumulative_nav=None,
                economic_confirmation=False, deployable=False)


def evaluate_generic_price_5td_study_v1(*, plan, output_root):
    plan, root, _ = _load(plan, output_root)
    parent = _stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        _record(plan, root, "evaluated")
        return root/"evaluated"
    body = json.loads((root/"trained"/"model.json").read_text(encoding="utf-8"))
    fitted = GenericPrice5TDFitV1(**{**body, "intervals_bps": tuple(tuple(v) for v in body["intervals_bps"])})
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    mask = rows[KEY[0]].between(pd.Timestamp(plan.configuration.test_start), pd.Timestamp(plan.configuration.test_end))
    test = rows.loc[mask].reset_index(drop=True)
    receipt = json.loads((root/"prepared"/"receipt.json").read_text(encoding="utf-8"))
    dates = [day for day in receipt["decision_dates"] if plan.configuration.test_start.isoformat() <= day <= plan.configuration.test_end.isoformat()]
    result = evaluate_generic_price_5td_cohorts_v1(rows=test, fitted=fitted, decision_dates=dates)
    publish_stage(study_root=root, stage="evaluated", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"],
                  artifacts={"evaluation.json": _json_bytes(result)})
    _record(plan, root, "evaluated")
    return root/"evaluated"
