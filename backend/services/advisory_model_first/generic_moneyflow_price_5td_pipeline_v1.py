"""One immutable development-only candidate; read-only parents, no QE submission."""
from dataclasses import asdict
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, _verify_reference, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_moneyflow_price_5td_contracts_v1 import (
    INTERVENTION_SUPPORT, KEY, MONEYFLOW_FEATURES, POLICY, POLICY_SHA256, ROSTER, SCHEMA_SHA256,
    STATISTICS, GenericMoneyflowPrice5TDPlanV1,
)
from backend.services.advisory_model_first.generic_moneyflow_price_5td_models_v1 import (
    GenericMoneyflowPrice5TDFitV1, query_moneyflow_nodes_v1, train_moneyflow_price_5td_v1, validate_fit,
)
from backend.services.advisory_model_first.generic_moneyflow_price_5td_source_v1 import read_moneyflow_development_v1
from backend.services.advisory_model_first.generic_price_5td_inference_v1 import query_price_nodes_v1
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import validate_roster
from backend.services.advisory_model_first.generic_price_5td_models_v1 import GenericPrice5TDFitV1, validate_fit as validate_control
from backend.services.advisory_model_first.generic_price_5td_pipeline_v1 import GenericPrice5TDPlanV1
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

RESOURCE_LIMIT_BYTES = 2*1024**3
FIT_LIMIT_SECONDS = 1800
QE_PATHS = ("single", "custom_evo", "multi_alpha")


def implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_moneyflow_price_5td_{name}_v1.py" for name in ("contracts", "source", "models", "pipeline")]
    names += [f"generic_price_5td_{name}_v1.py" for name in ("contracts", "labels", "models", "inference", "pipeline")]
    names += ["generic_daily_price_input_v1.py", "economic_price_campaign_models_v2.py", "economic_entry_pipeline.py",
              "research_control.py", "research_control_contracts.py", "economic_value_anchor_contracts_v1.py"]
    return sha({name: file_sha256(directory/name) for name in names})


def _control(body):
    fitted = GenericPrice5TDFitV1(**{**body, "intervals_bps": tuple(tuple(value) for value in body["intervals_bps"])})
    validate_control(fitted)
    return fitted


def _fitted(body):
    fitted = GenericMoneyflowPrice5TDFitV1(**{**body, "intervals_bps": tuple(tuple(value) for value in body["intervals_bps"])})
    validate_fit(fitted)
    return fitted


def _load(plan, output_root):
    plan = GenericMoneyflowPrice5TDPlanV1.model_validate(plan)
    declared, root = Path(output_root), Path(output_root).resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:" or declared != root:
        raise ValueError("moneyflow study requires explicit non-C real output root")
    if (plan.implementation_sha256 != implementation_sha256() or not plan.dataset_identity or not plan.parent_lineage
            or not plan.decision_dates or list(plan.decision_dates) != sorted(set(plan.decision_dates))):
        raise ValueError("moneyflow code/window/lineage identity differs")
    parent_file = _verify_reference(plan.gp5_plan_ref)
    parent = GenericPrice5TDPlanV1.model_validate_json(parent_file.read_bytes())
    digest, stages = None, {}
    for name, reference in (("preregistered", plan.gp5_plan_ref), ("prepared", plan.gp5_prepared_ref), ("trained", plan.gp5_trained_ref)):
        path = _verify_reference(reference)
        if path.parent != parent_file.parent.parent/name or (name != "preregistered" and path.name != "manifest.json"):
            raise ValueError("moneyflow parent GP5 stage reference differs")
        stages[name] = read_stage(path.parent, stage=name, plan_sha256=parent.plan_sha256, parent_sha256=digest)
        digest = stages[name]["stage_sha256"]
    expected_dates = tuple(day for day in parent.decision_dates
                           if plan.configuration.train_start <= day <= plan.configuration.train_end
                           or plan.configuration.validation_start <= day <= plan.configuration.validation_end)
    if (plan.configuration != parent.configuration or plan.decision_dates != expected_dates
            or plan.dataset_identity != parent.dataset_identity or parent.experiment_id not in plan.parent_lineage):
        raise ValueError("moneyflow original development population/configuration differs")
    funding_file = _verify_reference(plan.moneyflow_prepared_ref)
    header = json.loads(funding_file.read_bytes())
    if funding_file.name != "manifest.json":
        raise ValueError("moneyflow frozen prepared manifest reference differs")
    funding_stage = read_stage(funding_file.parent, stage="prepared", plan_sha256=header["plan_sha256"],
                               parent_sha256=header["parent_sha256"])
    return plan, root/plan.experiment_id, parent, parent_file.parent.parent, funding_file.parent, stages, funding_stage


def _stage(plan, root, until):
    digest = None
    for name in ("preregistered", "prepared", "trained", "evaluated"):
        result = read_stage(root/name, stage=name, plan_sha256=plan.plan_sha256, parent_sha256=digest)
        digest = result["stage_sha256"]
        if name == until:
            return result
    raise ValueError("unknown moneyflow study stage")


def _record(plan, root, stage):
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="exact_attempt_v1",
        research_stage=stage.upper(), study_type="EXPLORATORY_SCREEN", hypothesis_family_id="generic_moneyflow_price_5td_v1",
        parent_lineage=plan.parent_lineage, unique_variable="GP5-MONEYFLOW-INFO-1",
        objective_contract="RISK_MANAGED_ADVISORY", dataset_identity=plan.dataset_identity,
        schema_identity=SCHEMA_SHA256, policy_identity=POLICY_SHA256, planned_trial_count=1,
        generated_trial_count=int(stage in ("trained", "evaluated")), evaluated_trial_count=int(stage == "evaluated"),
        selected_trial_count=0, consumed_windows=(ConsumedWindowV1(window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=plan.dataset_identity, start_date=plan.configuration.train_start,
            end_date=plan.configuration.validation_end),), result_class="EXPLORATORY", decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(root/stage/"manifest.json", role="gp5_moneyflow_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _publish(plan, root, stage, parent, artifacts):
    existing = 0
    for name in ("preregistered", "prepared", "trained", "evaluated"):
        if name != stage and (root/name/"manifest.json").exists():
            header = json.loads((root/name/"manifest.json").read_bytes())
            existing += sum(body["size_bytes"] for body in header["files"].values())
    if existing+sum(len(value) for value in artifacts.values()) > RESOURCE_LIMIT_BYTES:
        raise ValueError("moneyflow new stage exceeds artifact budget")
    publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent, artifacts=artifacts)
    _record(plan, root, stage)
    return root/stage


def preregister_moneyflow_price_5td_v1(*, plan, output_root):
    plan, root, *_ = _load(plan, output_root)
    return _publish(plan, root, "preregistered", None,
        {"plan.json": _json_bytes(plan.model_dump(mode="json")), "policy.json": _json_bytes(POLICY)})/"plan.json"


def prepare_moneyflow_price_5td_v1(*, plan, output_root):
    plan, root, parent, parent_root, funding_root, stages, funding_stage = _load(plan, output_root)
    first = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        _record(plan, root, "prepared")
        return root/"prepared"
    calendar_path = _verify_reference(parent.inputs["calendar"])
    calendar = json.loads(calendar_path.read_bytes())
    control_body = json.loads((parent_root/"trained"/"model.json").read_bytes())
    control = _control(control_body)
    rows = read_moneyflow_development_v1(gp5_rows=parent_root/"prepared"/"rows.parquet",
        moneyflow_rows=funding_root/"rows.parquet", configuration=plan.configuration,
        decision_dates=plan.decision_dates, calendar=calendar)
    if control.recipe["configuration"] != plan.configuration.model_dump(mode="json"):
        raise ValueError("moneyflow frozen control configuration differs")
    _verify_reference(parent.inputs["calendar"])
    _load(plan, output_root)  # Detect changing sources before immutable publication.
    buffer = io.BytesIO()
    rows.to_parquet(buffer, index=False)
    receipt = dict(original_development_rows=len(rows), original_parent_rows=7720,
        decision_dates=[day.isoformat() for day in plan.decision_dates],
        parent_stage_sha256={name: body["stage_sha256"] for name, body in stages.items()},
        funding_stage_sha256=funding_stage["stage_sha256"], frozen_control_model_sha256=control.model_sha256,
        frozen_control_arm="candidate_19D", moneyflow_source="CURRENT_FROZEN_NON_VINTAGE",
        parent_source_evidence=parent.source_evidence, parent_refit=False, selection_regenerated=False,
        test="EXCLUDED_NOT_CONSUMED", database_written=False,
        moneyflow_status_counts=rows.moneyflow_feature_status.value_counts().to_dict(),
        moneyflow_known_counts={name: int(rows[name].notna().sum()) for name in MONEYFLOW_FEATURES},
        immature_numeric_targets_decoded=False)
    return _publish(plan, root, "prepared", first["stage_sha256"],
        {"rows.parquet": buffer.getvalue(), "control_model.json": _json_bytes(control_body),
         "calendar.json": _json_bytes(calendar), "receipt.json": _json_bytes(receipt)})


def _fresh_qe(probe):
    result = probe()
    if (not isinstance(result, dict) or set(result) != {"captured_at", "running_counts"}
            or not isinstance(result["running_counts"], dict) or set(result["running_counts"]) != set(QE_PATHS)):
        raise ValueError("moneyflow fit requires all three public QE path observations")
    try:
        captured = datetime.fromisoformat(result["captured_at"])
    except (ValueError, TypeError) as exc:
        raise ValueError("moneyflow QE observation date is invalid") from exc
    if captured.tzinfo is None or not 0 <= (datetime.now(timezone.utc)-captured).total_seconds() <= 60:
        raise ValueError("moneyflow QE observation is not fresh")
    if any(type(count) is not int or count != 0 for count in result["running_counts"].values()):
        raise ValueError("moneyflow fit waits for idle QE; no QE process is controlled")
    return result


def _budget(started):
    import psutil
    if time.monotonic()-started > FIT_LIMIT_SECONDS or psutil.Process().memory_info().rss > RESOURCE_LIMIT_BYTES:
        raise ValueError("moneyflow fit exceeded time/RSS budget; no automatic retry")


def train_moneyflow_price_5td_study_v1(*, plan, output_root, qe_idle_probe):
    from threadpoolctl import threadpool_limits
    plan, root, *_ = _load(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        _record(plan, root, "trained")
        return root/"trained"
    if (root/"fit_attempt.json").exists():
        raise ValueError("moneyflow incomplete STARTED attempt requires explicit reconciliation; never refit")
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")  # Own development-only projection.
    control = _control(json.loads((root/"prepared"/"control_model.json").read_bytes()))
    started, count = time.monotonic(), 0

    def journal(body):
        with (root/"fit_journal.jsonl").open("ab") as stream:
            stream.write(_json_bytes(body).replace(b"\n", b"")+b"\n")
            stream.flush()
            os.fsync(stream.fileno())

    def before_fit(head):
        nonlocal count
        _budget(started)
        observation = _fresh_qe(qe_idle_probe)
        if count == 0:
            with (root/"fit_attempt.json").open("xb") as stream:
                stream.write(_json_bytes(dict(status="STARTED", plan_sha256=plan.plan_sha256,
                    physical_fit_budget=2, started_at=datetime.now(timezone.utc).isoformat())))
                stream.flush()
                os.fsync(stream.fileno())
        count += 1
        if count > 2:
            raise ValueError("moneyflow physical fit budget exceeded")
        journal(dict(head=head, kind="PHYSICAL_FIT_STARTED", ordinal=count, qe_observation=observation))

    def after_fit(head):
        _budget(started)
        observation = _fresh_qe(qe_idle_probe)
        journal(dict(head=head, kind="PHYSICAL_FIT_COMPLETED", ordinal=count, qe_observation=observation))

    with threadpool_limits(limits=2):
        fitted = train_moneyflow_price_5td_v1(rows=rows, frozen_control=control, configuration=plan.configuration,
                                            before_fit=before_fit, after_fit=after_fit)
    if count != 2:
        raise ValueError("moneyflow physical fit journal differs")
    _load(plan, output_root)
    return _publish(plan, root, "trained", prepared["stage_sha256"],
        {"model.json": _json_bytes(asdict(fitted)), "fit_receipt.json": _json_bytes(dict(
            physical_fit_count=count, frozen_control_refit=False, elapsed_seconds=time.monotonic()-started,
            fit_attempt_sha256=file_sha256(root/"fit_attempt.json"), fit_journal_sha256=file_sha256(root/"fit_journal.jsonl"),
            implementation_sha256=plan.implementation_sha256, deployable=False))})


def _statistics(values):
    # The caller supplies the full calendar-mature span, never outcome-selected complete cases.
    if len(values) < 10 or any(value is None or not np.isfinite(value) for value in values):
        return dict(ci95_bps=None, mde80_bps=None, reason="INCOMPLETE_OR_TOO_SHORT_ORIGINAL_MATURE_PANEL")
    values = np.asarray(values, dtype=float)
    block = STATISTICS["block_sessions"]
    starts = np.arange(len(values)-block+1)
    rng = np.random.default_rng(STATISTICS["bootstrap_seed"])
    # Draw enough complete contiguous blocks, then truncate only the resampled tail.
    sample = rng.choice(starts, size=(STATISTICS["bootstrap_count"], (len(values)+block-1)//block))[:, :, None]+np.arange(block)
    means = values[sample.reshape(STATISTICS["bootstrap_count"], -1)[:, :len(values)]].mean(axis=1)
    return dict(ci95_bps=np.quantile(means, [.025, .975]).tolist(),
        mde80_bps=float((1.959963984540054+.8416212335729143)*np.std(means, ddof=1)),
        reason="ORIGINAL_MATURE_PANEL_MOVING_BLOCK_BOOTSTRAP", block_sessions=block)


def evaluate_moneyflow_price_5td_cohorts_v1(*, rows, fitted, frozen_control, decision_dates, calendar, validation_end):
    validate_fit(fitted)
    validate_control(frozen_control)
    if (fitted.recipe["frozen_control_model_sha256"] != frozen_control.model_sha256
            or fitted.intervals_bps != frozen_control.intervals_bps):
        raise ValueError("moneyflow evaluation frozen control/support identity differs")
    roster, decisions, days = validate_roster(rows.loc[:, ROSTER], decision_dates, calendar)
    if (not rows.index.is_unique or not rows.policy_sha256.eq(POLICY_SHA256).all()
            or not rows.label_contract.eq(POLICY["label_contract"]).all()):
        raise ValueError("moneyflow evaluation policy/index differs")
    cutoff = _day(validation_end)
    configuration = fitted.recipe["configuration"]
    if cutoff != _day(configuration["validation_end"]) or any(day > cutoff or day < _day(configuration["validation_start"]) for day in decisions):
        raise ValueError("moneyflow evaluation contains future/test decisions")
    candidate = query_moneyflow_nodes_v1(fitted=fitted, features=rows, scenario_gap_bps=rows.observed_gap_bps)
    control = query_price_nodes_v1(fitted=frozen_control, features=rows, scenario_gap_bps=rows.observed_gap_bps, arm="candidate")
    arms = ("baseline", "rule_300bps", "frozen_gp5", "moneyflow")
    cohorts, episodes = [], []
    for day in decisions:
        group = rows.loc[roster[KEY[0]].eq(pd.Timestamp(day)).to_numpy() & rows.selection_effective_rank.le(5)]
        arm_daily = {arm: dict(net_bps=0., takes=0, known_avoids=0, unknown_cash=0, not_executable=0,
                               unsettled_takes=0, empty_slots=5-len(group)) for arm in arms}
        for index, row in group.iterrows():
            gap = _number(row.observed_gap_bps)
            maturity = _day(row.label_information_end, nullable=True)
            if maturity is not None and maturity > cutoff and (gap is not None or row.label_status != "IMMATURE"):
                raise ValueError("moneyflow evaluation decoded an immature observed scenario")
            terminal = _number(row.gross_terminal_ratio, positive=True)
            actual = None
            if row.label_status == "AVAILABLE":
                if gap is None or gap <= -10000 or terminal is None or maturity is None or maturity > cutoff:
                    raise ValueError("moneyflow evaluation AVAILABLE outcome is incomplete")
                actual = 10000*(terminal*(1-POLICY["sell_bps"]/10000)/((1+gap/10000)*(1+POLICY["buy_bps"]/10000))-1)
            states = dict(baseline="TAKE", rule_300bps="UNKNOWN" if gap is None else "TAKE" if abs(gap) <= 300 else "AVOID")
            for arm, table in (("frozen_gp5", control), ("moneyflow", candidate)):
                states[arm] = {"ACCEPTABLE": "TAKE", "AVOID": "AVOID", "UNKNOWN_INPUT_OR_SUPPORT": "UNKNOWN"}[table.loc[index, "status"]]
            record = dict(decision_date=day.isoformat(), instrument=row.instrument, selection_effective_rank=int(row.selection_effective_rank),
                          label_status=row.label_status, regime="UNKNOWN", actual_net_bps=actual, actions=states)
            record["net_bps"] = {}
            for arm, state in states.items():
                daily = arm_daily[arm]
                if row.label_status == "ENTRY_NOT_EXECUTABLE":
                    daily["not_executable"] += 1
                    value = 0.
                elif state == "UNKNOWN":
                    daily["unknown_cash"] += 1
                    value = 0.
                elif state == "AVOID":
                    daily["known_avoids"] += 1
                    value = 0.
                else:
                    daily["takes"] += 1
                    value = actual
                    daily["unsettled_takes"] += int(actual is None)
                record["net_bps"][arm] = value
                if value is None:
                    daily["net_bps"] = None
                elif daily["net_bps"] is not None:
                    daily["net_bps"] += value/5
            episodes.append(record)
        cohorts.append(dict(decision_date=day.isoformat(), arms=arm_daily))
    summaries = {}
    for arm in arms:
        settled = [record["actual_net_bps"] for record in episodes if record["actions"][arm] == "TAKE"
                   and record["actual_net_bps"] is not None]
        daily = [record["arms"][arm]["net_bps"] for record in cohorts]
        summaries[arm] = dict(settled_takes=len(settled), win_rate=float(np.mean(np.asarray(settled) > 0)) if settled else None,
            average_win_bps=float(np.mean([v for v in settled if v > 0])) if any(v > 0 for v in settled) else None,
            average_loss_bps=float(np.mean([v for v in settled if v <= 0])) if any(v <= 0 for v in settled) else None,
            mean_cohort_bps=float(np.mean([v for v in daily if v is not None])) if any(v is not None for v in daily) else None,
            complete_cohort_days=sum(v is not None for v in daily),
            **{name: sum(record["arms"][arm][name] for record in cohorts)
               for name in ("takes", "known_avoids", "unknown_cash", "not_executable", "unsettled_takes", "empty_slots")})
    increments, interventions = {}, {}
    mature_days = [day for day in decisions if days.index(day)+5 < len(days) and days[days.index(day)+5] <= cutoff]
    for opponent in ("baseline", "frozen_gp5"):
        differences = [None if row["arms"]["moneyflow"]["net_bps"] is None or row["arms"][opponent]["net_bps"] is None
                       else row["arms"]["moneyflow"]["net_bps"]-row["arms"][opponent]["net_bps"] for row in cohorts]
        changed = [record for record in episodes if record["label_status"] != "ENTRY_NOT_EXECUTABLE"
                   and record["actions"]["moneyflow"] != "UNKNOWN" and record["actions"][opponent] != "UNKNOWN"
                   and record["actions"]["moneyflow"] != record["actions"][opponent]]
        changed_days = len({record["decision_date"] for record in changed})
        evaluable = len(mature_days)
        interventions[opponent] = dict(known_changed_episodes=len(changed), known_changed_days=changed_days,
            mature_days=evaluable, changed_day_fraction=changed_days/evaluable if evaluable else 0., regime_distribution="UNKNOWN",
            support_met=(len(changed) >= INTERVENTION_SUPPORT["minimum_episodes"]
                and changed_days >= INTERVENTION_SUPPORT["minimum_days"]
                and evaluable > 0 and changed_days/evaluable >= INTERVENTION_SUPPORT["minimum_day_fraction"]))
        panel = [value for day, value in zip(decisions, differences, strict=True) if day in mature_days]
        paired = [value for value in panel if value is not None]
        if any(days.index(b) != days.index(a)+1 for a, b in zip(mature_days, mature_days[1:])):
            panel = []  # An undeclared interior session cannot be compressed into a contiguous block.
        known_attribution, unknown_cash_difference, unresolved = 0., 0., 0
        loss_avoided, missed_profit, new_take_profit, new_take_loss = 0., 0., 0., 0.
        for record in episodes:
            a, b = record["net_bps"]["moneyflow"], record["net_bps"][opponent]
            if a is None or b is None:
                unresolved += 1
            elif "UNKNOWN" in (record["actions"]["moneyflow"], record["actions"][opponent]):
                unknown_cash_difference += (a-b)/5
            else:
                known_attribution += (a-b)/5
                if record["actions"]["moneyflow"] != record["actions"][opponent] and record["actual_net_bps"] is not None:
                    value = record["actual_net_bps"]/5
                    if record["actions"]["moneyflow"] == "AVOID":
                        loss_avoided += max(0., -value)
                        missed_profit += max(0., value)
                    else:
                        new_take_profit += max(0., value)
                        new_take_loss += max(0., -value)
        increments[opponent] = dict(paired_days=len(paired), mean_increment_bps=float(np.mean(paired)) if paired else None,
            statistics=_statistics(panel), known_action_increment_sum_bps=known_attribution,
            loss_avoided_sum_bps=loss_avoided, missed_profit_sum_bps=missed_profit,
            new_take_profit_sum_bps=new_take_profit, new_take_loss_sum_bps=new_take_loss,
            unknown_cash_difference_sum_bps=unknown_cash_difference, unresolved_pairs=unresolved)
    positive = all(increments[arm]["mean_increment_bps"] is not None and increments[arm]["mean_increment_bps"] > 0
                   and increments[arm]["known_action_increment_sum_bps"] > 0
                   for arm in ("baseline", "frozen_gp5"))
    return dict(arms=summaries, increments=increments, intervention_support=interventions, cohorts=cohorts, episodes=episodes,
        status="EXPLORATORY_POSITIVE_NOT_CONFIRMED" if positive else "NEGATIVE_STOP_THIS_CANDIDATE",
        decision_use="NAVIGATION_ONLY", deployable=False, naturally_collected=False, independent_oos_evidence=False,
        test="EXCLUDED_NOT_CONSUMED", is_nav=False, annualized_return=None, maximum_drawdown=None)


def evaluate_moneyflow_price_5td_study_v1(*, plan, output_root):
    plan, root, *_ = _load(plan, output_root)
    trained = _stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        _record(plan, root, "evaluated")
        return root/"evaluated"
    rows = pd.read_parquet(root/"prepared"/"rows.parquet")
    validation = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.configuration.validation_start), pd.Timestamp(plan.configuration.validation_end))]
    fitted = _fitted(json.loads((root/"trained"/"model.json").read_bytes()))
    control = _control(json.loads((root/"prepared"/"control_model.json").read_bytes()))
    calendar = json.loads((root/"prepared"/"calendar.json").read_bytes())
    dates = tuple(day for day in plan.decision_dates if plan.configuration.validation_start <= day <= plan.configuration.validation_end)
    result = evaluate_moneyflow_price_5td_cohorts_v1(rows=validation, fitted=fitted, frozen_control=control,
        decision_dates=dates, calendar=calendar, validation_end=plan.configuration.validation_end)
    _load(plan, output_root)
    return _publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(result)})
