"""Four immutable stages, one shared readonly snapshot, two fits; no production wiring."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import math
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.cross_package_consensus_price_5td_contracts_v1 import (
    ARMS, CONTEXT_COLUMNS, FEATURES, KEY, MARK_POLICY, MARK_POLICY_SHA256, PARAMETERS, SCHEMA_SHA256, SOURCE_COLUMNS, STATISTICS,
    CrossPackageConsensusStudyPlanV1, node_path,
)
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.cross_package_consensus_price_5td_inputs_v1 import prepare_consensus_inputs_v1
from backend.services.advisory_model_first.d_clock_population_price_5td_inputs_v1 import construct_mark_targets_v1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.parent_score_price_5td_pipeline_v1 import _exclusive_json, _journal, _node, _qe
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def consensus_implementation_sha256_v1():
    names = [f"cross_package_consensus_price_5td_{n}_v1.py" for n in ("contracts", "inputs", "model", "study")]
    names += ["d_clock_population_price_5td_contracts_v1.py", "d_clock_population_price_5td_inputs_v1.py",
        "cross_package_validation_inputs_v1.py", "generic_daily_price_input_v1.py", "generic_price_5td_contracts_v1.py",
        "generic_price_5td_models_v1.py", "generic_price_5td_labels_v1.py", "generic_price_5td_valuation_v2.py",
        "generic_population_price_5td_model_v1.py", "generic_population_price_5td_cli_v1.py",
        "parent_score_price_5td_contracts_v1.py", "parent_score_price_5td_inputs_v1.py",
        "parent_score_price_5td_pipeline_v1.py", "parent_score_price_5td_model_v1.py",
        "economic_entry_sources.py", "economic_entry_pipeline.py", "research_control.py", "research_control_contracts.py"]
    return sha({n: hashlib.sha256((Path(__file__).parent/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})

def _checked(plan, output_root):
    plan = CrossPackageConsensusStudyPlanV1.model_validate(plan)
    if plan.implementation_sha256 != consensus_implementation_sha256_v1():
        raise ValueError("consensus implementation changed after preregistration")
    return plan, node_path(output_root)/plan.experiment_id


def _stage(plan, root, until):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        header = read_stage(root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = header["stage_sha256"]
        if stage == until:
            return header
    raise ValueError("unknown consensus stage")


def _register(plan, root, stage, *, generated=0, evaluated=0, evidence=None, result_class="EXPLORATORY"):
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id=plan.attempt_id, research_stage=stage.upper(),
        study_type=plan.study_type, objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        hypothesis_family_id="GP5-CROSS-PACKAGE-CONSENSUS-1", unique_variable="CROSS_PACKAGE_WEAKER_FROZEN_RANK_STRENGTH",
        parent_lineage=tuple(s.run_id for s in plan.sources), dataset_identity=plan.plan_sha256,
        schema_identity=SCHEMA_SHA256, policy_identity=MARK_POLICY_SHA256, planned_trial_count=2,
        generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0, result_class=result_class,
        consumed_windows=(ConsumedWindowV1(window_id="CONSUMED_DEVELOPMENT_ONLY", dataset_identity=plan.plan_sha256,
            start_date=plan.train_start, end_date=plan.evaluation_end),),
        evidence_refs=(evidence_reference_for_file(evidence or root/stage/"manifest.json", role="consensus_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _publish(plan, root, stage, parent, artifacts, *, generated=0, evaluated=0, result_class="EXPLORATORY"):
    if sum(len(v) for v in artifacts.values()) > 8*1024**3:
        raise ValueError("consensus study artifact budget exceeds 8GiB")
    target = publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256,
                           parent_sha256=parent, artifacts=artifacts)
    _register(plan, root, stage, generated=generated, evaluated=evaluated, result_class=result_class)
    return target


def preregister_cross_package_consensus_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    if (root/"preregistered").exists():
        _stage(plan, root, "preregistered")
        return root/"preregistered"
    if plan.exact_retry_of is not None:
        # An explicit prior root, never 'latest'. Only an incomplete technical attempt is retryable.
        prior = node_path(plan.exact_retry_of)
        old = CrossPackageConsensusStudyPlanV1.model_validate_json((prior/"preregistered"/"plan.json").read_bytes())
        if not (prior/"fit_attempt.json").is_file() or (prior/"trained").exists() or (prior/"evaluated").exists():
            raise ValueError("exact retry cannot revive a completed/negative study")
        ignored = {"attempt_id", "exact_retry_of", "source_commit", "implementation_sha256"}
        if {k: v for k, v in old.model_dump(mode="json").items() if k not in ignored} != {
                k: v for k, v in plan.model_dump(mode="json").items() if k not in ignored}:
            raise ValueError("technical retry cannot change the economic contract")
        if old.experiment_id != prior.name:
            raise ValueError("retry root and original run identity differ")
    return _publish(plan, root, "preregistered", None, {
        "plan.json": _json_bytes(plan.model_dump(mode="json")),
        "policy.json": _json_bytes(dict(mark_policy=MARK_POLICY, mark_policy_sha256=MARK_POLICY_SHA256,
            parameters=dict(PARAMETERS), physical_fit_budget=2, independent_confirmation=False))})


def prepare_cross_package_consensus_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    initial = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        return root/"prepared"
    rows, days, calendar, encoding, context, parent = prepare_consensus_inputs_v1(plan=plan)
    receipt = dict(status=encoding["contrast_status"], original_rows=len(rows), original_package_days=len(days),
        unique_economic_clusters=int(rows.label_cluster.nunique()), original_parent_stage_sha256=parent["stage_sha256"],
        mark_policy_sha256=MARK_POLICY_SHA256, evaluation_targets_constructed=False, historical_native_receipt=False,
        original_pool_membership_claimed=False, dataset_vintage="CURRENT_DATABASE_NON_VINTAGE",
        database_queries=0, physical_fit_count=0, database_written=False, test_or_sealed_finance_consumed=False, deployable=False)
    return _publish(plan, root, "prepared", initial["stage_sha256"], {
        "rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days),
        "contexts.parquet": parquet_bytes(context), "encoding.json": _json_bytes(encoding),
        "source.json": _json_bytes(dict(daily_ref=plan.daily_ref.model_dump(mode="json"), parent_stage_sha256=parent["stage_sha256"])),
        "calendar.json": _json_bytes(calendar), "receipt.json": _json_bytes(receipt)})

def _public_evaluation_inputs(plan, root):
    predicate = (ds.field(KEY[0]) >= pd.Timestamp(plan.evaluation_start)) & (ds.field(KEY[0]) <= pd.Timestamp(plan.evaluation_end))
    rows = ds.dataset(root/"prepared"/"rows.parquet", format="parquet").to_table(
        columns=[*SOURCE_COLUMNS, *FEATURES, *CONTEXT_COLUMNS, "reference_cny", "reference_visible_through"], filter=predicate).to_pandas()
    if rows.groupby(list(KEY))[[*FEATURES, *CONTEXT_COLUMNS, "reference_cny", "reference_visible_through"]].nunique(dropna=False).gt(1).any().any():
        raise ValueError("shared D evaluation input conflicts between packages")
    return rows


def _freeze_evaluation_distributions(plan, root, models, prepared):
    from backend.services.advisory_model_first.cross_package_consensus_price_5td_model_v1 import (
        consensus_model_from_payload_v1, freeze_consensus_distribution_v1,
    )
    rows = _public_evaluation_inputs(plan, root)
    public = rows.drop_duplicates(list(KEY)).loc[:, [*KEY, *FEATURES, *CONTEXT_COLUMNS, "reference_cny", "reference_visible_through"]]
    frozen = []
    for arm in ARMS:
        model_root = root/"fits"/arm/"trained"
        header = read_stage(model_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
        model = consensus_model_from_payload_v1(json.loads((model_root/"model.json").read_bytes()))
        if header["stage_sha256"] != models[arm]["stage_sha256"] or model.model_sha256 != models[arm]["model_sha256"]:
            raise ValueError("fitted distribution/model binding changed")
        frame = freeze_consensus_distribution_v1(fitted=model, d_features=public.reset_index(drop=True))
        frozen.append(frame.assign(arm=arm))
    return pd.concat(frozen, ignore_index=True)


def train_cross_package_consensus_study_v1(*, plan, output_root, qe_idle_probe):
    from backend.services.advisory_model_first.cross_package_consensus_price_5td_model_v1 import consensus_model_payload_v1, train_cross_package_consensus_model_v1
    plan, root = _checked(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        return root/"trained"
    encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
    if encoding["contrast_status"] not in ("PREPARED_IDENTIFIABLE_NO_FIT", "NOT_IDENTIFIABLE_CONSENSUS_CONTRAST"):
        raise ValueError("unknown prepared contrast state cannot masquerade as zero-fit success")
    if encoding["contrast_status"] != "PREPARED_IDENTIFIABLE_NO_FIT":
        return _publish(plan, root, "trained", prepared["stage_sha256"], {
            "models.json": _json_bytes({}), "receipt.json": _json_bytes(dict(status=encoding["contrast_status"],
                physical_fit_count=0, evaluation_targets_constructed=False, deployable=False))})
    if (root/"fit_attempt.json").exists():
        raise ValueError("partial STARTED attempt retained; never implicitly refit")
    node, models, count, claimed = _node(plan), {}, 0, False
    after_error = None
    try:
        for arm in ARMS:
            column = "consensus_training_role"
            rows = ds.dataset(root/"prepared"/"rows.parquet", format="parquet").to_table(
                filter=(ds.field(column).isin(["STRUCTURE", "ESTIMATION"]))
                    & (ds.field(KEY[0]) >= pd.Timestamp(plan.train_start))
                    & (ds.field(KEY[0]) <= pd.Timestamp(plan.train_end))
                    & (ds.field("label_information_end") <= pd.Timestamp(plan.train_end))).to_pandas()
            def before_fit(name):
                nonlocal count, claimed
                if name != arm or count >= 2:
                    raise ValueError("consensus physical fit arm/ordinal differs")
                observation = _qe(qe_idle_probe, require_idle=True)
                if not claimed:
                    _exclusive_json(root/"fit_attempt.json", dict(status="STARTED", plan_sha256=plan.plan_sha256,
                        node=node, physical_fit_budget=2, captured_at=datetime.now(timezone.utc).isoformat()))
                    claimed = True
                count += 1
                attempt = root/"fits"/arm/"attempt.json"
                event = dict(kind="PHYSICAL_FIT_STARTED", arm=arm, ordinal=count, qe_observation=observation)
                _exclusive_json(attempt, event)
                _journal(root, event)
                _register(plan, root, "fit_started_"+str(count), generated=count, evidence=attempt)
            def after_fit(name):
                nonlocal after_error
                if name != arm:
                    raise ValueError("post-fit arm differs")
                try:
                    observation = _qe(qe_idle_probe, require_idle=False)
                    _journal(root, dict(kind="POST_FIT_QE_OBSERVATION", arm=arm, ordinal=count, qe_observation=observation))
                    if any(observation["active_counts"].values()):
                        after_error = "QE became active: retain completed component, no next fit"
                except (ValueError, OSError) as exc:
                    after_error = f"UNKNOWN_POST_FIT_QE_OBSERVATION:{type(exc).__name__}"
                    _journal(root, dict(kind="POST_FIT_QE_UNKNOWN", arm=arm, ordinal=count, error_type=type(exc).__name__))

            model = train_cross_package_consensus_model_v1(rows=rows, encoding=encoding, arm=arm,
                plan_sha256=plan.plan_sha256, before_fit=before_fit, after_fit=after_fit)
            target = publish_stage(study_root=root/"fits"/arm, stage="trained", plan_sha256=plan.plan_sha256,
                parent_sha256=prepared["stage_sha256"], artifacts={"model.json": _json_bytes(consensus_model_payload_v1(model))})
            header = read_stage(target, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
            models[arm] = dict(model_sha256=model.model_sha256, stage_sha256=header["stage_sha256"])
            _journal(root, dict(kind="PHYSICAL_FIT_COMPLETE", arm=arm, ordinal=count, **models[arm]))
            if after_error is not None:
                raise ValueError(after_error)
        distributions = _freeze_evaluation_distributions(plan, root, models, prepared)
    except Exception as exc:
        if claimed:
            _journal(root, dict(kind="ATTEMPT_INCOMPLETE_NO_IMPLICIT_RETRY", physical_fits_started=count, error_type=type(exc).__name__))
        raise
    return _publish(plan, root, "trained", prepared["stage_sha256"], {
        "models.json": _json_bytes(models), "d_distributions.parquet": parquet_bytes(distributions),
        "receipt.json": _json_bytes(dict(status="TWO_FITS_D_DISTRIBUTIONS_FROZEN_NO_ACTIVATION", physical_fit_count=count,
            public_unique_D_keys=len(distributions)//2, evaluation_targets_constructed=False, node=node, deployable=False))}, generated=count)


def evaluate_cross_package_consensus_study_v1(*, plan, output_root):
    from backend.services.advisory_model_first.cross_package_consensus_price_5td_model_v1 import consensus_model_from_payload_v1, query_frozen_consensus_nodes_v1
    plan, root = _checked(plan, output_root)
    trained = _stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        return root/"evaluated"
    receipt = json.loads((root/"trained"/"receipt.json").read_bytes())
    if receipt["physical_fit_count"] == 0:
        return _publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(dict(
            status=receipt["status"], physical_fit_count=0, financial_evaluation_performed=False,
            evidence_use="NAVIGATION_ONLY", deployable=False, activation_evidence=False))})
    if receipt.get("evaluation_targets_constructed") is not False:
        raise ValueError("evaluation targets were consumed before the D distributions froze")
    # Complete trained-stage digest is checked before any evaluation target is decoded.
    distributions = pd.read_parquet(root/"trained"/"d_distributions.parquet")
    rows = _public_evaluation_inputs(plan, root)
    calendar = json.loads((root/"prepared"/"calendar.json").read_bytes())
    source = json.loads((root/"prepared"/"source.json").read_bytes())
    ref = source["daily_ref"]
    from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
    path = node_path(ref["artifact_uri"])
    if ref != plan.daily_ref.model_dump(mode="json") or path.stat().st_size != ref["size_bytes"] or file_sha256(path) != ref["sha256"]:
        raise ValueError("original readonly evaluation price snapshot changed")
    # The snapshot cutoff was bound at prepare. Explicit allowed-date projection remains mandatory.
    daily = ds.dataset(path, format="parquet").to_table(
        columns=["trade_date", "instrument", "raw_open_cny", "raw_high_cny", "raw_low_cny", "raw_close_cny",
                 "adj_factor", "up_limit", "down_limit", "suspended", "tradability_unknown"],
        filter=(ds.field("trade_date") >= pd.Timestamp(plan.evaluation_start))
               & (ds.field("trade_date") <= pd.Timestamp(plan.evaluation_end))).to_pandas()
    labels = construct_mark_targets_v1(plan=plan, roster=rows, calendar=[pd.Timestamp(v).date() for v in calendar],
                                     daily=daily, cutoff=plan.evaluation_end, legacy=False)
    rows = rows.merge(labels, on=["package_id", *KEY], validate="one_to_one", sort=False)
    days = pd.read_parquet(root/"prepared"/"days.parquet")
    descriptors, predictions = json.loads((root/"trained"/"models.json").read_bytes()), {}
    prepared = json.loads((root/"prepared"/"manifest.json").read_bytes())
    for arm in ARMS:
        model_root = root/"fits"/arm/"trained"
        header = read_stage(model_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
        model = consensus_model_from_payload_v1(json.loads((model_root/"model.json").read_bytes()))
        if (header["stage_sha256"] != descriptors[arm]["stage_sha256"] or model.model_sha256 != descriptors[arm]["model_sha256"]
                or model.recipe["arm"] != arm or model.recipe["plan_sha256"] != plan.plan_sha256):
            raise ValueError("evaluation fitted arm/plan identity differs")
        frozen = distributions.loc[distributions.arm.eq(arm)].drop(columns="arm")
        if frozen.duplicated(list(KEY)).any():
            raise ValueError("frozen public distribution D keys duplicate")
        mapped = rows.loc[:, [*SOURCE_COLUMNS]].merge(frozen, on=list(KEY), how="left", validate="many_to_one", sort=False)
        if mapped.model_sha256.isna().any():
            raise ValueError("frozen D distribution omitted an original candidate")
        predictions[arm] = query_frozen_consensus_nodes_v1(fitted=model, frozen=mapped,
            price_cny=rows.entry_price_cny, reference_cny=rows.reference_cny)
    evaluation = evaluate_consensus_cohorts_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    return _publish(plan, root, "evaluated", trained["stage_sha256"], {
        "evaluation.json": _json_bytes(evaluation), "labels.parquet": parquet_bytes(labels),
        "predictions.parquet": parquet_bytes(pd.concat([f.assign(arm=a) for a, f in predictions.items()], ignore_index=True))},
        generated=2, evaluated=2, result_class="NEGATIVE" if evaluation["status"].startswith("NEGATIVE") else "EXPLORATORY")



COHORT_ARMS = (*ARMS, "baseline", "rule_300bps")
UNKNOWN = {"UNKNOWN_STOCK_INPUT", "UNKNOWN_REFERENCE", "UNKNOWN_PRICE", "UNKNOWN_MODEL_DISTRIBUTION", "UNKNOWN_CONTEXT"}


def paired_consensus_statistics_v1(panels):
    """Synchronized original-session blocks; never compress a missing observation."""
    if set(panels) != {ARMS[1], "baseline"} or len({len(v) for v in panels.values()}) != 1:
        raise ValueError("two registered D-clock endpoint panels differ")
    result = {name: dict(original_mature_days=len(panel), paired_days=sum(v is not None for v in panel),
        mean_increment_bps=float(np.mean([v for v in panel if v is not None])) if any(v is not None for v in panel) else None,
        simultaneous_95_bonferroni_bps=None, mde_80_bps=None, family_alpha=.05, endpoint_alpha=.025,
        inference_status="UNAVAILABLE_ORIGINAL_PANEL_HOLE_OR_TOO_SHORT") for name, panel in panels.items()}
    if any(len(p) < 10 or any(v is None for v in p) for p in panels.values()):
        return result
    n, block = len(next(iter(panels.values()))), STATISTICS["block_sessions"]
    rng = np.random.default_rng(STATISTICS["bootstrap_seed"])
    starts = rng.integers(0, n-block+1, size=(STATISTICS["bootstrap_count"], math.ceil(n/block)))
    indexes = (starts[:, :, None]+np.arange(block)).reshape(len(starts), -1)[:, :n]
    for name, panel in panels.items():
        means = np.asarray(panel, dtype=float)[indexes].mean(axis=1)
        result[name].update(simultaneous_95_bonferroni_bps=np.quantile(means, [.0125, .9875]).tolist(),
            mde_80_bps=float((2.241402727604947+.8416212335729143)*means.std(ddof=1)),
            inference_status="SYNC_BLOCK_TWO_ENDPOINT_EXPLORATORY_NOT_CONFIRMATION")
    return result


def _validate(rows, days, predictions, plan, calendar):
    identities = {s.package_id: s for s in plan.sources}
    if (len(rows) > 50000 or rows.duplicated(["package_id", *KEY]).any()
            or days.duplicated(["package_id", "decision_date"]).any() or set(predictions) != set(ARMS)
            or set(days.package_id) != set(identities) or not set(rows.package_id).issubset(identities)):
        raise ValueError("original cohort roster/source keys differ")
    sessions = list(pd.to_datetime(calendar))
    if sessions != sorted(set(sessions)):
        raise ValueError("original evaluation calendar must be sorted unique sessions")
    expected = [d for d in sessions if pd.Timestamp(plan.evaluation_start) <= d <= pd.Timestamp(plan.evaluation_end)]
    for source in plan.sources:
        block = rows.loc[rows.package_id.eq(source.package_id)]
        if (days.loc[days.package_id.eq(source.package_id), "decision_date"].tolist() != expected
                or any(not block[n].eq(getattr(source, n)).all() for n in ("manifest_sha256", "run_id", "source_id"))):
            raise ValueError("cohort omitted a package/date or changed its known identity")
    for day in days.itertuples(index=False):
        pos = sessions.index(day.decision_date)
        t, h = sessions[pos+1], sessions[pos+5] if pos+5 < len(sessions) else pd.NaT
        block = rows.loc[rows.package_id.eq(day.package_id) & rows[KEY[0]].eq(day.decision_date)]
        if (day.target_date != t or not (day.horizon_end == h or pd.isna(day.horizon_end) and pd.isna(h))
                or len(block) != day.original_candidates or len(block) > 50
                or block.selection_effective_rank.tolist() != list(range(1, len(block)+1))
                or not block.candidate_group_size.eq(len(block)).all() or not block[KEY[1]].eq(t).all()
                or (pd.notna(h) and not block.label_information_end.eq(h).all())):
            raise ValueError("D/T/H, original ranks, roster size or full dates differ")
        if not len(block) and day.source_status not in ("KNOWN_EMPTY", "UNKNOWN"):
            raise ValueError("an empty day must be explicitly empty or unknown")
        if day.source_status not in ("KNOWN_ROSTER", "KNOWN_EMPTY", "UNKNOWN") or len(block) and day.source_status == "KNOWN_EMPTY":
            raise ValueError("original source status contradicts its roster")
    common = [*FEATURES, "reference_cny", "terminal_mark_ratio", "path_mark_min_ratio", "mark_label_status",
              "entry_execution_status", "entry_price_cny", "observed_gap_bps", "label_information_end"]
    if rows.groupby(list(KEY))[common].nunique(dropna=False).gt(1).any().any() or not rows.mark_policy_sha256.eq(MARK_POLICY_SHA256).all():
        raise ValueError("cohort economic clusters have conflicting D/label/policy coordinates")
    known = rows.loc[rows.mark_label_status.eq("AVAILABLE")]
    if (pd.to_datetime(known.label_information_end).isna().any()
            or pd.to_datetime(known.label_information_end).gt(pd.Timestamp(plan.evaluation_end)).any()):
        raise ValueError("known evaluation mark target is beyond its original cutoff")
    targets = known.loc[:, ["terminal_mark_ratio", "path_mark_min_ratio"]].map(lambda v: _number(v, positive=True))
    if targets.isna().any().any() or targets.iloc[:, 1].gt(targets.iloc[:, 0]).any():
        raise ValueError("known mark targets have invalid ratios")
    expected_keys = rows.set_index(["package_id", *KEY])
    maps = {}
    for arm, frame in predictions.items():
        if (frame.duplicated(["package_id", *KEY]).any()
                or set(frame.set_index(["package_id", *KEY]).index) != set(expected_keys.index)
                or not frame.mark_policy_sha256.eq(MARK_POLICY_SHA256).all() or not frame.schema_sha256.eq(SCHEMA_SHA256).all()
                or not frame.status.isin(["ACCEPTABLE", "AVOID", *UNKNOWN]).all()):
            raise ValueError("prediction omitted original keys or changed policy/schema")
        aligned = frame.set_index(["package_id", *KEY]).loc[expected_keys.index]
        if not aligned.loc[:, ["manifest_sha256", "run_id", "source_id"]].equals(expected_keys.loc[:, ["manifest_sha256", "run_id", "source_id"]]):
            raise ValueError("prediction original identity differs")
        finite = frame.status.isin(["ACCEPTABLE", "AVOID"])
        values = frame.loc[finite, ["expected_net_bps", "profit_probability", "downside_q90_bps"]].map(_number)
        acceptable = values.expected_net_bps.gt(0) & values.downside_q90_bps.le(MARK_POLICY["risk_bps"])
        if (values.isna().any().any() or not values.profit_probability.between(0, 1).all() or not values.downside_q90_bps.ge(0).all()
                or not frame.loc[finite, "status"].eq("ACCEPTABLE").eq(acceptable).all()
                or frame.loc[~finite, ["expected_net_bps", "profit_probability", "downside_q90_bps"]].notna().any().any()):
            raise ValueError("known/unknown price predictions contradict economic semantics")
        maps[arm] = aligned.to_dict("index")
        if (not aligned.context_sha256.eq(expected_keys.context_sha256).all()
                or not aligned.context_pair_sha256.eq(expected_keys.context_pair_sha256).all()
                or frame.model_sha256.nunique() != 1):
            raise ValueError("prediction context/pair/model binding differs")
    return maps


def _episode(row, maps, mature):
    entry = row["entry_execution_status"]
    if not mature:
        return dict(package_id=row["package_id"], decision_date=row[KEY[0]].date().isoformat(), instrument=row["instrument"],
            rank=int(row["selection_effective_rank"]), actions={a: "IMMATURE" for a in COHORT_ARMS},
            contributions_bps={a: None for a in COHORT_ARMS}, mark_contributions_bps={a: None for a in COHORT_ARMS},
            actual_net_bps=None, mark_only_net_bps=None, actual_path_adverse_bps=None, mark_label_status="IMMATURE",
            mark_label_reason="HORIZON_AFTER_CUTOFF", entry_execution_status=entry, endpoint_execution_status="IMMATURE",
            carried_mark_sessions=0, actual_fill_proven=False, realized_return_bps=None)
    p, q, y, lower, g = (_number(row[n], positive=n != "observed_gap_bps") for n in
        ("entry_price_cny", "reference_cny", "terminal_mark_ratio", "path_mark_min_ratio", "observed_gap_bps"))
    actual, mark, adverse = None, None, None
    if row["mark_label_status"] == "AVAILABLE" and p is not None and q is not None:
        a = p/q
        if g is not None and not math.isclose(g, 10000*(a-1), abs_tol=1e-7):
            raise ValueError("actual entry quote/gap contradicts its D reference")
        actual = 10000*(y*(1-MARK_POLICY["hypothetical_sell_bps"]/10000)/(a*(1+MARK_POLICY["buy_bps"]/10000))-1)
        mark = 10000*(y/(a*(1+MARK_POLICY["buy_bps"]/10000))-1)
        adverse = 10000*max(0., 1-lower/a)
    key = tuple(row[n] for n in ("package_id", *KEY))
    actions, net, mark_net = {}, {}, {}
    for arm in COHORT_ARMS:
        if entry in ("CASH_ENTRY_SUSPENDED", "CASH_ENTRY_UP_LIMIT"):
            action = "NOT_EXECUTABLE"
        elif entry != "NOMINAL_ENTRY_ELIGIBLE_NOT_FILL_PROOF":
            action = "UNKNOWN_ENTRY"
        elif arm == "baseline":
            action = "TAKE"
        elif arm == "rule_300bps":
            action = "UNKNOWN_MODEL" if g is None else "TAKE" if -300 <= g <= 300 else "SKIP"
        else:
            status = maps[arm][key]["status"]
            action = "TAKE" if status == "ACCEPTABLE" else "SKIP" if status == "AVOID" else "UNKNOWN_MODEL"
        actions[arm] = action
        net[arm] = actual if action == "TAKE" else None if action in ("IMMATURE", "UNKNOWN_ENTRY") else 0.
        mark_net[arm] = mark if action == "TAKE" else None if action in ("IMMATURE", "UNKNOWN_ENTRY") else 0.
    return dict(package_id=row["package_id"], decision_date=row[KEY[0]].date().isoformat(), instrument=row["instrument"],
        rank=int(row["selection_effective_rank"]), actions=actions, contributions_bps=net, mark_contributions_bps=mark_net,
        actual_net_bps=actual, mark_only_net_bps=mark, actual_path_adverse_bps=adverse, mark_label_status=row["mark_label_status"],
        mark_label_reason=row["mark_label_reason"], entry_execution_status=entry, endpoint_execution_status=row["endpoint_execution_status"],
        carried_mark_sessions=int(row["carried_mark_sessions"]), actual_fill_proven=False, realized_return_bps=None)


def _attribution(episodes, paired_dates, opponent, package_count):
    parts = dict(avoided_loss_bps=0., missed_profit_bps=0., new_take_profit_bps=0., new_take_loss_bps=0.,
        known_action_contribution_bps=0., unknown_cash_contribution_bps=0.)
    clusters, days, trend_clusters = set(), set(), {n: set() for n in ("UP", "DOWN", "FLAT", "UNKNOWN")}
    if not paired_dates:
        return parts, clusters, days, {n: 0 for n in trend_clusters}
    scale = 1/(5*package_count*len(paired_dates))
    for e in episodes:
        if e["decision_date"] not in paired_dates:
            continue
        a, b = e["actions"][ARMS[0]], e["actions"][opponent]
        delta = e["contributions_bps"][ARMS[0]]-e["contributions_bps"][opponent]
        if "UNKNOWN_MODEL" in (a, b):
            parts["unknown_cash_contribution_bps"] += delta*scale
        else:
            parts["known_action_contribution_bps"] += delta*scale
            if a != b and e["actual_net_bps"] is not None and set((a, b)).issubset({"TAKE", "SKIP"}):
                clusters.add((e["decision_date"], e["instrument"]))
                days.add(e["decision_date"])
                trend_clusters[e["trend_proxy"]].add((e["decision_date"], e["instrument"]))
                r = e["actual_net_bps"]
                cell = ("avoided_loss_bps" if r < 0 else "missed_profit_bps") if a == "SKIP" else (
                    "new_take_profit_bps" if r >= 0 else "new_take_loss_bps")
                parts[cell] += abs(r)*scale
    economic = parts["avoided_loss_bps"]-parts["missed_profit_bps"]+parts["new_take_profit_bps"]-parts["new_take_loss_bps"]
    if not math.isclose(economic, parts["known_action_contribution_bps"], abs_tol=1e-7):
        raise ValueError("known action attribution is not an exact once-cost net ledger")
    return parts, clusters, days, {n: len(v) for n, v in trend_clusters.items()}


def _summary(groups, episodes, package_count, inference):
    dates = sorted({g["decision_date"] for g in groups if g["horizon_mature"]})
    values = {}
    for d in dates:
        cells = [g for g in groups if g["decision_date"] == d]
        values[d] = {a: None if len(cells) != package_count or any(g["cohort_net_bps"][a] is None for g in cells)
            else float(np.mean([g["cohort_net_bps"][a] for g in cells])) for a in COHORT_ARMS}
    panels = {b: [None if values[d][ARMS[0]] is None or values[d][b] is None else values[d][ARMS[0]]-values[d][b]
        for d in dates] for b in (ARMS[1], "baseline")}
    stats = paired_consensus_statistics_v1(panels)
    pairs = {}
    for opponent, panel in panels.items():
        paired_dates = {d for d, value in zip(dates, panel, strict=True) if value is not None}
        parts, clusters, intervention_days, trends = _attribution(episodes, paired_dates, opponent, package_count)
        mean = stats[opponent]["mean_increment_bps"]
        if mean is not None and not math.isclose(mean, parts["known_action_contribution_bps"]+parts["unknown_cash_contribution_bps"], abs_tol=1e-7):
            raise ValueError("paired increment does not reconcile to original slots and packages")
        if not inference:
            stats[opponent].update(simultaneous_95_bonferroni_bps=None, mde_80_bps=None, inference_status="DESCRIPTIVE_PACKAGE_STRATUM_ONLY")
        fraction = len(intervention_days)/len(dates) if dates else None
        pairs[opponent] = dict(**stats[opponent], attribution=parts, intervention_unique_stock_days=len(clusters),
            intervention_days=len(intervention_days), intervention_day_fraction=fraction, intervention_trend_proxy_counts=trends,
            exploratory_support_hint=bool(dates) and len(clusters) >= 20 and len(intervention_days) >= 12 and fraction >= .1,
            support_hint_is_confirmation_gate=False)
    common = [v for v in values.values() if all(x is not None for x in v.values())]
    means = {a: dict(known_days=sum(v[a] is not None for v in values.values()),
        mean_bps=float(np.mean([v[a] for v in values.values() if v[a] is not None])) if any(v[a] is not None for v in values.values()) else None,
        common_four_arm_days=len(common), common_four_arm_mean_bps=float(np.mean([v[a] for v in common])) if common else None) for a in COHORT_ARMS}
    takes = {}
    for a in COHORT_ARMS:
        items = [e for e in episodes if e["decision_date"] in dates and e["actions"][a] == "TAKE" and e["actual_net_bps"] is not None]
        r = [e["actual_net_bps"] for e in items]
        wins, losses = [x for x in r if x > 0], [x for x in r if x < 0]
        takes[a] = dict(valued_take_episodes=len(r), unique_valued_stock_days=len({(e["decision_date"], e["instrument"]) for e in items}),
            hit_rate=len(wins)/len(r) if r else None, mean_winner_bps=float(np.mean(wins)) if wins else None,
            mean_loser_bps=float(np.mean(losses)) if losses else None,
            average_win_loss_ratio=float(np.mean(wins)/abs(np.mean(losses))) if wins and losses else None,
            terminal_q10_net_bps=float(np.quantile(r, .1)) if r else None,
            action_counts={action: sum(e["decision_date"] in dates and e["actions"][a] == action for e in episodes)
                for action in ("TAKE", "SKIP", "NOT_EXECUTABLE", "UNKNOWN_MODEL", "UNKNOWN_ENTRY")})
    return dict(original_mature_days=len(dates), pairs=pairs, cohort_means=means, take_statistics=takes,
        original_daily_panel=[dict(decision_date=d, values_bps=values[d]) for d in dates], equal_package_weight=1/package_count,
        overlapping_five_session_cohort_not_nav=True)


def _prediction_diagnostics(rows, maps):
    results = {}
    bins = [0., .2, .4, .6, .8, 1.]
    for arm in ARMS:
        observations = []
        for row in rows.loc[rows.selection_effective_rank.le(5)].to_dict("records"):
            if row["mark_label_status"] != "AVAILABLE" or row["entry_execution_status"] != "NOMINAL_ENTRY_ELIGIBLE_NOT_FILL_PROOF":
                continue
            forecast = maps[arm][tuple(row[n] for n in ("package_id", *KEY))]
            probability = _number(forecast["profit_probability"])
            if probability is None:
                continue
            a = _number(row["entry_price_cny"], positive=True)/_number(row["reference_cny"], positive=True)
            net = 10000*(row["terminal_mark_ratio"]*(1-MARK_POLICY["hypothetical_sell_bps"]/10000)/(a*(1+MARK_POLICY["buy_bps"]/10000))-1)
            risk = 10000*max(0., 1-row["path_mark_min_ratio"]/a)
            observations.append(dict(p=probability, y=float(net > 0), risk=risk, q90=float(forecast["downside_q90_bps"]),
                carry=row["carried_mark_sessions"] > 0, net=net, gap=_number(row["observed_gap_bps"]),
                expected_net=float(forecast["expected_net_bps"])))
        groups = {}
        for name in ("all", "all_traded_path", "contains_verified_carry"):
            chosen = [v for v in observations if name == "all" or v["carry"] == (name == "contains_verified_carry")]
            groups[name] = dict(n=len(chosen), brier=float(np.mean([(v["p"]-v["y"])**2 for v in chosen])) if chosen else None,
                mark_path_q90_coverage=float(np.mean([v["risk"] <= v["q90"] for v in chosen])) if chosen else None,
                terminal_q10_net_bps=float(np.quantile([v["net"] for v in chosen], .1)) if chosen else None)
        groups["probability_bins"] = [dict(low=lo, high=hi, n=len(chosen), mean_probability=float(np.mean([v["p"] for v in chosen])) if chosen else None,
            outcome_frequency=float(np.mean([v["y"] for v in chosen])) if chosen else None)
            for lo, hi in zip(bins[:-1], bins[1:], strict=True)
            for chosen in [[v for v in observations if lo <= v["p"] < hi or hi == 1. and v["p"] == 1.]]]
        gaps = {}
        for name, chosen in (
                ("BELOW_MINUS_300", [v for v in observations if v["gap"] is not None and v["gap"] < -300]),
                ("MINUS_300_TO_ZERO_EXCLUSIVE", [v for v in observations if v["gap"] is not None and -300 <= v["gap"] < 0]),
                ("ZERO_TO_300_INCLUSIVE", [v for v in observations if v["gap"] is not None and 0 <= v["gap"] <= 300]),
                ("ABOVE_300", [v for v in observations if v["gap"] is not None and v["gap"] > 300]),
                ("UNKNOWN_GAP", [v for v in observations if v["gap"] is None])):
            gaps[name] = dict(n=len(chosen), mean_net_prediction_error_bps=float(np.mean([v["expected_net"]-v["net"] for v in chosen])) if chosen else None,
                brier=float(np.mean([(v["p"]-v["y"])**2 for v in chosen])) if chosen else None)
        groups["fixed_actual_gap_diagnostic"] = gaps
        results[arm] = dict(**groups, diagnostic_only=True, probability_clock="D_PAYMENT_FUNCTION_NOT_ACTUAL_OPEN_CONDITIONAL",
            risk_measure=MARK_POLICY["risk_measure"], calibration_fit_count=0)
    return results


def evaluate_consensus_cohorts_v1(*, rows, days, predictions, plan, calendar):
    # Original date projection is performed before any outcome parser.
    rows = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    days = days.loc[days.decision_date.between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    predictions = {a: f.loc[f[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy() for a, f in predictions.items()}
    maps = _validate(rows, days, predictions, plan, calendar)
    groups, episodes = [], []
    for day in days.to_dict("records"):
        original = rows.loc[rows.package_id.eq(day["package_id"]) & rows[KEY[0]].eq(day["decision_date"])]
        mature = pd.notna(day["horizon_end"]) and day["horizon_end"] <= pd.Timestamp(plan.evaluation_end)
        selected = original.loc[original.selection_effective_rank.le(5)]
        trend = _number(original.csi300_ret_5.iloc[0]) if len(original) else None
        proxy = "UNKNOWN" if trend is None else "UP" if trend > 0 else "DOWN" if trend < 0 else "FLAT"
        items = [dict(**_episode(row, maps, mature), trend_proxy=proxy) for row in selected.to_dict("records")]
        episodes.extend(items)
        source_known = day["source_status"] in ("KNOWN_ROSTER", "KNOWN_EMPTY")
        contributions = {a: None if not mature or not source_known or any(e["contributions_bps"][a] is None for e in items)
            else sum(e["contributions_bps"][a] for e in items)/5 for a in COHORT_ARMS}
        mark_contributions = {a: None if not mature or not source_known or any(e["mark_contributions_bps"][a] is None for e in items)
            else sum(e["mark_contributions_bps"][a] for e in items)/5 for a in COHORT_ARMS}
        groups.append(dict(package_id=day["package_id"], decision_date=day["decision_date"].date().isoformat(),
            horizon_mature=bool(mature), source_status=day["source_status"], original_candidates=len(original), original_slots=5,
            structural_empty_slots=5-len(items) if source_known else None, cohort_net_bps=contributions,
            cohort_mark_bps=mark_contributions, lagged_csi300_5d_proxy=proxy))
    primary = _summary(groups, episodes, 2, True)
    packages = {s.package_id: _summary([g for g in groups if g["package_id"] == s.package_id],
        [e for e in episodes if e["package_id"] == s.package_id], 1, False) for s in plan.sources}
    promising = all(p["exploratory_support_hint"] and p["mean_increment_bps"] is not None and p["mean_increment_bps"] > 0
        and p["attribution"]["known_action_contribution_bps"] > 0 for p in primary["pairs"].values())
    return dict(status="EXPLORATORY_POSITIVE_NAVIGATION" if promising else "NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE",
        hypothesis="GP5-CROSS-PACKAGE-CONSENSUS-1", primary=primary, packages=packages,
        prediction_diagnostics=_prediction_diagnostics(rows, maps),
        consensus_support={name: dict(original_rows=int(mask.sum()), unique_stock_days=int(rows.loc[mask, list(KEY)].drop_duplicates().shape[0]))
            for name, mask in {"KNOWN_ZERO": rows.context_availability.eq("AVAILABLE") & rows.cross_package_consensus_strength.eq(0),
                "KNOWN_POSITIVE": rows.context_availability.eq("AVAILABLE") & rows.cross_package_consensus_strength.gt(0),
                "UNKNOWN_CONTEXT": rows.context_availability.eq("UNKNOWN_CONTEXT")}.items()}, original_groups=groups, original_episodes=episodes,
        original_rows=len(rows), original_package_days=len(days),
        label_counts={str(k): int(v) for k, v in rows.mark_label_status.value_counts().items()},
        entry_execution_counts={str(k): int(v) for k, v in rows.entry_execution_status.value_counts().items()},
        endpoint_execution_counts={str(k): int(v) for k, v in rows.endpoint_execution_status.value_counts().items()},
        physical_fit_count=2, evidence_use="NAVIGATION_ONLY", objective_contract=plan.objective_contract, study_type=plan.study_type,
        actual_fill_proven=False, realized_return_bps=None, independent_oos_evidence=False, activation_evidence=False,
        dataset_vintage="CURRENT_DATABASE_NON_VINTAGE", nav_or_annualized_return_claimed=False, automatic_followup_trial=False, deployable=False)




def main(argv=None):
    import argparse
    from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("preregister", "prepare", "train", "evaluate"))
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args(argv)
    plan = CrossPackageConsensusStudyPlanV1.model_validate_json(node_path(args.plan).read_bytes())
    operations = dict(preregister=preregister_cross_package_consensus_study_v1, prepare=prepare_cross_package_consensus_study_v1,
        train=train_cross_package_consensus_study_v1, evaluate=evaluate_cross_package_consensus_study_v1)
    kwargs = dict(plan=plan, output_root=args.output_root)
    if args.operation == "train":
        kwargs["qe_idle_probe"] = lambda: public_qe_observation_v1(plan.qe_api_base)
    target = operations[args.operation](**kwargs)
    receipt = target/("evaluation.json" if args.operation == "evaluate" else "receipt.json")
    state = json.loads(receipt.read_bytes()) if receipt.is_file() else dict(status="PREREGISTERED_NO_FIT", physical_fit_count=0)
    print(json.dumps(dict(root=str(target), experiment_id=plan.experiment_id, status=state.get("status"),
        completed_stage=args.operation, physical_fit_count=state.get("physical_fit_count", 0), deployable=False,
        database_written=False, backend_restart_required=False, process_control=False)), flush=True)


if __name__ == "__main__":
    main()
