"""Immutable Advisory-only successor over consumed frozen parents; no DB/QE IO."""
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES, SEMANTICS
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1, EconomicEntryStudyPlanV1
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _authorize, _json_bytes, _parquet_bytes, _verify_reference, file_sha256, publish_stage, read_stage, verify_economic_dataset,
)
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import (
    COST, ValueAnchorEstimateV1, ValueAnchorGapSupportV1, value_anchor_policy_sha256_v1, value_anchor_policy_v1,
)
from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import build_value_anchor_labels_v1
from backend.services.advisory_model_first.economic_value_anchor_training_v1 import (
    ValueAnchorFitV1, ValueAnchorStudyPlanV1, assemble_value_anchor_rows_v1, train_value_anchor_v1,
)
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def value_anchor_implementation_sha256_v1():
    names = [f"economic_value_anchor_{name}_v1.py" for name in ("contracts", "labels", "inference", "training", "pipeline", "evaluation")]
    names += ["economic_daily_feature_core_v1.py", "policy_episode_labels.py", "shadow_portfolio_policy.py", "economic_entry_pipeline.py"]
    names += ["policy_contracts.py", "economic_entry_aligned_evaluation.py", "economic_entry_evaluation.py"]
    files = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for name in names}
    files["../advisory_list_transition.py"] = hashlib.sha256((Path(__file__).parent.parent/"advisory_list_transition.py").read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    return sha({"algorithm": "UTF8_LF_BYTES_V1", "files": files})


def _root(plan, output_root):
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root != declared.absolute() or root.drive.upper() == "C:":
        raise ValueError("value study needs an original absolute non-C root")
    return root/plan.experiment_id


def _source_git_receipt():
    repository = Path(__file__).resolve().parents[3]
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True).stdout.decode().strip()
    blobs = {}
    for name in ("contracts", "labels", "inference", "training", "pipeline", "evaluation"):
        relative = f"backend/services/advisory_model_first/economic_value_anchor_{name}_v1.py"
        blob = subprocess.run(["git", "rev-parse", f"{head}:{relative}"], cwd=repository, check=True, capture_output=True).stdout.decode().strip()
        tracked = subprocess.run(["git", "show", f"{head}:{relative}"], cwd=repository, check=True, capture_output=True).stdout
        if tracked.replace(b"\r\n", b"\n") != (repository/relative).read_bytes().replace(b"\r\n", b"\n"):
            raise ValueError("value source differs from its committed Git blob")
        blobs[relative] = blob
    return {"git_head": head, "git_blobs": blobs, "implementation_sha256": value_anchor_implementation_sha256_v1()}


def value_anchor_sources_v1(plan):
    """Authorize consumed parent data BEFORE parsing any outcome price arrays."""
    if plan.implementation_sha256 != value_anchor_implementation_sha256_v1():
        raise ValueError("value implementation identity changed")
    path = _verify_reference(plan.parent_plan_ref)
    parent = EconomicEntryStudyPlanV1.model_validate_json(path.read_text(encoding="utf-8"))
    root = path.parent.parent
    if path != root/"preregistered/plan.json" or root.name != parent.experiment_id:
        raise ValueError("value frozen parent path differs")
    _authorize(parent)  # Parent source-read authority only; not new-policy activation.
    verify_economic_dataset(parent)
    registered = read_stage(root/"preregistered", stage="preregistered", plan_sha256=parent.plan_sha256, parent_sha256=None)
    prepared_path = _verify_reference(plan.parent_prepared_manifest_ref)
    if prepared_path != root/"prepared/manifest.json":
        raise ValueError("value prepared prices leave the registered parent")
    read_stage(root/"prepared", stage="prepared", plan_sha256=parent.plan_sha256, parent_sha256=registered["stage_sha256"])
    identity = EconomicEntryInputIdentityV1.model_validate_json((root/"prepared/identity.json").read_text(encoding="utf-8"))
    if identity.dataset_manifest_sha256 != parent.dataset_manifest_ref.sha256 or identity.cost_policy != COST:
        raise ValueError("value parent dataset/cost identity differs")
    feature_path = _verify_reference(plan.feature_manifest_ref)
    if feature_path.name != "manifest.json" or feature_path.parent.name != "prepared":
        raise ValueError("value core snapshot path differs")
    feature_identity = json.loads((feature_path.parent/"identity.json").read_text(encoding="utf-8"))
    read_stage(feature_path.parent, stage="prepared", plan_sha256=sha(feature_identity), parent_sha256=file_sha256(prepared_path))
    recipe = feature_identity["recipe"]
    frozen = json.loads((root/"prepared/frozen_request.json").read_text(encoding="utf-8"))
    if (feature_identity["frozen_parent_manifest_sha256"] != file_sha256(prepared_path)
            or feature_identity["recipe_sha256"] != sha(recipe) or sha(recipe["core_semantics"]) != sha(SEMANTICS)
            or not set(D_FEATURES).issubset(recipe["feature_names"]) or recipe["package_id"] != identity.package_id
            or recipe["manifest_sha256"] != identity.manifest_sha256 or feature_identity["native_identity"] != "UNPROVEN"
            or recipe["terminal_weights"] != frozen["terminal_weights"]
            or set(recipe["roles"]) != {"lstm", "fund"} or set(recipe["roles"].values()) != set(frozen["terminal_weights"])
            or any(feature_identity[name] is not False for name in ("outcomes_read", "database_written", "sealed_accessed"))
            or feature_identity["model_fits"] != 0):
        raise ValueError("value D core recipe/parent/source evidence differs")
    return parent, root/"prepared", identity, feature_path.parent


def _record(plan, root, parent, stage, evidence, *, generated=0, evaluated=0):
    configuration = parent.configuration
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="fixed_value_anchor_attempt_v1", research_stage=stage,
        study_type=plan.study_type, hypothesis_family_id="economic_entry_price_value", parent_lineage=(parent.experiment_id, "H-TIMING-1_STOPPED"),
        unique_variable="price_independent_review_scene_D_only_gross_value_and_path_minimum_one_configuration_two_heads",
        objective_contract=plan.objective_contract, dataset_identity=parent.dataset_identity, schema_identity=plan.schema_version,
        policy_identity=sha({"parent_source_policy": parent.policy_identity, "value_policy": value_anchor_policy_sha256_v1(), "cost": COST.policy_sha256}),
        planned_trial_count=1, generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="P0C_DEVELOPMENT_V1", dataset_identity=parent.dataset_identity,
            start_date=configuration.train_start, end_date=configuration.label_cutoff),),
        result_class="CONTROL_READY" if stage == "PREREGISTERED" else "EXPLORATORY", decision_use="NAVIGATION_ONLY",
        evidence_refs=(evidence_reference_for_file(evidence, role="value_"+stage.lower()),))
    return AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _ledger(plan, root, stage, evidence):
    matches = [item for item in AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").read()
        if item.experiment_id == plan.experiment_id and item.research_stage == stage]
    if len(matches) != 1 or len(matches[0].evidence_refs) != 1 or matches[0].evidence_refs[0].sha256 != file_sha256(evidence):
        raise ValueError("value registry and durable stage differ")


def preregister_value_anchor_v1(*, plan, output_root):
    plan = ValueAnchorStudyPlanV1.model_validate(plan)
    parent, _, identity, _ = value_anchor_sources_v1(plan)
    root = _root(plan, output_root)
    if (root/"preregistered").exists():
        read_stage(root/"preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
        _record(plan, root, parent, "PREREGISTERED", root/"preregistered/manifest.json")
        return root/"preregistered/plan.json"
    policy = {"scenario_sha256": value_anchor_policy_sha256_v1(), "policy": asdict(value_anchor_policy_v1()),
        "cost_policy": COST.model_dump(mode="json"), "parent_identity": identity.model_dump(mode="json"),
        "parameters": plan.parameters, "source_read_authority": "CONSUMED_PARENT_WINDOW_NOT_ACTIVATION"}
    path = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json")), "policy.json": _json_bytes(policy),
            "source_receipt.json": _json_bytes(_source_git_receipt())})
    _record(plan, root, parent, "PREREGISTERED", path/"manifest.json")
    return path/"plan.json"


def load_value_anchor_v1(*, plan_path, output_root):
    plan = ValueAnchorStudyPlanV1.model_validate_json(Path(plan_path).read_text(encoding="utf-8"))
    root = _root(plan, output_root)
    if Path(plan_path).resolve() != root/"preregistered/plan.json":
        raise ValueError("value study must load its exact registered plan")
    registered = read_stage(root/"preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    source = value_anchor_sources_v1(plan)
    _ledger(plan, root, "PREREGISTERED", root/"preregistered/manifest.json")
    return plan, root, registered, source


def value_anchor_observations_v1(candidates, prices, references):
    # Only actual opens/reference/known trading flags; no future returns or HLC.
    rows = candidates.loc[:, KEY].merge(references.loc[:, KEY+["target_reference_raw_cny"]], on=KEY, how="left", validate="one_to_one")
    quoted = prices.loc[:, ["trade_date", "instrument", "raw_open_cny", "suspended", "tradability_unknown"]].rename(columns={"trade_date": KEY[1]})
    rows = rows.merge(quoted, on=KEY[1:], how="left", validate="many_to_one")
    known = rows.suspended.eq(False) & rows.tradability_unknown.eq(False)
    raw = pd.to_numeric(rows.raw_open_cny, errors="coerce")
    reference = pd.to_numeric(rows.target_reference_raw_cny, errors="coerce")
    rows["actual_gap_bps"] = ((raw/reference-1)*10000).where(known & raw.gt(0) & reference.gt(0))
    return rows.loc[:, KEY+["actual_gap_bps"]]


def _value_prices(frozen, configuration):
    path = frozen/"prices.parquet"
    clock = pd.read_parquet(path, columns=["trade_date"])
    dates = pd.to_datetime(clock.trade_date)
    if (len(clock) > 500000 or dates.isna().any() or dates.gt(pd.Timestamp(configuration.label_cutoff)).any()
            or dates.lt(pd.Timestamp(configuration.train_start)).any()):
        raise ValueError("value prices exceed the consumed source clock/budget; OHLC not read")
    return pd.read_parquet(path)


def prepare_value_anchor_v1(*, plan_path, output_root):
    plan, root, registered, source = load_value_anchor_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, feature = source
    if (root/"prepared").exists():
        read_stage(root/"prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
        _record(plan, root, parent, "PREPARED", root/"prepared/manifest.json")
        return root/"prepared"
    rankings = pd.read_parquet(frozen/"frozen_rankings.parquet")
    candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)].copy()
    prices, references = _value_prices(frozen, parent.configuration), pd.read_parquet(frozen/"references.parquet")
    if len(prices) > 500000 or len(candidates) > 100000:
        raise ValueError("value preparation exceeds the frozen budget")
    calendar = json.loads((frozen/"calendar.json").read_text(encoding="utf-8"))
    labels = build_value_anchor_labels_v1(candidates=candidates, rankings=rankings, prices=prices,
        references=references, calendar=calendar, parent_identity=identity)
    inputs = pd.read_parquet(feature/"features.parquet", columns=[*KEY, *D_FEATURES, "feature_visible_through", "daily_input_sha256"])
    observations = value_anchor_observations_v1(candidates, prices, references)
    rows = assemble_value_anchor_rows_v1(inputs=inputs, labels=labels, observations=observations, configuration=parent.configuration)
    target = publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"],
        artifacts={"labels.parquet": _parquet_bytes(labels), "rows.parquet": _parquet_bytes(rows),
            "preparation.json": _json_bytes({"candidate_rows": len(rows), "decisions": rows[KEY[0]].nunique(),
                "label_status": labels.value_label_status.value_counts().to_dict(), "value_policy_sha256": value_anchor_policy_sha256_v1(),
                "parent_identity_sha256": identity.identity_sha256, "source_evidence": identity.source_evidence,
                "evidence_limitations": identity.evidence_limitations, "head_fits": 0, "sealed_accessed": False, "database_written": False})})
    _record(plan, root, parent, "PREPARED", target/"manifest.json")
    return target


def train_value_anchor_study_v1(*, plan_path, output_root, qe_training_idle):
    if qe_training_idle is not True:
        raise ValueError("value fit cannot overlap QE training")
    plan, root, registered, source = load_value_anchor_v1(plan_path=plan_path, output_root=output_root)
    parent = source[0]
    prepared = read_stage(root/"prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    _ledger(plan, root, "PREPARED", root/"prepared/manifest.json")
    with _exclusive_file_lock(root/"fit.lock"):
        if (root/"trained").exists():
            read_stage(root/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
            _record(plan, root, parent, "TRAINED", root/"trained/manifest.json", generated=1)
            return root/"trained"
        attempt = root/"fit_attempt.json"
        if attempt.exists():
            raise ValueError("value partial fit exists; diagnosis required, no implicit refit")
        with attempt.open("xb") as handle:
            handle.write(_json_bytes({"plan_sha256": plan.plan_sha256, "planned_configurations": 1, "planned_heads": 2, "completed_heads": "UNPROVEN"}))
            handle.flush()
            os.fsync(handle.fileno())
        _record(plan, root, parent, "FIT_STARTED", attempt)
        fit = train_value_anchor_v1(rows=pd.read_parquet(root/"prepared/rows.parquet"), plan=plan, configuration=parent.configuration)
        metadata = {"plan_sha256": plan.plan_sha256, "feature_names": list(D_FEATURES), "parameters": plan.parameters,
            "constant": asdict(fit.constant), "gap_support": asdict(fit.gap_support), "diagnostics": fit.diagnostics}
        target = publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"],
            artifacts={"mean.txt": fit.mean_model.model_to_string().encode(), "path.txt": fit.path_model.model_to_string().encode(), "metadata.json": _json_bytes(metadata)})
        _record(plan, root, parent, "TRAINED", target/"manifest.json", generated=1)
        return target


def load_value_anchor_fit_v1(*, plan_path, output_root):
    import lightgbm as lgb
    plan, root, registered, _ = load_value_anchor_v1(plan_path=plan_path, output_root=output_root)
    prepared = read_stage(root/"prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    read_stage(root/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
    _ledger(plan, root, "TRAINED", root/"trained/manifest.json")
    metadata = json.loads((root/"trained/metadata.json").read_text(encoding="utf-8"))
    if metadata["plan_sha256"] != plan.plan_sha256 or metadata["feature_names"] != list(D_FEATURES) or metadata["parameters"] != plan.parameters or lgb.__version__ != "4.6.0":
        raise ValueError("value trained recipe/runtime differs")
    models = [lgb.Booster(model_file=str(root/"trained"/name)) for name in ("mean.txt", "path.txt")]
    if any(tuple(model.feature_name()) != tuple(D_FEATURES) for model in models):
        raise ValueError("value trained dimension/order differs")
    return ValueAnchorFitV1(*models, ValueAnchorEstimateV1(**metadata["constant"]),
        ValueAnchorGapSupportV1(tuple(tuple(pair) for pair in metadata["gap_support"]["intervals_bps"])), metadata["diagnostics"])
