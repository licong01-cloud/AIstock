"""Immutable H-TIMING stages over existing frozen parents, no QE dispatch."""
import json
import hashlib
import os
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import SEMANTICS as CORE_SEMANTICS
from backend.services.advisory_model_first.economic_entry_timing_features_v1 import SEMANTICS as TIMING_SEMANTICS
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryStudyPlanV3
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1, EconomicEntryStudyPlanV1, EconomicEntryTrainingRequestV1
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossStudyPlanV2, EntryLossLabelV2
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, _parquet_bytes, _verify_reference, _authorize, verify_economic_dataset, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import ARMS, CANDIDATE_NAMES, TimingStudyPlanV1, TimingTrainingRequestV1
from backend.services.advisory_model_first.economic_entry_timing_training_v1 import (
    TimingArmV1, assemble_timing_rows_v1, timing_common_fit_sha256, timing_input_rows_sha256, train_timing_entry_v1,
)
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file, research_policy_identity
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def code_sha256(path):
    """Algorithm identity normalizes CRLF only; artifact hashes stay byte-exact."""
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def timing_implementation_sha256():
    names = [f"economic_entry_timing_{name}_v1.py" for name in ("contracts", "training", "inference", "pipeline", "evaluation", "features")]
    names += ["economic_daily_feature_core_v1.py", "economic_common_core_daily_source_v1.py"]
    return sha({"algorithm": "UTF8_LF_BYTES_V1", "files": {name: code_sha256(Path(__file__).with_name(name)) for name in names}})


def _root(output_root):
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root != declared.absolute() or root.drive.upper() == "C:":
        _fail("timing requires an original absolute non-C output root")
    return root


def _parent(plan_path):
    # A frozen-data consumer, NOT old study resume or old weight qualification.
    # Keep historical implementation IDs intact; do not patch their strict
    # loaders or pretend the current checkout is their original source version.
    path = Path(plan_path).resolve()
    parent = AlignedEntryStudyPlanV3.model_validate_json(path.read_text(encoding="utf-8"))
    root = path.parent.parent
    if path != root/"preregistered/plan.json" or root.name != parent.experiment_id:
        _fail("timing parent snapshot path differs")
    read_stage(root/"preregistered", stage="preregistered", plan_sha256=parent.plan_sha256, parent_sha256=None)
    verify_timing_ledger(parent, root, "PREREGISTERED")
    risk_path = _verify_reference(parent.v2_plan_ref)
    risk = EntryLossStudyPlanV2.model_validate_json(risk_path.read_text(encoding="utf-8"))
    risk_root = risk_path.parent.parent
    if risk_path != risk_root/"preregistered/plan.json" or risk_root.name != risk.experiment_id:
        _fail("timing risk snapshot path differs")
    risk_registration = read_stage(risk_root/"preregistered", stage="preregistered", plan_sha256=risk.plan_sha256, parent_sha256=None)
    verify_timing_ledger(risk, risk_root, "PREREGISTERED")
    original_path = _verify_reference(risk.parent_plan_ref)
    original = EconomicEntryStudyPlanV1.model_validate_json(original_path.read_text(encoding="utf-8"))
    original_root = original_path.parent.parent
    if (original_path != original_root/"preregistered/plan.json" or original_root.name != original.experiment_id
            or original.experiment_id not in risk.parent_lineage or risk.experiment_id not in parent.parent_lineage):
        _fail("timing original snapshot path/lineage differs")
    # Reauthorize BEFORE prepared stage verification reads label artifact bytes.
    _authorize(original)
    dataset, frozen = verify_economic_dataset(original)
    policy_hash = research_policy_identity(baseline_policy_sha256=frozen.baseline_policy_sha256,
        shadow_policy_sha256=dataset["shadow_policy_sha256"], cost_policy_sha256=dataset["cost_policy_sha256"])
    if policy_hash != original.policy_identity:
        _fail("timing original frozen policy identity differs")
    registration = read_stage(original_root/"preregistered", stage="preregistered", plan_sha256=original.plan_sha256, parent_sha256=None)
    verify_timing_ledger(original, original_root, "PREREGISTERED")
    if _verify_reference(risk.parent_prepared_manifest_ref) != original_root/"prepared/manifest.json":
        _fail("timing original prepared reference differs")
    read_stage(original_root/"prepared", stage="prepared", plan_sha256=original.plan_sha256, parent_sha256=registration["stage_sha256"])
    verify_timing_ledger(original, original_root, "PREPARED")
    if _verify_reference(parent.v2_prepared_manifest_ref) != risk_root/"prepared/manifest.json":
        _fail("timing risk prepared reference differs")
    prepared = read_stage(risk_root/"prepared", stage="prepared", plan_sha256=risk.plan_sha256, parent_sha256=risk_registration["stage_sha256"])
    verify_timing_ledger(risk, risk_root, "PREPARED")
    labels = tuple(EntryLossLabelV2.model_validate(value) for value in json.loads((risk_root/"prepared/labels.json").read_text(encoding="utf-8")))
    source = EconomicEntryTrainingRequestV1.model_validate_json((original_root/"prepared/training_request.json").read_text(encoding="utf-8"))
    identity = EconomicEntryInputIdentityV1.model_validate_json((original_root/"prepared/identity.json").read_text(encoding="utf-8"))
    if (source != parent.training_request.source_request or source.input_identity_sha256 != identity.identity_sha256
            or source.model_dump(include=set(original.configuration.model_fields)) != original.configuration.model_dump()
            or identity.dataset_manifest_sha256 != original.dataset_manifest_ref.sha256
            or any(getattr(identity, field) != getattr(frozen, field) for field in identity.episode_identity())):
        _fail("timing frozen label/split/dataset identity differs")
    features = pd.read_parquet(original_root/"prepared/features.parquet")
    return parent, root, (risk, original, original_root, prepared, labels, source, features, None)


def _input(reference, loaded):
    path = _verify_reference(reference)
    if reference.role != "timing_input_manifest_v1" or path.name != "manifest.json" or path.parent.name != "prepared":
        _fail("timing prepared input role/path differs")
    identity = json.loads((path.parent/"identity.json").read_text(encoding="utf-8"))
    parent_hash = file_sha256(loaded[2]/"prepared/manifest.json")
    read_stage(path.parent, stage="prepared", plan_sha256=sha(identity), parent_sha256=parent_hash)
    recipe = identity["recipe"]
    frozen = json.loads((loaded[2]/"prepared/frozen_request.json").read_text(encoding="utf-8"))
    if (identity["frozen_parent_manifest_sha256"] != parent_hash or identity["recipe_sha256"] != sha(recipe)
            or identity["source_evidence"] != "CURRENT_DB_HISTORICAL_NON_VINTAGE" or identity["native_identity"] != "UNPROVEN"
            or any(identity[name] is not False for name in ("outcomes_read", "database_written", "sealed_accessed"))
            or identity["model_fits"] != 0 or tuple(recipe["feature_names"]) != CANDIDATE_NAMES[:-1]
            or sha(recipe["core_semantics"]) != sha(CORE_SEMANTICS) or sha(recipe["timing_semantics"]) != sha(TIMING_SEMANTICS)
            or recipe["terminal_weights"] != frozen["terminal_weights"]
            or set(recipe["roles"]) != {"lstm", "fund"} or set(recipe["roles"].values()) != set(frozen["terminal_weights"])
            or recipe["package_id"] != frozen["package_id"] or recipe["manifest_sha256"] != frozen["manifest_sha256"]
            or recipe["policy_sha256"] != frozen["shadow_policy_sha256"] or recipe["cost_sha256"] != frozen["cost_policy_sha256"]):
        _fail("timing prepared recipe/source/frozen identity differs")
    expected_files = ("economic_daily_feature_core_v1.py", "economic_entry_timing_features_v1.py", "economic_common_core_daily_source_v1.py")
    expected = {name: code_sha256(Path(__file__).with_name(name)) for name in expected_files}
    if recipe.get("implementation_hash_algorithm") != "UTF8_LF_BYTES_V1" or recipe["implementation"] != expected:
        _fail("timing input was generated by another source recipe")
    frame = pd.read_parquet(path.parent/"features.parquet")
    if file_sha256(path.parent/"features.parquet") != identity["rows_file_sha256"] or len(frame) != identity["rows"]:
        _fail("timing input bytes/row count differ")
    return frame, identity


def _scope(loaded, request):
    identity = EconomicEntryInputIdentityV1.model_validate_json((loaded[2]/"prepared/identity.json").read_text(encoding="utf-8"))
    if identity.identity_sha256 != request.parent_request.source_request.input_identity_sha256:
        _fail("timing parent input identity differs")
    return {"schema_version": "economic_entry_timing_scope_v1", "model_family": "ECONOMIC_ENTRY_TIMING_V1",
        "package_id": identity.package_id, "manifest_sha256": identity.manifest_sha256,
        "selection_runtime_semantics_hash": identity.selection_runtime_semantics_hash,
        "input_identity_sha256": identity.identity_sha256, "shadow_policy_sha256": identity.shadow_policy_sha256,
        "cost_policy_sha256": identity.cost_policy_sha256, "recipe_sha256": request.recipe_sha256,
        "input_rows_sha256": request.input_rows_sha256, "control_names": list(request.control_names),
        "candidate_names": list(request.candidate_names), "native_identity": "UNPROVEN", "deployable": False}


def build_timing_plan_v1(*, parent_plan_path, input_manifest_path):
    parent, root, loaded = _parent(parent_plan_path)
    reference = evidence_reference_for_file(input_manifest_path, role="timing_input_manifest_v1")
    inputs, identity = _input(reference, loaded)
    rows = assemble_timing_rows_v1(original_features=loaded[6], labels=loaded[4], inputs=inputs, parent_request=parent.training_request)
    request = TimingTrainingRequestV1(parent_request=parent.training_request, input_ref=reference,
        input_rows_sha256=timing_input_rows_sha256(inputs), recipe_sha256=identity["recipe_sha256"],
        common_fit_rows_sha256=timing_common_fit_sha256(rows), implementation_sha256=timing_implementation_sha256())
    return TimingStudyPlanV1(parent_plan_ref=evidence_reference_for_file(root/"preregistered/plan.json", role="timing_parent_v3_plan"),
        training_request=request, scope=_scope(loaded, request), simulator_sha256=parent.simulator_sha256,
        parent_lineage=(*parent.parent_lineage, parent.experiment_id, "H_TIMING_1_FIXED_CORE_13_VS_15"),
        expected_train_rows=int((rows.split.eq("train") & rows.timing_training_eligible).sum()),
        expected_validation_rows=int((rows.split.eq("validation") & rows.timing_training_eligible).sum()))


def timing_inputs(plan):
    if plan.parent_plan_ref.role != "timing_parent_v3_plan":
        _fail("timing parent evidence role differs")
    parent, _, loaded = _parent(_verify_reference(plan.parent_plan_ref))
    inputs, identity = _input(plan.training_request.input_ref, loaded)
    request = plan.training_request
    if (request.parent_request != parent.training_request or request.implementation_sha256 != timing_implementation_sha256()
            or request.recipe_sha256 != identity["recipe_sha256"] or request.input_rows_sha256 != timing_input_rows_sha256(inputs)
            or plan.scope != _scope(loaded, request) or parent.experiment_id not in plan.parent_lineage
            or plan.simulator_sha256 != file_sha256(Path(__file__).with_name("shadow_portfolio_policy.py"))):
        _fail("timing registered source/scope/implementation differs")
    rows = assemble_timing_rows_v1(original_features=loaded[6], labels=loaded[4], inputs=inputs, parent_request=parent.training_request)
    counts = tuple(int((rows.split.eq(split) & rows.timing_training_eligible).sum()) for split in ("train", "validation"))
    if timing_common_fit_sha256(rows) != request.common_fit_rows_sha256 or counts != (plan.expected_train_rows, plan.expected_validation_rows):
        _fail("timing registered common supervision differs")
    return loaded, inputs


def register_timing_stage(plan, loaded, root, stage, manifest, *, generated=0, evaluated=0):
    source = plan.training_request.parent_request.source_request
    parent = loaded[1]
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="exact_timing_attempt_v1", research_stage=stage,
        study_type=plan.study_type, hypothesis_family_id="economic_actual_open_entry_value_v1", parent_lineage=plan.parent_lineage,
        unique_variable="fixed_history_overnight_intraday_block_with_new_same_core_matched_fit", objective_contract=plan.objective_contract,
        dataset_identity=parent.dataset_identity, schema_identity=plan.schema_version, policy_identity=parent.policy_identity,
        planned_trial_count=2, generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="P0C_DEVELOPMENT_V1", dataset_identity=parent.dataset_identity,
            start_date=source.train_start, end_date=source.label_cutoff),),
        result_class="CONTROL_READY" if stage == "PREREGISTERED" else "EXPLORATORY", decision_use=plan.decision_use,
        evidence_refs=(evidence_reference_for_file(manifest, role=f"timing_{stage.lower()}"),))
    return AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def verify_timing_ledger(plan, root, stage):
    values = [record for record in AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").read()
        if record.experiment_id == plan.experiment_id and record.research_stage == stage]
    if len(values) != 1 or len(values[0].evidence_refs) != 1 or values[0].evidence_refs[0].sha256 != file_sha256(root/stage.lower()/"manifest.json"):
        _fail("timing stage registry identity differs")


def preregister_timing_study_v1(*, plan, output_root):
    plan = TimingStudyPlanV1.model_validate(plan.model_dump())
    loaded, _ = timing_inputs(plan)
    root = _root(output_root)/plan.experiment_id
    target = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json"))})
    register_timing_stage(plan, loaded, root, "PREREGISTERED", target/"manifest.json")
    return target/"plan.json"


def load_timing_study_v1(*, plan_path, output_root):
    plan = TimingStudyPlanV1.model_validate_json(Path(plan_path).read_text(encoding="utf-8"))
    root = _root(output_root)/plan.experiment_id
    if Path(plan_path).resolve() != root/"preregistered/plan.json":
        _fail("timing study must use its exact registered path")
    registered = read_stage(root/"preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    timing_inputs(plan)
    verify_timing_ledger(plan, root, "PREREGISTERED")
    return plan, root, registered


def train_timing_study_v1(*, plan_path, output_root, qe_training_idle):
    if qe_training_idle is not True:
        _fail("timing fit cannot overlap QE training")
    plan, root, registered = load_timing_study_v1(plan_path=plan_path, output_root=output_root)
    with _exclusive_file_lock(root/"fit.lock"):
        loaded, inputs = timing_inputs(plan)
        if (root/"trained").exists():
            read_stage(root/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
            register_timing_stage(plan, loaded, root, "TRAINED", root/"trained/manifest.json", generated=2)
            return root/"trained"
        attempt = root/"fit_attempt.json"
        if attempt.exists():
            _fail("timing prior fit attempt did not publish; explicit diagnosis required, no implicit refit")
        # Exclusive durable marker under the fit lock. Even a partial/crashed
        # marker blocks automatic refit; it is not a new publication stage.
        with attempt.open("xb") as handle:
            handle.write(_json_bytes({"plan_sha256": plan.plan_sha256, "planned_configurations": 2,
                "planned_heads": 4, "completed_heads": "UNPROVEN"}))
            handle.flush()
            os.fsync(handle.fileno())
        register_timing_stage(plan, loaded, root, "FIT_STARTED", attempt)
        fitted = train_timing_entry_v1(original_features=loaded[6], labels=loaded[4], inputs=inputs, request=plan.training_request)
        metadata = {"request": plan.training_request.model_dump(mode="json"), "scope": plan.scope,
            "request_sha256": plan.training_request.request_sha256, "diagnostics": fitted.diagnostics, "arms": {}}
        artifacts = {"split_receipt.parquet": _parquet_bytes(fitted.split_receipt)}
        for arm, value in fitted.arms.items():
            metadata["arms"][arm] = {"feature_names": list(value.feature_names), "common_bounds": value.common_bounds,
                "price_support": value.price_support, "diagnostics": value.diagnostics}
            artifacts[arm.lower()+"_return.txt"] = value.return_model.model_to_string().encode()
            artifacts[arm.lower()+"_risk.txt"] = value.risk_model.model_to_string().encode()
        artifacts["model_metadata.json"] = _json_bytes(metadata)
        target = publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"], artifacts=artifacts)
        register_timing_stage(plan, loaded, root, "TRAINED", target/"manifest.json", generated=2)
        return target


def load_fitted_timing_v1(*, plan_path, output_root):
    import lightgbm as lgb
    plan, root, registered = load_timing_study_v1(plan_path=plan_path, output_root=output_root)
    read_stage(root/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    verify_timing_ledger(plan, root, "TRAINED")
    metadata = json.loads((root/"trained/model_metadata.json").read_text(encoding="utf-8"))
    if (metadata["request"] != plan.training_request.model_dump(mode="json") or metadata["scope"] != plan.scope
            or metadata["request_sha256"] != plan.training_request.request_sha256 or set(metadata["arms"]) != set(ARMS)
            or lgb.__version__ != plan.training_request.parent_request.lightgbm_version):
        _fail("timing fitted contract/runtime differs")
    arms = {}
    for arm, names in ARMS.items():
        values = metadata["arms"][arm]
        models = [lgb.Booster(model_file=str(root/("trained/"+arm.lower()+suffix))) for suffix in ("_return.txt", "_risk.txt")]
        if tuple(values["feature_names"]) != names or any(tuple(model.feature_name()) != names for model in models):
            _fail("timing model dimension/order differs")
        arms[arm] = TimingArmV1(plan.training_request, arm, names, *models,
            {name: tuple(value) for name, value in values["common_bounds"].items()},
            {int(key): value for key, value in values["price_support"].items()}, values["diagnostics"])
    if arms["CORE_THIRTEEN"].common_bounds != arms["TIMING_FIFTEEN"].common_bounds or arms["CORE_THIRTEEN"].price_support != arms["TIMING_FIFTEEN"].price_support:
        _fail("timing fitted arms lack a shared prediction domain")
    return arms
