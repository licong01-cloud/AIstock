"""Immutable, separately registered aligned fits; no DB or QE dispatch."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryStudyPlanV3, AlignedEntryTrainingRequestV3
from backend.services.advisory_model_first.economic_entry_aligned_training import (
    AlignedEntryTrainingResultV3, assemble_aligned_rows_v3, common_fit_rows_sha256,
    prepare_aligned_rows_v3, risk_labels_content_sha256, train_aligned_entry_model_v3,
)
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryTrainingRequestV1
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, _parquet_bytes, _verify_reference, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossLabelV2, EntryLossStudyPlanV2
from backend.services.advisory_model_first.economic_risk_alignment_pipeline import _parent, entry_loss_implementation_sha256
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def aligned_implementation_sha256() -> str:
    paths = [Path(__file__).with_name(f"economic_entry_aligned_{name}.py")
             for name in ("contracts", "training", "inference", "pipeline", "evaluation")]
    return canonical_json_sha256({path.name: file_sha256(path) for path in paths})


def _sources(v2_plan_ref, v2_prepared_manifest_ref):
    path = _verify_reference(v2_plan_ref)
    plan = EntryLossStudyPlanV2.model_validate_json(path.read_text(encoding="utf-8"))
    root = path.parent.parent
    if path != root / "preregistered/plan.json" or root.name != plan.experiment_id:
        _fail("aligned source plan does not match its v2 study path")
    if plan.implementation_sha256 != entry_loss_implementation_sha256():
        _fail("aligned source v2 implementation identity differs")
    # This verifies the original source chain and consumed-window access first.
    parent_plan, parent_root, _, _, _ = _parent(plan)
    registration = read_stage(root / "preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    prepared_path = _verify_reference(v2_prepared_manifest_ref)
    if prepared_path != root / "prepared/manifest.json":
        _fail("aligned risk labels come from a different v2 study")
    prepared = read_stage(root / "prepared", stage="prepared", plan_sha256=plan.plan_sha256,
                          parent_sha256=registration["stage_sha256"])
    labels = tuple(EntryLossLabelV2.model_validate(value) for value in json.loads((root / "prepared/labels.json").read_text(encoding="utf-8")))
    source_request = EconomicEntryTrainingRequestV1.model_validate_json((parent_root / "prepared/training_request.json").read_text(encoding="utf-8"))
    features = pd.read_parquet(parent_root / "prepared/features.parquet")
    metadata = json.loads((parent_root / "trained/model_metadata.json").read_text(encoding="utf-8"))
    return plan, parent_plan, parent_root, prepared, labels, source_request, features, metadata


def build_aligned_plan_v3(*, v2_plan_path: str | Path) -> AlignedEntryStudyPlanV3:
    path = Path(v2_plan_path)
    refs = (evidence_reference_for_file(path, role="risk_v2_plan"),
            evidence_reference_for_file(path.parent.parent / "prepared/manifest.json", role="risk_v2_prepared"))
    v2, parent_plan, _, _, labels, source, features, metadata = _sources(*refs)
    rows = assemble_aligned_rows_v3(features=features, labels=labels, source_request=source)
    request = AlignedEntryTrainingRequestV3(
        source_request=source, risk_labels_content_sha256=risk_labels_content_sha256(labels),
        common_fit_rows_sha256=common_fit_rows_sha256(rows, source.feature_names),
        implementation_sha256=aligned_implementation_sha256(), lightgbm_version=metadata["lightgbm_version"],
    )
    return AlignedEntryStudyPlanV3(
        v2_plan_ref=refs[0], v2_prepared_manifest_ref=refs[1], training_request=request,
        simulator_sha256=file_sha256(Path(__file__).with_name("shadow_portfolio_policy.py")),
        parent_lineage=(parent_plan.experiment_id, v2.experiment_id, "aligned_entry_loss_v3"),
        expected_train_rows=int((rows.split.eq("train") & rows.common_training_eligible).sum()),
        expected_validation_rows=int((rows.split.eq("validation") & rows.common_training_eligible).sum()),
    )


def _root(plan, output_root):
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:":
        _fail("aligned output must be a non-C absolute durable root")
    target = root / plan.experiment_id
    if target.resolve() != target or not target.is_relative_to(root):
        _fail("aligned study path escapes its durable root")
    return target


def register_aligned_stage(plan, parent_plan, root, stage, manifest, *, generated=0, evaluated=0):
    configuration = plan.training_request.source_request
    record = build_trial_record(
        experiment_id=plan.experiment_id, attempt_id="exact_attempt_v3", research_stage=stage,
        study_type=plan.study_type, hypothesis_family_id="economic_actual_open_entry_value_v1", parent_lineage=plan.parent_lineage,
        unique_variable="common_execution_proven_supervision_return_and_entry_loss_refit", objective_contract=plan.objective_contract,
        dataset_identity=parent_plan.dataset_identity, schema_identity="aligned_entry_training_request_v3", policy_identity=parent_plan.policy_identity,
        planned_trial_count=1, generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="P0C_DEVELOPMENT_V1", dataset_identity=parent_plan.dataset_identity,
                                         start_date=configuration.train_start, end_date=configuration.label_cutoff),),
        result_class="CONTROL_READY" if generated == 0 else "EXPLORATORY", decision_use=plan.decision_use,
        evidence_refs=(evidence_reference_for_file(manifest, role=f"aligned_{stage.lower()}"),),
    )
    return AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").append_batch((record,))


def preregister_aligned_study_v3(*, plan: AlignedEntryStudyPlanV3, output_root: str | Path) -> Path:
    plan = AlignedEntryStudyPlanV3.model_validate(plan.model_dump())
    if plan.training_request.implementation_sha256 != aligned_implementation_sha256():
        _fail("aligned plan differs from the current implementation")
    v2, parent_plan, _, prepared, labels, source, features, metadata = _sources(plan.v2_plan_ref, plan.v2_prepared_manifest_ref)
    if parent_plan.experiment_id not in plan.parent_lineage or v2.experiment_id not in plan.parent_lineage:
        _fail("aligned lineage omits its original or risk-label source")
    if source != plan.training_request.source_request or metadata["lightgbm_version"] != plan.training_request.lightgbm_version:
        _fail("aligned original source/version differs from registered configuration")
    if plan.simulator_sha256 != file_sha256(Path(__file__).with_name("shadow_portfolio_policy.py")):
        _fail("aligned simulator identity differs")
    rows = prepare_aligned_rows_v3(features=features, labels=labels, request=plan.training_request)
    counts = tuple(int((rows.split.eq(split) & rows.common_training_eligible).sum()) for split in ("train", "validation"))
    if counts != (plan.expected_train_rows, plan.expected_validation_rows):
        _fail("aligned registered supervision counts differ")
    root = _root(plan, output_root)
    published = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
                              artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json")),
                                         "source_receipt.json": _json_bytes({"risk_prepared_sha256": prepared["stage_sha256"],
                                                                            "common_fit_rows_sha256": plan.training_request.common_fit_rows_sha256,
                                                                            "train_rows": counts[0], "validation_rows": counts[1],
                                                                            "candidate_rows_preserved": len(rows), "sealed_holdout_accessed": False})})
    register_aligned_stage(plan, parent_plan, root, "PREREGISTERED", published / "manifest.json")
    return published / "plan.json"


def load_aligned_study_v3(*, plan_path: str | Path, output_root: str | Path):
    plan = AlignedEntryStudyPlanV3.model_validate_json(Path(plan_path).read_text(encoding="utf-8"))
    root = _root(plan, output_root)
    if Path(plan_path).resolve() != root / "preregistered/plan.json":
        _fail("aligned plan path is not the registered study")
    registration = read_stage(root / "preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    if plan.training_request.implementation_sha256 != aligned_implementation_sha256():
        _fail("aligned implementation changed; no silent continuation")
    if plan.simulator_sha256 != file_sha256(Path(__file__).with_name("shadow_portfolio_policy.py")):
        _fail("aligned simulator changed after registration")
    verify_aligned_stage_ledger(plan, root, "PREREGISTERED", root / "preregistered/manifest.json")
    return plan, root, registration


def verify_aligned_stage_ledger(plan, root, stage, manifest):
    records = AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").read()
    matching = [value for value in records if value.experiment_id == plan.experiment_id and value.research_stage == stage]
    if len(matching) != 1 or len(matching[0].evidence_refs) != 1 or matching[0].evidence_refs[0].sha256 != file_sha256(manifest):
        _fail("aligned stage differs from its registry; exact retry registration required")


def train_aligned_study_v3(*, plan_path: str | Path, output_root: str | Path) -> Path:
    _, root, _ = load_aligned_study_v3(plan_path=plan_path, output_root=output_root)
    with _exclusive_file_lock(root / "fit.lock"):
        return _train_locked(plan_path=plan_path, output_root=output_root)


def _train_locked(*, plan_path: str | Path, output_root: str | Path) -> Path:
    plan, root, registered = load_aligned_study_v3(plan_path=plan_path, output_root=output_root)
    _, parent_plan, _, _, labels, source, features, _ = _sources(plan.v2_plan_ref, plan.v2_prepared_manifest_ref)
    if (root / "trained").exists():
        read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
        register_aligned_stage(plan, parent_plan, root, "TRAINED", root / "trained/manifest.json", generated=1)
        return root / "trained"
    fitted = train_aligned_entry_model_v3(features=features, labels=labels, request=plan.training_request)
    if (fitted.diagnostics["train_rows"], fitted.diagnostics["validation_rows"]) != (plan.expected_train_rows, plan.expected_validation_rows):
        _fail("aligned real fit supervision counts differ")
    metadata = {"training_request": plan.training_request.model_dump(mode="json"),
                "training_request_sha256": plan.training_request.request_sha256,
                "training_input_sha256": plan.training_request.training_input_sha256,
                "feature_names": list(source.feature_names), "feature_bounds": fitted.feature_bounds,
                "price_support": fitted.price_support, "diagnostics": fitted.diagnostics,
                "effective_parameters": source.effective_parameters, "deployable": False}
    published = publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256,
                              parent_sha256=registered["stage_sha256"], artifacts={
                                  "return_model.txt": fitted.return_model.model_to_string().encode(),
                                  "entry_loss_model.txt": fitted.risk_model.model_to_string().encode(),
                                  "model_metadata.json": _json_bytes(metadata), "split_receipt.parquet": _parquet_bytes(fitted.split_receipt)})
    register_aligned_stage(plan, parent_plan, root, "TRAINED", published / "manifest.json", generated=1)
    return published


def load_fitted_aligned_study_v3(*, plan_path: str | Path, output_root: str | Path):
    import lightgbm as lgb
    plan, root, registered = load_aligned_study_v3(plan_path=plan_path, output_root=output_root)
    read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    verify_aligned_stage_ledger(plan, root, "TRAINED", root / "trained/manifest.json")
    metadata = json.loads((root / "trained/model_metadata.json").read_text(encoding="utf-8"))
    if (metadata["training_request_sha256"] != plan.training_request.request_sha256
            or metadata["training_input_sha256"] != plan.training_request.training_input_sha256
            or metadata["training_request"] != plan.training_request.model_dump(mode="json")
            or metadata["effective_parameters"] != plan.training_request.source_request.effective_parameters
            or lgb.__version__ != plan.training_request.lightgbm_version):
        _fail("aligned fitted model metadata/version differs")
    models = [lgb.Booster(model_file=str(root / "trained" / name)) for name in ("return_model.txt", "entry_loss_model.txt")]
    if any(tuple(value.feature_name()) != plan.training_request.source_request.feature_names for value in models):
        _fail("aligned model feature order differs")
    return AlignedEntryTrainingResultV3(*models, plan.training_request, {name: tuple(value) for name, value in metadata["feature_bounds"].items()},
                                       {int(key): value for key, value in metadata["price_support"].items()}, metadata["diagnostics"],
                                       pd.read_parquet(root / "trained/split_receipt.parquet"))
