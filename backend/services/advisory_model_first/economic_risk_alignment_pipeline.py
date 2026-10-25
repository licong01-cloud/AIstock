"""Read-only parent consumer and immutable v2 preparation; no training dispatch."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1, EconomicEntryLabelV1
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _authorize, _json_bytes, _parquet_bytes, _prepared, _verify_reference,
    file_sha256, load_economic_study, load_fitted_economic_study, publish_stage, read_stage,
)
from backend.services.advisory_model_first.economic_entry_training import prepare_economic_training_rows
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossStudyPlanV2
from backend.services.advisory_model_first.economic_risk_alignment_labels import build_entry_loss_labels_v2
from backend.services.advisory_model_first.economic_risk_alignment_training import (
    prepare_entry_loss_training_v2, return_reuse_rows_sha256,
)
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def entry_loss_implementation_sha256() -> str:
    paths = [Path(__file__).with_name(f"economic_risk_alignment_{name}.py")
             for name in ("contracts", "labels", "training", "inference", "pipeline")]
    return canonical_json_sha256({path.name: file_sha256(path) for path in paths})


def _root(plan: EntryLossStudyPlanV2, output_root: str | Path) -> Path:
    declared = Path(output_root)
    if not declared.is_absolute() or declared.resolve().drive.upper() == "C:":
        _fail("entry loss output must be an absolute non-C durable root")
    root = declared.resolve()
    target = root / plan.experiment_id
    if target.resolve() != target or not target.is_relative_to(root):
        _fail("entry loss study path escapes its declared output root")
    return target


def _parent(plan: EntryLossStudyPlanV2):
    plan_path = _verify_reference(plan.parent_plan_ref)
    parent_plan, parent_root = load_economic_study(plan_path, output_root=plan_path.parents[2])
    if parent_plan.experiment_id not in plan.parent_lineage:
        _fail("v2 lineage omits its exact frozen parent study")
    # Reauthorize consumed development BEFORE loading any parent outcome artifact.
    access = _authorize(parent_plan)
    prepared_path = _verify_reference(plan.parent_prepared_manifest_ref)
    trained_path = _verify_reference(plan.parent_trained_manifest_ref)
    if prepared_path != parent_root / "prepared/manifest.json" or trained_path != parent_root / "trained/manifest.json":
        _fail("v2 parent manifests refer to a different study")
    prepared = _prepared(parent_root, parent_plan)
    trained = read_stage(parent_root / "trained", stage="trained", plan_sha256=parent_plan.plan_sha256,
                         parent_sha256=prepared["stage_sha256"])
    return parent_plan, parent_root, prepared, trained, access


def _register(plan, parent_plan, root, stage, manifest, *, blocked=False):
    configuration = parent_plan.configuration
    record = build_trial_record(
        experiment_id=plan.experiment_id, attempt_id="exact_attempt_v2", research_stage=stage,
        study_type=plan.study_type, hypothesis_family_id="economic_actual_open_entry_value_v1",
        parent_lineage=plan.parent_lineage, unique_variable=plan.hypothesis,
        objective_contract=plan.objective_contract, dataset_identity=parent_plan.dataset_identity,
        schema_identity="entry_loss_label_v2", policy_identity=parent_plan.policy_identity,
        planned_trial_count=1, generated_trial_count=0, evaluated_trial_count=0, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="P0C_DEVELOPMENT_V1", dataset_identity=parent_plan.dataset_identity,
                                         start_date=configuration.train_start, end_date=configuration.label_cutoff),),
        result_class="INCOMPLETE_NEGATIVE" if blocked else "CONTROL_READY", decision_use=plan.decision_use,
        evidence_refs=(evidence_reference_for_file(manifest, role=f"entry_loss_{stage.lower()}"),),
    )
    return AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").append_batch((record,))


def preregister_entry_loss_study_v2(*, plan: EntryLossStudyPlanV2, output_root: str | Path) -> Path:
    plan = EntryLossStudyPlanV2.model_validate(plan.model_dump())
    if plan.implementation_sha256 != entry_loss_implementation_sha256():
        _fail("v2 plan does not bind its exact implementation")
    parent_plan, _, _, _, access = _parent(plan)
    root = _root(plan, output_root)
    # Separate immutable container reuses publication, not v1 business semantics.
    published = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256,
                              parent_sha256=None, artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json")),
                                                           "window_access.json": _json_bytes(access)})
    _register(plan, parent_plan, root, "PREREGISTERED", published / "manifest.json")
    return published / "plan.json"


def prepare_entry_loss_study_v2(*, plan_path: str | Path, output_root: str | Path) -> Path:
    plan = EntryLossStudyPlanV2.model_validate_json(Path(plan_path).read_text(encoding="utf-8"))
    root = _root(plan, output_root)
    if Path(plan_path).resolve() != root / "preregistered/plan.json":
        _fail("v2 preparation requires its registered plan identity")
    if plan.implementation_sha256 != entry_loss_implementation_sha256():
        _fail("v2 implementation drift requires a new preregistration, not silent retry")
    registration = read_stage(root / "preregistered", stage="preregistered", plan_sha256=plan.plan_sha256,
                              parent_sha256=None)
    records = AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").read()
    matching = [value for value in records if value.experiment_id == plan.experiment_id and value.research_stage == "PREREGISTERED"]
    if len(matching) != 1 or matching[0].evidence_refs[0].sha256 != file_sha256(root / "preregistered/manifest.json"):
        _fail("v2 registration ledger differs; exact registration retry is required")
    parent_plan, parent_root, prepared, _, _ = _parent(plan)
    if (root / "prepared").exists():
        read_stage(root / "prepared", stage="prepared", plan_sha256=plan.plan_sha256,
                   parent_sha256=registration["stage_sha256"])
        summary = json.loads((root / "prepared/reuse_audit.json").read_text(encoding="utf-8"))
        _register(plan, parent_plan, root, "PREPARED", root / "prepared/manifest.json",
                  blocked=summary["weight_reuse_status"] != "ELIGIBILITY_VERIFIED")
        return root / "prepared"
    import pyarrow.parquet as pq
    for name, budget in (("prices.parquet", plan.resource_max_market_rows),
                         ("features.parquet", parent_plan.configuration.resource_max_rows),
                         ("frozen_episodes.parquet", parent_plan.configuration.resource_max_rows)):
        if pq.ParquetFile(parent_root / "prepared" / name).metadata.num_rows > budget:
            _fail("v2 parent artifact exceeds registered row budget")
    originals = tuple(EconomicEntryLabelV1.model_validate(value) for value in
                      json.loads((parent_root / "prepared/labels.json").read_text(encoding="utf-8")))
    identity = EconomicEntryInputIdentityV1.model_validate_json((parent_root / "prepared/identity.json").read_text(encoding="utf-8"))
    labels = build_entry_loss_labels_v2(
        original_labels=originals, episodes=pd.read_parquet(parent_root / "prepared/frozen_episodes.parquet"),
        prices=pd.read_parquet(parent_root / "prepared/prices.parquet"),
        trading_calendar=json.loads((parent_root / "prepared/calendar.json").read_text(encoding="utf-8")), identity=identity,
    )
    parent = load_fitted_economic_study(plan_path=plan.parent_plan_ref.artifact_uri, output_root=parent_root.parent)
    features = pd.read_parquet(parent_root / "prepared/features.parquet")
    original_rows = prepare_economic_training_rows(features=features, labels=originals, request=parent.request)
    rows_digest = return_reuse_rows_sha256(original_rows, parent.request)
    rows, summary = prepare_entry_loss_training_v2(features=features, labels=labels, parent=parent,
                                                 expected_parent_rows_sha256=rows_digest)
    summary.update({"original_prepared_stage_sha256": prepared["stage_sha256"], "new_model_trained": False,
                    "new_return_model_trained": False, "parent_fit_rows_sha256": rows_digest,
                    "source_evidence": identity.source_evidence,
                    "evidence_limitations": list(identity.evidence_limitations),
                    "next_action": "REUSE_BLOCKED_NO_FIT" if summary["weight_reuse_status"] != "ELIGIBILITY_VERIFIED"
                    else "SEPARATE_REGISTERED_FIT_REQUIRED"})
    published = publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256,
                              parent_sha256=registration["stage_sha256"], artifacts={
                                  "labels.json": _json_bytes([value.model_dump(mode="json") for value in labels]),
                                  "reuse_audit.json": _json_bytes(summary), "risk_rows.parquet": _parquet_bytes(rows),
                              })
    _register(plan, parent_plan, root, "PREPARED", published / "manifest.json",
              blocked=summary["weight_reuse_status"] != "ELIGIBILITY_VERIFIED")
    return published
