from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first import economic_risk_alignment_pipeline as module
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, file_sha256, read_stage
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossStudyPlanV2
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import evidence_reference_for_file

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def _registration(tmp_path, study, monkeypatch):
    references = []
    for name in ("plan", "prepared", "trained"):
        path = tmp_path / f"{name}.json"
        path.write_bytes(_json_bytes({"unit_identity": name}))
        references.append(evidence_reference_for_file(path, role=name))
    plan = EntryLossStudyPlanV2(parent_plan_ref=references[0], parent_prepared_manifest_ref=references[1],
                               parent_trained_manifest_ref=references[2], implementation_sha256=module.entry_loss_implementation_sha256(),
                               parent_lineage=("parent",))
    parent = SimpleNamespace(configuration=study["request"], dataset_identity="a" * 64, policy_identity="b" * 64)
    monkeypatch.setattr(module, "_parent", lambda value: (parent, tmp_path / "parent", {}, {}, {"sealed_holdout_accessed": False}))
    return plan, parent


def test_registration_is_exact_retry_and_prepared_retry_does_not_rebuild(tmp_path, study, monkeypatch):
    plan, parent = _registration(tmp_path, study, monkeypatch)
    path = module.preregister_entry_loss_study_v2(plan=plan, output_root=tmp_path / "output")
    assert module.preregister_entry_loss_study_v2(plan=plan, output_root=tmp_path / "output") == path
    root = path.parent.parent
    registered = read_stage(path.parent, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    # Minimal immutable business receipt fixture; not a successful model/run claim.
    summary = {"weight_reuse_status": "BLOCKED_ELIGIBILITY_DRIFT", "new_model_trained": False}
    module.publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256,
                         parent_sha256=registered["stage_sha256"], artifacts={"reuse_audit.json": _json_bytes(summary)})
    monkeypatch.setattr(module, "build_entry_loss_labels_v2", lambda **kwargs: pytest.fail("exact retry must not rebuild"))
    assert module.prepare_entry_loss_study_v2(plan_path=path, output_root=tmp_path / "output") == root / "prepared"
    assert file_sha256(path) == registered["files"]["plan.json"]["sha256"]
    records = module.AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").read()
    assert len(records) == 2 and records[-1].generated_trial_count == 0
    assert records[-1].result_class.value == "INCOMPLETE_NEGATIVE"


def test_code_drift_fails_before_source_read_or_registration(tmp_path, study, monkeypatch):
    plan, _ = _registration(tmp_path, study, monkeypatch)
    monkeypatch.setattr(module, "_parent", lambda value: pytest.fail("drift must fail before source access"))
    with pytest.raises(AdvisoryModelFirstError, match="exact implementation"):
        module.preregister_entry_loss_study_v2(plan=plan.model_copy(update={"implementation_sha256": "c" * 64}),
                                              output_root=tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_parent_reference_tamper_fails_before_outcome_loading(tmp_path, study, monkeypatch):
    plan, _ = _registration(tmp_path, study, monkeypatch)
    monkeypatch.undo()
    # Restore real consumer, but a changed parent plan must fail at the first hash.
    from pathlib import Path
    Path(plan.parent_plan_ref.artifact_uri).write_bytes(b"tampered\n")
    monkeypatch.setattr(module, "load_economic_study", lambda *a, **k: pytest.fail("tampered reference cannot load"))
    with pytest.raises(AdvisoryModelFirstError, match="identity changed"):
        module._parent(plan)
