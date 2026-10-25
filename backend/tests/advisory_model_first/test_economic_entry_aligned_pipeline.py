from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first import economic_entry_aligned_pipeline as module
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryStudyPlanV3
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_entry_aligned_training import _inputs

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def _plan(tmp_path, study, monkeypatch):
    args = _inputs(study)
    request = args["request"].model_copy(update={"implementation_sha256": module.aligned_implementation_sha256()})
    refs = []
    for name in ("v2_plan", "v2_prepared"):
        path = tmp_path / f"{name}.json"
        path.write_bytes(b"{}\n")
        refs.append(evidence_reference_for_file(path, role=name))
    plan = AlignedEntryStudyPlanV3(v2_plan_ref=refs[0], v2_prepared_manifest_ref=refs[1], training_request=request,
                                  simulator_sha256=module.file_sha256(module.Path(module.__file__).with_name("shadow_portfolio_policy.py")),
                                  parent_lineage=("parent", "risk_v2"), expected_train_rows=53, expected_validation_rows=30)
    parent = SimpleNamespace(experiment_id="parent", dataset_identity="a" * 64, policy_identity="b" * 64)
    risk = SimpleNamespace(experiment_id="risk_v2")
    monkeypatch.setattr(module, "_sources", lambda *a: (risk, parent, tmp_path, {"stage_sha256": "c" * 64},
                                                        args["labels"], study["request"], args["features"],
                                                        {"lightgbm_version": request.lightgbm_version}))
    return plan, parent


def test_exact_retry_trained_stage_does_not_fit_again(tmp_path, study, monkeypatch):
    plan, parent = _plan(tmp_path, study, monkeypatch)
    path = module.preregister_aligned_study_v3(plan=plan, output_root=tmp_path / "output")
    _, root, registered = module.load_aligned_study_v3(plan_path=path, output_root=tmp_path / "output")
    assert module.preregister_aligned_study_v3(plan=plan, output_root=tmp_path / "output") == path
    published = publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256,
                              parent_sha256=registered["stage_sha256"], artifacts={"unit_no_business_model.json": _json_bytes({"fixture_only": True})})
    module.register_aligned_stage(plan, parent, root, "TRAINED", published / "manifest.json", generated=1)
    monkeypatch.setattr(module, "train_aligned_entry_model_v3", lambda **kwargs: pytest.fail("exact retry may not fit again"))
    assert module.train_aligned_study_v3(plan_path=path, output_root=tmp_path / "output") == published
    assert len(module.AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").read()) == 2


def test_preregister_rejects_different_counts_before_publication(tmp_path, study, monkeypatch):
    plan, _ = _plan(tmp_path, study, monkeypatch)
    with pytest.raises(AdvisoryModelFirstError, match="counts differ"):
        module.preregister_aligned_study_v3(plan=plan.model_copy(update={"expected_train_rows": 99}), output_root=tmp_path / "output")
    assert not (tmp_path / "output").exists()
