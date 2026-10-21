"""Reuse validated factor outputs without mutating a published predecessor."""
import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release import monthly_fact_correction as subject
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.monthly_build_resume import _atomic, _signature
from backend.services.dataset_release.monthly_unified import (
    MonthlyReleaseConflict, MonthlyReleaseService,
)


def fixture(tmp_path):
    repair = tmp_path / "data_repair_staging"
    repair.mkdir()
    source = repair / "moneyflow.h5"
    frame = pd.DataFrame({"x": [1.0, 2.0]}, index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-08-31"), "300114.SZ"),
         (pd.Timestamp("2026-09-30"), "302132.SZ")], names=["datetime", "instrument"]))
    frame.to_hdf(source, key="data", format="table", data_columns=["datetime", "instrument"])
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    receipt = repair / "receipt.json"
    receipt.write_text(json.dumps({"schema_version": "aistock_auxiliary_fact_component_repair_v1",
        "status": "PRIVATE_COMPONENTS_NOT_A_RELEASE", "database_write": False,
        "profile_write": False, "runtime_action": False,
        "components": {"moneyflow": {"target_path": str(source), "target_sha256": digest,
            "existing_finite_values_unchanged": True}}}))
    static = repair / "static_factors.parquet"
    frame.to_parquet(static)
    static_receipt = repair / "static.json"
    static_receipt.write_text(json.dumps({
        "schema_version": "aistock_private_full_fact_static_repair_v1",
        "status": "PRIVATE_COMPONENT_NOT_A_RELEASE", "database_write": False,
        "profile_write": False, "runtime_action": False,
        "input_sha256": {"moneyflow": digest}, "target_path": str(static),
        "target_sha256": hashlib.sha256(static.read_bytes()).hexdigest()}))
    inputs = {"components": [{"dataset": "moneyflow", "receipt_path": str(receipt),
        "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest()},
        {"dataset": "static_factors", "receipt_path": str(static_receipt),
         "receipt_sha256": hashlib.sha256(static_receipt.read_bytes()).hexdigest()}]}
    return repair, source, inputs


def test_capture_reuses_existing_receipt_and_does_not_reaudit_history(tmp_path):
    repair, source, inputs = fixture(tmp_path)
    result = subject.capture_factor_repairs(inputs, repair_root=repair, cutoff="2026-09-30")
    assert result["files"][0]["source_path"] == str(source)
    assert result["files"][0]["dataset"] == "moneyflow"
    assert result["database_write"] is False
    assert result["historical_business_audit_performed"] is False


@pytest.mark.parametrize("defect", ["receipt_drift", "outside", "duplicate", "wrong_dataset", "traversal", "dependency"])
def test_capture_rejects_wrong_or_ambiguous_input(tmp_path, defect):
    repair, source, inputs = fixture(tmp_path)
    if defect == "receipt_drift":
        Path(inputs["components"][0]["receipt_path"]).write_text("{}")
    elif defect == "outside":
        inputs["components"][0]["receipt_path"] = str(tmp_path / "unregistered.json")
    elif defect == "duplicate":
        inputs["components"] *= 2
    elif defect == "wrong_dataset":
        inputs["components"][0]["dataset"] = "model"
    elif defect == "traversal":
        inputs["components"][0]["receipt_path"] = str(repair / ".." / "unregistered.json")
    else:
        row = inputs["components"][1]
        path = Path(row["receipt_path"])
        data = json.loads(path.read_bytes())
        data["input_sha256"]["moneyflow"] = "0" * 64
        path.write_text(json.dumps(data))
        row["receipt_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        subject.capture_factor_repairs(inputs, repair_root=repair, cutoff="2026-09-30")


def prepared(tmp_path):
    repair, source, inputs = fixture(tmp_path)
    authority = subject.capture_factor_repairs(inputs, repair_root=repair, cutoff="2026-09-30")
    root = tmp_path / "releases/.staging/correction.building"
    (root / "factor_bundle").mkdir(parents=True)
    target = root / "factor_bundle/moneyflow.h5"
    old_frame = pd.DataFrame({"x": [2.0]}, index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-09-30"), "302132.SZ")], names=["datetime", "instrument"]))
    old_frame.to_hdf(target, key="data", format="table", data_columns=["datetime", "instrument"])
    published = tmp_path / "published.h5"
    os.link(target, published)
    static = root / "factor_bundle/static_factors.parquet"
    old_frame.to_parquet(static)
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    flow_proof = authority["files"][0]["validation_payload"]["components"]["moneyflow"]
    flow_proof["baseline_sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    alias = {"status": "PASS", "moneyflow_sha256": flow_proof["baseline_sha256"],
        "expected": 1, "resolved": 1, "unknown": 0, "nonfinite": 0, "mismatched": 0}
    alias_path = root / "factor_bundle/moneyflow_alias_coverage_v1.json"
    alias_path.write_text(json.dumps(alias))
    factor = {"cutoff": "2026-09-30", "files": [{"dataset": "moneyflow",
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(), "size": target.stat().st_size,
        "target_signature": list(_signature(target)), "inherited_rows": 0, "month_rows": 1},
        {"dataset": "static_factors", "sha256": hashlib.sha256(static.read_bytes()).hexdigest(),
         "size": static.stat().st_size, "target_signature": list(_signature(static))}],
        "alias_coverage": alias, "metadata_files": [{"relative_path": alias_path.name,
            "sha256": hashlib.sha256(alias_path.read_bytes()).hexdigest(),
            "size": alias_path.stat().st_size, "target_signature": list(_signature(alias_path))}]}
    nested = {"refs": {"factor_receipt": cas.put_json(factor).as_dict()}}
    prepare = {"prepare_receipt_ref": cas.put_json(nested).as_dict()}
    record = {"binding_digest": "b" * 64, "steps": {"prepare": cas.put_json(prepare).as_dict()},
        "files": {"factor_bundle/moneyflow.h5": list(_signature(target)),
                  "factor_bundle/moneyflow_alias_coverage_v1.json": list(_signature(alias_path)),
                  "factor_bundle/static_factors.parquet": list(_signature(static))}}
    journal = tmp_path / "journal.json"
    _atomic(journal, record)
    resume = SimpleNamespace(root=root, path=journal, record=record,
        context=SimpleNamespace(operation_id="operation", checkpoint=lambda: None))
    binding = {"authority_ref": cas.put_json(authority).as_dict(), "operation_id": "operation",
        "target_cutoff": "2026-09-30"}
    plan = {"target_cutoff": "2026-09-30", subject.BINDING: binding}
    return resume, prepare, plan, cas, source, target, published


def test_apply_copy_on_write_preserves_old_release_and_is_idempotent(tmp_path):
    resume, prepare, plan, cas, source, target, old = prepared(tmp_path)
    before = old.read_bytes()
    result = subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    assert old.read_bytes() == before and target.read_bytes() == source.read_bytes()
    nested = cas.get_json(result["prepare_receipt_ref"])
    factor = cas.get_json(nested["refs"]["factor_receipt"])
    pin = factor["files"][0]
    assert pin["inherited_rows"] == 1 and pin["month_rows"] == 1
    assert pin["target_signature"] == list(_signature(target))
    assert factor["alias_coverage"]["expected"] == factor["alias_coverage"]["resolved"] == 1
    assert factor["alias_coverage"]["moneyflow_sha256"] == pin["sha256"]
    assert subject.apply_factor_repairs(resume, prepare=result, plan=plan, cas=cas) == result


def test_source_drift_never_replaces_completed_output(tmp_path):
    resume, prepare, plan, cas, source, target, old = prepared(tmp_path)
    before = target.read_bytes()
    with source.open("ab") as writer:
        writer.write(b"unexpected")
    with pytest.raises(ValueError, match="changed|differs"):
        subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    assert target.read_bytes() == before and old.read_bytes() == before


def test_interrupted_replace_recovers_without_reexport(tmp_path, monkeypatch):
    resume, prepare, plan, cas, source, target, old = prepared(tmp_path)
    before = old.read_bytes()
    replace = subject.os.replace

    def interrupted(a, b):
        if Path(b) == target:
            raise OSError("interrupted")
        return replace(a, b)

    monkeypatch.setattr(subject.os, "replace", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    monkeypatch.setattr(subject.os, "replace", replace)
    resume.record = subject.recover_factor_repairs(root=resume.root, journal=resume.path,
        record=resume.record, cas=cas)
    assert target.read_bytes() == source.read_bytes() and old.read_bytes() == before
    result = cas.get_json(resume.record["steps"]["prepare"])
    assert subject.apply_factor_repairs(resume, prepare=result, plan=plan, cas=cas) == result


def service_fixture(tmp_path):
    repair, source, inputs = fixture(tmp_path)
    root = tmp_path / "operation"
    root.mkdir()
    plan = {"candidate_root": str(tmp_path / "releases/correction"), "target_cutoff": "2026-09-30",
            "monthly_ready_correction": {"base_manifest_ref": {"sha256": "a" * 64}}}
    state = {"status": "FAILED", "current_stage": "BUILD"}
    checkpoints = {"SOURCE": {"canonical_sha256": "b" * 64}}
    store = SimpleNamespace(operation_root=lambda _: root, read_plan=lambda _: plan,
        read_state=lambda _: state, read_checkpoint=lambda _, stage: checkpoints.get(stage),
        replace_plan=lambda _, value: plan.update(value))
    return SimpleNamespace(store=store), inputs, plan, state, checkpoints, CASStore(
        ControlStore.initialize(tmp_path / "control").root)


def test_formal_binding_is_idempotent_and_does_not_activate_or_resume(tmp_path):
    service, inputs, plan, state, checkpoints, cas = service_fixture(tmp_path)
    result = MonthlyReleaseService.bind_factor_repairs(service, "operation", inputs=inputs, principal="operator", cas=cas)
    assert result["status"] == "BOUND" and state == {"status": "FAILED", "current_stage": "BUILD"}
    assert set(checkpoints) == {"SOURCE"}
    assert plan[subject.BINDING]["activation_authorized"] is False
    assert result == MonthlyReleaseService.bind_factor_repairs(
        service, "operation", inputs=inputs, principal="operator", cas=cas)
    with pytest.raises(MonthlyReleaseConflict, match="binding changed"):
        MonthlyReleaseService.bind_factor_repairs(service, "operation", inputs={}, principal="operator", cas=cas)


@pytest.mark.parametrize("defect", ["running", "not_correction", "no_source", "build_done"])
def test_formal_binding_requires_idle_private_correction(tmp_path, defect):
    service, inputs, plan, state, checkpoints, cas = service_fixture(tmp_path)
    if defect == "running":
        state["status"] = "RUNNING"
    elif defect == "not_correction":
        plan.pop("monthly_ready_correction")
    elif defect == "no_source":
        checkpoints.clear()
    else:
        checkpoints["BUILD"] = {}
    with pytest.raises(MonthlyReleaseConflict, match="idle retained BUILD"):
        MonthlyReleaseService.bind_factor_repairs(service, "operation", inputs=inputs, principal="operator", cas=cas)


def test_factor_binding_route_uses_existing_operator_and_service_contract(tmp_path):
    from backend.routers.monthly_dataset_releases import (
        UnifiedMonthlyRepairInputsRequest, bind_monthly_factor_repairs,
    )
    service, inputs, plan, state, checkpoints, cas = service_fixture(tmp_path)
    adapter = SimpleNamespace(bind_factor_repairs=lambda operation_id, **kwargs:
        MonthlyReleaseService.bind_factor_repairs(service, operation_id, cas=cas, **kwargs))
    result = bind_monthly_factor_repairs("operation", UnifiedMonthlyRepairInputsRequest(inputs=inputs),
        SimpleNamespace(principal_id="operator"), adapter)
    assert result["data"]["status"] == "BOUND"


def test_completed_commit_recovery_keeps_later_receipts(tmp_path, monkeypatch):
    resume, prepare, plan, cas, source, target, old = prepared(tmp_path)
    unlink = Path.unlink

    def interrupted(path, *args, **kwargs):
        if path == subject._pending(resume.root):
            raise OSError("after journal commit")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupted)
    with pytest.raises(OSError, match="after journal commit"):
        subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    monkeypatch.setattr(Path, "unlink", unlink)
    resume.record = json.loads(resume.path.read_bytes())
    resume.record["steps"]["later-validation"] = cas.put_json({"status": "PASS"}).as_dict()
    _atomic(resume.path, resume.record)
    recovered = subject.recover_factor_repairs(root=resume.root, journal=resume.path, record=resume.record, cas=cas)
    assert recovered == resume.record and "later-validation" in recovered["steps"]


def test_runner_applies_factor_repairs_after_industry_without_reexport(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_mature_build_runner as runner_module
    from backend.services.dataset_release import monthly_industry_history
    from backend.tests.dataset_release.test_monthly_mature_build_runner import _native_month_fixture
    runner, context, staging, compiled, operation, _, _ = _native_month_fixture(tmp_path)
    order = []

    def stage(invocation):
        result = {"schema_version": "dataset_release_build_stage_result_v1", "stage": invocation.stage, "status": "PASS"}
        if invocation.stage == "prepare":
            result["qlib_dump_operations"] = [operation]
        return result

    def applied(resume, *, prepare, **kwargs):
        order.append("factor")
        return prepare

    def industry(resume, *, prepare, **kwargs):
        order.append("industry")
        return prepare

    monkeypatch.setattr(runner_module, "run_build_stage", stage)
    monkeypatch.setattr(subject, "apply_factor_repairs", applied)
    monkeypatch.setattr(monthly_industry_history, "apply_retained_history", industry)
    runner.execute(context=context, staging_root=staging, compiled=compiled)
    assert order == ["industry", "factor"]


def test_next_ready_correction_retains_completed_factor_proof(tmp_path):
    from backend.services.dataset_release.monthly_ready_correction import seed_completed_exports
    from backend.services.dataset_release.monthly_build_resume import MonthlyBuildResume
    from backend.tests.dataset_release.test_monthly_mature_build_runner import _context, _compiled
    resume, prepare, plan, cas, source, target, old = prepared(tmp_path)
    prepared_result = subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    base = resume.root
    (base / "reports").mkdir()
    compiled = _compiled()
    context = _context()
    evidence = base / "reports/monthly_candidate_build_evidence.json"
    evidence.write_text(json.dumps({"operation_id": context.operation_id,
        "source_bundle_sha256": compiled.source_bundle_sha256, "stage_refs": {
            "prepare": cas.put_json(prepared_result).as_dict(), "finalize_bins": cas.put_json({"status": "PASS"}).as_dict()}}))
    manifest = base / "qe_dataset_manifest.json"
    manifest.write_text(json.dumps({"components": {"evidence": {"path": "reports/monthly_candidate_build_evidence.json",
        "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(), "size": evidence.stat().st_size}}}))
    context = replace(context, attempt=3, plan={**context.plan, "candidate_root": str(tmp_path / "successor"),
        "monthly_ready_correction": {"base_plan": {"candidate_root": str(base)},
            "base_manifest_ref": {"sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()}}})
    staging = tmp_path / ".staging" / f".successor.{context.operation_id}.attempt-3.building"
    assert seed_completed_exports(context=context, staging_root=staging, compiled=compiled, cas=cas)
    seeded = MonthlyBuildResume(context=context, compiled=compiled, staging_root=staging, release_root=tmp_path, cas=cas)
    assert seeded.record["steps"]["fact-correction"] == plan[subject.BINDING]["authority_ref"]
