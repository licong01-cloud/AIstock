"""Typed minute repair and single-cutoff pending-candidate supersession."""

from copy import deepcopy
import json
import shutil

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_late_publication import file_pin
from backend.services.dataset_release.monthly_minute_adoption import AMENDMENT, WINDOW, READBACK, FIELDS
from backend.services.dataset_release.monthly_unified import MonthlyReleaseConflict, MonthlyReleaseNotReady
from backend.tests.dataset_release.test_monthly_existing_adoption import _financing_successor
from backend.tests.dataset_release.test_monthly_unified_v2 import Pipeline, _service, _canonical, _sha
import hashlib


def fixture(tmp_path):
    service = _service(tmp_path, Pipeline())
    original = _financing_successor(tmp_path, service)
    for path in (service.active_profile, original["profile_candidate"]):
        p = json.loads(path.read_bytes())
        p["components"]["minute_pins"]["calendar_sha256"] = "c" * 64
        _canonical(path, p)
    original.update(
        predecessor_profile_sha256=_sha(service.active_profile),
        target_profile_sha256=_sha(original["profile_candidate"]),
    )
    pending = service.adopt_existing_successor(**original)
    base = original["candidate_root"]
    (base / "provenance").mkdir(exist_ok=True)
    shutil.copyfile(base / "reports/late-readback.json", base / "provenance/late-financing-readback.json")
    candidate = base.with_name(base.name + "-minute1")
    shutil.copytree(base, candidate)
    old = json.loads((base / "qe_dataset_manifest.json").read_bytes())
    manifest = deepcopy(old)
    manifest.pop("dataset_manifest_sha256")
    profile = json.loads(original["profile_candidate"].read_bytes())
    repair = {
        "schema_version": AMENDMENT,
        "status": "PRIVATE_COMPONENT_NOT_A_RELEASE",
        "predecessor_candidate_root": str(base),
        "predecessor_manifest_sha256": old["dataset_manifest_sha256"],
        "predecessor_profile": {"path": str(original["profile_candidate"]), **file_pin(original["profile_candidate"])},
        "calendar_sha256": "c" * 64,
        "fields": sorted(FIELDS),
        "stock_dates": [{"ts_code": "000001.SZ", "trade_date": "2026-07-01"}],
        "existing_finite_values_unchanged": True,
        "files": {},
        "checked_cells": 2880,
        "database_write": False,
        "active_profile_write": False,
        "runtime_action": False,
    }
    mutable = {}
    for field in FIELDS:
        relative = "features/000001.sz/" + field + ".1min.bin"
        for prefix in ("components/minute_bin_candidate/", "minute_bin/qlib/"):
            path = candidate / (prefix + relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"original market facts")
            mutable[prefix + relative] = {"path": prefix + relative, **file_pin(path)}
        repair["files"][relative] = {"before": None, "after": file_pin(path)}
    for prefix in ("components/minute_bin_candidate/", "minute_bin/qlib/"):
        for suffix in ("instruments/all.txt", "meta_export.json"):
            path = candidate / (prefix + suffix)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"metadata")
            mutable[prefix + suffix] = {"path": prefix + suffix, **file_pin(path)}
    window = {
        "schema_version": WINDOW,
        "status": "PASS",
        "physical_unexplained_gap_days_after": 0,
        "physical_gap_days_before": 1,
        "predecessor_manifest_sha256": old["dataset_manifest_sha256"],
        "active_manifest_sha256": json.loads(service.active_profile.read_bytes())["components"][
            "dataset_manifest_sha256"
        ],
        "consumer_qrun_sha256": "a" * 64,
        "consumer_coverage": {"expected_spans": 1},
        "database_write": False,
        "active_profile_write": False,
        "runtime_action": False,
    }
    for relative, value in [
        ("reports/minute-coverage-amendment.json", repair),
        ("reports/minute-full-window-validation.json", window),
        ("reports/minute-coverage-source.json", {"frozen_facts": True}),
        ("minute_bin/materialization_receipt.json", {"count": 1}),
        ("reports/monthly_consumer_layout_receipt.json", {"schema": "existing"}),
    ]:
        _canonical(candidate / relative, value)
        mutable[relative] = {"path": relative, **file_pin(candidate / relative)}
    manifest["components"].update(mutable)
    manifest["revision"] += "-minute1"
    manifest["source_contract"]["minute_coverage_amendment"] = {
        "predecessor_manifest_sha256": old["dataset_manifest_sha256"],
        "receipt_sha256": _sha(candidate / "reports/minute-coverage-amendment.json"),
    }
    manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    _canonical(candidate / "qe_dataset_manifest.json", manifest)
    profile["generation"] += "-minute1"
    profile["controller_paths"]["candidate_root"] = str(candidate)
    for binding in profile["node_bindings"].values():
        binding["candidate_root"] += "-minute1"
    profile["components"].update(
        dataset_manifest_sha256=manifest["dataset_manifest_sha256"],
        dataset_manifest_file_sha256=_sha(candidate / "qe_dataset_manifest.json"),
    )
    profile_path = original["profile_candidate"].with_name("minute-profile.json")
    _canonical(profile_path, profile)
    proof = {
        "schema_version": READBACK,
        "status": "PASS",
        "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
        "physical_predecessor_profile_sha256": _sha(original["profile_candidate"]),
        "logical_predecessor_profile_sha256": _sha(service.active_profile),
        "supersedes_operation_id": pending["operation_id"],
        "database_write": False,
        "active_profile_write": False,
        "runtime_action": False,
        "nodes": {},
    }
    for node in ("controller", "wsl2-5080", "rdagent-node1"):
        proof["nodes"][node] = {
            "status": "PASS",
            "dataset_identity_complete": True,
            "manifest": manifest["dataset_manifest_sha256"],
            "manifest_file_sha256": _sha(candidate / "qe_dataset_manifest.json"),
            "stock_dates_verified": 1,
            "field_cells_verified": 2880,
            "qrun_sha256": "a" * 64,
            "consumer_coverage": window["consumer_coverage"],
        }
    _canonical(candidate / "provenance/minute-nodes.json", proof)
    refs = [
        ("reports/minute-coverage-amendment.json", AMENDMENT),
        ("reports/minute-full-window-validation.json", WINDOW),
        ("provenance/minute-nodes.json", READBACK),
    ]
    values = {
        **original,
        "idempotency_key": "minute-adoption",
        "candidate_root": candidate,
        "profile_candidate": profile_path,
        "target_profile_sha256": _sha(profile_path),
        "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
        "dataset_manifest_file_sha256": _sha(candidate / "qe_dataset_manifest.json"),
        "expected_node_roots": {k: v["candidate_root"] for k, v in profile["node_bindings"].items()},
        "node_manifest_file_sha256": {k: _sha(candidate / "qe_dataset_manifest.json") for k in proof["nodes"]},
        "evidence_refs": [
            {"relative_path": path, "schema_version": schema, "sha256": _sha(candidate / path)} for path, schema in refs
        ],
    }
    return service, values, pending


def test_successor_retains_history_is_idempotent_and_does_not_activate(tmp_path):
    service, values, pending = fixture(tmp_path)
    oldroot = service.store.operation_root(pending["operation_id"])
    oldbytes = {p: (oldroot / p).read_bytes() for p in ["request.json", "plan.json", "receipts/ready.json"]}
    active = service.active_profile.read_bytes()
    result = service.adopt_existing_successor(**values)
    assert result["status"] == "READY_TO_ACTIVATE"
    assert service.adopt_existing_successor(**values)["operation_id"] == result["operation_id"]
    assert service.store.read_state(pending["operation_id"])["status"] == "CANCELLED"
    assert {p: (oldroot / p).read_bytes() for p in oldbytes} == oldbytes
    assert service.active_profile.read_bytes() == active
    with pytest.raises(MonthlyReleaseConflict, match="superseded"):
        service.resume(pending["operation_id"])


@pytest.mark.parametrize("failure", ["node", "cells", "identity", "active", "unrelated", "intent", "duplicate_proof"])
def test_invalid_successor_keeps_original_ready(tmp_path, failure):
    service, values, pending = fixture(tmp_path)
    candidate = values["candidate_root"]
    path = candidate / "provenance/minute-nodes.json"
    proof = json.loads(path.read_bytes())
    if failure == "node":
        proof["nodes"].pop("wsl2-5080")
    elif failure == "cells":
        proof["nodes"]["controller"]["field_cells_verified"] -= 1
    elif failure == "identity":
        proof["physical_predecessor_profile_sha256"] = "0" * 64
    elif failure == "active":
        proof["logical_predecessor_profile_sha256"] = "0" * 64
    elif failure == "unrelated":
        (candidate / "components/minute_bin_candidate/features/000001.sz/close.1min.bin").write_bytes(b"drift")
    elif failure == "intent":
        _canonical(service.store.operation_root(pending["operation_id"]) / "receipts/activation.json", {})
    else:
        values["evidence_refs"].append(dict(values["evidence_refs"][2]))
    _canonical(path, proof)
    values["evidence_refs"][2]["sha256"] = _sha(path)
    with pytest.raises((MonthlyReleaseConflict, MonthlyReleaseNotReady)):
        service.adopt_existing_successor(**values)
    assert service.store.read_state(pending["operation_id"])["status"] == "READY_TO_ACTIVATE"


@pytest.mark.parametrize("boundary", ["before_marker", "after_marker"])
def test_interrupted_retirement_blocks_both_activations_and_resumes_same_operation(tmp_path, monkeypatch, boundary):
    from backend.services.dataset_release import monthly_minute_adoption as module

    service, values, pending = fixture(tmp_path)
    old_id = pending["operation_id"]
    active = service.active_profile.read_bytes()
    original_retire = module.retire_pending_predecessor
    original_update = service.store.update_state

    def interrupt_retire(*args, **kwargs):
        raise RuntimeError("injected crash before marker")

    def interrupt_update(operation_id, **changes):
        if operation_id == old_id and changes.get("status") == "CANCELLED":
            raise RuntimeError("injected crash after marker")
        return original_update(operation_id, **changes)

    if boundary == "before_marker":
        monkeypatch.setattr(module, "retire_pending_predecessor", interrupt_retire)
    else:
        monkeypatch.setattr(service.store, "update_state", interrupt_update)
    with pytest.raises(RuntimeError, match="injected crash"):
        service.adopt_existing_successor(**values)
    requests = [json.loads(p.read_bytes()) for p in service.store.operations.glob("*/request.json")]
    new_id = next(r["operation_id"] for r in requests if r.get("idempotency_key") == "minute-adoption")
    for operation_id in (old_id, new_id):
        with pytest.raises(MonthlyReleaseNotReady):
            service.activate(
                operation_id, authorization_store=None, authorization_ref="unused", principal="test", verify_after=None
            )
    assert service.active_profile.read_bytes() == active
    monkeypatch.setattr(module, "retire_pending_predecessor", original_retire)
    monkeypatch.setattr(service.store, "update_state", original_update)
    assert service.adopt_existing_successor(**values)["operation_id"] == new_id
    module.guard_minute_activation(service, new_id)
    assert service.store.read_state(old_id)["status"] == "CANCELLED"
    assert service.active_profile.read_bytes() == active
