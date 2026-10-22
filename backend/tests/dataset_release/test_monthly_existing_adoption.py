import hashlib
import json
from pathlib import Path

import pytest

from backend.tests.dataset_release.test_monthly_unified_v2 import (
    Pipeline, _adoptable_successor, _canonical, _service, _sha,
)
from backend.services.dataset_release.monthly_unified import MonthlyReleaseNotReady
from backend.services.dataset_release.monthly_late_publication import SCHEMA, READBACK_SCHEMA, file_pin


def test_v4_is_not_adopted_with_legacy_v3_evidence(tmp_path):
    service = _service(tmp_path, Pipeline())
    values = _adoptable_successor(tmp_path, service)
    path = values["profile_candidate"]
    profile = json.loads(path.read_text(encoding="utf-8"))
    profile["schema_version"] = "aistock_active_dataset_profile_v4"
    _canonical(path, profile)
    values["target_profile_sha256"] = _sha(path)
    with pytest.raises(MonthlyReleaseNotReady, match="typed late-financing proof"):
        service.adopt_existing_successor(**values)


def test_financing_private_component_proof_alone_cannot_make_ready(tmp_path):
    service = _service(tmp_path, Pipeline())
    values = _adoptable_successor(tmp_path, service)
    path = values["candidate_root"] / "reports/late-component.json"
    schema = "aistock_monthly_late_financing_amendment_v1"
    _canonical(path, {"schema_version": schema, "status": "PRIVATE_COMPONENTS_NOT_A_RELEASE"})
    values["evidence_refs"] = [{"relative_path": "reports/late-component.json", "sha256": _sha(path),
                                "schema_version": schema}]
    with pytest.raises(MonthlyReleaseNotReady, match="incomplete or mixed"):
        service.adopt_existing_successor(**values)
    assert not list(service.store.operations.glob("dmr_*/receipts/ready.json"))


def _financing_successor(tmp_path, service):
    values = _adoptable_successor(tmp_path, service)
    active = json.loads(service.active_profile.read_text(encoding="utf-8"))
    active["schema_version"] = "aistock_active_dataset_profile_v4"
    base = Path(active["controller_paths"]["candidate_root"])
    base.mkdir(parents=True, exist_ok=True)
    candidate = values["candidate_root"]
    paths = [prefix + name for prefix in ("factor_bundle/", "components/factor_h5_static_candidate_v2/")
             for name in ("margin_detail.h5", "static_factors.parquet")]
    paths += ["components/factor_h5_static_candidate_v2/meta.json",
              "reports/monthly_consumer_layout_receipt.json", "untouched.bin"]
    before = {}
    after = {}
    for relative in paths:
        for root, content in ((base, b"old"), (candidate, b"old" if relative == "untouched.bin" else b"new")):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        before[relative] = {"path": relative, **file_pin(base / relative)}
        after[relative] = {"path": relative, **file_pin(candidate / relative)}
    old = {"release_id": active["release_id"], "cutoff_trade_date": active["cutoff"],
           "revision": "original", "source_contract": {}, "components": before}
    old["dataset_manifest_sha256"] = hashlib.sha256(
        json.dumps(old, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    _canonical(base / "qe_dataset_manifest.json", old)
    active["components"].update(dataset_manifest_sha256=old["dataset_manifest_sha256"],
                                 dataset_manifest_file_sha256=_sha(base / "qe_dataset_manifest.json"))
    _canonical(service.active_profile, active)
    report = {"schema_version": SCHEMA, "status": "PRIVATE_COMPONENTS_NOT_A_RELEASE",
              "predecessor_manifest_sha256": old["dataset_manifest_sha256"], "trade_date": active["cutoff"],
              "applicable_rows": 2, "files": {name: file_pin(candidate / "factor_bundle" / name)
                 for name in ("margin_detail.h5", "static_factors.parquet")},
              "price_export_performed": False, "database_write": False,
              "active_profile_write": False, "runtime_action": False}
    report_path = candidate / "reports/late-financing-amendment.json"
    _canonical(report_path, report)
    for relative in ("reports/late-financing-amendment.json", "reports/late-financing-source/capture.json",
                     "reports/late-financing-source/source.parquet"):
        path = candidate / relative
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"source")
        after[relative] = {"path": relative, **file_pin(path)}
    manifest = {**old, "revision": "original-margin1", "components": after,
                "source_contract": {"late_fact_amendment": {
                    "predecessor_manifest_sha256": old["dataset_manifest_sha256"],
                    "trade_date": active["cutoff"], "receipt_sha256": _sha(report_path)}}}
    manifest.pop("dataset_manifest_sha256")
    manifest["dataset_manifest_sha256"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    _canonical(candidate / "qe_dataset_manifest.json", manifest)
    profile = json.loads(values["profile_candidate"].read_text(encoding="utf-8"))
    profile = {**active, "generation": profile["generation"], "controller_paths": profile["controller_paths"],
               "node_bindings": profile["node_bindings"], "components": {**active["components"],
                   "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
                   "dataset_manifest_file_sha256": _sha(candidate / "qe_dataset_manifest.json")}}
    _canonical(values["profile_candidate"], profile)
    proof = {"schema_version": READBACK_SCHEMA, "status": "PASS",
             "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
             "price_export_performed": False, "database_write": False,
             "active_profile_write": False, "runtime_action": False, "nodes": {}}
    for node in ("controller", "wsl2-5080", "rdagent-node1"):
        proof["nodes"][node] = {"status": "PASS", "dataset_identity_complete": True,
            "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
            "manifest_file_sha256": _sha(candidate / "qe_dataset_manifest.json"),
            "financing_rows": 2, "source_mismatch_count": 0, "component_hashes": report["files"]}
    readback = candidate / "reports/late-readback.json"
    _canonical(readback, proof)
    return {**values, "predecessor_profile_sha256": _sha(service.active_profile),
            "target_profile_sha256": _sha(values["profile_candidate"]),
            "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
            "dataset_manifest_file_sha256": _sha(candidate / "qe_dataset_manifest.json"),
            "node_manifest_file_sha256": {node: _sha(candidate / "qe_dataset_manifest.json") for node in proof["nodes"]},
            "evidence_refs": [{"relative_path": path.relative_to(candidate).as_posix(), "sha256": _sha(path),
                               "schema_version": schema} for path, schema in ((report_path, SCHEMA), (readback, READBACK_SCHEMA))]}


def test_financing_amendment_adoption_is_ready_idempotent_and_does_not_activate(tmp_path):
    service = _service(tmp_path, Pipeline())
    values = _financing_successor(tmp_path, service)
    before = service.active_profile.read_bytes()
    result = service.adopt_existing_successor(**values)
    assert result["status"] == "READY_TO_ACTIVATE"
    assert service.adopt_existing_successor(**values)["operation_id"] == result["operation_id"]
    assert service.active_profile.read_bytes() == before


@pytest.mark.parametrize("matches", [True, False])
def test_financing_successor_only_follows_exact_activated_predecessor(tmp_path, matches):
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseConflict

    service = _service(tmp_path, Pipeline())
    values = _financing_successor(tmp_path, service)
    operation = "dmr_" + "1" * 32
    root = service.store.operation_root(operation)
    (root / "receipts").mkdir(parents=True)
    _canonical(root / "request.json", {"operation_id": operation, "product_profile": "qe_hmm_full_v2",
               "idempotency_key": "previous-monthly-run", "target_cutoff": values["target_cutoff"].isoformat()})
    _canonical(root / "state.json", {"status": "ACTIVATED_VERIFIED"})
    _canonical(root / "receipts/activation.json", {"new_profile_ref": {
        "sha256": values["predecessor_profile_sha256"] if matches else "f" * 64}})
    if matches:
        assert service.adopt_existing_successor(**values)["status"] == "READY_TO_ACTIVATE"
    else:
        with pytest.raises(MonthlyReleaseConflict, match="another operation"):
            service.adopt_existing_successor(**values)


@pytest.mark.parametrize("case", ["missing_node", "unrelated_content", "wrong_component", "side_effect"])
def test_financing_amendment_rejects_incomplete_or_wrong_readback(tmp_path, case):
    service = _service(tmp_path, Pipeline())
    values = _financing_successor(tmp_path, service)
    ref = values["evidence_refs"][1]
    path = values["candidate_root"] / ref["relative_path"]
    proof = json.loads(path.read_text(encoding="utf-8"))
    if case == "missing_node":
        proof["nodes"].pop("rdagent-node1")
    elif case == "wrong_component":
        proof["nodes"]["controller"]["component_hashes"] = {}
    elif case == "side_effect":
        proof["database_write"] = True
    else:
        (values["candidate_root"] / "factor_bundle/margin_detail.h5").write_bytes(b"different")
    _canonical(path, proof)
    ref["sha256"] = _sha(path)
    with pytest.raises(MonthlyReleaseNotReady, match="late financing"):
        service.adopt_existing_successor(**values)
