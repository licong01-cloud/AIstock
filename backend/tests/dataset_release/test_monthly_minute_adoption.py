"""Typed minute repair and single-cutoff pending-candidate supersession."""

from copy import deepcopy
import json
from pathlib import Path
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


def test_combined_fact_paths_are_bounded_and_pin_verified(tmp_path):
    from backend.services.dataset_release.monthly_minute_adoption import validate_fact_amendment

    path = 'components/factor_h5_static_candidate_v2/sector_data.h5'
    value = tmp_path / path
    value.parent.mkdir(parents=True)
    value.write_bytes(b'authoritative missing rows')
    old = {path: {'path': path, 'sha256': 'a' * 64, 'size': 3}}
    new = {path: {'path': path, **file_pin(value)}}
    extra = {'files': {path: {'before': old[path], 'after': new[path]}},
             'evidence': {}, 'stock_dates': 8702, 'existing_finite_values_unchanged': True,
             'database_write': False, 'active_profile_write': False, 'runtime_action': False}
    assert validate_fact_amendment(extra, old, new, tmp_path) == {path}
    extra['files']['stock_pools/stock_universe.txt'] = extra['files'][path]
    with pytest.raises(ValueError, match='fact amendment'):
        validate_fact_amendment(extra, old, new, tmp_path)


def _assembly_fixture(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import dataset_release_minute_coverage_correction as module

    base, work, target = [tmp_path / n for n in ('base', 'component', 'successor')]
    components = {}
    for prefix in ('components/minute_bin_candidate/', 'minute_bin/qlib/'):
        for relative, raw in [('instruments/all.txt', b'old'), ('features/000001.sz/close.1min.bin', b'old quote')]:
            path = base / (prefix + relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            components[prefix + relative] = {'path': prefix + relative, **file_pin(path)}
        _canonical(base / prefix / 'meta_export.json', {})
        name = prefix + 'meta_export.json'
        components[name] = {'path': name, **file_pin(base / name)}
    _canonical(base / 'minute_bin/materialization_receipt.json', {})
    name = 'minute_bin/materialization_receipt.json'
    components[name] = {'path': name, **file_pin(base / name)}
    _canonical(base / 'unchanged.json', {'fact': 1})
    components['unchanged'] = {'path': 'unchanged.json', **file_pin(base / 'unchanged.json')}
    manifest = {'components': components, 'source_contract': {}, 'revision': 'r1', 'release_id': 'release'}
    manifest['dataset_manifest_sha256'] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    _canonical(base / 'qe_dataset_manifest.json', manifest)
    profile_path = tmp_path / 'profile.json'
    _canonical(profile_path, {'generation': 'v1', 'controller_paths': {'candidate_root': str(base)},
        'node_bindings': {}, 'components': {'dataset_manifest_file_sha256': file_pin(base / 'qe_dataset_manifest.json')['sha256']}})
    relative = 'features/000001.sz/close.1min.bin'
    path = work / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(b'correct quotes')
    _canonical(work / 'source.json', {'frozen_source': True})
    _canonical(work / 'component-amendment.json', {
        'predecessor_profile': {'sha256': file_pin(profile_path)['sha256']},
        'files': {relative: {'before': file_pin(base / 'components/minute_bin_candidate' / relative), 'after': file_pin(path)}},
        'source_ref': {'path': str(work / 'source.json'), **file_pin(work / 'source.json')}, 'fields': ['close'], 'filled_cells': 2})
    monkeypatch.setattr(module, 'build_minute_quote_catalog', lambda _: SimpleNamespace(payload=b'catalog', calendar_sha256='a' * 64))
    return SimpleNamespace(profile=profile_path, component=work, output=target), base, module


def test_resume_retains_completed_files_and_never_overwrites_predecessor(tmp_path, monkeypatch):
    args, base, module = _assembly_fixture(tmp_path, monkeypatch)
    before = (base / 'components/minute_bin_candidate/features/000001.sz/close.1min.bin').read_bytes()
    real_copy, calls = module.shutil.copyfile, []

    def fail_once(source, destination):
        calls.append(source)
        if len(calls) == 2:
            raise OSError('injected interruption')
        return real_copy(source, destination)

    monkeypatch.setattr(module.shutil, 'copyfile', fail_once)
    with pytest.raises(OSError, match='interruption'):
        module.assemble(args)
    repaired = args.output / 'components/minute_bin_candidate/features/000001.sz/close.1min.bin'
    inode = repaired.stat().st_ino
    result = module.assemble(args)
    assert repaired.stat().st_ino == inode
    assert (base / 'components/minute_bin_candidate/features/000001.sz/close.1min.bin').read_bytes() == before
    assert (args.output / 'unchanged.json').stat().st_ino == (base / 'unchanged.json').stat().st_ino
    assert module.assemble(args) == result


def test_existing_unrelated_candidate_is_not_adopted(tmp_path, monkeypatch):
    args, _, module = _assembly_fixture(tmp_path, monkeypatch)
    args.output.mkdir()
    _canonical(args.output / '.minute-assembly-inputs.json', {'another': 'task'})
    with pytest.raises(ValueError, match='another assembly'):
        module.assemble(args)
    _canonical(args.output / 'release_closure_receipt.json', {'sealed': True})
    with pytest.raises(ValueError, match='immutable'):
        module.assemble(args)
    from scripts.dataset_release_combined_correction import prepare
    with pytest.raises(ValueError, match='immutable'):
        prepare(args)


def test_real_quotes_reduce_suspension_explanations_without_changing_population(tmp_path):
    service, values, _ = fixture(tmp_path)
    candidate = values['candidate_root']
    window_path = candidate / 'reports/minute-full-window-validation.json'
    window = json.loads(window_path.read_bytes())
    window['consumer_coverage']['suspension_explained_days'] = 3
    _canonical(window_path, window)
    manifest = json.loads((candidate / 'qe_dataset_manifest.json').read_bytes())
    manifest['components']['reports/minute-full-window-validation.json'].update(file_pin(window_path))
    manifest.pop('dataset_manifest_sha256')
    manifest['dataset_manifest_sha256'] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    _canonical(candidate / 'qe_dataset_manifest.json', manifest)
    path = candidate / 'provenance/minute-nodes.json'
    proof = json.loads(path.read_bytes())
    proof['dataset_manifest_sha256'] = manifest['dataset_manifest_sha256']
    for node in proof['nodes'].values():
        node.update(manifest=manifest['dataset_manifest_sha256'], manifest_file_sha256=_sha(candidate / 'qe_dataset_manifest.json'))
        node['consumer_coverage']['suspension_explained_days'] = 1
    _canonical(path, proof)
    profile = json.loads(values['profile_candidate'].read_bytes())
    profile['components'].update(dataset_manifest_sha256=manifest['dataset_manifest_sha256'],
                                 dataset_manifest_file_sha256=_sha(candidate / 'qe_dataset_manifest.json'))
    _canonical(values['profile_candidate'], profile)
    values.update(target_profile_sha256=_sha(values['profile_candidate']), dataset_manifest_sha256=manifest['dataset_manifest_sha256'],
                  dataset_manifest_file_sha256=_sha(candidate / 'qe_dataset_manifest.json'),
                  node_manifest_file_sha256={k:_sha(candidate / 'qe_dataset_manifest.json') for k in proof['nodes']})
    for reference in values['evidence_refs']:
        reference['sha256'] = _sha(candidate / reference['relative_path'])
    assert service.adopt_existing_successor(**values)['status'] == 'READY_TO_ACTIVATE'


def test_direct_active_predecessor_does_not_retire_an_unrelated_operation(tmp_path):
    from backend.services.dataset_release.monthly_minute_adoption import validate_minute_adoption

    _, values, _ = fixture(tmp_path)
    candidate = values['candidate_root']
    evidence = {r['schema_version']: json.loads((candidate / r['relative_path']).read_bytes())
                for r in values['evidence_refs']}
    repair, proof, window = [evidence[k] for k in (AMENDMENT, READBACK, WINDOW)]
    physical = json.loads(Path(repair['predecessor_profile']['path']).read_bytes())
    proof['logical_predecessor_profile_sha256'] = repair['predecessor_profile']['sha256']
    proof.pop('supersedes_operation_id')
    window['active_manifest_sha256'] = physical['components']['dataset_manifest_sha256']
    _canonical(candidate / 'reports/minute-full-window-validation.json', window)
    manifest = json.loads((candidate / 'qe_dataset_manifest.json').read_bytes())
    pin = manifest['components']['reports/minute-full-window-validation.json']
    pin.update(file_pin(candidate / pin['path']))
    manifest.pop('dataset_manifest_sha256')
    manifest['dataset_manifest_sha256'] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    _canonical(candidate / 'qe_dataset_manifest.json', manifest)
    proof['dataset_manifest_sha256'] = manifest['dataset_manifest_sha256']
    for node in proof['nodes'].values():
        node.update(manifest=manifest['dataset_manifest_sha256'], manifest_file_sha256=_sha(candidate / 'qe_dataset_manifest.json'))
    args = dict(predecessor_root=Path(physical['controller_paths']['candidate_root']), predecessor_profile=physical,
                predecessor_profile_sha256=repair['predecessor_profile']['sha256'], candidate=candidate,
                profile=json.loads(values['profile_candidate'].read_bytes()), manifest=manifest,
                evidence=evidence, required_nodes=('controller', 'wsl2-5080', 'rdagent-node1'))
    assert validate_minute_adoption(**args) is None
    proof['supersedes_operation_id'] = 'dmr_' + 'f' * 32
    with pytest.raises(ValueError, match='unrelated operation'):
        validate_minute_adoption(**args)


@pytest.mark.parametrize('failure', ['hash', 'predecessor', 'finite', 'database'])
def test_combined_fact_drift_is_rejected(tmp_path, failure):
    from backend.services.dataset_release.monthly_minute_adoption import validate_fact_amendment

    path = 'factor_bundle/static_factors.parquet'
    value = tmp_path / path
    value.parent.mkdir(parents=True)
    value.write_bytes(b'fact')
    old = {path: {'path': path, 'sha256': 'a' * 64, 'size': 2}}
    new = {path: {'path': path, **file_pin(value)}}
    extra = {'files': {path: {'before': dict(old[path]), 'after': dict(new[path])}},
             'evidence': {}, 'stock_dates': 1, 'existing_finite_values_unchanged': True,
             'database_write': False, 'active_profile_write': False, 'runtime_action': False}
    if failure == 'hash':
        value.write_bytes(b'drift')
    elif failure == 'predecessor':
        extra['files'][path]['before']['sha256'] = 'b' * 64
    elif failure == 'finite':
        extra['existing_finite_values_unchanged'] = False
    else:
        extra['database_write'] = True
    with pytest.raises(ValueError, match='fact amendment'):
        validate_fact_amendment(extra, old, new, tmp_path)


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


def _activation_history(service, operation_id, old_sha, new_sha, **request_changes):
    root = service.store.operation_root(operation_id)
    _canonical(root / "request.json", {
        "operation_id": operation_id, "product_profile": "qe_hmm_full_v2",
        "target_cutoff": "2026-08-31", "idempotency_key": operation_id,
        **request_changes,
    })
    _canonical(root / "state.json", {"status": "ACTIVATED_VERIFIED"})
    _canonical(root / "receipts/activation.json", {
        "old_profile_ref": {"sha256": old_sha}, "new_profile_ref": {"sha256": new_sha},
    })
    return root


@pytest.mark.parametrize("kind", ["minute", "financing"])
def test_verified_activation_ancestry_allows_next_correction_without_rewriting_history(tmp_path, kind):
    if kind == "minute":
        service, values, _ = fixture(tmp_path)
    else:
        service = _service(tmp_path, Pipeline())
        values = _financing_successor(tmp_path, service)
    base = _activation_history(service, "dmr_" + "1" * 32, "a" * 64, "b" * 64)
    direct = _activation_history(service, "dmr_" + "2" * 32, "b" * 64,
                                 values["predecessor_profile_sha256"])
    cancelled = _activation_history(service, "dmr_" + "3" * 32, "a" * 64, "b" * 64)
    # Model the real pending-candidate retirement, not a previously activated
    # operation with only an unauthenticated successor ID.
    (cancelled / "receipts/activation.json").unlink()
    retired = json.loads((cancelled / "request.json").read_bytes())
    retired["target_profile_sha256"] = "c" * 64
    _canonical(cancelled / "request.json", retired)
    successor = json.loads((base / "request.json").read_bytes())
    successor["supersedes_pending_operation"] = {
        "operation_id": retired["operation_id"], "physical_profile_sha256": "c" * 64}
    _canonical(base / "request.json", successor)
    _canonical(cancelled / "state.json", {"status": "CANCELLED"})
    _canonical(cancelled / "receipts/superseded.json", {
        "schema_version": "aistock_monthly_pending_successor_v1",
        "operation_id": retired["operation_id"], "successor_operation_id": "dmr_" + "1" * 32,
        "physical_predecessor_profile_sha256": "c" * 64,
        "logical_predecessor_profile_sha256": "a" * 64, "target_profile_sha256": "b" * 64})
    saved = {p: p.read_bytes() for root in (base, direct, cancelled) for p in root.rglob("*.json")}
    active = service.active_profile.read_bytes()
    result = service.adopt_existing_successor(**values)
    assert result["status"] == "READY_TO_ACTIVATE"
    assert service.adopt_existing_successor(**values)["operation_id"] == result["operation_id"]
    assert {p: p.read_bytes() for p in saved} == saved
    assert service.active_profile.read_bytes() == active


@pytest.mark.parametrize("break_at", ["unrelated", "missing_receipt", "not_verified", "cutoff", "product", "cycle", "invalid_hash"])
def test_unproven_activation_ancestry_remains_a_conflict(tmp_path, break_at):
    service, values, pending = fixture(tmp_path)
    _activation_history(service, "dmr_" + "1" * 32, "a" * 64, "b" * 64)
    changes = {"target_cutoff": "2026-09-30"} if break_at == "cutoff" else (
        {"product_profile": "another-product"} if break_at == "product" else {})
    direct = _activation_history(service, "dmr_" + "2" * 32, "b" * 64,
                                 values["predecessor_profile_sha256"], **changes)
    if break_at == "missing_receipt":
        (direct / "receipts/activation.json").unlink()
    elif break_at == "not_verified":
        _canonical(direct / "state.json", {"status": "ACTIVATING"})
    elif break_at in {"unrelated", "cycle", "invalid_hash"}:
        _canonical(direct / "receipts/activation.json", {
            "old_profile_ref": {"sha256": "b" * 64 if break_at != "invalid_hash" else "invalid"},
            "new_profile_ref": {"sha256": (values["predecessor_profile_sha256"] if break_at == "invalid_hash"
                                           else "a" * 64 if break_at == "cycle" else "f" * 64)},
        })
    active = service.active_profile.read_bytes()
    with pytest.raises(MonthlyReleaseConflict, match="another operation"):
        service.adopt_existing_successor(**values)
    assert service.active_profile.read_bytes() == active
    assert service.store.read_state(pending["operation_id"])["status"] == "READY_TO_ACTIVATE"


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
