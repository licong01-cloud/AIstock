import hashlib
import os
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_legacy_adoption import adopt_completed_exports
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.monthly_build_resume import (
    MonthlyBuildResume, MonthlyBuildResumeError, materialization_attempt, retained_staging_root,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


def _fixture(tmp_path):
    context = ProducerContext(stage="BUILD", operation_id="dmr_" + "1" * 32, attempt=2,
        request={}, prior_receipts={}, plan={"candidate_root": str(tmp_path / "release"),
        "release_id": "september", "target_cutoff": "2026-09-30", "predecessor": {"manifest": "a" * 64}})
    compiled = SimpleNamespace(source_bundle_sha256="b" * 64, physical_plan={"action_plan_digest": "c" * 64})
    root = tmp_path / ".staging" / f".release.{context.operation_id}.attempt-2.building"
    (root / "daily_bin").mkdir(parents=True)
    (root / "daily_bin/close.bin").write_bytes(b"accurate")
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    journal = MonthlyBuildResume(context=context, compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    receipt = cas.put_json({"status": "PASS", "real_file": "daily_bin/close.bin"})
    journal.save("prepare", receipt, roots=("daily_bin",))
    return context, compiled, root, cas, journal


def test_fresh_resume_keeps_exact_bytes_and_reuses_original_staging(tmp_path):
    context, compiled, root, cas, journal = _fixture(tmp_path)
    before = (root / "daily_bin/close.bin").stat()
    retry = replace(context, attempt=3)
    selected = retained_staging_root(retry, compiled, tmp_path, tmp_path / "new-staging")
    assert selected == root
    reopened = MonthlyBuildResume(context=retry, compiled=compiled, release_root=tmp_path, staging_root=selected, cas=cas)
    assert reopened.get("prepare")["status"] == "PASS"
    assert (root / "daily_bin/close.bin").stat().st_mtime_ns == before.st_mtime_ns
    reopened.write_lease("d" * 64)
    assert materialization_attempt(root, operation_id=context.operation_id, execution_attempt=3,
        candidate_name="release", release_digest="d" * 64) == 2


def test_interrupted_history_commit_keeps_price_exports_and_finishes_without_writers(tmp_path, monkeypatch):
    import copy
    from backend.services.dataset_release.monthly_industry_history import recover_history_commit
    from backend.services.dataset_release.monthly_build_resume import _atomic, _signature
    context, compiled, root, cas, journal = _fixture(tmp_path)
    factor = root / "factor_bundle"
    factor.mkdir()
    names = ("sector_data.h5", "static_factors.parquet", "factor_checkpoint.json")
    for name in names:
        (factor / name).write_bytes(b"original")
    journal.save("prepare", cas.put_json({"status": "PASS"}), roots=("factor_bundle",))
    scratch = root.parent / f".{root.name}.industry-history-{'e' * 64}"
    scratch.mkdir()
    proposed, files = copy.deepcopy(journal.record), {}
    proposed["steps"]["industry-history"] = cas.put_json({"projection_sha256": "e" * 64}).as_dict()
    for name in names:
        path = scratch / name
        path.write_bytes(b"repaired")
        files[name] = {"scratch_relative_path": path.relative_to(root.parent).as_posix(),
                       "target_signature": _signature(path), "original_signature": _signature(factor / name)}
        proposed["files"][f"factor_bundle/{name}"] = _signature(path)
    pending = root.parent / f".{root.name}.industry-history-commit.json"
    _atomic(pending, {"binding_digest": journal.record["binding_digest"], "projection_sha256": "e" * 64,
                      "record": proposed, "files": files})
    original_rename = Path.rename
    def interrupted(path, target):
        if path.parent == scratch and path.name == "static_factors.parquet":
            raise OSError("interrupted after preserving the original")
        return original_rename(path, target)
    monkeypatch.setattr(Path, "rename", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        recover_history_commit(root=root, journal=journal.path, record=journal.record, expected_projection="e" * 64)
    monkeypatch.setattr(Path, "rename", original_rename)
    recovered = recover_history_commit(root=root, journal=journal.path, record=journal.record, expected_projection="e" * 64)
    assert recovered == proposed
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"
    for name in names:
        assert (factor / name).read_bytes() == b"repaired"
        assert (scratch / (name + ".before")).read_bytes() == b"original"
    assert recover_history_commit(root=root, journal=journal.path, record=recovered, expected_projection="e" * 64) == proposed


def test_fresh_process_recovers_completed_export_without_any_writer(tmp_path):
    repo = str(Path(__file__).resolve().parents[3])
    script = """
import sys
sys.path.insert(0, sys.argv[1])
from pathlib import Path
from dataclasses import replace
from backend.tests.dataset_release.test_monthly_build_resume import _fixture
from backend.services.dataset_release.monthly_build_resume import MonthlyBuildResume, retained_staging_root
root = Path(sys.argv[2])
if sys.argv[3] == 'write':
    _fixture(root)
else:
    from types import SimpleNamespace
    from backend.services.dataset_release.monthly_worker import ProducerContext
    from backend.services.dataset_release.cas_store import CASStore
    context = ProducerContext(stage='BUILD', operation_id='dmr_' + '1' * 32, attempt=3,
        request={}, prior_receipts={}, plan={'candidate_root': str(root / 'release'),
        'release_id': 'september', 'target_cutoff': '2026-09-30', 'predecessor': {'manifest': 'a' * 64}})
    compiled = SimpleNamespace(source_bundle_sha256='b' * 64, physical_plan={'action_plan_digest': 'c' * 64})
    selected = retained_staging_root(context, compiled, root, root / 'never-created')
    journal = MonthlyBuildResume(context=context, compiled=compiled, release_root=root,
        staging_root=selected, cas=CASStore(root / 'control'))
    assert journal.get('prepare')['status'] == 'PASS'
    assert (selected / 'daily_bin/close.bin').read_bytes() == b'accurate'
    assert not (root / 'never-created').exists()
    print('FRESH_PROCESS_RESUME_PASS')
"""
    first = subprocess.run([sys.executable, "-I", "-c", script, repo, str(tmp_path), "write"],
        capture_output=True, text=True, timeout=30)
    assert first.returncode == 0, first.stderr
    exported = next((tmp_path / ".staging").glob("*.building")) / "daily_bin/close.bin"
    before = exported.stat()
    second = subprocess.run([sys.executable, "-I", "-c", script, repo, str(tmp_path), "read"],
        capture_output=True, text=True, timeout=30)
    assert second.returncode == 0, second.stderr
    assert "FRESH_PROCESS_RESUME_PASS" in second.stdout
    assert exported.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize("fault", ["source", "actions", "predecessor", "target", "operation"])
def test_changed_data_binding_does_not_adopt_or_remove_old_exports(tmp_path, fault):
    context, compiled, root, _, _ = _fixture(tmp_path)
    retry = replace(context, attempt=3)
    if fault == "source":
        compiled = SimpleNamespace(source_bundle_sha256="e" * 64, physical_plan=compiled.physical_plan)
    elif fault == "actions":
        compiled = SimpleNamespace(source_bundle_sha256=compiled.source_bundle_sha256, physical_plan={"action_plan_digest": "e" * 64})
    elif fault == "predecessor":
        retry = replace(retry, plan={**retry.plan, "predecessor": {"manifest": "e" * 64}})
    elif fault == "target":
        retry = replace(retry, plan={**retry.plan, "candidate_root": str(tmp_path / "other")})
    else:
        retry = replace(retry, operation_id="dmr_" + "2" * 32)
    requested = tmp_path / "new-staging"
    assert retained_staging_root(retry, compiled, tmp_path, requested) == requested
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"


@pytest.mark.parametrize("fault", ["changed", "missing", "escape"])
def test_unverified_data_is_not_reused_and_never_silently_reexported(tmp_path, fault):
    context, compiled, root, cas, journal = _fixture(tmp_path)
    data = root / "daily_bin/close.bin"
    if fault == "changed":
        data.write_bytes(b"wrong!!!")
    elif fault == "missing":
        data.rename(root / "preserved.bin")
    else:
        record = json.loads(journal.path.read_text(encoding="utf-8"))
        record["files"]["../outside.bin"] = [0, 0, 8, 0]
        journal.path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises((MonthlyBuildResumeError, FileNotFoundError)):
        MonthlyBuildResume(context=replace(context, attempt=3), compiled=compiled,
            release_root=tmp_path, staging_root=root, cas=cas)


def test_incomplete_writer_is_preserved_outside_publishable_candidate(tmp_path):
    context, compiled, root, cas, _ = _fixture(tmp_path)
    (root / "minute_bin").mkdir()
    (root / "minute_bin/partial.bin").write_bytes(b"keep-all-bytes")
    retry = replace(context, attempt=3)
    journal = MonthlyBuildResume(context=retry, compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    journal.preserve_incomplete_writer("minute_bin")
    preserved = root.parent / f".unfinished.{context.operation_id}.minute_bin.before-3/partial.bin"
    assert preserved.read_bytes() == b"keep-all-bytes"
    assert not (root / "minute_bin").exists()
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"


def test_old_worker_exports_without_journal_are_never_silently_reexported(tmp_path):
    context, compiled, root, _, journal = _fixture(tmp_path)
    journal.path.unlink()
    (root / "daily_bin/materialization_receipt.json").write_text("{}", encoding="utf-8")
    with pytest.raises(MonthlyBuildResumeError, match="re-export forbidden"):
        retained_staging_root(replace(context, attempt=3), compiled, tmp_path, tmp_path / "new")
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"
    assert not (tmp_path / "new").exists()


def test_partial_finalizer_retry_preserves_packaging_and_export_bytes(tmp_path):
    context, compiled, root, cas, journal = _fixture(tmp_path)
    journal.begin_finalization()
    (root / "reports").mkdir()
    (root / "reports/completed.json").write_bytes(b"accurate-packaging")
    daily = root / "daily_bin/close.bin"
    before = daily.stat()
    retry = MonthlyBuildResume(context=replace(context, attempt=3), compiled=compiled,
        release_root=tmp_path, staging_root=root, cas=cas)
    retry.begin_finalization()
    preserved = root.parent / f".unfinished.{context.operation_id}.packaging.before-3/reports/completed.json"
    assert preserved.read_bytes() == b"accurate-packaging"
    assert not (root / "reports").exists()
    assert daily.read_bytes() == b"accurate"
    assert daily.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize("field,value", [("execution_attempt", 2), ("operation_id", "dmr_" + "2" * 32),
    ("release_digest", "e" * 64), ("materialization_attempt", 4)])
def test_materialization_lease_never_weakens_current_execution_fence(tmp_path, field, value):
    context, compiled, root, cas, _ = _fixture(tmp_path)
    journal = MonthlyBuildResume(context=replace(context, attempt=3), compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    journal.write_lease("d" * 64)
    path = root / ".monthly-materialization-lease.json"
    lease = json.loads(path.read_text(encoding="utf-8"))
    lease[field] = value
    path.write_text(json.dumps(lease), encoding="utf-8")
    with pytest.raises(MonthlyBuildResumeError, match="identity"):
        materialization_attempt(root, operation_id=context.operation_id, execution_attempt=3,
            candidate_name="release", release_digest="d" * 64)


def _legacy(tmp_path):
    context = ProducerContext(stage="BUILD", operation_id="dmr_" + "a" * 32, attempt=3,
        request={}, prior_receipts={}, plan={"candidate_root": str(tmp_path / "release"),
        "release_id": "september", "target_cutoff": "2026-09-30",
        "predecessor": {"dataset_manifest_sha256": "a" * 64}})
    compiled = SimpleNamespace(source_bundle_sha256="b" * 64,
        physical_plan={"action_plan_digest": "c" * 64, "release_digest": "d" * 64})
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    relative = f".staging/.release.{context.operation_id}.attempt-2.building"
    root = tmp_path / relative

    def file(relative, data=b"actual-original-bytes"):
        p = root / relative
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        s = p.stat()
        return {"relative_path": relative, "sha256": hashlib.sha256(data).hexdigest(),
            "size": s.st_size, "target_signature": [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns]}

    identity = {"cutoff": "2026-09-30", "source_bundle_sha256": "b" * 64,
        "predecessor_manifest_sha256": "a" * 64, "status": "PASS"}
    operations, refs = [], {}
    for dataset, frequency in (("daily_bin", "day"), ("minute_bin", "1min")):
        cal = file(f"{dataset}/qlib/calendars/{frequency}.txt")
        instruments = file(f"{dataset}/qlib/instruments/all.txt")
        feature = file(f"{dataset}/qlib/features/000001.sz/close.{frequency}.bin")
        native = {"target_root": str(root / dataset / "qlib"), "frequency": frequency,
            "baseline_root": str(tmp_path / "baseline/components" / dataset),
            "calendar_signature": cal["target_signature"], "instruments_signature": instruments["target_signature"],
            "calendar_sha256": cal["sha256"], "instruments_sha256": instruments["sha256"],
            "feature_file_count": 1, "feature_receipts": [{"instrument": "000001.sz", "feature": "close",
                "target_size": feature["size"], "target_sha256": feature["sha256"],
                "target_signature": feature["target_signature"]}]}
        receipt = {**identity, "schema_version": "aistock_monthly_native_qlib_materialization_v1",
            "dataset": dataset, "native_writer": native}
        raw = json.dumps(receipt, sort_keys=True).encode()
        file(f"{dataset}/materialization_receipt.json", raw)
        refs[dataset] = cas.put_bytes(raw).as_dict()
        prepared = cas.put_json({**identity, "dataset": dataset,
            "schema_version": "aistock_monthly_legacy_qlib_operation_v1"})
        operations.append({"operation_id": dataset.removesuffix("_bin"), "dataset": dataset,
            "mode": "inherited_month", "preparation_ref": prepared.as_dict()})
    files = []
    for dataset in ("daily_pv", "daily_basic", "moneyflow", "bak_basic", "cyq_perf", "sector_data", "margin_detail", "static_factors"):
        name = dataset + (".parquet" if dataset == "static_factors" else ".h5")
        info = file("factor_bundle/" + name)
        info.pop("relative_path")
        files.append({"dataset": dataset, **info})
    factor_ref = cas.put_json({**identity, "schema_version": "aistock_monthly_native_factor_materialization_v1",
        "files": files, "metadata_files": []})
    index_file = file("index_context/index_daily.h5")
    index_file["relative_path"] = "index_daily.h5"
    index = {**identity, "schema_version": "aistock_monthly_native_index_materialization_v1", "files": [index_file]}
    index_raw = json.dumps(index).encode()
    file("index_context/index_materialization_receipt.json", index_raw)
    index_ref = cas.put_bytes(index_raw)
    prepare_inner = cas.put_json({"schema_version": "dataset_release_build_prepare_v1", "cutoff": "2026-09-30",
        "refs": {"factor_receipt": factor_ref.as_dict(), "index_materialization_receipt_file": index_ref.as_dict()}})
    finalize_inner = cas.put_json({"schema_version": "dataset_release_build_finalize_v1", "cutoff": "2026-09-30",
        "daily_materialization_receipt_file_ref": refs["daily_bin"],
        "minute_materialization_receipt_file_ref": refs["minute_bin"]})
    stage = {"schema_version": "dataset_release_build_stage_result_v1", "status": "PASS",
        "run_id": context.operation_id, "attempt_id": context.operation_id + "-attempt-2", "attempt_fence": 2,
        "staging_relative_path": relative, "release_id": "september", "release_digest": "d" * 64}
    prepare = cas.put_json({**stage, "stage": "prepare", "prepare_receipt_ref": prepare_inner.as_dict(), "qlib_dump_operations": operations})
    finalize = cas.put_json({**stage, "stage": "finalize-bins", "finalize_receipt_ref": finalize_inner.as_dict()})
    return context, compiled, root, cas, prepare, finalize


def test_dry_run_then_adopt_original_receipts_without_writing_exports(tmp_path):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    data = root / "minute_bin/qlib/features/000001.sz/close.1min.bin"
    before = data.stat()
    kwargs = dict(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
        prepare_ref=prepare, finalize_ref=finalize)
    dry = adopt_completed_exports(**kwargs, apply=False)
    assert dry["journal_written"] is False
    result = adopt_completed_exports(**kwargs, apply=True)
    assert result["materialization_attempt"] == 2
    assert result["exports_written"] is False
    assert data.stat() == before
    assert retained_staging_root(context, compiled, tmp_path, tmp_path / "never") == root
    journal = MonthlyBuildResume(context=context, compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    assert journal.get("prepare")["attempt_fence"] == 2
    assert journal.get("finalize-bins")["attempt_fence"] == 2
    assert journal.get("qlib_dump_minute")["status"] == "PASS"
    with pytest.raises(FileExistsError):
        adopt_completed_exports(**kwargs, apply=True)


@pytest.mark.parametrize("fault", ["source", "predecessor", "attempt", "file", "receipt", "missing", "extra"])
def test_wrong_receipt_or_changed_export_cannot_be_adopted(tmp_path, fault):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    if fault == "source":
        compiled.source_bundle_sha256 = "e" * 64
    elif fault == "predecessor":
        context = replace(context, plan={**context.plan, "predecessor": {"dataset_manifest_sha256": "e" * 64}})
    elif fault == "attempt":
        context = replace(context, attempt=1)
    elif fault == "file":
        (root / "factor_bundle/sector_data.h5").write_bytes(b"different")
    elif fault == "receipt":
        (root / "daily_bin/materialization_receipt.json").write_bytes(b"{}")
    elif fault == "missing":
        (root / "index_context/index_daily.h5").unlink()
    else:
        (root / "minute_bin/qlib/features/000001.sz/extra.1min.bin").write_bytes(b"not-authorized")
    with pytest.raises((ValueError, OSError)):
        adopt_completed_exports(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
            prepare_ref=prepare, finalize_ref=finalize, apply=True)
    assert not list((tmp_path / ".staging").glob("*.json"))


@pytest.mark.parametrize("hardlinked", [True, False])
def test_inherited_feature_requires_pinned_baseline_and_exact_unchanged_file_identity(tmp_path, hardlinked):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    baseline = tmp_path / "baseline"
    old = baseline / "components/daily_bin/features/000004.sz/close.day.bin"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old-delisted-stock-kept")
    target = root / "daily_bin/qlib/features/000004.sz/close.day.bin"
    target.parent.mkdir()
    if hardlinked:
        os.link(old, target)
    else:
        target.write_bytes(old.read_bytes())
    manifest = {"release_id": "old", "cutoff_trade_date": "2026-08-31",
        "components": {"day_provider_catalog": {"path": "components/daily_bin/instruments/all.txt"}}}
    manifest_hash = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest["dataset_manifest_sha256"] = manifest_hash
    raw = canonical_json_bytes(manifest) + b"\n"
    (baseline / "qe_dataset_manifest.json").write_bytes(raw)
    # The original materialization pins must still agree with the baseline
    # identity. Test fixture replaces only its controller binding, so keep
    # materialization identity checks explicit by rewriting fixture receipts.
    for ref in (prepare, finalize):
        stage = cas.get_json_bounded(ref, max_bytes=1024*1024)
        inner_key = "prepare_receipt_ref" if stage["stage"] == "prepare" else "finalize_receipt_ref"
        inner = cas.get_json_bounded(stage[inner_key], max_bytes=1024*1024)
        if stage["stage"] == "prepare":
            fact = cas.get_json_bounded(inner["refs"]["factor_receipt"], max_bytes=1024*1024)
            fact["predecessor_manifest_sha256"] = manifest_hash
            inner["refs"]["factor_receipt"] = cas.put_json(fact).as_dict()
            idx = cas.get_json_bounded(inner["refs"]["index_materialization_receipt_file"], max_bytes=1024*1024)
            idx["predecessor_manifest_sha256"] = manifest_hash
            idx_raw = canonical_json_bytes(idx)
            (root / "index_context/index_materialization_receipt.json").write_bytes(idx_raw)
            inner["refs"]["index_materialization_receipt_file"] = cas.put_bytes(idx_raw).as_dict()
            for op in stage["qlib_dump_operations"]:
                value = cas.get_json_bounded(op["preparation_ref"], max_bytes=1024*1024)
                value["predecessor_manifest_sha256"] = manifest_hash
                op["preparation_ref"] = cas.put_json(value).as_dict()
        else:
            for dataset in ("daily", "minute"):
                key = dataset + "_materialization_receipt_file_ref"
                value = cas.get_json_bounded(inner[key], max_bytes=1024*1024)
                value["predecessor_manifest_sha256"] = manifest_hash
                data = canonical_json_bytes(value)
                (root / (dataset + "_bin/materialization_receipt.json")).write_bytes(data)
                inner[key] = cas.put_bytes(data).as_dict()
        stage[inner_key] = cas.put_json(inner).as_dict()
        if stage["stage"] == "prepare":
            prepare = cas.put_json(stage)
        else:
            finalize = cas.put_json(stage)
    context = replace(context, plan={**context.plan, "predecessor": {"candidate_root": str(baseline),
        "dataset_manifest_sha256": manifest_hash, "cutoff": "2026-08-31", "release_id": "old"},
        "predecessor_manifest_ref": {"sha256": hashlib.sha256(raw).hexdigest()}})
    kwargs = dict(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
        prepare_ref=prepare, finalize_ref=finalize, apply=False)
    if hardlinked:
        assert adopt_completed_exports(**kwargs)["inherited_unchanged_feature_file_count"] == 1
    else:
        with pytest.raises(ValueError, match="unproved inherited"):
            adopt_completed_exports(**kwargs)


def test_failed_journal_flush_keeps_exports_and_does_not_publish_partial_journal(tmp_path, monkeypatch):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    def fail_flush(_fd):
        raise OSError("simulated disk flush failure")
    monkeypatch.setattr("backend.services.dataset_release.monthly_legacy_adoption.os.fsync", fail_flush)
    with pytest.raises(OSError, match="simulated disk"):
        adopt_completed_exports(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
            prepare_ref=prepare, finalize_ref=finalize, apply=True)
    assert (root / "daily_bin/qlib/features/000001.sz/close.day.bin").read_bytes() == b"actual-original-bytes"
    assert not list((tmp_path / ".staging").glob("*.json"))
    assert not list((tmp_path / ".staging").glob("*.partial"))
