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
