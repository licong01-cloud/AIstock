from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.services.qe_archive.workspace_lifecycle import (
    QEWorkspaceCleanupRequest,
    QEWorkspaceLifecycleService,
    QEWorkspaceManifestEntry,
)


def request_for(root: Path) -> QEWorkspaceCleanupRequest:
    folder = root / "run"
    folder.mkdir()
    artifact = folder / "result.pkl"
    artifact.write_bytes(b"result")
    return QEWorkspaceCleanupRequest(
        source_identity="task:loop:attempt",
        value_class="C",
        reason_code="valid",
        terminal_at=datetime.now(timezone.utc) - timedelta(days=8),
        manifest=(
            QEWorkspaceManifestEntry(str(artifact), "file", hashlib.sha256(b"result").hexdigest(), 6),
            QEWorkspaceManifestEntry(str(folder), "directory"),
        ),
        allowed_roots=(str(root),),
        archive_readback_complete=True,
        asset_readback_complete=True,
        reference_check_complete=True,
        process_check_complete=True,
        tracked_check_complete=True,
    )


@pytest.mark.parametrize(
    "field,reason",
    [
        ("active_process_paths", "active_process"),
        ("referenced_paths", "shared_reference"),
        ("tracked_paths", "tracked_path"),
    ],
)
@pytest.mark.parametrize("relative", [".", "run/retained.pkl"])
def test_ancestor_and_descendant_reference_protect_manifest(tmp_path, field, reason, relative):
    request = request_for(tmp_path)
    request = replace(request, **{field: (str(tmp_path / relative),)})
    plan = QEWorkspaceLifecycleService().plan(request)
    assert not plan.eligible
    assert f"qe_workspace_cleanup_{reason}" in plan.reason_codes


def test_common_string_prefix_is_not_shared_reference(tmp_path):
    request = request_for(tmp_path)
    request = replace(request, referenced_paths=(str(tmp_path / "run-other"),))
    assert QEWorkspaceLifecycleService().plan(request).eligible


@pytest.mark.parametrize(
    "changes",
    [
        {"active_process_paths": ("ANCESTOR",)},
        {"referenced_paths": ("ANCESTOR",)},
        {"tracked_check_complete": False},
        {"asset_readback_complete": False},
        {"value_class": "X", "asset_readback_complete": False},
        {"source_identity": "other"},
        {"allowed_roots": ("OTHER",)},
    ],
)
def test_apply_refreshes_programmatic_safety_before_any_delete(tmp_path, changes):
    request = request_for(tmp_path)
    changes = {
        key: ((str(tmp_path),) if value == ("ANCESTOR",) else (str(tmp_path.parent),) if value == ("OTHER",) else value)
        for key, value in changes.items()
    }
    service = QEWorkspaceLifecycleService()
    plan = service.plan(request)
    receipt = service.apply(plan, refresh_request=lambda: replace(request, **changes))
    assert receipt["workspace_status"] == "cleanup_incomplete"
    assert Path(request.manifest[0].path).read_bytes() == b"result"
    assert receipt["archive_rows_deleted"] == 0


def test_protection_added_between_files_stops_remaining_deletion(tmp_path):
    request = request_for(tmp_path)
    extra = tmp_path / "run" / "second.pkl"
    extra.write_bytes(b"result")
    request = replace(request, manifest=(*request.manifest, replace(request.manifest[0], path=str(extra))))
    calls = []

    def refresh():
        calls.append(True)
        return request if len(calls) <= 2 else replace(request, referenced_paths=(str(extra),))

    service = QEWorkspaceLifecycleService()
    receipt = service.apply(service.plan(request), refresh_request=refresh)
    assert receipt["workspace_status"] == "cleanup_incomplete"
    assert extra.exists()


def test_link_in_ancestor_is_rejected(tmp_path, monkeypatch):
    request = request_for(tmp_path)
    import backend.services.qe_archive.workspace_lifecycle as module

    actual = module._is_link_or_reparse
    monkeypatch.setattr(module, "_is_link_or_reparse", lambda p: p == tmp_path / "run" or actual(p))
    plan = QEWorkspaceLifecycleService().plan(request)
    assert "qe_workspace_cleanup_link_forbidden" in plan.reason_codes


def test_apply_rechecks_ancestor_reparse_after_planning(tmp_path, monkeypatch):
    request = request_for(tmp_path)
    service = QEWorkspaceLifecycleService()
    plan = service.plan(request)
    import backend.services.qe_archive.workspace_lifecycle as module

    actual = module._is_link_or_reparse
    monkeypatch.setattr(module, "_is_link_or_reparse", lambda p: p == tmp_path / "run" or actual(p))
    receipt = service.apply(plan, refresh_request=lambda: request)
    assert receipt["workspace_status"] == "cleanup_incomplete"
    assert Path(request.manifest[0].path).exists()


def test_apply_requires_fresh_programmatic_checks(tmp_path):
    request = request_for(tmp_path)
    service = QEWorkspaceLifecycleService()
    with pytest.raises(ValueError, match="fresh_checks_required"):
        service.apply(service.plan(request))
    assert Path(request.manifest[0].path).exists()


def test_forged_plan_cannot_add_unmanifested_file(tmp_path):
    request = request_for(tmp_path)
    keep = tmp_path / "keep.txt"
    keep.write_bytes(b"result")
    service = QEWorkspaceLifecycleService()
    plan = service.plan(request)
    plan = replace(plan, delete_files=(*plan.delete_files, str(keep)))
    receipt = service.apply(plan, refresh_request=lambda: request)
    assert receipt["workspace_status"] == "cleanup_incomplete"
    assert keep.exists()
    assert Path(request.manifest[0].path).exists()


def test_real_parent_link_is_not_traversed(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    request = request_for(target)
    link = tmp_path / "alias"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"OS does not allow unprivileged symlink: {exc}")
    request = replace(
        request,
        manifest=tuple(
            replace(entry, path=str(link / Path(entry.path).relative_to(target))) for entry in request.manifest
        ),
    )
    plan = QEWorkspaceLifecycleService().plan(request)
    assert not plan.eligible
    assert "qe_workspace_cleanup_link_forbidden" in plan.reason_codes
    assert (target / "run" / "result.pkl").exists()
