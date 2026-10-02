from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.services.qe_archive.workspace_lifecycle import (
    QEWorkspaceCleanupRequest,
    QEWorkspaceLifecycleService,
    QEWorkspaceManifestEntry,
)


def _request(root: Path, *, value_class: str = "C", reason_code: str = "valid") -> QEWorkspaceCleanupRequest:
    artifact = root / "run" / "result.pkl"
    directory = artifact.parent
    directory.mkdir(parents=True)
    artifact.write_bytes(b"result")
    return QEWorkspaceCleanupRequest(
        source_identity="task-1:loop-1:attempt-1",
        value_class=value_class,
        reason_code=reason_code,
        terminal_at=datetime.now(timezone.utc) - timedelta(days=8),
        manifest=(
            QEWorkspaceManifestEntry(
                path=str(artifact),
                entry_type="file",
                sha256=hashlib.sha256(b"result").hexdigest(),
                size_bytes=6,
            ),
            QEWorkspaceManifestEntry(path=str(directory), entry_type="directory"),
        ),
        allowed_roots=(str(root),),
        archive_readback_complete=True,
        asset_readback_complete=True,
        reference_check_complete=True,
        process_check_complete=True,
        tracked_check_complete=True,
    )


def test_abc_cleanup_requires_archive_and_asset_readback(tmp_path: Path) -> None:
    request = _request(tmp_path)
    blocked = QEWorkspaceLifecycleService().plan(
        QEWorkspaceCleanupRequest(
            **{
                **request.__dict__,
                "archive_readback_complete": False,
                "asset_readback_complete": False,
            }
        )
    )
    assert blocked.eligible is False
    assert "qe_workspace_cleanup_archive_readback_incomplete" in blocked.reason_codes
    assert "qe_workspace_cleanup_asset_readback_incomplete" in blocked.reason_codes


def test_cleanup_blocks_process_reference_tracked_path_and_outside_root(tmp_path: Path) -> None:
    request = _request(tmp_path)
    artifact = request.manifest[0].path
    outside = tmp_path.parent / "outside-qe-asset.txt"
    blocked = QEWorkspaceLifecycleService().plan(
        QEWorkspaceCleanupRequest(
            **{
                **request.__dict__,
                "manifest": (*request.manifest, QEWorkspaceManifestEntry(path=str(outside))),
                "active_process_paths": (artifact,),
                "referenced_paths": (artifact,),
                "tracked_paths": (artifact,),
            }
        )
    )
    assert blocked.eligible is False
    assert "qe_workspace_cleanup_active_process" in blocked.reason_codes
    assert "qe_workspace_cleanup_shared_reference" in blocked.reason_codes
    assert "qe_workspace_cleanup_tracked_path" in blocked.reason_codes
    assert "qe_workspace_cleanup_path_outside_allowed_root" in blocked.reason_codes


def test_x_validation_uses_short_grace_and_never_deletes_archive_rows(tmp_path: Path) -> None:
    request = _request(tmp_path, value_class="X", reason_code="qe_lifecycle_purpose_validation")
    request = QEWorkspaceCleanupRequest(
        **{
            **request.__dict__,
            "terminal_at": datetime.now(timezone.utc) - timedelta(hours=25),
            "archive_readback_complete": False,
            "asset_readback_complete": False,
        }
    )
    service = QEWorkspaceLifecycleService()
    plan = service.plan(request)
    receipt = service.apply(plan)

    assert plan.eligible is True
    assert receipt["workspace_status"] == "cleaned"
    assert receipt["archive_rows_deleted"] == 0
    assert not Path(request.manifest[0].path).exists()
    assert not Path(request.manifest[1].path).exists()


def test_partial_failure_receipt_is_retryable_and_does_not_guess(tmp_path: Path) -> None:
    request = _request(tmp_path)
    extra = Path(request.manifest[1].path) / "unmanifested.txt"
    extra.write_text("keep", encoding="utf-8")
    service = QEWorkspaceLifecycleService()
    plan = service.plan(request)
    receipt = service.apply(plan)

    assert receipt["workspace_status"] == "cleanup_incomplete"
    assert any(item["status"] == "unknown" for item in receipt["items"])
    assert extra.exists()


def test_cleanup_rechecks_file_identity_immediately_before_delete(tmp_path: Path) -> None:
    request = _request(tmp_path)
    service = QEWorkspaceLifecycleService()
    plan = service.plan(request)
    Path(request.manifest[0].path).write_bytes(b"changed")

    receipt = service.apply(plan)

    assert receipt["workspace_status"] == "cleanup_incomplete"
    assert Path(request.manifest[0].path).exists()
    assert any("identity_changed" in item.get("error", "") for item in receipt["items"])
