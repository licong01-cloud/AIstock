"""Exact-manifest QE workspace cleanup planning and execution."""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .models import sha256_json


@dataclass(frozen=True)
class QEWorkspaceManifestEntry:
    path: str
    entry_type: str = "file"
    sha256: str | None = None
    size_bytes: int | None = None


@dataclass(frozen=True)
class QEWorkspaceCleanupRequest:
    source_identity: str
    value_class: str
    reason_code: str
    terminal_at: datetime
    manifest: tuple[QEWorkspaceManifestEntry, ...]
    allowed_roots: tuple[str, ...]
    archive_readback_complete: bool
    asset_readback_complete: bool
    reference_check_complete: bool
    process_check_complete: bool
    tracked_check_complete: bool
    active_process_paths: tuple[str, ...] = ()
    referenced_paths: tuple[str, ...] = ()
    tracked_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class QEWorkspaceCleanupPlan:
    eligible: bool
    workspace_status: str
    reason_codes: tuple[str, ...]
    manifest_digest: str
    delete_files: tuple[str, ...]
    file_identities: tuple[tuple[str, str, int], ...]
    remove_directories: tuple[str, ...]
    grace_until: str
    source_request: QEWorkspaceCleanupRequest = field(repr=False)


class QEWorkspaceLifecycleService:
    def plan(
        self,
        request: QEWorkspaceCleanupRequest,
        *,
        now: datetime | None = None,
    ) -> QEWorkspaceCleanupPlan:
        current = now or datetime.now(timezone.utc)
        grace_until = request.terminal_at.astimezone(timezone.utc) + _grace_period(
            request.value_class,
            request.reason_code,
        )
        reasons: list[str] = []
        if current < grace_until:
            reasons.append("qe_workspace_cleanup_grace_period")
        if request.value_class in {"A", "B", "C"}:
            if not request.archive_readback_complete:
                reasons.append("qe_workspace_cleanup_archive_readback_incomplete")
            if not request.asset_readback_complete:
                reasons.append("qe_workspace_cleanup_asset_readback_incomplete")
        if not request.reference_check_complete:
            reasons.append("qe_workspace_cleanup_reference_check_incomplete")
        if not request.process_check_complete:
            reasons.append("qe_workspace_cleanup_process_check_incomplete")
        if not request.tracked_check_complete:
            reasons.append("qe_workspace_cleanup_tracked_check_incomplete")

        normalized_entries, validation_reasons = _validate_manifest(request)
        reasons.extend(validation_reasons)
        files = tuple(entry.path for entry in normalized_entries if entry.entry_type == "file")
        file_identities = tuple(
            (entry.path, str(entry.sha256), int(entry.size_bytes))
            for entry in normalized_entries
            if entry.entry_type == "file" and entry.sha256 is not None and entry.size_bytes is not None
        )
        directories = tuple(
            sorted(
                (entry.path for entry in normalized_entries if entry.entry_type == "directory"),
                key=lambda value: len(Path(value).parts),
                reverse=True,
            )
        )
        blocked_refs = _path_intersection(files + directories, request.referenced_paths)
        blocked_processes = _path_intersection(files + directories, request.active_process_paths)
        blocked_tracked = _path_intersection(files + directories, request.tracked_paths)
        if blocked_refs:
            reasons.append("qe_workspace_cleanup_shared_reference")
        if blocked_processes:
            reasons.append("qe_workspace_cleanup_active_process")
        if blocked_tracked:
            reasons.append("qe_workspace_cleanup_tracked_path")

        unique_reasons = tuple(dict.fromkeys(reasons))
        return QEWorkspaceCleanupPlan(
            eligible=not unique_reasons,
            workspace_status="cleanup_pending"
            if not unique_reasons
            else ("grace_period" if unique_reasons == ("qe_workspace_cleanup_grace_period",) else "active"),
            reason_codes=unique_reasons,
            manifest_digest=sha256_json(
                [
                    {
                        "path": entry.path,
                        "entry_type": entry.entry_type,
                        "sha256": entry.sha256,
                        "size_bytes": entry.size_bytes,
                    }
                    for entry in normalized_entries
                ]
            ),
            delete_files=files,
            file_identities=file_identities,
            remove_directories=directories,
            grace_until=grace_until.isoformat(),
            source_request=request,
        )

    def apply(
        self,
        plan: QEWorkspaceCleanupPlan,
        *,
        refresh_request: Callable[[], QEWorkspaceCleanupRequest] | None = None,
    ) -> dict[str, Any]:
        """Refresh existing programmatic safety checks, never ask for manual approval."""
        if not plan.eligible:
            raise ValueError(f"workspace cleanup plan is not eligible: {plan.reason_codes}")
        if refresh_request is None:
            raise ValueError("qe_workspace_cleanup_fresh_checks_required")
        items: list[dict[str, Any]] = []
        failed = False
        identities = {path: (digest, size) for path, digest, size in plan.file_identities}
        try:
            self._refresh(plan, refresh_request)
        except (OSError, ValueError) as exc:
            return _receipt(plan, [{"status": "unknown", "error": f"{type(exc).__name__}: {exc}"}], True)
        for value in plan.delete_files:
            path = Path(value)
            try:
                self._refresh(plan, refresh_request)
                if not path.exists():
                    items.append({"path": value, "status": "already_absent"})
                    continue
                expected = identities.get(value)
                if expected is None:
                    raise ValueError("qe_workspace_cleanup_file_identity_missing")
                before = path.stat(follow_symlinks=False)
                actual_sha, actual_size = _hash_file(path)
                if (actual_sha, actual_size) != expected:
                    raise ValueError("qe_workspace_cleanup_file_identity_changed")
                self._refresh(plan, refresh_request)
                after = path.stat(follow_symlinks=False)
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                    after.st_dev,
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                ):
                    raise ValueError("qe_workspace_cleanup_file_identity_changed")
                path.unlink()
                items.append({"path": value, "status": "deleted"})
            except (OSError, ValueError) as exc:
                failed = True
                items.append({"path": value, "status": "unknown", "error": f"{type(exc).__name__}: {exc}"})
                break
        for value in plan.remove_directories:
            if failed:
                break
            path = Path(value)
            try:
                self._refresh(plan, refresh_request)
                if not path.exists():
                    items.append({"path": value, "status": "already_absent"})
                    continue
                path.rmdir()
                items.append({"path": value, "status": "deleted"})
            except (OSError, ValueError) as exc:
                failed = True
                items.append({"path": value, "status": "unknown", "error": f"{type(exc).__name__}: {exc}"})
        return _receipt(plan, items, failed)

    def _refresh(self, plan: QEWorkspaceCleanupPlan, refresh_request: Callable[[], QEWorkspaceCleanupRequest]) -> None:
        request = refresh_request()
        original = plan.source_request
        if (
            request.source_identity != original.source_identity
            or request.value_class != original.value_class
            or request.reason_code != original.reason_code
            or request.terminal_at != original.terminal_at
            or request.allowed_roots != original.allowed_roots
            or request.manifest != original.manifest
        ):
            raise ValueError("qe_workspace_cleanup_request_changed")
        fresh = self.plan(request)
        if not fresh.eligible:
            raise ValueError(f"qe_workspace_cleanup_safety_changed: {fresh.reason_codes}")
        if (
            fresh.manifest_digest != plan.manifest_digest
            or fresh.delete_files != plan.delete_files
            or fresh.file_identities != plan.file_identities
            or fresh.remove_directories != plan.remove_directories
        ):
            raise ValueError("qe_workspace_cleanup_plan_changed")


def _receipt(plan: QEWorkspaceCleanupPlan, items: list[dict[str, Any]], failed: bool) -> dict[str, Any]:
    return {
        "schema_version": "qe_workspace_cleanup_receipt_v1",
        "workspace_status": "cleanup_incomplete" if failed else "cleaned",
        "manifest_digest": plan.manifest_digest,
        "items": items,
        "archive_rows_deleted": 0,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


def _grace_period(value_class: str, reason_code: str) -> timedelta:
    normalized = str(value_class or "").upper()
    if normalized in {"A", "B", "C"}:
        return timedelta(days=7)
    reason = str(reason_code or "").lower()
    if "validation" in reason or "fixture" in reason or "smoke" in reason or "duplicate" in reason:
        return timedelta(hours=24)
    return timedelta(days=3)


def _validate_manifest(
    request: QEWorkspaceCleanupRequest,
) -> tuple[tuple[QEWorkspaceManifestEntry, ...], list[str]]:
    reasons: list[str] = []
    allowed_roots = tuple(Path(root).resolve() for root in request.allowed_roots)
    if not allowed_roots:
        return (), ["qe_workspace_cleanup_allowed_root_missing"]
    seen: set[str] = set()
    normalized: list[QEWorkspaceManifestEntry] = []
    for entry in request.manifest:
        raw = str(entry.path or "").strip()
        if not raw or any(token in raw for token in ("*", "?", "[", "]")):
            reasons.append("qe_workspace_cleanup_manifest_path_invalid")
            continue
        path = Path(raw)
        try:
            resolved = path.resolve(strict=False)
        except OSError:
            reasons.append("qe_workspace_cleanup_manifest_path_invalid")
            continue
        if str(resolved) in seen:
            reasons.append("qe_workspace_cleanup_manifest_duplicate")
            continue
        seen.add(str(resolved))
        if not any(resolved == root or root in resolved.parents for root in allowed_roots):
            reasons.append("qe_workspace_cleanup_path_outside_allowed_root")
            continue
        if _has_link_ancestor(path):
            reasons.append("qe_workspace_cleanup_link_forbidden")
            continue
        if entry.entry_type not in {"file", "directory"}:
            reasons.append("qe_workspace_cleanup_entry_type_invalid")
            continue
        if path.exists() and entry.entry_type == "file" and not path.is_file():
            reasons.append("qe_workspace_cleanup_entry_type_mismatch")
            continue
        if entry.entry_type == "file":
            digest = str(entry.sha256 or "").strip().lower()
            if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
                reasons.append("qe_workspace_cleanup_file_identity_missing")
                continue
            if entry.size_bytes is None or int(entry.size_bytes) < 0:
                reasons.append("qe_workspace_cleanup_file_identity_missing")
                continue
        if path.exists() and entry.entry_type == "directory" and not path.is_dir():
            reasons.append("qe_workspace_cleanup_entry_type_mismatch")
            continue
        normalized.append(
            QEWorkspaceManifestEntry(
                path=str(resolved),
                entry_type=entry.entry_type,
                sha256=entry.sha256,
                size_bytes=entry.size_bytes,
            )
        )
    if not normalized:
        reasons.append("qe_workspace_cleanup_manifest_empty")
    return tuple(normalized), reasons


def _is_link_or_reparse(path: Path) -> bool:
    if path.is_symlink():
        return True
    attrs = getattr(path.stat(follow_symlinks=False), "st_file_attributes", 0)
    return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)) if os.name == "nt" else False


def _has_link_ancestor(path: Path) -> bool:
    absolute = path.absolute()
    return any(_is_link_or_reparse(item) for item in (absolute, *absolute.parents) if os.path.lexists(item))


def _path_intersection(candidates: Sequence[str], protected: Sequence[str]) -> tuple[str, ...]:
    candidate_paths = {Path(item).resolve(strict=False) for item in candidates}
    protected_paths = {Path(item).resolve(strict=False) for item in protected}
    return tuple(
        sorted(
            str(candidate)
            for candidate in candidate_paths
            if any(
                candidate == reference or candidate in reference.parents or reference in candidate.parents
                for reference in protected_paths
            )
        )
    )


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
