"""Keep successful BUILD work across retries without rereading historical data."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Mapping

from .canonical import canonical_json_bytes, digest_named_fields
from .cas_store import CASRef, CASStore


class MonthlyBuildResumeError(RuntimeError):
    """A retained export cannot be matched to the exact current data inputs."""


def _plain(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if not current.exists():
            break
        value = current.lstat()
        if stat.S_ISLNK(value.st_mode) or int(getattr(value, "st_file_attributes", 0)) & 0x0400:
            raise MonthlyBuildResumeError("retained build path contains a link")


def _signature(path: Path) -> list[int]:
    _plain(path)
    value = path.stat()
    if not stat.S_ISREG(value.st_mode):
        raise MonthlyBuildResumeError("retained build data is not a regular file")
    return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns]


def _atomic(path: Path, value: Mapping[str, Any]) -> None:
    _plain(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    _plain(path)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as writer:
            writer.write(canonical_json_bytes(value) + b"\n")
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _identity(context, compiled, release_root: Path):
    raw = context.plan.get("candidate_root")
    if raw is None:
        return None  # Legacy in-process callers have no durable monthly identity.
    target = Path(raw)
    _plain(release_root)
    if not target.is_absolute() or target.parent.resolve() != release_root.resolve():
        raise MonthlyBuildResumeError("retained build target is outside the release root")
    if re.fullmatch(r"dmr_[0-9a-f]{32}", context.operation_id) is None:
        raise MonthlyBuildResumeError("retained build operation is invalid")
    binding = {
        "operation_id": context.operation_id,
        "candidate_name": target.name,
        "release_id": context.plan.get("release_id"),
        "target_cutoff": context.plan.get("target_cutoff"),
        "source_bundle_sha256": compiled.source_bundle_sha256,
        "action_plan_digest": compiled.physical_plan.get("action_plan_digest"),
        "predecessor": context.plan.get("predecessor"),
    }
    digest = digest_named_fields("aistock_monthly_build_data_binding_v1", binding)
    path = release_root / ".staging" / f".{target.name}.{context.operation_id}.resume-{digest}.json"
    return binding, digest, path


def _read_record(path: Path, binding: Mapping[str, Any], digest: str) -> dict[str, Any]:
    _plain(path)
    if not path.is_file() or path.stat().st_size > 128 * 1024 * 1024:
        raise MonthlyBuildResumeError("retained build journal is unavailable")
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError) as exc:
        raise MonthlyBuildResumeError("retained build journal is unreadable") from exc
    if (
        not isinstance(record, dict)
        or record.get("schema_version") != "aistock_monthly_build_resume_v1"
        or record.get("binding") != dict(binding)
        or record.get("binding_digest") != digest
        or not isinstance(record.get("steps"), dict)
        or not isinstance(record.get("files"), dict)
    ):
        raise MonthlyBuildResumeError("retained build data identity differs")
    return record


def _retained_root(record, release_root: Path, binding, context) -> Path:
    attempt = record.get("materialization_attempt")
    if type(attempt) is not int or not 0 < attempt <= context.attempt:
        raise MonthlyBuildResumeError("retained build attempt is invalid")
    expected = f".staging/.{binding['candidate_name']}.{context.operation_id}.attempt-{attempt}.building"
    if record.get("staging_relative_path") != expected:
        raise MonthlyBuildResumeError("retained build staging identity differs")
    root = release_root / expected
    _plain(root)
    if not root.is_dir():
        raise MonthlyBuildResumeError("retained build staging is unavailable")
    return root


def retained_staging_root(context, compiled, release_root: Path, default: Path) -> Path:
    """Resolve data location only; the runner verifies signatures once on use."""
    identity = _identity(context, compiled, release_root)
    if identity is None:
        return default
    binding, digest, path = identity
    if not path.exists():
        # Older workers did not write this journal. Do not silently re-export
        # their completed assets during the first deployment of this repair.
        parent = release_root / ".staging"
        _plain(parent)
        pattern = re.compile(rf"\.{re.escape(binding['candidate_name'])}\.{re.escape(context.operation_id)}\.attempt-([0-9]+)\.building")
        if parent.is_dir():
            for previous in parent.iterdir():
                matched = pattern.fullmatch(previous.name)
                if matched and 0 < int(matched[1]) <= context.attempt:
                    _plain(previous)
                    if any((previous / component / receipt).is_file() for component, receipt in (
                        ("daily_bin", "materialization_receipt.json"),
                        ("minute_bin", "materialization_receipt.json"),
                        ("factor_bundle", "factor_checkpoint.json"),
                    )):
                        raise MonthlyBuildResumeError(
                            f"completed legacy exports retained at {previous}; explicit receipt adoption required, re-export forbidden"
                        )
        return default
    record = _read_record(path, binding, digest)
    if "prepare" not in record["steps"]:
        return default  # Preserve incomplete preparation; never overwrite it.
    return _retained_root(record, release_root, binding, context)


class MonthlyBuildResume:
    def __init__(self, *, context, compiled, release_root: Path, staging_root: Path, cas: CASStore):
        self.context, self.root, self.cas = context, staging_root, cas
        identity = _identity(context, compiled, release_root)
        self.path = None
        self.record: dict[str, Any] = {}
        if identity is None:
            return
        binding, digest, self.path = identity
        if self.path.exists():
            self.record = _read_record(self.path, binding, digest)
            if "prepare" in self.record["steps"]:
                if _retained_root(self.record, release_root, binding, context) != staging_root:
                    raise MonthlyBuildResumeError("runner did not use the retained data location")
                for relative, expected in self.record["files"].items():
                    if not isinstance(relative, str) or not relative or any(token in relative for token in ("\\", ":", "\x00")) or ".." in Path(relative).parts or Path(relative).is_absolute():
                        raise MonthlyBuildResumeError("retained data file escaped staging")
                    if _signature(staging_root / relative) != expected:
                        raise MonthlyBuildResumeError(f"retained export changed: {relative}")
                return
        self.record = {
            "schema_version": "aistock_monthly_build_resume_v1", "binding": binding,
            "binding_digest": digest, "materialization_attempt": context.attempt,
            "staging_relative_path": staging_root.relative_to(release_root).as_posix(),
            "steps": {}, "files": {},
        }

    def get(self, step: str) -> Mapping[str, Any] | None:
        ref = self.record.get("steps", {}).get(step)
        if ref is None:
            return None
        self.context.checkpoint()
        value = self.cas.get_json_bounded(ref, max_bytes=64 * 1024 * 1024)
        if not isinstance(value, Mapping):
            raise MonthlyBuildResumeError("retained completion receipt is invalid")
        self.context.progress({"phase": "BUILD_REUSE_COMPLETED", "dataset": step})
        return value

    def save(self, step: str, reference: CASRef, *, roots: tuple[str, ...]) -> None:
        if self.path is None:
            return
        self.context.checkpoint()
        # Metadata signatures only. Never rehash/revalidate old price history.
        for relative_root in roots:
            folder = self.root / relative_root
            if not folder.exists():
                continue
            _plain(folder)
            for base, dirs, names in os.walk(folder, followlinks=False):
                for name in dirs:
                    _plain(Path(base) / name)
                for name in names:
                    path = Path(base) / name
                    # meta_export is intentionally refreshed by finalize-bins.
                    if name == "meta_export.json":
                        continue
                    if name.endswith(".partial") or ".partial." in name:
                        raise MonthlyBuildResumeError("completed build still has partial output")
                    relative = path.relative_to(self.root).as_posix()
                    self.record["files"][relative] = _signature(path)
                self.context.checkpoint()
        self.record["steps"][step] = reference.as_dict()
        _atomic(self.path, self.record)

    def preserve_incomplete_writer(self, dataset: str) -> None:
        if self.path is None or not (self.root / dataset).exists():
            return
        if dataset not in {"daily_bin", "minute_bin"}:
            raise MonthlyBuildResumeError("unknown incomplete writer scope")
        self.context.checkpoint()
        source = self.root / dataset
        _plain(source)
        destination = self.root.parent / f".unfinished.{self.context.operation_id}.{dataset}.before-{self.context.attempt}"
        if destination.exists():
            raise MonthlyBuildResumeError("incomplete export preservation target exists")
        source.rename(destination)  # Keep every byte; never delete a failed export.

    def write_lease(self, release_digest: str) -> None:
        if self.path is None:
            return
        self.context.checkpoint()
        _atomic(self.root / ".monthly-materialization-lease.json", {
            "schema_version": "aistock_monthly_retained_materialization_lease_v1",
            "operation_id": self.context.operation_id,
            "candidate_name": self.record["binding"]["candidate_name"],
            "execution_attempt": self.context.attempt,
            "materialization_attempt": self.record["materialization_attempt"],
            "release_digest": release_digest,
            "data_binding_digest": self.record["binding_digest"],
        })

    def begin_finalization(self) -> None:
        """Retry partial packaging without replacing any materialized component."""
        if self.path is None:
            return
        self.context.checkpoint()
        if self.record.get("finalization_started"):
            destination = self.root.parent / f".unfinished.{self.context.operation_id}.packaging.before-{self.context.attempt}"
            if destination.exists():
                raise MonthlyBuildResumeError("partial packaging preservation target exists")
            names = ("components", "reports", "stock_pools", "provenance", "qe_dataset_manifest.json")
            existing = [self.root / name for name in names if (self.root / name).exists()]
            if existing:
                destination.mkdir()
                for source in existing:
                    _plain(source)
                    source.rename(destination / source.name)
        self.record["finalization_started"] = True
        _atomic(self.path, self.record)


def materialization_attempt(staging_root: Path, *, operation_id: str, execution_attempt: int, candidate_name: str, release_digest: str) -> int:
    """Keep current execution fencing distinct from retained data provenance."""
    path = staging_root / ".monthly-materialization-lease.json"
    _plain(path)
    if not path.is_file() or path.stat().st_size > 4096:
        raise MonthlyBuildResumeError("retained materialization lease is unavailable")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise MonthlyBuildResumeError("retained materialization lease is unreadable") from exc
    if not isinstance(value, dict):
        raise MonthlyBuildResumeError("retained materialization lease is not an object")
    attempt = value.get("materialization_attempt")
    if (
        value.get("schema_version") != "aistock_monthly_retained_materialization_lease_v1"
        or value.get("operation_id") != operation_id
        or value.get("execution_attempt") != execution_attempt
        or value.get("candidate_name") != candidate_name
        or value.get("release_digest") != release_digest
        or type(attempt) is not int or not 0 < attempt <= execution_attempt
        or re.fullmatch(r"[0-9a-f]{64}", str(value.get("data_binding_digest"))) is None
    ):
        raise MonthlyBuildResumeError("retained materialization lease identity differs")
    return attempt
