"""Fail-closed hardlink reuse with an explicit copy-on-write mutation set."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from .canonical import normalize_root_relative_path
from .errors import DatasetReleaseError


_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class CopyOnWriteError(DatasetReleaseError):
    """Base class for immutable clone contract failures."""

    code = "DATASET_RELEASE_COPY_ON_WRITE_INVALID"


class UnsafeMutationSet(CopyOnWriteError):
    """Raised when a writer target is missing or is still linked to its source."""

    code = "BLOCKED_UNSAFE_MUTATION_SET"


class SourceMerkleChanged(CopyOnWriteError):
    """Raised when reuse or mutation changed the source candidate bytes."""

    code = "BLOCKED_SOURCE_MERKLE_CHANGED"


@dataclass(frozen=True, slots=True)
class TreeFile:
    relative_path: str
    size_bytes: int
    sha256: str
    device: int
    inode: int
    link_count: int
    mtime_ns: int

    def content_identity(self) -> tuple[str, int, str]:
        return self.relative_path, self.size_bytes, self.sha256


@dataclass(frozen=True, slots=True)
class CowFile:
    relative_path: str
    mode: str
    source: TreeFile
    target_device: int
    target_inode: int
    target_link_count: int


@dataclass(frozen=True, slots=True)
class CopyOnWritePlan:
    source_root: Path
    target_root: Path
    replace_existing_paths: frozenset[str]
    create_new_paths: frozenset[str]
    deferred_existing_paths: frozenset[str]
    source_merkle_before: str
    source_files: tuple[TreeFile, ...]
    files: tuple[CowFile, ...]
    schema_version: str = "dataset_release_copy_on_write_v2"

    @property
    def mutation_paths(self) -> frozenset[str]:
        return self.replace_existing_paths.union(self.create_new_paths)

    def file(self, relative_path: str) -> CowFile:
        normalized = normalize_relative_path(relative_path)
        for item in self.files:
            if item.relative_path == normalized:
                return item
        raise KeyError(normalized)

    def receipt(self) -> dict[str, Any]:
        receipt = {
            "schema_version": self.schema_version,
            "source_root": str(self.source_root),
            "target_root": str(self.target_root),
            "replace_existing_targets": sorted(self.replace_existing_paths),
            "create_new_targets": sorted(self.create_new_paths),
            "mutation_paths": sorted(self.mutation_paths),
            "source_merkle_before": self.source_merkle_before,
            "files": [
                {
                    "relative_path": item.relative_path,
                    "mode": item.mode,
                    "sha256": item.source.sha256,
                    "size_bytes": item.source.size_bytes,
                    "source_identity": {
                        "device": item.source.device,
                        "inode": item.source.inode,
                        "link_count_after_clone": item.source.link_count,
                    },
                    "target_identity": {
                        "device": item.target_device,
                        "inode": item.target_inode,
                        "link_count_after_clone": item.target_link_count,
                    },
                }
                for item in self.files
            ],
        }
        if self.deferred_existing_paths:
            receipt["deferred_existing_targets"] = sorted(self.deferred_existing_paths)
        return receipt


def prepare_copy_on_write_tree(
    source_root: Path,
    target_root: Path,
    *,
    mutation_paths: Iterable[str | Path] | None = None,
    replace_existing_targets: Iterable[str | Path] = (),
    create_new_targets: Iterable[str | Path] = (),
    defer_replace_targets: Iterable[str | Path] = (),
    source_sealed: bool,
    expected_source_merkle: str,
) -> CopyOnWritePlan:
    """Clone a sealed tree, copying every exact future writer target.

    All non-mutation files are hardlinked.  The caller must explicitly assert
    that the source is sealed and provide its frozen Merkle root.  Both
    preconditions are checked before creating ``target_root``; otherwise
    classifying the complement as reusable would be unsafe.
    """

    if source_sealed is not True:
        raise CopyOnWriteError("hardlink reuse requires source_sealed=True")
    source_root = _require_plain_root(source_root, must_exist=True)
    requested_target = Path(target_root).expanduser()
    if not requested_target.is_absolute():
        requested_target = requested_target.absolute()
    target_parent = requested_target.parent.resolve(strict=True)
    _assert_existing_chain(target_parent)
    target_root = target_parent / requested_target.name
    if target_root.exists():
        raise FileExistsError(target_root)
    if source_root == target_root or source_root in target_root.parents:
        raise CopyOnWriteError("target_root must be outside source_root")
    explicit_replace = frozenset(normalize_relative_path(value) for value in replace_existing_targets)
    explicit_create = frozenset(normalize_relative_path(value) for value in create_new_targets)
    if mutation_paths is not None:
        if explicit_replace or explicit_create:
            raise UnsafeMutationSet("legacy mutation_paths cannot be mixed with split writer targets")
        explicit_replace = frozenset(normalize_relative_path(value) for value in mutation_paths)
    if explicit_replace.intersection(explicit_create):
        raise UnsafeMutationSet("replace/create writer targets overlap")
    mutations = explicit_replace.union(explicit_create)
    if not mutations:
        raise UnsafeMutationSet("mutation set must be explicitly non-empty")
    deferred = frozenset(normalize_relative_path(value) for value in defer_replace_targets)
    if not deferred.issubset(explicit_replace):
        raise UnsafeMutationSet("deferred targets must be declared replace-existing targets")

    source_files, source_merkle = tree_merkle(source_root)
    if source_merkle != expected_source_merkle:
        raise SourceMerkleChanged(
            "source candidate Merkle differs from frozen baseline: "
            f"expected={expected_source_merkle} actual={source_merkle}"
        )
    source_by_path = {normalize_relative_path(item.relative_path): item for item in source_files}
    missing = sorted(explicit_replace.difference(source_by_path))
    if missing:
        raise UnsafeMutationSet(f"mutation set references files absent from source: {missing}")
    create_conflicts = sorted(explicit_create.intersection(source_by_path))
    if create_conflicts:
        raise UnsafeMutationSet(f"create-new targets already exist in source: {create_conflicts}")

    target_root.mkdir(parents=True, exist_ok=False)
    cloned: list[CowFile] = []
    for source in source_files:
        canonical_relative = normalize_relative_path(source.relative_path)
        source_path = source_root / Path(source.relative_path)
        target_path = target_root / Path(source.relative_path)
        if canonical_relative in deferred:
            cloned.append(
                CowFile(
                    relative_path=canonical_relative,
                    mode="deferred",
                    source=source,
                    target_device=0,
                    target_inode=0,
                    target_link_count=0,
                )
            )
            continue
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _assert_plain_existing_chain(target_path.parent, target_root)
        mode = "copy" if canonical_relative in explicit_replace else "hardlink"
        if mode == "copy":
            copied_digest = hashlib.sha256()
            with target_path.open("xb") as destination, source_path.open("rb") as origin:
                while block := origin.read(1024 * 1024):
                    copied_digest.update(block)
                    destination.write(block)
                destination.flush()
                os.fsync(destination.fileno())
            if copied_digest.hexdigest() != source.sha256:
                raise SourceMerkleChanged(f"source bytes changed while copying: {source.relative_path}")
            shutil.copystat(source_path, target_path, follow_symlinks=False)
        else:
            try:
                os.link(source_path, target_path)
            except OSError as exc:
                raise CopyOnWriteError(
                    f"hardlink reuse failed for {source.relative_path}; "
                    "cross-volume or unsupported targets must use a different plan"
                ) from exc
        source_after = _file_metadata(source_path, source_root, known_sha256=source.sha256)
        target_identity = _file_metadata(target_path, target_root, known_sha256=source.sha256)
        _assert_stable_source_metadata(source, source_after)
        same_inode = _same_file_identity(source_after, target_identity)
        if mode == "copy" and same_inode:
            raise UnsafeMutationSet(f"mutation target still aliases source inode: {source.relative_path}")
        if mode == "hardlink" and not same_inode:
            raise CopyOnWriteError(f"reused target is not a hardlink: {source.relative_path}")
        cloned.append(
            CowFile(
                relative_path=canonical_relative,
                mode=mode,
                source=source_after,
                target_device=target_identity.device,
                target_inode=target_identity.inode,
                target_link_count=target_identity.link_count,
            )
        )

    for relative in sorted(explicit_create):
        target_path = target_root / Path(relative)
        if target_path.exists():
            raise UnsafeMutationSet(f"create-new target already exists: {relative}")
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise UnsafeMutationSet(f"create-new target parent conflicts: {relative}") from exc
        _assert_plain_existing_chain(target_path.parent, target_root)

    plan = CopyOnWritePlan(
        source_root=source_root,
        target_root=target_root,
        replace_existing_paths=explicit_replace,
        create_new_paths=explicit_create,
        deferred_existing_paths=deferred,
        source_merkle_before=source_merkle,
        source_files=source_files,
        files=tuple(cloned),
    )
    _assert_source_metadata_unchanged(plan)
    assert_writer_targets_private(plan, mutations)
    return plan


def clone_sealed_tree_for_reuse(
    source_root: Path,
    target_root: Path,
    *,
    source_sealed: bool,
    expected_source_merkle: str,
) -> dict[str, Any]:
    """Hardlink an immutable component without creating any writer target."""

    if source_sealed is not True:
        raise CopyOnWriteError("hardlink reuse requires source_sealed=True")
    source_root = _require_plain_root(source_root, must_exist=True)
    requested_target = Path(target_root).expanduser()
    if not requested_target.is_absolute():
        requested_target = requested_target.absolute()
    target_parent = requested_target.parent.resolve(strict=True)
    _assert_existing_chain(target_parent)
    target_root = target_parent / requested_target.name
    if target_root.exists():
        raise FileExistsError(target_root)
    if source_root == target_root or source_root in target_root.parents:
        raise CopyOnWriteError("target_root must be outside source_root")
    source_files, source_merkle = tree_merkle(source_root)
    if source_merkle != expected_source_merkle:
        raise SourceMerkleChanged(
            "source candidate Merkle differs from frozen baseline: "
            f"expected={expected_source_merkle} actual={source_merkle}"
        )
    target_root.mkdir(parents=False, exist_ok=False)
    cloned: list[dict[str, Any]] = []
    for source in source_files:
        source_path = source_root / Path(source.relative_path)
        target_path = target_root / Path(source.relative_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _assert_plain_existing_chain(target_path.parent, target_root)
        try:
            os.link(source_path, target_path)
        except OSError as exc:
            raise CopyOnWriteError(
                f"hardlink reuse failed for {source.relative_path}; source and candidate staging must share a volume"
            ) from exc
        source_after = _file_metadata(source_path, source_root, known_sha256=source.sha256)
        target_identity = _file_metadata(target_path, target_root, known_sha256=source.sha256)
        _assert_stable_source_metadata(source, source_after)
        if not _same_file_identity(source_after, target_identity):
            raise CopyOnWriteError(f"reused target is not a hardlink: {source.relative_path}")
        cloned.append(
            {
                "relative_path": source.relative_path,
                "size_bytes": source.size_bytes,
                "sha256": source.sha256,
                "source_device": source_after.device,
                "source_inode": source_after.inode,
                "target_device": target_identity.device,
                "target_inode": target_identity.inode,
            }
        )
    # One post-clone source read is sufficient: every target file is proven to
    # be the same inode, so hashing the target tree again would reread exactly
    # the same bytes without adding evidence.
    source_merkle_after = tree_merkle(source_root)[1]
    if source_merkle_after != source_merkle:
        raise SourceMerkleChanged("reuse clone changed source content Merkle")
    target_merkle = source_merkle_after
    return {
        "schema_version": "dataset_release_hardlink_reuse_clone_v1",
        "source_root": str(source_root),
        "target_root": str(target_root),
        "source_merkle_before": source_merkle,
        "source_merkle_after": source_merkle_after,
        "target_merkle": target_merkle,
        "files": cloned,
        "writer_targets": [],
    }


def restore_copy_on_write_plan(
    receipt: Mapping[str, Any],
    *,
    expected_source_root: Path,
    expected_target_root: Path,
) -> CopyOnWritePlan:
    """Restore a prepared COW plan across a supervised stage boundary.

    The receipt is only an identity hint.  Source bytes/Merkle, the complete
    target path set, and every copy-vs-hardlink inode relationship are read
    back before a trusted adopter may use the returned plan.
    """

    if receipt.get("schema_version") != "dataset_release_copy_on_write_v2":
        raise CopyOnWriteError("copy-on-write receipt schema differs")
    source_root = _require_plain_root(expected_source_root, must_exist=True)
    target_root = _require_plain_root(expected_target_root, must_exist=True)
    if (
        Path(str(receipt.get("source_root", ""))).resolve(strict=True) != source_root
        or Path(str(receipt.get("target_root", ""))).resolve(strict=True) != target_root
    ):
        raise CopyOnWriteError("copy-on-write receipt roots differ")
    replace = frozenset(normalize_relative_path(value) for value in receipt.get("replace_existing_targets") or ())
    create = frozenset(normalize_relative_path(value) for value in receipt.get("create_new_targets") or ())
    deferred = frozenset(normalize_relative_path(value) for value in receipt.get("deferred_existing_targets") or ())
    if not replace.union(create) or replace.intersection(create) or not deferred.issubset(replace):
        raise CopyOnWriteError("copy-on-write restored mutation set is invalid")
    source_files, source_merkle = tree_merkle(source_root)
    if source_merkle != receipt.get("source_merkle_before"):
        raise SourceMerkleChanged("restored copy-on-write source Merkle differs")
    source_by_path = {normalize_relative_path(item.relative_path): item for item in source_files}
    if not replace.issubset(source_by_path) or create.intersection(source_by_path):
        raise UnsafeMutationSet("restored copy-on-write targets differ from source")
    target_actual = _casefold_file_map(target_root)
    durable_target_actual = {
        path: physical for path, physical in target_actual.items() if not path.startswith(".writer-private/")
    }
    if set(durable_target_actual) != set(source_by_path).difference(deferred):
        raise UnsafeMutationSet("prepared copy-on-write target path set differs before adoption")
    receipt_files = receipt.get("files")
    if not isinstance(receipt_files, list):
        raise CopyOnWriteError("copy-on-write receipt file list is invalid")
    expected_modes = {
        normalize_relative_path(str(item.get("relative_path", ""))): str(item.get("mode", ""))
        for item in receipt_files
        if isinstance(item, Mapping)
    }
    if set(expected_modes) != set(source_by_path):
        raise CopyOnWriteError("copy-on-write receipt file identities differ")
    cloned: list[CowFile] = []
    for relative, source in sorted(source_by_path.items()):
        mode = "deferred" if relative in deferred else ("copy" if relative in replace else "hardlink")
        if expected_modes[relative] != mode:
            raise CopyOnWriteError("copy-on-write restored mode differs")
        source_path = source_root / Path(source.relative_path)
        source_identity = _file_metadata(source_path, source_root, known_sha256=source.sha256)
        if mode == "deferred":
            if relative in durable_target_actual:
                raise UnsafeMutationSet(f"deferred copy-on-write target already exists: {relative}")
            cloned.append(
                CowFile(
                    relative_path=relative,
                    mode=mode,
                    source=source_identity,
                    target_device=0,
                    target_inode=0,
                    target_link_count=0,
                )
            )
            continue
        target_path = target_root / Path(durable_target_actual[relative])
        target_identity = _file_metadata(target_path, target_root, known_sha256=source.sha256)
        same_inode = _same_file_identity(source_identity, target_identity)
        if (mode == "copy" and same_inode) or (mode == "hardlink" and not same_inode):
            raise UnsafeMutationSet(f"prepared copy-on-write inode relationship differs: {relative}")
        cloned.append(
            CowFile(
                relative_path=relative,
                mode=mode,
                source=source_identity,
                target_device=target_identity.device,
                target_inode=target_identity.inode,
                target_link_count=target_identity.link_count,
            )
        )
    plan = CopyOnWritePlan(
        source_root=source_root,
        target_root=target_root,
        replace_existing_paths=replace,
        create_new_paths=create,
        deferred_existing_paths=deferred,
        source_merkle_before=source_merkle,
        source_files=source_files,
        files=tuple(cloned),
    )
    assert_writer_targets_private(plan, plan.mutation_paths)
    return plan


def assert_writer_targets_private(
    plan: CopyOnWritePlan,
    writer_paths: Iterable[str | Path],
) -> None:
    """Fail before a writer starts unless every target has a private inode."""

    for raw_path in writer_paths:
        relative = normalize_relative_path(raw_path)
        if relative not in plan.mutation_paths:
            raise UnsafeMutationSet(f"writer target is absent from mutation set: {relative}")
        item = None if relative in plan.create_new_paths else plan.file(relative)
        if relative in plan.deferred_existing_paths:
            target_path = plan.target_root / Path(item.source.relative_path)
            if target_path.exists():
                raise UnsafeMutationSet(f"deferred writer target already exists: {relative}")
            _assert_plain_existing_chain(target_path.parent, plan.target_root)
            continue
        physical_relative = relative if item is None else item.source.relative_path
        target_path = plan.target_root / Path(physical_relative)
        if relative in plan.create_new_paths:
            if target_path.exists():
                raise UnsafeMutationSet(f"create-new writer target already exists: {relative}")
            _assert_plain_existing_chain(target_path.parent, plan.target_root)
            continue
        assert item is not None
        source_path = plan.source_root / Path(item.source.relative_path)
        source_identity = _file_metadata(
            source_path,
            plan.source_root,
            known_sha256=item.source.sha256,
        )
        target_identity = _file_metadata(
            target_path,
            plan.target_root,
            known_sha256=item.source.sha256,
        )
        if item.mode != "copy" or _same_file_identity(source_identity, target_identity):
            raise UnsafeMutationSet(f"writer target is not private from source: {relative}")


def atomic_write_mutation(
    plan: CopyOnWritePlan,
    relative_path: str | Path,
    payload: bytes | bytearray | memoryview,
) -> dict[str, Any]:
    """Atomically replace or create one explicitly declared staging target."""

    relative = normalize_relative_path(relative_path)
    assert_writer_targets_private(plan, [relative])
    item = None if relative in plan.create_new_paths else plan.file(relative)
    physical_relative = relative if item is None else item.source.relative_path
    target = plan.target_root / Path(physical_relative)
    descriptor, name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".cow.partial", dir=str(target.parent))
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
        if relative in plan.create_new_paths:
            try:
                os.link(temporary, target)
            except FileExistsError as exc:
                raise UnsafeMutationSet(f"create-new writer target appeared before publish: {relative}") from exc
        else:
            os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    _assert_source_metadata_unchanged(plan)
    target_identity = _file_metadata(
        target,
        plan.target_root,
        known_sha256=hashlib.sha256(bytes(payload)).hexdigest(),
    )
    if relative in plan.replace_existing_paths:
        assert item is not None
        source_identity = _file_metadata(
            plan.source_root / Path(item.source.relative_path),
            plan.source_root,
            known_sha256=item.source.sha256,
        )
        if _same_file_identity(source_identity, target_identity):
            raise UnsafeMutationSet(f"atomic mutation re-linked source inode: {relative}")
    return {
        "relative_path": relative,
        "write_mode": ("create_new_create_if_absent" if relative in plan.create_new_paths else "replace_private_copy"),
        "sha256": target_identity.sha256,
        "size_bytes": target.stat().st_size,
        "source_merkle_verification": "deferred_to_writer_target_manifest",
    }


def adopt_isolated_writer_patch(
    plan: CopyOnWritePlan,
    patch_root: Path,
    *,
    patch_targets: Mapping[str | Path, str | Path],
) -> dict[str, Any]:
    """Adopt exact private outputs without exposing baseline hardlinks to a writer.

    The untrusted/external writer runs only below ``patch_root``.  This trusted
    adopter accepts exactly one patch file for every declared mutation target,
    streams each patch once into a private temporary file, and atomically
    installs it into the already-private COW target.  Unmapped scratch files
    are ignored and can never alias or modify the sealed baseline.
    """

    patch_root = _require_plain_root(patch_root, must_exist=True)
    if (
        patch_root == plan.source_root
        or patch_root in plan.source_root.parents
        or plan.source_root in patch_root.parents
        or patch_root == plan.target_root
        or patch_root in plan.target_root.parents
        or plan.target_root in patch_root.parents
    ):
        raise UnsafeMutationSet("isolated patch root overlaps source/COW roots")
    normalized = {
        normalize_relative_path(target): normalize_relative_path(patch) for target, patch in patch_targets.items()
    }
    expected_targets = set(plan.mutation_paths).difference(plan.deferred_existing_paths)
    if set(normalized) != expected_targets:
        raise UnsafeMutationSet("isolated patch target set differs from declared mutation set")
    if len(set(normalized.values())) != len(normalized):
        raise UnsafeMutationSet("isolated patch files are duplicated")
    _assert_source_metadata_unchanged(plan)
    assert_writer_targets_private(plan, normalized)
    adopted: list[dict[str, Any]] = []
    for target_relative in sorted(normalized):
        patch_relative = normalized[target_relative]
        patch_actual = _casefold_file_map(patch_root).get(patch_relative)
        patch = patch_root / Path(patch_actual or patch_relative)
        if not patch.is_file():
            raise UnsafeMutationSet(f"isolated patch file is missing: {patch_relative}")
        _assert_plain_existing_chain(patch.parent, patch_root)
        _assert_plain_node(patch)
        item = None if target_relative in plan.create_new_paths else plan.file(target_relative)
        physical_relative = target_relative if item is None else item.source.relative_path
        target = plan.target_root / Path(physical_relative)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".adopt.partial", dir=target.parent
        )
        temporary = Path(temporary_name)
        digest = hashlib.sha256()
        size_bytes = 0
        try:
            with os.fdopen(descriptor, "wb") as destination, patch.open("rb") as source:
                while block := source.read(1024 * 1024):
                    digest.update(block)
                    size_bytes += len(block)
                    destination.write(block)
                destination.flush()
                os.fsync(destination.fileno())
            if target_relative in plan.create_new_paths:
                try:
                    os.link(temporary, target)
                except FileExistsError as exc:
                    raise UnsafeMutationSet(
                        f"create-new target appeared before patch adoption: {target_relative}"
                    ) from exc
            else:
                os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        adopted.append(
            {
                "relative_path": target_relative,
                "patch_relative_path": patch_relative,
                "sha256": digest.hexdigest(),
                "size_bytes": size_bytes,
            }
        )
    _assert_source_metadata_unchanged(plan)
    return {
        "schema_version": "dataset_release_isolated_patch_adoption_v1",
        "source_merkle_before": plan.source_merkle_before,
        "adopted": adopted,
        "untrusted_writer_baseline_visibility": 0,
        "path_authority": "exact_declared_mutation_targets_v1",
    }


def adopt_deferred_writer_outputs(
    plan: CopyOnWritePlan,
    private_root: Path,
    *,
    patch_targets: Mapping[str | Path, str | Path],
    baseline_copy_count: int = 1,
) -> dict[str, Any]:
    """Atomically move quiescent private outputs into omitted COW targets.

    A deferred target is never materialized in the final staging tree during
    prepare, so the private writer baseline is the only baseline byte copy.
    The trusted parent hashes each declared output once and then performs a
    same-volume atomic rename; it never copies the bytes a second time.
    """

    private_root = _require_plain_root(private_root, must_exist=True)
    if baseline_copy_count not in {0, 1}:
        raise CopyOnWriteError("deferred baseline copy count is invalid")
    normalized = {
        normalize_relative_path(target): normalize_relative_path(patch) for target, patch in patch_targets.items()
    }
    if set(normalized) != set(plan.deferred_existing_paths):
        raise UnsafeMutationSet("deferred patch target set differs from prepared authority")
    _assert_source_metadata_unchanged(plan)
    assert_writer_targets_private(plan, normalized)
    private_actual = _casefold_file_map(private_root)
    adopted: list[dict[str, Any]] = []
    for target_relative in sorted(normalized):
        patch_relative = normalized[target_relative]
        physical = private_actual.get(patch_relative)
        if physical is None:
            raise UnsafeMutationSet(f"deferred private output is missing: {patch_relative}")
        patch = private_root / Path(physical)
        _assert_plain_node(patch)
        item = plan.file(target_relative)
        target = plan.target_root / Path(item.source.relative_path)
        if target.exists():
            raise UnsafeMutationSet(f"deferred final target appeared before adoption: {target_relative}")
        target.parent.mkdir(parents=True, exist_ok=True)
        _assert_plain_existing_chain(target.parent, plan.target_root)
        digest = sha256_file(patch)
        size_bytes = int(patch.stat().st_size)
        try:
            os.replace(patch, target)
        except OSError as exc:
            raise CopyOnWriteError("deferred output adoption requires a same-volume atomic rename") from exc
        target_identity = _file_metadata(target, plan.target_root, known_sha256=digest)
        source_identity = _file_metadata(
            plan.source_root / Path(item.source.relative_path),
            plan.source_root,
            known_sha256=item.source.sha256,
        )
        if _same_file_identity(source_identity, target_identity):
            raise UnsafeMutationSet(f"deferred adopted target aliases baseline: {target_relative}")
        adopted.append(
            {
                "relative_path": target_relative,
                "private_relative_path": patch_relative,
                "sha256": digest,
                "size_bytes": size_bytes,
                "adoption": "same_volume_atomic_rename",
            }
        )
    _assert_source_metadata_unchanged(plan)
    return {
        "schema_version": "dataset_release_deferred_writer_adoption_v1",
        "source_merkle_before": plan.source_merkle_before,
        "adopted": adopted,
        "baseline_copy_count": baseline_copy_count,
        "final_recopy_count": 0,
        "untrusted_writer_final_staging_visibility": 0,
    }


def writer_target_manifest(plan: CopyOnWritePlan) -> dict[str, Any]:
    """Prove the final target tree differs only at declared writer targets.

    Source bytes are read exactly once in this post-writer audit.  Reused
    hardlinks inherit those verified hashes by same-file identity; only private
    mutation targets are hashed from the target tree.
    """

    source_files, source_merkle_after = tree_merkle(plan.source_root)
    if source_merkle_after != plan.source_merkle_before:
        raise SourceMerkleChanged(
            f"source candidate Merkle changed: before={plan.source_merkle_before} after={source_merkle_after}"
        )
    source_by_path = {normalize_relative_path(item.relative_path): item for item in source_files}
    expected_paths = set(source_by_path).union(plan.create_new_paths)
    target_actual_by_path = _casefold_file_map(plan.target_root)
    actual_paths = set(target_actual_by_path)
    missing = sorted(expected_paths.difference(actual_paths))
    extra = sorted(actual_paths.difference(expected_paths))
    if missing or extra:
        raise UnsafeMutationSet(
            f"target tree path set differs from baseline plus declared creates: missing={missing} extra={extra}"
        )

    files: list[dict[str, Any]] = []
    target_leaves: list[dict[str, Any]] = []
    for relative in sorted(actual_paths):
        target = plan.target_root / Path(target_actual_by_path[relative])
        _assert_plain_node(target)
        if relative not in plan.mutation_paths:
            source = source_by_path[relative]
            target_identity = _file_metadata(target, plan.target_root, known_sha256=source.sha256)
            if not _same_file_identity(source, target_identity):
                raise UnsafeMutationSet(f"non-target path is no longer the baseline hardlink: {relative}")
            digest = source.sha256
            size_bytes = source.size_bytes
        else:
            source = source_by_path.get(relative)
            source_path = (
                plan.source_root / Path(source.relative_path)
                if source is not None
                else plan.source_root / Path(relative)
            )
            if relative in plan.create_new_paths and source_path.exists():
                raise UnsafeMutationSet(f"create-new target appeared in source: {relative}")
            digest = sha256_file(target)
            size_bytes = target.stat().st_size
            target_identity = _file_metadata(target, plan.target_root, known_sha256=digest)
            if relative in plan.replace_existing_paths:
                assert source is not None
                if _same_file_identity(source, target_identity):
                    raise UnsafeMutationSet(f"replace target aliases source after write: {relative}")
            files.append(
                {
                    "relative_path": relative,
                    "mode": ("create_new" if relative in plan.create_new_paths else "replace_existing"),
                    "sha256": digest,
                    "size_bytes": size_bytes,
                }
            )
        target_leaves.append(
            {
                "relative_path": relative,
                "size_bytes": size_bytes,
                "sha256": digest,
            }
        )
    target_merkle = _merkle_from_leaves(target_leaves)
    return {
        "schema_version": "dataset_release_writer_target_manifest_v1",
        "replace_existing_targets": sorted(plan.replace_existing_paths),
        "create_new_targets": sorted(plan.create_new_paths),
        "files": files,
        "source_merkle_after": source_merkle_after,
        "target_merkle": target_merkle,
        "target_path_count": len(actual_paths),
        "path_set_rule": "baseline_union_declared_create_no_delete_v1",
    }


def verify_source_unchanged(plan: CopyOnWritePlan) -> str:
    _files, current = tree_merkle(plan.source_root)
    if current != plan.source_merkle_before:
        raise SourceMerkleChanged(
            f"source candidate Merkle changed: before={plan.source_merkle_before} after={current}"
        )
    return current


def tree_merkle(root: Path) -> tuple[tuple[TreeFile, ...], str]:
    root = _require_plain_root(root, must_exist=True)
    files: list[TreeFile] = []
    for path in sorted(root.rglob("*"), key=lambda value: value.relative_to(root).as_posix()):
        _assert_plain_node(path)
        if path.is_dir():
            continue
        if not path.is_file():
            raise CopyOnWriteError(f"source tree has a non-regular file: {path}")
        files.append(_file_identity(path, root))
    if not files:
        raise CopyOnWriteError("source tree contains no files")
    leaves = [
        {
            "relative_path": item.relative_path,
            "size_bytes": item.size_bytes,
            "sha256": item.sha256,
        }
        for item in files
    ]
    return tuple(files), _merkle_from_leaves(leaves)


def normalize_relative_path(value: str | Path) -> str:
    raw = str(value).replace("\\", "/").strip()
    path = PurePosixPath(raw)
    if (
        not raw
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(character in raw for character in ("*", "?", "[", "]", ":"))
    ):
        raise UnsafeMutationSet(f"unsafe mutation path: {value!r}")
    return normalize_root_relative_path(path.as_posix())


def sha256_file(path: Path, *, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while block := handle.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def _file_identity(path: Path, root: Path) -> TreeFile:
    _assert_plain_node(path)
    metadata = os.stat(path, follow_symlinks=False)
    return TreeFile(
        relative_path=path.relative_to(root).as_posix(),
        size_bytes=int(metadata.st_size),
        sha256=sha256_file(path),
        device=int(metadata.st_dev),
        inode=int(metadata.st_ino),
        link_count=int(metadata.st_nlink),
        mtime_ns=int(metadata.st_mtime_ns),
    )


def _file_metadata(path: Path, root: Path, *, known_sha256: str) -> TreeFile:
    """Return identity metadata without rereading file content."""

    _assert_plain_node(path)
    metadata = os.stat(path, follow_symlinks=False)
    return TreeFile(
        relative_path=path.relative_to(root).as_posix(),
        size_bytes=int(metadata.st_size),
        sha256=known_sha256,
        device=int(metadata.st_dev),
        inode=int(metadata.st_ino),
        link_count=int(metadata.st_nlink),
        mtime_ns=int(metadata.st_mtime_ns),
    )


def _assert_stable_source_metadata(before: TreeFile, after: TreeFile) -> None:
    if (
        before.relative_path != after.relative_path
        or before.size_bytes != after.size_bytes
        or before.device != after.device
        or before.inode != after.inode
        or before.mtime_ns != after.mtime_ns
    ):
        raise SourceMerkleChanged(f"source metadata changed during clone: {before.relative_path}")


def _assert_source_metadata_unchanged(plan: CopyOnWritePlan) -> None:
    for before in plan.source_files:
        after = _file_metadata(
            plan.source_root / Path(before.relative_path),
            plan.source_root,
            known_sha256=before.sha256,
        )
        _assert_stable_source_metadata(before, after)


def _tree_relative_file_paths(root: Path) -> tuple[str, ...]:
    root = _require_plain_root(root, must_exist=True)
    files: list[str] = []
    for path in sorted(root.rglob("*"), key=lambda value: value.relative_to(root).as_posix()):
        _assert_plain_node(path)
        if path.is_dir():
            continue
        if not path.is_file():
            raise UnsafeMutationSet(f"target tree has a non-regular file: {path}")
        files.append(path.relative_to(root).as_posix())
    return tuple(files)


def _casefold_file_map(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for physical in _tree_relative_file_paths(root):
        canonical = normalize_relative_path(physical)
        if canonical in result:
            raise UnsafeMutationSet("tree contains case-insensitive path collisions")
        result[canonical] = physical
    return result


def _merkle_from_leaves(leaves: list[dict[str, Any]]) -> str:
    canonical_leaves = [
        {
            **leaf,
            "relative_path": normalize_root_relative_path(str(leaf["relative_path"])),
        }
        for leaf in leaves
    ]
    canonical_paths = [str(leaf["relative_path"]) for leaf in canonical_leaves]
    if len(canonical_paths) != len(set(canonical_paths)):
        raise CopyOnWriteError("source tree contains case-insensitive path collisions")
    canonical_leaves.sort(key=lambda leaf: str(leaf["relative_path"]))
    payload = (
        json.dumps(
            canonical_leaves,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _same_file_identity(left: TreeFile, right: TreeFile) -> bool:
    return left.device == right.device and left.inode == right.inode


def _require_plain_root(path: Path, *, must_exist: bool) -> Path:
    expanded = Path(path).expanduser()
    if not expanded.is_absolute():
        expanded = expanded.absolute()
    resolved = expanded.resolve(strict=must_exist)
    if must_exist and not resolved.is_dir():
        raise CopyOnWriteError(f"source root is not a directory: {resolved}")
    _assert_existing_chain(resolved)
    return resolved


def _assert_existing_chain(path: Path) -> None:
    current = Path(path.anchor)
    if current.exists():
        _assert_plain_node(current)
    for part in path.parts[1:]:
        current = current / part
        if current.exists():
            _assert_plain_node(current)


def _assert_plain_existing_chain(path: Path, root: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise CopyOnWriteError("target path escapes target root") from exc
    current = root
    _assert_plain_node(current)
    for part in relative.parts:
        current = current / part
        if current.exists():
            _assert_plain_node(current)


def _assert_plain_node(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise CopyOnWriteError(f"path component is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or (int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise CopyOnWriteError(f"path traverses a symlink or reparse point: {path}")
