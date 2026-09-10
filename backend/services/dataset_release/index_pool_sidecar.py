"""Render small Qlib instruments files from the shared index PIT resolver.

This module writes only ``symbol/start/end`` selection files.  It does not
inspect, hash, validate, or rewrite price/factor dataset components.
"""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

from backend.services.core_index_membership import ResolvedUniverse, UniverseMode


_SAFE_NAME_RE = re.compile(r"^(?:index_pool__[a-z0-9_]+|filtered_pool__index_union__[a-z0-9_]+)\.txt$")


class IndexPoolSidecarError(RuntimeError):
    """Raised when a sidecar target or rendered payload is invalid."""


@dataclass(frozen=True, slots=True)
class SidecarResult:
    path: Path
    filename: str
    instrument_name: str
    interval_count: int
    symbol_count: int
    membership_revision: str


def sidecar_filename(resolved: ResolvedUniverse) -> str:
    if resolved.mode is UniverseMode.SINGLE_INDEX:
        if len(resolved.pool_ids) != 1:
            raise IndexPoolSidecarError("single_index result must contain one pool_id")
        return f"index_pool__{resolved.pool_ids[0]}.txt"
    if resolved.mode is UniverseMode.INDEX_UNION:
        if not resolved.pool_ids:
            raise IndexPoolSidecarError("index_union result must contain pool_ids")
        return f"filtered_pool__index_union__{'_'.join(resolved.pool_ids)}.txt"
    raise IndexPoolSidecarError("stock_universe uses the existing stock_universe.txt and needs no sidecar")


def render_sidecar_content(resolved: ResolvedUniverse) -> str:
    if not resolved.intervals:
        raise IndexPoolSidecarError("resolved universe contains no intervals")
    lines: list[str] = []
    prior: tuple[str, date] | None = None
    for row in resolved.intervals:
        key = (row.ts_code, row.eligible_start)
        if prior is not None and key <= prior:
            raise IndexPoolSidecarError("resolved intervals must be unique and sorted")
        if row.eligible_end < row.eligible_start:
            raise IndexPoolSidecarError("resolved interval ends before it starts")
        lines.append(f"{row.ts_code}\t{row.eligible_start.isoformat()}\t{row.eligible_end.isoformat()}")
        prior = (row.ts_code, row.eligible_start)
    return "\n".join(lines) + "\n"


def write_sidecar(
    output_dir: Path,
    resolved: ResolvedUniverse,
    *,
    replace: bool = False,
) -> SidecarResult:
    """Atomically write one selection sidecar and read back its exact text."""

    root = output_dir.resolve(strict=False)
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise IndexPoolSidecarError("sidecar output root must be a real directory")
    root.mkdir(parents=True, exist_ok=True)
    filename = sidecar_filename(resolved)
    if not _SAFE_NAME_RE.fullmatch(filename):
        raise IndexPoolSidecarError(f"unsafe sidecar filename: {filename}")
    target = root / filename
    if target.exists():
        if target.is_symlink() or not target.is_file():
            raise IndexPoolSidecarError("existing sidecar target is not a regular file")
        if not replace:
            raise FileExistsError(target)
    payload = render_sidecar_content(resolved).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{filename}.", suffix=".partial", dir=root)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if target.exists() and not replace:
            raise FileExistsError(target)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    if target.read_bytes() != payload:
        raise IndexPoolSidecarError("sidecar readback differs from rendered payload")
    return SidecarResult(
        path=target,
        filename=filename,
        instrument_name=filename.removesuffix(".txt"),
        interval_count=len(resolved.intervals),
        symbol_count=len({row.ts_code for row in resolved.intervals}),
        membership_revision=resolved.membership_revision,
    )


def write_sidecars(
    output_dir: Path,
    universes: Iterable[ResolvedUniverse],
    *,
    replace: bool = False,
) -> tuple[SidecarResult, ...]:
    return tuple(write_sidecar(output_dir, resolved, replace=replace) for resolved in universes)
