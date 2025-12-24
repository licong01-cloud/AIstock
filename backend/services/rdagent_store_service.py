from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class CopiedFileInfo:
    path_rel: str
    size_bytes: int
    sha256: Optional[str]


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_strategy_store_root() -> str:
    root = (os.getenv("AISTOCK_STRATEGY_STORE_ROOT") or "").strip().strip('"')
    if root:
        return root
    # fallback: keep everything inside repo if not configured
    candidate = Path(__file__).resolve().parents[3] / "strategy_store"
    return str(candidate)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path, compute_hash: bool = True) -> CopiedFileInfo:
    ensure_dir(dst.parent)
    shutil.copy2(src, dst)
    size_bytes = dst.stat().st_size
    sha256 = _sha256_of_file(dst) if compute_hash else None
    return CopiedFileInfo(path_rel=str(dst), size_bytes=size_bytes, sha256=sha256)


def copy_tree(src_dir: Path, dst_dir: Path) -> None:
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
    shutil.copytree(src_dir, dst_dir)
