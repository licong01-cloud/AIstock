"""Reuse real byte digests across monthly stages, never infer data readiness.

The bounded process-local cache holds only hashes computed from bytes or
supplied by the trusted writer after checking its unchanged output signature.
It is not a persisted baseline, a source audit, or a release admission gate.
"""
from __future__ import annotations

from collections import OrderedDict
import hashlib
from pathlib import Path
import re
import stat
from threading import RLock

_MAX_ENTRIES = 262_144
_CACHE: OrderedDict[tuple, str] = OrderedDict()
_LOCK = RLock()


def _signature(path: Path) -> tuple[int, int, int, int, int]:
    value = path.lstat()
    if (not stat.S_ISREG(value.st_mode)
        or int(getattr(value, "st_file_attributes", 0)) & 0x0400):
        raise ValueError("monthly digest requires a regular non-linked file")
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _key(path: Path, signature: tuple[int, int, int, int, int]) -> tuple:
    return signature if signature[1] else (str(path.absolute()), *signature)


def _remember(key: tuple, digest: str) -> None:
    with _LOCK:
        if key in _CACHE and _CACHE[key] != digest:
            raise ValueError("monthly verified file has conflicting digests")
        _CACHE[key] = digest
        _CACHE.move_to_end(key)
        if len(_CACHE) > _MAX_ENTRIES:
            _CACHE.popitem(last=False)


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_sha256(path: Path) -> str:
    before = _signature(path)
    key = _key(path, before)
    with _LOCK:
        cached = _CACHE.get(key)
        if cached is not None:
            _CACHE.move_to_end(key)
    digest = cached if cached is not None else _stream_sha256(path)
    if _signature(path) != before:
        raise ValueError("monthly file changed during digest readback")
    if cached is None:
        _remember(key, digest)
    return digest


def remember_verified_file(
    path: Path, digest: str, *, expected_signature: tuple[int, int, int, int],
) -> None:
    """Accept a code-owned writer's hash, not an arbitrary manifest claim."""
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("monthly writer digest is invalid")
    signature = _signature(path)
    if signature[:4] != tuple(expected_signature):
        raise ValueError("monthly writer output signature differs")
    _remember(_key(path, signature), digest)
