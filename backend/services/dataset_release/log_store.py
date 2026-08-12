from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


class LogStoreError(RuntimeError):
    """Bounded log store path, integrity or lifecycle error."""


class LogCapacityExceeded(LogStoreError):
    code = "CONTROL_ROOT_CAPACITY_EXCEEDED"


# Shared across stdout/stderr for one supervised child: at the default
# 16-MiB segment size this is a hard 2-GiB ceiling, not an accidental 64-GiB
# control-volume sink.
MAX_LOG_SEGMENTS = 128
MAX_LOG_TOTAL_BYTES = 2 * 1024**3


class LogSegmentBudget:
    """One shared, thread-safe segment and byte cap for all runner streams."""

    def __init__(
        self,
        max_segments: int = MAX_LOG_SEGMENTS,
        *,
        max_total_bytes: int = MAX_LOG_TOTAL_BYTES,
    ) -> None:
        if type(max_segments) is not int or not 1 <= max_segments <= MAX_LOG_SEGMENTS:
            raise LogStoreError("log segment count exceeds the hard contract")
        if type(max_total_bytes) is not int or not 1 <= max_total_bytes <= MAX_LOG_TOTAL_BYTES:
            raise LogStoreError("log total bytes exceed the hard contract")
        self.max_segments = max_segments
        self.max_total_bytes = max_total_bytes
        self._count = 0
        self._bytes = 0
        self._lock = threading.Lock()

    def claim(self) -> None:
        """Reserve one segment without bytes for compatibility with old callers."""

        self.reserve(0, new_segment=True)

    def reserve(self, byte_count: int, *, new_segment: bool) -> None:
        if type(byte_count) is not int or byte_count < 0:
            raise LogStoreError("log byte reservation is invalid")
        if not isinstance(new_segment, bool):
            raise LogStoreError("log segment reservation is invalid")
        with self._lock:
            if new_segment and self._count >= self.max_segments:
                raise LogCapacityExceeded(f"log segment capacity reached: {self.max_segments}")
            if self._bytes + byte_count > self.max_total_bytes:
                raise LogCapacityExceeded(f"log byte capacity reached: {self.max_total_bytes}")
            if new_segment:
                self._count += 1
            self._bytes += byte_count

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    @property
    def bytes_written(self) -> int:
        with self._lock:
            return self._bytes


@dataclass(frozen=True)
class LogSegment:
    stream: str
    generation: int
    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class LogPage:
    data: bytes
    generation: int
    byte_offset: int
    next_generation: int | None
    next_byte_offset: int | None
    has_more: bool


def _is_reparse_or_symlink(path: Path) -> bool:
    stat = path.lstat()
    if path.is_symlink():
        return True
    return bool(getattr(stat, "st_file_attributes", 0) & 0x400)


def _guard_directory(root: Path) -> Path:
    resolved = root.resolve(strict=True)
    current = resolved
    while True:
        if _is_reparse_or_symlink(current):
            raise LogStoreError(f"log path uses a reparse point: {current}")
        if current.parent == current:
            break
        current = current.parent
    return resolved


def _atomic_json(path: Path, payload: object) -> None:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    temp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    handle = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(handle, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.close(handle)
        handle = -1
        os.replace(temp, path)
    finally:
        if handle != -1:
            os.close(handle)
        if temp.exists():
            temp.unlink(missing_ok=True)


class RotatingLogWriter:
    def __init__(
        self,
        root: Path,
        stream: str,
        *,
        segment_limit_bytes: int = 16 * 1024**2,
        segment_budget: LogSegmentBudget | None = None,
    ) -> None:
        if stream not in {"stdout", "stderr", "worker"}:
            raise LogStoreError("log stream is not allowlisted")
        if not 1024 <= segment_limit_bytes <= 16 * 1024**2:
            raise LogStoreError("log segment limit must be between 1 KiB and 16 MiB")
        root.mkdir(parents=True, exist_ok=True)
        self.root = _guard_directory(root)
        self.stream = stream
        self.segment_limit_bytes = int(segment_limit_bytes)
        self.segment_budget = segment_budget or LogSegmentBudget()
        self._generation = 0
        self._handle = None
        self._partial: Path | None = None
        self._size = 0
        self._digest = hashlib.sha256()
        self._segments: list[LogSegment] = []
        self._lock = threading.Lock()
        self._closed = False

    @property
    def segments(self) -> tuple[LogSegment, ...]:
        return tuple(self._segments)

    def _open(self) -> None:
        self._generation += 1
        self._partial = self.root / f"{self.stream}.{self._generation:06d}.partial"
        final = self.root / f"{self.stream}.{self._generation:06d}.log"
        if self._partial.exists() or final.exists():
            raise LogStoreError("log generation already exists")
        self._handle = self._partial.open("xb", buffering=0)
        self._size = 0
        self._digest = hashlib.sha256()

    def _finalize(self) -> None:
        if self._handle is None or self._partial is None:
            return
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()
        final = self.root / f"{self.stream}.{self._generation:06d}.log"
        os.replace(self._partial, final)
        segment = LogSegment(
            stream=self.stream,
            generation=self._generation,
            path=final.name,
            size_bytes=self._size,
            sha256=self._digest.hexdigest(),
        )
        if hashlib.sha256(final.read_bytes()).hexdigest() != segment.sha256:
            raise LogStoreError("log segment readback mismatch")
        self._segments.append(segment)
        _atomic_json(
            self.root / f"{self.stream}.index.json",
            {"schema_version": "dataset_log_index_v1", "segments": [asdict(item) for item in self._segments]},
        )
        self._handle = None
        self._partial = None

    def write(self, data: bytes) -> None:
        if not isinstance(data, bytes):
            raise TypeError("log writer accepts bytes")
        with self._lock:
            if self._closed:
                raise LogStoreError("log writer is closed")
            view = memoryview(data)
            while view:
                new_segment = self._handle is None
                remaining = self.segment_limit_bytes if new_segment else self.segment_limit_bytes - self._size
                chunk = bytes(view[:remaining])
                self.segment_budget.reserve(len(chunk), new_segment=new_segment)
                if new_segment:
                    self._open()
                assert self._handle is not None
                self._handle.write(chunk)
                self._digest.update(chunk)
                self._size += len(chunk)
                view = view[len(chunk) :]
                if self._size == self.segment_limit_bytes:
                    self._finalize()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._finalize()
            self._closed = True

    def __enter__(self) -> "RotatingLogWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def bounded_tail(
    root: Path,
    stream: str,
    *,
    max_bytes: int = 1024**2,
    max_lines: int = 1000,
) -> bytes:
    if stream not in {"stdout", "stderr", "worker"}:
        raise LogStoreError("log stream is not allowlisted")
    if not 1 <= max_bytes <= 1024**2 or not 1 <= max_lines <= 1000:
        raise LogStoreError("tail bounds exceed API contract")
    guarded = _guard_directory(root)
    paths = sorted(guarded.glob(f"{stream}.*.log"), reverse=True)
    chunks: list[bytes] = []
    remaining = max_bytes
    for path in paths:
        if _is_reparse_or_symlink(path):
            raise LogStoreError("log segment is a reparse point")
        size = min(path.stat().st_size, remaining)
        if size <= 0:
            continue
        with path.open("rb") as stream_handle:
            stream_handle.seek(-size, os.SEEK_END)
            chunks.append(stream_handle.read(size))
        remaining -= size
        if remaining <= 0:
            break
    data = b"".join(reversed(chunks))[-max_bytes:]
    lines = data.splitlines(keepends=True)
    return b"".join(lines[-max_lines:])


def read_log_page(
    root: Path,
    stream: str,
    *,
    generation: int = 1,
    byte_offset: int = 0,
    max_bytes: int = 256 * 1024,
    max_lines: int = 1000,
) -> LogPage:
    """Read one forward-only page using a bounded byte and line allocation."""

    if stream not in {"stdout", "stderr", "worker"}:
        raise LogStoreError("log stream is not allowlisted")
    if generation <= 0 or byte_offset < 0:
        raise LogStoreError("log cursor position is invalid")
    if not 1 <= max_bytes <= 1024**2 or not 1 <= max_lines <= 1000:
        raise LogStoreError("log page bounds exceed API contract")
    if not root.exists():
        return LogPage(b"", generation, byte_offset, None, None, False)
    guarded = _guard_directory(root)
    current_generation = generation
    current_offset = byte_offset
    path = guarded / f"{stream}.{current_generation:06d}.log"
    if not path.exists():
        if current_generation == 1 and current_offset == 0:
            return LogPage(b"", current_generation, current_offset, None, None, False)
        raise LogStoreError("log cursor generation does not exist")
    if _is_reparse_or_symlink(path):
        raise LogStoreError("log segment is a reparse point")
    size = path.stat().st_size
    if current_offset > size:
        raise LogStoreError("log cursor byte offset exceeds segment size")
    if current_offset == size:
        following = guarded / f"{stream}.{current_generation + 1:06d}.log"
        if not following.exists():
            return LogPage(b"", current_generation, current_offset, None, None, False)
        if _is_reparse_or_symlink(following):
            raise LogStoreError("log segment is a reparse point")
        current_generation += 1
        current_offset = 0
        path = following
        size = path.stat().st_size

    with path.open("rb") as handle:
        handle.seek(current_offset)
        raw = handle.read(max_bytes + 1)
    byte_limited = len(raw) > max_bytes
    bounded = raw[:max_bytes]
    lines = bounded.splitlines(keepends=True)
    line_limited = len(lines) > max_lines
    data = b"".join(lines[:max_lines]) if line_limited else bounded
    next_offset = current_offset + len(data)
    next_generation: int | None = None
    next_byte_offset: int | None = None
    has_more = byte_limited or line_limited or next_offset < size
    if has_more:
        next_generation = current_generation
        next_byte_offset = next_offset
    else:
        following = guarded / f"{stream}.{current_generation + 1:06d}.log"
        if following.exists():
            if _is_reparse_or_symlink(following):
                raise LogStoreError("log segment is a reparse point")
            has_more = True
            next_generation = current_generation + 1
            next_byte_offset = 0
    return LogPage(
        data=data,
        generation=current_generation,
        byte_offset=current_offset,
        next_generation=next_generation,
        next_byte_offset=next_byte_offset,
        has_more=has_more,
    )


def manifest_segments(writers: Iterable[RotatingLogWriter]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for writer in writers:
        result.extend(asdict(segment) for segment in writer.segments)
    return sorted(result, key=lambda item: (str(item["stream"]), int(item["generation"])))


__all__ = [
    "LogSegment",
    "LogCapacityExceeded",
    "LogPage",
    "LogSegmentBudget",
    "LogStoreError",
    "MAX_LOG_SEGMENTS",
    "MAX_LOG_TOTAL_BYTES",
    "RotatingLogWriter",
    "bounded_tail",
    "read_log_page",
    "manifest_segments",
]
