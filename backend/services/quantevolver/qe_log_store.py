"""Bounded process-safe mirror for live QE log events.

The mirror is operational convenience, not experiment evidence.  Canonical
RD-Agent run.log files remain authoritative.  New writes are constrained to
five fixed JSONL slots of at most 1 GiB each.
"""

from __future__ import annotations

import json
import os
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

QE_LIVE_LOG_FILE_COUNT = 5
QE_LIVE_LOG_MAX_FILE_BYTES = 1024 * 1024 * 1024
QE_LIVE_LOG_MAX_READ_BYTES = 16 * 1024 * 1024
_FILE_PREFIX = "qe-live-"
_FILE_SUFFIX = ".jsonl"
QE_LIVE_LOG_STATE_ROOT_MISSING = "qe_live_log_state_root_missing"
QE_LIVE_LOG_STATE_ROOT_INVALID = "qe_live_log_state_root_invalid"


class QELiveLogConfigurationError(RuntimeError):
    """Loud configuration failure for the repo-external live-log mirror."""

    def __init__(self, reason_code: str, message: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {message}")


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_external_root(raw: str, *, field_name: str) -> Path:
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        raise QELiveLogConfigurationError(
            QE_LIVE_LOG_STATE_ROOT_INVALID,
            f"{field_name} must be an absolute path",
        )
    resolved = candidate.resolve(strict=False)
    repository_root = _repository_root()
    if resolved == repository_root or resolved.is_relative_to(repository_root):
        raise QELiveLogConfigurationError(
            QE_LIVE_LOG_STATE_ROOT_INVALID,
            f"{field_name} must resolve outside the AIstock repository",
        )
    for ancestor in (resolved, *resolved.parents):
        if (ancestor / ".git").exists():
            raise QELiveLogConfigurationError(
                QE_LIVE_LOG_STATE_ROOT_INVALID,
                f"{field_name} must not resolve inside a Git checkout",
            )
    return resolved


def default_qe_live_log_root() -> Path:
    explicit = str(os.getenv("QE_LIVE_LOG_DIR") or "").strip()
    if explicit:
        return _resolve_external_root(explicit, field_name="QE_LIVE_LOG_DIR")
    state_root = str(os.getenv("RDAGENT_STATE_ROOT") or "").strip()
    if not state_root:
        raise QELiveLogConfigurationError(
            QE_LIVE_LOG_STATE_ROOT_MISSING,
            "QE_LIVE_LOG_DIR or RDAGENT_STATE_ROOT is required; repository fallback is forbidden",
        )
    return _resolve_external_root(state_root, field_name="RDAGENT_STATE_ROOT") / "qe_live_logs"


class QELiveLogStore:
    """Thread-safe five-slot ring with bounded reverse tail reads."""

    def __init__(
        self,
        root: Path | str | None = None,
        *,
        max_file_bytes: int = QE_LIVE_LOG_MAX_FILE_BYTES,
        file_count: int = QE_LIVE_LOG_FILE_COUNT,
    ) -> None:
        if file_count != QE_LIVE_LOG_FILE_COUNT:
            raise ValueError("QE live log ring must contain exactly five slots")
        if max_file_bytes <= 0:
            raise ValueError("QE live log max_file_bytes must be positive")
        self.root = Path(root).resolve() if root is not None else default_qe_live_log_root()
        self.max_file_bytes = int(max_file_bytes)
        self.file_count = int(file_count)
        self._lock = threading.RLock()
        self._active_slot: int | None = None

    def slot_paths(self) -> tuple[Path, ...]:
        return tuple(
            self.root / f"{_FILE_PREFIX}{index}{_FILE_SUFFIX}"
            for index in range(self.file_count)
        )

    def _ensure_slots(self) -> tuple[Path, ...]:
        self.root.mkdir(parents=True, exist_ok=True)
        paths = self.slot_paths()
        for path in paths:
            if not path.exists():
                with path.open("xb"):
                    pass
        return paths

    @staticmethod
    def _newest_slot(paths: Iterable[Path]) -> int:
        ranked = [(path.stat().st_mtime_ns, index, path.stat().st_size) for index, path in enumerate(paths)]
        if ranked and all(size == 0 for _, _, size in ranked):
            return 0
        return max(ranked)[1] if ranked else 0

    def append(self, record: dict[str, Any]) -> Path:
        payload = dict(record)
        payload.setdefault("schema_version", "qe_live_log_record_v1")
        payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        raw = (json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n").encode("utf-8")
        if len(raw) > self.max_file_bytes:
            raise ValueError("QE live log record exceeds the one-file retention bound")

        with self._lock:
            paths = self._ensure_slots()
            if self._active_slot is None:
                self._active_slot = self._newest_slot(paths)
            target = paths[self._active_slot]
            if target.stat().st_size + len(raw) > self.max_file_bytes:
                self._active_slot = (self._active_slot + 1) % self.file_count
                target = paths[self._active_slot]
                with target.open("wb"):
                    pass
            with target.open("ab") as handle:
                handle.write(raw)
                handle.flush()
            return target

    def read_task_tail(
        self,
        task_id: str,
        *,
        tail: int,
        max_scan_bytes: int = QE_LIVE_LOG_MAX_READ_BYTES,
    ) -> dict[str, Any]:
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id:
            raise ValueError("QE live log tail requires task_id")
        if tail <= 0 or max_scan_bytes <= 0:
            raise ValueError("QE live log tail bounds must be positive")

        with self._lock:
            paths = [path for path in self.slot_paths() if path.is_file()]
            paths.sort(key=lambda path: path.stat().st_mtime_ns, reverse=True)
            matched: deque[str] = deque(maxlen=int(tail))
            scanned = 0
            for path in paths:
                if scanned >= max_scan_bytes or len(matched) >= tail:
                    break
                remaining = max_scan_bytes - scanned
                size = path.stat().st_size
                read_size = min(size, remaining)
                if read_size <= 0:
                    continue
                with path.open("rb") as handle:
                    handle.seek(size - read_size)
                    raw = handle.read(read_size)
                scanned += len(raw)
                lines = raw.splitlines()
                if size > read_size and lines:
                    lines = lines[1:]
                for encoded in reversed(lines):
                    try:
                        item = json.loads(encoded.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if str(item.get("task_id") or "") != normalized_task_id:
                        continue
                    payload = item.get("payload")
                    if isinstance(payload, dict):
                        values = payload.get("logs")
                        if isinstance(values, list):
                            for value in reversed(values):
                                if len(matched) >= tail:
                                    break
                                matched.appendleft(str(value))
                        elif values is not None:
                            matched.appendleft(str(values))
                    if len(matched) >= tail:
                        break
            return {
                "logs": list(matched)[-tail:],
                "source": "qe_live_log_ring",
                "scanned_bytes": scanned,
                "scan_truncated": scanned >= max_scan_bytes,
            }


_PROCESS_STORE: QELiveLogStore | None = None
_PROCESS_STORE_LOCK = threading.Lock()


def get_qe_live_log_store() -> QELiveLogStore:
    global _PROCESS_STORE
    with _PROCESS_STORE_LOCK:
        if _PROCESS_STORE is None:
            _PROCESS_STORE = QELiveLogStore()
        return _PROCESS_STORE
