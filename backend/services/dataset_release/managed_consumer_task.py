"""Create immutable shared-dataset bindings for managed business preparations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Callable, Mapping, Sequence

from .active_task_binding import (
    FrozenDatasetTaskBindingError,
    freeze_active_dataset_task_binding,
    require_frozen_dataset_task_binding,
)
from .canonical import canonical_json_bytes
from .profile_contract import ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS


MANAGED_DATASET_TASK_SCHEMA = "aistock_managed_dataset_task_request_v1"
MANAGED_DATASET_TASK_ROOT_ENV = "AISTOCK_MONTHLY_RELEASE_STATE_ROOT"
MANAGED_DATASET_TASK_DIRECTORY = "managed-consumer-tasks"
MANAGED_DATASET_CONSUMERS = frozenset({"advisory", "position_timing"})
_TASK_KEY = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_FIELDS = {
    "schema_version",
    "task_id",
    "consumer_id",
    "node_id",
    "business_task_key",
    "requested_window",
    "dataset_binding",
    "request_sha256",
    "database_read_performed",
    "database_write_performed",
    "candidate_write_performed",
    "profile_write_performed",
    "outcomes_read",
    "training_started",
    "experiment_started",
    "runtime_action_performed",
}


class ManagedDatasetTaskError(RuntimeError):
    """A managed preparation cannot freeze one exact active dataset."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ManagedDatasetTaskArtifact:
    path: Path
    value: Mapping[str, Any]
    sha256: str
    size: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "size": self.size,
            "request": dict(self.value),
        }


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _require_plain_existing_chain(path: Path) -> None:
    requested = path.absolute()
    current = Path(requested.anchor)
    for part in requested.parts[1:]:
        current /= part
        if not current.exists() or _is_link(current):
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
                "managed dataset task root traverses an unavailable link/reparse path",
            )


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _task_id(*, consumer_id: str, business_task_key: str) -> str:
    digest = _sha256_bytes(
        canonical_json_bytes(
            {
                "schema_version": MANAGED_DATASET_TASK_SCHEMA,
                "consumer_id": consumer_id,
                "business_task_key": business_task_key,
            }
        )
    )
    return f"mdt_{digest[:32]}"


def _window(start: date, end: date) -> dict[str, str]:
    if start > end:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_WINDOW_INVALID",
            "requested start_date must not be after end_date",
        )
    return {"start_date": start.isoformat(), "end_date": end.isoformat()}


def _scope(consumer_id: str, business_task_key: str) -> tuple[str, str]:
    consumer = str(consumer_id or "").strip()
    key = str(business_task_key or "").strip()
    if consumer not in MANAGED_DATASET_CONSUMERS:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_CONSUMER_UNSUPPORTED",
            f"consumer is not a managed offline preparation: {consumer}",
        )
    if _TASK_KEY.fullmatch(key) is None:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_KEY_INVALID",
            "business_task_key must be a stable 1..200 character identifier",
        )
    return consumer, key


def require_managed_dataset_task_request(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _FIELDS:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_CONTRACT_INVALID",
            "managed dataset task fields differ",
        )
    normalized = dict(value)
    consumer, key = _scope(
        str(normalized.get("consumer_id") or ""),
        str(normalized.get("business_task_key") or ""),
    )
    if (
        normalized.get("schema_version") != MANAGED_DATASET_TASK_SCHEMA
        or normalized.get("node_id") != "controller"
        or normalized.get("task_id")
        != _task_id(consumer_id=consumer, business_task_key=key)
        or normalized.get("database_read_performed") is not False
        or normalized.get("database_write_performed") is not False
        or normalized.get("candidate_write_performed") is not False
        or normalized.get("profile_write_performed") is not False
        or normalized.get("outcomes_read") is not False
        or normalized.get("training_started") is not False
        or normalized.get("experiment_started") is not False
        or normalized.get("runtime_action_performed") is not False
    ):
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_CONTRACT_INVALID",
            "managed dataset task scope differs",
        )
    requested_window = normalized.get("requested_window")
    if not isinstance(requested_window, Mapping) or set(requested_window) != {
        "start_date",
        "end_date",
    }:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_CONTRACT_INVALID",
            "managed dataset task window differs",
        )
    try:
        expected_window = _window(
            date.fromisoformat(str(requested_window["start_date"])),
            date.fromisoformat(str(requested_window["end_date"])),
        )
    except ValueError as exc:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_WINDOW_INVALID",
            "managed dataset task window is invalid",
        ) from exc
    if dict(requested_window) != expected_window:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_WINDOW_INVALID",
            "managed dataset task window is not canonical",
        )
    binding = normalized.get("dataset_binding")
    try:
        frozen = require_frozen_dataset_task_binding(
            binding,  # type: ignore[arg-type]
            consumer_id=consumer,
            node_id="controller",
        )
    except (FrozenDatasetTaskBindingError, TypeError) as exc:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_BINDING_INVALID",
            "managed dataset task binding is invalid",
        ) from exc
    if date.fromisoformat(expected_window["end_date"]) > date.fromisoformat(
        str(frozen["cutoff"])
    ):
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_WINDOW_OUTSIDE_BINDING",
            "managed dataset task window exceeds the frozen release cutoff",
        )
    required = sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer])
    if frozen["binding"].get("required_components") != required:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_BINDING_INVALID",
            "managed dataset task component contract differs",
        )
    unsigned = dict(normalized)
    claimed = str(unsigned.pop("request_sha256", ""))
    if _SHA256.fullmatch(claimed) is None or _sha256_bytes(
        canonical_json_bytes(unsigned)
    ) != claimed:
        raise ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_IDENTITY_INVALID",
            "managed dataset task canonical identity differs",
        )
    normalized["requested_window"] = expected_window
    normalized["dataset_binding"] = frozen
    return normalized


class ManagedDatasetTaskStore:
    """File-only store for resolve-once business preparation requests."""

    def __init__(self, root: Path) -> None:
        requested = Path(root)
        if not requested.is_absolute() or _is_link(requested) or not requested.is_dir():
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
                "managed dataset task root must be an existing plain absolute directory",
            )
        _require_plain_existing_chain(requested)
        try:
            self.root = requested.resolve(strict=True)
        except OSError as exc:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
                "managed dataset task root cannot be resolved",
            ) from exc

    @classmethod
    def from_env(cls) -> "ManagedDatasetTaskStore":
        state_root = str(os.getenv(MANAGED_DATASET_TASK_ROOT_ENV) or "").strip()
        if not state_root:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
                f"{MANAGED_DATASET_TASK_ROOT_ENV} is not configured",
            )
        base = Path(state_root)
        if not base.is_absolute() or _is_link(base) or not base.is_dir():
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
                "monthly release state root must be an existing plain absolute directory",
            )
        _require_plain_existing_chain(base)
        root = base.resolve(strict=True) / MANAGED_DATASET_TASK_DIRECTORY
        created = False
        try:
            root.mkdir(mode=0o750)
            created = True
        except FileExistsError:
            pass
        except OSError as exc:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
                "managed dataset task directory cannot be created",
            ) from exc
        try:
            if created:
                _fsync_directory(root.parent)
        except OSError as exc:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
                "managed dataset task directory cannot be persisted",
            ) from exc
        return cls(root)

    def _path(self, *, consumer_id: str, business_task_key: str) -> Path:
        return self.root / (
            _task_id(
                consumer_id=consumer_id,
                business_task_key=business_task_key,
            )
            + ".json"
        )

    def read(self, *, consumer_id: str, business_task_key: str) -> ManagedDatasetTaskArtifact | None:
        consumer, key = _scope(consumer_id, business_task_key)
        path = self._path(consumer_id=consumer, business_task_key=key)
        if not path.exists():
            return None
        if _is_link(path) or not path.is_file() or path.resolve(strict=True).parent != self.root:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ARTIFACT_INVALID",
                "managed dataset task artifact must be a plain file",
            )
        try:
            raw = path.read_bytes()
            decoded = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ARTIFACT_INVALID",
                "managed dataset task artifact is unreadable",
            ) from exc
        value = require_managed_dataset_task_request(decoded)
        if raw != canonical_json_bytes(value) + b"\n":
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ARTIFACT_INVALID",
                "managed dataset task artifact is not canonical",
            )
        return ManagedDatasetTaskArtifact(
            path=path,
            value=value,
            sha256=_sha256_bytes(raw),
            size=len(raw),
        )

    def create(
        self,
        *,
        consumer_id: str,
        business_task_key: str,
        start_date: date,
        end_date: date,
        resolver: Callable[..., Mapping[str, Any]] | None = None,
    ) -> ManagedDatasetTaskArtifact:
        consumer, key = _scope(consumer_id, business_task_key)
        requested_window = _window(start_date, end_date)
        existing = self.read(consumer_id=consumer, business_task_key=key)
        if existing is not None:
            if existing.value["requested_window"] != requested_window:
                raise ManagedDatasetTaskError(
                    "MANAGED_DATASET_TASK_IDEMPOTENCY_CONFLICT",
                    "business_task_key is already bound to another requested window",
                )
            return existing
        try:
            binding = freeze_active_dataset_task_binding(
                consumer_id=consumer,
                node_id="controller",
                resolver=resolver,
            )
        except FrozenDatasetTaskBindingError as exc:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ACTIVE_BINDING_INVALID",
                "active dataset cannot be frozen for the managed task",
            ) from exc
        unsigned: dict[str, Any] = {
            "schema_version": MANAGED_DATASET_TASK_SCHEMA,
            "task_id": _task_id(consumer_id=consumer, business_task_key=key),
            "consumer_id": consumer,
            "node_id": "controller",
            "business_task_key": key,
            "requested_window": requested_window,
            "dataset_binding": binding,
            "database_read_performed": False,
            "database_write_performed": False,
            "candidate_write_performed": False,
            "profile_write_performed": False,
            "outcomes_read": False,
            "training_started": False,
            "experiment_started": False,
            "runtime_action_performed": False,
        }
        value = {
            **unsigned,
            "request_sha256": _sha256_bytes(canonical_json_bytes(unsigned)),
        }
        normalized = require_managed_dataset_task_request(value)
        raw = canonical_json_bytes(normalized) + b"\n"
        path = self._path(consumer_id=consumer, business_task_key=key)
        try:
            with path.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            raced = self.read(consumer_id=consumer, business_task_key=key)
            if raced is None or raced.value["requested_window"] != requested_window:
                raise ManagedDatasetTaskError(
                    "MANAGED_DATASET_TASK_IDEMPOTENCY_CONFLICT",
                    "concurrent managed dataset task differs",
                ) from None
            return raced
        except OSError as exc:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ARTIFACT_WRITE_FAILED",
                "managed dataset task artifact cannot be persisted",
            ) from exc
        try:
            _fsync_directory(self.root)
        except OSError as exc:
            raise ManagedDatasetTaskError(
                "MANAGED_DATASET_TASK_ARTIFACT_WRITE_FAILED",
                "managed dataset task directory cannot be persisted",
            ) from exc
        return ManagedDatasetTaskArtifact(
            path=path,
            value=normalized,
            sha256=_sha256_bytes(raw),
            size=len(raw),
        )


__all__: Sequence[str] = (
    "MANAGED_DATASET_CONSUMERS",
    "MANAGED_DATASET_TASK_ROOT_ENV",
    "MANAGED_DATASET_TASK_SCHEMA",
    "ManagedDatasetTaskArtifact",
    "ManagedDatasetTaskError",
    "ManagedDatasetTaskStore",
    "require_managed_dataset_task_request",
)
