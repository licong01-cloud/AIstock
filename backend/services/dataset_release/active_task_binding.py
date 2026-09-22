"""Freeze one active-profile consumer binding at task creation time."""

from __future__ import annotations

import hashlib
import posixpath
from datetime import date
from typing import Any, Callable, Mapping, Sequence

from backend.services.quantevolver.qe_active_dataset_profile import (
    load_active_qe_profile,
    resolve_active_dataset_consumer_binding,
)

from .canonical import canonical_json_bytes, ensure_sha256
from .errors import CanonicalizationError


FROZEN_DATASET_TASK_BINDING_SCHEMA = "aistock_frozen_dataset_task_binding_v1"
_FIELDS = {
    "schema_version",
    "consumer_id",
    "node_id",
    "profile_sha256",
    "generation",
    "release_id",
    "cutoff",
    "dataset_manifest_sha256",
    "binding",
    "binding_sha256",
}
_DATASET_ENV_KEYS = frozenset(
    {
        "AISTOCK_DATASET_ROOT",
        "AISTOCK_DATASET_MANIFEST_SHA256",
        "AISTOCK_DATASET_PROFILE_SHA256",
        "AISTOCK_DATASET_BINDING_SHA256",
        "AISTOCK_DATASET_GENERATION",
        "AISTOCK_DATASET_RELEASE_ID",
        "AISTOCK_DATASET_CUTOFF",
        "AISTOCK_DATASET_CONSUMER_ID",
        "QE_DATASET_IDENTITY_ROOTS",
        "QE_QLIB_DATA_PATH",
        "QLIB_DAY_DATA",
        "QLIB_DATA_PATH_WSL",
        "QLIB_MINUTE_DATA",
        "QLIB_MINUTE_PATH_WSL",
        "RDAGENT_FACTOR_DATA_WSL",
        "AISTOCK_SECTOR_CONTEXT_DIR",
    }
)


class FrozenDatasetTaskBindingError(ValueError):
    """A task binding is incomplete, mutable or internally inconsistent."""


def _text(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text or "\x00" in text:
        raise FrozenDatasetTaskBindingError(f"{field} is empty or invalid")
    return text


def _binding_digest(binding: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(binding)).hexdigest()


def freeze_active_dataset_task_binding(
    *,
    consumer_id: str,
    node_id: str,
    resolver: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resolve active state exactly once and return a portable immutable envelope."""

    selected_resolver = resolver or resolve_active_dataset_consumer_binding
    binding = dict(selected_resolver(consumer_id=consumer_id, node_id=node_id))
    try:
        profile_sha = ensure_sha256(
            str(binding.get("profile_sha256") or ""),
            field="profile_sha256",
        )
        manifest_sha = ensure_sha256(
            str(binding.get("dataset_manifest_sha256") or ""),
            field="dataset_manifest_sha256",
        )
    except CanonicalizationError as exc:
        raise FrozenDatasetTaskBindingError("active consumer binding SHA256 is invalid") from exc
    if (
        binding.get("schema_version") != "aistock_active_dataset_consumer_binding_v1"
        or binding.get("consumer_id") != consumer_id
        or binding.get("node_id") != node_id
        or binding.get("resolved_once") is not True
        or binding.get("legacy_fallback") is not False
    ):
        raise FrozenDatasetTaskBindingError("active consumer binding contract differs")
    value = {
        "schema_version": FROZEN_DATASET_TASK_BINDING_SCHEMA,
        "consumer_id": consumer_id,
        "node_id": node_id,
        "profile_sha256": profile_sha,
        "generation": _text(binding.get("generation"), field="generation"),
        "release_id": _text(binding.get("release_id"), field="release_id"),
        "cutoff": _text(binding.get("cutoff"), field="cutoff"),
        "dataset_manifest_sha256": manifest_sha,
        "binding": binding,
        "binding_sha256": _binding_digest(binding),
    }
    return require_frozen_dataset_task_binding(
        value,
        consumer_id=consumer_id,
        node_id=node_id,
    )


def freeze_optional_active_dataset_task_binding(
    *,
    consumer_id: str,
    node_id: str,
) -> dict[str, Any] | None:
    """Keep explicit legacy mode only when no active profile exists at all."""

    profile = load_active_qe_profile()
    if profile is None:
        return None
    return freeze_active_dataset_task_binding(
        consumer_id=consumer_id,
        node_id=node_id,
        resolver=lambda **kwargs: resolve_active_dataset_consumer_binding(
            profile=profile,
            **kwargs,
        ),
    )


def require_frozen_dataset_task_binding(
    value: Mapping[str, Any],
    *,
    consumer_id: str,
    node_id: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _FIELDS:
        raise FrozenDatasetTaskBindingError("frozen dataset task binding fields differ")
    normalized = dict(value)
    if (
        normalized.get("schema_version") != FROZEN_DATASET_TASK_BINDING_SCHEMA
        or normalized.get("consumer_id") != consumer_id
        or normalized.get("node_id") != node_id
    ):
        raise FrozenDatasetTaskBindingError("frozen dataset task binding scope differs")
    binding = normalized.get("binding")
    if not isinstance(binding, Mapping):
        raise FrozenDatasetTaskBindingError("frozen dataset task binding payload is invalid")
    nested = dict(binding)
    if (
        nested.get("schema_version") != "aistock_active_dataset_consumer_binding_v1"
        or nested.get("consumer_id") != consumer_id
        or nested.get("node_id") != node_id
        or nested.get("resolved_once") is not True
        or nested.get("legacy_fallback") is not False
    ):
        raise FrozenDatasetTaskBindingError("frozen consumer binding contract differs")
    sha_fields = (
        "profile_sha256",
        "dataset_manifest_sha256",
        "binding_sha256",
    )
    for field in sha_fields:
        try:
            ensure_sha256(str(normalized.get(field) or ""), field=field)
        except (CanonicalizationError, ValueError) as exc:
            raise FrozenDatasetTaskBindingError(f"{field} is invalid") from exc
    for field in ("generation", "release_id", "cutoff"):
        if normalized.get(field) != nested.get(field):
            raise FrozenDatasetTaskBindingError(f"frozen {field} differs")
    try:
        date.fromisoformat(_text(normalized.get("cutoff"), field="cutoff"))
    except ValueError as exc:
        raise FrozenDatasetTaskBindingError("frozen cutoff is invalid") from exc
    if (
        normalized["profile_sha256"] != nested.get("profile_sha256")
        or normalized["dataset_manifest_sha256"]
        != nested.get("dataset_manifest_sha256")
        or normalized["binding_sha256"] != _binding_digest(nested)
    ):
        raise FrozenDatasetTaskBindingError("frozen dataset task identity differs")
    normalized["binding"] = nested
    return normalized


def frozen_dataset_environment(value: Mapping[str, Any]) -> dict[str, str]:
    """Derive remote process paths only from the persisted task binding."""

    consumer_id = _text(value.get("consumer_id"), field="consumer_id")
    node_id = _text(value.get("node_id"), field="node_id")
    frozen = require_frozen_dataset_task_binding(
        value,
        consumer_id=consumer_id,
        node_id=node_id,
    )
    root = str(frozen["binding"].get("candidate_root") or "")
    if (
        not root.startswith("/")
        or root == "/"
        or "\\" in root
        or "\x00" in root
        or posixpath.normpath(root) != root
    ):
        raise FrozenDatasetTaskBindingError("frozen candidate root is not canonical POSIX")
    day = posixpath.join(root, "components/daily_bin_candidate")
    minute = posixpath.join(root, "components/minute_bin_candidate")
    factor = posixpath.join(root, "components/factor_h5_static_candidate_v2")
    return {
        "AISTOCK_DATASET_ROOT": root,
        "AISTOCK_DATASET_MANIFEST_SHA256": str(frozen["dataset_manifest_sha256"]),
        "AISTOCK_DATASET_PROFILE_SHA256": str(frozen["profile_sha256"]),
        "AISTOCK_DATASET_BINDING_SHA256": str(frozen["binding_sha256"]),
        "AISTOCK_DATASET_GENERATION": str(frozen["generation"]),
        "AISTOCK_DATASET_RELEASE_ID": str(frozen["release_id"]),
        "AISTOCK_DATASET_CUTOFF": str(frozen["cutoff"]),
        "AISTOCK_DATASET_CONSUMER_ID": consumer_id,
        "QE_DATASET_IDENTITY_ROOTS": root,
        "QE_QLIB_DATA_PATH": day,
        "QLIB_DAY_DATA": day,
        "QLIB_DATA_PATH_WSL": day,
        "QLIB_MINUTE_DATA": minute,
        "QLIB_MINUTE_PATH_WSL": minute,
        "RDAGENT_FACTOR_DATA_WSL": factor,
        "AISTOCK_SECTOR_CONTEXT_DIR": posixpath.join(
            root,
            "components/sector_context_candidate_v1",
        ),
    }


def dataset_environment_keys() -> frozenset[str]:
    return _DATASET_ENV_KEYS


__all__: Sequence[str] = (
    "FROZEN_DATASET_TASK_BINDING_SCHEMA",
    "FrozenDatasetTaskBindingError",
    "dataset_environment_keys",
    "freeze_active_dataset_task_binding",
    "freeze_optional_active_dataset_task_binding",
    "frozen_dataset_environment",
    "require_frozen_dataset_task_binding",
)
