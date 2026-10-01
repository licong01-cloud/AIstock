"""Immutable transport contract for the formal QE assistance artifact.

The formal artifact is intentionally not embedded in the QE submission JSON.
It is deployed once to a content-addressed RD-Agent asset path and the loop
request carries this compact, hash-bound receipt.  The strategy still validates
the complete artifact and its canonical business hash when it first loads it.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from backend.services.hmm_risk.qe_assistance_adapter import SCHEMA_VERSION

BINDING_PARAM = "_precomputed_hmm_coefficients_artifact_binding"
BINDING_FILE = "hmm_sector_coefficients.binding.json"
BINDING_SCHEMA_VERSION = "hmm_risk_qe_assistance_artifact_binding_v1"
EXPECTED_FILENAME = "hmm_sector_coefficients.json"
EXPECTED_REMOTE_ROOT = "/home/lc999/aistock_immutable_assets/hmm_qe_assistance"
FORMAL_FILE_SHA256 = "fe6a2bc50037175c386fa04bbd2e49c4eec03672e435836d6f488094be9f407f"
FORMAL_CANONICAL_SHA256 = "de92f166c06112771c0d1385043f8599d22fbed44e5ed1c43fb8b138e86bc971"
REASON_BINDING_INVALID = "hmm_risk_qe_assistance_artifact_binding_invalid"
REASON_LOCAL_HASH_MISMATCH = "hmm_risk_qe_assistance_artifact_file_hash_mismatch"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class QEAssistanceTransportError(RuntimeError):
    """Typed fail-closed transport error."""

    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


def _sha256(value: Any, field: str) -> str:
    text = str(value or "").strip().lower()
    if _SHA256.fullmatch(text) is None:
        raise QEAssistanceTransportError(REASON_BINDING_INVALID, f"{field} must be a lowercase SHA-256")
    return text


def _remote_path(value: Any, file_sha256: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    normalized = posixpath.normpath(text)
    if (
        not text.startswith("/")
        or normalized != text
        or text.endswith("/")
        or posixpath.basename(text) != EXPECTED_FILENAME
        or posixpath.basename(posixpath.dirname(text)) != file_sha256
        or posixpath.dirname(posixpath.dirname(text)) != EXPECTED_REMOTE_ROOT
        or "/qe_workspace/" in text
        or "/rdagent_workspace/" in text
    ):
        raise QEAssistanceTransportError(
            REASON_BINDING_INVALID,
            "remote artifact path must be an absolute content-addressed non-workspace path",
        )
    return text


def normalize_artifact_binding(
    value: Mapping[str, Any],
    *,
    verify_local_file: bool,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise QEAssistanceTransportError(REASON_BINDING_INVALID, "artifact binding must be an object")
    if value.get("schema_version") != BINDING_SCHEMA_VERSION:
        raise QEAssistanceTransportError(REASON_BINDING_INVALID, "artifact binding schema is unsupported")
    if value.get("artifact_schema_version") != SCHEMA_VERSION:
        raise QEAssistanceTransportError(REASON_BINDING_INVALID, "artifact business schema is unsupported")
    file_sha256 = _sha256(value.get("file_sha256"), "file_sha256")
    canonical_sha256 = _sha256(value.get("canonical_sha256"), "canonical_sha256")
    remote_path = _remote_path(value.get("remote_path"), file_sha256)
    local_path_raw = str(value.get("local_path") or "").strip()
    local_path: str | None = None
    size_bytes = value.get("size_bytes")
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes <= 0:
        raise QEAssistanceTransportError(REASON_BINDING_INVALID, "size_bytes must be a positive integer")

    if verify_local_file:
        candidate = Path(local_path_raw)
        if not candidate.is_absolute() or not candidate.is_file() or candidate.is_symlink():
            raise QEAssistanceTransportError(
                REASON_BINDING_INVALID,
                "local artifact must be an absolute regular non-symlink file",
            )
        try:
            for path_part in (candidate, *candidate.parents):
                if path_part.is_symlink() or (
                    hasattr(path_part, "is_junction") and path_part.is_junction()
                ):
                    raise QEAssistanceTransportError(
                        REASON_BINDING_INVALID,
                        "local artifact path must not traverse a symlink or junction",
                    )
        except OSError as exc:
            raise QEAssistanceTransportError(REASON_BINDING_INVALID, "local artifact junction check failed") from exc
        digest = hashlib.sha256()
        observed_size = 0
        with candidate.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                observed_size += len(chunk)
                digest.update(chunk)
        if observed_size != size_bytes or digest.hexdigest() != file_sha256:
            raise QEAssistanceTransportError(
                REASON_LOCAL_HASH_MISMATCH,
                "local artifact size or SHA-256 differs from the immutable binding",
            )
        local_path = str(candidate)
    elif local_path_raw:
        raise QEAssistanceTransportError(
            REASON_BINDING_INVALID,
            "runtime binding must not persist a controller-local artifact path",
        )

    normalized = {
        "schema_version": BINDING_SCHEMA_VERSION,
        "artifact_schema_version": SCHEMA_VERSION,
        "remote_path": remote_path,
        "file_sha256": file_sha256,
        "canonical_sha256": canonical_sha256,
        "size_bytes": size_bytes,
    }
    if local_path is not None:
        normalized["local_path"] = local_path
    return normalized


def runtime_binding(value: Mapping[str, Any]) -> dict[str, Any]:
    """Drop the controller-local path before persistence or submission."""

    normalized = normalize_artifact_binding(value, verify_local_file=True)
    normalized.pop("local_path", None)
    return normalized


def formal_runtime_binding(value: Mapping[str, Any]) -> dict[str, Any]:
    """Verify and strip the controller path for the one approved formal artifact."""

    normalized = runtime_binding(value)
    if (
        normalized["file_sha256"] != FORMAL_FILE_SHA256
        or normalized["canonical_sha256"] != FORMAL_CANONICAL_SHA256
    ):
        raise QEAssistanceTransportError(
            REASON_BINDING_INVALID,
            "formal QE-assistance artifact file or canonical identity differs",
        )
    return normalized


def binding_json(value: Mapping[str, Any]) -> str:
    normalized = normalize_artifact_binding(value, verify_local_file=False)
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
