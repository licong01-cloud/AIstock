"""Content-addressed, no-replace artifact store for Phase 1E research plans."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.services.advisory_phase0a.policy import canonicalize


REASON_PLAN_ARTIFACT_CONFLICT = "ADVISORY_PHASE1E_PLAN_ARTIFACT_CONFLICT"
REASON_ARTIFACT_STORE_CONFIG_INVALID = "ADVISORY_PHASE1E_ARTIFACT_STORE_CONFIG_INVALID"
REASON_ARTIFACT_STORE_IO_FAILED = "ADVISORY_PHASE1E_ARTIFACT_STORE_IO_FAILED"

_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,200}$")
_KINDS = frozenset({"audit", "plan", "batch"})
LOGGER = logging.getLogger("aistock.advisory.phase1e.store")


class Phase1EArtifactStoreError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = context or {}


class ContentAddressedPlanStore:
    """Filesystem CAS with atomic hard-link publication and exact retry readback."""

    def __init__(self, *, root: Path, policy_hash: str, producer_code_commit: str | None = None) -> None:
        self._root = self._validate_root(root)
        self._policy_hash = self._sha256(policy_hash, field_name="policy_hash")
        self._producer_code_commit = str(producer_code_commit or "").strip() or None

    @classmethod
    def from_environment(cls, *, policy_hash: str, env: dict[str, str] | None = None) -> "ContentAddressedPlanStore":
        environment = env if env is not None else os.environ
        raw_root = str(environment.get("AISTOCK_ADVISORY_PHASE1E_ARTIFACT_ROOT") or "").strip()
        if not raw_root:
            raise Phase1EArtifactStoreError(
                REASON_ARTIFACT_STORE_CONFIG_INVALID,
                "AISTOCK_ADVISORY_PHASE1E_ARTIFACT_ROOT is required",
            )
        return cls(
            root=Path(raw_root),
            policy_hash=policy_hash,
            producer_code_commit=environment.get("AISTOCK_GIT_COMMIT"),
        )

    @property
    def root(self) -> Path:
        return self._root

    def publish(self, *, kind: str, identity: str, payload: dict[str, Any], semantic_hash: str) -> dict[str, Any]:
        kind = self._validate_kind(kind)
        identity = self._validate_identity(identity)
        semantic_hash = self._sha256(semantic_hash, field_name="semantic_hash")
        destination = self._destination(kind=kind, identity=identity, semantic_hash=semantic_hash)
        canonical_payload = canonicalize(payload)
        envelope = {
            "schema_version": "advisory_phase1e_artifact_envelope_v1",
            "kind": kind,
            "identity": identity,
            "semantic_hash": semantic_hash,
            "store_policy_hash": self._policy_hash,
            "payload": canonical_payload,
        }
        payload_bytes = self._canonical_bytes(envelope)
        file_sha256 = hashlib.sha256(payload_bytes).hexdigest()
        stored_envelope = {**envelope, "file_sha256": file_sha256}
        materialization = {
            "producer_code_commit": self._producer_code_commit,
            "first_written_at": datetime.now(UTC).isoformat(),
        }
        final_document = {**stored_envelope, "materialization": materialization}
        final_bytes = self._canonical_bytes(final_document)

        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            return self._read_existing(destination=destination, expected=envelope)

        temp_path: Path | None = None
        try:
            handle, raw_temp = tempfile.mkstemp(prefix=f".{identity}.", suffix=".tmp", dir=destination.parent)
            temp_path = Path(raw_temp)
            with os.fdopen(handle, "wb") as stream:
                stream.write(final_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temp_path, destination)
            except FileExistsError:
                return self._read_existing(destination=destination, expected=envelope)
            except OSError as exc:
                raise Phase1EArtifactStoreError(
                    REASON_ARTIFACT_STORE_IO_FAILED,
                    "artifact store does not support atomic no-replace hard-link publication",
                    context={"path": str(destination), "errno": exc.errno},
                ) from exc
            return self._read_existing(destination=destination, expected=envelope)
        except Phase1EArtifactStoreError:
            raise
        except OSError as exc:
            raise Phase1EArtifactStoreError(
                REASON_ARTIFACT_STORE_IO_FAILED,
                "unable to publish Phase 1E artifact",
                context={"path": str(destination), "errno": exc.errno},
            ) from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError as exc:
                    LOGGER.warning("phase1e_artifact_temp_cleanup_failed path=%s errno=%s", temp_path, exc.errno)

    def verify(self, *, kind: str, identity: str, semantic_hash: str) -> dict[str, Any]:
        kind = self._validate_kind(kind)
        identity = self._validate_identity(identity)
        semantic_hash = self._sha256(semantic_hash, field_name="semantic_hash")
        destination = self._destination(kind=kind, identity=identity, semantic_hash=semantic_hash)
        expected = {
            "kind": kind,
            "identity": identity,
            "semantic_hash": semantic_hash,
            "store_policy_hash": self._policy_hash,
        }
        return self._read_existing(destination=destination, expected=expected)

    def inspect(self, *, kind: str, identity: str, semantic_hash: str) -> dict[str, Any]:
        return self.verify(kind=kind, identity=identity, semantic_hash=semantic_hash)

    def _destination(self, *, kind: str, identity: str, semantic_hash: str) -> Path:
        prefix = semantic_hash[:2]
        namespace = self._root / "advisory" / "phase1e"
        if kind == "audit":
            return namespace / "audits" / prefix / identity / "audit.json"
        if kind == "plan":
            return namespace / "plans" / prefix / f"{semantic_hash}.json"
        return namespace / "batches" / prefix / f"{semantic_hash}.json"

    def _read_existing(self, *, destination: Path, expected: dict[str, Any]) -> dict[str, Any]:
        try:
            raw = destination.read_bytes()
            document = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise Phase1EArtifactStoreError(
                REASON_ARTIFACT_STORE_IO_FAILED,
                "existing artifact is unreadable or truncated",
                context={"path": str(destination)},
            ) from exc
        if not isinstance(document, dict):
            raise Phase1EArtifactStoreError(REASON_PLAN_ARTIFACT_CONFLICT, "existing artifact is not an object", context={"path": str(destination)})
        file_sha256 = str(document.get("file_sha256") or "")
        materialization = document.get("materialization")
        semantic_document = {key: value for key, value in document.items() if key not in {"file_sha256", "materialization"}}
        if not isinstance(materialization, dict) or hashlib.sha256(self._canonical_bytes(semantic_document)).hexdigest() != file_sha256:
            raise Phase1EArtifactStoreError(REASON_PLAN_ARTIFACT_CONFLICT, "existing artifact file hash is invalid", context={"path": str(destination)})
        for key, expected_value in expected.items():
            if semantic_document.get(key) != expected_value:
                raise Phase1EArtifactStoreError(
                    REASON_PLAN_ARTIFACT_CONFLICT,
                    "existing artifact path binds different immutable content",
                    context={"path": str(destination), "field": key},
                )
        return document

    @staticmethod
    def _canonical_bytes(payload: dict[str, Any]) -> bytes:
        return json.dumps(canonicalize(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _sha256(value: str, *, field_name: str) -> str:
        normalized = str(value or "").strip().lower()
        if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
            raise Phase1EArtifactStoreError(REASON_ARTIFACT_STORE_CONFIG_INVALID, f"{field_name} must be SHA256")
        return normalized

    @staticmethod
    def _validate_kind(kind: str) -> str:
        normalized = str(kind or "").strip()
        if normalized not in _KINDS:
            raise Phase1EArtifactStoreError(REASON_ARTIFACT_STORE_CONFIG_INVALID, "unsupported Phase 1E artifact kind")
        return normalized

    @staticmethod
    def _validate_identity(identity: str) -> str:
        normalized = str(identity or "").strip()
        if not _SAFE_ID.fullmatch(normalized):
            raise Phase1EArtifactStoreError(REASON_ARTIFACT_STORE_CONFIG_INVALID, "artifact identity is not path-safe")
        return normalized

    @staticmethod
    def _validate_root(root: Path) -> Path:
        raw_normalized = str(root.expanduser()).replace("\\", "/").lower()
        if raw_normalized.startswith("//wsl$/") or raw_normalized.startswith("//wsl.localhost/"):
            raise Phase1EArtifactStoreError(
                REASON_ARTIFACT_STORE_CONFIG_INVALID,
                "Phase 1E artifact root cannot be a WSL workspace path",
                context={"root": str(root)},
            )
        try:
            resolved = root.expanduser().resolve()
        except OSError as exc:
            raise Phase1EArtifactStoreError(REASON_ARTIFACT_STORE_CONFIG_INVALID, "artifact root cannot be resolved") from exc
        project_root = Path(__file__).resolve().parents[3]
        try:
            resolved.relative_to(project_root)
        except ValueError:
            pass
        else:
            raise Phase1EArtifactStoreError(
                REASON_ARTIFACT_STORE_CONFIG_INVALID,
                "Phase 1E artifact root must be outside the repository",
                context={"root": str(resolved)},
            )
        normalized = str(resolved).replace("\\", "/").lower()
        if (
            normalized.startswith("/mnt/")
            or "/wsl/" in normalized
            or normalized.startswith("//wsl$/")
            or normalized.startswith("//wsl.localhost/")
        ):
            raise Phase1EArtifactStoreError(
                REASON_ARTIFACT_STORE_CONFIG_INVALID,
                "Phase 1E artifact root cannot be a WSL workspace path",
                context={"root": str(resolved)},
            )
        return resolved
