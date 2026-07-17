"""Strongly typed provenance manifest for cached HMM artifacts."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ArtifactProvenance(BaseModel):
    """Authority and source coordinates for one cached artifact."""

    model_config = ConfigDict(extra="forbid")

    source: Literal["qe_workspace", "test_fixture"]
    task_id: str | None = None
    loop_name: str | None = None
    workspace_path: str | None = None
    remote_manifest_path: str | None = None
    remote_schema_version: str | None = None
    remote_sha256: str | None = None
    remote_size_bytes: int | None = Field(default=None, ge=0)
    remote_row_count: int | None = Field(default=None, ge=0)
    remote_quality_status: Literal["ok"] | None = None

    @field_validator("task_id", "loop_name")
    @classmethod
    def validate_component(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if (
            not normalized
            or normalized in {".", ".."}
            or "/" in normalized
            or "\\" in normalized
            or "\x00" in normalized
        ):
            raise ValueError("provenance component must be a non-empty path segment")
        return normalized

    @field_validator("workspace_path", "remote_manifest_path")
    @classmethod
    def validate_workspace_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        parts = normalized.split("/")
        if (
            not normalized
            or normalized.startswith("/")
            or "\\" in normalized
            or "\x00" in normalized
            or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError("workspace_path must be a normalized relative path")
        return normalized

    @model_validator(mode="after")
    def validate_qe_coordinates(self) -> "ArtifactProvenance":
        if self.source == "qe_workspace" and not all(
            (
                self.task_id,
                self.loop_name,
                self.workspace_path,
                self.remote_manifest_path,
                self.remote_schema_version,
                self.remote_sha256,
                self.remote_size_bytes is not None,
                self.remote_row_count is not None,
                self.remote_quality_status,
            )
        ):
            raise ValueError(
                "qe_workspace provenance requires complete trusted remote manifest fields"
            )
        if self.remote_sha256 is not None and not _SHA256_RE.fullmatch(
            self.remote_sha256
        ):
            raise ValueError("remote_sha256 must contain 64 lowercase hex characters")
        return self


class RemoteArtifactManifest(BaseModel):
    """Normalized manifest entry fetched from the authoritative QE workspace."""

    model_config = ConfigDict(extra="forbid")

    artifact_name: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    sha256: str
    size_bytes: int = Field(ge=0)
    row_count: int = Field(ge=0)
    quality_status: Literal["ok"]

    @field_validator("sha256")
    @classmethod
    def validate_remote_sha256(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not _SHA256_RE.fullmatch(normalized):
            raise ValueError("sha256 must contain 64 hex characters")
        return normalized

    @classmethod
    def from_remote_payload(
        cls,
        payload: Any,
        *,
        artifact_name: str,
    ) -> "RemoteArtifactManifest":
        """Accept a direct sidecar or a QE completion-contract manifest list."""
        candidates: list[dict[str, Any]]
        if isinstance(payload, dict) and isinstance(payload.get("artifact_manifest"), list):
            candidates = [item for item in payload["artifact_manifest"] if isinstance(item, dict)]
        elif isinstance(payload, dict) and isinstance(payload.get("artifacts"), list):
            candidates = [item for item in payload["artifacts"] if isinstance(item, dict)]
        elif isinstance(payload, dict):
            candidates = [payload]
        else:
            raise ValueError("remote manifest payload must be a JSON object")

        for candidate in candidates:
            candidate_name = str(
                candidate.get("artifact_name")
                or candidate.get("artifact_type")
                or ""
            ).strip()
            uri = str(candidate.get("uri") or "").replace("\\", "/")
            if candidate_name != artifact_name and uri.rsplit("/", 1)[-1] != artifact_name:
                continue
            metadata = candidate.get("metadata")
            metadata = metadata if isinstance(metadata, dict) else {}
            collection_status = str(candidate.get("collection_status") or "available")
            quality_status = str(
                candidate.get("quality_status")
                or metadata.get("quality_status")
                or ""
            )
            if collection_status != "available":
                raise ValueError(
                    f"remote artifact collection_status is {collection_status!r}"
                )
            return cls.model_validate(
                {
                    "artifact_name": artifact_name,
                    "schema_version": candidate.get("schema_version"),
                    "sha256": candidate.get("sha256"),
                    "size_bytes": candidate.get("size_bytes", candidate.get("file_size")),
                    "row_count": candidate.get("row_count"),
                    "quality_status": quality_status,
                }
            )
        raise ValueError(f"remote manifest has no entry for {artifact_name}")


class ArtifactManifest(BaseModel):
    """Immutable integrity and provenance contract for one cache entry."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["hmm_artifact_manifest_v1"] = "hmm_artifact_manifest_v1"
    loop_ref: str
    cache_key: str
    artifact_name: str
    file_size: int = Field(ge=0)
    sha256: str
    cached_at: datetime
    expires_at: datetime
    provenance: ArtifactProvenance

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("sha256 must contain exactly 64 lowercase hex characters")
        return value

    @model_validator(mode="after")
    def validate_expiry(self) -> "ArtifactManifest":
        if self.expires_at <= self.cached_at:
            raise ValueError("expires_at must be later than cached_at")
        return self

    @field_validator("cached_at", "expires_at")
    @classmethod
    def validate_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("manifest timestamps must be timezone-aware")
        return value
