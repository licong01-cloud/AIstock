"""Read-only registration of an already-built dataset candidate.

The cataloger deliberately has no exporter, database, provider, activation, or
process-control dependency.  It reads one exact allowlisted candidate and one
explicit evidence manifest, verifies immutable bytes, then writes only a
canonical receipt and candidate registration to the control repository.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION

from .canonical import ensure_sha256, normalize_root_relative_path
from .contracts import (
    Component,
    ProducerProvenanceState,
    Scope,
    UNKNOWN_PRODUCER_PROVENANCE,
)
from .control_service import DatasetReleaseControlService
from .control_store import CandidateRegistrationSpec, volume_identity
from .errors import DatasetReleaseError
from .index_contract import index_contract_payload
from .pit import pit_spans_sha256
from .profile import DatasetProfile
from .publisher import artifact_tree_digest


LEGACY_CATALOG_EVIDENCE_SCHEMA = "dataset_release_legacy_catalog_evidence_v1"
LEGACY_CATALOG_RECEIPT_SCHEMA = "dataset_release_legacy_catalog_receipt_v1"
MAX_EVIDENCE_BYTES = 2 * 1024 * 1024
MAX_COMPONENT_MANIFEST_BYTES = 16 * 1024 * 1024
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "profile",
        "semantic_profile_digest",
        "scope",
        "cutoff",
        "artifact_root",
        "artifact_schema_version",
        "pit_manifest",
        "pit_snapshot_digest",
        "moneyflow_contract",
        "static_contract",
        "index_contract",
        "component_manifests",
        "producer_provenance",
    }
)


class LegacyCatalogError(DatasetReleaseError):
    code = "BLOCKED_LEGACY_CATALOG_EVIDENCE_INVALID"


@dataclass(frozen=True, slots=True)
class LegacyCatalogRequest:
    candidate_path: Path
    evidence_manifest: Path
    scope: Scope
    cutoff: date


class LegacyCandidateCataloger:
    """Validate and catalog one candidate without writing inside its tree."""

    def __init__(
        self,
        *,
        service: DatasetReleaseControlService,
        profile: DatasetProfile,
        candidate_root: Path | None = None,
    ) -> None:
        self.service = service
        self.profile = profile
        self.candidate_root = _plain_directory(
            candidate_root if candidate_root is not None else Path(profile.candidate_root)
        )
        if profile.profile not in service.profile_ids:
            raise LegacyCatalogError("catalog profile is not registered by the control service")

    def catalog(self, request: LegacyCatalogRequest) -> dict[str, Any]:
        candidate = _contained_candidate(request.candidate_path, self.candidate_root)
        evidence_path = _plain_file(request.evidence_manifest)
        evidence_raw, evidence = _read_bounded_json(
            evidence_path,
            max_bytes=MAX_EVIDENCE_BYTES,
            label="legacy catalog evidence",
        )
        normalized = self._validate_evidence(
            evidence,
            candidate=candidate,
            scope=Scope(request.scope),
            cutoff=request.cutoff,
        )

        # A single scan can hash a mixed point in time if an operator violates
        # the immutable-candidate rule.  Two identical complete Merkle scans,
        # also matching the independently declared root, fail closed on such a
        # race without ever taking a write lock or touching the candidate.
        first_root = artifact_tree_digest(candidate)
        second_root = artifact_tree_digest(candidate)
        declared_root = str(normalized["artifact_root"])
        if first_root != second_root or first_root != declared_root:
            raise LegacyCatalogError("candidate artifact root is unstable or differs from evidence")

        root_relative_path = normalize_root_relative_path(candidate.relative_to(self.candidate_root).as_posix())
        if "candidate" not in Path(root_relative_path).name.casefold():
            raise LegacyCatalogError("catalog target name must explicitly contain candidate")
        volume_serial = volume_identity(self.candidate_root)
        receipt = {
            "schema_version": LEGACY_CATALOG_RECEIPT_SCHEMA,
            "profile": self.profile.profile,
            "semantic_profile_digest": self.profile.semantic_profile_digest,
            "scope": Scope(request.scope).value,
            "cutoff": request.cutoff.isoformat(),
            "allowlisted_root_id": self.profile.candidate_root_id,
            "volume_serial": volume_serial,
            "root_relative_path": root_relative_path,
            "artifact_root": first_root,
            "artifact_scan_count": 2,
            "artifact_stability": "two_complete_merkle_scans_equal",
            "artifact_schema_version": normalized["artifact_schema_version"],
            "evidence_manifest_sha256": hashlib.sha256(evidence_raw).hexdigest(),
            "pit_snapshot_digest": normalized["pit_snapshot_digest"],
            "pit_manifest": normalized["pit_manifest"],
            "moneyflow_contract": normalized["moneyflow_contract"],
            "static_contract": normalized["static_contract"],
            "index_contract": normalized["index_contract"],
            "component_manifests": normalized["component_manifests"],
            "producer_provenance": normalized["producer_provenance"],
            "source_manifest_ref": None,
            "source_equivalence": "not_claimed_catalog_only",
            "safety": {
                "candidate_writes": 0,
                "database_reads": 0,
                "database_writes": 0,
                "provider_calls": 0,
                "production_writes": 0,
                "production_deletes": 0,
                "production_pointer_changes": 0,
                "service_process_controls": 0,
            },
        }
        producer = normalized["producer_provenance"]
        return self.service.catalog_legacy_candidate(
            profile_id=self.profile.profile,
            receipt=receipt,
            registration=CandidateRegistrationSpec(
                allowlisted_root_id=self.profile.candidate_root_id,
                volume_serial=volume_serial,
                root_relative_path=root_relative_path,
                profile=self.profile.profile,
                scope=Scope(request.scope).value,
                cutoff=request.cutoff,
                lineage_anchor="LEGACY_RECEIPT:pending:" + "0" * 64,
                artifact_root=first_root,
                producer_provenance_state=str(producer["state"]),
                producer_provenance_digest_or_sentinel=str(producer["digest_or_sentinel"]),
                pit_provenance_state="KNOWN",
                pit_provenance_digest_or_sentinel=str(normalized["pit_snapshot_digest"]),
            ),
        )

    def _validate_evidence(
        self,
        evidence: Mapping[str, Any],
        *,
        candidate: Path,
        scope: Scope,
        cutoff: date,
    ) -> dict[str, Any]:
        if set(evidence) != _TOP_LEVEL_FIELDS:
            raise LegacyCatalogError("legacy catalog evidence fields differ from v1")
        expected_scalars = {
            "schema_version": LEGACY_CATALOG_EVIDENCE_SCHEMA,
            "profile": self.profile.profile,
            "semantic_profile_digest": self.profile.semantic_profile_digest,
            "scope": scope.value,
            "cutoff": cutoff.isoformat(),
            "moneyflow_contract": MONEYFLOW_UNIT_CONTRACT_VERSION,
        }
        for field, expected in expected_scalars.items():
            if evidence.get(field) != expected:
                raise LegacyCatalogError(f"legacy catalog evidence differs: {field}")
        artifact_root = ensure_sha256(str(evidence.get("artifact_root", "")), field="artifact_root")
        artifact_schema = str(evidence.get("artifact_schema_version", "")).strip()
        if not artifact_schema:
            raise LegacyCatalogError("artifact_schema_version must be non-empty")

        static_contract = evidence.get("static_contract")
        expected_static = {
            "schema_version": self.profile.static_schema_version,
            "ordered_columns_digest": self.profile.static_schema_digest,
            "column_count": self.profile.static_column_count,
            "l2_code_id_dtype": self.profile.l2_code_id_dtype,
            "l2_code_id_missing": self.profile.l2_code_id_missing,
        }
        if static_contract != expected_static:
            raise LegacyCatalogError("legacy static factor contract differs")
        if evidence.get("index_contract") != index_contract_payload():
            raise LegacyCatalogError("legacy index/HMM contract differs")

        pit_digest = ensure_sha256(
            str(evidence.get("pit_snapshot_digest", "")),
            field="pit_snapshot_digest",
        )
        pit_ref, pit_payload = _verified_json_manifest(
            candidate,
            evidence.get("pit_manifest"),
            max_bytes=MAX_COMPONENT_MANIFEST_BYTES,
            label="pit_manifest",
        )
        if (
            pit_payload.get("schema_version") != "dataset_release_frozen_pit_v1"
            or pit_payload.get("universe_key") != self.profile.universe_key
            or pit_payload.get("rule_version") != self.profile.universe_rule_version
            or pit_payload.get("scope") != {"start": self.profile.start_date.isoformat(), "cutoff": cutoff.isoformat()}
            or pit_payload.get("spans_sha256") != pit_digest
            or not isinstance(pit_payload.get("spans"), list)
            or pit_spans_sha256(pit_payload["spans"]) != pit_digest
        ):
            raise LegacyCatalogError("legacy PIT manifest contract/digest differs")

        manifests = evidence.get("component_manifests")
        expected_components = tuple(item.value for item in Component)
        if not isinstance(manifests, Mapping) or set(manifests) != set(expected_components):
            raise LegacyCatalogError("legacy component manifest set differs")
        normalized_manifests: dict[str, Any] = {}
        for component in expected_components:
            verified, _payload = _verified_json_manifest(
                candidate,
                manifests[component],
                max_bytes=MAX_COMPONENT_MANIFEST_BYTES,
                label=f"component_manifest:{component}",
            )
            normalized_manifests[component] = verified

        producer = evidence.get("producer_provenance")
        if not isinstance(producer, Mapping) or set(producer) != {
            "state",
            "digest_or_sentinel",
        }:
            raise LegacyCatalogError("producer provenance evidence is invalid")
        try:
            producer_state = ProducerProvenanceState(str(producer["state"]))
        except ValueError as exc:
            raise LegacyCatalogError("producer provenance state is invalid") from exc
        if producer_state is ProducerProvenanceState.RECONSTRUCTED_SOURCE_ONLY:
            raise LegacyCatalogError("catalog cannot claim reconstructed provenance before re-attestation")
        producer_digest = str(producer["digest_or_sentinel"])
        if producer_state is ProducerProvenanceState.KNOWN:
            ensure_sha256(producer_digest, field="producer_provenance_digest")
        elif producer_digest != UNKNOWN_PRODUCER_PROVENANCE:
            raise LegacyCatalogError("unknown producer provenance sentinel differs")

        return {
            "artifact_root": artifact_root,
            "artifact_schema_version": artifact_schema,
            "pit_snapshot_digest": pit_digest,
            "pit_manifest": pit_ref,
            "moneyflow_contract": MONEYFLOW_UNIT_CONTRACT_VERSION,
            "static_contract": expected_static,
            "index_contract": index_contract_payload(),
            "component_manifests": normalized_manifests,
            "producer_provenance": {
                "state": producer_state.value,
                "digest_or_sentinel": producer_digest,
            },
        }


def _verified_json_manifest(
    candidate: Path,
    raw_reference: Any,
    *,
    max_bytes: int,
    label: str,
) -> tuple[dict[str, Any], Mapping[str, Any]]:
    if not isinstance(raw_reference, Mapping) or set(raw_reference) != {
        "relative_path",
        "sha256",
        "schema_version",
    }:
        raise LegacyCatalogError(f"{label} reference is invalid")
    relative = normalize_root_relative_path(str(raw_reference["relative_path"]))
    path = _contained_file(candidate, relative)
    expected_digest = ensure_sha256(str(raw_reference["sha256"]), field=f"{label}:sha256")
    raw, payload = _read_bounded_json(path, max_bytes=max_bytes, label=label)
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        raise LegacyCatalogError(f"{label} sha256 differs")
    schema_version = str(raw_reference["schema_version"])
    if not schema_version or payload.get("schema_version") != schema_version:
        raise LegacyCatalogError(f"{label} schema_version differs")
    return (
        {
            "relative_path": relative,
            "sha256": expected_digest,
            "size_bytes": len(raw),
            "schema_version": schema_version,
        },
        payload,
    )


def _read_bounded_json(path: Path, *, max_bytes: int, label: str) -> tuple[bytes, Mapping[str, Any]]:
    size = path.stat().st_size
    if size <= 0 or size > max_bytes:
        raise LegacyCatalogError(f"{label} size is outside 1..{max_bytes}")
    with path.open("rb") as handle:
        raw = handle.read(max_bytes + 1)
    if len(raw) != size or len(raw) > max_bytes:
        raise LegacyCatalogError(f"{label} changed or exceeds the bounded read")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LegacyCatalogError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(payload, Mapping):
        raise LegacyCatalogError(f"{label} must be a JSON object")
    return raw, payload


def _contained_candidate(requested: Path, root: Path) -> Path:
    candidate = _plain_directory(requested)
    if candidate == root:
        raise LegacyCatalogError("candidate path cannot be the allowlisted root itself")
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise LegacyCatalogError("candidate path escapes the allowlisted root") from exc
    return candidate


def _contained_file(root: Path, relative: str) -> Path:
    path = root.joinpath(*relative.split("/"))
    resolved = _plain_file(path)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise LegacyCatalogError("manifest path escapes candidate") from exc
    return resolved


def _plain_directory(path: Path) -> Path:
    resolved = _plain_existing(path)
    if not resolved.is_dir():
        raise LegacyCatalogError(f"path is not a directory: {resolved}")
    return resolved


def _plain_file(path: Path) -> Path:
    resolved = _plain_existing(path)
    if not resolved.is_file():
        raise LegacyCatalogError(f"path is not a file: {resolved}")
    return resolved


def _plain_existing(path: Path) -> Path:
    requested = Path(path).expanduser()
    if not requested.is_absolute():
        raise LegacyCatalogError("catalog paths must be absolute")
    _assert_plain_chain(requested.absolute())
    try:
        return requested.resolve(strict=True)
    except OSError as exc:
        raise LegacyCatalogError(f"catalog path is unavailable: {requested}") from exc


def _assert_plain_chain(path: Path) -> None:
    current = Path(path.anchor)
    if current.exists():
        _assert_plain(current)
    for part in path.parts[1:]:
        current = current / part
        _assert_plain(current)


def _assert_plain(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise LegacyCatalogError(f"catalog path component is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or (int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise LegacyCatalogError(f"catalog path traverses symlink/reparse: {path}")


__all__ = [
    "LEGACY_CATALOG_EVIDENCE_SCHEMA",
    "LEGACY_CATALOG_RECEIPT_SCHEMA",
    "LegacyCandidateCataloger",
    "LegacyCatalogError",
    "LegacyCatalogRequest",
]
