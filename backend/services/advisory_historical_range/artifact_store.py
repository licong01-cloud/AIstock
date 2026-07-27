"""Explicit-root immutable CAS for Phase 1R historical-range artifacts."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.services.advisory_historical_range.canonical import canonical_json_sha256, canonical_json_text
from backend.services.advisory_historical_range.models import (
    REASON_ARTIFACT_COLLISION,
    REASON_ARTIFACT_NOT_FOUND,
    REASON_ARTIFACT_ROOT_INVALID,
    REASON_ARTIFACT_TAMPERED,
    HistoricalRangeArtifactEnvelopeV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeContractError,
    HistoricalRangePlanningArtifactEnvelopeV1,
    HistoricalRangeSourceRevisionRefV1,
)


ARTIFACT_ROOT_ENV = "AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT"
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400
_logger = logging.getLogger(__name__)

_NAMESPACE_BY_KIND: dict[HistoricalRangeArtifactKind, str] = {
    HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN: "source-requirement-plans",
    HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT: "source-catalog-checkpoints",
    HistoricalRangeArtifactKind.HMM_BINDING_SET: "hmm-binding-sets",
    HistoricalRangeArtifactKind.REQUEST: "requests",
    HistoricalRangeArtifactKind.DATE_PLAN: "date-plans",
    HistoricalRangeArtifactKind.FROZEN_PROGRAM: "frozen-programs",
    HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
    HistoricalRangeArtifactKind.DECISION_MARK_SET: "decision-mark-sets",
    HistoricalRangeArtifactKind.DAY_RECEIPT: "day-receipts",
    HistoricalRangeArtifactKind.RANGE_RECEIPT: "range-receipts",
    HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT: "outcome-refresh-receipts",
    HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT: "dataset-bridge-receipts",
    HistoricalRangeArtifactKind.OUTCOME: "outcomes",
    HistoricalRangeArtifactKind.SUMMARY: "summaries",
    HistoricalRangeArtifactKind.DATASET_BRIDGE: "dataset-bridges",
}

HistoricalRangeStoredEnvelope = HistoricalRangeArtifactEnvelopeV1 | HistoricalRangePlanningArtifactEnvelopeV1


@dataclass(frozen=True)
class StoredHistoricalRangeArtifact:
    ref: HistoricalRangeArtifactRefV1
    path: Path
    idempotent: bool


class HistoricalRangeArtifactStore:
    """Publish and load exact Phase 1R refs without scanning or replacement."""

    def __init__(self, *, root: Path) -> None:
        self._root = _validate_external_root(root)
        self._root_identity_hash = canonical_json_sha256(
            {
                "schema_version": "advisory_historical_range_artifact_root_identity_v1",
                "resolved_path": _normalized_path_identity(self._root),
            }
        )

    @classmethod
    def from_environment(
        cls,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> "HistoricalRangeArtifactStore":
        values = os.environ if environ is None else environ
        raw = str(values.get(ARTIFACT_ROOT_ENV, "")).strip()
        if not raw:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_ROOT_INVALID,
                f"{ARTIFACT_ROOT_ENV} must be explicitly configured",
            )
        return cls(root=Path(raw))

    @property
    def root(self) -> Path:
        return self._root

    @property
    def root_identity_hash(self) -> str:
        return self._root_identity_hash

    def publish(self, envelope: HistoricalRangeArtifactEnvelopeV1) -> StoredHistoricalRangeArtifact:
        return self._publish(envelope)

    def publish_planning(
        self,
        envelope: HistoricalRangePlanningArtifactEnvelopeV1,
    ) -> StoredHistoricalRangeArtifact:
        return self._publish(envelope)

    def _publish(self, envelope: HistoricalRangeStoredEnvelope) -> StoredHistoricalRangeArtifact:
        identity = str(envelope.semantic_content_hash)
        content = (canonical_json_text(envelope.model_dump(mode="json")) + "\n").encode("utf-8")
        destination = self._destination(envelope.artifact_kind, identity)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            _assert_contained(path=destination, root=self._root)
            _assert_no_reparse_path(path=destination.parent, root=self._root)
        except HistoricalRangeContractError:
            raise
        except OSError as exc:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_ROOT_INVALID,
                "unable to prepare historical-range artifact namespace",
                context={"artifact_kind": envelope.artifact_kind.value, "errno": exc.errno},
            ) from exc
        if destination.exists():
            return self._existing(envelope=envelope, destination=destination, expected=content)

        temporary: Path | None = None
        try:
            descriptor, name = tempfile.mkstemp(
                prefix=f".{identity}.",
                suffix=".tmp",
                dir=destination.parent,
            )
            temporary = Path(name)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            if not _publish_no_replace(source=temporary, target=destination):
                return self._existing(envelope=envelope, destination=destination, expected=content)
            persisted = self._read_bytes(destination)
            if persisted != content:
                raise HistoricalRangeContractError(
                    REASON_ARTIFACT_TAMPERED,
                    "historical-range artifact readback differs from canonical bytes",
                    context={"artifact_kind": envelope.artifact_kind.value, "identity": identity},
                )
            loaded = self._parse_exact(
                raw=persisted,
                expected_kind=envelope.artifact_kind,
                expected_identity=identity,
            )
            return self._stored(loaded, destination, persisted, idempotent=False)
        except HistoricalRangeContractError:
            raise
        except OSError as exc:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_ROOT_INVALID,
                "unable to publish historical-range artifact",
                context={"artifact_kind": envelope.artifact_kind.value, "errno": exc.errno},
            ) from exc
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError as exc:
                    _logger.warning(
                        "historical_range_artifact_temp_cleanup_failed artifact_kind=%s identity_prefix=%s errno=%s",
                        envelope.artifact_kind.value,
                        identity[:12],
                        exc.errno,
                    )

    def publish_payload(
        self,
        *,
        artifact_kind: HistoricalRangeArtifactKind,
        producer_contract_version: str,
        payload_schema_version: str,
        resolved_request_hash: str,
        payload: dict[str, Any],
        range_run_id: str | None = None,
        day_run_id: str | None = None,
        source_revision_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...] = (),
        upstream_refs: tuple[HistoricalRangeArtifactRefV1, ...] = (),
    ) -> StoredHistoricalRangeArtifact:
        return self.publish(
            HistoricalRangeArtifactEnvelopeV1(
                artifact_kind=artifact_kind,
                producer_contract_version=producer_contract_version,
                payload_schema_version=payload_schema_version,
                resolved_request_hash=resolved_request_hash,
                range_run_id=range_run_id,
                day_run_id=day_run_id,
                source_revision_refs=source_revision_refs,
                upstream_refs=upstream_refs,
                payload=payload,
            )
        )

    def publish_planning_payload(
        self,
        *,
        artifact_kind: HistoricalRangeArtifactKind,
        planning_identity_hash: str,
        batch_id: str,
        catalog_generation: int,
        producer_contract_version: str,
        payload_schema_version: str,
        payload: dict[str, Any],
    ) -> StoredHistoricalRangeArtifact:
        return self.publish_planning(
            HistoricalRangePlanningArtifactEnvelopeV1(
                artifact_kind=artifact_kind,
                planning_identity_hash=planning_identity_hash,
                batch_id=batch_id,
                catalog_generation=catalog_generation,
                producer_contract_version=producer_contract_version,
                payload_schema_version=payload_schema_version,
                payload=payload,
            )
        )

    def load(self, ref: HistoricalRangeArtifactRefV1) -> HistoricalRangeArtifactEnvelopeV1:
        envelope = self._load(ref, planning=False)
        if not isinstance(envelope, HistoricalRangeArtifactEnvelopeV1):
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "planning artifact was loaded through the sealed artifact API",
            )
        return envelope

    def load_planning(self, ref: HistoricalRangeArtifactRefV1) -> HistoricalRangePlanningArtifactEnvelopeV1:
        envelope = self._load(ref, planning=True)
        if not isinstance(envelope, HistoricalRangePlanningArtifactEnvelopeV1):
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "sealed artifact was loaded through the planning artifact API",
            )
        return envelope

    def _load(
        self,
        ref: HistoricalRangeArtifactRefV1,
        *,
        planning: bool,
    ) -> HistoricalRangeStoredEnvelope:
        destination = self._destination(ref.artifact_kind, ref.semantic_content_hash)
        if ref.relative_path != destination.relative_to(self._root).as_posix():
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "artifact ref path differs from its content-addressed identity",
                context={"relative_path": ref.relative_path},
            )
        raw = self._read_bytes(destination)
        file_sha256 = hashlib.sha256(raw).hexdigest()
        if file_sha256 != ref.file_sha256:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "artifact raw file hash differs from its exact ref",
                context={"relative_path": ref.relative_path},
            )
        envelope = self._parse_exact(
            raw=raw,
            expected_kind=ref.artifact_kind,
            expected_identity=ref.semantic_content_hash,
            expected_planning=planning,
        )
        if (
            envelope.producer_contract_version != ref.producer_contract_version
            or envelope.payload_schema_version != ref.payload_schema_version
            or envelope.payload_sha256 != ref.payload_sha256
        ):
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "artifact contract metadata differs from its exact ref",
                context={"relative_path": ref.relative_path},
            )
        return envelope

    def _existing(
        self,
        *,
        envelope: HistoricalRangeStoredEnvelope,
        destination: Path,
        expected: bytes,
    ) -> StoredHistoricalRangeArtifact:
        persisted = self._read_bytes(destination)
        if persisted != expected:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_COLLISION,
                "same historical-range artifact identity already contains different bytes",
                context={
                    "artifact_kind": envelope.artifact_kind.value,
                    "semantic_content_hash": envelope.semantic_content_hash,
                },
            )
        loaded = self._parse_exact(
            raw=persisted,
            expected_kind=envelope.artifact_kind,
            expected_identity=str(envelope.semantic_content_hash),
        )
        return self._stored(loaded, destination, persisted, idempotent=True)

    def _stored(
        self,
        envelope: HistoricalRangeStoredEnvelope,
        path: Path,
        raw: bytes,
        *,
        idempotent: bool,
    ) -> StoredHistoricalRangeArtifact:
        ref = HistoricalRangeArtifactRefV1(
            artifact_kind=envelope.artifact_kind,
            relative_path=path.relative_to(self._root).as_posix(),
            producer_contract_version=envelope.producer_contract_version,
            payload_schema_version=envelope.payload_schema_version,
            semantic_content_hash=str(envelope.semantic_content_hash),
            payload_sha256=str(envelope.payload_sha256),
            file_sha256=hashlib.sha256(raw).hexdigest(),
        )
        return StoredHistoricalRangeArtifact(ref=ref, path=path, idempotent=idempotent)

    def _read_bytes(self, path: Path) -> bytes:
        try:
            _assert_contained(path=path, root=self._root)
            _assert_no_reparse_path(path=path, root=self._root)
            return path.read_bytes()
        except HistoricalRangeContractError:
            raise
        except FileNotFoundError as exc:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_NOT_FOUND,
                "exact historical-range artifact ref is unavailable",
                context={"relative_path": _safe_relative(path, self._root)},
            ) from exc
        except OSError as exc:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "historical-range artifact cannot be read safely",
                context={"relative_path": _safe_relative(path, self._root), "errno": exc.errno},
            ) from exc

    @staticmethod
    def _parse_exact(
        *,
        raw: bytes,
        expected_kind: HistoricalRangeArtifactKind,
        expected_identity: str,
        expected_planning: bool | None = None,
    ) -> HistoricalRangeStoredEnvelope:
        try:
            document = json.loads(raw.decode("utf-8"))
            planning = document.get("schema_version") == "advisory_historical_range_planning_artifact_envelope_v1"
            if expected_planning is not None and planning is not expected_planning:
                raise ValueError("artifact envelope kind differs from the requested API")
            envelope = (
                HistoricalRangePlanningArtifactEnvelopeV1.model_validate(document)
                if planning
                else HistoricalRangeArtifactEnvelopeV1.model_validate(document)
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "stored historical-range artifact is not a valid canonical envelope",
            ) from exc
        if envelope.artifact_kind is not expected_kind or envelope.semantic_content_hash != expected_identity:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "stored artifact identity differs from its path/ref",
            )
        canonical = (canonical_json_text(envelope.model_dump(mode="json")) + "\n").encode("utf-8")
        if canonical != raw:
            raise HistoricalRangeContractError(
                REASON_ARTIFACT_TAMPERED,
                "stored historical-range artifact is not canonical JSON",
            )
        return envelope

    def _destination(self, kind: HistoricalRangeArtifactKind, identity: str) -> Path:
        identity = _require_sha256(identity)
        return self._root / _NAMESPACE_BY_KIND[kind] / f"{identity}.json"


def _validate_external_root(root: Path) -> Path:
    expanded = root.expanduser()
    if not expanded.is_absolute():
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_ROOT_INVALID,
            "historical-range artifact root must be an absolute path",
        )
    prospective = expanded.resolve(strict=False)
    repository_root = Path(__file__).resolve().parents[3]
    try:
        prospective.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_ROOT_INVALID,
            "historical-range artifact root must be outside the repository",
        )

    existing_ancestor = expanded
    while not existing_ancestor.exists() and existing_ancestor != existing_ancestor.parent:
        existing_ancestor = existing_ancestor.parent
    _assert_existing_path_has_no_reparse(existing_ancestor)
    try:
        expanded.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_ROOT_INVALID,
            "historical-range artifact root cannot be created",
            context={"errno": exc.errno},
        ) from exc
    _assert_existing_path_has_no_reparse(expanded)
    try:
        resolved = expanded.resolve(strict=True)
    except OSError as exc:
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_ROOT_INVALID,
            "historical-range artifact root cannot be resolved",
        ) from exc
    if not resolved.is_dir():
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_ROOT_INVALID,
            "historical-range artifact root must be a directory",
        )
    return resolved


def _publish_no_replace(*, source: Path, target: Path) -> bool:
    if os.name == "nt":
        import ctypes

        move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move_file_ex.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        move_file_ex.restype = ctypes.c_int
        if move_file_ex(str(source), str(target), 0x00000008):
            return True
        error_code = ctypes.get_last_error()
        if error_code in {80, 183}:
            return False
        raise ctypes.WinError(error_code)
    try:
        os.link(source, target)
    except FileExistsError:
        return False
    directory_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return True


def _assert_contained(*, path: Path, root: Path) -> None:
    try:
        parent = path.parent.resolve(strict=True)
        (parent / path.name).relative_to(root)
    except (OSError, ValueError) as exc:
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_TAMPERED,
            "historical-range artifact path escapes its configured root",
        ) from exc


def _assert_no_reparse_path(*, path: Path, root: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_TAMPERED,
            "historical-range artifact path escapes its configured root",
        ) from exc
    current = root
    _assert_path_node_not_reparse(current)
    for part in relative.parts:
        current = current / part
        _assert_path_node_not_reparse(current)


def _assert_existing_path_has_no_reparse(path: Path) -> None:
    resolved_parts = path.parts
    if not resolved_parts:
        return
    current = Path(path.anchor)
    _assert_path_node_not_reparse(current)
    for part in resolved_parts[1:]:
        current = current / part
        _assert_path_node_not_reparse(current)


def _assert_path_node_not_reparse(path: Path) -> None:
    try:
        attributes = os.lstat(path)
    except OSError as exc:
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_ROOT_INVALID,
            "artifact root/path contains an unreadable component",
            context={"path": str(path), "errno": exc.errno},
        ) from exc
    if stat.S_ISLNK(attributes.st_mode) or (
        getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
    ):
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_ROOT_INVALID,
            "artifact root/path cannot traverse a symlink or reparse point",
            context={"path": str(path)},
        )


def _require_sha256(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise HistoricalRangeContractError(
            REASON_ARTIFACT_TAMPERED,
            "artifact identity must be lowercase sha256",
        )
    return normalized


def _normalized_path_identity(path: Path) -> str:
    value = str(path).replace("\\", "/")
    return value.casefold() if os.name == "nt" else value


def _safe_relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return "<outside-root>"
