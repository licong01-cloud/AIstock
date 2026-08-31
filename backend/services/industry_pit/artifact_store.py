"""Repo-external writer/readback for C-013 authority candidates."""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import re
import shutil
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Iterator, Mapping, Sequence

from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields, sha256_hex

from .contracts import (
    CANDIDATE_BUNDLE_SCHEMA,
    PREFLIGHT_REPORT_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    CandidateInterval,
    IndustryPitContractError,
    UnavailableReason,
    authority_receipt_from_mapping,
    candidate_interval_from_mapping,
)
from .candidate_builder import TaxonomyCatalog, taxonomy_catalog_from_mapping


_GIT_OBJECT_RE = re.compile(r"^[0-9a-f]{40}$")
_FILE_HASH_CHUNK_BYTES = 1024 * 1024
_JSONL_SORT_CHUNK_BYTES = 8 * 1024 * 1024
_JSONL_SORT_CHUNK_ROWS = 2048
_JSONL_MERGE_FAN_IN = 32


@dataclass(frozen=True, slots=True)
class _FileObservation:
    sha256: str
    size_bytes: int
    row_count: int | None = None

    def manifest_entry(self) -> Mapping[str, Any]:
        payload: dict[str, Any] = {
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }
        if self.row_count is not None:
            payload["row_count"] = self.row_count
        return payload


@dataclass(frozen=True, slots=True)
class CandidateBundleReadback:
    artifact_root: Path
    manifest: Mapping[str, Any]
    classification_receipt: AuthorityReceipt
    index_membership_receipt: AuthorityReceipt
    classification_intervals: tuple[CandidateInterval, ...]
    index_membership_intervals: tuple[CandidateInterval, ...]
    preflight_report: Mapping[str, Any]


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def require_repo_external_root(path: Path, *, forbidden_roots: Sequence[Path]) -> Path:
    resolved = path.expanduser().resolve(strict=False)
    for root in forbidden_roots:
        root_resolved = root.expanduser().resolve(strict=False)
        if resolved == root_resolved or _is_relative_to(resolved, root_resolved):
            raise IndustryPitContractError(f"candidate artifact root must be repo-external: {resolved}")
    if resolved.parent == resolved:
        raise IndustryPitContractError("candidate artifact root cannot be a volume root")
    return resolved


def _write_canonical_json(path: Path, payload: Any) -> _FileObservation:
    encoded = canonical_json_bytes(payload) + b"\n"
    path.write_bytes(encoded)
    return _FileObservation(sha256=sha256_hex(encoded), size_bytes=len(encoded))


def _write_sorted_chunk(path: Path, rows: list[bytes]) -> None:
    rows.sort()
    with path.open("wb") as handle:
        for row in rows:
            handle.write(row)
            handle.write(b"\n")


def _iter_merged_lines(paths: Sequence[Path]) -> Iterator[bytes]:
    with ExitStack() as stack:
        handles = [stack.enter_context(path.open("rb")) for path in paths]
        yield from heapq.merge(*handles)


def _merge_sorted_files(paths: Sequence[Path], output: BinaryIO) -> int:
    row_count = 0
    for line in _iter_merged_lines(paths):
        output.write(line)
        row_count += 1
    return row_count


def _collapse_sort_chunks(sort_root: Path, paths: list[Path]) -> list[Path]:
    level = 0
    while len(paths) > _JSONL_MERGE_FAN_IN:
        next_paths: list[Path] = []
        for offset in range(0, len(paths), _JSONL_MERGE_FAN_IN):
            group = paths[offset : offset + _JSONL_MERGE_FAN_IN]
            merged = sort_root / f"merge-{level:04d}-{len(next_paths):08d}.jsonl"
            with merged.open("wb") as handle:
                _merge_sorted_files(group, handle)
            next_paths.append(merged)
        for old_path in paths:
            old_path.unlink()
        paths = next_paths
        level += 1
    return paths


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> _FileObservation:
    if (
        _JSONL_SORT_CHUNK_BYTES <= 0
        or _JSONL_SORT_CHUNK_ROWS <= 0
        or _JSONL_MERGE_FAN_IN < 2
    ):
        raise RuntimeError("industry PIT JSONL sort resource bounds are invalid")
    sort_root = path.parent / f".{path.name}.sort-{uuid.uuid4().hex}"
    sort_root.mkdir(parents=False, exist_ok=False)
    row_count = 0
    chunk: list[bytes] = []
    chunk_size_bytes = 0
    chunk_paths: list[Path] = []
    try:
        for row in rows:
            encoded = canonical_json_bytes(dict(row))
            chunk.append(encoded)
            chunk_size_bytes += len(encoded) + 1
            row_count += 1
            if (
                len(chunk) >= _JSONL_SORT_CHUNK_ROWS
                or chunk_size_bytes >= _JSONL_SORT_CHUNK_BYTES
            ):
                chunk_path = sort_root / f"chunk-{len(chunk_paths):08d}.jsonl"
                _write_sorted_chunk(chunk_path, chunk)
                chunk_paths.append(chunk_path)
                chunk = []
                chunk_size_bytes = 0
        if chunk:
            chunk_path = sort_root / f"chunk-{len(chunk_paths):08d}.jsonl"
            _write_sorted_chunk(chunk_path, chunk)
            chunk_paths.append(chunk_path)
        chunk_paths = _collapse_sort_chunks(sort_root, chunk_paths)

        digest = hashlib.sha256()
        size_bytes = 0
        observed_rows = 0
        with path.open("wb") as handle:
            for line in _iter_merged_lines(chunk_paths):
                handle.write(line)
                digest.update(line)
                size_bytes += len(line)
                observed_rows += 1
        if observed_rows != row_count:
            raise IndustryPitContractError(
                "candidate JSONL external sort row count mismatch: "
                f"expected={row_count} observed={observed_rows}"
            )
        observation = _FileObservation(
            sha256=digest.hexdigest(),
            size_bytes=size_bytes,
            row_count=row_count,
        )
        return observation
    except Exception:
        path.unlink(missing_ok=True)
        raise
    finally:
        if sort_root.exists():
            try:
                shutil.rmtree(sort_root)
            except Exception:
                path.unlink(missing_ok=True)
                raise


def _observe_file(path: Path, *, row_count: int | None = None) -> _FileObservation:
    digest = hashlib.sha256()
    size_bytes = 0
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(_FILE_HASH_CHUNK_BYTES), b""):
            digest.update(block)
            size_bytes += len(block)
    return _FileObservation(
        sha256=digest.hexdigest(),
        size_bytes=size_bytes,
        row_count=row_count,
    )


def _candidate_hash(
    *,
    authority_type: AuthorityType,
    receipt: AuthorityReceipt,
    intervals: Sequence[CandidateInterval],
    preflight_report: Mapping[str, Any],
) -> str:
    section = "classification" if authority_type is AuthorityType.CLASSIFICATION else "index_membership"
    return digest_named_fields(
        f"{authority_type.value}_candidate_hash_v1",
        {
            "authority_type": authority_type.value,
            "authority_receipt_hash": receipt.receipt_hash,
            "row_hashes": sorted(interval.row_hash for interval in intervals),
            "frozen_denominator": receipt.frozen_denominator,
            "denominator_digest": receipt.denominator_digest,
            "outcome": dict(preflight_report.get(section) or {}),
            "unavailable_by_reason": dict(preflight_report.get("unavailable_by_reason") or {}),
        },
    )


def _validate_preflight_report(report: Mapping[str, Any]) -> None:
    if report.get("schema_version") != PREFLIGHT_REPORT_SCHEMA:
        raise IndustryPitContractError("preflight report schema is invalid")
    payload = dict(report)
    observed_hash = str(payload.pop("canonical_hash", ""))
    expected_hash = digest_named_fields(PREFLIGHT_REPORT_SCHEMA, payload)
    if observed_hash != expected_hash:
        raise IndustryPitContractError("preflight canonical hash mismatch")
    total = report.get("total_opportunities")
    if type(total) is not int or total <= 0:
        raise IndustryPitContractError("preflight denominator is invalid")
    classification = report.get("classification")
    index_membership = report.get("index_membership")
    closure = report.get("closure")
    if not all(isinstance(value, Mapping) for value in (classification, index_membership, closure)):
        raise IndustryPitContractError("preflight closure payload is invalid")
    count_values = (
        classification.get("resolved", 0),
        classification.get("unavailable", 0),
        index_membership.get("resolved", 0),
        index_membership.get("unavailable", 0),
    )
    if any(type(value) is not int or value < 0 for value in count_values):
        raise IndustryPitContractError("preflight outcome counts are invalid")
    classification_total = count_values[0] + count_values[1]
    index_total = count_values[2] + count_values[3]
    if (
        classification_total != total
        or index_total != total
        or closure.get("classification_resolved_plus_unavailable") != total
        or closure.get("index_resolved_plus_unavailable") != total
        or closure.get("expected_denominator") != total
        or closure.get("passed") is not True
    ):
        raise IndustryPitContractError("preflight denominator closure mismatch")


def _validate_authority_bundle(
    *,
    catalog: TaxonomyCatalog,
    classification_receipt: AuthorityReceipt,
    index_membership_receipt: AuthorityReceipt,
    classification_intervals: Sequence[CandidateInterval],
    index_membership_intervals: Sequence[CandidateInterval],
    preflight_report: Mapping[str, Any],
) -> None:
    _validate_preflight_report(preflight_report)
    if classification_receipt.authority_type is not AuthorityType.CLASSIFICATION:
        raise IndustryPitContractError("classification receipt authority mismatch")
    if index_membership_receipt.authority_type is not AuthorityType.INDEX_MEMBERSHIP:
        raise IndustryPitContractError("index membership receipt authority mismatch")
    receipts = (classification_receipt, index_membership_receipt)
    if any(
        receipt.taxonomy_contract_id != catalog.contract_id
        or receipt.taxonomy_version != catalog.version
        or catalog.source_sha256 not in receipt.source_hashes
        for receipt in receipts
    ):
        raise IndustryPitContractError("authority receipt and taxonomy catalog mismatch")
    if (
        classification_receipt.frozen_denominator != index_membership_receipt.frozen_denominator
        or classification_receipt.denominator_digest != index_membership_receipt.denominator_digest
        or classification_receipt.frozen_denominator != preflight_report.get("total_opportunities")
        or classification_receipt.denominator_digest != preflight_report.get("denominator_digest")
    ):
        raise IndustryPitContractError("authority receipt denominator mismatch")
    for authority, receipt, intervals in (
        (AuthorityType.CLASSIFICATION, classification_receipt, classification_intervals),
        (AuthorityType.INDEX_MEMBERSHIP, index_membership_receipt, index_membership_intervals),
    ):
        for interval in intervals:
            if (
                interval.authority_type is not authority
                or interval.authority_receipt_hash != receipt.receipt_hash
                or interval.taxonomy_contract_id != catalog.contract_id
                or interval.taxonomy_version != catalog.version
            ):
                raise IndustryPitContractError("candidate row authority/readback mismatch")


def write_candidate_bundle(
    *,
    artifact_root: Path,
    forbidden_roots: Sequence[Path],
    taxonomy_catalog: Mapping[str, Any],
    classification_receipt: AuthorityReceipt,
    index_membership_receipt: AuthorityReceipt,
    classification_intervals: Sequence[CandidateInterval],
    index_membership_intervals: Sequence[CandidateInterval],
    preflight_report: Mapping[str, Any],
    producer_commit: str,
    producer_tree: str,
) -> CandidateBundleReadback:
    target = require_repo_external_root(artifact_root, forbidden_roots=forbidden_roots)
    if target.exists():
        raise IndustryPitContractError(f"refusing to overwrite candidate artifact root: {target}")
    if not _GIT_OBJECT_RE.fullmatch(str(producer_commit)) or not _GIT_OBJECT_RE.fullmatch(str(producer_tree)):
        raise IndustryPitContractError("candidate producer commit/tree identity is invalid")
    catalog = taxonomy_catalog_from_mapping(taxonomy_catalog)
    _validate_authority_bundle(
        catalog=catalog,
        classification_receipt=classification_receipt,
        index_membership_receipt=index_membership_receipt,
        classification_intervals=classification_intervals,
        index_membership_intervals=index_membership_intervals,
        preflight_report=preflight_report,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.tmp-{uuid.uuid4().hex}"
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        catalog_path = temporary / "taxonomy_catalog.json"
        classification_receipt_path = temporary / "classification_authority_receipt.json"
        index_receipt_path = temporary / "index_membership_authority_receipt.json"
        classification_path = temporary / "classification_candidate.jsonl"
        index_path = temporary / "index_membership_candidate.jsonl"
        report_path = temporary / "full_denominator_preflight.json"
        catalog_observation = _write_canonical_json(catalog_path, catalog.as_dict())
        classification_receipt_observation = _write_canonical_json(
            classification_receipt_path,
            {**classification_receipt.as_dict(), "receipt_hash": classification_receipt.receipt_hash},
        )
        index_receipt_observation = _write_canonical_json(
            index_receipt_path,
            {**index_membership_receipt.as_dict(), "receipt_hash": index_membership_receipt.receipt_hash},
        )
        classification_observation = _write_jsonl(
            classification_path,
            (value.as_dict() for value in classification_intervals),
        )
        index_observation = _write_jsonl(
            index_path,
            (value.as_dict() for value in index_membership_intervals),
        )
        report_observation = _write_canonical_json(report_path, preflight_report)

        classification_hash = _candidate_hash(
            authority_type=AuthorityType.CLASSIFICATION,
            receipt=classification_receipt,
            intervals=classification_intervals,
            preflight_report=preflight_report,
        )
        index_hash = _candidate_hash(
            authority_type=AuthorityType.INDEX_MEMBERSHIP,
            receipt=index_membership_receipt,
            intervals=index_membership_intervals,
            preflight_report=preflight_report,
        )
        if classification_hash == index_hash:
            raise IndustryPitContractError("classification and index candidate hashes must differ")
        bundle_hash = digest_named_fields(
            CANDIDATE_BUNDLE_SCHEMA,
            {
                "taxonomy_catalog_hash": catalog.catalog_hash,
                "classification_candidate_hash": classification_hash,
                "index_membership_candidate_hash": index_hash,
                "preflight_canonical_hash": preflight_report.get("canonical_hash"),
                "producer_commit": producer_commit,
                "producer_tree": producer_tree,
            },
        )
        manifest = {
            "schema_version": CANDIDATE_BUNDLE_SCHEMA,
            "classification_authority_type": AuthorityType.CLASSIFICATION.value,
            "index_membership_authority_type": AuthorityType.INDEX_MEMBERSHIP.value,
            "classification_candidate_hash": classification_hash,
            "index_membership_candidate_hash": index_hash,
            "bundle_hash": bundle_hash,
            "producer_commit": producer_commit,
            "producer_tree": producer_tree,
            "files": {
                "taxonomy_catalog.json": catalog_observation.manifest_entry(),
                "classification_authority_receipt.json": (
                    classification_receipt_observation.manifest_entry()
                ),
                "index_membership_authority_receipt.json": (
                    index_receipt_observation.manifest_entry()
                ),
                "classification_candidate.jsonl": classification_observation.manifest_entry(),
                "index_membership_candidate.jsonl": index_observation.manifest_entry(),
                "full_denominator_preflight.json": report_observation.manifest_entry(),
            },
        }
        _write_canonical_json(temporary / "candidate_bundle_manifest.json", manifest)
        os.replace(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return read_candidate_bundle(artifact_root=target, forbidden_roots=forbidden_roots)


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IndustryPitContractError(f"candidate readback failed for {path.name}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise IndustryPitContractError(f"candidate readback payload must be an object: {path.name}")
    return payload


def _read_intervals(path: Path) -> tuple[CandidateInterval, ...]:
    output: list[CandidateInterval] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    raise IndustryPitContractError(f"blank JSONL row at {path.name}:{line_number}")
                output.append(candidate_interval_from_mapping(json.loads(line)))
    except (OSError, json.JSONDecodeError) as exc:
        raise IndustryPitContractError(f"candidate JSONL readback failed: {path.name}: {exc}") from exc
    return tuple(output)


def read_candidate_bundle(
    *,
    artifact_root: Path,
    forbidden_roots: Sequence[Path],
) -> CandidateBundleReadback:
    root = require_repo_external_root(artifact_root, forbidden_roots=forbidden_roots)
    manifest = _read_json(root / "candidate_bundle_manifest.json")
    if manifest.get("schema_version") != CANDIDATE_BUNDLE_SCHEMA:
        raise IndustryPitContractError("candidate bundle schema is invalid")
    expected_manifest_keys = {
        "schema_version",
        "classification_authority_type",
        "index_membership_authority_type",
        "classification_candidate_hash",
        "index_membership_candidate_hash",
        "bundle_hash",
        "producer_commit",
        "producer_tree",
        "files",
    }
    if set(manifest) != expected_manifest_keys:
        raise IndustryPitContractError("candidate bundle manifest keys differ from schema")
    if (
        manifest.get("classification_authority_type") != AuthorityType.CLASSIFICATION.value
        or manifest.get("index_membership_authority_type") != AuthorityType.INDEX_MEMBERSHIP.value
    ):
        raise IndustryPitContractError("candidate bundle authority types are invalid")
    files = manifest.get("files")
    if not isinstance(files, Mapping):
        raise IndustryPitContractError("candidate bundle file manifest is missing")
    expected_names = {
        "taxonomy_catalog.json",
        "classification_authority_receipt.json",
        "index_membership_authority_receipt.json",
        "classification_candidate.jsonl",
        "index_membership_candidate.jsonl",
        "full_denominator_preflight.json",
    }
    if set(files) != expected_names:
        raise IndustryPitContractError("candidate bundle file set is invalid")
    actual_names = {path.name for path in root.iterdir() if path.is_file()}
    if actual_names != {*expected_names, "candidate_bundle_manifest.json"}:
        raise IndustryPitContractError("candidate bundle directory file set is invalid")
    for name in sorted(expected_names):
        path = root / name
        entry = files[name]
        if not path.is_file() or not isinstance(entry, Mapping):
            raise IndustryPitContractError(f"candidate bundle file is missing: {name}")
        expected_entry_keys = {"sha256", "size_bytes"}
        if name.endswith("_candidate.jsonl"):
            expected_entry_keys.add("row_count")
        if set(entry) != expected_entry_keys:
            raise IndustryPitContractError(f"candidate bundle file entry differs from schema: {name}")
        if type(entry.get("size_bytes")) is not int or entry["size_bytes"] < 0:
            raise IndustryPitContractError(f"candidate bundle file size is invalid: {name}")
        if "row_count" in entry and (type(entry.get("row_count")) is not int or entry["row_count"] < 0):
            raise IndustryPitContractError(f"candidate bundle row count is invalid: {name}")
        observation = _observe_file(path)
        if observation.sha256 != entry.get("sha256") or observation.size_bytes != entry.get("size_bytes"):
            raise IndustryPitContractError(
                f"{UnavailableReason.WRITER_READBACK_HASH_MISMATCH.value}: {name}"
            )
    catalog_payload = _read_json(root / "taxonomy_catalog.json")
    catalog = taxonomy_catalog_from_mapping(catalog_payload)
    classification_receipt = authority_receipt_from_mapping(
        _read_json(root / "classification_authority_receipt.json")
    )
    index_receipt = authority_receipt_from_mapping(_read_json(root / "index_membership_authority_receipt.json"))
    classification = _read_intervals(root / "classification_candidate.jsonl")
    index_membership = _read_intervals(root / "index_membership_candidate.jsonl")
    if len(classification) != files["classification_candidate.jsonl"].get("row_count"):
        raise IndustryPitContractError("classification candidate row count mismatch")
    if len(index_membership) != files["index_membership_candidate.jsonl"].get("row_count"):
        raise IndustryPitContractError("index membership candidate row count mismatch")
    report = _read_json(root / "full_denominator_preflight.json")
    _validate_authority_bundle(
        catalog=catalog,
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=classification,
        index_membership_intervals=index_membership,
        preflight_report=report,
    )
    classification_hash = _candidate_hash(
        authority_type=AuthorityType.CLASSIFICATION,
        receipt=classification_receipt,
        intervals=classification,
        preflight_report=report,
    )
    index_hash = _candidate_hash(
        authority_type=AuthorityType.INDEX_MEMBERSHIP,
        receipt=index_receipt,
        intervals=index_membership,
        preflight_report=report,
    )
    bundle_hash = digest_named_fields(
        CANDIDATE_BUNDLE_SCHEMA,
        {
            "taxonomy_catalog_hash": catalog.catalog_hash,
            "classification_candidate_hash": classification_hash,
            "index_membership_candidate_hash": index_hash,
            "preflight_canonical_hash": report.get("canonical_hash"),
            "producer_commit": manifest.get("producer_commit"),
            "producer_tree": manifest.get("producer_tree"),
        },
    )
    if not _GIT_OBJECT_RE.fullmatch(str(manifest.get("producer_commit") or "")) or not _GIT_OBJECT_RE.fullmatch(
        str(manifest.get("producer_tree") or "")
    ):
        raise IndustryPitContractError("candidate producer identity is invalid on readback")
    if (
        classification_hash != manifest.get("classification_candidate_hash")
        or index_hash != manifest.get("index_membership_candidate_hash")
        or bundle_hash != manifest.get("bundle_hash")
    ):
        raise IndustryPitContractError(UnavailableReason.WRITER_READBACK_HASH_MISMATCH.value)
    return CandidateBundleReadback(
        artifact_root=root,
        manifest=manifest,
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=classification,
        index_membership_intervals=index_membership,
        preflight_report=report,
    )


__all__ = [
    "CandidateBundleReadback",
    "read_candidate_bundle",
    "require_repo_external_root",
    "write_candidate_bundle",
]
