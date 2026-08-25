"""Repo-external writer/readback for C-013 authority candidates."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields, sha256_hex

from .contracts import (
    CANDIDATE_BUNDLE_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    CandidateInterval,
    IndustryPitContractError,
    UnavailableReason,
    authority_receipt_from_mapping,
    candidate_interval_from_mapping,
)


_GIT_OBJECT_RE = re.compile(r"^[0-9a-f]{40}$")


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


def _write_canonical_json(path: Path, payload: Any) -> None:
    path.write_bytes(canonical_json_bytes(payload) + b"\n")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> tuple[int, str]:
    encoded = sorted(canonical_json_bytes(dict(row)) for row in rows)
    with path.open("wb") as handle:
        for row in encoded:
            handle.write(row)
            handle.write(b"\n")
    return len(encoded), sha256_hex(path.read_bytes())


def _file_entry(path: Path, *, row_count: int | None = None) -> Mapping[str, Any]:
    payload: dict[str, Any] = {
        "sha256": sha256_hex(path.read_bytes()),
        "size_bytes": path.stat().st_size,
    }
    if row_count is not None:
        payload["row_count"] = row_count
    return payload


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
        _write_canonical_json(catalog_path, taxonomy_catalog)
        _write_canonical_json(
            classification_receipt_path,
            {**classification_receipt.as_dict(), "receipt_hash": classification_receipt.receipt_hash},
        )
        _write_canonical_json(
            index_receipt_path,
            {**index_membership_receipt.as_dict(), "receipt_hash": index_membership_receipt.receipt_hash},
        )
        classification_count, _ = _write_jsonl(
            classification_path,
            (value.as_dict() for value in classification_intervals),
        )
        index_count, _ = _write_jsonl(index_path, (value.as_dict() for value in index_membership_intervals))
        _write_canonical_json(report_path, preflight_report)

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
                "taxonomy_catalog_hash": taxonomy_catalog.get("catalog_hash"),
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
                "taxonomy_catalog.json": _file_entry(catalog_path),
                "classification_authority_receipt.json": _file_entry(classification_receipt_path),
                "index_membership_authority_receipt.json": _file_entry(index_receipt_path),
                "classification_candidate.jsonl": _file_entry(
                    classification_path, row_count=classification_count
                ),
                "index_membership_candidate.jsonl": _file_entry(index_path, row_count=index_count),
                "full_denominator_preflight.json": _file_entry(report_path),
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
    for name in sorted(expected_names):
        path = root / name
        entry = files[name]
        if not path.is_file() or not isinstance(entry, Mapping):
            raise IndustryPitContractError(f"candidate bundle file is missing: {name}")
        if sha256_hex(path.read_bytes()) != entry.get("sha256") or path.stat().st_size != entry.get("size_bytes"):
            raise IndustryPitContractError(
                f"{UnavailableReason.WRITER_READBACK_HASH_MISMATCH.value}: {name}"
            )
    catalog = _read_json(root / "taxonomy_catalog.json")
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
            "taxonomy_catalog_hash": catalog.get("catalog_hash"),
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
