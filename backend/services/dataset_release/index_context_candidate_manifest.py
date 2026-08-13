"""Candidate-local immutable manifest for the QE/HMM index context."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .canonical import digest_named_fields
from .errors import DatasetReleaseError
from .index_contract import (
    HMM_BENCHMARK_CODE,
    INDEX_H5_COLUMNS,
    INDEX_H5_DTYPES,
    INDEX_QLIB_FIELDS,
    index_contract_digest,
    index_contract_payload,
)
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile
from .streaming_artifacts import iter_hdf_frames, sha256_file


INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA = "dataset_release_index_context_candidate_manifest_v1"


class IndexContextCandidateManifestError(DatasetReleaseError):
    code = "BLOCKED_INDEX_CONTEXT_CANDIDATE_MANIFEST_INVALID"


def produce_index_context_candidate_manifest(
    *,
    candidate_root: Path,
    profile: DatasetProfile,
    cutoff: date,
    pit_snapshot: FrozenPitSnapshot,
    release_id: str,
    release_digest: str,
    source_content_root: str,
    artifact_ready_content_root: str,
    producer_fingerprint: str,
    artifact_fingerprint: str,
    validation_fingerprint: str,
    max_rows: int,
) -> Mapping[str, Any]:
    root = Path(candidate_root).resolve(strict=True)
    payload = _observed_payload(root, profile=profile, cutoff=cutoff, max_rows=max_rows)
    manifest = {
        "schema_version": INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA,
        "profile": profile.profile,
        "cutoff": cutoff.isoformat(),
        "release_id": release_id,
        "release_digest": release_digest,
        "source_content_root": source_content_root,
        "artifact_ready_content_root": artifact_ready_content_root,
        "pit_snapshot_digest": pit_snapshot.spans_sha256,
        "producer_fingerprint": producer_fingerprint,
        "artifact_fingerprint": artifact_fingerprint,
        "validation_fingerprint": validation_fingerprint,
        "index_contract": index_contract_payload(),
        "index_contract_digest": index_contract_digest(),
        "ordered_codes": [item.daily_code for item in profile.indices],
        "benchmark": HMM_BENCHMARK_CODE,
        "hmm_consumer_activation": "not_activated",
        **payload,
        "safety": {
            "database_writes": 0,
            "production_writes": 0,
            "production_pointer_changes": 0,
            "service_process_controls": 0,
        },
    }
    manifest["manifest_identity"] = digest_named_fields(INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA, manifest)
    metadata = root / "metadata"
    metadata.mkdir(exist_ok=True)
    path = metadata / "index_context_manifest.json"
    if path.exists():
        existing = validate_index_context_candidate_manifest(
            root,
            profile=profile,
            cutoff=cutoff,
            pit_snapshot_digest=pit_snapshot.spans_sha256,
            producer_fingerprint=producer_fingerprint,
            artifact_fingerprint=artifact_fingerprint,
            validation_fingerprint=validation_fingerprint,
            expected_release_id=release_id,
            expected_release_digest=release_digest,
            expected_source_content_root=source_content_root,
            expected_artifact_ready_content_root=artifact_ready_content_root,
            max_rows=max_rows,
        )
        if existing != manifest:
            raise IndexContextCandidateManifestError("existing index context candidate manifest identity conflicts")
        return existing
    encoded = (
        json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".index_context_manifest.", suffix=".partial", dir=metadata)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    if (
        validate_index_context_candidate_manifest(
            root,
            profile=profile,
            cutoff=cutoff,
            pit_snapshot_digest=pit_snapshot.spans_sha256,
            producer_fingerprint=producer_fingerprint,
            artifact_fingerprint=artifact_fingerprint,
            validation_fingerprint=validation_fingerprint,
            expected_release_id=release_id,
            expected_release_digest=release_digest,
            expected_source_content_root=source_content_root,
            expected_artifact_ready_content_root=artifact_ready_content_root,
            max_rows=max_rows,
        )
        != manifest
    ):
        raise IndexContextCandidateManifestError("index context candidate manifest readback differs")
    return manifest


def validate_index_context_candidate_manifest(
    candidate_root: Path,
    *,
    profile: DatasetProfile,
    cutoff: date,
    pit_snapshot_digest: str,
    producer_fingerprint: str,
    artifact_fingerprint: str,
    validation_fingerprint: str,
    expected_release_id: str | None = None,
    expected_release_digest: str | None = None,
    expected_source_content_root: str | None = None,
    expected_artifact_ready_content_root: str | None = None,
    max_rows: int,
) -> Mapping[str, Any]:
    root = Path(candidate_root).resolve(strict=True)
    path = root / "metadata" / "index_context_manifest.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IndexContextCandidateManifestError("index context candidate manifest is unreadable") from exc
    if not isinstance(value, Mapping):
        raise IndexContextCandidateManifestError("index context candidate manifest is not an object")
    body = dict(value)
    identity = body.pop("manifest_identity", None)
    expected_identity = digest_named_fields(INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA, body)
    if (
        identity != expected_identity
        or body.get("schema_version") != INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA
        or body.get("profile") != profile.profile
        or body.get("cutoff") != cutoff.isoformat()
        or body.get("pit_snapshot_digest") != pit_snapshot_digest
        or body.get("producer_fingerprint") != producer_fingerprint
        or body.get("artifact_fingerprint") != artifact_fingerprint
        or body.get("validation_fingerprint") != validation_fingerprint
        or (expected_release_id is not None and body.get("release_id") != expected_release_id)
        or (expected_release_digest is not None and body.get("release_digest") != expected_release_digest)
        or (
            expected_source_content_root is not None and body.get("source_content_root") != expected_source_content_root
        )
        or (
            expected_artifact_ready_content_root is not None
            and body.get("artifact_ready_content_root") != expected_artifact_ready_content_root
        )
        or body.get("index_contract") != index_contract_payload()
        or body.get("index_contract_digest") != index_contract_digest()
        or body.get("ordered_codes") != [item.daily_code for item in profile.indices]
        or body.get("benchmark") != HMM_BENCHMARK_CODE
        or body.get("hmm_consumer_activation") != "not_activated"
    ):
        raise IndexContextCandidateManifestError("index context candidate manifest identity differs")
    observed = _observed_payload(root, profile=profile, cutoff=cutoff, max_rows=max_rows)
    for field, expected in observed.items():
        if body.get(field) != expected:
            raise IndexContextCandidateManifestError(f"index context candidate manifest file evidence differs: {field}")
    return dict(value)


def _observed_payload(root: Path, *, profile: DatasetProfile, cutoff: date, max_rows: int) -> dict[str, Any]:
    index_root = root / "index_context"
    h5_path = index_root / "index_daily.h5"
    coverage: dict[str, dict[str, Any]] = {}
    digests = {code: hashlib.sha256() for code in profile.index_codes}
    counts = {code: 0 for code in profile.index_codes}
    starts: dict[str, str] = {}
    ends: dict[str, str] = {}
    observed_dtypes: dict[str, str] | None = None
    for frame in iter_hdf_frames(h5_path, chunksize=max_rows):
        actual_columns = tuple(str(value) for value in frame.columns)
        actual_dtypes = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        if actual_columns != INDEX_H5_COLUMNS or actual_dtypes != dict(INDEX_H5_DTYPES):
            raise IndexContextCandidateManifestError("index H5 schema/dtype differs while producing manifest")
        observed_dtypes = actual_dtypes
        for (timestamp, code), row in frame.iterrows():
            instrument = str(code).upper()
            if instrument not in digests:
                raise IndexContextCandidateManifestError("index H5 code exceeds manifest authority")
            day = pd.Timestamp(timestamp).date().isoformat()
            starts.setdefault(instrument, day)
            ends[instrument] = day
            counts[instrument] += 1
            digests[instrument].update(
                json.dumps(
                    [day, *[float(row[field]) for field in INDEX_H5_COLUMNS]],
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("ascii")
                + b"\n"
            )
    for definition in profile.indices:
        code = definition.daily_code
        if counts[code] <= 0 or ends.get(code) != cutoff.isoformat():
            raise IndexContextCandidateManifestError(f"index manifest coverage is incomplete: {code}")
        coverage[code] = {
            "semantic_role": definition.semantic_role,
            "required_from": definition.required_from.isoformat(),
            "weight_api_code": definition.weight_api_code,
            "hmm_benchmark": definition.hmm_benchmark,
            "rows": counts[code],
            "start": starts[code],
            "end": ends[code],
            "ordered_rows_root": digests[code].hexdigest(),
        }
    files: dict[str, dict[str, Any]] = {}
    required = [
        h5_path,
        index_root / "index_context.parquet",
        index_root / "index_materialization_receipt.json",
        root / "daily_bin" / "qlib" / "calendars" / "day.txt",
        root / "daily_bin" / "qlib" / "instruments" / "index.txt",
    ]
    required.extend(index_root / "index_csv" / f"{code}.csv" for code in profile.index_codes)
    required.extend(
        root / "daily_bin" / "qlib" / "features" / code.lower() / f"{field}.day.bin"
        for code in profile.index_codes
        for field in INDEX_QLIB_FIELDS
    )
    for path in required:
        if not path.is_file():
            raise IndexContextCandidateManifestError(f"index candidate manifest file is missing: {path}")
        relative = path.relative_to(root).as_posix()
        files[relative] = {
            "size_bytes": int(path.stat().st_size),
            "sha256": sha256_file(path),
        }
    return {
        "h5_schema": {
            "columns": list(INDEX_H5_COLUMNS),
            "dtypes": observed_dtypes or {},
        },
        "qlib_schema": {"fields": list(INDEX_QLIB_FIELDS), "dtype": "float32"},
        "per_code_coverage": coverage,
        "files": {key: files[key] for key in sorted(files)},
    }


__all__ = [
    "INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA",
    "IndexContextCandidateManifestError",
    "produce_index_context_candidate_manifest",
    "validate_index_context_candidate_manifest",
]
