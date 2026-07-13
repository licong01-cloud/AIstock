"""Real local CAS contracts for Phase 1C-3 calculation evidence."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import hashlib
import os

import pytest

from backend.services.advisory_phase1.calculation_evidence import (
    LOCAL_CALCULATION_EVIDENCE_STORE_SCHEMA_VERSION,
    REASON_CAS_CONTENT_CONFLICT,
    REASON_STORE_INVALID,
    CalculationEvidenceStoreError,
    LocalCalculationEvidenceStore,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.label_builder_postgres import _same_calculation_evidence
from backend.services.advisory_phase1.outcome_engine import CalculationEvidenceBundle, OwnerType


def _bundle(value: str = "fixture") -> CalculationEvidenceBundle:
    return CalculationEvidenceBundle(evidence_payload={"owner": "fixture", "value": value})


def test_canonical_evidence_comparison_accepts_json_type_normalization() -> None:
    typed = CalculationEvidenceBundle(
        evidence_payload={
            "owner_type": OwnerType.CANDIDATE,
            "trade_date": date(2026, 7, 3),
            "price": Decimal("10.500000000000"),
        }
    )
    restored = CalculationEvidenceBundle.model_validate_json(typed.canonical_bytes())

    assert typed != restored
    assert typed.evidence_hash == restored.evidence_hash
    assert _same_calculation_evidence(typed, restored)


def _store_identity() -> dict[str, str]:
    return {
        "backend": "LOCAL_FILESYSTEM_V1",
        "durability_mode": (
            "WINDOWS_FILE_AND_DIRECTORY_FLUSH_V1"
            if os.name == "nt"
            else "POSIX_FILE_AND_DIRECTORY_FSYNC_V1"
        ),
        "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
    }


def test_local_cas_writes_real_bytes_and_exact_retry_returns_same_blob(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = LocalCalculationEvidenceStore(
        root=tmp_path / "outside-repository-evidence",
        repository_root=tmp_path / "repository",
        store_identity=_store_identity(),
    )
    bundle = _bundle()

    first = store.put(bundle)
    second = store.put(bundle)

    assert first == second
    target = tmp_path / "outside-repository-evidence" / "blobs" / "sha256" / first.sha256[:2] / first.sha256
    assert target.read_bytes() == bundle.canonical_bytes()
    assert hashlib.sha256(target.read_bytes()).hexdigest() == first.sha256
    assert store.get(
        uri=first.uri,
        sha256=first.sha256,
        size_bytes=first.size_bytes,
        store_backend_hash=first.store_backend_hash,
    ) == bundle


def test_local_cas_reader_rejects_uri_outside_the_content_addressed_root(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = LocalCalculationEvidenceStore(
        root=tmp_path / "outside-repository-evidence",
        repository_root=tmp_path / "repository",
        store_identity=_store_identity(),
    )
    stored = store.put(_bundle())
    outside = tmp_path / "outside.json"
    outside.write_bytes(_bundle().canonical_bytes())

    with pytest.raises(CalculationEvidenceStoreError, match=REASON_STORE_INVALID):
        store.get(
            uri=outside.as_uri(),
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
            store_backend_hash=stored.store_backend_hash,
        )


def test_local_cas_reader_rejects_non_blob_uri_inside_store(tmp_path) -> None:  # type: ignore[no-untyped-def]
    identity = _store_identity()
    evidence_store = LocalCalculationEvidenceStore(
        root=tmp_path / "outside-repository-evidence",
        repository_root=tmp_path / "repository",
        store_identity=identity,
    )
    raw_store = LocalContentAddressedStore(
        root=tmp_path / "outside-repository-evidence",
        repository_root=tmp_path / "repository",
        store_identity=identity,
        schema_version=LOCAL_CALCULATION_EVIDENCE_STORE_SCHEMA_VERSION,
    )
    bundle = _bundle("wrong-kind")
    document = raw_store.put_document_bytes(kind="manifests", payload=bundle.canonical_bytes())

    with pytest.raises(CalculationEvidenceStoreError, match=REASON_STORE_INVALID):
        evidence_store.get(
            uri=document.uri,
            sha256=document.sha256,
            size_bytes=document.size_bytes,
            store_backend_hash=document.store_backend_hash,
        )


def test_existing_hash_path_with_different_bytes_is_a_conflict(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = LocalCalculationEvidenceStore(
        root=tmp_path / "outside-repository-evidence",
        repository_root=tmp_path / "repository",
        store_identity=_store_identity(),
    )
    bundle = _bundle()
    stored = store.put(bundle)
    target = tmp_path / "outside-repository-evidence" / "blobs" / "sha256" / stored.sha256[:2] / stored.sha256
    target.write_bytes(b"corrupted")

    with pytest.raises(CalculationEvidenceStoreError, match=REASON_CAS_CONTENT_CONFLICT):
        store.put(bundle)


def test_platform_durability_mode_must_be_explicit(tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(CalculationEvidenceStoreError, match="durability"):
        LocalCalculationEvidenceStore(
            root=tmp_path / "outside-repository-evidence",
            repository_root=tmp_path / "repository",
            store_identity={"backend": "LOCAL_FILESYSTEM_V1", "durability_mode": "UNSPECIFIED"},
        )


def test_store_root_inside_repository_is_rejected(tmp_path) -> None:  # type: ignore[no-untyped-def]
    repository = tmp_path / "repository"

    with pytest.raises(CalculationEvidenceStoreError, match="outside the repository"):
        LocalCalculationEvidenceStore(
            root=repository / "evidence",
            repository_root=repository,
            store_identity=_store_identity(),
        )


def test_atomic_publish_failure_leaves_no_authoritative_or_staging_blob(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path / "outside-repository-evidence"
    store = LocalCalculationEvidenceStore(
        root=root,
        repository_root=tmp_path / "repository",
        store_identity=_store_identity(),
    )
    bundle = _bundle()

    def fail_link(source, target) -> None:  # type: ignore[no-untyped-def]
        raise OSError("injected publish failure")

    monkeypatch.setattr(os, "link", fail_link)

    with pytest.raises(CalculationEvidenceStoreError, match=REASON_STORE_INVALID):
        store.put(bundle)

    parent = root / "blobs" / "sha256" / bundle.evidence_hash[:2]
    target = parent / bundle.evidence_hash
    assert not target.exists()
    assert not tuple(parent.glob("*.tmp"))


def test_atomic_publish_mode_must_be_explicit(tmp_path) -> None:  # type: ignore[no-untyped-def]
    identity = _store_identity()
    identity.pop("atomic_publish_mode")

    with pytest.raises(CalculationEvidenceStoreError, match="HARDLINK_CREATE_IF_ABSENT_V1"):
        LocalCalculationEvidenceStore(
            root=tmp_path / "outside-repository-evidence",
            repository_root=tmp_path / "repository",
            store_identity=identity,
        )


def test_store_paths_and_identity_are_fail_closed(tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(CalculationEvidenceStoreError, match="store root"):
        LocalCalculationEvidenceStore(
            root=tmp_path.relative_to(tmp_path.parent),
            repository_root=tmp_path,
            store_identity=_store_identity(),
        )
    with pytest.raises(CalculationEvidenceStoreError, match="repository root"):
        LocalCalculationEvidenceStore(
            root=tmp_path,
            repository_root=tmp_path.relative_to(tmp_path.parent),
            store_identity=_store_identity(),
        )
    with pytest.raises(CalculationEvidenceStoreError, match="identity cannot be empty"):
        LocalCalculationEvidenceStore(
            root=tmp_path,
            repository_root=tmp_path / "repository",
            store_identity={},
        )


def test_bundle_hash_drift_is_rejected_before_publish(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = LocalCalculationEvidenceStore(
        root=tmp_path / "outside-repository-evidence",
        repository_root=tmp_path / "repository",
        store_identity=_store_identity(),
    )
    invalid_bundle = CalculationEvidenceBundle.model_construct(
        evidence_payload={"owner": "fixture"},
        evidence_hash="f" * 64,
        schema_version="advisory_phase1_calculation_evidence_v1",
    )

    with pytest.raises(CalculationEvidenceStoreError, match=REASON_CAS_CONTENT_CONFLICT):
        store.put(invalid_bundle)
