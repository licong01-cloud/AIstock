from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_historical_range.artifact_store import (
    ARTIFACT_ROOT_ENV,
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.models import (
    REASON_ARTIFACT_COLLISION,
    REASON_ARTIFACT_ROOT_INVALID,
    REASON_ARTIFACT_TAMPERED,
    HistoricalRangeArtifactKind,
    HistoricalRangeContractError,
    HistoricalRangeSourceRevisionRefV1,
)
from backend.tests.advisory_historical_range.conftest import resolved_request


def test_environment_root_is_explicit_and_absolute(tmp_path: Path) -> None:
    with pytest.raises(HistoricalRangeContractError) as missing:
        HistoricalRangeArtifactStore.from_environment(environ={})
    assert missing.value.reason_code == REASON_ARTIFACT_ROOT_INVALID

    with pytest.raises(HistoricalRangeContractError) as relative:
        HistoricalRangeArtifactStore.from_environment(environ={ARTIFACT_ROOT_ENV: "relative/artifacts"})
    assert relative.value.reason_code == REASON_ARTIFACT_ROOT_INVALID

    store = HistoricalRangeArtifactStore.from_environment(environ={ARTIFACT_ROOT_ENV: str(tmp_path / "phase1r")})
    assert store.root.is_absolute()
    assert len(store.root_identity_hash) == 64


def test_root_inside_repository_is_rejected() -> None:
    repository_root = Path(__file__).resolve().parents[3]
    with pytest.raises(HistoricalRangeContractError) as exc_info:
        HistoricalRangeArtifactStore(root=repository_root / ".phase1r-artifacts")
    assert exc_info.value.reason_code == REASON_ARTIFACT_ROOT_INVALID


def test_publish_load_and_exact_retry_are_content_addressed(tmp_path: Path) -> None:
    resolved = resolved_request()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    first = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="phase1r_r1",
        payload_schema_version=resolved.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=resolved.model_dump(mode="json"),
        source_revision_refs=(
            HistoricalRangeSourceRevisionRefV1(
                revision_id="source-b", revision_hash=resolved.source_revision_catalog_hash
            ),
            HistoricalRangeSourceRevisionRefV1(
                revision_id="source-a", revision_hash=resolved.source_revision_catalog_hash
            ),
        ),
    )
    second = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="phase1r_r1",
        payload_schema_version=resolved.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=resolved.model_dump(mode="json"),
        source_revision_refs=(
            HistoricalRangeSourceRevisionRefV1(
                revision_id="source-a", revision_hash=resolved.source_revision_catalog_hash
            ),
            HistoricalRangeSourceRevisionRefV1(
                revision_id="source-b", revision_hash=resolved.source_revision_catalog_hash
            ),
        ),
    )

    assert first.idempotent is False
    assert second.idempotent is True
    assert first.ref == second.ref
    loaded = store.load(first.ref)
    assert loaded.payload == resolved.model_dump(mode="json")
    assert first.path == store.root / "requests" / f"{first.ref.semantic_content_hash}.json"


def test_existing_same_identity_with_different_bytes_is_collision(tmp_path: Path) -> None:
    resolved = resolved_request()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    stored = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="phase1r_r1",
        payload_schema_version=resolved.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=resolved.model_dump(mode="json"),
    )
    stored.path.write_bytes(b"{}\n")

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.REQUEST,
            producer_contract_version="phase1r_r1",
            payload_schema_version=resolved.schema_version,
            resolved_request_hash=resolved.request_payload_sha256,
            payload=resolved.model_dump(mode="json"),
        )
    assert exc_info.value.reason_code == REASON_ARTIFACT_COLLISION


def test_load_detects_raw_file_tamper_and_ref_path_tamper(tmp_path: Path) -> None:
    resolved = resolved_request()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    stored = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DATE_PLAN,
        producer_contract_version="phase1r_r1",
        payload_schema_version=resolved.date_plan.schema_version,
        resolved_request_hash=resolved.request_payload_sha256,
        payload=resolved.date_plan.model_dump(mode="json"),
    )
    stored.path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(HistoricalRangeContractError) as tamper:
        store.load(stored.ref)
    assert tamper.value.reason_code == REASON_ARTIFACT_TAMPERED

    wrong_ref = stored.ref.model_copy(update={"relative_path": "date-plans/not-the-hash.json"})
    with pytest.raises(HistoricalRangeContractError) as path_tamper:
        store.load(wrong_ref)
    assert path_tamper.value.reason_code == REASON_ARTIFACT_TAMPERED
