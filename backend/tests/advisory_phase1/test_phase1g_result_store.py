from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from backend.services.advisory_phase1.phase1g_contract import (
    REASON_ATTEMPT_RECEIPT_STORE_FAILED,
    REASON_RESULT_STORE_FAILED,
    Phase1GAttemptReceipt,
    Phase1GAttemptStatus,
    Phase1GBatchAttemptReceipt,
    Phase1GBatchStatus,
    Phase1GOutputArtifactRef,
)
from backend.services.advisory_phase1.phase1g_result_store import (
    Phase1GResultStore,
    Phase1GResultStoreError,
)
from backend.tests.advisory_phase1.phase1g_test_support import capture_result, h


def _attempt(*, result_ref, result_hash: str) -> Phase1GAttemptReceipt:  # type: ignore[no-untyped-def]
    return Phase1GAttemptReceipt(
        target_plan_hash=h("1"),
        target_request_hash=h("2"),
        attempt_invocation_id="invocation-a",
        started_at=datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
        finished_at=datetime(2026, 7, 15, 2, 1, tzinfo=UTC),
        operation_status=Phase1GAttemptStatus.SUCCESS,
        dml_executed=True,
        committed_phases=("CAPTURE_BATCH", "OBSERVATION"),
        capture_batch_id="capture-a",
        capture_attempt_no=1,
        capture_batch_status="COMPLETE",
        capture_result_ref=result_ref,
        capture_result_hash=result_hash,
    )


def test_result_store_publishes_canonical_artifacts_and_exact_retry_is_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "phase1g-results"
    root.mkdir()
    store = Phase1GResultStore(root=root)
    result = capture_result()

    first = store.publish_result(result)
    second = store.publish_result(result)

    assert first.idempotent is False
    assert second.idempotent is True
    assert first.path == second.path
    assert first.ref == second.ref
    assert first.ref.relative_path == f"results/{str(result.capture_result_hash)[:2]}/{result.capture_result_hash}.json"
    assert store.load(first.ref) == result


def test_result_store_separates_stable_result_attempt_and_batch_receipts(tmp_path: Path) -> None:
    root = tmp_path / "phase1g-results"
    root.mkdir()
    store = Phase1GResultStore(root=root)
    result = capture_result()
    result_artifact = store.publish_result(result)
    attempt = _attempt(result_ref=result_artifact.ref, result_hash=str(result.capture_result_hash))
    attempt_artifact = store.publish_attempt(attempt)
    batch = Phase1GBatchAttemptReceipt(
        batch_request_hash=h("3"),
        batch_plan_hash=h("4"),
        target_count=1,
        succeeded_count=1,
        failed_count=0,
        target_attempt_receipt_hashes=(str(attempt.attempt_receipt_hash),),
        successful_capture_result_hashes=(str(result.capture_result_hash),),
        batch_status=Phase1GBatchStatus.SUCCESS,
    )
    batch_artifact = store.publish_batch(batch)

    assert result_artifact.path.parent.parent.name == "results"
    assert attempt_artifact.path.parent.parent.name == "attempts"
    assert batch_artifact.path.parent.parent.name == "batches"
    assert store.load(attempt_artifact.ref) == attempt
    assert store.load(batch_artifact.ref) == batch


def test_result_store_rejects_identity_collision_and_tampered_readback(tmp_path: Path) -> None:
    root = tmp_path / "phase1g-results"
    root.mkdir()
    store = Phase1GResultStore(root=root)
    result = capture_result()
    artifact = store.publish_result(result)
    artifact.path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(Phase1GResultStoreError) as collision:
        store.publish_result(result)
    assert collision.value.reason_code == REASON_RESULT_STORE_FAILED

    with pytest.raises(Phase1GResultStoreError) as tamper:
        store.load(artifact.ref)
    assert tamper.value.reason_code == REASON_RESULT_STORE_FAILED


def test_result_store_rejects_ref_policy_path_and_noncanonical_bytes(tmp_path: Path) -> None:
    root = tmp_path / "phase1g-results"
    root.mkdir()
    store = Phase1GResultStore(root=root)
    artifact = store.publish_result(capture_result())
    payload = artifact.ref.model_dump(mode="json")
    payload["store_policy_hash"] = h("0")
    with pytest.raises(Phase1GResultStoreError, match="store policy"):
        store.load(Phase1GOutputArtifactRef.model_validate(payload))

    payload = artifact.ref.model_dump(mode="json")
    payload["relative_path"] = "results/wrong.json"
    with pytest.raises(Phase1GResultStoreError, match="path"):
        store.load(Phase1GOutputArtifactRef.model_validate(payload))

    document = json.loads(artifact.path.read_text(encoding="utf-8"))
    noncanonical = (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    artifact.path.write_bytes(noncanonical)
    payload = artifact.ref.model_dump(mode="json")
    payload["file_sha256"] = hashlib.sha256(noncanonical).hexdigest()
    with pytest.raises(Phase1GResultStoreError, match="not canonical JSON"):
        store.load(Phase1GOutputArtifactRef.model_validate(payload))


def test_attempt_store_failure_keeps_attempt_specific_reason_code(tmp_path: Path) -> None:
    root = tmp_path / "phase1g-results"
    root.mkdir()
    store = Phase1GResultStore(root=root)
    result = capture_result()
    result_artifact = store.publish_result(result)
    attempt = _attempt(result_ref=result_artifact.ref, result_hash=str(result.capture_result_hash))
    artifact = store.publish_attempt(attempt)
    artifact.path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(Phase1GResultStoreError) as error:
        store.publish_attempt(attempt)
    assert error.value.reason_code == REASON_ATTEMPT_RECEIPT_STORE_FAILED


def test_result_store_rejects_repository_root_and_creates_explicit_external_root(tmp_path: Path) -> None:
    with pytest.raises(Phase1GResultStoreError) as repository:
        Phase1GResultStore(root=Path("backend").resolve())
    assert repository.value.reason_code == REASON_RESULT_STORE_FAILED

    missing = tmp_path / "not-created"
    store = Phase1GResultStore(root=missing)
    assert store.root == missing.resolve()
    assert missing.is_dir()
