from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.services.advisory_phase1.readiness_plan_store import (
    ContentAddressedPlanStore,
    Phase1EArtifactStoreError,
    REASON_ARTIFACT_STORE_CONFIG_INVALID,
    REASON_PLAN_ARTIFACT_CONFLICT,
)


HASH = "a" * 64


def test_store_exact_retry_and_full_readback(tmp_path) -> None:
    store = ContentAddressedPlanStore(root=tmp_path, policy_hash=HASH)
    first = store.publish(kind="plan", identity=HASH, payload={"plan": "same"}, semantic_hash=HASH)
    second = store.publish(kind="plan", identity=HASH, payload={"plan": "same"}, semantic_hash=HASH)

    assert first["semantic_hash"] == second["semantic_hash"] == HASH
    assert first["file_sha256"] == second["file_sha256"]
    assert store.verify(kind="plan", identity=HASH, semantic_hash=HASH)["payload"] == {"plan": "same"}


def test_store_rejects_existing_path_with_different_payload(tmp_path) -> None:
    store = ContentAddressedPlanStore(root=tmp_path, policy_hash=HASH)
    store.publish(kind="plan", identity=HASH, payload={"plan": "first"}, semantic_hash=HASH)

    with pytest.raises(Phase1EArtifactStoreError) as error:
        store.publish(kind="plan", identity=HASH, payload={"plan": "different"}, semantic_hash=HASH)

    assert error.value.reason_code == REASON_PLAN_ARTIFACT_CONFLICT


def test_two_writers_converge_without_overwrite(tmp_path) -> None:
    store = ContentAddressedPlanStore(root=tmp_path, policy_hash=HASH)

    def publish() -> str:
        return store.publish(kind="plan", identity=HASH, payload={"plan": "same"}, semantic_hash=HASH)["file_sha256"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        hashes = list(executor.map(lambda _: publish(), range(2)))

    assert hashes[0] == hashes[1]


def test_store_rejects_wsl_unc_root_before_any_write() -> None:
    with pytest.raises(Phase1EArtifactStoreError) as error:
        ContentAddressedPlanStore(root=Path(r"\\wsl$\Ubuntu\home\aistock-artifacts"), policy_hash=HASH)

    assert error.value.reason_code == REASON_ARTIFACT_STORE_CONFIG_INVALID
