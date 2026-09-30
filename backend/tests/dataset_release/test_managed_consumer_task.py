from __future__ import annotations

from copy import deepcopy
from datetime import date
from pathlib import Path

import pytest

from backend.services.dataset_release.managed_consumer_task import (
    ManagedDatasetTaskError,
    ManagedDatasetTaskStore,
    require_managed_dataset_task_request,
)


def _resolved(*, consumer_id: str, node_id: str, root: str) -> dict:
    return {
        "schema_version": "aistock_active_dataset_consumer_binding_v1",
        "consumer_id": consumer_id,
        "node_id": node_id,
        "generation": "20260930-monthly-v2-unified",
        "release_id": "qe_hmm_full_v2_20260930",
        "cutoff": "2026-09-30",
        "dataset_manifest_sha256": "b" * 64,
        "profile_sha256": "a" * 64,
        "candidate_root": root,
        "required_components": (
            ["day", "factor", "manifest", "stock_pools"]
            if consumer_id == "position_timing"
            else ["day", "factor", "index", "manifest", "minute", "stock_pools", "suspend"]
        ),
        "derived_asset_registry_sha256": "c" * 64,
        "derived_asset_registry_path": root + "/derived/registry.json",
        "release_closure_sha256": "d" * 64,
        "release_closure_path": root + "/release_closure_receipt.json",
        "derived_assets": [],
        "resolved_once": True,
        "legacy_fallback": False,
    }


def test_create_is_canonical_and_retry_does_not_reresolve_active(tmp_path: Path) -> None:
    store = ManagedDatasetTaskStore(tmp_path)
    calls = 0

    def resolver(**kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return _resolved(**kwargs, root="C:/datasets/r9")

    first = store.create(
        consumer_id="position_timing",
        business_task_key="PT-NEXT-030",
        start_date=date(2018, 8, 1),
        end_date=date(2026, 9, 30),
        resolver=resolver,
    )
    second = store.create(
        consumer_id="position_timing",
        business_task_key="PT-NEXT-030",
        start_date=date(2018, 8, 1),
        end_date=date(2026, 9, 30),
        resolver=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry must not resolve the new active profile")
        ),
    )

    assert first == second
    assert calls == 1
    assert first.value["dataset_binding"]["dataset_manifest_sha256"] == "b" * 64
    assert first.value["database_read_performed"] is False
    assert first.value["database_write_performed"] is False
    assert first.value["candidate_write_performed"] is False
    assert first.value["profile_write_performed"] is False
    assert first.value["outcomes_read"] is False
    assert first.path.read_bytes().endswith(b"\n")


def test_same_business_key_rejects_window_drift(tmp_path: Path) -> None:
    store = ManagedDatasetTaskStore(tmp_path)
    store.create(
        consumer_id="advisory",
        business_task_key="ADV-OFFLINE-001",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 9, 30),
        resolver=lambda **kwargs: _resolved(**kwargs, root="C:/datasets/r9"),
    )

    with pytest.raises(ManagedDatasetTaskError, match="another requested window"):
        store.create(
            consumer_id="advisory",
            business_task_key="ADV-OFFLINE-001",
            start_date=date(2026, 2, 1),
            end_date=date(2026, 9, 30),
            resolver=lambda **kwargs: _resolved(**kwargs, root="C:/datasets/r10"),
        )


def test_request_rejects_binding_tampering_and_unknown_consumer(tmp_path: Path) -> None:
    store = ManagedDatasetTaskStore(tmp_path)
    artifact = store.create(
        consumer_id="position_timing",
        business_task_key="PT-NEXT-031",
        start_date=date(2024, 7, 1),
        end_date=date(2026, 9, 30),
        resolver=lambda **kwargs: _resolved(**kwargs, root="C:/datasets/r9"),
    )
    tampered = deepcopy(artifact.value)
    tampered["dataset_binding"]["binding"]["candidate_root"] = "C:/datasets/drift"

    with pytest.raises(ManagedDatasetTaskError, match="binding is invalid"):
        require_managed_dataset_task_request(tampered)
    with pytest.raises(ManagedDatasetTaskError, match="not a managed offline preparation"):
        store.create(
            consumer_id="qe_single",
            business_task_key="QE-001",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 9, 30),
        )


def test_request_rejects_window_beyond_frozen_cutoff(tmp_path: Path) -> None:
    store = ManagedDatasetTaskStore(tmp_path)

    with pytest.raises(ManagedDatasetTaskError, match="exceeds the frozen release cutoff"):
        store.create(
            consumer_id="advisory",
            business_task_key="ADV-OFFLINE-002",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 10, 1),
            resolver=lambda **kwargs: _resolved(**kwargs, root="C:/datasets/r9"),
        )

    assert list(tmp_path.iterdir()) == []


def test_concurrent_first_writer_wins_without_rebinding_retry(tmp_path: Path) -> None:
    store = ManagedDatasetTaskStore(tmp_path)
    window = {
        "consumer_id": "advisory",
        "business_task_key": "ADV-OFFLINE-RACE",
        "start_date": date(2026, 1, 1),
        "end_date": date(2026, 9, 30),
    }

    def late_resolver(**kwargs):  # type: ignore[no-untyped-def]
        winner = store.create(
            **window,
            resolver=lambda **inner: _resolved(**inner, root="C:/datasets/winner"),
        )
        assert winner.value["dataset_binding"]["binding"]["candidate_root"].endswith(
            "/winner"
        )
        return _resolved(**kwargs, root="C:/datasets/loser")

    raced = store.create(**window, resolver=late_resolver)

    assert raced.value["dataset_binding"]["binding"]["candidate_root"] == (
        "C:/datasets/winner"
    )
