from __future__ import annotations

from copy import deepcopy
import json
from typing import Any

import pytest

from backend.services.dataset_release.active_task_binding import (
    FrozenDatasetTaskBindingError,
    freeze_active_dataset_task_binding,
    frozen_dataset_environment,
    require_frozen_dataset_task_binding,
)


PROFILE = "a" * 64
MANIFEST = "b" * 64


def _resolved(*, consumer_id: str, node_id: str, root: str = "/releases/r9") -> dict[str, Any]:
    return {
        "schema_version": "aistock_active_dataset_consumer_binding_v1",
        "consumer_id": consumer_id,
        "node_id": node_id,
        "generation": "20260930-monthly-v2-unified",
        "release_id": "qe_hmm_full_v2_20260930",
        "cutoff": "2026-09-30",
        "dataset_manifest_sha256": MANIFEST,
        "profile_sha256": PROFILE,
        "candidate_root": root,
        "required_components": ["day", "factor", "manifest", "stock_pools"],
        "derived_asset_registry_sha256": "c" * 64,
        "derived_asset_registry_path": root + "/derived/registry.json",
        "release_closure_sha256": "d" * 64,
        "release_closure_path": root + "/release_closure_receipt.json",
        "derived_assets": [],
        "resolved_once": True,
        "legacy_fallback": False,
    }


def test_freeze_and_environment_bind_profile_manifest_and_paths() -> None:
    calls = 0

    def resolver(**kwargs: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _resolved(**kwargs)

    frozen = freeze_active_dataset_task_binding(
        consumer_id="factor_research",
        node_id="rdagent-node1",
        resolver=resolver,
    )
    environment = frozen_dataset_environment(frozen)

    assert calls == 1
    assert frozen["profile_sha256"] == PROFILE
    assert frozen["dataset_manifest_sha256"] == MANIFEST
    assert environment["AISTOCK_DATASET_ROOT"] == "/releases/r9"
    assert json.loads(environment["QE_DATASET_IDENTITY_ROOTS"]) == {
        "rdagent-node1": ["/releases/r9"]
    }
    assert environment["AISTOCK_DATASET_BINDING_SHA256"] == frozen["binding_sha256"]
    assert environment["QLIB_DAY_DATA"].endswith("/daily_bin_candidate")
    assert environment["QLIB_MINUTE_DATA"].endswith("/minute_bin_candidate")


def test_frozen_binding_survives_active_switch_without_resolving_again() -> None:
    old = freeze_active_dataset_task_binding(
        consumer_id="selection",
        node_id="rdagent-node1",
        resolver=lambda **kwargs: _resolved(**kwargs, root="/releases/old"),
    )
    new = freeze_active_dataset_task_binding(
        consumer_id="selection",
        node_id="rdagent-node1",
        resolver=lambda **kwargs: {
            **_resolved(**kwargs, root="/releases/new"),
            "generation": "20261031-monthly-v2-unified",
            "release_id": "qe_hmm_full_v2_20261031",
            "cutoff": "2026-10-30",
            "profile_sha256": "e" * 64,
            "dataset_manifest_sha256": "f" * 64,
        },
    )

    assert frozen_dataset_environment(old)["AISTOCK_DATASET_ROOT"] == "/releases/old"
    assert frozen_dataset_environment(new)["AISTOCK_DATASET_ROOT"] == "/releases/new"
    assert old["binding_sha256"] != new["binding_sha256"]


def test_frozen_binding_rejects_tampering_scope_and_noncanonical_root() -> None:
    frozen = freeze_active_dataset_task_binding(
        consumer_id="unified_backtest",
        node_id="rdagent-node1",
        resolver=lambda **kwargs: _resolved(**kwargs),
    )
    tampered = deepcopy(frozen)
    tampered["binding"]["generation"] = "changed"
    with pytest.raises(FrozenDatasetTaskBindingError, match="generation differs"):
        require_frozen_dataset_task_binding(
            tampered,
            consumer_id="unified_backtest",
            node_id="rdagent-node1",
        )

    bad_root = freeze_active_dataset_task_binding(
        consumer_id="unified_backtest",
        node_id="rdagent-node1",
        resolver=lambda **kwargs: _resolved(**kwargs, root="/releases/../escape"),
    )
    with pytest.raises(FrozenDatasetTaskBindingError, match="not canonical POSIX"):
        frozen_dataset_environment(bad_root)


def test_freeze_normalizes_invalid_active_sha_error() -> None:
    with pytest.raises(FrozenDatasetTaskBindingError, match="SHA256 is invalid"):
        freeze_active_dataset_task_binding(
            consumer_id="factor_research",
            node_id="rdagent-node1",
            resolver=lambda **kwargs: {
                **_resolved(**kwargs),
                "profile_sha256": "not-a-sha",
            },
        )
