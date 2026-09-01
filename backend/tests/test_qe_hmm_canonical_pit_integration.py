from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    CANONICAL_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    canonical_rule_parameters_digest,
)
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.dataset_release.pit import (
    DATASET_CANDIDATE_MANIFEST_SCHEMA,
    DATASET_PIT_BINDING_SCHEMA,
    freeze_pit_snapshot,
)
from backend.services.hmm_evolution.errors import InvalidSpecError
from backend.services.hmm_evolution.universe import require_hmm_formal_dataset_binding
from backend.services.quantevolver.qe_dataset_contract import require_qe_formal_dataset_binding


def _release_manifest(*, scope: str = "full") -> dict:
    release_id = "qe-hmm-v2-20260731"
    snapshot = freeze_pit_snapshot(
        [
            {
                "ts_code": "600000.SH",
                "eligible_start": "2025-01-02",
                "eligible_end": "2025-01-03",
                "entry_reason": "fixture",
                "exit_reason": None,
            }
        ],
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        scope_start=date(2025, 1, 1),
        cutoff=date(2026, 7, 31),
        state_identity="fixture-state",
        source_fingerprint_sha256="b" * 64,
        parameter_hash=canonical_rule_parameters_digest(),
    )
    return {
        "schema_version": DATASET_CANDIDATE_MANIFEST_SCHEMA,
        "release_id": release_id,
        "cutoff": snapshot.cutoff.isoformat(),
        "scope": scope,
        "artifact_root": "c" * 64,
        "pit_binding": {
            "schema_version": DATASET_PIT_BINDING_SCHEMA,
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "authority_status": PitAuthorityStatus.ACTIVE_CANONICAL.value,
            "scope": scope,
            "rolling_universe_key": CANONICAL_PIT_UNIVERSE_KEY,
            "frozen_universe_key": f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{release_id}",
            "rule_version": CANONICAL_PIT_RULE_VERSION,
            "rule_parameters_digest": canonical_rule_parameters_digest(),
            "cutoff": snapshot.cutoff.isoformat(),
            "rolling_cutoff_spans_sha256": snapshot.spans_sha256,
            "frozen_snapshot_digest": snapshot.spans_sha256,
            "release_id": release_id,
        },
    }


def _request(manifest: dict) -> dict:
    return {
        "schema_version": "qe_formal_canonical_pit_dataset_request_v1",
        "usage_mode": "formal_prediction",
        "expected_manifest_digest": hashlib.sha256(canonical_json_bytes(manifest)).hexdigest(),
        "release_manifest": manifest,
        "runtime_pins": {
            "schema_version": "qe_formal_frozen_runtime_pins_v1",
            "artifact_root": manifest["artifact_root"],
            "qlib_bin_snapshot_id": "qe-hmm-v2-20260731-daily",
            "qlib_instruments_sha256": "f" * 64,
            "qlib_calendar_sha256": "1" * 64,
            "qlib_meta_export_sha256": "2" * 64,
            "suspend_dataset_id": "qe-hmm-v2-20260731-suspend",
            "suspend_parquet_sha256": "3" * 64,
            "suspend_manifest_sha256": "4" * 64,
            "suspend_source_contract": "tushare_suspend_d_shsz_S_v1",
        },
    }


def test_qe_and_hmm_project_the_same_canonical_frozen_identity() -> None:
    manifest = _release_manifest()
    request = _request(manifest)

    hmm = require_hmm_formal_dataset_binding(request)
    qe = require_qe_formal_dataset_binding(
        manifest,
        usage_mode="formal_prediction",
        expected_manifest_digest=request["expected_manifest_digest"],
    )

    common_keys = {
        "authority_id",
        "rule_version",
        "rule_parameters_digest",
        "release_id",
        "cutoff",
        "frozen_snapshot_digest",
        "manifest_digest",
    }
    assert {key: hmm.as_dict()[key] for key in common_keys} == {
        key: qe.as_dict()[key] for key in common_keys
    }
    assert hmm.frozen_universe_key == f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{qe.release_id}"


@pytest.mark.parametrize(
    "mutation",
    [
        "sample",
        "legacy_schema",
        "manifest_tamper",
        "runtime_pin_drift",
        "runtime_pin_schema",
        "runtime_pin_incomplete",
    ],
)
def test_hmm_formal_binding_rejects_noncanonical_or_mutated_release(mutation: str) -> None:
    manifest = _release_manifest(scope="sample" if mutation == "sample" else "full")
    if mutation == "legacy_schema":
        manifest["schema_version"] = "legacy_dataset_manifest_v1"
    request = _request(manifest)
    if mutation == "manifest_tamper":
        request["release_manifest"]["artifact_root"] = "d" * 64
    if mutation == "runtime_pin_drift":
        request["runtime_pins"]["artifact_root"] = "d" * 64
    if mutation == "runtime_pin_schema":
        request["runtime_pins"]["schema_version"] = "qe_formal_runtime_pins_v0"
    if mutation == "runtime_pin_incomplete":
        request["runtime_pins"].pop("qlib_calendar_sha256")

    with pytest.raises(InvalidSpecError):
        require_hmm_formal_dataset_binding(request)


def test_frozen_hmm_data_plane_has_no_online_pit_repository_fallback_import() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    source_paths = [
        *sorted((repo_root / "backend/services/hmm_data_source").glob("*.py")),
        repo_root / "backend/services/hmm_evolution/universe.py",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in source_paths)

    assert "hmm_risk.stock_fact_repository" not in combined
    assert "StockUniversePitService" not in combined
