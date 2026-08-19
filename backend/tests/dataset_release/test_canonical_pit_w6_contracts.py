from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.services.dataset_release.canonical_pit_activation_envelope import (
    CanonicalPitActivationEnvelopeError,
    build_activation_envelope,
)
from backend.services.dataset_release.canonical_pit_candidate_bundle import (
    CanonicalPitCandidateBundleError,
    build_fixture_candidate_validation_bundle,
    validate_candidate_validation_bundle,
)
from backend.services.dataset_release.canonical_pit_w8_attestation import (
    build_fixture_w8_attestation,
)
from backend.services.canonical_pit_inference_boundary import (
    CanonicalPitInferenceBoundaryError,
    InferencePitMode,
    resolve_inference_pit_identity,
)


SHA = "a" * 64


def _bundle():
    return build_fixture_candidate_validation_bundle(
        candidate_validation_id="fixture-validation-1",
        created_at=datetime(2026, 8, 19, tzinfo=timezone.utc),
        source_commit="8bd31797a3ebb18991be179b04998e5b56043fbb",
        profile_id="qe_hmm_full_v2",
        profile_digest=SHA,
        toolchain_sha=SHA,
        candidate_id="fixture-candidate-1",
        release_id="fixture-release-1",
        requested_cutoff="2026-07-31",
        effective_cutoff="2026-07-31",
        artifact_root_identity={"root_id": "w6-fixture", "root_relative_path": "fixture/candidate"},
        artifact_root_digest=SHA,
        frozen_snapshot_digest=SHA,
        rolling_at_cutoff_digest=SHA,
        calendar_digest=SHA,
        manifest_digest=SHA,
        consumer_inventory_digest=SHA,
        state_source_digest=SHA,
        component_digests={
            "daily_bin": SHA,
            "minute_bin": SHA,
            "factor_h5": SHA,
            "static_factors": SHA,
            "domestic_index": SHA,
            "hmm_inputs": SHA,
        },
        instrument_universe_digest=SHA,
        validation_results={"status": "pass_fixture", "receipt_digest": SHA},
        resource_receipt_digest=SHA,
        consumer_smoke_results={"status": "pass_fixture", "receipt_digest": SHA},
        no_external_path_dependency_proof={"status": "pass", "external_mutable_path_count": 0, "proof_digest": SHA},
        historical_baseline_immutability_digest=SHA,
    )


def test_fixture_bundle_is_immutable_and_content_changes_change_digest() -> None:
    bundle = _bundle()
    assert bundle.digest == validate_candidate_validation_bundle(bundle.as_dict(), expected_digest=bundle.digest).digest
    changed = bundle.as_dict()
    changed["candidate_identity"]["candidate_id"] = "fixture-candidate-2"
    changed["candidate_identity"]["scope"] = "fixture"
    changed["candidate_identity"]["production_eligible"] = False
    changed["candidate_identity"]["training_eligible"] = False
    assert validate_candidate_validation_bundle(changed).digest != bundle.digest


def test_bundle_rejects_external_path_and_real_candidate_claim() -> None:
    value = _bundle().as_dict()
    value["artifact_root_identity"]["root_relative_path"] = "../outside"
    with pytest.raises(CanonicalPitCandidateBundleError):
        validate_candidate_validation_bundle(value)


def test_w8_fixture_is_not_independently_attested_and_activation_stays_blocked() -> None:
    bundle = _bundle()
    receipt = build_fixture_w8_attestation(
        candidate_bundle=bundle.as_dict(),
        candidate_bundle_digest=bundle.digest,
        attestation_id="fixture-w8-1",
        observed_at=datetime(2026, 8, 19, tzinfo=timezone.utc),
    )
    envelope = build_activation_envelope(
        candidate_bundle_digest=bundle.digest,
        w8_receipt=receipt.as_dict(),
        expected_pointer_generation=0,
        expected_pointer_key="aistock_equity_pit_canonical_v2",
        expected_pointer_envelope_digest=SHA,
        expected_source_commit="8bd31797a3ebb18991be179b04998e5b56043fbb",
        inactive_distribution_readback={"status": "not_run_not_authorized", "digest": SHA},
        node_readback={"status": "not_run_not_authorized", "digest": SHA},
        session_drain_readiness={"status": "not_run_not_authorized", "digest": SHA},
        rollback_target={"status": "not_run_not_authorized", "digest": SHA},
    )
    assert envelope.payload["status"] == "blocked_w8_attestation"
    assert envelope.payload["activation_performed"] is False


def test_activation_rejects_a_sealed_fixture_envelope() -> None:
    bundle = _bundle()
    receipt = build_fixture_w8_attestation(
        candidate_bundle=bundle.as_dict(),
        candidate_bundle_digest=bundle.digest,
        attestation_id="fixture-w8-2",
        observed_at=datetime(2026, 8, 19, tzinfo=timezone.utc),
    )
    with pytest.raises(CanonicalPitActivationEnvelopeError):
        build_activation_envelope(
            candidate_bundle_digest=bundle.digest,
            w8_receipt={**receipt.as_dict(), "outcome": "pass", "independently_attested": True},
            expected_pointer_generation=0,
            expected_pointer_key="aistock_equity_pit_canonical_v2",
            expected_pointer_envelope_digest=SHA,
            expected_source_commit="8bd31797a3ebb18991be179b04998e5b56043fbb",
            inactive_distribution_readback={"status": "ready", "digest": SHA},
            node_readback={"status": "ready", "digest": SHA},
            session_drain_readiness={"status": "ready", "digest": SHA},
            rollback_target={"status": "ready", "digest": SHA},
        )


def test_consumer_inventory_has_unknown_zero_and_required_classes() -> None:
    path = Path(__file__).parents[3] / "tests" / "aistock_validation" / "pit_v2" / "consumer_inventory.json"
    inventory = json.loads(path.read_text(encoding="utf-8"))
    assert inventory["schema_version"] == "canonical_pit_consumer_inventory_v1"
    assert inventory["unknown_count"] == 0
    assert inventory["classification_counts"]["unknown"] == 0
    classes = {item["class"] for item in inventory["consumers"]}
    assert set(inventory["classification_values"]) == {
        "canonical_v2_formal",
        "canonical_v2_frozen_candidate",
        "canonical_v2_rolling_runtime",
        "legacy_reproduction_only",
        "test_fixture",
        "migration_tool",
        "unknown",
    }
    assert classes == {
        "canonical_v2_formal",
        "canonical_v2_frozen_candidate",
        "canonical_v2_rolling_runtime",
        "legacy_reproduction_only",
        "test_fixture",
        "migration_tool",
    }


def test_inference_boundary_rejects_missing_identity_and_accepts_detached_frozen_identity() -> None:
    with pytest.raises(CanonicalPitInferenceBoundaryError):
        resolve_inference_pit_identity(None, version_tag="strategy_package_live")
    identity = resolve_inference_pit_identity(
        {
            "mode": "canonical_v2_frozen_candidate",
            "release_id": "fixture-release-1",
            "cutoff": "2026-07-31",
            "snapshot_digest": SHA,
            "universe_codes": ["000001.SZ", "600000.SH"],
        },
        version_tag="strategy_package_live",
    )
    assert identity.mode is InferencePitMode.FROZEN_CANDIDATE
    assert identity.binding.release_id == "fixture-release-1"
    assert identity.universe_codes == ("000001.SZ", "600000.SH")


def test_inference_boundary_requires_explicit_legacy_reproduction_identity() -> None:
    identity = resolve_inference_pit_identity(
        {"mode": "legacy_reproduction_only"},
        version_tag="legacy_reproduction",
    )
    assert identity.mode is InferencePitMode.LEGACY_REPRODUCTION
