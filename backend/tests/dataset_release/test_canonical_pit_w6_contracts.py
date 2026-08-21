from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from backend.inference_engine import _resolve_inference_pit_identity_for_run
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
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_PIT_RULE_VERSION,
    LEGACY_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
    legacy_rule_parameters_digest,
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


@pytest.mark.parametrize("field", ["production_eligible", "training_eligible"])
def test_bundle_rejects_tampered_eligibility_flags(field: str) -> None:
    value = _bundle().as_dict()
    value["candidate_identity"][field] = True
    with pytest.raises(CanonicalPitCandidateBundleError):
        validate_candidate_validation_bundle(value)


def test_builder_rejects_malformed_artifact_root_with_typed_error() -> None:
    kwargs = {
        "candidate_validation_id": "fixture-validation-1",
        "created_at": datetime(2026, 8, 19, tzinfo=timezone.utc),
        "source_commit": "8bd31797a3ebb18991be179b04998e5b56043fbb",
        "profile_id": "qe_hmm_full_v2",
        "profile_digest": SHA,
        "toolchain_sha": SHA,
        "candidate_id": "fixture-candidate-1",
        "release_id": "fixture-release-1",
        "requested_cutoff": "2026-07-31",
        "effective_cutoff": "2026-07-31",
        "artifact_root_identity": {"root_relative_path": "fixture/candidate"},
        "artifact_root_digest": SHA,
        "frozen_snapshot_digest": SHA,
        "rolling_at_cutoff_digest": SHA,
        "calendar_digest": SHA,
        "manifest_digest": SHA,
        "consumer_inventory_digest": SHA,
        "state_source_digest": SHA,
        "component_digests": {name: SHA for name in ("daily_bin", "minute_bin", "factor_h5", "static_factors", "domestic_index", "hmm_inputs")},
        "instrument_universe_digest": SHA,
        "validation_results": {"status": "pass_fixture", "receipt_digest": SHA},
        "resource_receipt_digest": SHA,
        "consumer_smoke_results": {"status": "pass_fixture", "receipt_digest": SHA},
        "no_external_path_dependency_proof": {"status": "pass", "external_mutable_path_count": 0, "proof_digest": SHA},
        "historical_baseline_immutability_digest": SHA,
    }
    with pytest.raises(CanonicalPitCandidateBundleError):
        build_fixture_candidate_validation_bundle(**kwargs)


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
    path = Path(__file__).parents[3] / "tests" / "aistock_validation" / "pit_v2" / "window_scope_receipt.json"
    inventory = json.loads(path.read_text(encoding="utf-8"))["consumer_inventory"]
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
    with pytest.raises(CanonicalPitInferenceBoundaryError):
        resolve_inference_pit_identity(
            {"mode": "legacy_reproduction_only"},
            version_tag="legacy_reproduction",
        )
    identity = resolve_inference_pit_identity(
        {
            "mode": "legacy_reproduction_only",
            "release_id": "legacy-release-20260630",
            "cutoff": "2026-06-30",
            "snapshot_digest": SHA,
            "universe_codes": ["600000.SH", "000001.SZ"],
        },
        version_tag="legacy_reproduction",
    )
    assert identity.mode is InferencePitMode.LEGACY_REPRODUCTION
    assert identity.binding.reproduction_mode is True
    assert identity.binding.universe_key.startswith("shsz_st_pit_qe_dataset_")
    assert identity.universe_codes == ("000001.SZ", "600000.SH")


def test_real_w8_pass_can_only_prepare_an_unsealed_w9_envelope() -> None:
    bundle = _bundle()
    real_receipt = {
        "schema_version": "canonical_pit_w8_independent_attestation_v1",
        "attestation_id": "real-w8-1",
        "observed_at": "2026-08-19T00:00:00Z",
        "candidate_bundle_digest": bundle.digest,
        "subject": {
            "candidate_id": "candidate-v2-20260731",
            "release_id": "release-v2-20260731",
            "artifact_root_digest": SHA,
        },
        "attestation_scope": "real_candidate",
        "independently_attested": True,
        "outcome": "pass",
        "runtime_real_data_evidence": "real_candidate_evidence",
        "validator_identity": "w8-independent-validator-v1",
    }
    envelope = build_activation_envelope(
        candidate_bundle_digest=bundle.digest,
        w8_receipt=real_receipt,
        expected_pointer_generation=0,
        expected_pointer_key="shsz_st_pit_active_v1",
        expected_pointer_envelope_digest=SHA,
        expected_source_commit="source-commit",
        inactive_distribution_readback={"status": "ready", "digest": SHA},
        node_readback={"status": "ready", "digest": SHA},
        session_drain_readiness={"status": "ready", "digest": SHA},
        rollback_target={"status": "ready", "digest": SHA},
    )
    assert envelope.payload["status"] == "w9_seal_required"
    assert envelope.payload["activation_performed"] is False


def _live_binding(*, canonical: bool) -> PitConsumerBinding:
    return PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=(
            PitAuthorityStatus.ACTIVE_CANONICAL
            if canonical
            else PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION
        ),
        universe_key=CANONICAL_PIT_UNIVERSE_KEY if canonical else LEGACY_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION if canonical else LEGACY_PIT_RULE_VERSION,
        rule_parameters_digest=(
            canonical_rule_parameters_digest() if canonical else legacy_rule_parameters_digest()
        ),
        activation_generation=1 if canonical else 0,
        activation_envelope_digest=SHA if canonical else None,
        expected_source_commit="source-commit" if canonical else None,
        state_source_digest=SHA,
        coverage_start=date(2018, 8, 1),
        coverage_end=date(2026, 7, 31),
    )


def test_inference_boundary_preserves_authority_pointer_legacy_migration_runtime() -> None:
    identity = resolve_inference_pit_identity(
        None,
        version_tag="strategy_package_live",
        live_binding=_live_binding(canonical=False),
        allow_active_canonical_pointer=False,
    )
    assert identity.mode is InferencePitMode.ROLLING_RUNTIME
    assert identity.binding.authority_status is PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION
    assert identity.receipt_mode == "deployed_legacy_pending_migration"


def test_retrospective_inference_cannot_borrow_active_canonical_pointer() -> None:
    with pytest.raises(CanonicalPitInferenceBoundaryError):
        resolve_inference_pit_identity(
            None,
            version_tag="strategy_package_live",
            live_binding=_live_binding(canonical=True),
            allow_active_canonical_pointer=False,
        )


def test_prospective_inference_accepts_resolver_issued_active_canonical_pointer() -> None:
    identity = resolve_inference_pit_identity(
        None,
        version_tag="strategy_package_live",
        live_binding=_live_binding(canonical=True),
        allow_active_canonical_pointer=True,
    )
    assert identity.mode is InferencePitMode.ROLLING_RUNTIME
    assert identity.binding.universe_key == CANONICAL_PIT_UNIVERSE_KEY


def test_inference_run_uses_singleton_pointer_only_when_manifest_has_no_identity() -> None:
    class Resolver:
        calls = 0

        def resolve_live_binding(self) -> PitConsumerBinding:
            self.calls += 1
            return _live_binding(canonical=False)

    resolver = Resolver()
    identity = _resolve_inference_pit_identity_for_run(
        manifest={"primary_assets": {}},
        pit_identity=None,
        version_tag="strategy_package_live",
        receipt_admissibility="PROSPECTIVE_FIRST_OBSERVED",
        authority_resolver=resolver,  # type: ignore[arg-type]
    )
    assert resolver.calls == 1
    assert identity.receipt_mode == "deployed_legacy_pending_migration"


def test_inference_run_does_not_read_pointer_for_detached_frozen_identity() -> None:
    class Resolver:
        def resolve_live_binding(self) -> PitConsumerBinding:
            raise AssertionError("frozen inference must not read the live pointer")

    identity = _resolve_inference_pit_identity_for_run(
        manifest={
            "canonical_pit_identity": {
                "mode": "canonical_v2_frozen_candidate",
                "release_id": "fixture-release-1",
                "cutoff": "2026-07-31",
                "snapshot_digest": SHA,
                "universe_codes": ["000001.SZ"],
            }
        },
        pit_identity=None,
        version_tag="strategy_package_live",
        receipt_admissibility="RETROSPECTIVE_DB_CONTENT_HASH",
        authority_resolver=Resolver(),  # type: ignore[arg-type]
    )
    assert identity.mode is InferencePitMode.FROZEN_CANDIDATE


def test_strategy_package_training_binding_is_not_misread_as_runtime_identity() -> None:
    class Resolver:
        calls = 0

        def resolve_live_binding(self) -> PitConsumerBinding:
            self.calls += 1
            return _live_binding(canonical=False)

    resolver = Resolver()
    identity = _resolve_inference_pit_identity_for_run(
        manifest={
            "canonical_pit_binding": {
                "schema_version": "strategy_package_canonical_pit_binding_v2",
                "source_usage_mode": "formal_training",
                "release_id": "release-v2-20260731",
            }
        },
        pit_identity=None,
        version_tag="strategy_package_live",
        receipt_admissibility="PROSPECTIVE_FIRST_OBSERVED",
        authority_resolver=resolver,  # type: ignore[arg-type]
    )
    assert resolver.calls == 1
    assert identity.receipt_mode == "deployed_legacy_pending_migration"


def test_malformed_explicit_manifest_inference_identity_fails_without_pointer_fallback() -> None:
    class Resolver:
        def resolve_live_binding(self) -> PitConsumerBinding:
            raise AssertionError("malformed explicit identity must not fall back to the live pointer")

    with pytest.raises(CanonicalPitInferenceBoundaryError):
        _resolve_inference_pit_identity_for_run(
            manifest={"canonical_pit_identity": "not-an-object"},
            pit_identity=None,
            version_tag="strategy_package_live",
            receipt_admissibility="PROSPECTIVE_FIRST_OBSERVED",
            authority_resolver=Resolver(),  # type: ignore[arg-type]
        )
