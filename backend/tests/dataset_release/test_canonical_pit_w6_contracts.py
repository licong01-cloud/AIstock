from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from backend.inference_engine import InferenceEngine, _resolve_inference_pit_identity_for_run
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


def _real_bundle_payload():
    value = _bundle().as_dict()
    value["candidate_identity"].update(scope="full", production_eligible=True, training_eligible=True)
    value["rolling_observation"]["row_count"] = 1
    value["validation_results"]["status"] = "pass"
    value["consumer_smoke_results"]["status"] = "pass"
    value["terminal_outcome"] = "CANDIDATE_VALIDATED"
    value["runtime_real_data_evidence"] = "real_candidate_evidence"
    return value


def test_real_bundle_requires_explicit_real_validation_and_closed_links():
    value = _real_bundle_payload()
    with pytest.raises(CanonicalPitCandidateBundleError):
        validate_candidate_validation_bundle(value)
    assert validate_candidate_validation_bundle(value, allow_real=True).payload["candidate_identity"]["scope"] == "full"
    value["frozen_release"]["artifact_root_digest"] = "b" * 64
    with pytest.raises(CanonicalPitCandidateBundleError, match="identity"):
        validate_candidate_validation_bundle(value, allow_real=True)


def test_real_bundle_cannot_be_made_from_a_fixture_attestation():
    from backend.services.dataset_release.canonical_pit_migration import seal_real_activation

    bundle = _bundle()
    receipt = build_fixture_w8_attestation(candidate_bundle=bundle.as_dict(), candidate_bundle_digest=bundle.digest,
        attestation_id="fixture-w8-2", observed_at=datetime(2026, 8, 19, tzinfo=timezone.utc))
    with pytest.raises(ValueError):
        seal_real_activation(bundle.as_dict(), receipt.as_dict(), {})


def test_independent_membership_audit_reports_exact_drift_and_duplicates():
    from backend.services.dataset_release.canonical_pit_migration import audit_eligibility_intervals

    result = audit_eligibility_intervals(
        [("000001.SZ", "2018-08-01", "2026-08-31")],
        [("000001.SZ", "2018-08-01", "2026-08-31"), ("000002.SZ", "2018-08-01", "2026-08-31")],
        start=date(2018, 8, 1), cutoff=date(2026, 8, 31))
    assert result["status"] == "BLOCKED"
    assert result["rolling_only"] == [["000002.SZ", "2018-08-01", "2026-08-31"]]
    duplicate = [("000001.SZ", "2018-08-01", "2026-08-31")] * 2
    with pytest.raises(ValueError, match="duplicate"):
        audit_eligibility_intervals(duplicate, duplicate, start=date(2018, 8, 1), cutoff=date(2026, 8, 31))


def test_real_asset_readback_rejects_hash_drift(tmp_path):
    from backend.services.dataset_release.canonical_pit_migration import read_sealed_json

    source = tmp_path / "receipt.json"
    source.write_text('{"status":"pass"}', encoding="utf-8")
    with pytest.raises(ValueError, match="digest"):
        read_sealed_json(source, expected_digest=SHA)


def test_sealed_json_readback_ignores_access_time_not_content_drift(tmp_path, monkeypatch):
    import os
    from backend.services.dataset_release.canonical_pit_migration import read_sealed_json
    path = tmp_path / "access.json"
    path.write_text('{"value":1}', encoding="utf-8")
    read = Path.read_bytes
    def access_changed(self):
        raw = read(self)
        info = self.stat()
        os.utime(self, ns=(1, info.st_mtime_ns))
        return raw
    monkeypatch.setattr(Path, "read_bytes", access_changed)
    assert read_sealed_json(path) == {"value": 1}
    def content_changed(self):
        raw = read(self)
        self.write_bytes(b'{"value":22}')
        return raw
    monkeypatch.setattr(Path, "read_bytes", content_changed)
    with pytest.raises(ValueError, match="changed during"):
        read_sealed_json(path)


def test_forward_profile_plan_uses_owner_normalization_without_business_drift(monkeypatch):
    from types import SimpleNamespace
    from backend.services.dataset_release.canonical_pit_migration import plan_forward_profiles
    from backend.services.paper_trading_v2.models import compute_runtime_config_sha256
    from backend.services.paper_trading_v2 import service as owner
    from backend.services.paper_trading_v2 import canonical_pit_control as control

    source = {"business": 7, "pit": "legacy"}
    version = SimpleNamespace(profile_id="p", config_json=source,
        config_sha256=compute_runtime_config_sha256(source))
    repo = SimpleNamespace(get_runtime_profile_version=lambda _: version,
        get_runtime_profile=lambda _: SimpleNamespace(portfolio_id="pf"),
        get_portfolio=lambda _: SimpleNamespace(frozen_manifest=None))
    def migrate(config):
        return {**config, "pit": "canonical"}
    monkeypatch.setattr(control, "plan_paper_runtime_profile_migration", lambda v: {
        "action": "CREATE_NEW_CANONICAL_VERSION", "source_config_sha256": v.config_sha256,
        "target_config_sha256": compute_runtime_config_sha256(migrate(v.config_json)),
        "target_config_json": migrate(v.config_json)})
    monkeypatch.setattr(control, "migrate_runtime_config_to_canonical_pointer", migrate)
    class Normalizer:
        def __init__(self, repository):
            pass
        def _normalize_runtime_profile_config(self, config, *, manifest):
            return {**config, "owner_default": True}
    monkeypatch.setattr(owner, "PaperTradingV2PortfolioService", Normalizer)
    records = [{"profile_id": "p", "current_version_id": "v", "portfolio_id": "pf"}]
    plan = plan_forward_profiles(repo, records)
    assert plan["profiles"][0]["target_config_sha256"] == compute_runtime_config_sha256(
        {"business": 7, "pit": "canonical", "owner_default": True})
    assert plan["profiles"][0]["owner_normalization_changed"] is True
    class DriftingNormalizer(Normalizer):
        def _normalize_runtime_profile_config(self, config, *, manifest):
            return {**config, "business": 8 if config["pit"] == "canonical" else 7}
    monkeypatch.setattr(owner, "PaperTradingV2PortfolioService", DriftingNormalizer)
    with pytest.raises(ValueError, match="business semantics"):
        plan_forward_profiles(repo, records)
    version.profile_id = "other"
    with pytest.raises(ValueError, match="ownership"):
        plan_forward_profiles(repo, records)


@pytest.mark.parametrize("content", ['{"a":1,"a":2}', '{"a":NaN}', '['])
def test_real_evidence_rejects_ambiguous_or_malformed_json(tmp_path, content):
    from backend.services.dataset_release.canonical_pit_migration import read_sealed_json
    source = tmp_path / "ambiguous.json"
    source.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError):
        read_sealed_json(source)


def _real_file_bundle(tmp_path, rows=None):
    """Test-only files; never emit an independent receipt or production asset."""
    import hashlib
    from backend.services.dataset_release.canonical import canonical_json_bytes
    files = {}
    def persist(name, value):
        raw = canonical_json_bytes(value)
        digest = hashlib.sha256(raw).hexdigest()
        path = tmp_path / name
        path.write_bytes(raw + b"\n")
        files[digest] = path
        return digest
    def replace(value, old, new):
        if isinstance(value, dict):
            return {key: replace(item, old, new) for key, item in value.items()}
        if isinstance(value, list):
            return [replace(item, old, new) for item in value]
        return new if value == old else value
    metadata = persist("metadata.json", {"test_only": True})
    value = replace(_real_bundle_payload(), SHA, metadata)
    rows = rows if rows is not None else [["000001.SZ", "2018-08-01", "2026-07-31", "IPO_ELIGIBLE", None]]
    row_digest = persist("snapshot.json", rows)
    value["pit_identity"].update(frozen_snapshot_digest=row_digest, rolling_at_cutoff_digest=row_digest)
    value["frozen_release"]["pit_snapshot_digest"] = row_digest
    value["rolling_observation"].update(digest=row_digest, row_count=len(rows))
    value["validation"]["independent_pit_receipt"] = persist("oracle.json", {
        "status": "PASS", "frozen_snapshot_digest": row_digest,
        "rolling_cutoff_spans_sha256": row_digest, "row_count": len(rows), "cutoff": "2026-07-31"})
    return value, files


def test_real_bundle_reads_full_logical_rows_and_independent_oracle(tmp_path):
    from backend.services.dataset_release.canonical_pit_migration import build_real_candidate_validation_bundle
    value, files = _real_file_bundle(tmp_path)
    assert build_real_candidate_validation_bundle(value, evidence_files=files).payload["rolling_observation"]["row_count"] == 1
    oracle = files[value["validation"]["independent_pit_receipt"]]
    oracle.write_text('{"status":"PASS"}', encoding="utf-8")
    with pytest.raises(ValueError, match="digest"):
        build_real_candidate_validation_bundle(value, evidence_files=files)


@pytest.mark.parametrize("rows", [
    [["000001.SZ", "2018-08-01", "2026-07-31"]],
    [["000001.SZ", "2018-08-01", "2026-08-31", "IPO_ELIGIBLE", None]],
    [["000001.SZ", "2018-08-01", "2026-07-31", "", None]],
    [["000001.SZ", "2018-08-01", "2026-07-31", "IPO_ELIGIBLE", None]] * 2,
])
def test_real_bundle_rejects_incomplete_ambiguous_or_postcutoff_rows(tmp_path, rows):
    from backend.services.dataset_release.canonical_pit_migration import build_real_candidate_validation_bundle
    value, files = _real_file_bundle(tmp_path, rows)
    with pytest.raises(ValueError):
        build_real_candidate_validation_bundle(value, evidence_files=files)


def test_real_w8_binds_actual_candidate_subject_not_just_pass_status():
    from backend.services.dataset_release.canonical_pit_w8_attestation import bind_real_w8_attestation
    bundle = validate_candidate_validation_bundle(_real_bundle_payload(), allow_real=True)
    fixture = _bundle()
    receipt = build_fixture_w8_attestation(candidate_bundle=fixture.as_dict(), candidate_bundle_digest=fixture.digest,
        attestation_id="test-w8", observed_at=datetime(2026, 8, 19, tzinfo=timezone.utc)).as_dict()
    receipt.update(candidate_bundle_digest=bundle.digest, attestation_scope="real_candidate",
        independently_attested=True, outcome="pass", runtime_real_data_evidence="real_candidate_evidence")
    assert bind_real_w8_attestation(bundle.as_dict(), receipt).payload["outcome"] == "pass"
    receipt["subject"]["release_id"] = "another-release"
    with pytest.raises(ValueError, match="subject"):
        bind_real_w8_attestation(bundle.as_dict(), receipt)


def test_initial_legacy_pointer_can_use_null_envelope_but_cannot_self_seal():
    bundle = _bundle()
    receipt = build_fixture_w8_attestation(candidate_bundle=bundle.as_dict(), candidate_bundle_digest=bundle.digest,
        attestation_id="initial-w8", observed_at=datetime(2026, 8, 19, tzinfo=timezone.utc))
    readback = {"status": "not_run_not_authorized", "digest": SHA}
    inputs = dict(candidate_bundle_digest=bundle.digest, w8_receipt=receipt.as_dict(),
        expected_pointer_generation=0, expected_pointer_key=LEGACY_PIT_UNIVERSE_KEY,
        expected_pointer_envelope_digest=None, expected_source_commit="main",
        inactive_distribution_readback=readback, node_readback=readback,
        session_drain_readiness=readback, rollback_target=readback)
    result = build_activation_envelope(**inputs)
    assert result.payload["expected_pointer_envelope_digest"] is None
    inputs["expected_pointer_generation"] = 1
    with pytest.raises(CanonicalPitActivationEnvelopeError, match="digest"):
        build_activation_envelope(**inputs)
    from backend.services.dataset_release.canonical_pit_activation_envelope import validate_activation_envelope
    tampered = result.as_dict()
    tampered["status"] = "READY_TO_ACTIVATE"
    with pytest.raises(CanonicalPitActivationEnvelopeError):
        validate_activation_envelope(tampered)


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
            "universe_as_of": "2026-07-31",
            "universe_codes": ["000001.SZ", "600000.SH"],
        },
        version_tag="strategy_package_live",
    )
    assert identity.mode is InferencePitMode.FROZEN_CANDIDATE
    assert identity.binding.release_id == "fixture-release-1"
    assert identity.universe_codes == ("000001.SZ", "600000.SH")


def test_detached_frozen_universe_cannot_be_reused_for_another_trade_date() -> None:
    identity = resolve_inference_pit_identity(
        {
            "mode": "canonical_v2_frozen_candidate",
            "release_id": "fixture-release-1",
            "cutoff": "2026-07-31",
            "snapshot_digest": SHA,
            "universe_as_of": "2026-07-31",
            "universe_codes": ["000001.SZ"],
        },
        version_tag="strategy_package_live",
    )
    with pytest.raises(CanonicalPitInferenceBoundaryError):
        InferenceEngine()._get_default_universe_excluding_st(
            datetime(2026, 7, 30),
            ensure=False,
            pit_identity=identity,
        )


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
            "universe_as_of": "2026-06-30",
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


def test_rolling_pointer_must_cover_the_inference_date_before_query() -> None:
    identity = resolve_inference_pit_identity(
        None,
        version_tag="strategy_package_live",
        live_binding=_live_binding(canonical=False),
    )
    with pytest.raises(CanonicalPitInferenceBoundaryError):
        InferenceEngine()._get_default_universe_excluding_st(
            datetime(2026, 8, 1),
            ensure=False,
            pit_identity=identity,
        )


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
                "universe_as_of": "2026-07-31",
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
