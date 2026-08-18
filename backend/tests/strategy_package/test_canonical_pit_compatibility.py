from __future__ import annotations

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    canonical_rule_parameters_digest,
)
from backend.services.strategy_package.canonical_pit_compatibility import (
    CANONICAL_V2_READY,
    LEGACY_REPRODUCTION_ONLY,
    build_canonical_pit_v2_manifest,
    classify_strategy_package_pit,
    require_canonical_pit_strategy_package,
)
from backend.services.strategy_package.manifest import compute_manifest_sha256, freeze_manifest
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _dataset_binding() -> dict[str, str]:
    return {
        "schema_version": "qe_formal_canonical_pit_dataset_binding_v1",
        "usage_mode": "formal_training",
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "rule_parameters_digest": canonical_rule_parameters_digest(),
        "release_id": "qe_hmm_full_v2_20260731",
        "cutoff": "2026-07-31",
        "frozen_snapshot_digest": "a" * 64,
        "manifest_digest": "b" * 64,
    }


def _alpha_core_manifest() -> StrategyPackageManifest:
    payload = make_manifest().model_dump(mode="json")
    payload.update(
        {
            "manifest_version": "alpha_core_v1",
            "strategy_config": {},
            "universe_policy": None,
            "portfolio_policy": None,
            "execution_policy": None,
            "minute_execution_policy": None,
            "risk_policy": None,
            "manifest_sha256": None,
        }
    )
    return freeze_manifest(StrategyPackageManifest.model_validate(payload))


def test_legacy_manifest_identity_is_stable_and_classified_reproduction_only() -> None:
    legacy = freeze_manifest(make_manifest())
    before = legacy.model_dump(mode="json", exclude={"canonical_pit_binding"})

    restored = StrategyPackageManifest.model_validate(before)

    assert restored.canonical_pit_binding is None
    assert compute_manifest_sha256(restored) == legacy.manifest_sha256
    compatibility = classify_strategy_package_pit(restored)
    assert compatibility.disposition == LEGACY_REPRODUCTION_ONLY
    assert compatibility.reproduction_only is True
    assert compatibility.canonical_binding is None


def test_v2_builder_creates_new_version_without_mutating_published_source() -> None:
    legacy = _alpha_core_manifest()
    before = legacy.model_dump(mode="json")

    migrated = build_canonical_pit_v2_manifest(
        legacy,
        package_id=f"{legacy.package_id}_pitv2",
        package_version="2.0.0",
        dataset_binding=_dataset_binding(),
        qualification_method="REVALIDATED",
        qualification_evidence_digest="c" * 64,
    )

    assert legacy.model_dump(mode="json") == before
    assert migrated.package_id == f"{legacy.package_id}_pitv2"
    assert migrated.package_version == "2.0.0"
    assert migrated.manifest_version == "alpha_core_v2"
    assert migrated.manifest_sha256 == compute_manifest_sha256(migrated)
    assert migrated.source_evidence["canonical_pit_migration"] == {
        "schema_version": "strategy_package_canonical_pit_migration_source_v1",
        "source_package_id": legacy.package_id,
        "source_package_version": legacy.package_version,
        "source_manifest_sha256": legacy.manifest_sha256,
    }
    binding = require_canonical_pit_strategy_package(migrated, operation="advisory_prediction")
    assert binding.release_id == "qe_hmm_full_v2_20260731"
    assert binding.release_cutoff.isoformat() == "2026-07-31"
    assert binding.frozen_universe_key == "aistock_equity_pit_snapshot_qe_hmm_full_v2_20260731"
    assert classify_strategy_package_pit(migrated).disposition == CANONICAL_V2_READY

    repository = InMemoryStrategyPackageRepository()
    legacy_record = repository.save_manifest(legacy)
    v2_record = repository.save_manifest(migrated)
    assert legacy_record.package_id != v2_record.package_id
    assert repository.get(legacy.package_id).manifest_sha256 == legacy.manifest_sha256
    assert repository.get(migrated.package_id).manifest_sha256 == migrated.manifest_sha256


def test_legacy_manifest_cannot_enter_new_formal_operation() -> None:
    legacy = freeze_manifest(make_manifest())

    try:
        require_canonical_pit_strategy_package(legacy, operation="selection")
    except StrategyPackageValidationError as exc:
        assert exc.context["required_disposition"] == CANONICAL_V2_READY
    else:  # pragma: no cover - explicit business oracle
        raise AssertionError("legacy package unexpectedly entered a formal v2 operation")


def test_v2_builder_rejects_same_version_and_noncanonical_binding() -> None:
    legacy = _alpha_core_manifest()
    try:
        build_canonical_pit_v2_manifest(
            legacy,
            package_id=legacy.package_id,
            package_version="2.0.0",
            dataset_binding=_dataset_binding(),
            qualification_method="RETRAINED",
            qualification_evidence_digest="c" * 64,
        )
    except StrategyPackageValidationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("same-package-id migration unexpectedly succeeded")

    try:
        build_canonical_pit_v2_manifest(
            legacy,
            package_id=f"{legacy.package_id}_pitv2",
            package_version=legacy.package_version,
            dataset_binding=_dataset_binding(),
            qualification_method="RETRAINED",
            qualification_evidence_digest="c" * 64,
        )
    except StrategyPackageValidationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("same-version migration unexpectedly succeeded")

    invalid = {**_dataset_binding(), "rule_version": "legacy_rule"}
    try:
        build_canonical_pit_v2_manifest(
            legacy,
            package_id=f"{legacy.package_id}_pitv2",
            package_version="2.0.0",
            dataset_binding=invalid,
            qualification_method="RETRAINED",
            qualification_evidence_digest="c" * 64,
        )
    except ValueError as exc:
        assert "canonical PIT rule version" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("noncanonical PIT binding unexpectedly succeeded")

    prediction_only = {**_dataset_binding(), "usage_mode": "formal_prediction"}
    try:
        build_canonical_pit_v2_manifest(
            legacy,
            package_id=f"{legacy.package_id}_pitv2_prediction",
            package_version="2.0.1",
            dataset_binding=prediction_only,
            qualification_method="REVALIDATED",
            qualification_evidence_digest="d" * 64,
        )
    except StrategyPackageValidationError as exc:
        assert "formal_training" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("prediction-only binding unexpectedly created a training package")
