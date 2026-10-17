from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.services.simulation_runtime.localsim_runtime_profile import (
    LocalSimRuntimeProfileConfigV1,
    LocalSimRuntimeProfileConfigRequestV1,
    LocalSimRuntimeProfileStatus,
    LocalSimRuntimeProfileValidationStatus,
)
from backend.services.simulation_runtime.localsim_runtime_profile_repository import (
    InMemoryLocalSimRuntimeProfileRepository,
)
from backend.services.simulation_runtime.localsim_runtime_profile_service import LocalSimRuntimeProfileService
from backend.services.trading_core.errors import InvalidStateTransitionError, RuntimeConfigInvalidError


NOW = datetime(2026, 8, 31, 2, 30, tzinfo=UTC)


class _Authority:
    def require_package_identity(self, *, package_id: str, manifest_sha256: str) -> None:
        if (package_id, manifest_sha256) != ("pkg_alpha", "a" * 64):
            raise RuntimeConfigInvalidError("package identity mismatch")

    def validate_and_materialize_config(
        self, *, package_id: str, manifest_sha256: str, config: LocalSimRuntimeProfileConfigRequestV1
    ) -> tuple[LocalSimRuntimeProfileConfigV1, dict[str, object]]:
        self.require_package_identity(package_id=package_id, manifest_sha256=manifest_sha256)
        return config.materialize(runtime_variant_materialized_config=None), {
            "package_identity_verified": True,
            "hmm_reference_verified": True,
            "runtime_variant_verified": config.runtime_variant_id is not None,
        }


def _service():
    repository = InMemoryLocalSimRuntimeProfileRepository()
    return (
        LocalSimRuntimeProfileService(repository=repository, authority=_Authority(), clock=lambda: NOW),
        repository,
    )


def _config() -> dict[str, object]:
    return {
        "schema_version": "localsim_runtime_profile_config_request_v1",
        "daily_strategy": {
            "strategy_id": "daily_small_cap_v1",
            "strategy_version": "2026-08-31",
            "top_k": 20,
            "industry_filters": ["801010"],
            "sector_filters": [],
            "parameters": {"rebalance": "daily"},
        },
        "hmm": {
            "enabled": False,
            "snapshot_id": None,
            "model_version": None,
            "preset": None,
            "state_mapping": {},
        },
        "risk_policy": {"max_position_weight": 0.1},
        "fee_policy": {"commission_bps": 3},
        "runtime_variant_id": None,
        "runtime_variant_hash": None,
        "notes": "current LocalSIM configuration",
        "metadata": {},
    }


def test_profile_and_version_are_package_scoped_append_only_and_idempotent() -> None:
    service, repository = _service()
    profile = service.create_profile(
        package_id="pkg_alpha",
        manifest_sha256="a" * 64,
        profile_name="LocalSIM six-month replay",
        created_by="test",
    )
    updated, version = service.create_version(
        profile_id=profile.profile_id,
        expected_profile_version=profile.version,
        config_json=_config(),
        created_by="test",
    )

    assert updated.version == 2
    assert version.validation_status is LocalSimRuntimeProfileValidationStatus.VALIDATED
    assert version.package_id == profile.package_id
    assert version.daily_strategy_profile_version_id.startswith("lsdaily_")
    duplicate_profile, duplicate = service.create_version(
        profile_id=profile.profile_id,
        expected_profile_version=1,
        config_json=_config(),
        created_by="test",
    )
    assert duplicate == version
    assert duplicate_profile.version == 2
    assert len(repository.versions) == 1


def test_profile_version_cas_and_retirement_fail_closed_without_partial_append() -> None:
    service, repository = _service()
    profile = service.create_profile(
        package_id="pkg_alpha",
        manifest_sha256="a" * 64,
        profile_name="mutable runtime config",
        created_by="test",
    )
    service.create_version(
        profile_id=profile.profile_id,
        expected_profile_version=1,
        config_json=_config(),
        created_by="test",
    )
    changed = _config()
    changed["risk_policy"] = {"max_position_weight": 0.05}
    with pytest.raises(InvalidStateTransitionError, match="CAS failed"):
        service.create_version(
            profile_id=profile.profile_id,
            expected_profile_version=1,
            config_json=changed,
            created_by="test",
        )
    assert len(repository.versions) == 1
    retired = service.retire_profile(profile_id=profile.profile_id, expected_version=2)
    assert retired.status is LocalSimRuntimeProfileStatus.RETIRED
    with pytest.raises(InvalidStateTransitionError, match="retired"):
        service.create_version(
            profile_id=profile.profile_id,
            expected_profile_version=retired.version,
            config_json=changed,
            created_by="test",
        )


def test_profile_rejects_alpha_economic_execution_and_ambiguous_hmm_fields() -> None:
    invalid = _config()
    invalid["risk_policy"] = {"alpha_components": ["forbidden"]}
    with pytest.raises(RuntimeConfigInvalidError, match="forbidden authority field"):
        LocalSimRuntimeProfileConfigRequestV1.model_validate(invalid)

    invalid = _config()
    invalid["fee_policy"] = {"execution_policy": {"algo_code": "V25_TWO_STAGE"}}
    with pytest.raises(RuntimeConfigInvalidError, match="forbidden authority field"):
        LocalSimRuntimeProfileConfigRequestV1.model_validate(invalid)

    invalid = _config()
    invalid["hmm"] = {
        "enabled": True,
        "snapshot_id": None,
        "model_version": None,
        "preset": None,
        "state_mapping": {},
    }
    with pytest.raises(ValueError, match="enabled HMM requires"):
        LocalSimRuntimeProfileConfigRequestV1.model_validate(invalid)


def test_profile_request_rejects_client_materialized_runtime_variant() -> None:
    invalid = _config()
    invalid["runtime_variant_materialized_config"] = {"strategy_config": {"top_k": 30}}
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        LocalSimRuntimeProfileConfigRequestV1.model_validate(invalid)
