from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backend.services.simulation_runtime.localsim_product_authority import LocalSimProductAuthority
from backend.services.simulation_runtime.localsim_runtime_profile_repository import (
    InMemoryLocalSimRuntimeProfileRepository,
)
from backend.services.simulation_runtime.localsim_runtime_profile_service import LocalSimRuntimeProfileService
from backend.services.strategy_package.execution_policy import ValidatedExecutionPolicy
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import PackageStatusEvent
from backend.services.strategy_package.runtime_variant import RuntimeVariantValidationStatus
from backend.services.trading_core.errors import RuntimeConfigInvalidError


NOW = datetime(2026, 8, 31, 4, 0, tzinfo=UTC)
MANIFEST = "a" * 64


class _PackageRepository:
    def __init__(self) -> None:
        self.record = SimpleNamespace(
            package_id="pkg_alpha",
            manifest_sha256=MANIFEST,
            package_status=PackageStatus.PAPER_ENABLED,
        )
        self.event = PackageStatusEvent(
            package_id="pkg_alpha",
            from_status=PackageStatus.BACKTEST_APPROVED,
            to_status=PackageStatus.PAPER_ENABLED,
            reason="enable_paper",
            context={"source": "test"},
            created_at=NOW,
        )
        self.policy = _policy("TWAP")
        self.variant = None

    def get(self, package_id: str):
        if package_id != self.record.package_id:
            raise KeyError(package_id)
        return self.record

    def list_status_events(self, package_id: str, *, limit: int = 200):
        assert package_id == self.record.package_id
        assert limit == 200
        return [self.event]

    def get_execution_policy(self, package_id: str, policy_id: str):
        assert package_id == self.record.package_id
        if policy_id != self.policy.policy_id:
            raise KeyError(policy_id)
        return self.policy

    def get_runtime_variant(self, package_id: str, variant_id: str):
        assert package_id == self.record.package_id
        if self.variant is None or variant_id != self.variant.variant_id:
            raise KeyError(variant_id)
        return self.variant


class _PackageService:
    def __init__(self, repository: _PackageRepository) -> None:
        self.repository = repository

    def paper_simulation_admission(self, package_id: str, *, governance_limit: int):
        assert package_id == "pkg_alpha"
        assert governance_limit == 0
        return {
            "package_id": package_id,
            "manifest_sha256": MANIFEST,
            "paper_simulation_allowed": True,
            "blockers": [],
            "asset_eligibility": {
                "package_id": package_id,
                "manifest_sha256": MANIFEST,
                "alpha_core_sha256": "b" * 64,
                "eligible": True,
                "status": "ELIGIBLE",
                "blockers": [],
                "warnings": ["display-only"],
                "evaluated_at": "changes-with-each-read",
                "checks": [
                    {
                        "name": "manifest_identity",
                        "status": "PASS",
                        "severity": "hard",
                        "message": "display-only wording",
                        "context": {"manifest_sha256": MANIFEST},
                    }
                ],
            },
        }


class _HMM:
    def get_snapshot(self, snapshot_id: str):
        if snapshot_id != "hmm_snapshot_001":
            return None
        return {
            "snapshot_id": snapshot_id,
            "config_id": "hmm_config_001",
            "status": "COMPLETED",
            "trained_at": NOW,
            "sector_count": 31,
            "metrics_json": {"log_likelihood": 12.5},
        }

    def get_config(self, config_id: str):
        if config_id != "hmm_config_001":
            return None
        return {"config_json": {"signal_presets": {"preset_A": {"trending": 1.05}}}}


def _policy(algo_code: str) -> ValidatedExecutionPolicy:
    return ValidatedExecutionPolicy(
        policy_id=f"policy_{algo_code.lower()}",
        package_id="pkg_alpha",
        manifest_sha256=MANIFEST,
        policy_name=f"{algo_code} validated policy",
        policy_json={
            "execution_level": "minute",
            "bar_freq": "1m",
            "algo_code": algo_code,
            "algo_config": {},
            "fallback_algo_code": None,
            "fallback_policy": {"on_algo_error": "fail"},
            "unfilled_handler": "carry_to_tail",
            "unfilled_handler_params": {"max_minutes": 10},
        },
        source_backtest_id="bt_001",
        source_backtest_status="SUCCEEDED",
    )


def _request_config(**overrides):
    payload = {
        "schema_version": "localsim_runtime_profile_config_request_v1",
        "daily_strategy": {
            "strategy_id": "daily_small_cap_v1",
            "strategy_version": "2026-08-31",
            "top_k": 20,
            "industry_filters": [],
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
        "notes": None,
        "metadata": {},
    }
    payload.update(overrides)
    return payload


def _services():
    package_repository = _PackageRepository()
    profile_repository = InMemoryLocalSimRuntimeProfileRepository()
    authority = LocalSimProductAuthority(
        profile_repository=profile_repository,
        package_repository=package_repository,
        package_service=_PackageService(package_repository),
        hmm_authority=_HMM(),
    )
    profile_service = LocalSimRuntimeProfileService(
        repository=profile_repository,
        authority=authority,
        clock=lambda: NOW,
    )
    return package_repository, profile_repository, authority, profile_service


def _create_profile_version(profile_service: LocalSimRuntimeProfileService, config: dict | None = None):
    profile = profile_service.create_profile(
        package_id="pkg_alpha",
        manifest_sha256=MANIFEST,
        profile_name="LocalSIM product profile",
        created_by="test",
    )
    _, version = profile_service.create_version(
        profile_id=profile.profile_id,
        expected_profile_version=1,
        config_json=config or _request_config(),
        created_by="test",
    )
    return profile, version


def test_product_authority_resolves_durable_receipt_profile_twap_and_tail() -> None:
    package_repository, _, authority, profile_service = _services()
    profile, version = _create_profile_version(profile_service)

    resolved = authority.resolve_product(
        package_id="pkg_alpha",
        runtime_profile_version_id=version.profile_version_id,
        execution_policy_version_id=package_repository.policy.policy_id,
    )
    repeated = authority.resolve_product(
        package_id="pkg_alpha",
        runtime_profile_version_id=version.profile_version_id,
        execution_policy_version_id=package_repository.policy.policy_id,
    )

    assert resolved.runtime_profile.profile_id == profile.profile_id
    assert resolved.runtime_profile.version == 2
    assert resolved.runtime_profile_version == version
    assert resolved.admission_receipt_id == repeated.admission_receipt_id
    assert resolved.admission_receipt_hash == repeated.admission_receipt_hash
    assert "warnings" not in resolved.admission_receipt_payload["asset_eligibility"]
    assert "evaluated_at" not in resolved.admission_receipt_payload["asset_eligibility"]
    assert resolved.execution_policy.algo_code == "TWAP"
    assert resolved.tail_policy_version_id.startswith("lstail_")
    evidence = resolved.release_validation_evidence()
    assert evidence["admission_receipt"]["payload"] == resolved.admission_receipt_payload
    assert evidence["runtime_profile"]["profile_version_id"] == version.profile_version_id


def test_profile_authority_materializes_exact_validated_variant_and_hmm_reference() -> None:
    package_repository, _, _, profile_service = _services()
    package_repository.variant = SimpleNamespace(
        variant_id="rtv_001",
        variant_hash="c" * 64,
        package_id="pkg_alpha",
        manifest_sha256=MANIFEST,
        validation_status=RuntimeVariantValidationStatus.VALIDATION_PASSED,
        variant_config={"strategy_config": {"top_k": 30}, "notes": "validated"},
        validation_evidence={"test": "passed"},
    )
    profile, version = _create_profile_version(
        profile_service,
        _request_config(
            hmm={
                "enabled": True,
                "snapshot_id": "hmm_snapshot_001",
                "model_version": "hmm_snapshot_001",
                "preset": "preset_A",
                "state_mapping": {"1": "trending"},
            },
            runtime_variant_id="rtv_001",
            runtime_variant_hash="c" * 64,
        ),
    )

    assert profile.package_id == "pkg_alpha"
    assert version.config_json["runtime_variant_materialized_config"] == package_repository.variant.variant_config
    assert version.validation_evidence["hmm_reference"]["reference_sha256"]
    assert version.validation_evidence["runtime_variant"]["variant_id"] == "rtv_001"


def test_product_authority_fails_closed_for_non_twap_and_reference_drift() -> None:
    package_repository, _, authority, profile_service = _services()
    _, version = _create_profile_version(profile_service)
    package_repository.policy = _policy("V25_TWO_STAGE")
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        authority.resolve_product(
            package_id="pkg_alpha",
            runtime_profile_version_id=version.profile_version_id,
            execution_policy_version_id=package_repository.policy.policy_id,
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_TWAP_ONLY_POLICY_REQUIRED"

    package_repository.record.manifest_sha256 = "d" * 64
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        authority.resolve_product(
            package_id="pkg_alpha",
            runtime_profile_version_id=version.profile_version_id,
            execution_policy_version_id=package_repository.policy.policy_id,
        )
    assert exc_info.value.context["reason_code"] in {
        "LOCALSIM_PACKAGE_ADMISSION_DRIFT",
        "LOCALSIM_RUNTIME_PROFILE_PACKAGE_MISMATCH",
    }
