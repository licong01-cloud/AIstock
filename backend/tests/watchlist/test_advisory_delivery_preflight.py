from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.advisory_delivery_preflight import AdvisoryDeliveryPreflightService
from backend.services.advisory_universe import AdvisoryUniverseContractError
from backend.services.strategy_package.models import PackageStatus


@dataclass
class _Record:
    manifest: Any
    package_id: str = "pkg_test"
    manifest_sha256: str = "a" * 64
    package_status: PackageStatus = PackageStatus.SELECTION_ENABLED

    def current_manifest(self) -> Any:
        return self.manifest


class _PackageService:
    def __init__(self, record: _Record, *, eligible: bool = True, blockers: tuple[str, ...] = ()) -> None:
        self.record = record
        self.get_calls: list[str] = []
        eligibility = SimpleNamespace(eligible=eligible, blockers=blockers, warnings=())
        self.asset_eligibility = SimpleNamespace(summarize=lambda _record: eligibility)

    def get_package(self, package_id: str) -> _Record:
        self.get_calls.append(package_id)
        return self.record


class _ProgramService:
    def __init__(self, binding: dict[str, Any]) -> None:
        self.binding = binding
        self.calls: list[str] = []

    def active_binding(self, program_id: str) -> dict[str, Any]:
        self.calls.append(program_id)
        return self.binding

    def get_program(self, program_id: str) -> Any:
        return SimpleNamespace(program_id=program_id, target_count=20)


class _ModelResolver:
    def __init__(self, configured: bool) -> None:
        self.configured = configured
        self.calls: list[dict[str, Any]] = []

    def is_configured(self, **kwargs: Any) -> bool:
        self.calls.append(kwargs)
        return self.configured


def _manifest(*, backtest_universe: Any = None, source_universe: Any = None, backtest_topk: int | None = None) -> Any:
    backtest_context: dict[str, Any] = {}
    source_evidence: dict[str, Any] = {}
    if backtest_universe is not None or backtest_topk is not None:
        custom_params = {"universe_selection": backtest_universe} if backtest_universe is not None else {}
        backtest_context = {"daily_strategy": {"topk": backtest_topk, "custom_params": custom_params}}
    if source_universe is not None:
        source_evidence = {"custom_params": {"universe_selection": source_universe}}
    return SimpleNamespace(
        backtest_context=backtest_context,
        source_evidence=source_evidence,
        source=SimpleNamespace(source_type="qe_experiment", source_id="qe_test"),
    )


def _service(
    manifest: Any,
    *,
    package_status: PackageStatus = PackageStatus.SELECTION_ENABLED,
    eligible: bool = True,
    asset_blockers: tuple[str, ...] = (),
    binding: dict[str, Any] | None = None,
    model_configured: bool = False,
    model_root: str = "",
) -> tuple[AdvisoryDeliveryPreflightService, _PackageService, _ProgramService, _ModelResolver]:
    package_service = _PackageService(
        _Record(manifest=manifest, package_status=package_status),
        eligible=eligible,
        blockers=asset_blockers,
    )
    program_service = _ProgramService(
        binding
        or {
            "binding_version_id": "advb_current",
            "package_mode": "single_package",
            "package_ids": ["pkg_test"],
            "universe_selection": {"mode": "single_index", "pool_ids": ["csi300"]},
        }
    )
    resolver = _ModelResolver(model_configured)
    return (
        AdvisoryDeliveryPreflightService(
            package_service=package_service,
            program_service=program_service,
            model_resolver=resolver,
            model_root_provider=lambda: model_root,
        ),
        package_service,
        program_service,
        resolver,
    )


def test_exact_universe_new_binding_is_ready_baseline_only() -> None:
    universe = {"mode": "single_index", "pool_ids": ["csi300"]}
    service, package_service, program_service, resolver = _service(
        _manifest(backtest_universe=universe, source_universe=universe)
    )

    result = service.preflight(package_id="pkg_test", universe_selection=universe, target_count=20)

    assert result["overall_status"] == "READY_BASELINE_ONLY"
    assert result["universe_compatibility"]["status"] == "EXACT_UNIVERSE_MATCHED"
    assert result["model_compatibility"]["status"] == "REQUIRED_AFTER_BINDING"
    assert package_service.get_calls == ["pkg_test"]
    assert program_service.calls == []
    assert resolver.calls == []


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (
            {"mode": "stock_universe", "pool_ids": []},
            {"mode": "single_index", "pool_ids": ["csi300"]},
        ),
        (
            {"mode": "index_union", "pool_ids": ["csi300", "csi500"]},
            {"mode": "single_index", "pool_ids": ["csi300"]},
        ),
    ],
)
def test_broader_source_universe_is_explicit_filter_only(source: dict[str, Any], target: dict[str, Any]) -> None:
    service, *_ = _service(_manifest(backtest_universe=source))

    result = service.preflight(package_id="pkg_test", universe_selection=target, target_count=20)

    assert result["overall_status"] == "READY_BASELINE_ONLY"
    assert result["universe_compatibility"]["status"] == "FILTER_ONLY_COMPATIBLE"
    assert "ADVISORY_POST_SELECTION_FILTER_NOT_QE_EXACT_UNIVERSE" in result["warnings"]


def test_legacy_package_without_universe_identity_remains_compatible_but_not_exact() -> None:
    service, *_ = _service(_manifest())

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "index_union", "pool_ids": ["csi300", "csi500"]},
        target_count=20,
    )

    assert result["overall_status"] == "READY_BASELINE_ONLY"
    assert result["universe_compatibility"]["status"] == "LEGACY_UNIVERSE_UNSPECIFIED"
    assert result["universe_compatibility"]["source_declared"] is None
    assert "PACKAGE_UNIVERSE_IDENTITY_UNSPECIFIED" in result["warnings"]


def test_restricted_package_cannot_expand_to_an_unseen_pool() -> None:
    service, *_ = _service(
        _manifest(backtest_universe={"mode": "single_index", "pool_ids": ["csi300"]})
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "index_union", "pool_ids": ["csi300", "csi500"]},
        target_count=20,
    )

    assert result["overall_status"] == "BLOCKED"
    assert result["universe_compatibility"]["status"] == "PACKAGE_IDENTITY_MISMATCH"
    assert result["model_compatibility"]["status"] == "NOT_APPLICABLE"


def test_conflicting_frozen_universe_evidence_blocks_delivery() -> None:
    service, *_ = _service(
        _manifest(
            backtest_universe={"mode": "single_index", "pool_ids": ["csi300"]},
            source_universe={"mode": "single_index", "pool_ids": ["csi500"]},
        )
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        target_count=20,
    )

    assert result["overall_status"] == "BLOCKED"
    assert result["universe_compatibility"]["status"] == "DELIVERY_CONTRACT_INCOMPLETE"
    assert result["blockers"] == ["DELIVERY_UNIVERSE_EVIDENCE_CONFLICT"]


def test_audit_only_source_evidence_cannot_replace_primary_frozen_universe_identity() -> None:
    service, *_ = _service(
        _manifest(source_universe={"mode": "single_index", "pool_ids": ["csi300"]})
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        target_count=20,
    )

    assert result["overall_status"] == "BLOCKED"
    assert result["universe_compatibility"]["status"] == "DELIVERY_CONTRACT_INCOMPLETE"
    assert result["blockers"] == ["DELIVERY_UNIVERSE_PRIMARY_EVIDENCE_MISSING"]


def test_same_restricted_pool_with_different_mode_shape_is_compatible_but_not_exact() -> None:
    service, *_ = _service(
        _manifest(backtest_universe={"mode": "index_union", "pool_ids": ["csi300"]})
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        target_count=20,
    )

    assert result["overall_status"] == "READY_BASELINE_ONLY"
    assert result["universe_compatibility"]["status"] == "FILTER_ONLY_COMPATIBLE"


def test_invalid_frozen_universe_shape_blocks_delivery_without_fallback() -> None:
    service, *_ = _service(_manifest(backtest_universe={"mode": "single_index", "pool_ids": []}))

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        target_count=20,
    )

    assert result["overall_status"] == "BLOCKED"
    assert result["universe_compatibility"]["status"] == "DELIVERY_CONTRACT_INCOMPLETE"
    assert result["universe_compatibility"]["evidence_errors"][0]["reason_code"] == "ADVISORY_UNIVERSE_SELECTION_INVALID"


@pytest.mark.parametrize(
    ("package_status", "eligible", "asset_blockers", "expected_blocker"),
    [
        (PackageStatus.RETIRED, True, (), "PACKAGE_RETIRED"),
        (PackageStatus.SELECTION_ENABLED, False, ("asset_hash_invalid",), "PACKAGE_ASSET_INELIGIBLE:asset_hash_invalid"),
    ],
)
def test_retired_or_asset_ineligible_package_is_blocked(
    package_status: PackageStatus,
    eligible: bool,
    asset_blockers: tuple[str, ...],
    expected_blocker: str,
) -> None:
    service, *_ = _service(
        _manifest(),
        package_status=package_status,
        eligible=eligible,
        asset_blockers=asset_blockers,
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "stock_universe", "pool_ids": []},
        target_count=20,
    )

    assert result["overall_status"] == "BLOCKED"
    assert expected_blocker in result["blockers"]


@pytest.mark.parametrize(
    ("configured", "expected_overall", "expected_model"),
    [
        (True, "READY_WITH_MODEL", "DESCRIPTOR_FILE_PRESENT"),
        (False, "READY_BASELINE_ONLY", "MODEL_DESCRIPTOR_UNAVAILABLE"),
    ],
)
def test_unchanged_active_binding_checks_descriptor_presence_only(
    configured: bool,
    expected_overall: str,
    expected_model: str,
) -> None:
    universe = {"mode": "single_index", "pool_ids": ["csi300"]}
    service, _packages, programs, resolver = _service(
        _manifest(backtest_universe=universe, backtest_topk=50),
        model_configured=configured,
        model_root="F:/models",
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection=universe,
        target_count=20,
        program_id="advp_current",
    )

    assert result["overall_status"] == expected_overall
    assert result["model_compatibility"]["status"] == expected_model
    assert result["model_compatibility"]["validation_stage"] == "PRESENCE_ONLY"
    assert result["policy_compatibility"] == {
        "status": "ACTIVE_POLICY_MATCH",
        "requested_target_count": 20,
        "active_target_count": 20,
        "package_backtest_topk": 50,
        "package_policy_authority": "DIAGNOSTIC_ONLY_NOT_ADVISORY_RUNTIME_AUTHORITY",
    }
    assert programs.calls == ["advp_current"]
    assert resolver.calls[0]["binding_version_id"] == "advb_current"


def test_changed_binding_does_not_probe_a_descriptor_for_the_old_identity() -> None:
    service, _packages, _programs, resolver = _service(
        _manifest(backtest_universe={"mode": "stock_universe", "pool_ids": []}),
        model_root="F:/models",
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection={"mode": "stock_universe", "pool_ids": []},
        target_count=20,
        program_id="advp_current",
    )

    assert result["model_compatibility"]["status"] == "REQUIRED_AFTER_BINDING"
    assert resolver.calls == []


def test_target_count_change_requires_a_new_policy_binding_before_descriptor_probe() -> None:
    universe = {"mode": "single_index", "pool_ids": ["csi300"]}
    service, _packages, _programs, resolver = _service(
        _manifest(backtest_universe=universe),
        model_root="F:/models",
        model_configured=True,
    )

    result = service.preflight(
        package_id="pkg_test",
        universe_selection=universe,
        target_count=5,
        program_id="advp_current",
    )

    assert result["policy_compatibility"] == {
        "status": "NEW_POLICY_BINDING_REQUIRED",
        "requested_target_count": 5,
        "active_target_count": 20,
        "package_backtest_topk": None,
        "package_policy_authority": "DIAGNOSTIC_ONLY_NOT_ADVISORY_RUNTIME_AUTHORITY",
    }
    assert result["model_compatibility"]["status"] == "REQUIRED_AFTER_BINDING"
    assert resolver.calls == []


def test_requested_universe_contract_is_validated_before_package_read() -> None:
    service, packages, *_ = _service(_manifest())

    with pytest.raises(AdvisoryUniverseContractError):
        service.preflight(
            package_id="pkg_test",
            universe_selection={"mode": "single_index", "pool_ids": []},
            target_count=20,
        )

    assert packages.get_calls == []
