from __future__ import annotations

from datetime import date

import pytest

from backend.services.simulation_runtime.models import (
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
)
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
from backend.services.simulation_runtime.service import StrategyRuntimeReleaseService
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import FactorAsset, ModelAsset
from backend.tests.strategy_package.test_manifest_v1 import make_manifest
from scripts import strategy_package_binding_refreeze as refreeze


ACTIVE_ON = date(2026, 7, 1)
OLD_SHA = "b3fa7f6eed5cf929c79ad1726ade31eb80a9ad54f45bfad764c6ef52a9fe0dfe"


def _manifest_pair() -> tuple[object, object]:
    old = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_id": "pkg_unit",
                "package_name": "pkg_unit",
                "manifest_sha256": None,
            }
        )
    )
    current = freeze_manifest(
        old.model_copy(
            update={
                "factor_set": [
                    FactorAsset(
                        factor_id="factor_a",
                        factor_name="factor_a",
                        asset_ref="aistock-package-asset://factor-a",
                        sha256="a" * 64,
                        size_bytes=10,
                        source_uri="qe-workspace://factor-a",
                    ),
                    FactorAsset(
                        factor_id="factor_b",
                        factor_name="factor_b",
                        asset_ref="aistock-package-asset://factor-b",
                        sha256="b" * 64,
                        size_bytes=20,
                        source_uri="qe-workspace://factor-b",
                    ),
                ],
                "model_asset": ModelAsset(
                    model_id="model_1",
                    asset_ref="aistock-package-asset://model-1",
                    sha256="c" * 64,
                    size_bytes=30,
                    source_uri="qe-workspace://model-1",
                ),
                "manifest_sha256": None,
            }
        )
    )
    return old, current


def _snapshot(manifest, *, stored_sha: str | None = None) -> refreeze.PackageSnapshot:  # noqa: ANN001
    sha = stored_sha or manifest.manifest_sha256 or ""
    return refreeze.PackageSnapshot(
        package_id=manifest.package_id,
        manifest_sha256=sha,
        manifest_json=manifest.model_dump(mode="json"),
        package_status=manifest.package_status.value,
        manifest=manifest.model_copy(update={"manifest_sha256": sha}),
    )


def _repo_with_binding(
    *,
    manifest_sha256: str,
    approval_state: SimulationBindingApprovalState = SimulationBindingApprovalState.SIM_VALIDATING,
    broker_backend: SimulationBrokerBackend = SimulationBrokerBackend.MINIQMT_SIM,
    effective_to: date | None = None,
) -> tuple[InMemorySimulationRuntimeRepository, str]:
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    release = service.create_release(
        package_id="pkg_unit",
        manifest_sha256=manifest_sha256,
        runtime_profile_id="runtime_profile",
        runtime_profile_version_id="runtime_profile_v1",
        runtime_profile_sha256="1" * 64,
        daily_strategy_profile_version_id="daily_profile_v1",
        execution_policy_version_id="execution_policy_v1",
        execution_policy_sha256="2" * 64,
        tail_policy_version_id="tail_policy_v1",
        tail_policy_sha256="3" * 64,
        validation_state=RuntimeReleaseValidationState.SIM_VALIDATING,
        effective_from=date(2026, 6, 1),
        effective_to=effective_to,
        created_by="pytest",
    )
    binding = service.create_binding(
        strategy_id="strategy_l2",
        release=release,
        broker_backend=broker_backend,
        capital_allocation=100000.0,
        broker_account_id="62266303",
        account_group_id="ag_minqmt_62266303_sim" if broker_backend == SimulationBrokerBackend.MINIQMT_SIM else None,
        strategy_slot_id="codex_final_ms_l2_20260603",
        strategy_name="codex_final_ms_l2_20260603",
        approval_state=approval_state,
        effective_from=date(2026, 6, 1),
        effective_to=effective_to,
        created_by="pytest",
    )
    return repo, binding.binding_id


def _empty_portfolios(_package_id: str, _old_sha: str, _backend: str) -> list[refreeze.PortfolioRefreezeCandidate]:
    return []


def test_mismatch_binding_plans_new_release_and_binding_without_saving() -> None:
    old, current = _manifest_pair()
    repo, binding_id = _repo_with_binding(manifest_sha256=old.manifest_sha256 or "")
    repo.save_strategy_runtime_release = pytest.fail  # type: ignore[method-assign]
    repo.save_simulation_release_binding = pytest.fail  # type: ignore[method-assign]

    report = refreeze.build_refreeze_plan(
        repo,
        package_loader=lambda _package_id: _snapshot(current),
        portfolio_loader=lambda _package_id, old_sha, _backend: [
            refreeze.PortfolioRefreezeCandidate(
                portfolio_id="paper_auto",
                portfolio_name="auto",
                status="READY",
                auto_run_enabled=True,
                old_manifest_sha256=old_sha,
            )
        ],
        binding_ids=[binding_id],
        active_on=ACTIVE_ON,
        limit=10,
        operator="pytest",
    )

    item = report["items"][0]
    assert report["counts"] == {"scanned_bindings": 1, "planned_refreeze": 1, "skipped": 0, "portfolio_updates": 1}
    assert item["action"] == "planned_refreeze"
    assert item["old_manifest_sha256"] == old.manifest_sha256
    assert item["current_manifest_sha256"] == current.manifest_sha256
    assert item["new_release_id"].startswith("srr_")
    assert item["new_binding_id"].startswith("simbind_")
    assert item["portfolio_updates"][0]["portfolio_id"] == "paper_auto"
    assert report["db_writes_executed"] is False


def test_already_matched_binding_skips_without_duplicate_release() -> None:
    _old, current = _manifest_pair()
    repo, binding_id = _repo_with_binding(manifest_sha256=current.manifest_sha256 or "")

    report = refreeze.build_refreeze_plan(
        repo,
        package_loader=lambda _package_id: _snapshot(current),
        portfolio_loader=_empty_portfolios,
        binding_ids=[binding_id],
        active_on=ACTIVE_ON,
        limit=10,
        operator="pytest",
    )

    assert report["counts"]["planned_refreeze"] == 0
    assert report["items"][0]["action"] == "skip"
    assert report["items"][0]["reason_code"] == "BINDING_ALREADY_CURRENT"


def test_package_manifest_self_inconsistent_raises_loudly() -> None:
    old, current = _manifest_pair()
    repo, binding_id = _repo_with_binding(manifest_sha256=old.manifest_sha256 or "")

    with pytest.raises(refreeze.BindingRefreezeScriptError) as excinfo:
        refreeze.build_refreeze_plan(
            repo,
            package_loader=lambda _package_id: _snapshot(current, stored_sha="d" * 64),
            portfolio_loader=_empty_portfolios,
            binding_ids=[binding_id],
            active_on=ACTIVE_ON,
            limit=10,
            operator="pytest",
        )

    assert excinfo.value.context["reason_code"] == "PACKAGE_MANIFEST_SELF_INCONSISTENT"


def test_live_binding_is_rejected() -> None:
    _old, current = _manifest_pair()
    repo, binding_id = _repo_with_binding(
        manifest_sha256=current.manifest_sha256 or "",
        approval_state=SimulationBindingApprovalState.LIVE_APPROVED,
    )

    with pytest.raises(refreeze.BindingRefreezeScriptError) as excinfo:
        refreeze.build_refreeze_plan(
            repo,
            package_loader=lambda _package_id: _snapshot(current),
            portfolio_loader=_empty_portfolios,
            binding_ids=[binding_id],
            active_on=ACTIVE_ON,
            limit=10,
            operator="pytest",
        )

    assert excinfo.value.context["reason_code"] == "BINDING_REFREEZE_LIVE_BINDING_REJECTED"


def test_historical_binding_is_not_selected_by_default() -> None:
    old, current = _manifest_pair()
    repo, _binding_id = _repo_with_binding(
        manifest_sha256=old.manifest_sha256 or "",
        effective_to=date(2026, 6, 30),
    )

    report = refreeze.build_refreeze_plan(
        repo,
        package_loader=lambda _package_id: _snapshot(current),
        portfolio_loader=_empty_portfolios,
        active_on=ACTIVE_ON,
        limit=10,
        operator="pytest",
    )

    assert report["counts"] == {"scanned_bindings": 0, "planned_refreeze": 0, "skipped": 0, "portfolio_updates": 0}
    assert report["items"] == []


def test_same_day_binding_plans_next_day_replacement_without_invalid_window() -> None:
    old, current = _manifest_pair()
    repo, binding_id = _repo_with_binding(
        manifest_sha256=old.manifest_sha256 or "",
        effective_to=ACTIVE_ON,
    )
    source = repo.get_simulation_release_binding(binding_id)
    repo.bindings[binding_id] = source.model_copy(update={"effective_from": ACTIVE_ON, "effective_to": ACTIVE_ON})

    report = refreeze.build_refreeze_plan(
        repo,
        package_loader=lambda _package_id: _snapshot(current),
        portfolio_loader=_empty_portfolios,
        binding_ids=[binding_id],
        active_on=ACTIVE_ON,
        limit=10,
        operator="pytest",
    )

    item = report["items"][0]
    assert item["action"] == "planned_refreeze"
    assert item["effective_from"] == "2026-07-02"
    assert item["old_binding_effective_to_after"] == "2026-07-01"
    assert item["details"]["window_policy"] == "future_replacement_preserves_same_day_source_window"


def test_apply_gate_requires_prod_flag_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    args = refreeze.argparse.Namespace(
        apply=True,
        target_db=refreeze.TARGET_PROD,
        confirm_production_dml=False,
        confirm_scratch_dml=False,
    )

    with pytest.raises(refreeze.BindingRefreezeScriptError, match="confirm-production-dml"):
        refreeze._validate_apply_gate(args)  # noqa: SLF001

    args.confirm_production_dml = True
    monkeypatch.delenv(refreeze.APPLY_CONFIRM_ENV, raising=False)
    with pytest.raises(refreeze.BindingRefreezeScriptError, match=refreeze.APPLY_CONFIRM_ENV):
        refreeze._validate_apply_gate(args)  # noqa: SLF001

    monkeypatch.setenv(refreeze.APPLY_CONFIRM_ENV, refreeze.APPLY_CONFIRM_VALUE)
    refreeze._validate_apply_gate(args)  # noqa: SLF001

