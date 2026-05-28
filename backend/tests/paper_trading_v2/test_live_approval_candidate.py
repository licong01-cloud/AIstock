from __future__ import annotations

from datetime import date

import pytest

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.simulation_runtime import InMemorySimulationRuntimeRepository, StrategyRuntimeReleaseService
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import LiveApprovalStatus, PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime_variant import derive_locked_core_hash
from backend.services.trading_core.errors import LiveApprovalRequiredError
from backend.tests.paper_trading_v2.test_day_runner import save_manifest_with_default_execution_policy
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


TRADE_DATE = date(2026, 5, 20)


def _service_fixture() -> tuple[
    PaperTradingV2PortfolioService,
    InMemoryStrategyPackageRepository,
    InMemoryPaperTradingV2Repository,
    object,
]:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    runtime_repo = InMemorySimulationRuntimeRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.PAPER_ENABLED}))
    save_manifest_with_default_execution_policy(package_repo, manifest)
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
        runtime_release_service=StrategyRuntimeReleaseService(repository=runtime_repo),
    )
    return service, package_repo, paper_repo, manifest


def _evidence() -> tuple[dict, dict]:
    return (
        {
            "paper_v2": {"status": "PASSED", "run_id": "paper_v2_validation_run"},
            "miniqmt_sim": {"status": "VERIFIED", "run_id": "miniqmt_sim_validation_run"},
        },
        {"status": "VERIFIED", "broker_backend": "minqmt_live", "adapter_version": "unit-test"},
    )


def test_paper_v2_live_approval_candidate_requires_runtime_and_execution_activations() -> None:
    service, _package_repo, _paper_repo, manifest = _service_fixture()
    portfolio = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="live approval missing activation",
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    sim_evidence, broker_compatibility = _evidence()

    with pytest.raises(LiveApprovalRequiredError, match="runtime profile activation"):
        service.create_live_approval_candidate(
            portfolio_id=portfolio.portfolio_id,
            trade_date=TRADE_DATE,
            target_broker_backend="minqmt_live",
            sim_validation_evidence=sim_evidence,
            broker_compatibility=broker_compatibility,
        )

    _profile, version = service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="pre-live runtime profile",
        config_json={"runtime_profile": {"selection": {"top_k": 10}, "tradability": {"exclude_suspended": True}}},
        created_by="unit_test",
        reason="runtime profile for live approval",
    )
    service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        profile_version_id=version.profile_version_id,
        activated_by="unit_test",
        reason="activate runtime before live approval",
    )

    with pytest.raises(LiveApprovalRequiredError, match="execution policy activation"):
        service.create_live_approval_candidate(
            portfolio_id=portfolio.portfolio_id,
            trade_date=TRADE_DATE,
            target_broker_backend="minqmt_live",
            sim_validation_evidence=sim_evidence,
            broker_compatibility=broker_compatibility,
        )


def test_paper_v2_live_approval_candidate_binds_immutable_release_hashes() -> None:
    service, _package_repo, _paper_repo, manifest = _service_fixture()
    portfolio = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="live approval complete",
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    profile, version = service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="pre-live runtime profile",
        config_json={"runtime_profile": {"selection": {"top_k": 10}, "tradability": {"exclude_suspended": True}}},
        created_by="unit_test",
        reason="runtime profile for live approval",
    )
    runtime_activation = service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        profile_version_id=version.profile_version_id,
        activated_by="unit_test",
        reason="activate runtime before live approval",
    )
    execution_activation = service.activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        policy_id=portfolio.execution_policy["validated_execution_policy_id"],
        activated_by="unit_test",
        reason="activate execution policy before live approval",
    )
    sim_evidence, broker_compatibility = _evidence()

    approval = service.create_live_approval_candidate(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        target_broker_backend="minqmt_live",
        sim_validation_evidence=sim_evidence,
        broker_compatibility=broker_compatibility,
        broker_account_id="broker_account_unit_test",
        requested_by="unit_requester",
    )

    assert approval.approval_status == LiveApprovalStatus.LIVE_CANDIDATE
    assert approval.manifest_sha256 == manifest.manifest_sha256
    assert approval.alpha_core_sha256 == derive_locked_core_hash(manifest)
    assert approval.runtime_profile_id == profile.profile_id
    assert approval.runtime_profile_version_id == version.profile_version_id
    assert approval.runtime_profile_sha256 == version.config_sha256
    assert approval.execution_policy_id == execution_activation.policy_id
    assert approval.execution_policy_sha256 == execution_activation.policy_sha256
    assert approval.runtime_release_sha256
    assert approval.runtime_release_id.startswith("srr_")
    assert approval.tail_policy_id == "tail_policy:TWAP"
    assert approval.broker_compatibility["target_broker_backend"] == "minqmt_live"
    assert approval.broker_compatibility["simulation_binding_id"].startswith("simbind_")
    assert approval.audit_json["runtime_release"]["metadata"]["runtime_config_activation_id"] == runtime_activation.activation_id
    assert approval.audit_json["runtime_release"]["metadata"]["execution_policy_activation_id"] == execution_activation.activation_id
    assert approval.audit_json["runtime_release"]["daily_strategy"]["profile_version_id"] == (
        "platform_default_daily_strategy_profile_v1"
    )
    assert approval.audit_json["simulation_release_binding"]["release_hash"] == approval.runtime_release_sha256
    assert approval.audit_json["simulation_release_binding"]["binding_config_json"]["broker_backend"] == "minqmt_sim"
    assert "broker_account_id" not in approval.audit_json["runtime_release"]

    pending = service.submit_live_approval(
        package_id=manifest.package_id,
        approval_id=approval.approval_id,
        requested_by="unit_requester",
        risk_note="validated live-like simulation evidence reviewed",
        rollback_plan="disable live order env flag and retire approval",
    )
    approved = service.approve_live_approval(
        package_id=manifest.package_id,
        approval_id=approval.approval_id,
        approved_by="unit_approver",
    )
    listed = service.list_live_approvals(portfolio_id=portfolio.portfolio_id)

    assert pending.approval_status == LiveApprovalStatus.LIVE_APPROVAL_PENDING
    assert approved.approval_status == LiveApprovalStatus.LIVE_APPROVED
    assert [row.approval_id for row in listed] == [approval.approval_id]
