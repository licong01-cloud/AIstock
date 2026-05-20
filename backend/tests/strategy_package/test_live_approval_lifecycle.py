from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import LiveApprovalStatus, PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime_variant import derive_locked_core_hash
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


REPO_ROOT = Path(__file__).resolve().parents[3]
TRADING_CORE_MIGRATION = REPO_ROOT / "backend" / "migrations" / "trading_core_v2_schema.sql"
TRADING_CORE_INIT = REPO_ROOT / "backend" / "db" / "init_trading_core_v2_schema.py"


def _service_with_manifest(
    *,
    status: PackageStatus = PackageStatus.BACKTEST_APPROVED,
) -> tuple[StrategyPackageService, object]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": status}))
    repo.save_manifest(manifest)
    return StrategyPackageService(repository=repo), manifest


def _approval_kwargs(manifest, *, runtime_release_sha256: str = "sha256:runtime-release") -> dict:
    return {
        "package_id": manifest.package_id,
        "manifest_sha256": manifest.manifest_sha256,
        "alpha_core_sha256": derive_locked_core_hash(manifest),
        "runtime_release_id": "live_release_unit_test",
        "runtime_release_sha256": runtime_release_sha256,
        "runtime_profile_id": "runtime_profile_unit_test",
        "runtime_profile_version_id": "runtime_profile_version_unit_test",
        "runtime_profile_sha256": "sha256:runtime-profile",
        "execution_policy_id": "execution_policy_unit_test",
        "execution_policy_sha256": "sha256:execution-policy",
        "tail_policy_id": "tail_policy:TWAP",
        "tail_policy_sha256": "sha256:tail-policy",
        "target_broker_backend": "minqmt_live",
        "portfolio_id": "paper_unit_test",
        "broker_account_id": "broker_unit_test",
        "sim_validation_evidence": {
            "paper_v2": {"status": "PASSED", "run_id": "paper_run_unit_test"},
            "miniqmt_sim": {"status": "VERIFIED", "run_id": "miniqmt_sim_unit_test"},
        },
        "broker_compatibility": {
            "status": "VERIFIED",
            "target_broker_backend": "minqmt_live",
            "adapter": "xtquant",
        },
    }


def test_live_approval_requires_sim_evidence_and_broker_compatibility() -> None:
    service, manifest = _service_with_manifest()
    kwargs = _approval_kwargs(manifest)
    kwargs["sim_validation_evidence"] = {"paper_v2": {"status": "PASSED"}}

    with pytest.raises(StrategyPackageValidationError, match="Paper/MiniQMT SIM validation") as exc_info:
        service.create_live_approval_candidate(**kwargs)

    assert "missing_sim_validation_evidence" in exc_info.value.context["blockers"]
    assert exc_info.value.context["missing_sim_validation_keys"] == ["miniqmt_sim"]


def test_live_approval_lifecycle_is_auditable_and_required_for_live() -> None:
    service, manifest = _service_with_manifest()
    approval = service.create_live_approval_candidate(**_approval_kwargs(manifest))

    with pytest.raises(StrategyPackageValidationError, match="approved StrategyPackage live approval"):
        service.require_live_approval(
            package_id=manifest.package_id,
            approval_id=approval.approval_id,
            runtime_release_sha256=approval.runtime_release_sha256,
            target_broker_backend="minqmt_live",
        )

    pending = service.submit_live_approval(
        package_id=manifest.package_id,
        approval_id=approval.approval_id,
        requested_by="unit_requester",
        risk_note="validated paper and MiniQMT SIM evidence reviewed",
        rollback_plan="disable live env switch and retire approval",
    )
    approved = service.approve_live_approval(
        package_id=manifest.package_id,
        approval_id=approval.approval_id,
        approved_by="unit_approver",
    )
    required = service.require_live_approval(
        package_id=manifest.package_id,
        approval_id=approval.approval_id,
        manifest_sha256=manifest.manifest_sha256,
        runtime_release_sha256=approval.runtime_release_sha256,
        target_broker_backend="minqmt_live",
    )

    assert pending.approval_status == LiveApprovalStatus.LIVE_APPROVAL_PENDING
    assert approved.approval_status == LiveApprovalStatus.LIVE_APPROVED
    assert required.approval_id == approval.approval_id
    assert required.approved_by == "unit_approver"
    assert [event["action"] for event in required.audit_json["events"]] == [
        "submit_live_approval",
        "approve_live_approval",
    ]

    retired = service.retire_live_approval(
        package_id=manifest.package_id,
        approval_id=approval.approval_id,
        retired_by="unit_operator",
        retirement_reason="rollback after validation window",
    )
    assert retired.approval_status == LiveApprovalStatus.LIVE_RETIRED
    with pytest.raises(StrategyPackageValidationError) as exc_info:
        service.require_live_approval(
            package_id=manifest.package_id,
            approval_id=approval.approval_id,
            runtime_release_sha256=approval.runtime_release_sha256,
            target_broker_backend="minqmt_live",
        )
    assert "approval_status=LIVE_RETIRED" in exc_info.value.context["blockers"]


def test_rejected_approval_and_paper_status_alone_do_not_grant_live_access() -> None:
    service, manifest = _service_with_manifest(status=PackageStatus.PAPER_PASSED)

    with pytest.raises(DataUnavailableError):
        service.require_live_approval(
            package_id=manifest.package_id,
            approval_id="paper_status_only_is_not_live_approval",
            target_broker_backend="minqmt_live",
        )

    rejected = service.create_live_approval_candidate(**_approval_kwargs(manifest, runtime_release_sha256="sha256:reject"))
    rejected = service.reject_live_approval(
        package_id=manifest.package_id,
        approval_id=rejected.approval_id,
        rejected_by="unit_reviewer",
        rejection_reason="MiniQMT SIM evidence period is insufficient for live admission",
    )

    assert rejected.approval_status == LiveApprovalStatus.LIVE_REJECTED
    with pytest.raises(StrategyPackageValidationError) as exc_info:
        service.require_live_approval(
            package_id=manifest.package_id,
            approval_id=rejected.approval_id,
            runtime_release_sha256=rejected.runtime_release_sha256,
            target_broker_backend="minqmt_live",
        )
    assert "approval_status=LIVE_REJECTED" in exc_info.value.context["blockers"]


def test_live_approval_schema_has_comments_in_migration_and_bootstrap() -> None:
    migration = TRADING_CORE_MIGRATION.read_text(encoding="utf-8")
    bootstrap = TRADING_CORE_INIT.read_text(encoding="utf-8")
    columns = [
        "approval_id",
        "package_id",
        "manifest_sha256",
        "alpha_core_sha256",
        "portfolio_id",
        "runtime_release_id",
        "runtime_release_sha256",
        "runtime_profile_id",
        "runtime_profile_version_id",
        "runtime_profile_sha256",
        "execution_policy_id",
        "execution_policy_sha256",
        "tail_policy_id",
        "tail_policy_sha256",
        "target_broker_backend",
        "broker_account_id",
        "approval_status",
        "sim_validation_evidence",
        "broker_compatibility",
        "risk_note",
        "rollback_plan",
        "requested_by",
        "requested_at",
        "approved_by",
        "approved_at",
        "rejected_by",
        "rejected_at",
        "rejection_reason",
        "retired_by",
        "retired_at",
        "retirement_reason",
        "audit_json",
        "created_at",
        "updated_at",
    ]

    for ddl in (migration, bootstrap):
        assert "CREATE TABLE IF NOT EXISTS strategy_pkg.live_approval" in ddl
        assert "COMMENT ON TABLE strategy_pkg.live_approval" in ddl
        assert "idx_strategy_pkg_live_approval_package_status" in ddl
        assert "idx_strategy_pkg_live_approval_portfolio_status" in ddl
        for column in columns:
            assert f"COMMENT ON COLUMN strategy_pkg.live_approval.{column}" in ddl
