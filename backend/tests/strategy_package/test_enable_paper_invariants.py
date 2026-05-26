"""Invariant tests for enable_paper() compatibility behavior.

Paper v2 gate decoupling keeps legacy PAPER_ENABLED as compatibility metadata,
but immutable package asset identity must still fail fast.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import (
    InMemoryStrategyPackageRepository,
    StrategyPackageRecord,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import PackageAssetInvalidError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def test_enable_paper_raises_on_manifest_sha256_mismatch() -> None:
    """enable_paper must fail fast when frozen manifest identity is tampered."""

    repo = InMemoryStrategyPackageRepository()
    correct = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    bad_digest = "b" * 64
    assert correct.manifest_sha256 != bad_digest, "fixture digest collision"
    tampered_manifest = correct.model_copy(update={"manifest_sha256": bad_digest})

    now = datetime.now(timezone.utc)
    record = StrategyPackageRecord(
        package_id=tampered_manifest.package_id,
        package_name=tampered_manifest.package_name,
        package_version=tampered_manifest.package_version,
        source_type=tampered_manifest.source.source_type.value,
        source_id=tampered_manifest.source.source_id,
        loop_id=tampered_manifest.source.loop_id,
        run_id=tampered_manifest.source.run_id,
        package_status=PackageStatus.BACKTEST_APPROVED,
        manifest=tampered_manifest,
        manifest_sha256=bad_digest,
        created_at=now,
        updated_at=now,
    )
    repo.records[record.package_id] = record

    service = StrategyPackageService(repository=repo)

    with pytest.raises(PackageAssetInvalidError) as exc_info:
        service.enable_paper(record.package_id)

    err = exc_info.value
    context = err.context or {}
    blockers_str = " ".join(context.get("blockers") or [])
    check_messages = " ".join(str(check.get("message", "")) for check in context.get("checks") or [])
    assert err.error_code == "PACKAGE_ASSET_INVALID"
    assert context.get("eligible") is False
    assert "manifest_hash" in blockers_str or "manifest_sha256" in check_messages
    assert context.get("manifest_sha256") == bad_digest
    assert context.get("status") == "BLOCKED"


def test_enable_paper_treats_legacy_paper_enabled_as_noop() -> None:
    """Legacy PAPER_ENABLED must not re-enter the deprecated status transition."""

    repo = InMemoryStrategyPackageRepository()
    saved_manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    record = repo.save_manifest(saved_manifest)

    service = StrategyPackageService(repository=repo)
    repo.records[record.package_id] = repo.get(record.package_id).model_copy(
        update={"package_status": PackageStatus.PAPER_ENABLED}
    )

    paper = service.enable_paper(record.package_id)

    assert paper.package_status == PackageStatus.PAPER_ENABLED
    assert [event.reason for event in repo.list_status_events(record.package_id)] == ["package_created"]
