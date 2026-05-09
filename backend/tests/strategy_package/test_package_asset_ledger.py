import inspect
import re
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.routers.strategy_packages as strategy_package_router
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository, StrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend" / "migrations" / "strategy_pkg_package_asset_20260509.sql"


def _migration_sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _service_with_manifest() -> tuple[StrategyPackageService, str]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    return StrategyPackageService(repository=repo), manifest.package_id


def test_phase2_package_asset_migration_is_additive_and_commented() -> None:
    sql = _migration_sql()

    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.package_asset" in sql
    assert "ALTER TABLE strategy_pkg.package_asset" in sql
    assert "DROP " not in sql.upper()
    assert "COMMENT ON TABLE strategy_pkg.package_asset" in sql
    for column in ["asset_id", "package_id", "asset_type", "asset_ref", "asset_sha256", "metadata", "created_at"]:
        assert f"COMMENT ON COLUMN strategy_pkg.package_asset.{column}" in sql
    for column in ["asset_role", "asset_size_bytes", "protected_asset", "source_uri"]:
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
        assert f"COMMENT ON COLUMN strategy_pkg.package_asset.{column}" in sql
    assert re.search(r"CREATE UNIQUE INDEX IF NOT EXISTS idx_package_asset_package_ref", sql)


def test_package_asset_ledger_records_protected_metadata_without_touching_assets() -> None:
    service, package_id = _service_with_manifest()
    before_manifest = service.get_package(package_id).manifest_sha256

    asset = service.record_package_asset(
        package_id,
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref="rdagent_assets/strategy_package_runtime_dev/pkg/model.pkl",
        asset_sha256="sha256:unit-test-model",
        asset_size_bytes=1234,
        metadata={"copy_status": "already_controlled"},
        source_uri="qe://experiment/Loop1/model.pkl",
    )
    listed = service.list_package_assets(package_id, protected_only=True)
    after_manifest = service.get_package(package_id).manifest_sha256

    assert asset.asset_id == 1
    assert asset.protected_asset is True
    assert listed[0].asset_sha256 == "sha256:unit-test-model"
    assert before_manifest == after_manifest


def test_package_asset_ledger_upserts_by_package_type_and_ref() -> None:
    service, package_id = _service_with_manifest()

    first = service.record_package_asset(
        package_id,
        asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
        asset_ref="schemas/features.json",
        asset_sha256="sha256:old",
    )
    second = service.record_package_asset(
        package_id,
        asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
        asset_ref="schemas/features.json",
        asset_sha256="sha256:new",
        protected_asset=False,
    )

    assert first.asset_id == second.asset_id
    assert service.list_package_assets(package_id)[0].asset_sha256 == "sha256:new"
    assert service.list_package_assets(package_id, protected_only=True) == []


def test_package_asset_router_exposes_record_and_list(monkeypatch) -> None:
    class FakeService:
        def __init__(self) -> None:
            self.service, self.real_package_id = _service_with_manifest()

        def record_package_asset(self, package_id, **kwargs):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert kwargs["asset_type"] == StrategyPackageAssetType.VALIDATION_REPORT
            return self.service.record_package_asset(self.real_package_id, **kwargs)

        def list_package_assets(self, package_id, *, protected_only=False):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert protected_only is True
            return []

    monkeypatch.setattr(strategy_package_router, "StrategyPackageService", lambda: FakeService())
    app = FastAPI()
    app.include_router(strategy_package_router.router)
    client = TestClient(app)

    created = client.post(
        "/strategy-packages/pkg_1/assets",
        json={
            "asset_type": "validation_report",
            "asset_ref": "reports/original_retest.json",
            "asset_sha256": "sha256:report",
        },
    )
    listed = client.get("/strategy-packages/pkg_1/assets?protected_only=true")

    assert created.status_code == 200
    assert created.json()["asset"]["protected_asset"] is True
    assert listed.status_code == 200
    assert listed.json()["assets"] == []


def test_postgres_package_asset_repository_does_not_update_manifest_or_delete_assets() -> None:
    save_source = inspect.getsource(StrategyPackageRepository.save_package_asset)
    list_source = inspect.getsource(StrategyPackageRepository.list_package_assets)

    assert "strategy_pkg.package_asset" in save_source
    assert "UPDATE strategy_pkg.package\n" not in save_source
    assert "DELETE" not in save_source.upper()
    assert "strategy_pkg.package_asset" in list_source
