import inspect
import re
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.routers.strategy_packages as strategy_package_router
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from backend.services.strategy_package.repository import (
    InMemoryStrategyPackageRepository, StrategyPackageRepository, manifest_asset_keys,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.strategy_package.test_frozen_preprocessor_assets import freeze
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


@pytest.mark.parametrize("processors", [False, True])
def test_frozen_package_persists_exact_protected_asset_closure(tmp_path, processors):
    kwargs = {} if processors else {"conf": b"task: {}\n"}
    store, result = freeze(tmp_path, **kwargs)
    repo = InMemoryStrategyPackageRepository()
    before = result.manifest.model_dump(mode="json")
    record = repo.save_manifest_with_assets(result.manifest, result.assets)
    rows = repo.list_package_assets(record.package_id, protected_only=True)
    assert record.manifest.model_dump(mode="json") == before
    assert len(rows) == len(result.assets)
    assert {(a.asset_type, a.asset_ref, a.asset_sha256) for a in rows} == manifest_asset_keys(record.manifest)
    fitted = record.manifest.model_asset.preprocessor_asset
    fitted_rows = [a for a in rows if a.asset_type == StrategyPackageAssetType.PREPROCESSOR]
    if processors:
        assert fitted is not None and len(fitted_rows) == 1
        assert fitted_rows[0].asset_size_bytes == fitted.size_bytes
        assert len(store.get(fitted.asset_ref)) == fitted.size_bytes
    else:
        assert fitted is None and fitted_rows == []
    # Identical saves reuse both package and ledger identities.
    assert repo.save_manifest_with_assets(result.manifest, result.assets).package_id == record.package_id
    assert repo.list_package_assets(record.package_id, protected_only=True) == rows


def test_all_model_preprocessors_belong_to_manifest_asset_closure(tmp_path):
    _, result = freeze(tmp_path)
    first = result.manifest.model_asset
    processor = first.preprocessor_asset.model_copy(update={"asset_ref": "unit://second-dataset", "sha256": "f" * 64})
    second = first.model_copy(update={"model_id": "model_2", "preprocessor_asset": processor})
    manifest = result.manifest.model_copy(update={"model_asset": [first, second]})
    rows = result.assets + [next(a for a in result.assets if a.asset_type == StrategyPackageAssetType.PREPROCESSOR).model_copy(
        update={"asset_ref": processor.asset_ref, "asset_sha256": processor.sha256},
    )]
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest_with_assets(manifest, rows)
    assert len([a for a in repo.list_package_assets(record.package_id, protected_only=True)
                if a.asset_type == StrategyPackageAssetType.PREPROCESSOR]) == 2


@pytest.mark.parametrize("repository", ["memory", "postgres"])
@pytest.mark.parametrize("case,reason", [
    ("missing", "strategy_package_assets_incomplete"),
    ("wrong_sha", "strategy_package_assets_incomplete"),
    ("wrong_ref", "strategy_package_assets_incomplete"),
    ("undeclared", "strategy_package_assets_unexpected"),
    ("wrong_package", "strategy_package_asset_package_mismatch"),
    ("empty_manifest_ref", "strategy_package_assets_incomplete"),
    ("empty_manifest_sha", "strategy_package_assets_incomplete"),
    ("extra_report", "strategy_package_assets_unexpected"),
    ("prediction", "strategy_package_prediction_asset_forbidden"),
])
def test_invalid_fitted_asset_closure_fails_before_any_persistence(tmp_path, repository, case, reason):
    _, result = freeze(tmp_path)
    manifest, rows = result.manifest, list(result.assets)
    index = next(i for i, row in enumerate(rows) if row.asset_type == StrategyPackageAssetType.PREPROCESSOR)
    if case == "missing":
        rows.pop(index)
    elif case in {"wrong_sha", "wrong_ref", "wrong_package"}:
        field, value = {"wrong_sha": ("asset_sha256", "0" * 64), "wrong_ref": ("asset_ref", "unit://wrong"),
                        "wrong_package": ("package_id", "pkg_other")}[case]
        rows[index] = rows[index].model_copy(update={field: value})
    elif case == "extra_report":
        rows.append(StrategyPackageAssetRecord(package_id=manifest.package_id,
                    asset_type=StrategyPackageAssetType.VALIDATION_REPORT, asset_ref="unit://report", asset_sha256="a" * 64))
    elif case == "prediction":
        ref = "unit://combined_prediction.pkl"
        processor = manifest.model_asset.preprocessor_asset.model_copy(update={"asset_ref": ref})
        model = manifest.model_asset.model_copy(update={"preprocessor_asset": processor})
        manifest = manifest.model_copy(update={"model_asset": model})
        rows[index] = rows[index].model_copy(update={"asset_ref": ref})
    else:
        processor = manifest.model_asset.preprocessor_asset
        processor = None if case == "undeclared" else processor.model_copy(
            update={"asset_ref" if case == "empty_manifest_ref" else "sha256": ""},
        )
        model = manifest.model_asset.model_copy(update={"preprocessor_asset": processor})
        manifest = manifest.model_copy(update={"model_asset": model})
    repo = InMemoryStrategyPackageRepository() if repository == "memory" else StrategyPackageRepository(
        conn_factory=lambda: pytest.fail("invalid closure must fail before DB access"),
    )
    with pytest.raises(StrategyPackageValidationError) as caught:
        repo.save_manifest_with_assets(manifest, rows)
    assert caught.value.context["reason_code"] == reason
    if repository == "memory":
        assert repo.records == {} and repo.package_assets == {}
