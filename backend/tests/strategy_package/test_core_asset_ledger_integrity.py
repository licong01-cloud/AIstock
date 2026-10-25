"""Manifest-owned ledger identity/protection is enforced at every write boundary."""

from contextlib import contextmanager

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import Alpha158SchemaAsset, ModelCodeAsset, RuntimeAssetManifest
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from backend.services.strategy_package.repository import (
    InMemoryStrategyPackageRepository,
    StrategyPackageRepository,
    _validate_asset_records_for_manifest,
    manifest_asset_ledger_covers,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import InvalidStateTransitionError, StrategyPackageValidationError
from backend.tests.strategy_package.test_frozen_preprocessor_assets import freeze


@pytest.mark.parametrize("change", [{"protected_asset": False}, {"asset_size_bytes": 0}, {"asset_size_bytes": None}])
def test_atomic_freeze_rejects_invalid_core_ledger_before_any_persistence(tmp_path, change):
    _, frozen = freeze(tmp_path)
    broken = [frozen.assets[0].model_copy(update=change), *frozen.assets[1:]]
    repo = InMemoryStrategyPackageRepository()
    with pytest.raises(StrategyPackageValidationError):
        repo.save_manifest_with_assets(frozen.manifest, broken)
    assert repo.records == repo.package_assets == {}


@pytest.mark.parametrize(
    "change",
    [{"asset_sha256": "0" * 64}, {"protected_asset": False}, {"asset_size_bytes": 0}, {"asset_size_bytes": None}],
)
def test_public_registration_cannot_rewrite_manifest_owned_identity(tmp_path, change):
    _, frozen = freeze(tmp_path)
    repo = InMemoryStrategyPackageRepository()
    repo.save_manifest_with_assets(frozen.manifest, frozen.assets)
    original = frozen.assets[0]
    changed = original.model_copy(update=change)
    service = StrategyPackageService(repository=repo)
    with pytest.raises(StrategyPackageValidationError):
        service.record_package_asset(
            original.package_id, **changed.model_dump(exclude={"package_id", "asset_id", "created_at"})
        )
    saved = next(row for row in repo.list_package_assets(original.package_id) if row.asset_ref == original.asset_ref)
    assert (saved.asset_sha256, saved.protected_asset, saved.asset_size_bytes) == (
        original.asset_sha256,
        True,
        original.asset_size_bytes,
    )


def test_idempotent_package_creation_does_not_accept_damaged_existing_ledger(tmp_path):
    _, frozen = freeze(tmp_path)
    repo = InMemoryStrategyPackageRepository()
    repo.save_manifest_with_assets(frozen.manifest, frozen.assets)
    original = frozen.assets[0]
    key = (original.package_id, original.asset_type, original.asset_ref)
    repo.package_assets[key] = original.model_copy(update={"protected_asset": False})
    with pytest.raises(InvalidStateTransitionError):
        repo.save_manifest_with_assets(frozen.manifest, frozen.assets)


def test_valid_core_metadata_refresh_preserves_manifest_and_complete_ledger(tmp_path):
    _, frozen = freeze(tmp_path)
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest_with_assets(frozen.manifest, frozen.assets)
    original = frozen.assets[0]
    refreshed = repo.save_package_asset(original.model_copy(update={"metadata": {"note": "checked"}}))
    assert refreshed.metadata == {"note": "checked"}
    assert repo.get(record.package_id).manifest_sha256 == record.manifest_sha256
    _validate_asset_records_for_manifest(frozen.manifest, repo.list_package_assets(record.package_id))


def test_non_core_report_registration_remains_mutable(tmp_path):
    _, frozen = freeze(tmp_path)
    repo = InMemoryStrategyPackageRepository()
    repo.save_manifest_with_assets(frozen.manifest, frozen.assets)
    service = StrategyPackageService(repository=repo)
    first = service.record_package_asset(
        frozen.manifest.package_id,
        asset_type=StrategyPackageAssetType.VALIDATION_REPORT,
        asset_ref="unit://report",
        asset_sha256="old",
    )
    updated = service.record_package_asset(
        frozen.manifest.package_id,
        asset_type=StrategyPackageAssetType.VALIDATION_REPORT,
        asset_ref="unit://report",
        asset_sha256="new",
        protected_asset=False,
    )
    assert updated.asset_id == first.asset_id
    assert updated.asset_sha256 == "new" and updated.protected_asset is False


def test_postgres_public_registration_uses_the_existing_immutable_upsert(tmp_path):
    _, frozen = freeze(tmp_path)
    memory = InMemoryStrategyPackageRepository()
    record = memory.save_manifest_with_assets(frozen.manifest, frozen.assets)
    original = frozen.assets[0]
    statements = []

    class Cursor:
        def execute(self, statement, params):
            statements.append(statement)

        def fetchone(self):
            return {**original.model_dump(), "asset_id": 1}

    @contextmanager
    def cursor(**kwargs):
        yield Cursor()

    @contextmanager
    def connection():
        class Connection:
            pass

        conn = Connection()
        conn.cursor = cursor
        yield conn

    repo = StrategyPackageRepository(conn_factory=connection)
    repo.get = lambda package_id: record
    assert repo.save_package_asset(original).asset_sha256 == original.asset_sha256
    assert "WHERE strategy_pkg.package_asset.asset_sha256 IS NOT DISTINCT FROM EXCLUDED.asset_sha256" in statements[0]
    with pytest.raises(StrategyPackageValidationError):
        repo.save_package_asset(original.model_copy(update={"protected_asset": False}))
    assert len(statements) == 1


@pytest.mark.parametrize(
    "kind",
    [
        StrategyPackageAssetType.MODEL_WEIGHT,
        StrategyPackageAssetType.FACTOR_CODE,
        StrategyPackageAssetType.FACTOR_SCHEMA,
        StrategyPackageAssetType.MODEL_CODE,
        StrategyPackageAssetType.PREPROCESSOR,
    ],
)
def test_all_five_core_asset_kinds_require_protection_and_size(kind, tmp_path):
    _, frozen = freeze(tmp_path)
    model = frozen.manifest.model_asset.model_copy(
        update={
            "model_code_assets": [
                ModelCodeAsset(
                    module_name="model",
                    relative_path="model.py",
                    asset_ref="unit://code",
                    sha256="3" * 64,
                    size_bytes=7,
                )
            ]
        }
    )
    manifest = freeze_manifest(
        frozen.manifest.model_copy(
            update={
                "model_asset": model,
                "runtime_assets": RuntimeAssetManifest(
                    alpha158=Alpha158SchemaAsset(
                        enabled=True,
                        aliases=["RESI5"],
                        alias_count=1,
                        asset_ref="unit://schema",
                        sha256="4" * 64,
                        size_bytes=7,
                    )
                ),
            }
        )
    )
    rows = [
        *frozen.assets,
        StrategyPackageAssetRecord(
            package_id=manifest.package_id,
            asset_type=StrategyPackageAssetType.MODEL_CODE,
            asset_ref="unit://code",
            asset_sha256="3" * 64,
            asset_size_bytes=7,
        ),
        StrategyPackageAssetRecord(
            package_id=manifest.package_id,
            asset_type=StrategyPackageAssetType.FACTOR_SCHEMA,
            asset_ref="unit://schema",
            asset_sha256="4" * 64,
            asset_size_bytes=7,
        ),
    ]
    assert manifest_asset_ledger_covers(manifest, rows)
    for change in ({"protected_asset": False}, {"asset_size_bytes": 0}):
        broken = [row.model_copy(update=change) if row.asset_type == kind else row for row in rows]
        assert manifest_asset_ledger_covers(manifest, broken) is False
        with pytest.raises(StrategyPackageValidationError):
            _validate_asset_records_for_manifest(manifest, broken)
