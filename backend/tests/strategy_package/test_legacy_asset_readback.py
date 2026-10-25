from datetime import datetime, timezone

import pytest

from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.trading_core.errors import StrategyPackageValidationError


def row(asset_type):
    return dict(
        asset_id=1,
        package_id="package",
        asset_type=asset_type,
        asset_ref="asset",
        asset_sha256="a" * 64,
        metadata={"origin": "legacy"},
        protected_asset=False,
        asset_size_bytes=12,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.parametrize("asset_type", ["MODEL_WEIGHT", "protected_asset_ledger_evidence"])
def test_exact_known_legacy_types_can_be_read_without_protection_upgrade(asset_type):
    source = row(asset_type)
    result = StrategyPackageRepository._package_asset_from_row(source)
    assert result.asset_id == 1
    assert result.protected_asset is False
    assert result.asset_size_bytes == 12
    assert source["metadata"] == {"origin": "legacy"}
    if asset_type == "MODEL_WEIGHT":
        assert result.asset_type == StrategyPackageAssetType.MODEL_WEIGHT
        assert result.metadata["legacy_asset_type"] == "MODEL_WEIGHT"
    else:
        assert result.asset_type.value == asset_type


@pytest.mark.parametrize("asset_type", ["model_WEIGHT", "arbitrary_unknown_type"])
def test_unknown_types_are_not_silently_mapped_to_other(asset_type):
    with pytest.raises(StrategyPackageValidationError) as caught:
        StrategyPackageRepository._package_asset_from_row(row(asset_type))
    assert caught.value.context["reason_code"] == "strategy_package_asset_type_unsupported"


@pytest.mark.parametrize("asset_type", list(StrategyPackageAssetType))
def test_current_type_roundtrip_preserves_identity_metadata_and_protection(asset_type):
    source = row(asset_type.value)
    result = StrategyPackageRepository._package_asset_from_row(source)
    assert result.asset_type == asset_type
    assert result.metadata == source["metadata"]
    assert result.asset_ref == source["asset_ref"]
    assert result.asset_sha256 == source["asset_sha256"]
    assert result.protected_asset is False


@pytest.mark.parametrize("asset_type", ["MODEL_WEIGHT", "protected_asset_ledger_evidence", "unknown"])
def test_assets_api_translates_known_legacy_rows_and_unknown_errors(monkeypatch, asset_type):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import backend.routers.strategy_packages as module

    class Service:
        def list_package_assets(self, package_id, *, protected_only=False):
            result = StrategyPackageRepository._package_asset_from_row(row(asset_type))
            return [result] if not protected_only or result.protected_asset else []

    monkeypatch.setattr(module, "StrategyPackageService", Service)
    app = FastAPI()
    app.include_router(module.router)
    with TestClient(app) as client:
        response = client.get(f"{module.router.prefix}/package/assets")
        if asset_type == "unknown":
            assert 400 <= response.status_code < 500
            assert "strategy_package_asset_type_unsupported" in response.text
        else:
            assert response.status_code == 200
            assert response.json()["assets"][0]["protected_asset"] is False
            protected = client.get(f"{module.router.prefix}/package/assets?protected_only=true")
            assert protected.json()["assets"] == []
