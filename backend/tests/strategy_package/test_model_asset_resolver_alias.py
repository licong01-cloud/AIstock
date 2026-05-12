from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.model_asset_resolver import ModelAssetResolver
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def make_v25_manifest(
    *,
    algo_code: str,
    early_model_path: str,
    late_model_path: str,
    asset_namespace: str | None = None,
):
    manifest = make_manifest(algo_code=algo_code)
    algo_config = {
        "early_model_path": early_model_path,
        "late_model_path": late_model_path,
        "device": "cpu",
    }
    if asset_namespace is not None:
        algo_config["asset_namespace"] = asset_namespace
    policy = manifest.minute_execution_policy.model_copy(update={"algo_config": algo_config})
    return freeze_manifest(
        manifest.model_copy(
            update={
                "minute_execution_policy": policy,
                "manifest_sha256": None,
            }
        )
    )


@pytest.fixture
def workspace_tmp() -> Path:
    root = Path("backend/tests/.tmp_model_asset_resolver_alias") / uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    try:
        yield root
    finally:
        if ".tmp_model_asset_resolver_alias" in str(root):
            shutil.rmtree(root, ignore_errors=True)


def _write_legacy_v25_cache(cache_root: Path) -> tuple[Path, Path]:
    legacy_dir = cache_root / "V25_TWO_STAGE"
    legacy_dir.mkdir(parents=True)
    legacy_early = legacy_dir / "v25_early_net_joint_fixed.pt"
    legacy_late = legacy_dir / "v25_late_net_joint_fixed.pt"
    legacy_early.write_bytes(b"legacy-early")
    legacy_late.write_bytes(b"legacy-late")
    return legacy_early, legacy_late


def test_v25_1_small_cap_uses_v25_asset_namespace_from_lookup(workspace_tmp) -> None:
    cache_root = workspace_tmp / "cache"
    legacy_early, legacy_late = _write_legacy_v25_cache(cache_root)
    manifest = make_v25_manifest(
        algo_code="V25_1_SMALL_CAP",
        early_model_path="/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt",
        late_model_path="/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt",
    )

    resolved = ModelAssetResolver(
        cache_root=cache_root,
        asset_namespace_lookup={"V25_1_SMALL_CAP": "V25_TWO_STAGE"},
    ).resolve_manifest_assets(manifest)

    config = resolved.minute_execution_policy.algo_config
    cached_early = Path(config["early_model_path"])
    cached_late = Path(config["late_model_path"])

    assert cached_early.parent == cache_root / "V25_TWO_STAGE"
    assert cached_late.parent == cache_root / "V25_TWO_STAGE"
    assert cached_early != legacy_early
    assert cached_late != legacy_late
    assert cached_early.read_bytes() == b"legacy-early"
    assert cached_late.read_bytes() == b"legacy-late"
    assert not (cache_root / "V25_1_SMALL_CAP").exists()
    assert config["runtime_asset_cache_status"] == {"early_model_path": "copied", "late_model_path": "copied"}
    StrategyPackageValidator().validate_manifest(resolved)


def test_v25_1_small_cap_uses_explicit_asset_namespace_from_config(workspace_tmp) -> None:
    cache_root = workspace_tmp / "cache"
    _write_legacy_v25_cache(cache_root)
    manifest = make_v25_manifest(
        algo_code="V25_1_SMALL_CAP",
        early_model_path="/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt",
        late_model_path="/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt",
        asset_namespace="V25_TWO_STAGE",
    )

    resolved = ModelAssetResolver(cache_root=cache_root).resolve_manifest_assets(manifest)
    config = resolved.minute_execution_policy.algo_config

    assert Path(config["early_model_path"]).parent == cache_root / "V25_TWO_STAGE"
    assert Path(config["late_model_path"]).parent == cache_root / "V25_TWO_STAGE"
    assert config["asset_namespace"] == "V25_TWO_STAGE"
    StrategyPackageValidator().validate_manifest(resolved)


def test_v25_two_stage_direct_namespace_still_resolves(workspace_tmp) -> None:
    cache_root = workspace_tmp / "cache"
    _write_legacy_v25_cache(cache_root)
    manifest = make_v25_manifest(
        algo_code="V25_TWO_STAGE",
        early_model_path="/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt",
        late_model_path="/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt",
    )

    resolved = ModelAssetResolver(cache_root=cache_root).resolve_manifest_assets(manifest)
    config = resolved.minute_execution_policy.algo_config

    assert Path(config["early_model_path"]).parent == cache_root / "V25_TWO_STAGE"
    assert Path(config["late_model_path"]).parent == cache_root / "V25_TWO_STAGE"
    StrategyPackageValidator().validate_manifest(resolved)


def test_asset_namespace_missing_assets_still_fail_fast(workspace_tmp) -> None:
    cache_root = workspace_tmp / "cache"
    manifest = make_v25_manifest(
        algo_code="V25_1_SMALL_CAP",
        early_model_path="/home/lc999/data/rl_models/v25/missing_early.pt",
        late_model_path="/home/lc999/data/rl_models/v25/missing_late.pt",
        asset_namespace="V25_TWO_STAGE",
    )

    with pytest.raises(DataUnavailableError, match="early_model_path is not accessible") as exc_info:
        ModelAssetResolver(cache_root=cache_root).resolve_manifest_assets(manifest)

    context = exc_info.value.context
    assert context["algo_code"] == "V25_1_SMALL_CAP"
    assert context["asset_namespace"] == "V25_TWO_STAGE"
    assert str(cache_root / "V25_TWO_STAGE" / "missing_early.pt") in context["attempted_paths"]
    assert str(cache_root / "V25_1_SMALL_CAP" / "missing_early.pt") not in context["attempted_paths"]
