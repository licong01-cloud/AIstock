from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package import model_asset_resolver as resolver_module
from backend.services.strategy_package.model_asset_resolver import ModelAssetResolver
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class FakeExternalResolver(ModelAssetResolver):
    def __init__(self, *, source: Path, cache_root: Path) -> None:
        super().__init__(cache_root=cache_root)
        self.source = source

    def _candidate_paths(self, original_path: str) -> list[Path]:
        return [Path(original_path), self.source]


def make_v24_manifest(model_path: str):
    manifest = make_manifest(algo_code="V24_PLAN")
    policy = manifest.minute_execution_policy.model_copy(
        update={"algo_config": {"model_path": model_path}}
    )
    return freeze_manifest(
        manifest.model_copy(
            update={
                "minute_execution_policy": policy,
                "manifest_sha256": None,
            }
        )
    )


def make_v25_manifest(early_model_path: str, late_model_path: str):
    manifest = make_manifest(algo_code="V25_TWO_STAGE")
    policy = manifest.minute_execution_policy.model_copy(
        update={
            "algo_config": {
                "early_model_path": early_model_path,
                "late_model_path": late_model_path,
                "device": "cpu",
            }
        }
    )
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
    root = Path("backend/tests/.tmp_model_asset_resolver") / uuid4().hex
    root.mkdir(parents=True, exist_ok=False)
    try:
        yield root
    finally:
        if ".tmp_model_asset_resolver" in str(root):
            shutil.rmtree(root, ignore_errors=True)


def test_model_asset_resolver_copies_external_v24_model_without_mutating_source(workspace_tmp) -> None:
    source = workspace_tmp / "external" / "v24_plan_net.pt"
    source.parent.mkdir()
    source.write_bytes(b"unit-test-model")
    original_bytes = source.read_bytes()
    manifest = make_v24_manifest("/home/lc999/data/rl_models/v24/v24_plan_net.pt")

    resolved = FakeExternalResolver(
        source=source,
        cache_root=workspace_tmp / "cache",
    ).resolve_manifest_assets(manifest)

    config = resolved.minute_execution_policy.algo_config
    cached_path = Path(config["model_path"])
    assert cached_path.exists()
    assert cached_path.read_bytes() == original_bytes
    assert source.read_bytes() == original_bytes
    assert config["original_model_path"] == "/home/lc999/data/rl_models/v24/v24_plan_net.pt"
    assert config["model_asset_cache_status"] == "copied"
    assert resolved.manifest_sha256 != manifest.manifest_sha256
    StrategyPackageValidator().validate_manifest(resolved)


def test_model_asset_resolver_fails_when_v24_model_source_and_cache_are_missing(workspace_tmp) -> None:
    manifest = make_v24_manifest("/home/lc999/data/rl_models/v24/missing.pt")

    with pytest.raises(DataUnavailableError, match="model_path is not accessible"):
        ModelAssetResolver(cache_root=workspace_tmp / "cache").resolve_manifest_assets(manifest)


def test_model_asset_resolver_does_not_probe_wsl_unc_paths_on_windows(monkeypatch, workspace_tmp) -> None:
    monkeypatch.setattr(resolver_module.os, "name", "nt")
    resolver = ModelAssetResolver(cache_root=workspace_tmp / "cache")

    candidates = [str(path) for path in resolver._candidate_paths("/home/lc999/model.pt")]

    assert candidates == [str(Path("/home/lc999/model.pt"))]
    assert all("\\\\wsl" not in item.lower() for item in candidates)


def test_model_asset_resolver_refuses_wsl_mount_translation(monkeypatch, workspace_tmp) -> None:
    monkeypatch.setattr(resolver_module.os, "name", "nt")
    resolver = ModelAssetResolver(cache_root=workspace_tmp / "cache")

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        resolver._candidate_paths("/mnt/f/worker_models/model.pt")


def test_model_asset_resolver_fails_fast_on_worker_model_path(workspace_tmp) -> None:
    manifest = make_v24_manifest("/mnt/f/worker_models/v24_plan_net.pt")

    with pytest.raises(DataUnavailableError, match="worker workspace path"):
        ModelAssetResolver(cache_root=workspace_tmp / "cache").resolve_manifest_assets(manifest)


def test_model_asset_resolver_copies_all_v25_model_assets(workspace_tmp) -> None:
    external = workspace_tmp / "external"
    early = external / "v25_early.pt"
    late = external / "v25_late.pt"
    external.mkdir()
    early.write_bytes(b"early-model")
    late.write_bytes(b"late-model")
    manifest = make_v25_manifest(
        "/home/lc999/data/rl_models/v25/v25_early.pt",
        "/home/lc999/data/rl_models/v25/v25_late.pt",
    )

    class MultiSourceResolver(ModelAssetResolver):
        def _candidate_paths(self, original_path: str) -> list[Path]:
            if original_path.endswith("v25_early.pt"):
                return [Path(original_path), early]
            return [Path(original_path), late]

    resolved = MultiSourceResolver(cache_root=workspace_tmp / "cache").resolve_manifest_assets(manifest)
    config = resolved.minute_execution_policy.algo_config

    assert Path(config["early_model_path"]).read_bytes() == b"early-model"
    assert Path(config["late_model_path"]).read_bytes() == b"late-model"
    assert config["original_early_model_path"] == "/home/lc999/data/rl_models/v25/v25_early.pt"
    assert config["original_late_model_path"] == "/home/lc999/data/rl_models/v25/v25_late.pt"
    assert config["runtime_asset_cache_status"] == {"early_model_path": "copied", "late_model_path": "copied"}
    assert resolved.manifest_sha256 != manifest.manifest_sha256
    StrategyPackageValidator().validate_manifest(resolved)
