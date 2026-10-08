from concurrent.futures import ThreadPoolExecutor
import os

from backend.services.strategy_package.live_inference import QEExperimentRuntimeAssetResolver
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.tests.strategy_package.test_runtime_package_assets_batch2 import _frozen_manifest


def test_identical_package_source_requests_do_not_reset_existing_reader(tmp_path):
    store = LocalPackageAssetStore(tmp_path / "cas")
    manifest = _frozen_manifest(store)
    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "cache", asset_store=store)

    def load():
        return resolver.load_source_for_strategy_package(
            source_type="qe_experiment",
            source_id="frozen",
            manifest=manifest,
            package_id=manifest.package_id,
        )

    first = load()
    sentinel = first.asset_workspace_path / "active-reader.txt"
    sentinel.write_text("keep")
    second = load()
    assert sentinel.read_text() == "keep"
    assert first.asset_workspace_path != second.asset_workspace_path
    assert os.path.samefile(
        first.asset_workspace_path / "mlruns/package_asset/artifacts/params.pkl",
        second.asset_workspace_path / "mlruns/package_asset/artifacts/params.pkl",
    )


def test_parallel_prepared_workspaces_are_isolated_but_share_model_inode(tmp_path):
    store = LocalPackageAssetStore(tmp_path / "cas")
    manifest = _frozen_manifest(store)
    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "cache", asset_store=store)
    source = resolver.load_source_for_strategy_package(
        source_type="qe_experiment",
        source_id="frozen",
        manifest=manifest,
        package_id=manifest.package_id,
    )

    def prepare(_):
        return resolver.prepare_workspace(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256,
            source=source,
            cache_namespace="same_leg_seed_date",
        )

    # In the old implementation another preparation removes this reader's path.
    first = prepare(0)
    sentinel = first.workspace_path / "active-reader.txt"
    sentinel.write_text("keep")
    with ThreadPoolExecutor(max_workers=2) as pool:
        second, third = list(pool.map(prepare, range(2)))
    assert sentinel.read_text() == "keep"
    assert len({first.workspace_path, second.workspace_path, third.workspace_path}) == 3
    for prepared in (first, second, third):
        assert prepared.factor_order_path.exists()
        assert os.path.samefile(prepared.model_params_path, store._path_from_uri(manifest.model_asset.asset_ref))
