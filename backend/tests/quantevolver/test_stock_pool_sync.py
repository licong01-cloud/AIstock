from __future__ import annotations

from backend.services.quantevolver import stock_pool_sync


def test_index_pool_sidecars_are_supported_without_broadening_arbitrary_paths() -> None:
    assert stock_pool_sync.is_filtered_stock_pool("index_pool__csi300.txt") is True
    assert stock_pool_sync.is_filtered_stock_pool("filtered_pool__index_union__csi300_csi500.txt") is True
    assert stock_pool_sync.is_filtered_stock_pool("../../index_pool__csi300.txt") is False
    assert stock_pool_sync.is_filtered_stock_pool("index_pool__CSI300.txt") is False
    assert stock_pool_sync.is_filtered_stock_pool("all.txt") is False


def test_index_pool_filename_keeps_existing_small_file_checksum_transport() -> None:
    command = stock_pool_sync.build_stock_pool_install_command(
        filename="index_pool__csi300.txt",
        remote_instruments_dir="/data/qlib/instruments",
        expected_sha256="a" * 64,
    )

    assert "index_pool__csi300.txt" in command
    assert "sha256sum" in command
