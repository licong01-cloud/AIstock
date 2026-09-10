from __future__ import annotations

from datetime import date
from types import MappingProxyType

import pytest

from backend.services.core_index_membership import (
    ResolvedUniverse,
    UniverseInterval,
    UniverseMode,
)
from backend.services.dataset_release.index_pool_sidecar import (
    IndexPoolSidecarError,
    render_sidecar_content,
    sidecar_filename,
    write_sidecar,
)


def resolved(mode=UniverseMode.SINGLE_INDEX, pools=("csi300",)):
    return ResolvedUniverse(
        mode=mode,
        pool_ids=pools,
        benchmark_code="000300.SH",
        membership_revision="2026-09-04T08:00:00+08:00",
        intervals=(
            UniverseInterval("000001.SZ", date(2024, 1, 2), date(2024, 1, 5)),
            UniverseInterval("600000.SH", date(2024, 1, 3), date(2024, 1, 8)),
        ),
        source_pool_ids_by_symbol=MappingProxyType({}),
    )


def test_single_and_union_sidecar_names_are_stable() -> None:
    assert sidecar_filename(resolved()) == "index_pool__csi300.txt"
    assert (
        sidecar_filename(resolved(UniverseMode.INDEX_UNION, ("csi300", "csi500")))
        == "filtered_pool__index_union__csi300_csi500.txt"
    )


def test_sidecar_uses_existing_dot_suffix_instrument_format() -> None:
    assert render_sidecar_content(resolved()) == (
        "000001.SZ\t2024-01-02\t2024-01-05\n600000.SH\t2024-01-03\t2024-01-08\n"
    )


def test_write_sidecar_is_atomic_and_does_not_touch_existing_instruments(tmp_path) -> None:
    existing = tmp_path / "stock_universe.txt"
    existing.write_text("unchanged\n", encoding="utf-8")

    result = write_sidecar(tmp_path, resolved())

    assert result.path.read_text(encoding="utf-8").startswith("000001.SZ\t")
    assert result.symbol_count == 2
    assert existing.read_text(encoding="utf-8") == "unchanged\n"
    assert list(tmp_path.glob("*.partial")) == []


def test_write_refuses_existing_target_without_explicit_replace(tmp_path) -> None:
    write_sidecar(tmp_path, resolved())
    with pytest.raises(FileExistsError):
        write_sidecar(tmp_path, resolved())


def test_stock_universe_reuses_existing_file_instead_of_generating_sidecar() -> None:
    with pytest.raises(IndexPoolSidecarError, match="needs no sidecar"):
        sidecar_filename(resolved(UniverseMode.STOCK_UNIVERSE, ()))
