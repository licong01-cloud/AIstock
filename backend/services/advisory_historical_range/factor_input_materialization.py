"""Materialize compatible factor representations once, never duplicate aliases."""

from __future__ import annotations

import os
from pathlib import Path
import stat

import pandas as pd


STATIC_ALIASES = (
    "daily_basic.h5", "moneyflow.h5", "sector_data.h5",
    "bak_basic.h5", "cyq_perf.h5", "margin_detail.h5",
)


class FactorInputLease:
    """Protect task-created inputs and detect accidental writes through a link."""

    def __init__(self, paths: list[Path], counters: dict[str, int]) -> None:
        self.paths = paths
        self.counters = counters
        try:
            for path in paths:
                path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            self.identities = {path: self._identity(path) for path in paths}
        except Exception:
            self.release()
            raise

    @staticmethod
    def _identity(path: Path) -> tuple[int, int, int]:
        info = path.stat()
        # Reader hard-link creation/removal changes Unix ctime without changing
        # content. Do not mistake the generated adapter's valid links for writes.
        return info.st_ino, info.st_size, info.st_mtime_ns

    def verify(self) -> None:
        if any(self._identity(path) != identity for path, identity in self.identities.items()):
            raise RuntimeError("factor changed a shared task input; scores cannot be published")

    def release(self) -> None:
        # Only these exact files were created by this call. Windows temporary
        # cleanup needs their read-only bit released; never touch formal assets.
        for path in self.paths:
            if path.exists():
                path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def _link(source: Path, destination: Path) -> None:
    try:
        os.link(source, destination)
    except OSError:
        try:
            destination.symlink_to(source)
        except OSError as exc:
            raise RuntimeError("factor input alias cannot be linked; copying large inputs is forbidden") from exc


def materialize_factor_aliases(*, root: Path, price: pd.DataFrame, fundamental: pd.DataFrame) -> FactorInputLease:
    """Match the existing generated adapter's schemas, including its clean PV."""
    canonical = root / "static_factors.h5"
    clean_price = root / "daily_pv_clean.h5"
    outputs = [canonical, clean_price, *(root / name for name in STATIC_ALIASES)]
    if any(path.exists() or path.is_symlink() for path in outputs):
        raise FileExistsError("factor input materialization must own new paths, not overwrite existing inputs")
    fundamental.to_hdf(canonical, key="data", mode="w")
    price.loc[:, [column for column in price.columns if not str(column).startswith("$")]].to_hdf(clean_price, key="data", mode="w")
    for name in STATIC_ALIASES:
        _link(canonical, root / name)
    return FactorInputLease(
        [*outputs, root / "daily_pv.h5", root / "static_factors.parquet"],
        {"static_hdf_materializations": 1, "static_hdf_aliases": len(STATIC_ALIASES),
         "static_hdf_physical_bytes": canonical.stat().st_size, "clean_price_hdf_materializations": 1,
         "clean_price_hdf_physical_bytes": clean_price.stat().st_size},
    )
