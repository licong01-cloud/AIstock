"""Materialize compatible factor representations once, never duplicate aliases."""

from __future__ import annotations

import os
from collections import OrderedDict
from pathlib import Path
import stat
import tempfile
from threading import RLock
from typing import Callable

import pandas as pd

from .inference_inputs import frame_identity


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


class _ViewLease:
    def __init__(self, paths: list[Path], source: FactorInputLease, release: Callable,
                 identity: str, counters: dict[str, int]):
        self.paths, self.source, self._release = paths, source, release
        self.input_identity, self.counters = identity, counters
        self.released = False
        self.identities = {path: FactorInputLease._identity(path) for path in paths}

    def verify(self) -> None:
        self.source.verify()
        if any(not path.exists() or FactorInputLease._identity(path) != identity
               for path, identity in self.identities.items()):
            raise RuntimeError("factor changed a task input alias")

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        # Unlink only the exact aliases created by this lease. Do not chmod a
        # retained hard-link: that would unprotect the canonical input too.
        try:
            for path in self.paths:
                if path.exists() or path.is_symlink():
                    if os.name == "nt":
                        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
                    path.unlink()
        finally:
            self._release()


class FactorInputSession:
    """Own-run, bounded physical views; not a persisted or cross-run cache.

    Only identical complete legal inputs reuse a representation. A later date's
    qfq/precomputed view can never be sliced for an earlier model request.
    """

    def __init__(self, *, root: Path, max_bytes: int = 2 * 1024**3, max_entries: int = 2):
        root = root.resolve()
        on_x = root.drive.upper() == "X:" if os.name == "nt" else root.is_relative_to(Path("/mnt/x"))
        if not on_x or (os.name != "nt" and not Path("/mnt/x").is_mount()):
            raise ValueError("materialized input cache must resolve to mounted X")
        if max_bytes < 0 or max_entries < 1:
            raise ValueError("invalid materialized input retention budget")
        root.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="input_views_", dir=root))
        self.max_bytes, self.max_entries = max_bytes, max_entries
        self.retained_bytes = 0
        self._entries: OrderedDict[tuple, tuple[Path, FactorInputLease, int]] = OrderedDict()
        self._lock = RLock()
        self._active = False

    @staticmethod
    def _remove(entry: tuple[Path, FactorInputLease, int]) -> None:
        root, lease, _ = entry
        lease.release()
        for path in lease.paths:
            path.unlink(missing_ok=True)
        root.rmdir()  # Never recursive: an unexpected file is not discarded.

    def acquire(self, *, root: Path, price: pd.DataFrame, fundamental: pd.DataFrame,
                aliases: bool) -> _ViewLease:
        identity = frame_identity(price) + ":" + frame_identity(fundamental)
        key = (identity, aliases, "factor-input-layout-v1")
        self._lock.acquire()
        if self._active:
            self._lock.release()
            raise RuntimeError("materialized input session already has an active lease")
        entry = None
        links: list[Path] = []
        try:
            cached = key in self._entries
            if cached:
                entry = self._entries[key]
                entry[1].verify()
                self._entries.move_to_end(key)
            else:
                directory = Path(tempfile.mkdtemp(prefix="view_", dir=self.root))
                paths: list[Path] = []
                try:
                    paths.append(directory / "daily_pv.h5")
                    price.to_hdf(paths[-1], key="data", mode="w")
                    if not fundamental.empty:
                        paths.append(directory / "static_factors.parquet")
                        fundamental.to_parquet(paths[-1])
                        if aliases:
                            paths.append(directory / "static_factors.h5")
                            fundamental.to_hdf(paths[-1], key="data", mode="w")
                            paths.append(directory / "daily_pv_clean.h5")
                            price.loc[:, [column for column in price.columns if not str(column).startswith("$")]].to_hdf(paths[-1], key="data", mode="w")
                    lease = FactorInputLease(paths, {})
                    size = sum(path.stat().st_size for path in paths)
                    entry = directory, lease, size
                except Exception:
                    for path in paths:
                        if path.exists():
                            path.chmod(stat.S_IRUSR | stat.S_IWUSR)
                            path.unlink()
                    directory.rmdir()
                    raise
                if size <= self.max_bytes:
                    while self._entries and (self.retained_bytes + size > self.max_bytes or len(self._entries) >= self.max_entries):
                        _, old = self._entries.popitem(last=False)
                        self.retained_bytes -= old[2]
                        self._remove(old)
                    self._entries[key] = entry
                    self.retained_bytes += size
            directory, source, size = entry
            destinations = {path.name: path for path in source.paths}
            if aliases and not fundamental.empty:
                destinations.update({name: directory / "static_factors.h5" for name in STATIC_ALIASES})
            for name, path in destinations.items():
                destination = root / name
                if destination.exists() or destination.is_symlink():
                    raise FileExistsError("factor materialization cannot overwrite a task input")
                _link(path, destination)
                links.append(destination)
            def release():
                try:
                    if key not in self._entries:
                        self._remove(entry)
                    else:
                        # Windows alias removal temporarily clears read-only.
                        for path in source.paths:
                            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                finally:
                    self._active = False
                    self._lock.release()
            result = _ViewLease(links, source, release, identity,
                                {"materialized_view_hits": int(cached), "materialized_view_writes": int(not cached),
                                 "materialized_view_retained_bytes": self.retained_bytes,
                                 "static_hdf_materializations": int(aliases and not fundamental.empty and not cached),
                                 "static_hdf_aliases": len(STATIC_ALIASES) if aliases and not fundamental.empty else 0,
                                 "materialized_view_physical_bytes": size})
            self._active = True
            return result
        except Exception:
            try:
                for path in links:
                    if os.name == "nt":
                        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
                    path.unlink(missing_ok=True)
                if entry is not None:
                    if key not in self._entries:
                        self._remove(entry)
                    else:
                        for path in entry[1].paths:
                            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            finally:
                self._lock.release()
            raise

    def close(self) -> None:
        with self._lock:
            if self._active:
                raise RuntimeError("cannot close materialized inputs during factor execution")
            for entry in self._entries.values():
                self._remove(entry)
            self._entries.clear()
            self.retained_bytes = 0
            self.root.rmdir()
