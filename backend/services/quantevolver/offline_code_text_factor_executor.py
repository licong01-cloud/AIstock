"""Execute raw catalog code_text against the read-once backtest data cache."""
from __future__ import annotations

import contextlib
import builtins as _builtins
import os
import tempfile
import threading
import time
import traceback
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .backtest_base_data_memory_cache import BacktestBaseDataMemoryCache

_THREAD_LOCAL = threading.local()
_HDF_PATCH_LOCK = threading.Lock()
_HDF_IO_LOCK = threading.Lock()
_HDF_PATCHED = False
_ORIGINAL_DF_TO_HDF = pd.DataFrame.to_hdf
_ORIGINAL_SERIES_TO_HDF = pd.Series.to_hdf
_MISSING = object()


@dataclass
class FactorExecutionResult:
    factor_name: str
    success: bool
    dataframe: pd.DataFrame | None = None
    elapsed_sec: float = 0.0
    error: str | None = None
    error_type: str | None = None
    traceback_tail: list[str] | None = None


class OfflineCodeTextFactorExecutor:
    """Run unmodified offline factor code_text with pandas file reads redirected to memory."""

    def __init__(self, base_cache: BacktestBaseDataMemoryCache) -> None:
        self.base_cache = base_cache

    def compute_factor(self, factor_name: str, code_text: str) -> FactorExecutionResult:
        t0 = time.time()
        with tempfile.TemporaryDirectory(prefix=f"official_factor_{factor_name}_") as tmpdir:
            factor_py = Path(tmpdir) / "factor.py"
            factor_py.write_text(code_text, encoding="utf-8")
            self._materialize_base_data_existence_markers(Path(tmpdir))
            try:
                with _factor_output_dir(tmpdir):
                    ns = self._run_factor_file(factor_py)
                    df = self._collect_result(factor_name, ns, Path(tmpdir))
                return FactorExecutionResult(
                    factor_name=factor_name,
                    success=True,
                    dataframe=self._normalize_result(factor_name, df),
                    elapsed_sec=round(time.time() - t0, 3),
                )
            except Exception as exc:
                return FactorExecutionResult(
                    factor_name=factor_name,
                    success=False,
                    elapsed_sec=round(time.time() - t0, 3),
                    error=f"{type(exc).__name__}: {exc}",
                    error_type=type(exc).__name__,
                    traceback_tail=traceback.format_exc().splitlines()[-20:],
                )

    def compute_batch(self, factors: list[dict[str, Any]]) -> dict[str, FactorExecutionResult]:
        results: dict[str, FactorExecutionResult] = {}
        for item in factors:
            name = str(item.get("factor_name") or "").strip()
            code_text = str(item.get("code_text") or "")
            if not name or not code_text.strip():
                results[name or "<missing>"] = FactorExecutionResult(
                    factor_name=name or "<missing>",
                    success=False,
                    error="missing_code_text",
                    error_type="missing_code_text",
                )
                continue
            results[name] = self.compute_factor(name, code_text)
        return results

    def _run_factor_file(self, factor_py: Path) -> dict[str, Any]:
        panda_proxy = self._build_pandas_proxy()
        os_proxy = self._build_os_proxy()
        real_import = _builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "pandas" or name.startswith("pandas."):
                return panda_proxy
            if name == "os":
                return os_proxy
            if name == "os.path":
                return os_proxy.path if fromlist else os_proxy
            return real_import(name, globals, locals, fromlist, level)

        _ensure_hdf_redirect_patch()
        safe_builtins = dict(vars(_builtins))
        safe_builtins["__import__"] = guarded_import
        namespace: dict[str, Any] = {
            "__name__": "__main__",
            "__file__": str(factor_py),
            "__package__": None,
            "__cached__": None,
            "__builtins__": safe_builtins,
        }
        code = factor_py.read_text(encoding="utf-8")
        exec(compile(code, str(factor_py), "exec"), namespace)
        return namespace

    def _materialize_base_data_existence_markers(self, work_dir: Path) -> None:
        """Expose loaded base-data names for legacy relative existence checks only."""
        for name in self.base_cache.entries:
            marker = work_dir / name
            if marker.exists():
                continue
            marker.touch()

    def _build_pandas_proxy(self):
        original_read_hdf = pd.read_hdf

        def read_hdf(path_or_buf, *args, **kwargs):
            path_s = str(path_or_buf)
            if Path(path_s).name == "result.h5":
                captured = _get_captured_result_h5(path_or_buf)
                if captured is not None:
                    return captured
                with _HDF_IO_LOCK:
                    return original_read_hdf(path_or_buf, *args, **kwargs)
            return self.base_cache.get(path_s)

        def read_parquet(path, *args, **kwargs):
            path_s = str(path)
            columns = kwargs.get("columns")
            return self.base_cache.get(path_s, columns=columns)

        proxy = types.ModuleType("pandas")
        proxy.__dict__.update(pd.__dict__)
        proxy.read_hdf = read_hdf
        proxy.read_parquet = read_parquet
        return proxy

    def _build_os_proxy(self):
        loaded_base_data_names = set(self.base_cache.entries)
        original_exists = os.path.exists
        original_isfile = os.path.isfile

        def is_loaded_base_data_path(path: object) -> bool:
            try:
                return Path(os.fspath(path)).name in loaded_base_data_names
            except TypeError:
                return False

        def exists(path):
            if is_loaded_base_data_path(path):
                return True
            return original_exists(path)

        def isfile(path):
            if is_loaded_base_data_path(path):
                return True
            return original_isfile(path)

        path_proxy = types.ModuleType(os.path.__name__)
        path_proxy.__dict__.update(os.path.__dict__)
        path_proxy.exists = exists
        path_proxy.isfile = isfile

        proxy = types.ModuleType("os")
        proxy.__dict__.update(os.__dict__)
        proxy.path = path_proxy
        return proxy

    def _collect_result(self, factor_name: str, ns: dict[str, Any], work_dir: Path) -> pd.DataFrame:
        captured = _get_captured_result_h5("result.h5")
        if captured is not None:
            return captured
        result_h5 = work_dir / "result.h5"
        if result_h5.exists():
            with _HDF_IO_LOCK:
                return pd.read_hdf(result_h5)
        for key in ("result", "df", "factor", factor_name):
            value = ns.get(key)
            if isinstance(value, (pd.DataFrame, pd.Series)):
                return value.to_frame(name=factor_name) if isinstance(value, pd.Series) else value
        calc_name = f"calculate_{factor_name}"
        calc = ns.get(calc_name)
        if callable(calc):
            instruments = self._infer_instruments()
            value = calc(instruments, self.base_cache.start_date, self.base_cache.end_date)
            if isinstance(value, pd.Series):
                return value.to_frame(name=factor_name)
            if isinstance(value, pd.DataFrame):
                return value
        # Some generated factor names are sanitized in function names; try first calculate_*.
        for key, value in ns.items():
            if key.startswith("calculate_") and callable(value):
                res = value(self._infer_instruments(), self.base_cache.start_date, self.base_cache.end_date)
                if isinstance(res, pd.Series):
                    return res.to_frame(name=factor_name)
                if isinstance(res, pd.DataFrame):
                    return res
        raise RuntimeError("factor code produced neither result.h5 nor a DataFrame result")

    def _normalize_result(self, factor_name: str, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df, pd.Series):
            df = df.to_frame(name=factor_name)
        if df.empty:
            raise RuntimeError("factor result is empty")
        if not isinstance(df.index, pd.MultiIndex):
            if {"datetime", "instrument"}.issubset(df.columns):
                df = df.copy()
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df = df.set_index(["datetime", "instrument"])
            else:
                raise RuntimeError("factor result missing MultiIndex(datetime, instrument)")
        names = list(df.index.names)
        if "datetime" in names and "instrument" in names:
            if names[:2] != ["datetime", "instrument"]:
                df = df.reorder_levels(["datetime", "instrument"])
        else:
            df.index.names = ["datetime", "instrument"] + names[2:]
        df = df.sort_index()
        dates = pd.to_datetime(df.index.get_level_values("datetime"), errors="coerce")
        df = df[(dates >= pd.Timestamp(self.base_cache.start_date)) & (dates <= pd.Timestamp(self.base_cache.end_date))]
        if df.empty:
            raise RuntimeError(f"factor result has no rows in {self.base_cache.start_date}~{self.base_cache.end_date}")
        if df.shape[1] == 1:
            df = df.rename(columns={df.columns[0]: "value"})
        elif factor_name in df.columns:
            df = df[[factor_name]].rename(columns={factor_name: "value"})
        else:
            df = df.iloc[:, :1].rename(columns={df.columns[0]: "value"})
        df = df[~df.index.duplicated(keep="last")]
        return df

    def _infer_instruments(self) -> list[str]:
        for entry in self.base_cache.entries.values():
            df = entry.dataframe
            if isinstance(df.index, pd.MultiIndex) and "instrument" in df.index.names:
                return sorted(map(str, df.index.get_level_values("instrument").unique()))
            if "instrument" in getattr(df, "columns", []):
                return sorted(map(str, df["instrument"].dropna().unique()))
        return []


@contextlib.contextmanager
def _factor_output_dir(path: str | os.PathLike[str]):
    old = getattr(_THREAD_LOCAL, "factor_output_dir", None)
    old_result = getattr(_THREAD_LOCAL, "factor_result_h5", _MISSING)
    _THREAD_LOCAL.factor_output_dir = Path(path)
    _THREAD_LOCAL.factor_result_h5 = None
    try:
        yield
    finally:
        if old is None:
            try:
                delattr(_THREAD_LOCAL, "factor_output_dir")
            except AttributeError:
                pass
        else:
            _THREAD_LOCAL.factor_output_dir = old
        if old_result is _MISSING:
            try:
                delattr(_THREAD_LOCAL, "factor_result_h5")
            except AttributeError:
                pass
        else:
            _THREAD_LOCAL.factor_result_h5 = old_result


def _redirect_hdf_path(path_or_buf):
    output_dir = getattr(_THREAD_LOCAL, "factor_output_dir", None)
    if output_dir is None or not isinstance(path_or_buf, (str, os.PathLike)):
        return path_or_buf
    path = Path(path_or_buf)
    if path.is_absolute():
        return path_or_buf
    return Path(output_dir) / path


def _capture_result_h5(path_or_buf, value: pd.DataFrame | pd.Series) -> bool:
    """Capture legacy result.h5 writes in thread-local memory.

    Official full-compute runs factor code concurrently. PyTables/HDF5 is not
    safe for these per-factor result files in worker threads, so only
    result.h5 output is intercepted; base-data reads are already served by the
    memory cache.
    """
    if getattr(_THREAD_LOCAL, "factor_output_dir", None) is None:
        return False
    if not isinstance(path_or_buf, (str, os.PathLike)):
        return False
    if Path(path_or_buf).name != "result.h5":
        return False
    if isinstance(value, pd.Series):
        captured = value.to_frame(name=value.name or "value")
    else:
        captured = value
    _THREAD_LOCAL.factor_result_h5 = captured.copy(deep=False)
    return True


def _get_captured_result_h5(path_or_buf) -> pd.DataFrame | pd.Series | None:
    if not isinstance(path_or_buf, (str, os.PathLike)):
        return None
    if Path(path_or_buf).name != "result.h5":
        return None
    captured = getattr(_THREAD_LOCAL, "factor_result_h5", None)
    if isinstance(captured, (pd.DataFrame, pd.Series)):
        return captured.copy(deep=False)
    return None


def _ensure_hdf_redirect_patch() -> None:
    global _HDF_PATCHED
    if _HDF_PATCHED:
        return
    with _HDF_PATCH_LOCK:
        if _HDF_PATCHED:
            return

        def dataframe_to_hdf(self, path_or_buf, *args, **kwargs):
            if _capture_result_h5(path_or_buf, self):
                return None
            with _HDF_IO_LOCK:
                return _ORIGINAL_DF_TO_HDF(self, _redirect_hdf_path(path_or_buf), *args, **kwargs)

        def series_to_hdf(self, path_or_buf, *args, **kwargs):
            if _capture_result_h5(path_or_buf, self):
                return None
            with _HDF_IO_LOCK:
                return _ORIGINAL_SERIES_TO_HDF(self, _redirect_hdf_path(path_or_buf), *args, **kwargs)

        pd.DataFrame.to_hdf = dataframe_to_hdf
        pd.Series.to_hdf = series_to_hdf
        _HDF_PATCHED = True
