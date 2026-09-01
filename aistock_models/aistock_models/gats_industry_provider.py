"""Point-in-time Shenwan L2 providers for EfficientGATs (frozen files only).

Data-plane invariant: QE training, prediction, backtest and multi-alpha combine
computations must read industry ids exclusively from frozen dataset files
(``sector_data.h5`` or ``static_factors.parquet``) that ship an explicit
``l2_code_id`` column with per-trading-day PIT alignment.  This module contains
no database driver, no connection configuration and no credential environment
variables; any missing file, missing field, unalignable symbol or insufficient
coverage fails loud instead of falling back to a database or a current
snapshot.

The QE runner cannot pass callables through ``conf.yaml``.  It injects the
provider from Python when EfficientGATs needs industry ids for either
``industry_bias`` adjacency or ``gats_industry_embedding=on``.  The provider
clears its loaded frame when pickled so qlib ``params.pkl`` does not embed
frozen source data; only the file path and thresholds persist.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


INDUSTRY_BIAS_MODE = "industry_bias"
INDUSTRY_EMBEDDING_ON = "on"
DEFAULT_MIN_COVERAGE = 0.90
MIN_COVERAGE_ENV = "QE_GATS_INDUSTRY_MIN_COVERAGE"
SOURCE_ENV = "QE_GATS_INDUSTRY_SOURCE_PATH"
RDAGENT_FACTOR_DATA_ENV = "RDAGENT_FACTOR_DATA_WSL"
DEFAULT_SECTOR_SOURCE_NAME = "sector_data.h5"
FALLBACK_SECTOR_SOURCE_NAME = "static_factors.parquet"
L2_CODE_ID_COLUMN = "l2_code_id"
UNKNOWN_L2_CODE_ID = -1

_SOURCE_KEYS = ("data", "/data")
_EXPLICIT_ID_COLUMNS = (L2_CODE_ID_COLUMN,)
_SUPPORTED_SUFFIXES = {".h5", ".hdf", ".hdf5", ".parquet"}
_MISSING_TOKENS = {"", "nan", "NaN", "None", "none", "<NA>"}


class GatsIndustryProviderError(RuntimeError):
    """Fail-loud provider error carrying a stable reason_code in the message."""


@dataclass
class SectorDataIndustryIdProvider:
    """Return PIT Shenwan L2 ids aligned to a qlib segment MultiIndex.

    The only accepted industry identity is the explicit ``l2_code_id`` column
    of a frozen ``sector_data.h5`` / ``static_factors.parquet`` file indexed by
    ``(datetime, instrument)``.  Rows are aligned per trading day with a
    backward as-of lookup inside the frozen file, so historical PIT membership
    is preserved; a current snapshot is never substituted for PIT.  There is
    intentionally no industry-feature-signature inference: when the explicit
    column is absent the provider fails loud.
    """

    source_path: str | os.PathLike[str]
    min_coverage: float = DEFAULT_MIN_COVERAGE
    hdf_key: str = "data"
    source_name: str = DEFAULT_SECTOR_SOURCE_NAME
    _frame: pd.DataFrame | None = field(default=None, init=False, repr=False)
    _id_column: str | None = field(default=None, init=False, repr=False)
    last_coverage: dict[str, Any] | None = field(default=None, init=False)
    coverage_history: list[dict[str, Any]] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.source_path = str(Path(self.source_path))
        self.min_coverage = _validate_min_coverage(self.min_coverage)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state["_frame"] = None
        return state

    def __call__(self, *args: Any) -> pd.Series:
        segment_name = "unknown"
        if len(args) >= 3:
            _segment, index, segment_name = args[:3]
        elif len(args) == 2:
            _segment, index = args
        elif len(args) == 1:
            (index,) = args
        else:
            raise TypeError("SectorDataIndustryIdProvider expects index or segment,index")
        return self.get_industry_ids(index, segment_name=str(segment_name or "unknown"))

    def get_industry_ids(self, index: Any, *, segment_name: str = "unknown") -> pd.Series:
        return_index = _coerce_return_index(index)
        target_index = _normalise_target_index(return_index)
        if len(target_index) == 0:
            return pd.Series([], index=return_index, dtype="object")

        source = self._load_source()
        matched = self._lookup_asof(source, target_index)
        values = self._industry_values_from_rows(matched)

        missing = values.isna() | values.astype(str).isin(_MISSING_TOKENS)
        covered_rows = int((~missing).sum())
        rows = int(len(target_index))
        coverage = covered_rows / rows if rows else 1.0
        payload = {
            "reason_code": "qe_gats_industry_coverage",
            "segment": segment_name,
            "rows": rows,
            "covered_rows": covered_rows,
            "missing_rows": int(missing.sum()),
            "coverage": coverage,
            "threshold": self.min_coverage,
            "source": self.source_path,
            "id_source": self._id_column,
        }
        self.last_coverage = payload
        self.coverage_history.append(payload)

        if coverage < self.min_coverage:
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_coverage_below_threshold: "
                f"segment={segment_name} source={self.source_path} "
                f"coverage={coverage:.6f} threshold={self.min_coverage:.6f} "
                f"rows={rows} covered_rows={covered_rows} missing_rows={int(missing.sum())}"
            )

        return pd.Series(values.to_numpy(dtype=object), index=return_index, dtype="object")

    def validate_source_available(self) -> None:
        path = Path(self.source_path)
        if not path.exists():
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_source_missing: "
                f"source={path} source_name={self.source_name}"
            )
        if not path.is_file():
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_source_not_file: "
                f"source={path} source_name={self.source_name}"
            )

    def _load_source(self) -> pd.DataFrame:
        if self._frame is not None:
            return self._frame

        self.validate_source_available()
        path = Path(self.source_path)
        suffix = path.suffix.lower()
        try:
            if suffix in {".h5", ".hdf", ".hdf5"}:
                frame = _read_hdf_first_key(path, self.hdf_key)
            elif suffix == ".parquet":
                frame = pd.read_parquet(path)
            else:
                raise GatsIndustryProviderError(
                    "reason_code=qe_gats_industry_source_format_unsupported: "
                    f"source={path} suffix={path.suffix} supported={sorted(_SUPPORTED_SUFFIXES)}"
                )
        except GatsIndustryProviderError:
            raise
        except Exception as exc:
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_source_load_failed: "
                f"source={path} error={type(exc).__name__}: {exc}"
            ) from exc

        frame = _normalise_source_frame(frame, source=str(path))
        self._id_column = next((col for col in _EXPLICIT_ID_COLUMNS if col in frame.columns), None)
        if self._id_column is None:
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_source_schema_invalid: "
                f"source={path} missing explicit {L2_CODE_ID_COLUMN} column; "
                "industry-feature-signature inference is forbidden"
            )

        frame = frame[[self._id_column]].sort_index()
        if frame.index.get_level_values("datetime").nunique() < 1:
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_source_empty: "
                f"source={path} no trading dates available"
            )
        self._frame = frame
        return frame

    def _lookup_asof(self, source: pd.DataFrame, target_index: pd.MultiIndex) -> pd.DataFrame:
        exact = source.reindex(target_index)
        missing_mask = exact.isna().all(axis=1)
        if not bool(missing_mask.any()):
            return exact

        missing_index = target_index[missing_mask.to_numpy()]
        asof_rows = _asof_rows_by_instrument(source, missing_index)
        if not asof_rows.empty:
            # Object dtype on both sides keeps the assignment free of pandas
            # incompatible-dtype downcasting warnings (int16 vs float64 NaN).
            exact = exact.astype("object")
            exact.loc[asof_rows.index, asof_rows.columns] = asof_rows.astype("object")
        return exact

    def _industry_values_from_rows(self, rows: pd.DataFrame) -> pd.Series:
        numeric = pd.to_numeric(rows[self._id_column], errors="coerce")
        unknown = numeric.isna() | (numeric == UNKNOWN_L2_CODE_ID)
        values = numeric.astype("Int64").astype("object")
        return values.where(~unknown, np.nan)


def inject_gats_industry_provider_if_needed(
    config: dict[str, Any],
    *,
    cwd: str | os.PathLike[str] | None = None,
    print_fn=print,
) -> SectorDataIndustryIdProvider | None:
    """Inject a frozen-file PIT L2 provider into ``config.task.model.kwargs``."""

    model_kwargs = _model_kwargs_if_industry_requested(config)
    if model_kwargs is None:
        return None

    existing = model_kwargs.get("gats_industry_id_provider")
    if existing is not None:
        if callable(existing) or isinstance(existing, dict):
            return existing
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_provider_invalid: "
            f"type={type(existing).__name__} expected=callable_or_dict"
        )

    source_path = resolve_sector_source_path(config, cwd=cwd)
    if source_path is None:
        candidates = ", ".join(str(path) for path in sector_source_candidates(config, cwd=cwd))
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_source_missing: "
            f"source_names=[{DEFAULT_SECTOR_SOURCE_NAME}, {FALLBACK_SECTOR_SOURCE_NAME}] "
            f"candidates=[{candidates}]"
        )

    provider = SectorDataIndustryIdProvider(
        source_path=source_path,
        min_coverage=resolve_min_coverage(config),
        source_name=Path(source_path).name,
    )
    provider.validate_source_available()
    model_kwargs["gats_industry_id_provider"] = provider
    print_fn(
        "[INFO] EfficientGATs industry: injected frozen-file PIT L2 provider "
        f"source={provider.source_path} min_coverage={provider.min_coverage:.4f}"
    )
    return provider


def attach_gats_industry_provider_to_model(
    model: Any,
    config: dict[str, Any],
    *,
    cwd: str | os.PathLike[str] | None = None,
    print_fn=print,
) -> Any | None:
    """Attach the injected provider to an already-loaded EfficientGATs model."""

    if not model_requests_gats_industry(model):
        return None
    model_kwargs = _model_kwargs_if_industry_requested(config)
    if model_kwargs is None:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_config_missing: "
            "loaded_model_requires_industry but config.task.model.kwargs does not request industry ids"
        )
    provider = inject_gats_industry_provider_if_needed(config, cwd=cwd, print_fn=print_fn)
    if provider is None:
        provider = model_kwargs.get("gats_industry_id_provider")
    setattr(model, "gats_industry_id_provider", provider)
    return provider


def model_requests_gats_industry(model: Any) -> bool:
    return (
        getattr(model, "gats_adjacency_mode", None) == INDUSTRY_BIAS_MODE
        or _flag_is_on(getattr(model, "gats_industry_embedding", "off"))
    )


def config_requests_gats_industry(config: dict[str, Any]) -> bool:
    return _model_kwargs_if_industry_requested(config) is not None


def resolve_min_coverage(config: dict[str, Any]) -> float:
    raw = os.environ.get(MIN_COVERAGE_ENV)
    if raw in (None, ""):
        runtime = config.get("qe_runtime") if isinstance(config, dict) else None
        if isinstance(runtime, dict):
            raw = runtime.get("gats_industry_min_coverage")
    if raw in (None, ""):
        return DEFAULT_MIN_COVERAGE
    return _validate_min_coverage(raw)


def resolve_sector_source_path(
    config: dict[str, Any],
    *,
    cwd: str | os.PathLike[str] | None = None,
) -> Path | None:
    for candidate in sector_source_candidates(config, cwd=cwd):
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def sector_source_candidates(
    config: dict[str, Any],
    *,
    cwd: str | os.PathLike[str] | None = None,
) -> list[Path]:
    base = Path(cwd or os.getcwd())
    raw_candidates: list[str | os.PathLike[str]] = []

    env_source = os.environ.get(SOURCE_ENV)
    if env_source:
        raw_candidates.append(env_source)

    runtime = config.get("qe_runtime") if isinstance(config, dict) else None
    if isinstance(runtime, dict):
        for key in ("gats_industry_source_path", "sector_data_path"):
            value = runtime.get(key)
            if value:
                raw_candidates.append(value)

    for name in (DEFAULT_SECTOR_SOURCE_NAME, FALLBACK_SECTOR_SOURCE_NAME):
        raw_candidates.append(base / name)

    factor_dir = os.environ.get(RDAGENT_FACTOR_DATA_ENV)
    if factor_dir:
        for name in (DEFAULT_SECTOR_SOURCE_NAME, FALLBACK_SECTOR_SOURCE_NAME):
            raw_candidates.append(Path(factor_dir) / name)

    result: list[Path] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        path = Path(raw)
        if not path.is_absolute():
            path = base / path
        key = str(path)
        if key not in seen:
            result.append(path)
            seen.add(key)
    return result


def _model_kwargs_if_industry_requested(config: dict[str, Any]) -> dict[str, Any] | None:
    task = config.get("task") if isinstance(config, dict) else None
    model_cfg = task.get("model") if isinstance(task, dict) else None
    model_kwargs = model_cfg.get("kwargs") if isinstance(model_cfg, dict) else None
    if not isinstance(model_kwargs, dict):
        return None
    if model_kwargs.get("gats_adjacency_mode", "off") == INDUSTRY_BIAS_MODE:
        return model_kwargs
    if _flag_is_on(model_kwargs.get("gats_industry_embedding", "off")):
        return model_kwargs
    return None


def _normalise_target_index(index: Any) -> pd.MultiIndex:
    if isinstance(index, pd.MultiIndex):
        target = index
    else:
        target = pd.MultiIndex.from_tuples(list(index), names=["datetime", "instrument"])
    if target.nlevels < 2:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_index_invalid: "
            f"index_nlevels={target.nlevels} expected>=2"
        )
    return pd.MultiIndex.from_arrays(
        [
            pd.to_datetime(target.get_level_values(0)),
            [_normalise_instrument_code(value) for value in target.get_level_values(target.nlevels - 1)],
        ],
        names=["datetime", "instrument"],
    )


def _coerce_return_index(index: Any) -> pd.MultiIndex:
    if isinstance(index, pd.MultiIndex):
        return index
    return pd.MultiIndex.from_tuples(list(index), names=["datetime", "instrument"])


def _normalise_source_frame(frame: pd.DataFrame, *, source: str) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_source_empty: "
            f"source={source}"
        )

    if isinstance(frame.index, pd.MultiIndex) and frame.index.nlevels >= 2:
        source_index = pd.MultiIndex.from_arrays(
            [
                pd.to_datetime(frame.index.get_level_values(0)),
                [_normalise_instrument_code(value) for value in frame.index.get_level_values(frame.index.nlevels - 1)],
            ],
            names=["datetime", "instrument"],
        )
        frame = frame.copy()
        frame.index = source_index
        return frame

    columns = {str(col): col for col in frame.columns}
    date_col = next((columns[key] for key in ("datetime", "trade_date", "date") if key in columns), None)
    inst_col = next((columns[key] for key in ("instrument", "ts_code", "symbol") if key in columns), None)
    if date_col is None or inst_col is None:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_source_index_invalid: "
            f"source={source} expected MultiIndex(datetime,instrument) or date/instrument columns"
        )
    frame = frame.copy()
    frame.index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(frame[date_col]), [_normalise_instrument_code(value) for value in frame[inst_col]]],
        names=["datetime", "instrument"],
    )
    return frame.drop(columns=[date_col, inst_col], errors="ignore")


def _normalise_instrument_code(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return text
    upper = text.upper()
    if "." in upper:
        return upper
    if len(upper) == 8 and upper[:2] in {"SH", "SZ", "BJ"} and upper[2:].isdigit():
        return f"{upper[2:]}.{upper[:2]}"
    return upper


def _read_hdf_first_key(path: Path, preferred_key: str) -> pd.DataFrame:
    try:
        return pd.read_hdf(path, key=preferred_key)
    except (KeyError, ValueError):
        with pd.HDFStore(path, mode="r") as store:
            keys = store.keys()
            for key in _SOURCE_KEYS:
                if key in keys:
                    return store[key]
            if len(keys) == 1:
                return store[keys[0]]
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_hdf_key_missing: "
                f"source={path} preferred_key={preferred_key} keys={keys}"
            )


def _asof_rows_by_instrument(source: pd.DataFrame, missing_index: pd.MultiIndex) -> pd.DataFrame:
    if len(missing_index) == 0:
        return pd.DataFrame(index=missing_index, columns=source.columns)

    missing = pd.DataFrame(index=missing_index).reset_index()
    missing["_target_order"] = np.arange(len(missing), dtype=np.int64)
    source_reset = source.reset_index()

    rows: list[pd.DataFrame] = []
    for instrument, target_group in missing.groupby("instrument", sort=False):
        source_group = source_reset[source_reset["instrument"] == instrument]
        if source_group.empty:
            continue
        target_sorted = target_group.sort_values("datetime")
        source_sorted = source_group.sort_values("datetime")
        merged = pd.merge_asof(
            target_sorted,
            source_sorted,
            on="datetime",
            by="instrument",
            direction="backward",
            allow_exact_matches=True,
        )
        rows.append(merged)

    if not rows:
        return pd.DataFrame(index=missing_index, columns=source.columns)

    out = pd.concat(rows, ignore_index=True).sort_values("_target_order")
    out_index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(out["datetime"]), out["instrument"].astype(str)],
        names=["datetime", "instrument"],
    )
    return out.set_index(out_index)[list(source.columns)]


def _flag_is_on(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "off").strip().lower() == INDUSTRY_EMBEDDING_ON


def _validate_min_coverage(value: Any) -> float:
    try:
        coverage = float(value)
    except (TypeError, ValueError) as exc:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_min_coverage_invalid: "
            f"value={value!r}"
        ) from exc
    if not 0.0 <= coverage <= 1.0:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_min_coverage_invalid: "
            f"value={coverage} expected_between_0_and_1"
        )
    return coverage
