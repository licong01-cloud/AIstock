"""Point-in-time industry providers for EfficientGATs industry bias.

The QE runner cannot pass callables through ``conf.yaml``.  It injects the
provider from Python when ``gats_adjacency_mode=industry_bias`` is requested.
The provider is intentionally path-based and clears its cached frame when
pickled so qlib ``params.pkl`` does not embed the sector source.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


INDUSTRY_BIAS_MODE = "industry_bias"
DEFAULT_SECTOR_SOURCE_NAME = "sector_data.h5"
DEFAULT_MIN_COVERAGE = 0.90
SOURCE_ENV = "QE_GATS_INDUSTRY_SOURCE_PATH"
MIN_COVERAGE_ENV = "QE_GATS_INDUSTRY_MIN_COVERAGE"
RDAGENT_FACTOR_DATA_ENV = "RDAGENT_FACTOR_DATA_WSL"

_SOURCE_KEYS = ("data", "/data")
_EXPLICIT_ID_COLUMNS = (
    "sw2_code",
    "sw2_id",
    "l2_code",
    "sw_l2_code",
    "industry_code_l2",
    "sector_code",
    "industry_code",
    "industry_id",
    "l1_code",
    "sw_l1_code",
)


class GatsIndustryProviderError(RuntimeError):
    """Fail-loud provider error carrying a stable reason_code in the message."""


@dataclass
class SectorDataIndustryIdProvider:
    """Return PIT SW industry ids aligned to a qlib segment MultiIndex.

    ``sector_data.h5`` currently stores expanded SW2 factor values rather than
    raw membership codes.  When no explicit industry-code column exists, rows
    are grouped by a per-day SW2 factor signature.  This still supplies the GAT
    model with the required same-industry equivalence relation while preserving
    the source table's PIT mapping.
    """

    source_path: str | os.PathLike[str]
    min_coverage: float = DEFAULT_MIN_COVERAGE
    hdf_key: str = "data"
    source_name: str = DEFAULT_SECTOR_SOURCE_NAME
    _frame: pd.DataFrame | None = field(default=None, init=False, repr=False)
    _id_column: str | None = field(default=None, init=False, repr=False)
    _signature_columns: tuple[str, ...] = field(default=(), init=False, repr=False)
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

        missing = values.isna() | values.astype(str).isin(["", "nan", "NaN", "None", "none", "<NA>"])
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
            "id_source": self._id_column or "sw2_signature",
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
        try:
            if path.suffix.lower() in {".h5", ".hdf", ".hdf5"}:
                frame = _read_hdf_first_key(path, self.hdf_key)
            elif path.suffix.lower() == ".parquet":
                frame = pd.read_parquet(path)
            elif path.suffix.lower() == ".csv":
                frame = pd.read_csv(path)
            else:
                raise GatsIndustryProviderError(
                    "reason_code=qe_gats_industry_source_format_unsupported: "
                    f"source={path} suffix={path.suffix}"
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
        self._signature_columns = tuple(col for col in frame.columns if str(col).startswith("sw2_"))
        if self._id_column is None and not self._signature_columns:
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_source_schema_invalid: "
                f"source={path} missing explicit industry id columns and sw2_* signature columns"
            )

        keep_columns = [self._id_column] if self._id_column is not None else list(self._signature_columns)
        frame = frame[keep_columns].sort_index()
        self._frame = frame
        return frame

    def _lookup_asof(self, source: pd.DataFrame, target_index: pd.MultiIndex) -> pd.DataFrame:
        exact = source.reindex(target_index)
        if not exact.isna().all(axis=1).any():
            return exact

        missing_mask = exact.isna().all(axis=1)
        if not bool(missing_mask.any()):
            return exact

        missing_index = target_index[missing_mask.to_numpy()]
        asof_rows = _asof_rows_by_instrument(source, missing_index)
        if not asof_rows.empty:
            exact.loc[asof_rows.index, asof_rows.columns] = asof_rows
        return exact

    def _industry_values_from_rows(self, rows: pd.DataFrame) -> pd.Series:
        if self._id_column is not None:
            values = rows[self._id_column].astype("object")
            return values.where(~values.isna(), np.nan)

        signature_columns = list(self._signature_columns)
        all_missing = rows[signature_columns].isna().all(axis=1)
        hashes = pd.util.hash_pandas_object(rows[signature_columns], index=False).astype("uint64")
        values = hashes.map(lambda value: f"sw2sig_{int(value):016x}").astype("object")
        values.loc[all_missing] = np.nan
        return values


def inject_gats_industry_provider_if_needed(
    config: dict[str, Any],
    *,
    cwd: str | os.PathLike[str] | None = None,
    print_fn=print,
) -> SectorDataIndustryIdProvider | None:
    """Inject a PIT SW2 provider into ``config.task.model.kwargs`` when needed."""

    model_kwargs = _model_kwargs_if_industry_bias(config)
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
            f"source_name={DEFAULT_SECTOR_SOURCE_NAME} candidates=[{candidates}]"
        )

    provider = SectorDataIndustryIdProvider(
        source_path=source_path,
        min_coverage=resolve_min_coverage(config),
    )
    provider.validate_source_available()
    model_kwargs["gats_industry_id_provider"] = provider
    print_fn(
        "[INFO] EfficientGATs industry_bias: injected PIT SW2 provider "
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

    if getattr(model, "gats_adjacency_mode", None) != INDUSTRY_BIAS_MODE:
        return None
    model_kwargs = _model_kwargs_if_industry_bias(config)
    if model_kwargs is None:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_config_missing: "
            "loaded_model_mode=industry_bias but config.task.model.kwargs.gats_adjacency_mode is not industry_bias"
        )
    provider = inject_gats_industry_provider_if_needed(config, cwd=cwd, print_fn=print_fn)
    if provider is None:
        provider = model_kwargs.get("gats_industry_id_provider")
    setattr(model, "gats_industry_id_provider", provider)
    return provider


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

    raw_candidates.append(base / DEFAULT_SECTOR_SOURCE_NAME)

    factor_dir = os.environ.get(RDAGENT_FACTOR_DATA_ENV)
    if factor_dir:
        raw_candidates.append(Path(factor_dir) / DEFAULT_SECTOR_SOURCE_NAME)

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


def _model_kwargs_if_industry_bias(config: dict[str, Any]) -> dict[str, Any] | None:
    task = config.get("task") if isinstance(config, dict) else None
    model_cfg = task.get("model") if isinstance(task, dict) else None
    model_kwargs = model_cfg.get("kwargs") if isinstance(model_cfg, dict) else None
    if not isinstance(model_kwargs, dict):
        return None
    if model_kwargs.get("gats_adjacency_mode", "off") != INDUSTRY_BIAS_MODE:
        return None
    return model_kwargs


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
    names = list(target.names)
    if names[0] != "datetime" or names[-1] != "instrument":
        target = pd.MultiIndex.from_arrays(
            [
                pd.to_datetime(target.get_level_values(0)),
                [_normalise_instrument_code(value) for value in target.get_level_values(target.nlevels - 1)],
            ],
            names=["datetime", "instrument"],
        )
    else:
        target = pd.MultiIndex.from_arrays(
            [
                pd.to_datetime(target.get_level_values("datetime")),
                [_normalise_instrument_code(value) for value in target.get_level_values("instrument")],
            ],
            names=["datetime", "instrument"],
        )
    return target


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
