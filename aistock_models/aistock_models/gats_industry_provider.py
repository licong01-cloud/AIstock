"""Point-in-time Shenwan L2 providers for EfficientGATs.

The QE runner cannot pass callables through ``conf.yaml``.  It injects the
provider from Python when EfficientGATs needs industry ids for either
``industry_bias`` adjacency or ``gats_industry_embedding=on``.  The provider
queries authoritative ``market.sw_index_member`` PIT rows at runtime and clears
all lookup caches when pickled so qlib ``params.pkl`` does not embed source
membership data.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

import numpy as np
import pandas as pd


INDUSTRY_BIAS_MODE = "industry_bias"
INDUSTRY_EMBEDDING_ON = "on"
DEFAULT_MIN_COVERAGE = 0.90
MIN_COVERAGE_ENV = "QE_GATS_INDUSTRY_MIN_COVERAGE"
DEFAULT_SOURCE_NAME = "market.sw_index_member"

ConnFactory = Callable[[], Iterator[Any]]


class GatsIndustryProviderError(RuntimeError):
    """Fail-loud provider error carrying a stable reason_code in the message."""


@dataclass
class SwIndexMemberIndustryIdProvider:
    """Return PIT Shenwan L2 codes aligned to a qlib segment MultiIndex.

    The SQL mirrors Selection Center's authoritative provider: for each
    ``(trade_date, ts_code)``, choose the latest membership row whose
    ``in_date`` is not after the target date and whose ``out_date`` is either
    null or not before the target date.  Only ``l2_code`` is returned to the
    model; missing codes remain missing so the model can map them to the
    explicit unknown class.
    """

    min_coverage: float = DEFAULT_MIN_COVERAGE
    conn_factory: ConnFactory | None = field(default=None, repr=False, compare=False)
    source_name: str = DEFAULT_SOURCE_NAME
    _daily_cache: dict[tuple[date, tuple[str, ...]], dict[str, str | None]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    last_coverage: dict[str, Any] | None = field(default=None, init=False)
    coverage_history: list[dict[str, Any]] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.min_coverage = _validate_min_coverage(self.min_coverage)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state["_daily_cache"] = {}
        state["conn_factory"] = None
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
            raise TypeError("SwIndexMemberIndustryIdProvider expects index or segment,index")
        return self.get_industry_ids(index, segment_name=str(segment_name or "unknown"))

    def get_industry_ids(self, index: Any, *, segment_name: str = "unknown") -> pd.Series:
        return_index = _coerce_return_index(index)
        target_index = _normalise_target_index(return_index)
        if len(target_index) == 0:
            return pd.Series([], index=return_index, dtype="object")

        target_frame = pd.DataFrame(
            {
                "_pos": np.arange(len(target_index), dtype=np.int64),
                "trade_date": [pd.Timestamp(value).date() for value in target_index.get_level_values("datetime")],
                "ts_code": list(target_index.get_level_values("instrument")),
            }
        )
        values: list[str | None] = [None] * len(target_frame)
        for trade_date, group in target_frame.groupby("trade_date", sort=False):
            symbols = [str(symbol) for symbol in group["ts_code"].drop_duplicates().tolist()]
            daily = self._lookup_l2_codes(trade_date, symbols)
            for pos, symbol in zip(group["_pos"], group["ts_code"]):
                values[int(pos)] = daily.get(str(symbol))

        series = pd.Series(values, index=return_index, dtype="object")
        missing = series.isna() | series.astype(str).isin(["", "nan", "NaN", "None", "none", "<NA>"])
        covered_rows = int((~missing).sum())
        rows = int(len(series))
        coverage = covered_rows / rows if rows else 1.0
        payload = {
            "reason_code": "qe_gats_industry_coverage",
            "segment": segment_name,
            "rows": rows,
            "covered_rows": covered_rows,
            "missing_rows": int(missing.sum()),
            "coverage": coverage,
            "threshold": self.min_coverage,
            "source": self.source_name,
            "id_source": "l2_code",
        }
        self.last_coverage = payload
        self.coverage_history.append(payload)

        if coverage < self.min_coverage:
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_coverage_below_threshold: "
                f"segment={segment_name} source={self.source_name} "
                f"coverage={coverage:.6f} threshold={self.min_coverage:.6f} "
                f"rows={rows} covered_rows={covered_rows} missing_rows={int(missing.sum())}"
            )

        return series

    def _lookup_l2_codes(self, trade_date: date, symbols: list[str]) -> dict[str, str | None]:
        if not symbols:
            return {}
        key = (trade_date, tuple(sorted(symbols)))
        cached = self._daily_cache.get(key)
        if cached is not None:
            return cached

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH ranked AS (
                            SELECT
                                ts_code, l2_code, in_date, out_date,
                                ROW_NUMBER() OVER (
                                    PARTITION BY ts_code
                                    ORDER BY in_date DESC NULLS LAST,
                                             out_date DESC NULLS LAST,
                                             l3_code NULLS LAST
                                ) AS rn
                            FROM market.sw_index_member
                            WHERE ts_code = ANY(%s)
                              AND in_date <= %s
                              AND (out_date IS NULL OR out_date >= %s)
                        )
                        SELECT ts_code, l2_code, in_date, out_date
                        FROM ranked
                        WHERE rn = 1
                        ORDER BY ts_code
                        """,
                        (symbols, trade_date, trade_date),
                    )
                    rows = cur.fetchall()
        except GatsIndustryProviderError:
            raise
        except Exception as exc:
            raise GatsIndustryProviderError(
                "reason_code=qe_gats_industry_lookup_failed: "
                f"source={self.source_name} trade_date={trade_date.isoformat()} "
                f"symbol_count={len(symbols)} error={type(exc).__name__}: {exc}"
            ) from exc

        result = {symbol: None for symbol in symbols}
        for row in rows:
            symbol = _row_value(row, 0, "ts_code")
            l2_code = _clean(_row_value(row, 1, "l2_code"))
            if symbol is not None:
                result[str(symbol)] = l2_code
        self._daily_cache[key] = result
        return result

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        factory = self.conn_factory
        if factory is not None:
            with factory() as conn:
                yield conn
            return
        with _default_conn_factory() as conn:
            yield conn


class SectorDataIndustryIdProvider(SwIndexMemberIndustryIdProvider):
    """Backward-compatible name for the upgraded DB-backed provider.

    Older call sites passed a sector source path to this class.  The true L2
    implementation intentionally ignores that file path and reads
    ``market.sw_index_member`` instead; no SW2 signature fallback remains.
    """

    def __init__(self, source_path: str | os.PathLike[str] | None = None, **kwargs: Any) -> None:
        self.legacy_source_path = str(source_path) if source_path is not None else None
        super().__init__(**kwargs)

    def __getstate__(self) -> dict[str, Any]:
        state = super().__getstate__()
        state["legacy_source_path"] = None
        return state


def inject_gats_industry_provider_if_needed(
    config: dict[str, Any],
    *,
    cwd: str | os.PathLike[str] | None = None,
    print_fn=print,
) -> SwIndexMemberIndustryIdProvider | None:
    """Inject a PIT SW L2 provider into ``config.task.model.kwargs`` when needed."""

    del cwd
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

    provider = SwIndexMemberIndustryIdProvider(min_coverage=resolve_min_coverage(config))
    model_kwargs["gats_industry_id_provider"] = provider
    print_fn(
        "[INFO] EfficientGATs industry: injected PIT SW L2 provider "
        f"source={provider.source_name} min_coverage={provider.min_coverage:.4f}"
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
    """Deprecated compatibility shim: true L2 lookup no longer reads files."""

    del config, cwd
    return None


def sector_source_candidates(
    config: dict[str, Any],
    *,
    cwd: str | os.PathLike[str] | None = None,
) -> list[Path]:
    """Deprecated compatibility shim: true L2 lookup no longer reads files."""

    del config, cwd
    return []


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


def _row_value(row: Any, index: int, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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


def _db_cfg() -> dict[str, Any]:
    return {
        "host": _first_env("TDX_DB_HOST", "POSTGRES_HOST", "PG_HOST", default="127.0.0.1"),
        "port": int(_first_env("TDX_DB_PORT", "POSTGRES_PORT", "PG_PORT", default="5432")),
        "user": _first_env("TDX_DB_USER", "POSTGRES_USER", "PG_USER", default="postgres"),
        "password": _require_env(
            "TDX_DB_PASSWORD",
            "POSTGRES_PASSWORD",
            "PG_PASSWORD",
        ),
        "dbname": _first_env("TDX_DB_NAME", "POSTGRES_DB", "PG_DATABASE", default="aistock"),
        "application_name": "AIstock-EfficientGATs-industry-provider",
        "options": "-c client_encoding=utf8",
    }


def _first_env(*keys: str, default: str) -> str:
    for key in keys:
        value = os.environ.get(key)
        if value not in (None, ""):
            return str(value)
    return default


def _require_env(*keys: str) -> str:
    for key in keys:
        try:
            value = os.environ[key]
        except KeyError:
            continue
        if value != "":
            return value
    raise GatsIndustryProviderError(
        "reason_code=qe_gats_industry_db_password_missing: "
        "set one of TDX_DB_PASSWORD/POSTGRES_PASSWORD/PG_PASSWORD"
    )


@contextmanager
def _default_conn_factory() -> Iterator[Any]:
    try:
        import psycopg2
    except ModuleNotFoundError as exc:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_db_driver_missing: "
            "psycopg2 is required to query market.sw_index_member"
        ) from exc

    try:
        conn = psycopg2.connect(**_db_cfg())
    except GatsIndustryProviderError:
        raise
    except Exception as exc:
        raise GatsIndustryProviderError(
            "reason_code=qe_gats_industry_db_connect_failed: "
            f"source={DEFAULT_SOURCE_NAME} error={type(exc).__name__}: {exc}"
        ) from exc
    try:
        yield conn
    finally:
        conn.close()
