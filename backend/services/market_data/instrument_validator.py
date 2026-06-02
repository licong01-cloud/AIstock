"""Shared market instrument validation before SQL execution.

The Paper v2 unified MiniQMT path uses the same guard for QE, realtime factor,
selection, and scheduler data loads. Malformed values must fail before SQL so a
bad symbol such as a timestamp-mixed ``603819.S2026-...`` cannot enter a large
market query or be silently dropped.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import itertools
import logging
import re
from typing import Any, Callable
from uuid import uuid4

TS_CODE_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")
INVALID_INSTRUMENT_SAMPLE_LIMIT = 10
DEFAULT_SQL_CHUNK_SIZE = 500
LARGE_QUERY_WARN_THRESHOLD = 800

logger = logging.getLogger("aistock.market_data.instrument_validator")


@dataclass(frozen=True)
class InstrumentValidationResult:
    raw_count: int
    ts_codes: list[str]
    invalid: list[dict[str, str]]
    source: str
    start_date: object | None = None
    end_date: object | None = None

    @property
    def valid_count(self) -> int:
        return len(self.ts_codes)

    @property
    def invalid_count(self) -> int:
        return len(self.invalid)

    def context(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "instruments_count": self.raw_count,
            "valid_count": self.valid_count,
            "invalid_count": self.invalid_count,
            "start_date": str(self.start_date) if self.start_date is not None else None,
            "end_date": str(self.end_date) if self.end_date is not None else None,
            "invalid_samples": self.invalid[:INVALID_INSTRUMENT_SAMPLE_LIMIT],
        }


def normalize_ts_code(code: object) -> str:
    value = str(code).strip()
    if not value:
        return value
    if "." in value:
        return value.upper()
    upper = value.upper()
    if len(upper) == 8 and upper[:2] in {"SH", "SZ", "BJ"} and upper[2:].isdigit():
        return f"{upper[2:]}.{upper[:2]}"
    return upper


def validate_ts_codes(
    instruments: Iterable[object] | object,
    *,
    source: str,
    start_date: object | None = None,
    end_date: object | None = None,
    allow_empty: bool = True,
) -> InstrumentValidationResult:
    if instruments is None:
        raw_values: list[object] = []
    elif isinstance(instruments, str):
        raw_values = [instruments]
    else:
        try:
            raw_values = list(instruments)  # type: ignore[arg-type]
        except TypeError:
            raw_values = [instruments]

    ts_codes: list[str] = []
    invalid: list[dict[str, str]] = []
    for index, raw in enumerate(raw_values):
        normalized = normalize_ts_code(raw)
        ts_codes.append(normalized)
        if not TS_CODE_PATTERN.fullmatch(normalized):
            invalid.append({"index": str(index), "raw": str(raw), "normalized": normalized})

    result = InstrumentValidationResult(
        raw_count=len(raw_values),
        ts_codes=ts_codes,
        invalid=invalid,
        source=source,
        start_date=start_date,
        end_date=end_date,
    )
    if invalid:
        raise ValueError(
            "invalid ts_code values before SQL execution: "
            f"source={source} instruments_count={result.raw_count} invalid_count={len(invalid)} "
            f"start_date={start_date} end_date={end_date} "
            f"invalid_samples={invalid[:INVALID_INSTRUMENT_SAMPLE_LIMIT]}"
        )
    if not allow_empty and not ts_codes:
        raise ValueError(
            "empty ts_code list before SQL execution: "
            f"source={source} start_date={start_date} end_date={end_date}"
        )
    return result


def normalize_and_validate_ts_codes(
    instruments: Iterable[object] | object,
    *,
    source: str,
    start_date: object | None = None,
    end_date: object | None = None,
    allow_empty: bool = True,
) -> list[str]:
    return validate_ts_codes(
        instruments,
        source=source,
        start_date=start_date,
        end_date=end_date,
        allow_empty=allow_empty,
    ).ts_codes


def chunked_ts_codes(ts_codes: Iterable[str], *, chunk_size: int = DEFAULT_SQL_CHUNK_SIZE) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive: {chunk_size}")
    values = list(ts_codes)
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def query_correlation_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"


def load_chunks_with_logging(
    *,
    ts_codes: list[str],
    source: str,
    start_date: object,
    end_date: object,
    chunk_size: int = DEFAULT_SQL_CHUNK_SIZE,
    loader: Callable[[list[str], int, str], Any],
) -> list[Any]:
    correlation_id = query_correlation_id(source)
    chunks = chunked_ts_codes(ts_codes, chunk_size=chunk_size)
    if len(ts_codes) >= LARGE_QUERY_WARN_THRESHOLD or len(chunks) > 1:
        logger.info(
            "market data SQL chunked query: source=%s correlation_id=%s symbols=%s chunks=%s chunk_size=%s start_date=%s end_date=%s",
            source,
            correlation_id,
            len(ts_codes),
            len(chunks),
            chunk_size,
            start_date,
            end_date,
        )
    results: list[Any] = []
    for index, chunk in enumerate(chunks, start=1):
        logger.debug(
            "market data SQL chunk: source=%s correlation_id=%s chunk=%s/%s symbols=%s sample=%s",
            source,
            correlation_id,
            index,
            len(chunks),
            len(chunk),
            list(itertools.islice(chunk, 3)),
        )
        results.append(loader(chunk, index, correlation_id))
    return results
