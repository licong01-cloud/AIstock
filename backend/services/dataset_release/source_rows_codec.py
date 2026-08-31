"""Versioned streaming envelope for data-bearing source partition rows.

The semantic partition digest is computed by ``CanonicalPartitionHasher`` over
canonical rows before this envelope is applied.  CAS identity deliberately
binds the deterministic compressed byte stream instead.  Keeping those two
identities separate lets a future codec migration avoid pretending that the
source values changed.
"""

from __future__ import annotations

import zlib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Any

from .errors import SourceManifestError


SOURCE_ROWS_FORMAT = "canonical_ndjson_gzip_v1"
SOURCE_ROWS_CODEC = "gzip"
SOURCE_ROWS_CODEC_VERSION = "gzip_zlib_level1_v1"
SOURCE_ROWS_CODEC_LEVEL = 1
SOURCE_ROWS_CODEC_IDENTITY = "canonical_ndjson_gzip_v1:gzip:gzip_zlib_level1_v1:level-1"
SOURCE_ROWS_COMPRESSION_RATIO_QUANTUM = Decimal("0.00000001")
MAX_CODEC_CHUNK_BYTES = 16 * 1024 * 1024


@dataclass(slots=True)
class StreamingCompressionStats:
    """Mutable counters populated only while the bounded stream is consumed."""

    uncompressed_bytes: int = 0
    compressed_bytes: int = 0
    finalized: bool = False

    @property
    def compression_ratio(self) -> str:
        if not self.finalized or self.uncompressed_bytes <= 0:
            raise SourceManifestError("source row compression stream is incomplete")
        return compression_ratio_text(self.compressed_bytes, self.uncompressed_bytes)

    def as_descriptor_fields(self) -> dict[str, Any]:
        if not self.finalized or self.uncompressed_bytes <= 0:
            raise SourceManifestError("source row compression stream is incomplete")
        return {
            "rows_format": SOURCE_ROWS_FORMAT,
            "rows_codec": SOURCE_ROWS_CODEC,
            "rows_codec_version": SOURCE_ROWS_CODEC_VERSION,
            "rows_codec_level": SOURCE_ROWS_CODEC_LEVEL,
            "rows_uncompressed_bytes": self.uncompressed_bytes,
            "rows_compressed_bytes": self.compressed_bytes,
            "rows_compression_ratio": self.compression_ratio,
        }


def iter_gzip_level1(
    chunks: Iterable[bytes | bytearray | memoryview],
    stats: StreamingCompressionStats,
    *,
    max_chunk_bytes: int = MAX_CODEC_CHUNK_BYTES,
) -> Iterator[bytes]:
    """Yield a deterministic gzip stream without buffering a partition.

    ``wbits=31`` requests the RFC 1952 gzip wrapper.  CPython's zlib wrapper
    emits a deterministic zero-mtime header, unlike a default ``gzip.GzipFile``
    whose header may carry wall-clock time.
    """

    if type(max_chunk_bytes) is not int or not 0 < max_chunk_bytes <= MAX_CODEC_CHUNK_BYTES:
        raise ValueError("source row codec chunk boundary is invalid")
    if stats.uncompressed_bytes or stats.compressed_bytes or stats.finalized:
        raise SourceManifestError("source row compression stats cannot be reused")
    compressor = zlib.compressobj(
        SOURCE_ROWS_CODEC_LEVEL,
        zlib.DEFLATED,
        31,
    )
    for raw_chunk in chunks:
        if not isinstance(raw_chunk, (bytes, bytearray, memoryview)):
            raise SourceManifestError("source row codec chunks must be bytes-like")
        chunk = bytes(raw_chunk)
        if len(chunk) > max_chunk_bytes:
            raise SourceManifestError("source row codec input chunk exceeds memory bound")
        if not chunk:
            continue
        stats.uncompressed_bytes += len(chunk)
        compressed = compressor.compress(chunk)
        yield from _bounded_chunks(compressed, max_chunk_bytes=max_chunk_bytes, stats=stats)
    tail = compressor.flush(zlib.Z_FINISH)
    yield from _bounded_chunks(tail, max_chunk_bytes=max_chunk_bytes, stats=stats)
    stats.finalized = True


def validate_rows_envelope(
    value: Mapping[str, Any],
    *,
    cas_size: int,
) -> dict[str, Any]:
    """Validate and canonicalize one compressed-row descriptor."""

    expected_scalars = {
        "rows_format": SOURCE_ROWS_FORMAT,
        "rows_codec": SOURCE_ROWS_CODEC,
        "rows_codec_version": SOURCE_ROWS_CODEC_VERSION,
        "rows_codec_level": SOURCE_ROWS_CODEC_LEVEL,
    }
    if any(value.get(field) != expected for field, expected in expected_scalars.items()):
        raise SourceManifestError("source row compression envelope version differs")
    uncompressed = _bounded_positive_int(value.get("rows_uncompressed_bytes"), field="rows_uncompressed_bytes")
    compressed = _bounded_positive_int(value.get("rows_compressed_bytes"), field="rows_compressed_bytes")
    if compressed != cas_size:
        raise SourceManifestError("source row compressed size differs from CAS reference")
    expected_ratio = compression_ratio_text(compressed, uncompressed)
    if value.get("rows_compression_ratio") != expected_ratio:
        raise SourceManifestError("source row compression ratio differs")
    return {
        **expected_scalars,
        "rows_codec_identity": SOURCE_ROWS_CODEC_IDENTITY,
        "rows_uncompressed_bytes": uncompressed,
        "rows_compressed_bytes": compressed,
        "rows_compression_ratio": expected_ratio,
    }


def validate_rows_codec_identity(value: Mapping[str, Any]) -> str:
    """Return the exact codec identity after validating all version scalars."""

    expected = {
        "rows_format": SOURCE_ROWS_FORMAT,
        "rows_codec": SOURCE_ROWS_CODEC,
        "rows_codec_version": SOURCE_ROWS_CODEC_VERSION,
        "rows_codec_level": SOURCE_ROWS_CODEC_LEVEL,
        "rows_codec_identity": SOURCE_ROWS_CODEC_IDENTITY,
    }
    if any(value.get(field) != item for field, item in expected.items()):
        raise SourceManifestError("source row compression codec identity differs")
    return SOURCE_ROWS_CODEC_IDENTITY


def compression_ratio_text(compressed_bytes: int, uncompressed_bytes: int) -> str:
    if compressed_bytes <= 0 or uncompressed_bytes <= 0:
        raise SourceManifestError("source row compression sizes must be positive")
    with localcontext() as context:
        context.prec = 50
        value = (Decimal(compressed_bytes) / Decimal(uncompressed_bytes)).quantize(
            SOURCE_ROWS_COMPRESSION_RATIO_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
    return format(value, "f")


def _bounded_chunks(
    value: bytes,
    *,
    max_chunk_bytes: int,
    stats: StreamingCompressionStats,
) -> Iterator[bytes]:
    for offset in range(0, len(value), max_chunk_bytes):
        chunk = value[offset : offset + max_chunk_bytes]
        if chunk:
            stats.compressed_bytes += len(chunk)
            yield chunk


def _bounded_positive_int(value: Any, *, field: str) -> int:
    if type(value) is not int or not 0 < value <= 1 << 63:
        raise SourceManifestError(f"{field} is outside the bounded integer contract")
    return value


__all__ = [
    "MAX_CODEC_CHUNK_BYTES",
    "SOURCE_ROWS_CODEC",
    "SOURCE_ROWS_CODEC_IDENTITY",
    "SOURCE_ROWS_CODEC_LEVEL",
    "SOURCE_ROWS_CODEC_VERSION",
    "SOURCE_ROWS_FORMAT",
    "StreamingCompressionStats",
    "compression_ratio_text",
    "iter_gzip_level1",
    "validate_rows_codec_identity",
    "validate_rows_envelope",
]
