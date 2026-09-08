"""Immutable explicit suspension evidence for action-value research.

The shared candidate can contain an incomplete historical ``suspend_d``
component.  This module freezes the existing local authority into a
timing-owned artifact and only unions explicit ``suspend_type='S'`` keys.  It
never infers suspension from a missing price bar and never writes the database.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

from .action_value import ActionValueError
from .action_value_data import DailyCandidate, file_reference
from .artifact_store import PositionTimingArtifactStore
from .contracts import canonical_json_bytes, canonical_sha256


SNAPSHOT_SCHEMA = "position_timing_suspension_snapshot_v1"
SOURCE_QUERY_IDENTITY = {
    "table": "market.suspend_d",
    "filter": "ts_code IN symbols AND trade_date BETWEEN start AND end AND suspend_type='S'",
    "key": ["ts_code", "trade_date"],
    "semantics": "EXPLICIT_SUSPEND_D_NO_FILL",
}
_SYMBOL_PATTERN = re.compile(r"^\d{6}\.(SH|SZ)$")


@dataclass(frozen=True)
class SuspensionSnapshotBook:
    keys: tuple[tuple[str, date], ...]
    snapshot_sha256: str
    symbols: tuple[str, ...]
    start: date
    end: date

    def __post_init__(self) -> None:
        if (
            self.keys != tuple(sorted(self.keys))
            or len(self.keys) != len(set(self.keys))
            or not self.symbols
            or self.symbols != tuple(sorted(set(self.symbols)))
            or self.start > self.end
            or len(self.snapshot_sha256) != 64
            or any(symbol not in self.symbols or not self.start <= day <= self.end for symbol, day in self.keys)
        ):
            raise ActionValueError("SUSPENSION_SNAPSHOT_BOOK_INVALID")

    @classmethod
    def open(cls, path: Path) -> "SuspensionSnapshotBook":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ActionValueError("SUSPENSION_SNAPSHOT_UNAVAILABLE") from exc
        if not isinstance(payload, Mapping):
            raise ActionValueError("SUSPENSION_SNAPSHOT_SCHEMA_INVALID")
        identity = {key: value for key, value in payload.items() if key != "snapshot_sha256"}
        if (
            payload.get("schema_version") != SNAPSHOT_SCHEMA
            or payload.get("source_query") != SOURCE_QUERY_IDENTITY
            or payload.get("snapshot_sha256") != canonical_sha256(identity)
        ):
            raise ActionValueError("SUSPENSION_SNAPSHOT_IDENTITY_MISMATCH")
        try:
            scope = payload["scope"]
            rows = tuple(payload["suspensions"])
            if any(not isinstance(item, Mapping) for item in rows):
                raise TypeError("suspension row is not a mapping")
            symbols = tuple(str(item).upper() for item in scope["symbols"])
            start = date.fromisoformat(scope["start"])
            end = date.fromisoformat(scope["end"])
            source_row_count = int(payload["source_row_count"])
            keys = tuple(
                (str(item["symbol"]).upper(), date.fromisoformat(item["trade_date"]))
                for item in rows
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ActionValueError("SUSPENSION_SNAPSHOT_SCHEMA_INVALID") from exc
        if (
            source_row_count != len(rows)
            or any(item.get("suspend_type") != "S" for item in rows)
            or any(not _SYMBOL_PATTERN.fullmatch(symbol) for symbol in symbols)
        ):
            raise ActionValueError("SUSPENSION_SNAPSHOT_SCHEMA_INVALID")
        return cls(keys, payload["snapshot_sha256"], symbols, start, end)

    def apply(self, candidate: DailyCandidate, *, snapshot_path: Path) -> DailyCandidate:
        reference = file_reference(snapshot_path)
        references = {**candidate.references, "timing_suspension_snapshot": reference}
        return replace(
            candidate,
            suspension_keys=set(candidate.suspension_keys).union(self.keys),
            references=references,
        )


def freeze_suspension_snapshot(
    connection: Any,
    *,
    symbols: Sequence[str],
    start: date,
    end: date,
    timing_root: Path,
) -> Path:
    """Freeze an explicit, scoped, read-only view of ``market.suspend_d``."""

    normalized = tuple(sorted({str(symbol).upper() for symbol in symbols}))
    if (
        not normalized
        or start > end
        or any(not _SYMBOL_PATTERN.fullmatch(symbol) for symbol in normalized)
    ):
        raise ActionValueError("SUSPENSION_SNAPSHOT_SCOPE_INVALID")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT ts_code, trade_date, suspend_type, suspend_timing
                  FROM market.suspend_d
                 WHERE ts_code = ANY(%s)
                   AND trade_date BETWEEN %s AND %s
                   AND suspend_type = 'S'
                 ORDER BY ts_code, trade_date
                """,
                (list(normalized), start, end),
            )
            raw_rows = cursor.fetchall()
    except Exception as exc:
        raise ActionValueError("SUSPENSION_SOURCE_READ_FAILED") from exc
    payload = _snapshot_payload(raw_rows, symbols=normalized, start=start, end=end)
    digest = payload["snapshot_sha256"]
    path = (
        timing_root.resolve()
        / "research"
        / "action_value_incremental_v1"
        / "suspensions"
        / f"{digest}.json"
    ).resolve()
    if not path.is_relative_to(timing_root.resolve()):
        raise ActionValueError("SUSPENSION_SNAPSHOT_PATH_OUTSIDE_OWNER")
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(payload))
    return path


def _snapshot_payload(
    raw_rows: Iterable[Sequence[Any]],
    *,
    symbols: tuple[str, ...],
    start: date,
    end: date,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    keys: set[tuple[str, date]] = set()
    for values in raw_rows:
        if len(values) != 4:
            raise ActionValueError("SUSPENSION_SOURCE_SCHEMA_INVALID")
        symbol = str(values[0]).upper()
        trade_date = _as_date(values[1])
        suspend_type = str(values[2])
        if (
            symbol not in symbols
            or not start <= trade_date <= end
            or suspend_type != "S"
            or (symbol, trade_date) in keys
        ):
            raise ActionValueError("SUSPENSION_SOURCE_SCOPE_INVALID", symbol=symbol)
        keys.add((symbol, trade_date))
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "suspend_type": "S",
                "suspend_timing": None if values[3] is None else str(values[3]).strip() or None,
            }
        )
    rows.sort(key=lambda item: (item["symbol"], item["trade_date"]))
    identity = {
        "schema_version": SNAPSHOT_SCHEMA,
        "source_query": SOURCE_QUERY_IDENTITY,
        "scope": {"symbols": list(symbols), "start": start.isoformat(), "end": end.isoformat()},
        "source_row_count": len(rows),
        "suspensions": rows,
    }
    return {**identity, "snapshot_sha256": canonical_sha256(identity)}


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise ActionValueError("SUSPENSION_SOURCE_DATE_INVALID") from exc


__all__ = [
    "SNAPSHOT_SCHEMA",
    "SOURCE_QUERY_IDENTITY",
    "SuspensionSnapshotBook",
    "freeze_suspension_snapshot",
]
