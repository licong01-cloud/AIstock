"""Frozen point-in-time stock-universe primitives for dataset releases.

This module deliberately has no database access.  A planner reads PIT state in
one read-only source transaction, then passes the rows and state identity here.
The returned snapshot is immutable and is the only PIT authority accepted by
materializers and validators.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    CANONICAL_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
    require_canonical_consumer_binding,
)

from .errors import DatasetReleaseError


PIT_COLUMNS = (
    "ts_code",
    "eligible_start",
    "eligible_end",
    "entry_reason",
    "exit_reason",
)
DATASET_PIT_BINDING_SCHEMA = "dataset_release_canonical_pit_binding_v1"
DATASET_CANDIDATE_MANIFEST_SCHEMA = "dataset_release_candidate_manifest_v1"
_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_NULL_REASON = "<NULL>"


class PitSnapshotError(DatasetReleaseError):
    """Base class for a frozen PIT contract failure."""

    code = "DATASET_RELEASE_PIT_SNAPSHOT_INVALID"


class PitStateNotReady(PitSnapshotError):
    """Raised when the supplied PIT state is not ready for the requested scope."""

    code = "BLOCKED_PIT_STATE_NOT_READY"


class PitSpanInvalid(PitSnapshotError):
    """Raised when PIT rows are ambiguous, overlapping, or malformed."""

    code = "DATASET_RELEASE_PIT_SPAN_INVALID"


def require_canonical_source_snapshot(snapshot: "FrozenPitSnapshot") -> PitConsumerBinding:
    """Validate the sealed cutoff view before a release identity exists."""

    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=snapshot.universe_key,
        rule_version=snapshot.rule_version,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        snapshot_digest=snapshot.spans_sha256,
        cutoff=snapshot.cutoff,
    )
    try:
        return require_canonical_consumer_binding(binding, consumer="dataset_release_source")
    except ValueError as exc:
        raise PitSnapshotError(str(exc)) from exc


def require_canonical_frozen_snapshot(
    snapshot: "FrozenPitSnapshot",
    *,
    release_id: str,
    rolling_cutoff_spans_sha256: str,
    consumer: str = "qe",
) -> PitConsumerBinding:
    """Prove a frozen release is the rolling canonical rule at its cutoff."""

    if snapshot.universe_key != CANONICAL_PIT_UNIVERSE_KEY:
        raise PitSnapshotError("frozen PIT source is not the active canonical rolling universe")
    if snapshot.rule_version != CANONICAL_PIT_RULE_VERSION:
        raise PitSnapshotError("frozen PIT rule version differs from canonical authority")
    _require_sha256(rolling_cutoff_spans_sha256, field="rolling_cutoff_spans_sha256")
    if snapshot.spans_sha256 != rolling_cutoff_spans_sha256:
        raise PitSnapshotError("frozen PIT digest differs from rolling canonical digest at cutoff")
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{str(release_id).strip()}",
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        snapshot_digest=snapshot.spans_sha256,
        cutoff=snapshot.cutoff,
        release_id=str(release_id).strip(),
    )
    try:
        return require_canonical_consumer_binding(binding, consumer=consumer, immutable_snapshot_required=True)
    except ValueError as exc:
        raise PitSnapshotError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class DatasetPitBinding:
    """Immutable release binding shared by QE/training compute consumers.

    The rolling and frozen digests are kept as separate fields so a release
    cannot claim canonical parity by supplying only one self-consistent value.
    They must be equal at the release cutoff before this object is accepted.
    """

    authority_id: str
    authority_status: str
    scope: str
    rolling_universe_key: str
    frozen_universe_key: str
    rule_version: str
    rule_parameters_digest: str
    cutoff: date
    rolling_cutoff_spans_sha256: str
    frozen_snapshot_digest: str
    release_id: str
    schema_version: str = DATASET_PIT_BINDING_SCHEMA

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "authority_id": self.authority_id,
            "authority_status": self.authority_status,
            "scope": self.scope,
            "rolling_universe_key": self.rolling_universe_key,
            "frozen_universe_key": self.frozen_universe_key,
            "rule_version": self.rule_version,
            "rule_parameters_digest": self.rule_parameters_digest,
            "cutoff": self.cutoff.isoformat(),
            "rolling_cutoff_spans_sha256": self.rolling_cutoff_spans_sha256,
            "frozen_snapshot_digest": self.frozen_snapshot_digest,
            "release_id": self.release_id,
        }

    def consumer_binding(self, *, consumer: str) -> PitConsumerBinding:
        if self.scope != "full" and consumer != "candidate_validation":
            raise PitSnapshotError("sample PIT binding cannot drive QE/training consumers")
        try:
            binding = PitConsumerBinding(
                authority_id=self.authority_id,
                authority_status=PitAuthorityStatus(self.authority_status),
                universe_key=self.frozen_universe_key,
                rule_version=self.rule_version,
                rule_parameters_digest=self.rule_parameters_digest,
                snapshot_digest=self.frozen_snapshot_digest,
                cutoff=self.cutoff,
                release_id=self.release_id,
            )
            return require_canonical_consumer_binding(
                binding,
                consumer=consumer,
                immutable_snapshot_required=True,
            )
        except ValueError as exc:
            raise PitSnapshotError(str(exc)) from exc

    @classmethod
    def from_release_manifest(cls, value: Mapping[str, Any]) -> "DatasetPitBinding":
        manifest = dict(value)
        if manifest.get("schema_version") == DATASET_CANDIDATE_MANIFEST_SCHEMA:
            binding_value = manifest.get("pit_binding")
            if not isinstance(binding_value, Mapping):
                raise PitSnapshotError("release manifest has no canonical PIT binding")
            expected_release_id = str(manifest.get("release_id") or "").strip()
            expected_cutoff = str(manifest.get("cutoff") or "").strip()
            expected_scope = str(manifest.get("scope") or "").strip()
        else:
            binding_value = manifest
            expected_release_id = ""
            expected_cutoff = ""
            expected_scope = ""
        required = {
            "schema_version",
            "authority_id",
            "authority_status",
            "scope",
            "rolling_universe_key",
            "frozen_universe_key",
            "rule_version",
            "rule_parameters_digest",
            "cutoff",
            "rolling_cutoff_spans_sha256",
            "frozen_snapshot_digest",
            "release_id",
        }
        if set(binding_value) != required or binding_value.get("schema_version") != DATASET_PIT_BINDING_SCHEMA:
            raise PitSnapshotError("release PIT binding schema/fields are invalid")
        try:
            cutoff = date.fromisoformat(str(binding_value["cutoff"]))
        except ValueError as exc:
            raise PitSnapshotError("release PIT binding cutoff is invalid") from exc
        release_id = str(binding_value["release_id"] or "").strip()
        if not release_id:
            raise PitSnapshotError("release PIT binding release_id is empty")
        rolling_digest = str(binding_value["rolling_cutoff_spans_sha256"])
        frozen_digest = str(binding_value["frozen_snapshot_digest"])
        _require_sha256(rolling_digest, field="rolling_cutoff_spans_sha256")
        _require_sha256(frozen_digest, field="frozen_snapshot_digest")
        if rolling_digest != frozen_digest:
            raise PitSnapshotError("release rolling/frozen PIT digests differ")
        binding = cls(
            authority_id=str(binding_value["authority_id"]),
            authority_status=str(binding_value["authority_status"]),
            scope=str(binding_value["scope"]),
            rolling_universe_key=str(binding_value["rolling_universe_key"]),
            frozen_universe_key=str(binding_value["frozen_universe_key"]),
            rule_version=str(binding_value["rule_version"]),
            rule_parameters_digest=str(binding_value["rule_parameters_digest"]),
            cutoff=cutoff,
            rolling_cutoff_spans_sha256=rolling_digest,
            frozen_snapshot_digest=frozen_digest,
            release_id=release_id,
        )
        if expected_release_id and release_id != expected_release_id:
            raise PitSnapshotError("release manifest/PIT release_id differ")
        if expected_cutoff and cutoff.isoformat() != expected_cutoff:
            raise PitSnapshotError("release manifest/PIT cutoff differ")
        if expected_scope and binding.scope != expected_scope:
            raise PitSnapshotError("release manifest/PIT scope differ")
        if binding.rolling_universe_key != CANONICAL_PIT_UNIVERSE_KEY:
            raise PitSnapshotError("release rolling PIT key is not canonical")
        expected_frozen_key = f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{release_id}"
        if binding.frozen_universe_key != expected_frozen_key:
            raise PitSnapshotError("release frozen PIT key differs from release identity")
        if binding.scope not in {"sample", "full"}:
            raise PitSnapshotError("release PIT binding scope is invalid")
        binding.consumer_binding(consumer=("candidate_validation" if binding.scope == "sample" else "qe_training"))
        return binding


def build_dataset_pit_binding(
    snapshot: "FrozenPitSnapshot",
    *,
    release_id: str,
    rolling_cutoff_spans_sha256: str,
    scope: str = "full",
) -> DatasetPitBinding:
    """Build and read back the canonical release binding from one PIT snapshot."""

    normalized_scope = str(scope).strip().lower()
    if normalized_scope not in {"sample", "full"}:
        raise PitSnapshotError("release PIT binding scope is invalid")
    consumer = require_canonical_frozen_snapshot(
        snapshot,
        release_id=release_id,
        rolling_cutoff_spans_sha256=rolling_cutoff_spans_sha256,
        consumer="qe_training",
    )
    value = DatasetPitBinding(
        authority_id=consumer.authority_id,
        authority_status=consumer.authority_status.value,
        scope=normalized_scope,
        rolling_universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        frozen_universe_key=consumer.universe_key,
        rule_version=consumer.rule_version,
        rule_parameters_digest=consumer.rule_parameters_digest,
        cutoff=snapshot.cutoff,
        rolling_cutoff_spans_sha256=rolling_cutoff_spans_sha256,
        frozen_snapshot_digest=snapshot.spans_sha256,
        release_id=str(release_id).strip(),
    )
    return DatasetPitBinding.from_release_manifest(value.as_dict())


@dataclass(frozen=True, slots=True)
class FrozenPitSpan:
    ts_code: str
    eligible_start: date
    eligible_end: date
    entry_reason: str | None
    exit_reason: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ts_code": self.ts_code,
            "eligible_start": self.eligible_start.isoformat(),
            "eligible_end": self.eligible_end.isoformat(),
            "entry_reason": self.entry_reason,
            "exit_reason": self.exit_reason,
        }


@dataclass(frozen=True, slots=True)
class FrozenPitSnapshot:
    """Canonical, scope-clipped PIT artifact bound to source state identity."""

    universe_key: str
    rule_version: str
    scope_start: date
    cutoff: date
    state_identity: str
    source_fingerprint_sha256: str
    parameter_hash: str
    spans_sha256: str
    spans: tuple[FrozenPitSpan, ...]
    schema_version: str = "dataset_release_frozen_pit_v1"

    @property
    def unique_instruments(self) -> int:
        return len({span.ts_code for span in self.spans})

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": span.ts_code,
                    "eligible_start": span.eligible_start,
                    "eligible_end": span.eligible_end,
                    "entry_reason": span.entry_reason,
                    "exit_reason": span.exit_reason,
                }
                for span in self.spans
            ],
            columns=PIT_COLUMNS,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "universe_key": self.universe_key,
            "rule_version": self.rule_version,
            "scope": {
                "start": self.scope_start.isoformat(),
                "cutoff": self.cutoff.isoformat(),
            },
            "state_identity": self.state_identity,
            "source_fingerprint_sha256": self.source_fingerprint_sha256,
            "parameter_hash": self.parameter_hash,
            "spans_sha256": self.spans_sha256,
            "span_count": len(self.spans),
            "instrument_count": self.unique_instruments,
            "spans": [span.as_dict() for span in self.spans],
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.as_dict())


def frozen_pit_snapshot_from_mapping(value: Mapping[str, Any]) -> FrozenPitSnapshot:
    """Strictly reconstruct a frozen PIT artifact from its canonical mapping."""

    expected_fields = {
        "schema_version",
        "universe_key",
        "rule_version",
        "scope",
        "state_identity",
        "source_fingerprint_sha256",
        "parameter_hash",
        "spans_sha256",
        "span_count",
        "instrument_count",
        "spans",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected_fields
        or value.get("schema_version") != "dataset_release_frozen_pit_v1"
        or not isinstance(value.get("scope"), Mapping)
        or set(value["scope"]) != {"start", "cutoff"}
        or not isinstance(value.get("spans"), list)
    ):
        raise PitSnapshotError("frozen PIT artifact schema is invalid")
    try:
        snapshot = freeze_pit_snapshot(
            value["spans"],
            universe_key=str(value["universe_key"]),
            rule_version=str(value["rule_version"]),
            scope_start=date.fromisoformat(str(value["scope"]["start"])),
            cutoff=date.fromisoformat(str(value["scope"]["cutoff"])),
            state_identity=str(value["state_identity"]),
            source_fingerprint_sha256=str(value["source_fingerprint_sha256"]),
            parameter_hash=str(value["parameter_hash"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PitSnapshotError("frozen PIT artifact payload is invalid") from exc
    if snapshot.as_dict() != dict(value):
        raise PitSnapshotError("frozen PIT artifact identity/digest differs")
    return snapshot


def freeze_pit_snapshot(
    spans: pd.DataFrame | Iterable[Mapping[str, Any] | Sequence[Any]],
    *,
    universe_key: str,
    rule_version: str,
    scope_start: date,
    cutoff: date,
    state_identity: str,
    source_fingerprint_sha256: str,
    parameter_hash: str,
    state_status: str = "ready",
    state_dirty: bool = False,
    state_start: date | None = None,
    state_end: date | None = None,
) -> FrozenPitSnapshot:
    """Validate and freeze PIT rows without rebuilding or querying anything.

    Spans are clipped to the requested release scope before hashing.  A PIT-only
    change inside that scope therefore changes ``spans_sha256`` and release
    identity, while irrelevant history outside the scope does not.
    """

    if cutoff < scope_start:
        raise PitSnapshotError("PIT cutoff must not precede scope_start")
    if str(state_status).lower() != "ready" or bool(state_dirty):
        raise PitStateNotReady(f"PIT state is not ready: status={state_status!r} dirty={bool(state_dirty)}")
    if state_start is not None and _to_date(state_start, field="state_start") > scope_start:
        raise PitStateNotReady("PIT state does not cover requested scope_start")
    if state_end is not None and _to_date(state_end, field="state_end") < cutoff:
        raise PitStateNotReady("PIT state does not cover requested cutoff")
    for field, value in (
        ("universe_key", universe_key),
        ("rule_version", rule_version),
        ("state_identity", state_identity),
    ):
        if not str(value or "").strip():
            raise PitSnapshotError(f"{field} must be non-empty")
    _require_sha256(source_fingerprint_sha256, field="source_fingerprint_sha256")
    _require_sha256(parameter_hash, field="parameter_hash")

    frame = canonicalize_pit_spans(spans, scope_start=scope_start, cutoff=cutoff)
    if frame.empty:
        raise PitSpanInvalid("frozen PIT snapshot would be empty")
    frozen = tuple(
        FrozenPitSpan(
            ts_code=str(row.ts_code),
            eligible_start=row.eligible_start,
            eligible_end=row.eligible_end,
            entry_reason=row.entry_reason,
            exit_reason=row.exit_reason,
        )
        for row in frame.itertuples(index=False)
    )
    digest = pit_spans_sha256(frame)
    return FrozenPitSnapshot(
        universe_key=str(universe_key),
        rule_version=str(rule_version),
        scope_start=scope_start,
        cutoff=cutoff,
        state_identity=str(state_identity),
        source_fingerprint_sha256=source_fingerprint_sha256,
        parameter_hash=parameter_hash,
        spans_sha256=digest,
        spans=frozen,
    )


def canonicalize_pit_spans(
    spans: pd.DataFrame | Iterable[Mapping[str, Any] | Sequence[Any]],
    *,
    scope_start: date | None = None,
    cutoff: date | None = None,
) -> pd.DataFrame:
    frame = _as_frame(spans)
    if frame.empty:
        return pd.DataFrame(columns=PIT_COLUMNS)
    missing = [column for column in PIT_COLUMNS if column not in frame.columns]
    if missing:
        raise PitSpanInvalid(f"PIT spans missing columns: {missing}")
    frame = frame.loc[:, PIT_COLUMNS].copy()
    frame["ts_code"] = frame["ts_code"].map(_normalize_code)
    frame["eligible_start"] = frame["eligible_start"].map(lambda value: _to_date(value, field="eligible_start"))
    frame["eligible_end"] = frame["eligible_end"].map(lambda value: _to_date(value, field="eligible_end"))
    for reason in ("entry_reason", "exit_reason"):
        frame[reason] = frame[reason].map(_normalize_reason)

    if scope_start is not None:
        frame = frame.loc[frame["eligible_end"] >= scope_start].copy()
        frame["eligible_start"] = frame["eligible_start"].map(lambda value: max(value, scope_start))
    if cutoff is not None:
        frame = frame.loc[frame["eligible_start"] <= cutoff].copy()
        frame["eligible_end"] = frame["eligible_end"].map(lambda value: min(value, cutoff))
    if (frame["eligible_start"] > frame["eligible_end"]).any():
        raise PitSpanInvalid("PIT span start is after end")
    frame = frame.sort_values(
        ["ts_code", "eligible_start", "eligible_end", "entry_reason", "exit_reason"],
        na_position="first",
        kind="mergesort",
    ).reset_index(drop=True)
    if frame.duplicated(list(PIT_COLUMNS), keep=False).any():
        raise PitSpanInvalid("duplicate canonical PIT spans detected")

    for code, group in frame.groupby("ts_code", sort=False):
        previous_end: date | None = None
        for row in group.itertuples(index=False):
            if previous_end is not None and row.eligible_start <= previous_end:
                raise PitSpanInvalid(f"overlapping PIT spans detected for {code}")
            previous_end = row.eligible_end
    return frame


def pit_spans_sha256(
    spans: FrozenPitSnapshot | pd.DataFrame | Iterable[Mapping[str, Any] | Sequence[Any]],
) -> str:
    if isinstance(spans, FrozenPitSnapshot):
        return spans.spans_sha256
    frame = canonicalize_pit_spans(spans)
    rows = []
    for row in frame.itertuples(index=False):
        rows.append(
            [
                row.ts_code,
                row.eligible_start.isoformat(),
                row.eligible_end.isoformat(),
                row.entry_reason if row.entry_reason is not None else _NULL_REASON,
                row.exit_reason if row.exit_reason is not None else _NULL_REASON,
            ]
        )
    return hashlib.sha256(_canonical_json_bytes(rows)).hexdigest()


def filter_frame_to_pit_spans(
    frame: pd.DataFrame,
    snapshot_or_spans: FrozenPitSnapshot | pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Apply a frozen multi-span PIT mask to a Qlib-indexed frame."""

    if frame is None or frame.empty:
        empty = pd.DataFrame() if frame is None else frame.copy()
        return empty, {
            "rows_before": 0,
            "rows_after": 0,
            "rows_removed": 0,
            "removed_by_reason": {"no_span": 0, "outside_span": 0},
            "pit_spans_sha256": pit_spans_sha256(snapshot_or_spans),
        }
    if not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != [
        "datetime",
        "instrument",
    ]:
        raise PitSpanInvalid("PIT mask requires MultiIndex[datetime,instrument]")
    if frame.index.has_duplicates:
        raise PitSpanInvalid("PIT input frame contains duplicate index rows")

    spans = (
        snapshot_or_spans.to_frame()
        if isinstance(snapshot_or_spans, FrozenPitSnapshot)
        else canonicalize_pit_spans(snapshot_or_spans)
    )
    span_map = {
        str(code): tuple((row.eligible_start, row.eligible_end) for row in group.itertuples(index=False))
        for code, group in spans.groupby("ts_code", sort=False)
    }
    pieces: list[pd.DataFrame] = []
    removed = {"no_span": 0, "outside_span": 0}
    for instrument, group in frame.groupby(level="instrument", sort=False):
        ranges = span_map.get(str(instrument).upper())
        if not ranges:
            removed["no_span"] += len(group)
            continue
        dates = pd.to_datetime(group.index.get_level_values("datetime"), errors="raise").date
        keep = np.zeros(len(group), dtype=bool)
        for eligible_start, eligible_end in ranges:
            keep |= (dates >= eligible_start) & (dates <= eligible_end)
        removed["outside_span"] += int((~keep).sum())
        if keep.any():
            pieces.append(group.iloc[np.flatnonzero(keep)])
    output = pd.concat(pieces).sort_index() if pieces else frame.iloc[0:0].copy()
    if output.index.has_duplicates:
        raise PitSpanInvalid("PIT-masked frame contains duplicate index rows")
    return output, {
        "rows_before": int(len(frame)),
        "rows_after": int(len(output)),
        "rows_removed": int(len(frame) - len(output)),
        "removed_by_reason": removed,
        "pit_spans_sha256": pit_spans_sha256(snapshot_or_spans),
    }


def write_frozen_pit_snapshot(path: Path, snapshot: FrozenPitSnapshot) -> dict[str, Any]:
    """Create a frozen PIT artifact without replacing an existing file."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = snapshot.canonical_bytes()
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return {
        "path": str(path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
        "spans_sha256": snapshot.spans_sha256,
    }


def write_pit_all_txt(
    path: Path,
    snapshot_or_spans: FrozenPitSnapshot | pd.DataFrame,
    data_frame: pd.DataFrame,
    *,
    separator: str = "\t",
) -> dict[str, Any]:
    """Write Qlib PIT spans to a new staging file, clipped to observed data."""

    if len(separator) != 1:
        raise ValueError("separator must be one character")
    if data_frame.empty:
        raise PitSpanInvalid("cannot write PIT all.txt from an empty data frame")
    if not isinstance(data_frame.index, pd.MultiIndex) or list(data_frame.index.names) != [
        "datetime",
        "instrument",
    ]:
        raise PitSpanInvalid("PIT all.txt requires MultiIndex[datetime,instrument]")
    spans = (
        snapshot_or_spans.to_frame()
        if isinstance(snapshot_or_spans, FrozenPitSnapshot)
        else canonicalize_pit_spans(snapshot_or_spans)
    )
    observed = (
        data_frame.index.to_frame(index=False)
        .assign(
            datetime=lambda value: pd.to_datetime(value["datetime"]).dt.date,
            instrument=lambda value: value["instrument"].astype(str).str.upper(),
        )
        .groupby("instrument")["datetime"]
        .agg(data_start="min", data_end="max")
    )
    lines: list[str] = []
    skipped = 0
    for span in spans.itertuples(index=False):
        if span.ts_code not in observed.index:
            skipped += 1
            continue
        effective_start = max(span.eligible_start, observed.loc[span.ts_code, "data_start"])
        effective_end = min(span.eligible_end, observed.loc[span.ts_code, "data_end"])
        if effective_start <= effective_end:
            lines.append(separator.join([span.ts_code, effective_start.isoformat(), effective_end.isoformat()]))
    if not lines:
        raise PitSpanInvalid("PIT all.txt would be empty")
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    instruments = {line.split(separator, 1)[0] for line in lines}
    return {
        "path": str(path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "span_lines": len(lines),
        "unique_instruments": len(instruments),
        "multi_span_lines": len(lines) - len(instruments),
        "skipped_no_data": skipped,
        "pit_spans_sha256": pit_spans_sha256(snapshot_or_spans),
    }


def _as_frame(
    spans: pd.DataFrame | Iterable[Mapping[str, Any] | Sequence[Any]],
) -> pd.DataFrame:
    if isinstance(spans, pd.DataFrame):
        return spans.copy()
    values = list(spans)
    if not values:
        return pd.DataFrame(columns=PIT_COLUMNS)
    if isinstance(values[0], Mapping):
        return pd.DataFrame(values)
    return pd.DataFrame(values, columns=PIT_COLUMNS)


def _normalize_code(value: Any) -> str:
    code = str(value or "").strip().upper()
    if not _CODE_PATTERN.fullmatch(code):
        raise PitSpanInvalid(f"PIT span has invalid SH/SZ ts_code: {value!r}")
    return code


def _normalize_reason(value: Any) -> str | None:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    normalized = str(value).strip()
    return normalized or None


def _to_date(value: Any, *, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise PitSpanInvalid(f"invalid {field}: {value!r}") from exc


def _require_sha256(value: str, *, field: str) -> None:
    normalized = str(value or "")
    if normalized != normalized.lower() or not _SHA256_PATTERN.fullmatch(normalized):
        raise PitSnapshotError(f"{field} must be lowercase SHA-256")


def _canonical_json_bytes(payload: Any) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
