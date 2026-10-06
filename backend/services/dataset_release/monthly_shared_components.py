"""Build shared monthly release sidecars from one sealed SOURCE snapshot.

The physical Qlib/H5 writers finish before this boundary.  This builder reads
only their unpublished staging files and the immutable CAS source graph.  It
does not reopen PostgreSQL or a provider, and every output is create-exclusive.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

from .canonical import canonical_json_bytes, digest_named_fields
from .cas_store import CASStore
from .monthly_build_bridge import CompiledMonthlyBuild
from .monthly_candidate_finalizer import SharedReleaseComponents
from .monthly_consumer_layout import publish_consumer_layout
from .monthly_worker import ProducerContext
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile
from .sealed_source_reader import CASSealedPartitionReader
from .sector_enrichment import FrozenSectorEnricher, UNKNOWN_L2_CODE_ID
from .monthly_sector_mapping import (
    build_bound_sector_enricher, frozen_sector_mapping_binding, sector_mapping_catalog_receipt,
)
from .shared_sector_context import (
    MARKET_CONTEXT_SCHEMA,
    MARKET_VOLUME_DEFINITION,
    RELEASE_SW_L2_CODE_MAP_SCHEMA,
    SECTOR_MEMBERSHIP_SPANS_SCHEMA,
    SECTOR_QUOTE_AVAILABILITY_SCHEMA,
    build_release_sw_l2_code_map_payload,
    validate_market_context_frame,
    validate_membership_frame,
    validate_release_sw_l2_code_map,
    validate_sector_quote_availability,
)
from .source_authority import FrozenSourceAuthoritySnapshot, load_source_stage_receipt
from .sw_l2_quote_policy import (
    QUOTE_PUBLICATION_POLICY_SCHEMA,
    build_quote_availability_payload,
    quote_publication_policy_identity,
)
from backend.services.core_index_catalog import P0_POOL_IDS, POOL_DEFINITIONS


BASE_DATASET_MANIFEST_SCHEMA = "aistock_monthly_base_dataset_manifest_v1"
CORE_INDEX_COVERAGE_SCHEMA = "aistock_monthly_six_pool_coverage_v1"
SECTOR_CONTEXT_RECEIPT_SCHEMA = "aistock_sector_context_receipt_v1"
SUSPEND_SCHEMA = "qe_direct_suspend_d_v1"
MEMBERSHIP_DENOMINATOR = "pit_stock_universe_intersection_policy_window_calendar_v1"
_STOCK = re.compile(r"^[0-9]{6}[.](?:SH|SZ)$")
_POOL_IDS = ("stock_universe", *P0_POOL_IDS)


class MonthlySharedComponentsError(RuntimeError):
    """Shared release data could not be derived without fallback."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise MonthlySharedComponentsError(
            f"shared component target already exists: {path}"
        ) from exc
    return path


def _plain_existing_file(root: Path, relative: str, *, label: str) -> Path:
    path = root / Path(relative)
    if path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)()):
        raise MonthlySharedComponentsError(f"{label} is linked")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise MonthlySharedComponentsError(f"{label} is missing") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise MonthlySharedComponentsError(f"{label} escapes candidate")
    return resolved


def _as_date(value: Any, *, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise MonthlySharedComponentsError(f"{field} is not an ISO date") from exc


def _source_rows(
    cas: CASStore,
    frozen: FrozenSourceAuthoritySnapshot,
    dataset: str,
) -> tuple[dict[str, Any], ...]:
    descriptors = [item.as_build_input() for item in frozen.partitions]
    selected = sorted(
        (item for item in descriptors if item.get("dataset") == dataset),
        key=lambda item: str(item.get("partition_key", "")),
    )
    if not selected:
        raise MonthlySharedComponentsError(f"sealed SOURCE omits {dataset}")
    reader = CASSealedPartitionReader(cas, descriptors, max_partition_rows=1_000_000)
    rows: list[dict[str, Any]] = []
    for descriptor in selected:
        stream = reader.iter_rows(dataset, str(descriptor["partition_key"]))
        with stream:
            rows.extend(dict(row) for row in stream)
    return tuple(rows)


def _parse_calendar(path: Path, *, cutoff: date) -> tuple[date, ...]:
    try:
        values = tuple(
            date.fromisoformat(line.strip()[:10])
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise MonthlySharedComponentsError("Qlib day calendar is unreadable") from exc
    if not values or values != tuple(sorted(set(values))) or values[-1] != cutoff:
        raise MonthlySharedComponentsError(
            "Qlib day calendar is empty, duplicated, or does not reach cutoff"
        )
    return values


def _snap(
    calendar: Sequence[date], start: date, end: date
) -> tuple[date, date] | None:
    left = bisect_left(calendar, start)
    right = bisect_right(calendar, end) - 1
    if left >= len(calendar) or right < left:
        return None
    return calendar[left], calendar[right]


def _merge_intervals(
    rows: Iterable[tuple[str, date, date]], calendar: Sequence[date]
) -> tuple[tuple[str, date, date], ...]:
    index = {value: offset for offset, value in enumerate(calendar)}
    grouped: dict[str, list[tuple[date, date]]] = {}
    for symbol, start, end in rows:
        grouped.setdefault(symbol, []).append((start, end))
    output: list[tuple[str, date, date]] = []
    for symbol, values in sorted(grouped.items()):
        active_start, active_end = sorted(values)[0]
        for start, end in sorted(values)[1:]:
            if index[start] <= index[active_end] + 1:
                active_end = max(active_end, end)
            else:
                output.append((symbol, active_start, active_end))
                active_start, active_end = start, end
        output.append((symbol, active_start, active_end))
    return tuple(output)


def _pit_intervals(
    snapshot: FrozenPitSnapshot, calendar: Sequence[date]
) -> tuple[tuple[str, date, date], ...]:
    rows: list[tuple[str, date, date]] = []
    for span in snapshot.spans:
        snapped = _snap(calendar, span.eligible_start, span.eligible_end)
        if snapped is not None:
            rows.append((span.ts_code, *snapped))
    result = _merge_intervals(rows, calendar)
    if not result or any(_STOCK.fullmatch(row[0]) is None for row in result):
        raise MonthlySharedComponentsError("frozen PIT stock universe is invalid")
    return result


def _index_pool_intervals(
    *,
    membership_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[tuple[str, date, date]],
    calendar: Sequence[date],
    cutoff: date,
) -> dict[str, tuple[tuple[str, date, date], ...]]:
    pit_by_symbol: dict[str, list[tuple[date, date]]] = {}
    for symbol, start, end in pit_rows:
        pit_by_symbol.setdefault(symbol, []).append((start, end))
    output: dict[str, list[tuple[str, date, date]]] = {
        pool_id: [] for pool_id in P0_POOL_IDS
    }
    seen_source_rows: set[tuple[str, str, date]] = set()
    for raw in membership_rows:
        pool_id = str(raw.get("pool_id", "")).strip().lower()
        if pool_id not in output:
            raise MonthlySharedComponentsError(
                f"sealed index membership contains an unexpected pool: {pool_id}"
            )
        definition = POOL_DEFINITIONS[pool_id]
        symbol = str(raw.get("ts_code", "")).strip().upper()
        start = _as_date(raw.get("effective_from"), field="effective_from")
        key = (pool_id, symbol, start)
        if key in seen_source_rows:
            raise MonthlySharedComponentsError("sealed index membership is duplicated")
        seen_source_rows.add(key)
        if (
            _STOCK.fullmatch(symbol) is None
            or str(raw.get("index_code", "")).strip().upper()
            != definition.index_code
            or str(raw.get("source_provider", "")).strip().upper()
            != definition.source_provider
            or not str(raw.get("source_reference", "")).strip()
            or raw.get("updated_at") in {None, ""}
        ):
            raise MonthlySharedComponentsError(
                f"sealed index membership identity is invalid: {pool_id}:{symbol}"
            )
        exclusive = raw.get("effective_to_exclusive")
        end = cutoff if exclusive in {None, ""} else _as_date(
            exclusive, field="effective_to_exclusive"
        ) - timedelta(days=1)
        if end < start:
            raise MonthlySharedComponentsError("sealed index membership interval is inverted")
        for pit_start, pit_end in pit_by_symbol.get(symbol, ()):
            snapped = _snap(calendar, max(start, pit_start), min(end, pit_end, cutoff))
            if snapped is not None:
                output[pool_id].append((symbol, *snapped))
    missing = sorted(pool_id for pool_id, rows in output.items() if not rows)
    if missing:
        raise MonthlySharedComponentsError(
            f"sealed index membership pools are empty: {missing}"
        )
    return {
        pool_id: _merge_intervals(rows, calendar)
        for pool_id, rows in sorted(output.items())
    }


def _write_sidecar(path: Path, rows: Sequence[tuple[str, date, date]]) -> Path:
    if not rows or tuple(rows) != tuple(sorted(set(rows))):
        raise MonthlySharedComponentsError("stock-pool sidecar rows are non-canonical")
    payload = "".join(
        f"{symbol}\t{start.isoformat()}\t{end.isoformat()}\n"
        for symbol, start, end in rows
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise MonthlySharedComponentsError(f"stock-pool target exists: {path}") from exc
    return path


def _pin(root: Path, path: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": _sha256(path),
        "size": path.stat().st_size,
    }


def _build_suspend(
    *,
    root: Path,
    rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[tuple[str, date, date]],
    calendar: Sequence[date],
    profile: DatasetProfile,
    cutoff: date,
    prefix=None,
) -> tuple[Path, Path, set[tuple[str, date]], int]:
    import pandas as pd

    month_start = cutoff.replace(day=1)
    effective_calendar = tuple(day for day in calendar if month_start <= day <= cutoff) if prefix is not None else tuple(calendar)
    calendar_set = set(effective_calendar)
    pit_by_symbol: dict[str, list[tuple[date, date]]] = {}
    for symbol, start, end in pit_rows:
        pit_by_symbol.setdefault(symbol, []).append((start, end))
    normalized: list[dict[str, Any]] = []
    keys: set[tuple[str, date]] = set()
    for raw in rows:
        if str(raw.get("suspend_type", "")).strip().upper() != "S":
            continue
        symbol = str(raw.get("ts_code", "")).strip().upper()
        day = _as_date(raw.get("trade_date"), field="suspend trade_date")
        if prefix is not None and not month_start <= day <= cutoff:
            continue
        if day not in calendar_set or not any(
            start <= day <= end for start, end in pit_by_symbol.get(symbol, ())
        ):
            continue
        key = (symbol, day)
        if key in keys:
            raise MonthlySharedComponentsError("suspend_d stock-date is duplicated")
        keys.add(key)
        normalized.append(
            {
                "trade_date": pd.Timestamp(day),
                "ts_code": symbol,
                "suspend_type": "S",
                "suspend_timing": raw.get("suspend_timing"),
            }
        )
    normalized.sort(key=lambda item: (item["trade_date"], item["ts_code"]))
    frame = pd.DataFrame(
        normalized,
        columns=["trade_date", "ts_code", "suspend_type", "suspend_timing"],
    )
    counts = frame.groupby("trade_date").size().to_dict() if not frame.empty else {}
    daily_counts = {day.isoformat(): int(counts.get(pd.Timestamp(day), 0)) for day in effective_calendar}
    lineage = {}
    if prefix is not None:
        if prefix.cutoff != month_start - date.resolution or not effective_calendar:
            raise MonthlySharedComponentsError("suspend prefix is not the preceding month")
        old = pd.read_parquet(io.BytesIO(_read_prefix_sidecar(prefix, "suspend_data")))
        old_meta = json.loads(_read_prefix_sidecar(prefix, "suspend_meta"))
        old_counts = old_meta.get("daily_row_counts")
        if (tuple(old.columns) != tuple(frame.columns) or old_meta.get("end") != prefix.cutoff.isoformat()
            or old_meta.get("row_count") != len(old) or not isinstance(old_counts, Mapping)
            or set(old_counts).intersection(daily_counts)):
            raise MonthlySharedComponentsError("inherited suspend metadata differs")
        frame = pd.concat([old, frame], ignore_index=True)
        daily_counts = {**old_counts, **daily_counts}
        lineage = {
            "validation_scope": "month_delta", "historical_values_validated": 0,
            "predecessor_manifest_sha256": prefix.manifest_sha256,
            "inherited_suspend_sha256": prefix.manifest["components"]["suspend_data"]["sha256"],
        }
    component = root / "components" / "suspend_d_daily_candidate_v2"
    component.mkdir(parents=True, exist_ok=False)
    parquet = component / "suspend_d.parquet"
    frame.to_parquet(parquet, index=False)
    meta = _write_json(
        component / "meta.json",
        {
            "schema_version": SUSPEND_SCHEMA,
            "component": "suspend_d",
            "start": profile.start_date.isoformat(),
            "end": cutoff.isoformat(),
            "universe_key": profile.universe_key,
            "source_table": "market.suspend_d",
            "suspend_type": "S",
            "row_count": len(frame),
            "stock_count": int(frame["ts_code"].nunique()) if not frame.empty else 0,
            "trade_date_count": len(calendar),
            "days_with_suspensions": sum(value > 0 for value in daily_counts.values()),
            "daily_row_counts": daily_counts,
            "source_freeze": True,
            "full_history_content_hash": True,
            **lineage,
        },
    )
    return parquet, meta, keys, len(rows)


def _read_sector_frame(path: Path):
    import numpy as np
    import pandas as pd

    frame = pd.read_hdf(
        path,
        "data",
        columns=["l2_code_id", "sw2_pct_change", "sw2_vol", "sw2_amount"],
    ).reset_index()
    required = {
        "datetime",
        "instrument",
        "l2_code_id",
        "sw2_pct_change",
        "sw2_vol",
        "sw2_amount",
    }
    if set(frame.columns) != required or frame.empty:
        raise MonthlySharedComponentsError("sector_data.h5 schema or rows differ")
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="raise").dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str).str.strip().str.upper()
    frame["l2_code_id"] = pd.to_numeric(frame["l2_code_id"], errors="raise")
    if (
        (~np.isfinite(frame["l2_code_id"])).any()
        or (frame["l2_code_id"] % 1 != 0).any()
    ):
        raise MonthlySharedComponentsError("sector_data contains an invalid l2_code_id")
    frame["l2_code_id"] = frame["l2_code_id"].astype("int32")
    for column in ("sw2_pct_change", "sw2_vol", "sw2_amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _build_sector_membership(
    *,
    enricher: FrozenSectorEnricher,
    pit_rows: Sequence[tuple[str, date, date]],
    calendar: Sequence[date],
    start: date,
    end: date,
    frozen_assignments: Mapping[tuple[str, date], int] | None = None,
):
    import pandas as pd

    target = tuple(value for value in calendar if start <= value <= end)
    if not target:
        raise MonthlySharedComponentsError("sector membership calendar is empty")
    output: list[dict[str, Any]] = []
    resolved_days = 0
    frozen_days = 0
    gap_fill_days = 0
    assignments = dict(frozen_assignments or {})
    for symbol, eligible_start, eligible_end in pit_rows:
        left = bisect_left(target, max(start, eligible_start))
        right = bisect_right(target, min(end, eligible_end))
        dates = target[left:right]
        if not dates:
            continue
        span_start = dates[0]
        prior_day = dates[0]
        active_id: int | None = None
        for day in dates:
            frozen_sector_id = assignments.get((symbol, day))
            if frozen_sector_id is not None and frozen_sector_id >= 0:
                sector_id = int(frozen_sector_id)
                frozen_days += 1
            else:
                sector_id = int(
                    enricher.enrich({"ts_code": symbol, "trade_date": day})[
                        "l2_code_id"
                    ]
                )
                gap_fill_days += 1
            if sector_id == UNKNOWN_L2_CODE_ID:
                raise MonthlySharedComponentsError(
                    "frozen industry PIT cannot resolve an active stock-date: "
                    f"{symbol}:{day.isoformat()}"
                )
            resolved_days += 1
            if active_id is None:
                active_id = sector_id
                span_start = day
            elif sector_id != active_id:
                output.append(
                    {
                        "instrument": symbol,
                        "start_date": span_start,
                        "end_date": prior_day,
                        "l2_code_id": active_id,
                    }
                )
                span_start = day
                active_id = sector_id
            prior_day = day
        output.append(
            {
                "instrument": symbol,
                "start_date": span_start,
                "end_date": prior_day,
                "l2_code_id": active_id,
            }
        )
    if not output:
        raise MonthlySharedComponentsError("sector membership output is empty")
    frame = pd.DataFrame(output).sort_values(
        ["instrument", "start_date", "end_date", "l2_code_id"], kind="stable"
    ).reset_index(drop=True)
    frame["l2_code_id"] = frame["l2_code_id"].astype("int32")
    return frame, resolved_days, frozen_days, gap_fill_days


def _frozen_sector_assignments(frame, *, start: date, end: date) -> dict[tuple[str, date], int]:
    selected = frame.loc[
        (frame["datetime"].dt.date >= start)
        & (frame["datetime"].dt.date <= end)
        & (frame["l2_code_id"] >= 0),
        ["instrument", "datetime", "l2_code_id"],
    ]
    assignments: dict[tuple[str, date], int] = {}
    for row in selected.itertuples(index=False):
        key = (str(row.instrument), row.datetime.date())
        sector_id = int(row.l2_code_id)
        prior = assignments.get(key)
        if prior is not None and prior != sector_id:
            raise MonthlySharedComponentsError(
                f"frozen sector_data assignment conflicts: {key}"
            )
        assignments[key] = sector_id
    if not assignments:
        raise MonthlySharedComponentsError("frozen sector_data assignments are empty")
    return assignments


def _build_market_context(frame):
    import numpy as np

    active = frame.loc[frame["l2_code_id"] >= 0].copy()
    conflicts = active.groupby(["datetime", "l2_code_id"], sort=True)[
        "sw2_vol"
    ].nunique(dropna=False)
    if bool(conflicts.gt(1).any()):
        raise MonthlySharedComponentsError("sector quote volume conflicts within a day")
    unique = active.drop_duplicates(["datetime", "l2_code_id"], keep="first")
    volume = unique["sw2_vol"]
    if np.isinf(volume).any() or volume.lt(0).any():
        raise MonthlySharedComponentsError("sector quote volume is negative or infinite")
    market = (
        unique.assign(sw2_vol=volume.fillna(0.0))
        .groupby("datetime", sort=True, as_index=False)["sw2_vol"]
        .sum()
        .rename(
            columns={"datetime": "trade_date", "sw2_vol": "sw_daily_total_vol"}
        )
    )
    market["trade_date"] = market["trade_date"].dt.date
    return market[["trade_date", "sw_daily_total_vol"]]


def _validate_sector_alignment(
    *,
    frame,
    membership,
    calendar: Sequence[date],
    code_map,
    quote_availability,
    start: date,
    end: date,
) -> dict[str, int]:
    import numpy as np
    import pandas as pd

    by_symbol = {
        symbol: tuple(group.itertuples(index=False))
        for symbol, group in membership.groupby("instrument", sort=False)
    }
    selected = frame.loc[
        (frame["datetime"].dt.date >= start)
        & (frame["datetime"].dt.date <= end)
        & (frame["l2_code_id"] >= 0)
    ]
    mismatches: list[tuple[str, date, int]] = []
    for row in selected.itertuples(index=False):
        day = row.datetime.date()
        matches = [
            span
            for span in by_symbol.get(row.instrument, ())
            if span.start_date <= day <= span.end_date
        ]
        if matches and (
            len(matches) != 1 or int(matches[0].l2_code_id) != int(row.l2_code_id)
        ):
            mismatches.append((row.instrument, day, int(row.l2_code_id)))
            if len(mismatches) == 10:
                break
    if mismatches:
        raise MonthlySharedComponentsError(
            f"sector_data and PIT membership differ: {mismatches}"
        )

    finite = selected.assign(
        finite=np.isfinite(
            selected[["sw2_pct_change", "sw2_vol", "sw2_amount"]].to_numpy()
        ).all(axis=1)
    )
    finite_keys = set(
        finite.loc[finite["finite"], ["datetime", "l2_code_id"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    required: set[tuple[Any, int]] = set()
    target = tuple(value for value in calendar if start <= value <= end)
    for span in membership.itertuples(index=False):
        code = code_map.id_to_code[int(span.l2_code_id)]
        availability = quote_availability.entries[code]
        for day in target:
            if span.start_date <= day <= span.end_date and any(
                left <= day <= right for left, right in availability
            ):
                required.add((pd.Timestamp(day), int(span.l2_code_id)))
    missing = sorted(required - finite_keys)
    if missing:
        raise MonthlySharedComponentsError(
            f"quote-available sector membership lacks finite quotes: {missing[:10]}"
        )
    return {
        "required_sector_date_count": len(required),
        "missing_sector_date_count": 0,
    }


def _base_manifest(
    *,
    root: Path,
    frozen: FrozenSourceAuthoritySnapshot,
    cutoff: date,
    files: Sequence[Path],
    verified_output_files: Mapping[str, Any] | None = None,
) -> tuple[Path, str]:
    def pin(path: Path) -> Mapping[str, Any]:
        relative = path.relative_to(root).as_posix()
        previous = (verified_output_files or {}).get(relative)
        if previous is not None:
            from .monthly_legacy_prefix import _plain_chain, _signature
            _plain_chain(path)
            state = _signature(path)
            if (list(state) != previous.get("signature") or state[2] != previous.get("size")
                or re.fullmatch(r"[0-9a-f]{64}", str(previous.get("sha256"))) is None):
                raise MonthlySharedComponentsError("shared output changed after physical validation")
            return {"sha256": previous["sha256"], "size": previous["size"]}
        return {"sha256": _sha256(path), "size": path.stat().st_size}

    entries = {
        path.relative_to(root).as_posix(): pin(path)
        for path in sorted(set(files), key=lambda item: item.relative_to(root).as_posix())
    }
    body = {
        "schema_version": BASE_DATASET_MANIFEST_SCHEMA,
        "cutoff_trade_date": cutoff.isoformat(),
        "source_content_root": frozen.source_content_root,
        "artifact_ready_content_root": frozen.artifact_ready_content_root,
        "pit_snapshot_digest": frozen.pit_snapshot_digest,
        "files": entries,
    }
    identity = digest_named_fields(BASE_DATASET_MANIFEST_SCHEMA, body)
    path = _write_json(
        root / "provenance" / "base_dataset_manifest.json",
        {**body, "base_dataset_manifest_sha256": identity},
    )
    return path, identity


def _append_sector_month_sidecars(
    *, old_membership, old_market, new_membership, new_market,
    calendar: Sequence[date], predecessor_cutoff: date, cutoff: date,
):
    """Inherit frozen metadata; merge only causal, trading-adjacent tail spans.

    No old sector quote values are read or re-evaluated. Historical sidecars
    must be authenticated by their caller before entering this constructor.
    """
    import pandas as pd

    start = cutoff.replace(day=1)
    if predecessor_cutoff != start - timedelta(days=1):
        raise MonthlySharedComponentsError("sector predecessor is not the preceding month")
    required = tuple(day for day in calendar if start <= day <= cutoff)
    if not required or tuple(new_market["trade_date"]) != required:
        raise MonthlySharedComponentsError("sector market context differs from month calendar")
    if not old_market.empty and max(old_market["trade_date"]) > predecessor_cutoff:
        raise MonthlySharedComponentsError("inherited market context crosses month boundary")
    if (not old_membership.empty and max(old_membership["end_date"]) > predecessor_cutoff
        or not new_membership.empty and (
            min(new_membership["start_date"]) < start or max(new_membership["end_date"]) > cutoff
        )):
        raise MonthlySharedComponentsError("sector membership crosses month boundary")
    records = old_membership.to_dict("records")
    last = {row["instrument"]: ordinal for ordinal, row in enumerate(records)}
    day_order = {day: ordinal for ordinal, day in enumerate(calendar)}
    for row in new_membership.to_dict("records"):
        ordinal = last.get(row["instrument"])
        previous = records[ordinal] if ordinal is not None else None
        if previous is not None and row["start_date"] <= previous["end_date"]:
            raise MonthlySharedComponentsError("sector membership tail overlaps inherited assignment")
        if (previous is not None and previous["l2_code_id"] == row["l2_code_id"]
            and previous["end_date"] in day_order and row["start_date"] in day_order
            and day_order[row["start_date"]] == day_order[previous["end_date"]] + 1):
            previous["end_date"] = row["end_date"]
        else:
            last[row["instrument"]] = len(records)
            records.append(row)
    membership = pd.DataFrame(records, columns=old_membership.columns)
    membership = membership.sort_values(["instrument", "start_date", "end_date", "l2_code_id"]).reset_index(drop=True)
    membership["l2_code_id"] = membership["l2_code_id"].astype("int32")
    return membership, pd.concat([old_market, new_market], ignore_index=True)


def _read_prefix_sidecar(prefix, key: str) -> bytes:
    """Read a small declared metadata file, not an inherited data component."""
    from .monthly_legacy_prefix import _plain_chain, _relative, _signature
    pin = prefix.manifest["components"].get(key)
    if (not isinstance(pin, Mapping) or type(pin.get("size")) is not int
        or not 0 < pin["size"] <= 16 * 1024 * 1024):
        raise MonthlySharedComponentsError("inherited shared sidecar pin is missing or invalid")
    path = prefix.root / _relative(pin.get("path"))
    _plain_chain(path)
    before = _signature(path)
    if before[2] != pin["size"]:
        raise MonthlySharedComponentsError("inherited shared sidecar size differs")
    raw = path.read_bytes()
    if _signature(path) != before or hashlib.sha256(raw).hexdigest() != pin.get("sha256"):
        raise MonthlySharedComponentsError("inherited shared sidecar bytes differ")
    return raw


def _native_sector_summary(
    *, prefix, sector_h5: Path, physical_authority: Mapping[str, Any], profile,
    enricher, code_map, quote, calendar, pit_rows, cutoff: date, checkpoint=lambda: None,
):
    """Use the same formal sector validator, restricted to the actual new tail."""
    import pandas as pd
    from .monthly_preparation_shared import summarize_sector_context
    from .monthly_legacy_prefix import _plain_chain, _signature

    old_code_map = validate_release_sw_l2_code_map(json.loads(_read_prefix_sidecar(prefix, "sector_code_map")))
    if old_code_map.id_to_code != code_map.id_to_code:
        raise MonthlySharedComponentsError("sector month code mapping differs from inherited identity")
    old_member = pd.read_parquet(io.BytesIO(_read_prefix_sidecar(prefix, "sector_membership_spans")))
    old_market = pd.read_parquet(io.BytesIO(_read_prefix_sidecar(prefix, "market_context")))
    for name in ("start_date", "end_date"):
        old_member[name] = pd.to_datetime(old_member[name], errors="raise").dt.date
    old_market["trade_date"] = pd.to_datetime(old_market["trade_date"], errors="raise").dt.date
    pin = (physical_authority.get("verified_output_files") or {}).get("factor_bundle/sector_data.h5")
    rows = (physical_authority.get("physical_month_ranges") or {}).get("sector_data")
    _plain_chain(sector_h5)
    before = _signature(sector_h5)
    if (not isinstance(pin, Mapping) or not isinstance(rows, Mapping)
        or pin.get("signature") != list(before) or pin.get("size") != before[2]):
        raise MonthlySharedComponentsError("sector month physical writer authority is incomplete or stale")
    inherited_count = (prefix.manifest["components"].get("sector_data") or {}).get("row_count")
    if inherited_count is not None and (
        type(inherited_count) is not int or inherited_count != rows.get("start_row")
    ):
        raise MonthlySharedComponentsError("sector physical tail differs from inherited row count")
    month_start = cutoff.replace(day=1)
    month_days = tuple(day for day in calendar if month_start <= day <= cutoff)
    if not month_days:
        raise MonthlySharedComponentsError("sector month calendar is empty")
    member, market, counts = summarize_sector_context(
        sector_h5=sector_h5, profile=profile, enricher=enricher, code_map=code_map,
        quote=quote, calendar=calendar, pit_rows=pit_rows, start=month_days[0], cutoff=cutoff,
        bound=profile.resource_policy.validation_read_chunk_rows, checkpoint=checkpoint,
        market_start=month_start, source_start_row=rows.get("start_row"), source_month_rows=rows.get("row_count"),
    )
    if _signature(sector_h5) != before:
        raise MonthlySharedComponentsError("sector month data changed during validation")
    member, market = _append_sector_month_sidecars(
        old_membership=old_member, old_market=old_market, new_membership=member, new_market=market,
        calendar=calendar, predecessor_cutoff=prefix.cutoff, cutoff=cutoff,
    )
    return member, market, {**counts, "validation_scope": "month_delta",
        "historical_sector_values_read": 0, "predecessor_manifest_sha256": prefix.manifest_sha256,
        "inherited_membership_sha256": prefix.manifest["components"]["sector_membership_spans"]["sha256"],
        "inherited_market_context_sha256": prefix.manifest["components"]["market_context"]["sha256"]}


@dataclass(frozen=True, slots=True)
class FrozenMonthlySharedComponentBuilder:
    """Concrete shared-component producer for the unified monthly BUILD."""

    profile: DatasetProfile
    cas: CASStore
    sector_membership_start: date

    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        compiled: CompiledMonthlyBuild,
        validation_result: Mapping[str, Any],
    ) -> SharedReleaseComponents:
        if context.stage != "BUILD":
            raise MonthlySharedComponentsError("shared builder received another stage")
        try:
            cutoff = date.fromisoformat(str(context.plan.get("target_cutoff") or ""))
        except ValueError as exc:
            raise MonthlySharedComponentsError("shared builder cutoff is invalid") from exc
        if not self.profile.start_date <= self.sector_membership_start <= cutoff:
            raise MonthlySharedComponentsError("shared builder window differs from profile")
        root = staging_root.resolve(strict=True)
        validation_ref = validation_result.get("validation_ref")
        component_manifest_ref = validation_result.get(
            "component_artifact_manifest_ref"
        )
        if not isinstance(validation_ref, Mapping) or not isinstance(
            component_manifest_ref, Mapping
        ):
            raise MonthlySharedComponentsError(
                "physical validation evidence is incomplete"
            )
        frozen = load_source_stage_receipt(
            self.cas,
            compiled.source_stage_receipt_ref,
            expected_profile=self.profile.profile,
            expected_cutoff=cutoff,
            profile=self.profile,
        )
        if frozen.artifact_ready_contract_ref is None:
            raise MonthlySharedComponentsError("SOURCE lacks artifact-ready authority")
        from .monthly_preparation_shared import recover_prepared_shared_components
        from .monthly_preparation_executor import adopt_pinned_shared_file

        native = validation_result.get("validation_scope") == "month_delta"
        prefix = None
        physical_authority = {}
        if native:
            from .monthly_build_bridge import load_monthly_predecessor_prefix
            prefix_plan = compiled.physical_plan.get("build_inputs", {}).get("monthly_legacy_predecessor")
            if not isinstance(prefix_plan, Mapping):
                raise MonthlySharedComponentsError("native shared builder lacks exact predecessor binding")
            prefix = load_monthly_predecessor_prefix(context_plan=prefix_plan, profile=self.profile)
            physical_authority = self.cas.get_json_bounded(component_manifest_ref, max_bytes=16 * 1024 * 1024)
            if (physical_authority.get("schema_version") != "aistock_monthly_native_component_authority_v1"
                or physical_authority.get("validation_scope") != "month_delta"
                or physical_authority.get("cutoff") != cutoff.isoformat()
                or physical_authority.get("release_id") != context.plan.get("release_id")
                or physical_authority.get("source_bundle_sha256") != compiled.source_bundle_sha256
                or physical_authority.get("predecessor_manifest_sha256") != prefix.manifest_sha256
                or physical_authority.get("verified_output_files") != validation_result.get("verified_output_files")):
                raise MonthlySharedComponentsError("native shared physical authority identity differs")
        # A full-history prepared sector frame is not month-delta QA. Native
        # BUILD derives the tail from the real append writer's physical offset.
        prepared = {} if native else recover_prepared_shared_components(
            context=context, snapshot=frozen, profile=self.profile,
            sector_membership_start=self.sector_membership_start, staging_root=root,
        )

        def adopt(domain: str, relative_path: str, target: Path) -> Path:
            return adopt_pinned_shared_file(
                root=Path(self.profile.candidate_root), verified=prepared[domain],
                relative_path=relative_path, destination=target,
            )

        def sidecar(domain: str, target: Path, rows: Sequence[tuple[str, date, date]]) -> Path:
            if domain not in prepared:
                return _write_sidecar(target, rows)
            expected = "".join(f"{symbol}\t{left.isoformat()}\t{right.isoformat()}\n" for symbol, left, right in rows).encode("utf-8")
            path = adopt(domain, target.name, target)
            if path.read_bytes() != expected:
                raise MonthlySharedComponentsError("prepared sidecar differs from complete SOURCE")
            return path
        calendar_path = _plain_existing_file(
            root, "daily_bin/qlib/calendars/day.txt", label="Qlib day calendar"
        )
        instruments_path = _plain_existing_file(
            root,
            "daily_bin/qlib/instruments/all.txt",
            label="Qlib provider catalog",
        )
        sector_h5 = _plain_existing_file(
            root, "factor_bundle/sector_data.h5", label="sector_data.h5"
        )
        calendar = _parse_calendar(calendar_path, cutoff=cutoff)

        pit_rows = _pit_intervals(frozen.pit_snapshot, calendar)
        membership_source = _source_rows(self.cas, frozen, "index_membership_pit")
        index_pools = _index_pool_intervals(
            membership_rows=membership_source,
            pit_rows=pit_rows,
            calendar=calendar,
            cutoff=cutoff,
        )
        pool_root = root / "stock_pools"
        pool_paths = {
            "stock_universe": sidecar("stock_pools",
                pool_root / "stock_universe.txt", pit_rows
            )
        }
        for pool_id, rows in index_pools.items():
            pool_paths[pool_id] = sidecar("stock_pools",
                pool_root / f"index_pool__{pool_id}.txt", rows
            )
        if set(pool_paths) != set(_POOL_IDS):
            raise MonthlySharedComponentsError("six-pool output set differs")
        benchmark_path = sidecar("benchmark",
            pool_root / "benchmark.txt",
            (("000300.SH", calendar[0], cutoff),),
        )

        suspend_rows = _source_rows(self.cas, frozen, "suspend_d")
        if "suspend" in prepared:
            component = "components/suspend_d_daily_candidate_v2"
            suspend_parquet = adopt("suspend", f"{component}/suspend_d.parquet", root / component / "suspend_d.parquet")
            suspend_meta = adopt("suspend", f"{component}/meta.json", root / component / "meta.json")
            suspend_source_rows = len(suspend_rows)
        else:
            suspend_parquet, suspend_meta, _suspended_keys, suspend_source_rows = _build_suspend(
                root=root,
                rows=suspend_rows,
                pit_rows=pit_rows,
                calendar=calendar,
                profile=self.profile,
                cutoff=cutoff,
                prefix=prefix,
            )
        pool_coverage = {
            pool_id: {
                "symbol_count": len({row[0] for row in rows}),
                "span_count": len(rows),
                "day_gap_count": 0,
                "minute_gap_count": 0,
                "subset_of_frozen_pit": True,
                "sidecar_sha256": _sha256(pool_paths[pool_id]),
            }
            for pool_id, rows in {
                "stock_universe": pit_rows,
                **index_pools,
            }.items()
        }
        coverage_path = _write_json(
            root / "reports" / "index_pool_coverage.json",
            {
                "schema_version": CORE_INDEX_COVERAGE_SCHEMA,
                "cutoff_trade_date": cutoff.isoformat(),
                "source_content_root": frozen.source_content_root,
                "artifact_ready_content_root": frozen.artifact_ready_content_root,
                "pit_snapshot_digest": frozen.pit_snapshot_digest,
                "physical_validation_ref": dict(validation_ref),
                "component_artifact_manifest_ref": dict(component_manifest_ref),
                "coverage_basis": "six_pools_subset_of_validated_frozen_pit_v1",
                "pools": pool_coverage,
                "unexplained_gap_count": 0,
            },
        )

        classify = _source_rows(self.cas, frozen, "sw_index_classify")
        members = _source_rows(self.cas, frozen, "sw_index_member")
        mapping_binding = frozen_sector_mapping_binding(self.cas, frozen)
        enricher = build_bound_sector_enricher(classify, members, binding=mapping_binding)
        mapping_authority_sha = digest_named_fields(
            "aistock_monthly_sw_l2_mapping_authority_v1",
            {
                "source_content_root": frozen.source_content_root,
                "code_map_digest": enricher.code_map_digest,
                "membership_digest": enricher.membership_digest,
            },
        )
        code_map_payload = build_release_sw_l2_code_map_payload(
            code_to_id=dict(enricher.code_map),
            member_backed_codes=tuple(enricher.code_map),
            authority_id=f"monthly-source:{mapping_authority_sha[:16]}",
            authority_sha256=mapping_authority_sha,
        )
        code_map = validate_release_sw_l2_code_map(code_map_payload)
        quote_payload = build_quote_availability_payload(
            code_map=code_map,
            required_start=self.sector_membership_start,
            cutoff=cutoff,
        )
        quote = validate_sector_quote_availability(
            quote_payload, code_map=code_map, required_end=cutoff
        )
        from .monthly_preparation_shared import summarize_sector_context
        from .monthly_component_preparation import ComponentPreparationError
        try:
            if native:
                sector_membership, market, counts = _native_sector_summary(
                    prefix=prefix, sector_h5=sector_h5, physical_authority=physical_authority,
                    profile=self.profile, enricher=enricher, code_map=code_map, quote=quote,
                    calendar=calendar, pit_rows=pit_rows, cutoff=cutoff,
                )
            else:
                sector_membership, market, counts = summarize_sector_context(
                    sector_h5=sector_h5, profile=self.profile, enricher=enricher,
                    code_map=code_map, quote=quote, calendar=calendar, pit_rows=pit_rows,
                    start=self.sector_membership_start, cutoff=cutoff,
                    bound=self.profile.resource_policy.validation_read_chunk_rows,
                    checkpoint=lambda: None,
                )
        except ComponentPreparationError as exc:
            raise MonthlySharedComponentsError(str(exc)) from exc
        frozen_days = counts["frozen_stock_trading_day_count"]
        gap_fill_days = counts["member_gap_fill_stock_trading_day_count"]
        resolved_days = frozen_days + gap_fill_days
        if "sector_context" in prepared:
            import pandas as pd
            proof = prepared["sector_context"]
            prior_root = Path(self.profile.candidate_root) / proof.record["component_root"]
            if (
                not sector_membership.equals(pd.read_parquet(prior_root / "sector_membership_spans.parquet"))
                or not market.equals(pd.read_parquet(prior_root / "market_context.parquet"))
            ):
                raise MonthlySharedComponentsError("prepared sector sidecars differ from complete frozen source")
        if native:
            # Historical sidecars are pinned metadata, not historical quote
            # observations to audit again. The formal validator checked delta.
            membership_start = min(sector_membership["start_date"])
            membership_end = max(sector_membership["end_date"])
            membership_count = len(sector_membership)
            symbol_count = int(sector_membership["instrument"].nunique())
            market_start, market_end, market_count = min(market["trade_date"]), max(market["trade_date"]), len(market)
        else:
            membership_start, membership_end, membership_count, symbol_count = validate_membership_frame(
                sector_membership,
                id_to_code=code_map.id_to_code,
                required_start=self.sector_membership_start,
                required_end=cutoff,
            )
            market_start, market_end, market_count = validate_market_context_frame(
                market, required_start=self.sector_membership_start, required_end=cutoff,
            )
        quote_coverage = {
            "required_sector_date_count": counts["quote_required_sector_date_count"],
            "missing_sector_date_count": counts["quote_gap_count"],
        }

        base_manifest_path, base_manifest_sha = _base_manifest(
            root=root,
            frozen=frozen,
            cutoff=cutoff,
            files=(
                calendar_path,
                instruments_path,
                sector_h5,
                *pool_paths.values(),
                benchmark_path,
                suspend_parquet,
                suspend_meta,
                coverage_path,
            ),
            verified_output_files=physical_authority.get("verified_output_files") if native else None,
        )
        base_manifest = json.loads(base_manifest_path.read_bytes())
        sector_sha = base_manifest["files"][sector_h5.relative_to(root).as_posix()]["sha256"]
        sector_root = root / "components" / "sector_context_candidate_v1"
        sector_root.mkdir(parents=True, exist_ok=False)
        code_map_path = _write_json(sector_root / "sector_code_map.json", code_map_payload)
        quote_path = _write_json(
            sector_root / "sector_quote_availability.json", quote_payload
        )
        market_path = sector_root / "market_context.parquet"
        membership_path = sector_root / "sector_membership_spans.parquet"
        if "sector_context" in prepared:
            adopt("sector_context", market_path.name, market_path)
            adopt("sector_context", membership_path.name, membership_path)
        else:
            market.to_parquet(market_path, index=False)
            sector_membership.to_parquet(membership_path, index=False)
        receipt = {
            "schema_version": SECTOR_CONTEXT_RECEIPT_SCHEMA,
            "source_dataset_manifest_sha256": base_manifest_sha,
            "sector_data": {
                "path": sector_h5.relative_to(root).as_posix(),
                "sha256": sector_sha,
                "byte_size": sector_h5.stat().st_size,
                "used_l2_code_id_count": counts["used_l2_code_id_count"],
                "used_l2_code_id_count_scope": "month_delta" if native else "release",
            },
            "sector_code_map": {
                "schema_version": RELEASE_SW_L2_CODE_MAP_SCHEMA,
                "path": code_map_path.name,
                "sha256": _sha256(code_map_path),
                "byte_size": code_map_path.stat().st_size,
                "code_map_digest": code_map.code_map_digest,
                "member_backed_digest": code_map.member_backed_digest,
                "member_backed_count": len(code_map.member_backed_codes),
                "authority": dict(code_map.mapping_authority),
                "source_enrichment_code_map_digest": enricher.code_map_digest,
            },
            "market_context": {
                "schema_version": MARKET_CONTEXT_SCHEMA,
                "path": market_path.name,
                "sha256": _sha256(market_path),
                "byte_size": market_path.stat().st_size,
                "definition": MARKET_VOLUME_DEFINITION,
                "start": market_start.isoformat(),
                "end": market_end.isoformat(),
                "row_count": market_count,
            },
            "quote_availability": {
                "schema_version": SECTOR_QUOTE_AVAILABILITY_SCHEMA,
                "path": quote_path.name,
                "sha256": _sha256(quote_path),
                "byte_size": quote_path.stat().st_size,
                "canonical_digest": quote.quote_availability_digest,
                "catalog_count": len(quote.entries),
                "mapping_authority": dict(quote.mapping_authority),
                "publication_policy_schema": QUOTE_PUBLICATION_POLICY_SCHEMA,
                "publication_policy_identity": quote_publication_policy_identity(),
            },
            "membership": {
                "schema_version": SECTOR_MEMBERSHIP_SPANS_SCHEMA,
                "path": membership_path.name,
                "sha256": _sha256(membership_path),
                "byte_size": membership_path.stat().st_size,
                "start": membership_start.isoformat(),
                "end": membership_end.isoformat(),
                "span_count": membership_count,
                "symbol_count": symbol_count,
                "resolved_stock_trading_day_count": resolved_days,
                "denominator": MEMBERSHIP_DENOMINATOR,
                "source_membership_digest": enricher.membership_digest,
                "assignment_policy": "frozen_dated_sector_assignment_then_sw_member_gap_fill_v1",
                "frozen_sector_trading_day_count": frozen_days,
                "sw_member_gap_fill_trading_day_count": gap_fill_days,
                "quote_coverage": quote_coverage,
                "current_snapshot_backfill": False,
                "default_industry_assignment": False,
                "silent_symbol_exclusion": False,
            },
            "database_read": False,
            "database_write": False,
            "runtime_action": False,
            **({"monthly_catalog_lineage": sector_mapping_catalog_receipt(
                classify, members, binding=mapping_binding,
            )} if mapping_binding is not None else {}),
            **({"validation_scope": "month_delta", "month_validation": counts,
                "historical_business_audit_performed": False} if native else {}),
        }
        receipt_path = _write_json(sector_root / "component_receipt.json", receipt)

        st_pit = {
            "schema_version": "qe_st_pit_manifest_v1",
            "snapshot_id": (
                f"{frozen.pit_snapshot.universe_key}_{frozen.pit_snapshot_digest[:16]}"
            ),
            "cutoff_trade_date": cutoff.isoformat(),
            "universe_key": frozen.pit_snapshot.universe_key,
            "rule_version": frozen.pit_snapshot.rule_version,
            "selection_universe": _pin(root, pool_paths["stock_universe"]),
            "index_membership_sidecars": {
                pool_id: _pin(root, pool_paths[pool_id])
                for pool_id in sorted(pool_paths)
            },
        }
        st_pit_path = _write_json(pool_root / "st_pit_manifest.json", st_pit)
        consumer_layout = publish_consumer_layout(
            root=root,
            profile=self.profile,
            cutoff=cutoff,
            release_id=str(context.plan.get("release_id") or ""),
            st_pit_manifest=st_pit,
            validation_authority={
                "validation_ref": dict(validation_ref),
                "component_artifact_manifest_ref": dict(component_manifest_ref),
            },
            verified_output_files=physical_authority.get("verified_output_files") if native else None,
        )
        required = (
            *tuple(pool_paths.values()),
            benchmark_path,
            suspend_parquet,
            suspend_meta,
            coverage_path,
            base_manifest_path,
            code_map_path,
            quote_path,
            market_path,
            membership_path,
            receipt_path,
            st_pit_path,
            *consumer_layout.required_files,
        )
        source_rows_read = (
            len(membership_source)
            + suspend_source_rows
            + len(classify)
            + len(members)
        )
        return SharedReleaseComponents(
            required_files=tuple(required),
            qlib_calendar_path=consumer_layout.day_calendar_path,
            qlib_instruments_path=consumer_layout.day_instruments_path,
            st_pit_manifest=st_pit,
            source_contract={
                "schema_version": "aistock_monthly_sealed_source_contract_v1",
                "source_content_root": frozen.source_content_root,
                "artifact_ready_content_root": frozen.artifact_ready_content_root,
                "pit_snapshot_digest": frozen.pit_snapshot_digest,
                "base_dataset_manifest_sha256": base_manifest_sha,
                "database_fallback": False,
                "provider_fallback_after_source_seal": False,
                "no_fabrication": True,
            },
            source_rows_read=source_rows_read,
            computed_rows=(
                len(pit_rows)
                + sum(len(value) for value in index_pools.values())
                + len(sector_membership)
                + len(market)
            ),
            verified_output_files=consumer_layout.verified_output_files,
        )


__all__: Sequence[str] = (
    "BASE_DATASET_MANIFEST_SCHEMA",
    "CORE_INDEX_COVERAGE_SCHEMA",
    "FrozenMonthlySharedComponentBuilder",
    "MonthlySharedComponentsError",
)
