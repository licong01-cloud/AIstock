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
import os
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

from .canonical import canonical_json_bytes, digest_named_fields
from .cas_store import CASStore
from .monthly_build_bridge import CompiledMonthlyBuild
from .monthly_candidate_finalizer import SharedReleaseComponents
from .monthly_worker import ProducerContext
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile
from .sealed_source_reader import CASSealedPartitionReader
from .sector_enrichment import FrozenSectorEnricher, UNKNOWN_L2_CODE_ID
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
) -> tuple[Path, Path, set[tuple[str, date]], int]:
    import pandas as pd

    calendar_set = set(calendar)
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
    component = root / "components" / "suspend_d_daily_candidate_v2"
    component.mkdir(parents=True, exist_ok=False)
    parquet = component / "suspend_d.parquet"
    frame.to_parquet(parquet, index=False)
    counts = frame.groupby("trade_date").size().to_dict() if not frame.empty else {}
    daily_counts = {
        day.isoformat(): int(counts.get(pd.Timestamp(day), 0)) for day in calendar
    }
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
) -> tuple[Path, str]:
    entries = {
        path.relative_to(root).as_posix(): {
            "sha256": _sha256(path),
            "size": path.stat().st_size,
        }
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
            "stock_universe": _write_sidecar(
                pool_root / "stock_universe.txt", pit_rows
            )
        }
        for pool_id, rows in index_pools.items():
            pool_paths[pool_id] = _write_sidecar(
                pool_root / f"index_pool__{pool_id}.txt", rows
            )
        if set(pool_paths) != set(_POOL_IDS):
            raise MonthlySharedComponentsError("six-pool output set differs")
        benchmark_path = _write_sidecar(
            pool_root / "benchmark.txt",
            (("000300.SH", calendar[0], cutoff),),
        )

        suspend_rows = _source_rows(self.cas, frozen, "suspend_d")
        suspend_parquet, suspend_meta, _suspended_keys, suspend_source_rows = (
            _build_suspend(
                root=root,
                rows=suspend_rows,
                pit_rows=pit_rows,
                calendar=calendar,
                profile=self.profile,
                cutoff=cutoff,
            )
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
        enricher = FrozenSectorEnricher.build(classify, members)
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
        sector_frame = _read_sector_frame(sector_h5)
        unknown_ids = sorted(
            set(sector_frame.loc[sector_frame["l2_code_id"] >= 0, "l2_code_id"])
            - set(code_map.id_to_code)
        )
        if unknown_ids:
            raise MonthlySharedComponentsError(
                f"sector_data contains unknown l2_code_id values: {unknown_ids[:10]}"
            )
        frozen_assignments = _frozen_sector_assignments(
            sector_frame,
            start=self.sector_membership_start,
            end=cutoff,
        )
        sector_membership, resolved_days, frozen_days, gap_fill_days = _build_sector_membership(
            enricher=enricher,
            pit_rows=pit_rows,
            calendar=calendar,
            start=self.sector_membership_start,
            end=cutoff,
            frozen_assignments=frozen_assignments,
        )
        membership_start, membership_end, membership_count, symbol_count = (
            validate_membership_frame(
                sector_membership,
                id_to_code=code_map.id_to_code,
                required_start=self.sector_membership_start,
                required_end=cutoff,
            )
        )
        market = _build_market_context(sector_frame)
        market_start, market_end, market_count = validate_market_context_frame(
            market,
            required_start=self.sector_membership_start,
            required_end=cutoff,
        )
        quote_coverage = _validate_sector_alignment(
            frame=sector_frame,
            membership=sector_membership,
            calendar=calendar,
            code_map=code_map,
            quote_availability=quote,
            start=self.sector_membership_start,
            end=cutoff,
        )

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
        )
        sector_root = root / "components" / "sector_context_candidate_v1"
        sector_root.mkdir(parents=True, exist_ok=False)
        code_map_path = _write_json(sector_root / "sector_code_map.json", code_map_payload)
        quote_path = _write_json(
            sector_root / "sector_quote_availability.json", quote_payload
        )
        market_path = sector_root / "market_context.parquet"
        membership_path = sector_root / "sector_membership_spans.parquet"
        market.to_parquet(market_path, index=False)
        sector_membership.to_parquet(membership_path, index=False)
        receipt = {
            "schema_version": SECTOR_CONTEXT_RECEIPT_SCHEMA,
            "source_dataset_manifest_sha256": base_manifest_sha,
            "sector_data": {
                "path": sector_h5.relative_to(root).as_posix(),
                "sha256": _sha256(sector_h5),
                "byte_size": sector_h5.stat().st_size,
                "used_l2_code_id_count": len(
                    set(sector_frame.loc[sector_frame["l2_code_id"] >= 0, "l2_code_id"])
                ),
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
        required = (
            calendar_path,
            instruments_path,
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
        )
        source_rows_read = (
            len(membership_source)
            + suspend_source_rows
            + len(classify)
            + len(members)
        )
        return SharedReleaseComponents(
            required_files=tuple(required),
            qlib_calendar_path=calendar_path,
            qlib_instruments_path=instruments_path,
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
        )


__all__: Sequence[str] = (
    "BASE_DATASET_MANIFEST_SCHEMA",
    "CORE_INDEX_COVERAGE_SCHEMA",
    "FrozenMonthlySharedComponentBuilder",
    "MonthlySharedComponentsError",
)
