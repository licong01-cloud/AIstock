"""Build-only readers over a validated artifact-ready source CAS graph."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from .artifact_ready_source import (
    ARTIFACT_READY_ADJ_COVERAGE_SCHEMA,
    ARTIFACT_READY_COMPONENT_SCHEMA,
    ARTIFACT_READY_DAILY_COVERAGE_SCHEMA,
    ARTIFACT_READY_INDEX_CHUNK_SCHEMA,
    ARTIFACT_READY_MINUTE_COVERAGE_SCHEMA,
    load_artifact_ready_contract,
)
from .cas_store import CASRef, CASStore
from .contracts import Component
from .errors import DatasetReleaseError
from .external_ordered_rows import OrderedMappingPartition
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile
from .sealed_source_reader import CASSealedPartitionReader


class ArtifactReadyBuildSourceError(DatasetReleaseError):
    code = "BLOCKED_ARTIFACT_READY_BUILD_SOURCE_INVALID"


_MINUTE_PARTITION = re.compile(
    r"^(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})_"
    r"bucket-(?P<bucket>\d{4})(?:-[0-9a-f]{16})?$"
)
_DATE_PARTITION = re.compile(r"^(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})$")
_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "candidate_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class ArtifactReadyBuildSource:
    """Validate and stream one immutable artifact-ready build input graph."""

    def __init__(
        self,
        *,
        cas: CASStore,
        profile: DatasetProfile,
        cutoff: date,
        pit_snapshot: FrozenPitSnapshot,
        source_content_root: str,
        source_partitions: Sequence[Mapping[str, Any]],
        artifact_ready_contract_ref: CASRef | Mapping[str, Any] | str,
    ) -> None:
        self.cas = cas
        self.profile = profile
        self.cutoff = cutoff
        self.pit_snapshot = pit_snapshot
        self.source_content_root = source_content_root
        if cutoff != pit_snapshot.cutoff:
            raise ArtifactReadyBuildSourceError("artifact-ready build cutoff differs from PIT")
        self._descriptors = {
            f"{item.get('dataset')}:{item.get('partition_key')}": dict(item) for item in source_partitions
        }
        if not self._descriptors or len(self._descriptors) != len(source_partitions):
            raise ArtifactReadyBuildSourceError("artifact-ready raw source descriptors are empty or duplicated")
        self._reader = CASSealedPartitionReader(
            cas,
            tuple(self._descriptors.values()),
            max_partition_rows=1_000_000,
        )
        loaded = load_artifact_ready_contract(
            cas,
            profile,
            artifact_ready_contract_ref,
            expected_source_content_root=source_content_root,
            expected_pit_snapshot_digest=pit_snapshot.spans_sha256,
        )
        self.contract_ref = loaded.reference
        self.contract = dict(loaded.payload)
        self.component_manifests: dict[Component, Mapping[str, Any]] = {}
        for component in Component:
            reference = _complete_ref(cas, self.contract["component_manifests"][component.value])
            value = cas.get_json_bounded(reference, max_bytes=32 * 1024 * 1024)
            if not isinstance(value, Mapping):
                raise ArtifactReadyBuildSourceError("artifact-ready component manifest is invalid")
            self._validate_component_manifest(component, value)
            self.component_manifests[component] = dict(value)
        self.qfq_authority = loaded.qfq_denominator_authority

    @property
    def artifact_ready_content_root(self) -> str:
        return str(self.contract["artifact_ready_content_root"])

    @property
    def qfq_source_summary(self) -> Mapping[str, Any]:
        value = self.contract.get("qfq_source_summary")
        if not isinstance(value, Mapping):
            raise ArtifactReadyBuildSourceError("QFQ source summary is missing")
        return dict(value)

    @property
    def factor_overlay_summary(self) -> Mapping[str, Any]:
        details = self.component_manifests[Component.FACTOR_H5_STATIC].get("details")
        value = details.get("overlay_summary") if isinstance(details, Mapping) else None
        if not isinstance(value, Mapping):
            raise ArtifactReadyBuildSourceError("factor overlay summary/receipt is missing")
        return dict(value)

    @property
    def minute_overlay_summary(self) -> Mapping[str, Any]:
        """Aggregate immutable day receipts into validator-facing provenance."""

        manifest = self.component_manifests[Component.MINUTE_BIN]
        database_rows = 0
        overlay_rows = 0
        synthesized = 0
        tdx_rows = 0
        tushare_rows = 0
        overlap_rows = 0
        coverage_entries = 0
        for entry in manifest["partitions"]:
            if not isinstance(entry, Mapping) or entry.get("dataset") != "minute_coverage":
                continue
            receipt = self._derived_receipt(entry, ARTIFACT_READY_MINUTE_COVERAGE_SCHEMA)
            days = receipt.get("days")
            if not isinstance(days, list):
                raise ArtifactReadyBuildSourceError("minute coverage days are invalid")
            for item in days:
                if not isinstance(item, Mapping):
                    raise ArtifactReadyBuildSourceError("minute coverage day is invalid")
                status = str(item.get("status", ""))
                coverage_entries += 1
                if status == "SUSPENDED_FULL_DAY":
                    synthesized += 240
                    continue
                database_count = int(item.get("database_rows", -1))
                filled = int(item.get("overlay_rows", -1))
                verified = int(item.get("overlap_rows_verified", -1))
                if min(database_count, filled, verified) < 0:
                    raise ArtifactReadyBuildSourceError("minute coverage counts are invalid")
                database_rows += database_count
                overlay_rows += filled
                overlap_rows += verified
                provider = str(item.get("provider", ""))
                if status == "PROVIDER_FILLED":
                    if provider == "tdx":
                        tdx_rows += filled
                    elif provider == "tushare":
                        tushare_rows += filled
                    else:
                        raise ArtifactReadyBuildSourceError("minute coverage provider is invalid")
                elif status != "DATABASE_COMPLETE":
                    raise ArtifactReadyBuildSourceError("minute coverage status is invalid")
        if coverage_entries == 0:
            raise ArtifactReadyBuildSourceError("minute coverage is empty")
        return {
            "source_policy": self.profile.minute_source_policy,
            "database_rows": database_rows,
            "overlay_rows": overlay_rows,
            "synthesized_suspend_rows": synthesized,
            "tdx_rows": tdx_rows,
            "tushare_rows": tushare_rows,
            "overlap_rows_verified": overlap_rows,
            "missing_keys": 0,
            "duplicate_keys": 0,
            "overlap_mismatch_cells": 0,
            "provider_concurrency": 1,
            "database_writes": 0,
            "production_writes": 0,
        }

    def trading_days(self) -> tuple[date, ...]:
        values: list[date] = []
        for partition in self.ordered_partitions(Component.DAILY_BIN, "trading_calendar"):
            iterator = iter(partition.rows)
            try:
                for row in iterator:
                    if bool(row.get("is_trading")):
                        observed = _as_date(row.get("cal_date"))
                        if self.profile.start_date <= observed <= self.cutoff:
                            values.append(observed)
            finally:
                _close_iterator(iterator)
        result = tuple(sorted(set(values)))
        if not result or len(result) != len(values) or result[-1] != self.cutoff:
            raise ArtifactReadyBuildSourceError("artifact-ready trading calendar is incomplete/duplicated")
        return result

    def ordered_partitions(
        self,
        component: Component,
        dataset: str,
        *,
        effective: bool = True,
        date_ranges: Sequence[tuple[date, date]] = (),
        instruments: Sequence[str] = (),
    ) -> tuple[OrderedMappingPartition, ...]:
        manifest = self.component_manifests[component]
        entries = [
            dict(item)
            for item in manifest["partitions"]
            if isinstance(item, Mapping) and item.get("dataset") == dataset
        ]
        if not entries:
            raise ArtifactReadyBuildSourceError(f"artifact-ready component omits dataset: {component.value}:{dataset}")
        output: list[OrderedMappingPartition] = []
        ranges = tuple(sorted(date_ranges))
        selected_codes = frozenset(str(value).upper() for value in instruments)
        minute_buckets = (
            frozenset(_minute_bucket(value, self.profile.minute_code_bucket_count) for value in selected_codes)
            if dataset == "kline_minute_raw" and selected_codes
            else frozenset()
        )
        for entry in sorted(entries, key=lambda item: str(item["partition_key"])):
            if ranges and not _partition_overlaps_ranges(str(entry["partition_key"]), ranges):
                continue
            if minute_buckets:
                match = _MINUTE_PARTITION.fullmatch(str(entry["partition_key"]))
                if match is not None and int(match.group("bucket")) not in minute_buckets:
                    continue
            identity = str(entry["identity"])
            if entry.get("role") != "sealed_database_source":
                raise ArtifactReadyBuildSourceError(f"requested raw dataset has a derived role: {identity}")
            descriptor = self._raw_descriptor(entry)
            rows: Iterable[Mapping[str, Any]] = self._reader.iter_rows(
                dataset,
                str(entry["partition_key"]),
                decode_row_payload=True,
            )
            if effective and dataset == "adj_factor":
                rows = self._effective_adj_rows(component, descriptor, rows)
            elif effective and dataset == "kline_daily_raw" and self.profile.pit_authority_status == "ACTIVE_CANONICAL":
                rows = self._effective_daily_rows(component, descriptor, rows)
            elif effective and dataset == "kline_minute_raw":
                rows = self._effective_minute_rows(component, descriptor, rows)
            if ranges or selected_codes:
                rows = _filter_bounded_rows(
                    rows,
                    dataset=dataset,
                    date_ranges=ranges,
                    instruments=selected_codes,
                )
            output.append(OrderedMappingPartition(identity, rows))
        if not output:
            raise ArtifactReadyBuildSourceError(f"bounded source selection is empty: {component.value}:{dataset}")
        return tuple(output)

    def index_rows(self) -> Iterator[Mapping[str, Any]]:
        manifest = self.component_manifests[Component.DOMESTIC_INDEX_CONTEXT]
        entries = [
            item
            for item in manifest["partitions"]
            if isinstance(item, Mapping) and item.get("dataset") == "index_daily_merged"
        ]
        if not entries:
            raise ArtifactReadyBuildSourceError("merged index source is missing")
        previous: tuple[str, date] | None = None
        for entry in sorted(entries, key=lambda item: str(item["partition_key"])):
            receipt = self._derived_receipt(entry, ARTIFACT_READY_INDEX_CHUNK_SCHEMA)
            rows = receipt.get("rows")
            if not isinstance(rows, list):
                raise ArtifactReadyBuildSourceError("merged index rows are invalid")
            for row in rows:
                if not isinstance(row, Mapping):
                    raise ArtifactReadyBuildSourceError("merged index row is invalid")
                key = (str(row.get("ts_code", "")), _as_date(row.get("trade_date")))
                if previous is not None and key <= previous:
                    raise ArtifactReadyBuildSourceError("merged index rows are not globally ordered")
                previous = key
                yield dict(row)

    def iter_factor_frames(
        self,
        dataset: str,
        partition_key: str,
        *,
        start: date,
        end: date,
        max_rows: int,
        instruments: Sequence[str] = (),
    ) -> Iterator[pd.DataFrame]:
        aliases = {"daily_raw": "kline_daily_raw", "moneyflow": "moneyflow_ts"}
        source_dataset = aliases.get(dataset, dataset)
        partitions = [
            item
            for item in self.ordered_partitions(
                Component.FACTOR_H5_STATIC,
                source_dataset,
            )
            if item.identity == f"{source_dataset}:{partition_key}"
        ]
        if len(partitions) != 1:
            raise ArtifactReadyBuildSourceError(
                f"factor backing partition is missing/ambiguous: {dataset}:{partition_key}"
            )
        buffered: list[Mapping[str, Any]] = []
        requested = {str(value).upper() for value in instruments}
        iterator = iter(partitions[0].rows)
        try:
            for row in iterator:
                observed = _as_date(row.get("trade_date"))
                if start <= observed <= end and (not requested or str(row.get("ts_code", "")).upper() in requested):
                    buffered.append(row)
                    if len(buffered) == max_rows:
                        yield pd.DataFrame.from_records(buffered)
                        buffered = []
        finally:
            _close_iterator(iterator)
        if buffered:
            yield pd.DataFrame.from_records(buffered)

    def source_partition_evidence(self, component: Component) -> list[dict[str, Any]]:
        manifest = self.component_manifests[component]
        effective = manifest.get("effective_partitions")
        if not isinstance(effective, list):
            raise ArtifactReadyBuildSourceError(f"component effective source evidence is missing: {component.value}")
        output = [dict(entry) for entry in effective if isinstance(entry, Mapping)]
        if not output:
            raise ArtifactReadyBuildSourceError(f"component has no effective source evidence: {component.value}")
        if len(output) != len(effective):
            raise ArtifactReadyBuildSourceError(f"component effective source evidence is invalid: {component.value}")
        return output

    def factor_partition_plan(self) -> tuple[dict[str, Any], ...]:
        """Map natural-month outputs to one shared immutable backing partition."""

        datasets = (
            "kline_daily_raw",
            "adj_factor",
            "daily_basic",
            "moneyflow_ts",
            "bak_basic",
            "cyq_perf",
            "sector_data",
            "margin_detail",
        )
        manifest = self.component_manifests[Component.FACTOR_H5_STATIC]
        intervals: dict[str, list[tuple[date, date, str]]] = {}
        for dataset in datasets:
            values: list[tuple[date, date, str]] = []
            for item in manifest["partitions"]:
                if (
                    not isinstance(item, Mapping)
                    or item.get("dataset") != dataset
                    or item.get("role") != "sealed_database_source"
                ):
                    continue
                match = _DATE_PARTITION.fullmatch(str(item.get("partition_key", "")))
                if match is None:
                    raise ArtifactReadyBuildSourceError(f"factor source partition identity is invalid: {dataset}")
                values.append(
                    (
                        date.fromisoformat(match.group("start")),
                        date.fromisoformat(match.group("end")),
                        str(item["partition_key"]),
                    )
                )
            if not values:
                raise ArtifactReadyBuildSourceError(f"factor source dataset is missing: {dataset}")
            intervals[dataset] = sorted(values)

        output: list[dict[str, Any]] = []
        cursor = date(self.profile.start_date.year, self.profile.start_date.month, 1)
        while cursor <= self.cutoff:
            following = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
            start = max(cursor, self.profile.start_date)
            end = min(following - timedelta(days=1), self.cutoff)
            backings: set[str] = set()
            for dataset, values in intervals.items():
                matches = [key for left, right, key in values if left <= start and end <= right]
                if len(matches) != 1:
                    raise ArtifactReadyBuildSourceError(
                        f"factor month backing is missing/ambiguous: {dataset}:{cursor:%Y-%m}"
                    )
                backings.add(matches[0])
            if len(backings) != 1:
                raise ArtifactReadyBuildSourceError(
                    f"factor datasets do not share one backing partition: {cursor:%Y-%m}"
                )
            output.append(
                {
                    "partition_key": f"{cursor.year:04d}-{cursor.month:02d}",
                    "start": start,
                    "end": end,
                    "source_partition_key": next(iter(backings)),
                }
            )
            cursor = following
        if not output or output[-1]["end"] != self.cutoff:
            raise ArtifactReadyBuildSourceError("factor partition plan is incomplete")
        return tuple(output)

    def _validate_component_manifest(self, component: Component, value: Mapping[str, Any]) -> None:
        partitions = value.get("partitions")
        if (
            value.get("schema_version") != ARTIFACT_READY_COMPONENT_SCHEMA
            or value.get("component") != component.value
            or value.get("source_content_root") != self.source_content_root
            or not isinstance(partitions, list)
            or value.get("safety") != _ZERO_SAFETY
        ):
            raise ArtifactReadyBuildSourceError("artifact-ready component manifest identity differs")
        for entry in partitions:
            if not isinstance(entry, Mapping):
                raise ArtifactReadyBuildSourceError("component source entry is invalid")
            _complete_ref(self.cas, entry.get("rows_ref"))

    def _raw_descriptor(self, entry: Mapping[str, Any]) -> Mapping[str, Any]:
        identity = str(entry["identity"])
        descriptor = self._descriptors.get(identity)
        if descriptor is None:
            raise ArtifactReadyBuildSourceError(f"raw descriptor is missing from frozen build inputs: {identity}")
        for field in ("dataset", "partition_key", "row_count", "content_digest", "schema_digest", "rows_ref"):
            if descriptor.get(field) != entry.get(field):
                raise ArtifactReadyBuildSourceError(f"artifact-ready/raw descriptor differs: {identity}:{field}")
        return descriptor

    def _derived_receipt(self, entry: Mapping[str, Any], schema: str) -> Mapping[str, Any]:
        reference = _complete_ref(self.cas, entry.get("rows_ref"))
        value = self.cas.get_json_bounded(reference, max_bytes=32 * 1024 * 1024)
        if not isinstance(value, Mapping) or value.get("schema_version") != schema:
            raise ArtifactReadyBuildSourceError("derived source receipt schema differs")
        return value

    def _effective_adj_rows(
        self,
        component: Component,
        descriptor: Mapping[str, Any],
        database_rows: Iterable[Mapping[str, Any]],
    ) -> Iterator[Mapping[str, Any]]:
        entry = self._derived_entry(
            component,
            dataset="adj_factor_coverage",
            partition_key=str(descriptor["partition_key"]),
        )
        receipt = self._derived_receipt(entry, ARTIFACT_READY_ADJ_COVERAGE_SCHEMA)
        overlay = receipt.get("overlay_rows")
        if not isinstance(overlay, list):
            raise ArtifactReadyBuildSourceError("adj_factor overlay rows are invalid")
        cleaned = (
            {key: value for key, value in row.items() if key != "provider_ref"}
            for row in overlay
            if isinstance(row, Mapping)
        )
        yield from _merge_missing_only(
            database_rows,
            cleaned,
            key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
            source="adj_factor",
        )

    def _effective_daily_rows(
        self,
        component: Component,
        descriptor: Mapping[str, Any],
        database_rows: Iterable[Mapping[str, Any]],
    ) -> Iterator[Mapping[str, Any]]:
        entry = self._derived_entry(
            component,
            dataset="daily_coverage",
            partition_key=str(descriptor["partition_key"]),
        )
        receipt = self._derived_receipt(entry, ARTIFACT_READY_DAILY_COVERAGE_SCHEMA)
        overlay = receipt.get("overlay_rows")
        if not isinstance(overlay, list) or any(not isinstance(row, Mapping) for row in overlay):
            raise ArtifactReadyBuildSourceError("daily overlay rows are invalid")
        yield from _merge_missing_only(
            database_rows,
            (dict(row) for row in overlay),
            key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
            source="kline_daily_raw",
        )

    def _effective_minute_rows(
        self,
        component: Component,
        descriptor: Mapping[str, Any],
        database_rows: Iterable[Mapping[str, Any]],
    ) -> Iterator[Mapping[str, Any]]:
        entry = self._derived_entry(
            component,
            dataset="minute_coverage",
            partition_key=str(descriptor["partition_key"]),
        )
        receipt = self._derived_receipt(entry, ARTIFACT_READY_MINUTE_COVERAGE_SCHEMA)
        days = receipt.get("days")
        if not isinstance(days, list):
            raise ArtifactReadyBuildSourceError("minute coverage days are invalid")
        overlay_refs: dict[str, CASRef] = {}
        allowed_keys: set[tuple[str, date]] = set()
        for item in days:
            if not isinstance(item, Mapping) or item.get("status") != "PROVIDER_FILLED":
                continue
            reference = _complete_ref(self.cas, item.get("overlay_ref"))
            overlay_refs[reference.sha256] = reference
            allowed_keys.add((str(item["ts_code"]), _as_date(item["trade_date"])))
        overlay_rows: list[Mapping[str, Any]] = []
        for reference in overlay_refs.values():
            value = self.cas.get_json_bounded(reference, max_bytes=32 * 1024 * 1024)
            rows = value.get("rows") if isinstance(value, Mapping) else None
            if (
                not isinstance(value, Mapping)
                or value.get("schema_version") != "dataset_release_minute_overlay_window_v1"
                or not isinstance(rows, list)
            ):
                raise ArtifactReadyBuildSourceError("minute overlay window is invalid")
            overlay_rows.extend(
                dict(row)
                for row in rows
                if isinstance(row, Mapping)
                and (
                    str(row.get("ts_code", "")),
                    _as_datetime(row.get("trade_time")).date(),
                )
                in allowed_keys
            )
        overlay_rows.sort(
            key=lambda row: (
                str(row["ts_code"]),
                _as_datetime(row["trade_time"]),
                str(row.get("freq", "1m")),
            )
        )
        yield from _merge_missing_only(
            database_rows,
            overlay_rows,
            key=lambda row: (
                str(row["ts_code"]),
                _as_datetime(row["trade_time"]),
                str(row.get("freq", "1m")),
            ),
            source="kline_minute_raw",
        )

    def _derived_entry(self, component: Component, *, dataset: str, partition_key: str) -> Mapping[str, Any]:
        matches = [
            item
            for item in self.component_manifests[component]["partitions"]
            if isinstance(item, Mapping)
            and item.get("dataset") == dataset
            and item.get("partition_key") == partition_key
        ]
        if len(matches) != 1:
            raise ArtifactReadyBuildSourceError(f"derived source entry is missing/ambiguous: {dataset}:{partition_key}")
        return matches[0]


def _merge_missing_only(
    database: Iterable[Mapping[str, Any]],
    overlay: Iterable[Mapping[str, Any]],
    *,
    key,
    source: str,
) -> Iterator[Mapping[str, Any]]:
    left = iter(database)
    right = iter(overlay)
    try:
        left_row = next(left, None)
        right_row = next(right, None)
        previous: tuple[Any, ...] | None = None
        while left_row is not None or right_row is not None:
            if left_row is None:
                row, right_row = right_row, next(right, None)
            elif right_row is None:
                row, left_row = left_row, next(left, None)
            else:
                left_key, right_key = key(left_row), key(right_row)
                if left_key == right_key:
                    raise ArtifactReadyBuildSourceError(
                        f"{source} provider overlay attempts to override a database key"
                    )
                if left_key < right_key:
                    row, left_row = left_row, next(left, None)
                else:
                    row, right_row = right_row, next(right, None)
            assert row is not None
            observed = key(row)
            if previous is not None and observed <= previous:
                raise ArtifactReadyBuildSourceError(f"{source} effective rows are duplicated or unordered")
            previous = observed
            yield row
    finally:
        _close_iterator(left)
        _close_iterator(right)


def _close_iterator(iterator: Iterator[Mapping[str, Any]]) -> None:
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def _complete_ref(cas: CASStore, value: Any) -> CASRef:
    try:
        supplied = CASRef.from_value(value)
    except Exception as exc:
        raise ArtifactReadyBuildSourceError("artifact-ready CAS reference is invalid") from exc
    if supplied.size < 0:
        raise ArtifactReadyBuildSourceError("artifact-ready CAS reference is incomplete")
    verified = cas.verify(supplied)
    if supplied.relative_path != verified.relative_path:
        raise ArtifactReadyBuildSourceError("artifact-ready CAS path is non-canonical")
    return verified


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise ArtifactReadyBuildSourceError("artifact-ready date is invalid") from exc


def _partition_overlaps_ranges(partition_key: str, ranges: Sequence[tuple[date, date]]) -> bool:
    match = _DATE_PARTITION.fullmatch(partition_key) or _MINUTE_PARTITION.fullmatch(partition_key)
    if match is None:
        return True
    start = date.fromisoformat(match.group("start"))
    end = date.fromisoformat(match.group("end"))
    return any(left <= end and start <= right for left, right in ranges)


def _minute_bucket(code: str, bucket_count: int) -> int:
    if type(bucket_count) is not int or bucket_count <= 0 or bucket_count & (bucket_count - 1):
        raise ArtifactReadyBuildSourceError("minute bucket authority is invalid")
    return int(hashlib.sha256(code.upper().encode("utf-8")).hexdigest()[:16], 16) % bucket_count


def _filter_bounded_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    dataset: str,
    date_ranges: Sequence[tuple[date, date]],
    instruments: frozenset[str],
) -> Iterator[Mapping[str, Any]]:
    date_field = {
        "kline_daily_raw": "trade_date",
        "kline_minute_raw": "trade_time",
        "adj_factor": "trade_date",
        "stk_limit": "trade_date",
        "suspend_d": "trade_date",
    }.get(dataset)
    for row in rows:
        if instruments and str(row.get("ts_code", "")).upper() not in instruments:
            continue
        if date_ranges and date_field is not None:
            observed = _as_date(row.get(date_field))
            if not any(start <= observed <= end for start, end in date_ranges):
                continue
        yield row


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ArtifactReadyBuildSourceError("artifact-ready datetime is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed
    return parsed.astimezone(_SHANGHAI).replace(tzinfo=None)


__all__ = ["ArtifactReadyBuildSource", "ArtifactReadyBuildSourceError"]
