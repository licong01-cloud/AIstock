"""Build-only readers over a validated artifact-ready source CAS graph."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from backend.data_service.security_source_identity import (
    MONEYFLOW_DATASET,
    SecuritySourceIdentityManifest,
    load_default_security_source_identity_manifest,
)

from .artifact_ready_source import (
    ARTIFACT_READY_ADJ_COVERAGE_SCHEMA,
    ARTIFACT_READY_COMPONENT_SCHEMA,
    ARTIFACT_READY_DAILY_COVERAGE_SCHEMA,
    ARTIFACT_READY_INDEX_CHUNK_SCHEMA,
    ARTIFACT_READY_LIMIT_COVERAGE_SCHEMA,
    ARTIFACT_READY_MINUTE_COVERAGE_SCHEMA,
    _validate_limit_overlay_manifest_contract,
    _COMPONENT_DATASETS,
    _effective_partition_projection,
    load_artifact_ready_contract,
)
from .a_share_limit_rule import PRICE_LIMIT_RULE_VERSION
from .cas_store import CASRef, CASStore
from .canonical import digest_named_fields
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
        security_source_identity: SecuritySourceIdentityManifest | None = None,
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
        self.security_source_identity = security_source_identity or load_default_security_source_identity_manifest()

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
        calendar_component = (
            Component.DAILY_BIN if Component.DAILY_BIN in self.component_manifests else next(iter(self.component_manifests))
        )
        for partition in self.ordered_partitions(calendar_component, "trading_calendar"):
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
            if isinstance(item, Mapping) and (item.get("dataset") == dataset or (
                dataset == "adj_factor" and item.get("dataset") == "adj_factor_construction"
            ))
        ]
        if not entries:
            raise ArtifactReadyBuildSourceError(f"artifact-ready component omits dataset: {component.value}:{dataset}")
        output: list[OrderedMappingPartition] = []
        ranges = tuple(sorted(date_ranges))
        # SOURCE has already verified construction maxima against the same
        # snapshot's monthly facts. They remain normalization/boundary evidence,
        # not a second series partition for dates backed by regular adj_factor.
        series_ranges = tuple(
            (_as_date(match.group("start")), _as_date(match.group("end")))
            for item in entries
            if dataset == "adj_factor" and item["dataset"] == "adj_factor"
            if (match := _DATE_PARTITION.fullmatch(str(item["partition_key"]))) is not None
        )
        selected_codes = frozenset(str(value).upper() for value in instruments)
        minute_buckets = (
            frozenset(_minute_bucket(value, self.profile.minute_code_bucket_count) for value in selected_codes)
            if dataset == "kline_minute_raw" and selected_codes
            else frozenset()
        )
        for entry in sorted(entries, key=lambda item: str(item["partition_key"])):
            construction = entry["dataset"] == "adj_factor_construction"
            if ranges and not construction and not _partition_overlaps_ranges(str(entry["partition_key"]), ranges):
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
                str(entry["dataset"]),
                str(entry["partition_key"]),
                decode_row_payload=True,
            )
            if effective and dataset == "adj_factor" and not construction:
                rows = self._effective_adj_rows(component, descriptor, rows)
            elif effective and dataset == "stk_limit" and self.profile.pit_authority_status == "ACTIVE_CANONICAL":
                rows = self._effective_limit_rows(component, descriptor, rows)
            elif effective and dataset == "kline_daily_raw" and self.profile.pit_authority_status == "ACTIVE_CANONICAL":
                rows = self._effective_daily_rows(component, descriptor, rows)
            elif effective and dataset == "kline_minute_raw":
                rows = self._effective_minute_rows(component, descriptor, rows)
            if ranges or selected_codes or (construction and series_ranges):
                rows = _filter_bounded_rows(
                    rows,
                    dataset=dataset,
                    date_ranges=ranges,
                    instruments=selected_codes,
                    excluded_date_ranges=series_ranges if construction else (),
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
        requested = {
            str(value).upper() for value in (instruments or tuple(span.ts_code for span in self.pit_snapshot.spans))
        }
        source_requested = (
            set(
                self.security_source_identity.query_source_codes(
                    requested,
                    start,
                    end,
                    MONEYFLOW_DATASET,
                )
            )
            if dataset == "moneyflow"
            else requested
        )

        def frame_from_rows(rows: list[Mapping[str, Any]]) -> pd.DataFrame:
            frame = pd.DataFrame.from_records(rows)
            if dataset != "moneyflow" or frame.empty:
                return frame
            annotated = self.security_source_identity.annotate_source_rows(
                frame,
                canonical_codes=requested,
                source_dataset=MONEYFLOW_DATASET,
            )
            annotated["source_ts_code"] = annotated["ts_code"].astype(str).str.upper()
            annotated["ts_code"] = annotated.pop("_canonical_ts_code")
            return annotated

        iterator = iter(partitions[0].rows)
        try:
            for row in iterator:
                observed = _as_date(row.get("trade_date"))
                if start <= observed <= end and str(row.get("ts_code", "")).upper() in source_requested:
                    buffered.append(row)
                    if len(buffered) == max_rows:
                        yield frame_from_rows(buffered)
                        buffered = []
        finally:
            _close_iterator(iterator)
        if buffered:
            yield frame_from_rows(buffered)

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

    def factor_partition_plan(self, *, start: date | None = None) -> tuple[dict[str, Any], ...]:
        """Map natural-month outputs to one shared immutable backing partition."""

        range_start = self.profile.start_date if start is None else start
        if type(range_start) is not date or not self.profile.start_date <= range_start <= self.cutoff:
            raise ArtifactReadyBuildSourceError("factor partition start is outside the release range")
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
        cursor = range_start.replace(day=1)
        while cursor <= self.cutoff:
            following = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
            start = max(cursor, range_start)
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
        try:
            _validate_limit_overlay_manifest_contract(self.profile, component, value)
        except DatasetReleaseError as exc:
            raise ArtifactReadyBuildSourceError(str(exc)) from exc

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

    def _effective_limit_rows(
        self,
        component: Component,
        descriptor: Mapping[str, Any],
        database_rows: Iterable[Mapping[str, Any]],
    ) -> Iterator[Mapping[str, Any]]:
        matches = [
            item
            for item in self.component_manifests[component]["partitions"]
            if isinstance(item, Mapping)
            and item.get("dataset") == "stk_limit_rule_coverage"
            and item.get("partition_key") == str(descriptor["partition_key"])
        ]
        if not matches:
            yield from _filter_stk_limit_rows_to_pit(database_rows, self.pit_snapshot)
            return
        if len(matches) != 1:
            raise ArtifactReadyBuildSourceError("stk_limit rule overlay entry is ambiguous")
        entry = matches[0]
        receipt = self._derived_receipt(entry, ARTIFACT_READY_LIMIT_COVERAGE_SCHEMA)
        overlay = receipt.get("overlay_rows")
        if (
            receipt.get("raw_partition_identity") != f"stk_limit:{descriptor['partition_key']}"
            or receipt.get("pit_snapshot_digest") != self.pit_snapshot.spans_sha256
            or receipt.get("rule_version") != PRICE_LIMIT_RULE_VERSION
            or not isinstance(receipt.get("database_completion_rows"), int)
            or receipt.get("database_completion_rows", -1) < 0
            or receipt.get("database_override_rows") != 0
            or receipt.get("unresolved_keys") != 0
            or receipt.get("safety") != _ZERO_SAFETY
            or receipt.get("effective_content_root") != entry.get("content_digest")
            or not isinstance(overlay, list)
            or any(not isinstance(row, Mapping) for row in overlay)
        ):
            raise ArtifactReadyBuildSourceError("stk_limit rule overlay receipt is invalid")
        yield from _filter_stk_limit_rows_to_pit(
            _merge_stk_limit_completion(
                database_rows,
                (dict(row) for row in overlay),
                expected_completion_rows=int(receipt["database_completion_rows"]),
            ),
            self.pit_snapshot,
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


def _merge_stk_limit_completion(
    database: Iterable[Mapping[str, Any]],
    overlay: Iterable[Mapping[str, Any]],
    *,
    expected_completion_rows: int,
) -> Iterator[Mapping[str, Any]]:
    """Merge missing rows and exact completions without overriding valid data."""

    def key(row: Mapping[str, Any]) -> tuple[str, date]:
        return str(row["ts_code"]), _as_date(row["trade_date"])

    left = iter(database)
    right = iter(overlay)
    completions = 0
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
                    _require_limit_completion_match(left_row, right_row)
                    row = right_row
                    completions += 1
                    left_row, right_row = next(left, None), next(right, None)
                elif left_key < right_key:
                    row, left_row = left_row, next(left, None)
                else:
                    row, right_row = right_row, next(right, None)
            assert row is not None
            observed = key(row)
            if previous is not None and observed <= previous:
                raise ArtifactReadyBuildSourceError("stk_limit effective rows are duplicated or unordered")
            previous = observed
            yield row
        if completions != expected_completion_rows:
            raise ArtifactReadyBuildSourceError("stk_limit completion receipt count differs")
    finally:
        _close_iterator(left)
        _close_iterator(right)


def _require_limit_completion_match(
    database_row: Mapping[str, Any],
    overlay_row: Mapping[str, Any],
) -> None:
    missing = 0
    for field in ("pre_close", "up_limit", "down_limit"):
        raw = database_row.get(field)
        if raw is None or str(raw).strip() == "":
            missing += 1
            continue
        try:
            observed_raw = Decimal(str(raw))
            expected_raw = Decimal(str(overlay_row[field]))
            observed = observed_raw.quantize(Decimal("0.01"))
            expected = expected_raw.quantize(Decimal("0.01"))
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            raise ArtifactReadyBuildSourceError("stk_limit completion value is invalid") from exc
        if (
            not observed.is_finite()
            or observed <= 0
            or observed_raw != observed
            or expected_raw != expected
            or observed != expected
        ):
            raise ArtifactReadyBuildSourceError("stk_limit completion conflicts with database value")
    if missing == 0:
        raise ArtifactReadyBuildSourceError("stk_limit overlay attempts to override a complete database key")


def _filter_stk_limit_rows_to_pit(
    rows: Iterable[Mapping[str, Any]],
    pit_snapshot: FrozenPitSnapshot,
) -> Iterator[Mapping[str, Any]]:
    """Expose only canonical PIT stock-days after validating global order.

    Sealed ``stk_limit`` rows intentionally retain non-PIT history so the
    artifact stage can prove source identity.  Nullable repair fields are
    allowed there, but only PIT rows may enter the final Qlib normalizer.
    """

    mutable_ranges: dict[str, list[tuple[date, date]]] = {}
    for span in pit_snapshot.spans:
        mutable_ranges.setdefault(str(span.ts_code).upper(), []).append((span.eligible_start, span.eligible_end))
    ranges_by_code: dict[str, tuple[tuple[date, date], ...]] = {
        code: tuple(sorted(ranges)) for code, ranges in mutable_ranges.items()
    }
    previous: tuple[str, date] | None = None
    for row in rows:
        key = (str(row.get("ts_code", "")).upper(), _as_date(row.get("trade_date")))
        if previous is not None and key <= previous:
            raise ArtifactReadyBuildSourceError("stk_limit effective rows are duplicated or unordered")
        previous = key
        if any(start <= key[1] <= end for start, end in ranges_by_code.get(key[0], ())):
            yield row


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
    excluded_date_ranges: Sequence[tuple[date, date]] = (),
) -> Iterator[Mapping[str, Any]]:
    date_field = {
        "kline_daily_raw": "trade_date",
        "kline_minute_raw": "trade_time",
        "adj_factor": "trade_date",
        "stk_limit": "trade_date",
        "suspend_d": "trade_date",
    }.get(dataset)
    iterator = iter(rows)
    try:
        for row in iterator:
            if instruments and str(row.get("ts_code", "")).upper() not in instruments:
                continue
            if (date_ranges or excluded_date_ranges) and date_field is not None:
                observed = _as_date(row.get(date_field))
                if date_ranges and not any(start <= observed <= end for start, end in date_ranges):
                    continue
                if any(start <= observed <= end for start, end in excluded_date_ranges):
                    continue
            yield row
    finally:
        _close_iterator(iterator)


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


class ArtifactReadyPreparationBuildSource(ArtifactReadyBuildSource):
    """Explicit private reader; the normal full constructor remains strict.

    Only sealed preparation graphs are accepted. This type is not used by the
    ordinary SOURCE loader or build-input compiler and cannot publish a release.
    It reuses the exact effective-row and unit transformations above.
    """

    def __init__(self, *, cas, profile, snapshot, reference):
        from .monthly_preparation_source import PreparationSourceSnapshot
        from .monthly_preparation_artifacts import PREPARATION_ARTIFACT_SCHEMA
        from .artifact_ready_source import qfq_denominator_authority_from_mapping

        if not isinstance(snapshot, PreparationSourceSnapshot):
            raise ArtifactReadyBuildSourceError("private build requires a preparation source")
        ref = _complete_ref(cas, reference)
        payload = cas.get_json_bounded(ref, max_bytes=32 * 1024 * 1024)
        fields = {
            "schema_version",
            "operation_id",
            "profile",
            "cutoff",
            "preparation_source_manifest_ref",
            "source_content_root",
            "pit_snapshot_ref",
            "pit_snapshot_digest",
            "component_manifests",
            "qfq_denominator_authority_ref",
            "qfq_source_summary",
            "provider_receipt_refs",
            "derived_source_receipt_refs",
            "consistent_input_set_complete",
            "publication_allowed",
            "database_write_performed",
            "canonical_digest",
        }
        if not isinstance(payload, Mapping) or set(payload) != fields:
            raise ArtifactReadyBuildSourceError("private artifact graph fields differ")
        body = {key: value for key, value in payload.items() if key != "canonical_digest"}
        if (
            payload["schema_version"] != PREPARATION_ARTIFACT_SCHEMA
            or payload["canonical_digest"] != digest_named_fields(PREPARATION_ARTIFACT_SCHEMA, body)
            or payload["operation_id"] != snapshot.operation_id
            or payload["profile"] != profile.profile
            or payload["cutoff"] != snapshot.official_cutoff.isoformat()
            or payload["preparation_source_manifest_ref"] != snapshot.source_manifest_ref.as_dict()
            or payload["source_content_root"] != snapshot.source_content_root
            or payload["pit_snapshot_ref"] != snapshot.pit_snapshot_ref.as_dict()
            or payload["pit_snapshot_digest"] != snapshot.pit_snapshot_digest
            or snapshot.official_cutoff != snapshot.pit_snapshot.cutoff
            or any(
                payload[field] is not False
                for field in (
                    "consistent_input_set_complete",
                    "publication_allowed",
                    "database_write_performed",
                )
            )
        ):
            raise ArtifactReadyBuildSourceError("private artifact graph identity differs")
        components = payload["component_manifests"]
        supported = {Component.DAILY_BIN.value, Component.MINUTE_BIN.value, Component.DOMESTIC_INDEX_CONTEXT.value}
        if not isinstance(components, Mapping) or not components or not set(components) <= supported:
            raise ArtifactReadyBuildSourceError("private artifact graph component set differs")
        if not isinstance(payload["qfq_source_summary"], Mapping):
            raise ArtifactReadyBuildSourceError("private QFQ summary is invalid")
        if (
            cas.verify(snapshot.pit_snapshot_ref).sha256
            != hashlib.sha256(snapshot.pit_snapshot.canonical_bytes()).hexdigest()
        ):
            raise ArtifactReadyBuildSourceError("private PIT bytes differ")
        self.cas, self.profile = cas, profile
        self.cutoff, self.pit_snapshot = snapshot.official_cutoff, snapshot.pit_snapshot
        self.source_content_root = snapshot.source_content_root
        descriptors = tuple(part.as_build_input() for part in snapshot.partitions)
        self._descriptors = {f"{item['dataset']}:{item['partition_key']}": item for item in descriptors}
        if not self._descriptors or len(self._descriptors) != len(descriptors):
            raise ArtifactReadyBuildSourceError("private raw descriptor set differs")
        self._reader = CASSealedPartitionReader(cas, descriptors, max_partition_rows=1_000_000)
        self.contract_ref, self.contract = ref, dict(payload)
        self.component_manifests = {}
        for key, raw in components.items():
            component = Component(key)
            value = cas.get_json_bounded(_complete_ref(cas, raw), max_bytes=32 * 1024 * 1024)
            if not isinstance(value, Mapping):
                raise ArtifactReadyBuildSourceError("private component manifest is invalid")
            self._validate_component_manifest(component, value)
            partitions = value["partitions"]
            identities = [entry.get("identity") for entry in partitions]
            if len(identities) != len(set(identities)) or any(
                not isinstance(item, str) or not item for item in identities
            ):
                raise ArtifactReadyBuildSourceError("private component source identity is duplicated or missing")
            for entry in partitions:
                if entry.get("role") == "sealed_database_source":
                    self._raw_descriptor(entry)
            if not set(_COMPONENT_DATASETS[component]) <= {entry.get("dataset") for entry in partitions}:
                raise ArtifactReadyBuildSourceError("private component required datasets are missing")
            effective = _effective_partition_projection(component, partitions)
            details = value.get("details")
            if not isinstance(details, Mapping):
                raise ArtifactReadyBuildSourceError("private component details are missing")
            qfq_summary = details.get("qfq_source_summary")
            effective_root = digest_named_fields(
                "dataset_release_artifact_ready_component_effective_v1",
                {
                    "component": component.value,
                    "partitions": effective,
                    "qfq_denominator_authority_digest": qfq_summary.get("qfq_denominator_authority_digest")
                    if isinstance(qfq_summary, Mapping)
                    else None,
                },
            )
            provenance_root = digest_named_fields(
                ARTIFACT_READY_COMPONENT_SCHEMA,
                {
                    "component": component.value,
                    "source_content_root": self.source_content_root,
                    "partitions": partitions,
                    "details": dict(details),
                },
            )
            if (
                value.get("effective_partitions") != effective
                or value.get("component_content_root") != effective_root
                or value.get("component_effective_content_root") != effective_root
                or value.get("component_provenance_root") != provenance_root
            ):
                raise ArtifactReadyBuildSourceError("private component content identity differs")
            self.component_manifests[component] = dict(value)
        for field in ("provider_receipt_refs", "derived_source_receipt_refs"):
            if not isinstance(payload[field], list):
                raise ArtifactReadyBuildSourceError("private source proof refs are invalid")
            for raw in payload[field]:
                _complete_ref(cas, raw)
        self.qfq_authority = None
        if set(components) & {Component.DAILY_BIN.value, Component.MINUTE_BIN.value}:
            qfq_ref = _complete_ref(cas, payload["qfq_denominator_authority_ref"])
            self.qfq_authority = qfq_denominator_authority_from_mapping(
                cas.get_json_bounded(qfq_ref, max_bytes=32 * 1024 * 1024),
                expected_cutoff=self.cutoff,
                expected_pit_spans_sha256=self.pit_snapshot.spans_sha256,
            )
            if payload["qfq_source_summary"].get("qfq_denominator_authority_digest") != self.qfq_authority.digest:
                raise ArtifactReadyBuildSourceError("private QFQ identity differs")
            for component, manifest in self.component_manifests.items():
                if component in {Component.DAILY_BIN, Component.MINUTE_BIN} and (
                    manifest["details"].get("qfq_denominator_authority_ref") != qfq_ref.as_dict()
                    or manifest["details"].get("qfq_source_summary") != payload["qfq_source_summary"]
                ):
                    raise ArtifactReadyBuildSourceError("private component QFQ identity differs")
        elif payload["qfq_denominator_authority_ref"] is not None:
            raise ArtifactReadyBuildSourceError("private non-price component includes QFQ authority")
        self.security_source_identity = load_default_security_source_identity_manifest()

    @property
    def artifact_ready_content_root(self) -> str:
        raise ArtifactReadyBuildSourceError("private preparation is not a full artifact-ready graph")


__all__ = ["ArtifactReadyBuildSource", "ArtifactReadyBuildSourceError", "ArtifactReadyPreparationBuildSource"]
