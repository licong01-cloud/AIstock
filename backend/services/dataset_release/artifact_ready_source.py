"""Immutable artifact-ready source contracts derived from sealed source CAS.

The source authority freezes raw database partitions.  This module is the
only bridge allowed to add candidate-local, missing-key provider evidence:
minute data uses DB -> fixed-loopback TDX -> Tushare ``stk_mins`` and index
data uses DB -> Tushare.  Provider data is never written back to a database.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import threading
from bisect import bisect_left, bisect_right
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Iterable, Iterator, Mapping, Protocol, Sequence

from .canonical import (
    digest_named_fields,
    ensure_sha256,
    merkle_root_from_named_digests,
)
from .canonical_stock_transformer import (
    QfqDenominatorAuthority,
    qfq_denominator_authority_from_mapping,
)
from .cas_store import CASRef, CASStore, CASStoreError
from .contracts import Component
from .errors import DatasetReleaseError, IndexOverlapConflict, SourceManifestError
from .index_contract import (
    DOMESTIC_INDEX_DEFINITIONS,
    INDEX_SOURCE_VALUE_FIELDS,
    IndexDefinition,
    merge_index_rows_missing_only,
)
from .index_sources import IndexProviderRateLimitTerminal, IndexProviderUnavailable
from .minute_overlay import (
    CHINA_TZ,
    MinuteGap,
    MinuteOverlayBuilder,
    MinuteProviderRateLimitTerminal,
    MinuteProviderTerminal,
    MinuteSourceConflict,
    normalize_database_rows,
)
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile
from .sealed_source_reader import CASSealedPartitionReader, VerifiedRowStream
from .source_rows_codec import (
    validate_rows_codec_identity,
    validate_rows_envelope,
)


ARTIFACT_READY_CONTRACT_SCHEMA = "dataset_release_artifact_ready_contract_v1"
ARTIFACT_READY_COMPONENT_SCHEMA = "dataset_release_artifact_ready_component_manifest_v1"
ARTIFACT_READY_MINUTE_COVERAGE_SCHEMA = "dataset_release_artifact_ready_minute_coverage_v1"
ARTIFACT_READY_INDEX_CHUNK_SCHEMA = "dataset_release_artifact_ready_index_chunk_v1"
ARTIFACT_READY_ADJ_COVERAGE_SCHEMA = "dataset_release_artifact_ready_adj_factor_coverage_v1"
ARTIFACT_READY_FACTOR_OVERLAY_SCHEMA = "dataset_release_artifact_ready_factor_overlay_coverage_v1"
ARTIFACT_READY_DAILY_COVERAGE_SCHEMA = "dataset_release_artifact_ready_daily_coverage_v1"
ARTIFACT_READY_RECHECK_SCHEMA = "dataset_release_artifact_ready_recheck_v1"
ARTIFACT_READY_EFFECTIVE_SCHEMA = "dataset_release_artifact_ready_effective_v1"
ARTIFACT_READY_PROVENANCE_SCHEMA = "dataset_release_artifact_ready_provenance_v1"
SOURCE_MONTH_CONTENT_LEAF_SCHEMA = "dataset_release_source_month_content_leaf_v1"
PROVIDER_FAILURE_SCHEMA = "dataset_release_artifact_ready_provider_failure_v1"
MAX_CONTROL_RECEIPT_BYTES = 32 * 1024 * 1024
MAX_TDX_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_TDX_WINDOW_BARS = 25_000
MAX_TUSHARE_PROVIDER_FRAME_BYTES = 16 * 1024 * 1024
MAX_TUSHARE_PROVIDER_RECEIPT_BYTES = 16 * 1024 * 1024
MAX_TUSHARE_MINUTE_ROWS_PER_DAY = 241
MAX_TUSHARE_ADJ_FACTOR_ROWS_PER_DAY = 20_000
MAX_TUSHARE_DAILY_ROWS_PER_CODE = 3_000
MAX_DAILY_OVERLAY_ROWS_PER_PARTITION = 50_000
TDX_DEFAULT_PORT = 19080
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

_COMPONENT_DATASETS: Mapping[Component, tuple[str, ...]] = {
    Component.DAILY_BIN: (
        "trading_calendar",
        "kline_daily_raw",
        "adj_factor",
        "stk_limit",
        "suspend_d",
        "index_daily",
    ),
    Component.MINUTE_BIN: (
        "trading_calendar",
        "kline_daily_raw",
        "adj_factor",
        "stk_limit",
        "suspend_d",
        "kline_minute_raw",
    ),
    Component.FACTOR_H5_STATIC: (
        "trading_calendar",
        "kline_daily_raw",
        "adj_factor",
        "daily_basic",
        "moneyflow_ts",
        "bak_basic",
        "cyq_perf",
        "sector_data",
        "margin_detail",
        "stock_basic",
        "sw_index_classify",
        "sw_index_member",
    ),
    Component.DOMESTIC_INDEX_CONTEXT: ("trading_calendar", "index_daily"),
}


class ArtifactReadySourceError(DatasetReleaseError):
    code = "BLOCKED_ARTIFACT_READY_SOURCE_INVALID"


class ArtifactReadyProviderTerminal(ArtifactReadySourceError):
    code = "BLOCKED_PROVIDER_TERMINAL"


class ArtifactReadyMinuteConflict(ArtifactReadySourceError):
    code = "BLOCKED_MINUTE_PROVIDER_CONFLICT"


class ArtifactReadyIndexConflict(ArtifactReadySourceError):
    code = "DATASET_RELEASE_INDEX_PROVIDER_CONFLICT"


class ArtifactReadyCoverageIncomplete(ArtifactReadySourceError):
    code = "BLOCKED_ARTIFACT_READY_COVERAGE_INCOMPLETE"


class ArtifactReadySourceRevised(ArtifactReadySourceError):
    code = "BLOCKED_SOURCE_REVISED"


class ArtifactSourceView(Protocol):
    def descriptors(self, dataset: str) -> Sequence[Mapping[str, Any]]: ...

    def iter_partition_rows(self, descriptor: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]: ...


class ArtifactSnapshot(Protocol):
    official_cutoff: date
    pit_snapshot: FrozenPitSnapshot
    source_content_root: str
    pit_snapshot_digest: str
    partitions: Sequence[Any]


MinuteTdxRows = Callable[[str, date, date], Sequence[Mapping[str, Any]]]
MinuteTushareRows = Callable[[str, date], Sequence[Mapping[str, Any]]]
IndexTushareRows = Callable[[IndexDefinition, date, date], Sequence[Mapping[str, Any]]]
AdjFactorTushareRows = Callable[[date], Sequence[Mapping[str, Any]]]
DailyTushareRows = Callable[[str, date, date], Sequence[Mapping[str, Any]]]


@dataclass(frozen=True, slots=True)
class ArtifactReadySourceBundle:
    artifact_ready_contract_ref: CASRef
    artifact_ready_content_root: str
    provider_receipt_refs: tuple[CASRef, ...]
    derived_source_receipt_refs: tuple[CASRef, ...]


@dataclass(frozen=True, slots=True)
class ArtifactReadyRecheckResult:
    source_probe_key: str
    source_probe_ref: CASRef
    artifact_ready_content_root: str


@dataclass(frozen=True, slots=True)
class _MinuteDateWindow:
    lower: date
    upper: date
    trading_left: int
    trading_right: int


@dataclass(frozen=True, slots=True)
class _MinuteEligibilityIndex:
    """Bounded PIT/calendar index for descriptor-local minute coverage keys."""

    trading_dates: tuple[date, ...]
    spans_by_code: Mapping[str, tuple[Any, ...]]
    span_ends_by_code: Mapping[str, tuple[date, ...]]
    bucket_codes: Mapping[int, tuple[str, ...]]
    minute_start_date: date

    @classmethod
    def build(
        cls,
        *,
        trading_dates: Iterable[date],
        spans: Iterable[Any],
        minute_start_date: date,
        bucket_count: int,
    ) -> "_MinuteEligibilityIndex":
        dates = tuple(trading_dates)
        if not dates or any(current <= previous for previous, current in zip(dates, dates[1:])):
            raise ArtifactReadyCoverageIncomplete("minute trading dates are empty, duplicated, or unordered")
        span_lists: dict[str, list[Any]] = {}
        for span in spans:
            span_lists.setdefault(str(span.ts_code), []).append(span)
        if not span_lists:
            raise ArtifactReadyCoverageIncomplete("minute PIT spans are empty")
        spans_by_code: dict[str, tuple[Any, ...]] = {}
        span_ends_by_code: dict[str, tuple[date, ...]] = {}
        bucket_codes: dict[int, list[str]] = {}
        for code in sorted(span_lists):
            values = tuple(span_lists[code])
            previous_end: date | None = None
            for span in values:
                if (
                    str(span.ts_code) != code
                    or span.eligible_end < span.eligible_start
                    or (previous_end is not None and span.eligible_start <= previous_end)
                ):
                    raise ArtifactReadyCoverageIncomplete("minute PIT spans are invalid or overlapping")
                previous_end = span.eligible_end
            spans_by_code[code] = values
            span_ends_by_code[code] = tuple(span.eligible_end for span in values)
            bucket_codes.setdefault(_minute_bucket(code, bucket_count), []).append(code)
        return cls(
            trading_dates=dates,
            spans_by_code=spans_by_code,
            span_ends_by_code=span_ends_by_code,
            bucket_codes={bucket: tuple(codes) for bucket, codes in bucket_codes.items()},
            minute_start_date=minute_start_date,
        )

    def _window(self, start: date, end: date) -> _MinuteDateWindow | None:
        lower = max(start, self.minute_start_date)
        if end < lower:
            return None
        left = bisect_left(self.trading_dates, lower)
        right = bisect_right(self.trading_dates, end, left)
        if left >= right:
            return None
        return _MinuteDateWindow(lower, end, left, right)

    def iter_expected(self, *, bucket: int, start: date, end: date) -> Iterator[tuple[str, date]]:
        window = self._window(start, end)
        if window is None:
            return
        for code in self.bucket_codes.get(bucket, ()):
            spans = self.spans_by_code[code]
            span_ends = self.span_ends_by_code[code]
            first = bisect_left(span_ends, window.lower)
            for span in spans[first:]:
                if span.eligible_start > window.upper:
                    break
                eligible_start = max(window.lower, span.eligible_start)
                eligible_end = min(window.upper, span.eligible_end)
                trading_left = bisect_left(
                    self.trading_dates,
                    eligible_start,
                    window.trading_left,
                    window.trading_right,
                )
                trading_right = bisect_right(
                    self.trading_dates,
                    eligible_end,
                    trading_left,
                    window.trading_right,
                )
                for ordinal in range(trading_left, trading_right):
                    yield code, self.trading_dates[ordinal]


@dataclass(frozen=True, slots=True)
class LoadedArtifactReadyContract:
    reference: CASRef
    artifact_ready_effective_content_root: str
    artifact_ready_provenance_root: str
    component_manifest_refs: Mapping[str, CASRef]
    component_effective_content_roots: Mapping[str, str]
    qfq_denominator_authority_ref: CASRef
    qfq_denominator_authority: QfqDenominatorAuthority
    payload: Mapping[str, Any]


def load_artifact_ready_contract(
    cas: CASStore,
    profile: DatasetProfile,
    reference: CASRef | Mapping[str, Any] | str,
    *,
    expected_source_content_root: str,
    expected_pit_snapshot_digest: str,
    verify_partition_payloads: bool = True,
) -> LoadedArtifactReadyContract:
    """Strict provider-free loader for immutable build-stage consumption."""

    expected_source = ensure_sha256(expected_source_content_root, field="expected_source_content_root")
    expected_pit = ensure_sha256(expected_pit_snapshot_digest, field="expected_pit_snapshot_digest")
    contract_ref = _complete_ref(cas, reference, field="artifact-ready contract")
    payload = cas.get_json_bounded(contract_ref, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
    if not isinstance(payload, Mapping):
        raise ArtifactReadySourceError("artifact-ready contract is not an object")
    raw_components = payload.get("component_manifests")
    expected_components = {item.value for item in Component}
    if (
        payload.get("schema_version") != ARTIFACT_READY_CONTRACT_SCHEMA
        or payload.get("profile") != profile.profile
        or payload.get("source_content_root") != expected_source
        or payload.get("pit_snapshot_digest") != expected_pit
        or payload.get("static_schema_digest") != profile.static_schema_digest
        or payload.get("safety") != _ZERO_SAFETY
        or not isinstance(raw_components, Mapping)
        or set(raw_components) != expected_components
    ):
        raise ArtifactReadySourceError("artifact-ready contract identity differs")
    qfq_ref = _complete_ref(
        cas,
        payload.get("qfq_denominator_authority_ref"),
        field="qfq denominator authority",
    )
    qfq_payload = cas.get_json_bounded(qfq_ref, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
    if not isinstance(qfq_payload, Mapping):
        raise ArtifactReadySourceError("QFQ denominator authority is invalid")
    authority = qfq_denominator_authority_from_mapping(
        qfq_payload,
        expected_cutoff=date.fromisoformat(str(payload.get("cutoff"))),
        expected_pit_spans_sha256=expected_pit,
    )
    if payload.get("qfq_denominator_authority_digest") != authority.digest:
        raise ArtifactReadySourceError("QFQ denominator authority differs")
    for raw_ref in (
        *(payload.get("provider_receipt_refs") or ()),
        *(payload.get("derived_source_receipt_refs") or ()),
    ):
        _complete_ref(cas, raw_ref, field="artifact-ready evidence")
    component_refs: dict[str, CASRef] = {}
    component_roots: dict[str, str] = {}
    for component, raw_ref in raw_components.items():
        manifest_ref = _complete_ref(cas, raw_ref, field=f"{component} artifact-ready manifest")
        manifest = cas.get_json_bounded(manifest_ref, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
        if (
            not isinstance(manifest, Mapping)
            or manifest.get("schema_version") != ARTIFACT_READY_COMPONENT_SCHEMA
            or manifest.get("component") != component
            or manifest.get("source_content_root") != expected_source
            or manifest.get("safety") != _ZERO_SAFETY
            or not isinstance(manifest.get("partitions"), list)
            or not isinstance(manifest.get("effective_partitions"), list)
        ):
            raise ArtifactReadySourceError("artifact-ready component manifest identity differs")
        for partition in manifest["partitions"]:
            if not isinstance(partition, Mapping):
                raise ArtifactReadySourceError("artifact-ready component partition is invalid")
            if verify_partition_payloads:
                _complete_ref(
                    cas,
                    partition.get("rows_ref"),
                    field=f"{component} artifact-ready partition",
                )
            else:
                try:
                    cas.validate_reference_metadata(partition.get("rows_ref"))
                except CASStoreError as exc:
                    raise ArtifactReadySourceError("artifact-ready partition CAS metadata differs") from exc
        details = manifest.get("details")
        details = details if isinstance(details, Mapping) else {}
        qfq_summary = details.get("qfq_source_summary")
        component_qfq_digest = (
            qfq_summary.get("qfq_denominator_authority_digest") if isinstance(qfq_summary, Mapping) else None
        )
        expected_effective = digest_named_fields(
            "dataset_release_artifact_ready_component_effective_v1",
            {
                "component": component,
                "partitions": manifest["effective_partitions"],
                "qfq_denominator_authority_digest": component_qfq_digest,
            },
        )
        expected_provenance = digest_named_fields(
            ARTIFACT_READY_COMPONENT_SCHEMA,
            {
                "component": component,
                "source_content_root": expected_source,
                "partitions": manifest["partitions"],
                "details": dict(details),
            },
        )
        if (
            manifest.get("component_effective_content_root") != expected_effective
            or manifest.get("component_content_root") != expected_effective
            or manifest.get("component_provenance_root") != expected_provenance
        ):
            raise ArtifactReadySourceError("artifact-ready component root identity differs")
        component_refs[component] = manifest_ref
        component_roots[component] = expected_effective
    effective_root = digest_named_fields(
        ARTIFACT_READY_EFFECTIVE_SCHEMA,
        {
            "profile": profile.profile,
            "cutoff": date.fromisoformat(str(payload.get("cutoff"))),
            "pit_snapshot_digest": expected_pit,
            "static_schema_digest": profile.static_schema_digest,
            "qfq_denominator_authority_digest": authority.digest,
            "component_effective_content_roots": component_roots,
        },
    )
    provenance_body = dict(payload)
    actual_content = str(provenance_body.pop("artifact_ready_content_root", ""))
    explicit_effective = str(provenance_body.pop("artifact_ready_effective_content_root", ""))
    actual_provenance = str(provenance_body.pop("artifact_ready_provenance_root", ""))
    expected_provenance = digest_named_fields(ARTIFACT_READY_PROVENANCE_SCHEMA, provenance_body)
    if (
        actual_content != effective_root
        or explicit_effective != effective_root
        or actual_provenance != expected_provenance
    ):
        raise ArtifactReadySourceError("artifact-ready global root identity differs")
    return LoadedArtifactReadyContract(
        reference=contract_ref,
        artifact_ready_effective_content_root=effective_root,
        artifact_ready_provenance_root=expected_provenance,
        component_manifest_refs=component_refs,
        component_effective_content_roots=component_roots,
        qfq_denominator_authority_ref=qfq_ref,
        qfq_denominator_authority=authority,
        payload=dict(payload),
    )


def load_artifact_ready_recheck_expectations(
    cas: CASStore,
    loaded: LoadedArtifactReadyContract,
) -> tuple[Mapping[str, Any], ...]:
    """Load complete sealed source/PIT identities without blob payload reads."""

    raw_values = loaded.payload.get("source_recheck_partition_expectations")
    if not isinstance(raw_values, list) or not raw_values:
        raise ArtifactReadySourceRevised("artifact-ready contract lacks recheck partition expectations")
    values: list[Mapping[str, Any]] = []
    identities: set[str] = set()
    scopes: set[str] = set()
    for raw in raw_values:
        if not isinstance(raw, Mapping):
            raise ArtifactReadySourceRevised("artifact-ready recheck partition expectation is invalid")
        value = dict(raw)
        scope = str(value.get("recheck_partition_scope", ""))
        identity = f"{value.get('dataset')}:{value.get('partition_key')}"
        if (
            scope not in {"source", "pit"}
            or not all((value.get("dataset"), value.get("partition_key")))
            or identity in identities
        ):
            raise ArtifactReadySourceRevised("artifact-ready recheck partition identity differs")
        try:
            reference = cas.validate_reference_metadata(value.get("rows_ref"))
            validate_rows_envelope(value, cas_size=reference.size)
            validate_rows_codec_identity(value)
            for field in (
                "schema_digest",
                "content_digest",
                "merkle_root",
                "source_table_schema_digest",
                "ingestion_audit_identity",
            ):
                ensure_sha256(str(value.get(field, "")), field=field)
            if type(value.get("row_count")) is not int or value["row_count"] < 0:
                raise ValueError("row count")
        except (CASStoreError, SourceManifestError, TypeError, ValueError) as exc:
            raise ArtifactReadySourceRevised("artifact-ready recheck partition contract is incomplete") from exc
        value["rows_ref"] = reference.as_dict()
        identities.add(identity)
        scopes.add(scope)
        values.append(value)
    if scopes != {"source", "pit"}:
        raise ArtifactReadySourceRevised("artifact-ready recheck source and PIT expectations are required")
    source_root = merkle_root_from_named_digests(
        "dataset_release_source_content_root_v1",
        (
            (f"{item['dataset']}:{item['partition_key']}", item["content_digest"])
            for item in values
            if item["recheck_partition_scope"] == "source"
        ),
    )
    if source_root != loaded.payload.get("source_content_root"):
        raise ArtifactReadySourceRevised("artifact-ready recheck partition root differs")
    return tuple(
        sorted(
            values,
            key=lambda item: (
                str(item["recheck_partition_scope"]),
                str(item["dataset"]),
                str(item["partition_key"]),
            ),
        )
    )


class _SealedSnapshotView:
    def __init__(self, cas: CASStore, snapshot: ArtifactSnapshot) -> None:
        values = tuple(
            dict(item.as_build_input()) if hasattr(item, "as_build_input") else dict(item)
            for item in snapshot.partitions
        )
        self._values = values
        self._reader = CASSealedPartitionReader(
            cas,
            values,
            max_partition_rows=1_000_000,
        )

    def descriptors(self, dataset: str) -> Sequence[Mapping[str, Any]]:
        return tuple(
            value
            for value in sorted(
                self._values,
                key=lambda item: (str(item.get("dataset")), str(item.get("partition_key"))),
            )
            if value.get("dataset") == dataset
        )

    def iter_partition_rows(self, descriptor: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        return self._reader.iter_rows(
            str(descriptor["dataset"]),
            str(descriptor["partition_key"]),
            decode_row_payload=True,
        )


@contextmanager
def _managed_partition_rows(
    view: ArtifactSourceView,
    descriptor: Mapping[str, Any],
) -> Iterator[Iterator[Mapping[str, Any]]]:
    """Verify sealed rows on success and abort any iterator on failure."""

    rows = view.iter_partition_rows(descriptor)
    if isinstance(rows, VerifiedRowStream):
        with rows as stream:
            yield stream
        return
    iterator = iter(rows)
    try:
        yield iterator
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


class _LazyTushare:
    def __init__(self) -> None:
        self._provider: Any | None = None

    def provider(self) -> Any:
        if self._provider is not None:
            return self._provider
        token = os.getenv("TUSHARE_TOKEN", "").strip()
        if not token:
            raise ArtifactReadyProviderTerminal("Tushare credential is unavailable")
        try:
            import tushare as ts

            self._provider = ts.pro_api(token)
        except Exception as exc:
            raise ArtifactReadyProviderTerminal("Tushare provider initialization failed") from exc
        return self._provider


class _FrozenProviderReplay:
    """Read-only adapter over the initial immutable provider CAS receipts."""

    def __init__(self, cas: CASStore, raw_refs: Sequence[Any]) -> None:
        self.cas = cas
        self.minute: list[tuple[str, date, date, CASRef]] = []
        self.adj: dict[date, CASRef] = {}
        self.daily: dict[str, CASRef] = {}
        self.index: list[tuple[str, date, date, CASRef]] = []
        for raw_ref in raw_refs:
            reference = _complete_ref(cas, raw_ref, field="provider replay ref")
            payload = cas.get_json_bounded(reference, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
            if not isinstance(payload, Mapping):
                raise ArtifactReadySourceError("provider replay receipt is invalid")
            schema = payload.get("schema_version")
            if schema == "dataset_release_minute_provider_window_v1":
                self.minute.append(
                    (
                        str(payload.get("ts_code", "")).upper(),
                        _as_date(payload.get("start"), field="minute replay start"),
                        _as_date(payload.get("end"), field="minute replay end"),
                        reference,
                    )
                )
            elif schema == "dataset_release_adj_factor_provider_snapshot_v1":
                day = _as_date(payload.get("trade_date"), field="adj_factor replay date")
                if day in self.adj:
                    raise ArtifactReadySourceError("adj_factor replay date is duplicated")
                self.adj[day] = reference
            elif schema == "dataset_release_daily_provider_snapshot_v1":
                code = str(payload.get("ts_code", "")).upper()
                if not code or code in self.daily:
                    raise ArtifactReadySourceError("daily provider replay code is invalid or duplicated")
                self.daily[code] = reference
            elif schema == "dataset_release_index_provider_snapshot_v1":
                self.index.append(
                    (
                        str(payload.get("ts_code", "")).upper(),
                        _as_date(payload.get("start"), field="index replay start"),
                        _as_date(payload.get("end"), field="index replay end"),
                        reference,
                    )
                )
            else:
                raise ArtifactReadySourceError("provider replay receipt schema is not allowlisted")

    def _minute_calls(self, code: str, start: date, end: date) -> Sequence[Mapping[str, Any]]:
        matches = [item for item in self.minute if item[0] == code and item[1] <= start and end <= item[2]]
        if len(matches) != 1:
            raise MinuteProviderTerminal("immutable minute provider window is unavailable or ambiguous")
        payload = self.cas.get_json_bounded(matches[0][3], max_bytes=MAX_CONTROL_RECEIPT_BYTES)
        calls = payload.get("calls") if isinstance(payload, Mapping) else None
        if not isinstance(calls, list):
            raise MinuteProviderTerminal("immutable minute provider calls are invalid")
        return tuple(call for call in calls if isinstance(call, Mapping))

    def fetch_tdx_rows(self, code: str, start: date, end: date) -> Sequence[Mapping[str, Any]]:
        rows = [
            row
            for call in self._minute_calls(code, start, end)
            if call.get("provider") == "tdx"
            for row in (call.get("rows") or ())
            if isinstance(row, Mapping)
        ]
        if not rows:
            raise MinuteProviderTerminal("immutable TDX observation is unavailable")
        return rows

    def fetch_tushare_minute_rows(self, code: str, day: date) -> Sequence[Mapping[str, Any]]:
        rows = [
            row
            for call in self._minute_calls(code, day, day)
            if call.get("provider") == "tushare" and call.get("start") == day.isoformat()
            for row in (call.get("rows") or ())
            if isinstance(row, Mapping)
        ]
        if not rows:
            raise MinuteProviderTerminal("immutable Tushare minute observation is unavailable")
        return rows

    def fetch_tushare_adj_factor_rows(self, day: date) -> Sequence[Mapping[str, Any]]:
        reference = self.adj.get(day)
        if reference is None:
            raise ArtifactReadyProviderTerminal("immutable Tushare adj_factor observation is unavailable")
        payload = self.cas.get_json_bounded(reference, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
        rows = payload.get("rows") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise ArtifactReadyProviderTerminal("immutable Tushare adj_factor observation is invalid")
        return tuple(row for row in rows if isinstance(row, Mapping))

    def fetch_tushare_daily_rows(self, code: str, start: date, end: date) -> Sequence[Mapping[str, Any]]:
        reference = self.daily.get(code)
        if reference is None:
            raise ArtifactReadyProviderTerminal("immutable Tushare daily observation is unavailable")
        payload = self.cas.get_json_bounded(reference, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
        rows = payload.get("rows") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise ArtifactReadyProviderTerminal("immutable Tushare daily observation is invalid")
        return tuple(
            row
            for row in rows
            if isinstance(row, Mapping) and start <= _as_date(row.get("trade_date"), field="daily replay date") <= end
        )

    def fetch_tushare_index_rows(
        self, definition: IndexDefinition, start: date, end: date
    ) -> Sequence[Mapping[str, Any]]:
        matches = [
            item for item in self.index if item[0] == definition.daily_code and item[1] <= start and end <= item[2]
        ]
        if len(matches) != 1:
            raise IndexProviderUnavailable("immutable Tushare index observation is unavailable or ambiguous")
        payload = self.cas.get_json_bounded(matches[0][3], max_bytes=MAX_CONTROL_RECEIPT_BYTES)
        rows = payload.get("rows") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise IndexProviderUnavailable("immutable Tushare index observation is invalid")
        return tuple(row for row in rows if isinstance(row, Mapping))


class ArtifactReadySourceBuilder:
    """Build provider-complete immutable manifests without source DB writes."""

    def __init__(
        self,
        profile: DatasetProfile,
        cas: CASStore,
        *,
        fetch_tdx_rows: MinuteTdxRows | None = None,
        fetch_tushare_minute_rows: MinuteTushareRows | None = None,
        fetch_tushare_index_rows: IndexTushareRows | None = None,
        fetch_tushare_adj_factor_rows: AdjFactorTushareRows | None = None,
        fetch_tushare_daily_rows: DailyTushareRows | None = None,
    ) -> None:
        self.profile = profile
        self.cas = cas
        self._tushare = _LazyTushare()
        self.fetch_tdx_rows = fetch_tdx_rows or self._fetch_tdx_rows
        self.fetch_tushare_minute_rows = fetch_tushare_minute_rows or self._fetch_tushare_minute_rows
        self.fetch_tushare_index_rows = fetch_tushare_index_rows or self._fetch_tushare_index_rows
        self.fetch_tushare_adj_factor_rows = fetch_tushare_adj_factor_rows or self._fetch_tushare_adj_factor_rows
        self.fetch_tushare_daily_rows = fetch_tushare_daily_rows or self._fetch_tushare_daily_rows
        self._provider_lock = threading.Lock()
        self._active_provider_calls = 0
        self.peak_provider_calls = 0

    def build(
        self,
        snapshot: ArtifactSnapshot,
        *,
        checkpoint: Callable[[], None] = lambda: None,
        source_view: ArtifactSourceView | None = None,
    ) -> ArtifactReadySourceBundle:
        if snapshot.official_cutoff != snapshot.pit_snapshot.cutoff:
            raise ArtifactReadySourceError("artifact-ready cutoff differs from PIT")
        if snapshot.official_cutoff != self.profile_cutoff(snapshot):
            raise ArtifactReadySourceError("artifact-ready snapshot cutoff is invalid")
        ensure_sha256(snapshot.source_content_root, field="source_content_root")
        if snapshot.pit_snapshot_digest != snapshot.pit_snapshot.spans_sha256:
            raise ArtifactReadySourceError("artifact-ready PIT digest differs")
        view = source_view or _SealedSnapshotView(self.cas, snapshot)
        checkpoint()
        trading_dates = self._trading_dates(view, snapshot.official_cutoff)
        suspended = self._full_day_suspensions(view)
        daily_entries, daily_provider, daily_derived, daily_summary, daily_overlay_keys = self._daily_entries(
            view,
            snapshot=snapshot,
            trading_dates=trading_dates,
            suspended=suspended,
            checkpoint=checkpoint,
        )
        factor_overlay_entry, factor_overlay_ref, factor_overlay_summary = self._factor_overlay_coverage(view)
        (
            adj_entries,
            adj_provider,
            adj_derived,
            qfq_summary,
            qfq_authority_ref,
        ) = self._adj_factor_entries(
            view,
            snapshot=snapshot,
            checkpoint=checkpoint,
            daily_overlay_keys=daily_overlay_keys,
        )
        minute_entries, minute_provider, minute_derived, minute_summary = self._minute_entries(
            view,
            snapshot=snapshot,
            trading_dates=trading_dates,
            suspended=suspended,
            checkpoint=checkpoint,
        )
        index_entries, index_provider, index_derived = self._index_entries(
            view,
            cutoff=snapshot.official_cutoff,
            trading_dates=trading_dates,
            checkpoint=checkpoint,
        )
        provider_refs = _dedupe_refs((*daily_provider, *adj_provider, *minute_provider, *index_provider))
        derived_refs = _dedupe_refs(
            (
                *adj_derived,
                *daily_derived,
                *minute_derived,
                *index_derived,
                qfq_authority_ref,
                factor_overlay_ref,
            )
        )
        component_refs: dict[str, CASRef] = {}
        for component in Component:
            raw = self._raw_entries(view, component)
            derived: Sequence[Mapping[str, Any]] = ()
            details: Mapping[str, Any] = {}
            if component in {
                Component.DAILY_BIN,
                Component.MINUTE_BIN,
                Component.FACTOR_H5_STATIC,
            }:
                derived = (*daily_entries, *adj_entries)
                details = {
                    "qfq_source_summary": qfq_summary,
                    "qfq_denominator_authority_ref": qfq_authority_ref.as_dict(),
                    "daily_provider_summary": daily_summary,
                }
            if component is Component.DAILY_BIN:
                derived = (*derived, *index_entries)
            if component is Component.FACTOR_H5_STATIC:
                derived = (*derived, factor_overlay_entry)
                details = {
                    **details,
                    "overlay_summary": factor_overlay_summary,
                }
            if component is Component.MINUTE_BIN:
                derived = (*derived, *minute_entries)
                details = {**details, "minute_coverage": minute_summary}
            elif component is Component.DOMESTIC_INDEX_CONTEXT:
                derived = index_entries
            component_refs[component.value] = self._seal_component_manifest(
                component,
                source_content_root=snapshot.source_content_root,
                partitions=(*raw, *derived),
                details=details,
            )
        body = {
            "schema_version": ARTIFACT_READY_CONTRACT_SCHEMA,
            "profile": self.profile.profile,
            "cutoff": snapshot.official_cutoff.isoformat(),
            "source_content_root": snapshot.source_content_root,
            "pit_snapshot_digest": snapshot.pit_snapshot_digest,
            "static_schema_digest": self.profile.static_schema_digest,
            "qfq_denominator_authority_ref": qfq_authority_ref.as_dict(),
            "qfq_denominator_authority_digest": qfq_summary["qfq_denominator_authority_digest"],
            "qfq_source_summary": qfq_summary,
            "daily_provider_summary": daily_summary,
            "factor_overlay_summary": factor_overlay_summary,
            "factor_overlay_coverage_ref": factor_overlay_ref.as_dict(),
            "component_manifests": {key: component_refs[key].as_dict() for key in sorted(component_refs)},
            "provider_receipt_refs": [value.as_dict() for value in provider_refs],
            "derived_source_receipt_refs": [value.as_dict() for value in derived_refs],
            "source_recheck_partition_expectations": (_snapshot_recheck_partition_expectations(snapshot)),
            "safety": dict(_ZERO_SAFETY),
        }
        component_effective_roots = {
            key: ensure_sha256(
                str(self.cas.get_json(component_refs[key]).get("component_effective_content_root", "")),
                field=f"{key}_component_effective_content_root",
            )
            for key in sorted(component_refs)
        }
        content_root = digest_named_fields(
            ARTIFACT_READY_EFFECTIVE_SCHEMA,
            {
                "profile": self.profile.profile,
                "cutoff": snapshot.official_cutoff,
                "pit_snapshot_digest": snapshot.pit_snapshot_digest,
                "static_schema_digest": self.profile.static_schema_digest,
                "qfq_denominator_authority_digest": qfq_summary["qfq_denominator_authority_digest"],
                "component_effective_content_roots": component_effective_roots,
            },
        )
        provenance_root = digest_named_fields(ARTIFACT_READY_PROVENANCE_SCHEMA, body)
        contract_ref = self.cas.put_json(
            {
                **body,
                "artifact_ready_content_root": content_root,
                "artifact_ready_effective_content_root": content_root,
                "artifact_ready_provenance_root": provenance_root,
            }
        )
        self.cas.verify(contract_ref)
        checkpoint()
        return ArtifactReadySourceBundle(
            artifact_ready_contract_ref=contract_ref,
            artifact_ready_content_root=content_root,
            provider_receipt_refs=provider_refs,
            derived_source_receipt_refs=derived_refs,
        )

    @staticmethod
    def profile_cutoff(snapshot: ArtifactSnapshot) -> date:
        return snapshot.official_cutoff

    def verify_current_exact(
        self,
        loaded_contract: LoadedArtifactReadyContract,
        *,
        fresh_snapshot: ArtifactSnapshot,
        observed_at: datetime,
        execution_id: str,
        run_id: str,
        attempt_id: str,
        attempt_fence: int,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> ArtifactReadyRecheckResult:
        """Seal a probe after exact no-write DB partition hash comparison."""

        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ArtifactReadySourceRevised("source recheck time is naive")
        if (
            not _safe_identity(execution_id)
            or not _safe_identity(run_id)
            or not _safe_identity(attempt_id)
            or type(attempt_fence) is not int
            or attempt_fence <= 0
        ):
            raise ArtifactReadySourceRevised("source recheck ownership is invalid")
        contract = loaded_contract.payload
        initial_source_root = ensure_sha256(
            str(contract.get("source_content_root", "")),
            field="initial_source_content_root",
        )
        initial_pit_digest = ensure_sha256(
            str(contract.get("pit_snapshot_digest", "")),
            field="initial_pit_snapshot_digest",
        )
        if (
            fresh_snapshot.official_cutoff.isoformat() != contract.get("cutoff")
            or fresh_snapshot.source_content_root != initial_source_root
            or fresh_snapshot.pit_snapshot_digest != initial_pit_digest
            or fresh_snapshot.pit_snapshot.spans_sha256 != initial_pit_digest
        ):
            raise ArtifactReadySourceRevised("fresh exact source or PIT identity differs")
        component_roots = dict(loaded_contract.component_effective_content_roots)
        _effective, component_provenance = self._component_roots(contract)
        if _effective != component_roots:
            raise ArtifactReadySourceRevised("artifact-ready component roots changed during recheck")
        root = loaded_contract.artifact_ready_effective_content_root
        source_probe_key = digest_named_fields(
            ARTIFACT_READY_RECHECK_SCHEMA,
            {
                "artifact_ready_contract_ref": loaded_contract.reference.sha256,
                "artifact_ready_content_root": root,
                "initial_source_content_root": initial_source_root,
                "fresh_source_content_root": fresh_snapshot.source_content_root,
                "pit_snapshot_digest": initial_pit_digest,
                "effective_component_roots": component_roots,
                "execution_id": execution_id,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "attempt_fence": attempt_fence,
                "observed_at": observed_at,
            },
        )
        receipt = self.cas.put_json(
            {
                "schema_version": ARTIFACT_READY_RECHECK_SCHEMA,
                "profile": self.profile.profile,
                "artifact_ready_contract_ref": loaded_contract.reference.as_dict(),
                "artifact_ready_content_root": root,
                "initial_source_content_root": initial_source_root,
                "fresh_source_content_root": fresh_snapshot.source_content_root,
                "raw_source_changed": False,
                "pit_snapshot_digest": initial_pit_digest,
                "effective_component_roots": component_roots,
                "initial_component_provenance_roots": component_provenance,
                "fresh_component_provenance_roots": component_provenance,
                "fresh_artifact_ready_contract_ref": (loaded_contract.reference.as_dict()),
                "observed_at": _timestamp(observed_at),
                "valid_until": _timestamp(
                    observed_at + timedelta(seconds=self.profile.source_content_probe_ttl_seconds)
                ),
                "execution_id": execution_id,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "attempt_fence": attempt_fence,
                "source_probe_key": source_probe_key,
                "status": "PASS",
                "freshness_authority": ("fresh_db_readback_plus_immutable_provider_overlay_v1"),
                "provider_recheck_policy": "no_provider_refetch_v1",
                "source_recheck_scan_policy": "exact_partition_hash_only_v1",
                "safety": dict(_ZERO_SAFETY),
            }
        )
        checkpoint()
        return ArtifactReadyRecheckResult(
            source_probe_key=source_probe_key,
            source_probe_ref=self.cas.verify(receipt),
            artifact_ready_content_root=root,
        )

    def verify_current(
        self,
        bundle_or_ref: ArtifactReadySourceBundle | CASRef | Mapping[str, Any] | str,
        *,
        fresh_snapshot: ArtifactSnapshot,
        observed_at: datetime,
        execution_id: str,
        run_id: str,
        attempt_id: str,
        attempt_fence: int,
        source_view: ArtifactSourceView | None = None,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> ArtifactReadyRecheckResult:
        """Rebuild effective roots from fresh DB rows and immutable overlays.

        The supplied ``fresh_snapshot``/view must be produced by the separately
        supervised source-recheck child.  Provider evidence is replayed only
        from the original CAS; this method never refetches TDX or Tushare.
        """

        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ArtifactReadySourceError("artifact-ready recheck time is naive")
        if (
            not _safe_identity(execution_id)
            or not _safe_identity(run_id)
            or not _safe_identity(attempt_id)
            or type(attempt_fence) is not int
            or attempt_fence <= 0
        ):
            raise ArtifactReadySourceError("artifact-ready recheck ownership is invalid")
        reference = (
            bundle_or_ref.artifact_ready_contract_ref
            if isinstance(bundle_or_ref, ArtifactReadySourceBundle)
            else self.cas.verify(bundle_or_ref)
        )
        contract = self.cas.get_json_bounded(reference, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
        if not isinstance(contract, Mapping):
            raise ArtifactReadySourceError("artifact-ready contract is not an object")
        initial_source_root = ensure_sha256(
            str(contract.get("source_content_root", "")),
            field="initial_source_content_root",
        )
        initial_pit_digest = ensure_sha256(
            str(contract.get("pit_snapshot_digest", "")),
            field="initial_pit_snapshot_digest",
        )
        root = self._validate_contract(
            contract,
            expected_source_content_root=initial_source_root,
            expected_pit_snapshot_digest=initial_pit_digest,
        )
        if (
            fresh_snapshot.official_cutoff.isoformat() != contract.get("cutoff")
            or fresh_snapshot.pit_snapshot_digest != initial_pit_digest
            or fresh_snapshot.pit_snapshot.spans_sha256 != initial_pit_digest
        ):
            raise ArtifactReadySourceError("fresh artifact-ready cutoff or PIT identity differs")
        durable_roots, durable_provenance = self._component_roots(contract)
        replay = _FrozenProviderReplay(self.cas, contract.get("provider_receipt_refs") or ())
        fresh_builder = ArtifactReadySourceBuilder(
            self.profile,
            self.cas,
            fetch_tdx_rows=replay.fetch_tdx_rows,
            fetch_tushare_minute_rows=replay.fetch_tushare_minute_rows,
            fetch_tushare_index_rows=replay.fetch_tushare_index_rows,
            fetch_tushare_adj_factor_rows=replay.fetch_tushare_adj_factor_rows,
            fetch_tushare_daily_rows=replay.fetch_tushare_daily_rows,
        )
        fresh_bundle = fresh_builder.build(
            fresh_snapshot,
            source_view=source_view,
            checkpoint=checkpoint,
        )
        fresh_contract = self.cas.get_json_bounded(
            fresh_bundle.artifact_ready_contract_ref,
            max_bytes=MAX_CONTROL_RECEIPT_BYTES,
        )
        if not isinstance(fresh_contract, Mapping):
            raise ArtifactReadySourceError("fresh artifact-ready contract is invalid")
        fresh_roots, fresh_provenance = self._component_roots(fresh_contract)
        if fresh_roots != durable_roots:
            raise ArtifactReadySourceError("fresh effective DB plus immutable overlay roots differ")
        source_probe_key = digest_named_fields(
            ARTIFACT_READY_RECHECK_SCHEMA,
            {
                "artifact_ready_contract_ref": reference.sha256,
                "artifact_ready_content_root": root,
                "initial_source_content_root": initial_source_root,
                "fresh_source_content_root": fresh_snapshot.source_content_root,
                "pit_snapshot_digest": initial_pit_digest,
                "effective_component_roots": fresh_roots,
                "execution_id": execution_id,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "attempt_fence": attempt_fence,
                "observed_at": observed_at,
            },
        )
        receipt = self.cas.put_json(
            {
                "schema_version": ARTIFACT_READY_RECHECK_SCHEMA,
                "profile": self.profile.profile,
                "artifact_ready_contract_ref": reference.as_dict(),
                "artifact_ready_content_root": root,
                "initial_source_content_root": initial_source_root,
                "fresh_source_content_root": fresh_snapshot.source_content_root,
                "raw_source_changed": (fresh_snapshot.source_content_root != initial_source_root),
                "pit_snapshot_digest": initial_pit_digest,
                "effective_component_roots": fresh_roots,
                "initial_component_provenance_roots": durable_provenance,
                "fresh_component_provenance_roots": fresh_provenance,
                "fresh_artifact_ready_contract_ref": (fresh_bundle.artifact_ready_contract_ref.as_dict()),
                "observed_at": _timestamp(observed_at),
                "valid_until": _timestamp(
                    observed_at + timedelta(seconds=self.profile.source_content_probe_ttl_seconds)
                ),
                "execution_id": execution_id,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "attempt_fence": attempt_fence,
                "source_probe_key": source_probe_key,
                "status": "PASS",
                "freshness_authority": ("fresh_db_readback_plus_immutable_provider_overlay_v1"),
                "provider_recheck_policy": "no_provider_refetch_v1",
                "safety": dict(_ZERO_SAFETY),
            }
        )
        return ArtifactReadyRecheckResult(
            source_probe_key=source_probe_key,
            source_probe_ref=self.cas.verify(receipt),
            artifact_ready_content_root=root,
        )

    def _component_roots(self, contract: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
        components = contract.get("component_manifests")
        expected = {item.value for item in Component}
        if not isinstance(components, Mapping) or set(components) != expected:
            raise ArtifactReadySourceError("artifact-ready component manifests are incomplete")
        effective: dict[str, str] = {}
        provenance: dict[str, str] = {}
        for component, raw_ref in components.items():
            reference = _complete_ref(self.cas, raw_ref, field=f"{component} component manifest")
            manifest = self.cas.get_json_bounded(reference, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
            if (
                not isinstance(manifest, Mapping)
                or manifest.get("schema_version") != ARTIFACT_READY_COMPONENT_SCHEMA
                or manifest.get("component") != component
            ):
                raise ArtifactReadySourceError("artifact-ready component manifest differs during recheck")
            effective[component] = ensure_sha256(
                str(manifest.get("component_effective_content_root", "")),
                field="component_effective_content_root",
            )
            provenance[component] = ensure_sha256(
                str(manifest.get("component_provenance_root", "")),
                field="component_provenance_root",
            )
        return effective, provenance

    def _validate_contract(
        self,
        value: Any,
        *,
        expected_source_content_root: str,
        expected_pit_snapshot_digest: str,
    ) -> str:
        if not isinstance(value, Mapping):
            raise ArtifactReadySourceError("artifact-ready contract is not an object")
        expected_components = {item.value for item in Component}
        components = value.get("component_manifests")
        if (
            value.get("schema_version") != ARTIFACT_READY_CONTRACT_SCHEMA
            or value.get("profile") != self.profile.profile
            or value.get("source_content_root") != expected_source_content_root
            or value.get("pit_snapshot_digest") != expected_pit_snapshot_digest
            or value.get("static_schema_digest") != self.profile.static_schema_digest
            or not isinstance(components, Mapping)
            or set(components) != expected_components
            or value.get("safety") != _ZERO_SAFETY
        ):
            raise ArtifactReadySourceError("artifact-ready contract identity differs")
        for raw in (
            *components.values(),
            *(value.get("provider_receipt_refs") or ()),
            *(value.get("derived_source_receipt_refs") or ()),
            value.get("qfq_denominator_authority_ref"),
        ):
            self.cas.verify(_complete_ref(self.cas, raw, field="artifact-ready ref"))
        qfq_ref = _complete_ref(
            self.cas,
            value.get("qfq_denominator_authority_ref"),
            field="qfq denominator authority",
        )
        qfq_payload = self.cas.get_json_bounded(qfq_ref, max_bytes=MAX_CONTROL_RECEIPT_BYTES)
        if not isinstance(qfq_payload, Mapping):
            raise ArtifactReadySourceError("QFQ authority receipt is invalid")
        authority = qfq_denominator_authority_from_mapping(
            qfq_payload,
            expected_cutoff=date.fromisoformat(str(value.get("cutoff"))),
            expected_pit_spans_sha256=expected_pit_snapshot_digest,
        )
        if value.get("qfq_denominator_authority_digest") != authority.digest:
            raise ArtifactReadySourceError("QFQ authority identity differs")
        body = dict(value)
        actual_root = str(body.pop("artifact_ready_content_root", ""))
        explicit_effective = str(body.pop("artifact_ready_effective_content_root", ""))
        actual_provenance = str(body.pop("artifact_ready_provenance_root", ""))
        expected_provenance = digest_named_fields(ARTIFACT_READY_PROVENANCE_SCHEMA, body)
        component_roots, _provenance_roots = self._component_roots(value)
        expected_root = digest_named_fields(
            ARTIFACT_READY_EFFECTIVE_SCHEMA,
            {
                "profile": value.get("profile"),
                "cutoff": date.fromisoformat(str(value.get("cutoff"))),
                "pit_snapshot_digest": expected_pit_snapshot_digest,
                "static_schema_digest": value.get("static_schema_digest"),
                "qfq_denominator_authority_digest": authority.digest,
                "component_effective_content_roots": component_roots,
            },
        )
        if (
            actual_root != expected_root
            or explicit_effective != expected_root
            or actual_provenance != expected_provenance
        ):
            raise ArtifactReadySourceError("artifact-ready root identity differs")
        return actual_root

    def _trading_dates(self, view: ArtifactSourceView, cutoff: date) -> tuple[date, ...]:
        dates: list[date] = []
        for descriptor in view.descriptors("trading_calendar"):
            with _managed_partition_rows(view, descriptor) as rows:
                for row in rows:
                    if bool(row.get("is_trading")):
                        observed = _as_date(row.get("cal_date"), field="cal_date")
                        if observed <= cutoff:
                            dates.append(observed)
        result = tuple(sorted(set(dates)))
        if not result or len(result) != len(dates):
            raise ArtifactReadyCoverageIncomplete("trading calendar is empty or duplicated")
        return result

    def _full_day_suspensions(self, view: ArtifactSourceView) -> frozenset[tuple[str, date]]:
        suspended: set[tuple[str, date]] = set()
        for descriptor in view.descriptors("suspend_d"):
            with _managed_partition_rows(view, descriptor) as rows:
                for row in rows:
                    if str(row.get("suspend_type", "")).upper() != "S":
                        continue
                    timing = row.get("suspend_timing")
                    if timing is None or not str(timing).strip():
                        suspended.add(
                            (
                                str(row.get("ts_code", "")).upper(),
                                _as_date(
                                    row.get("trade_date"),
                                    field="suspend trade_date",
                                ),
                            )
                        )
        return frozenset(suspended)

    def _daily_entries(
        self,
        view: ArtifactSourceView,
        *,
        snapshot: ArtifactSnapshot,
        trading_dates: Sequence[date],
        suspended: frozenset[tuple[str, date]],
        checkpoint: Callable[[], None],
    ) -> tuple[
        tuple[Mapping[str, Any], ...],
        tuple[CASRef, ...],
        tuple[CASRef, ...],
        Mapping[str, Any],
        frozenset[tuple[str, date]],
    ]:
        """Fill historical D/P daily bars candidate-locally from Tushare.

        Only canonical v2 uses this path. The scan retains rows for historical
        D/P codes only, so memory is bounded by that subset rather than the
        full market panel. Provider rows may fill missing keys but can never
        override a database key.
        """

        if self.profile.pit_authority_status != "ACTIVE_CANONICAL":
            return (), (), (), {
                "source_precedence": "legacy_database_only_v1",
                "historical_terminal_codes": 0,
                "provider_fill_rows": 0,
                "provider_override_rows": 0,
            }, frozenset()
        terminal_codes: set[str] = set()
        for descriptor in view.descriptors("stock_basic"):
            with _managed_partition_rows(view, descriptor) as rows:
                for row in rows:
                    if str(row.get("list_status", "")).upper() in {"D", "P"}:
                        terminal_codes.add(str(row.get("ts_code", "")).upper())
        if not terminal_codes:
            raise ArtifactReadyCoverageIncomplete("canonical daily overlay found no historical D/P securities")

        descriptors = sorted(
            view.descriptors("kline_daily_raw"),
            key=lambda item: str(item.get("partition_key", "")),
        )
        partition_ranges: dict[str, tuple[date, date]] = {}
        for descriptor in descriptors:
            key = str(descriptor.get("partition_key", ""))
            match = _DATE_PARTITION.fullmatch(key)
            if match is None:
                raise ArtifactReadyCoverageIncomplete("daily partition identity is invalid")
            partition_ranges[key] = (
                date.fromisoformat(match.group("start")),
                min(snapshot.official_cutoff, date.fromisoformat(match.group("end"))),
            )
        expected_by_partition: dict[str, set[tuple[str, date]]] = {key: set() for key in partition_ranges}
        trading = tuple(trading_dates)
        partition_for_day: dict[date, str] = {}
        for day in trading:
            matches = [key for key, (start, end) in partition_ranges.items() if start <= day <= end]
            if len(matches) != 1:
                raise ArtifactReadyCoverageIncomplete("daily trading day has no unique source partition")
            partition_for_day[day] = matches[0]
        for span in snapshot.pit_snapshot.spans:
            code = str(span.ts_code).upper()
            if code not in terminal_codes:
                continue
            left = bisect_left(trading, span.eligible_start)
            right = bisect_right(trading, span.eligible_end)
            for day in trading[left:right]:
                if (code, day) in suspended:
                    continue
                expected_by_partition[partition_for_day[day]].add((code, day))

        database_by_code: dict[str, dict[date, dict[str, Any]]] = {code: {} for code in terminal_codes}
        database_keys_by_partition: dict[str, set[tuple[str, date]]] = {key: set() for key in partition_ranges}
        for descriptor in descriptors:
            with _managed_partition_rows(view, descriptor) as rows:
                for raw in rows:
                    code = str(raw.get("ts_code", "")).upper()
                    if code not in terminal_codes:
                        continue
                    day = _as_date(raw.get("trade_date"), field="daily database trade_date")
                    if day in database_by_code[code]:
                        raise ArtifactReadyCoverageIncomplete("historical D/P daily database key is duplicated")
                    database_by_code[code][day] = _portable_daily_row(raw)
                    partition_key = partition_for_day.get(day)
                    if partition_key is not None:
                        database_keys_by_partition[partition_key].add((code, day))

        missing_by_partition = {
            key: expected.difference(database_keys_by_partition[key])
            for key, expected in expected_by_partition.items()
        }
        missing_codes = sorted({code for values in missing_by_partition.values() for code, _day in values})
        overlay_by_partition: dict[str, list[dict[str, Any]]] = {key: [] for key in partition_ranges}
        provider_refs: list[CASRef] = []
        overlap_rows_verified = 0
        provider_rows_observed = 0
        for code in missing_codes:
            required = sorted(
                day
                for values in missing_by_partition.values()
                for missing_code, day in values
                if missing_code == code
            )
            try:
                rows = tuple(
                    self._provider_call(
                        self.fetch_tushare_daily_rows,
                        code,
                        min(required),
                        max(required),
                    )
                )
            except Exception as exc:
                self._raise_provider_failure(
                    exc,
                    code=ArtifactReadyCoverageIncomplete.code,
                    stage="daily_provider",
                    subject=code,
                )
            normalized: dict[date, dict[str, Any]] = {}
            for raw in rows:
                row = _portable_daily_row(raw)
                if row["ts_code"] != code:
                    raise ArtifactReadyCoverageIncomplete("daily provider returned a different code")
                day = _as_date(row["trade_date"], field="daily provider trade_date")
                if day in normalized:
                    raise ArtifactReadyCoverageIncomplete("daily provider key is duplicated")
                normalized[day] = row
                database_row = database_by_code[code].get(day)
                if database_row is not None:
                    if _daily_value_tuple(database_row) != _daily_value_tuple(row):
                        self._raise_provider_failure(
                            ArtifactReadyCoverageIncomplete("daily provider/database overlap differs"),
                            code="BLOCKED_DAILY_PROVIDER_CONFLICT",
                            stage="daily_overlap",
                            subject=f"{code}:{day.isoformat()}",
                        )
                    overlap_rows_verified += 1
            provider_rows_observed += len(normalized)
            required_set = set(required)
            relevant_rows = [
                row
                for day, row in sorted(normalized.items())
                if day in required_set or day in database_by_code[code]
            ]
            provider_ref = self.cas.put_json(
                {
                    "schema_version": "dataset_release_daily_provider_snapshot_v1",
                    "provider": "tushare_pro_bar",
                    "ts_code": code,
                    "start": min(required).isoformat(),
                    "end": max(required).isoformat(),
                    "rows": relevant_rows,
                    "safety": dict(_ZERO_SAFETY),
                }
            )
            provider_refs.append(provider_ref)
            for partition_key, missing in missing_by_partition.items():
                for missing_code, day in sorted(missing):
                    if missing_code != code:
                        continue
                    row = normalized.get(day)
                    if row is None:
                        continue
                    overlay_by_partition[partition_key].append(row)
            checkpoint()

        entries: list[Mapping[str, Any]] = []
        derived_refs: list[CASRef] = []
        unresolved: list[tuple[str, date]] = []
        for descriptor in descriptors:
            partition_key = str(descriptor["partition_key"])
            overlay_rows = sorted(
                overlay_by_partition[partition_key],
                key=lambda row: (str(row["ts_code"]), str(row["trade_date"])),
            )
            if len(overlay_rows) > MAX_DAILY_OVERLAY_ROWS_PER_PARTITION:
                raise ArtifactReadyCoverageIncomplete("daily provider overlay partition exceeds row bound")
            overlay_keys = {(row["ts_code"], _as_date(row["trade_date"], field="daily overlay date")) for row in overlay_rows}
            unresolved.extend(sorted(missing_by_partition[partition_key].difference(overlay_keys)))
            body = {
                "schema_version": ARTIFACT_READY_DAILY_COVERAGE_SCHEMA,
                "raw_partition_identity": _identity(descriptor),
                "partition_key": partition_key,
                "overlay_rows": overlay_rows,
                "expected_terminal_keys": len(expected_by_partition[partition_key]),
                "provider_fill_rows": len(overlay_rows),
                "provider_override_rows": 0,
                "safety": dict(_ZERO_SAFETY),
            }
            content_root = digest_named_fields(ARTIFACT_READY_DAILY_COVERAGE_SCHEMA, body)
            reference = self.cas.put_json({**body, "effective_content_root": content_root})
            derived_refs.append(reference)
            entries.append(
                _derived_entry(
                    dataset="daily_coverage",
                    partition_key=partition_key,
                    role="database_tushare_missing_only",
                    row_count=len(overlay_rows),
                    reference=reference,
                    content_digest=content_root,
                )
            )
        if unresolved:
            sample = [(code, day.isoformat()) for code, day in unresolved[:20]]
            self._raise_provider_failure(
                ArtifactReadyCoverageIncomplete("Tushare daily left historical D/P keys missing"),
                code=ArtifactReadyCoverageIncomplete.code,
                stage="daily_coverage",
                subject=json.dumps({"count": len(unresolved), "sample": sample}, sort_keys=True),
            )
        summary = {
            "source_precedence": "database_then_tushare_pro_bar_missing_only_v1",
            "historical_terminal_codes": len(terminal_codes),
            "provider_requested_codes": len(missing_codes),
            "provider_rows_observed": provider_rows_observed,
            "provider_fill_rows": sum(len(rows) for rows in overlay_by_partition.values()),
            "provider_override_rows": 0,
            "overlap_rows_verified": overlap_rows_verified,
            "unresolved_keys": 0,
        }
        overlay_keys = frozenset(
            (str(row["ts_code"]), _as_date(row["trade_date"], field="daily overlay date"))
            for rows in overlay_by_partition.values()
            for row in rows
        )
        return tuple(entries), _dedupe_refs(provider_refs), _dedupe_refs(derived_refs), summary, overlay_keys

    def _factor_overlay_coverage(self, view: ArtifactSourceView) -> tuple[Mapping[str, Any], CASRef, Mapping[str, Any]]:
        """Prove sparse factor auxiliaries remain DB-authoritative and unmodified."""

        datasets = (
            "daily_basic",
            "moneyflow_ts",
            "bak_basic",
            "cyq_perf",
            "sector_data",
            "margin_detail",
        )
        coverage: dict[str, Any] = {}
        database_rows = 0
        for dataset in datasets:
            descriptors = view.descriptors(dataset)
            if not descriptors:
                raise ArtifactReadyCoverageIncomplete(f"factor overlay source is missing: {dataset}")
            rows = sum(int(item.get("row_count", -1)) for item in descriptors)
            if rows < 0:
                raise ArtifactReadyCoverageIncomplete(f"factor overlay source row count is invalid: {dataset}")
            database_rows += rows
            coverage[dataset] = {
                "partition_count": len(descriptors),
                "database_rows": rows,
                "partition_content_root": digest_named_fields(
                    "dataset_release_factor_aux_database_partitions_v1",
                    {
                        "dataset": dataset,
                        "partitions": [
                            {
                                "partition_key": str(item.get("partition_key")),
                                "row_count": int(item.get("row_count", -1)),
                                "content_digest": ensure_sha256(
                                    str(item.get("content_digest", "")),
                                    field=f"{dataset}_content_digest",
                                ),
                                "schema_digest": ensure_sha256(
                                    str(item.get("schema_digest", "")),
                                    field=f"{dataset}_schema_digest",
                                ),
                            }
                            for item in descriptors
                        ],
                    },
                ),
            }
        body = {
            "schema_version": ARTIFACT_READY_FACTOR_OVERLAY_SCHEMA,
            "coverage_authority": ("sealed_sparse_database_partitions_no_synthetic_fill_v1"),
            "source_precedence": ("database_then_provider_missing_keys_conflict_fail_v1"),
            "datasets": coverage,
            "database_rows": database_rows,
            "provider_fill_rows": 0,
            "provider_override_rows": 0,
            "overlap_rows_verified": 0,
            "overlap_mismatch_cells": 0,
            "safety": dict(_ZERO_SAFETY),
        }
        content_root = digest_named_fields(ARTIFACT_READY_FACTOR_OVERLAY_SCHEMA, body)
        reference = self.cas.put_json({**body, "effective_content_root": content_root})
        summary = {key: value for key, value in body.items() if key not in {"schema_version", "datasets", "safety"}}
        summary["coverage_ref"] = reference.as_dict()
        summary["effective_content_root"] = content_root
        entry = _derived_entry(
            dataset="factor_aux_overlay_coverage",
            partition_key="all",
            role="sealed_database_sparse_no_fill",
            row_count=database_rows,
            reference=reference,
            content_digest=content_root,
        )
        return entry, self.cas.verify(reference), summary

    def _adj_factor_entries(
        self,
        view: ArtifactSourceView,
        *,
        snapshot: ArtifactSnapshot,
        checkpoint: Callable[[], None],
        daily_overlay_keys: frozenset[tuple[str, date]] = frozenset(),
    ) -> tuple[
        tuple[Mapping[str, Any], ...],
        tuple[CASRef, ...],
        tuple[CASRef, ...],
        Mapping[str, Any],
        CASRef,
    ]:
        """Seal candidate-only adj-factor gaps and one common QFQ authority."""

        descriptors = sorted(
            view.descriptors("adj_factor"),
            key=lambda item: str(item.get("partition_key", "")),
        )
        if not descriptors:
            raise ArtifactReadyCoverageIncomplete("sealed adj_factor partitions are missing")
        codes = sorted({span.ts_code for span in snapshot.pit_snapshot.spans})
        entries: list[Mapping[str, Any]] = []
        provider_refs: list[CASRef] = []
        derived_refs: list[CASRef] = []
        maxima: dict[str, Decimal] = {}
        series_hashers = {code: hashlib.sha256() for code in codes}
        series_counts = {code: 0 for code in codes}
        series_last_dates: dict[str, date] = {}
        totals = {
            "database_rows": 0,
            "provider_fill_rows": 0,
            "provider_override_rows": 0,
            "overlap_rows_verified": 0,
            "overlap_mismatch_cells": 0,
        }
        previous_end: date | None = None
        for descriptor in descriptors:
            match = _DATE_PARTITION.fullmatch(str(descriptor.get("partition_key", "")))
            if match is None:
                raise ArtifactReadyCoverageIncomplete("adj_factor partition identity is invalid")
            start = date.fromisoformat(match.group("start"))
            end = min(snapshot.official_cutoff, date.fromisoformat(match.group("end")))
            if end < start or (previous_end is not None and start <= previous_end):
                raise ArtifactReadyCoverageIncomplete("adj_factor partitions overlap or regress")
            previous_end = end
            daily_keys = self._daily_stock_keys(
                view,
                start=start,
                end=end,
                codes=frozenset(codes),
            )
            daily_keys = frozenset(
                set(daily_keys).union(
                    key
                    for key in daily_overlay_keys
                    if key[0] in codes and start <= key[1] <= end
                )
            )
            expected_codes_by_day: dict[date, list[str]] = {}
            for code, day in sorted(daily_keys):
                expected_codes_by_day.setdefault(day, []).append(code)
            expected_by_day = {day: tuple(day_codes) for day, day_codes in expected_codes_by_day.items()}
            expected = {(code, day) for day, day_codes in expected_by_day.items() for code in day_codes}
            database: dict[tuple[str, date], Decimal] = {}
            with _managed_partition_rows(view, descriptor) as rows:
                for raw in rows:
                    row = _normalize_adj_factor_row(raw, source="database")
                    key = (row["ts_code"], row["trade_date"])
                    if key[1] < start or key[1] > end:
                        raise ArtifactReadyCoverageIncomplete("database adj_factor row is outside its partition")
                    if key[0] not in series_hashers:
                        continue
                    if key in database:
                        raise ArtifactReadyMinuteConflict("database adj_factor key is duplicated")
                    database[key] = row["adj_factor"]
            provider_fill: dict[tuple[str, date], Decimal] = {}
            provider_ref_by_key: dict[tuple[str, date], CASRef] = {}
            overlap_verified = 0
            for day, day_codes in expected_by_day.items():
                missing = {code for code in day_codes if (code, day) not in database}
                if not missing:
                    continue
                try:
                    raw_provider = self._provider_call(self.fetch_tushare_adj_factor_rows, day)
                except Exception as exc:
                    self._raise_provider_failure(
                        exc,
                        code=(
                            "BLOCKED_PROVIDER_TERMINAL_40203"
                            if _is_40203(exc)
                            else ArtifactReadyCoverageIncomplete.code
                        ),
                        stage="adj_factor_provider",
                        subject=day.isoformat(),
                    )
                provider: dict[tuple[str, date], Decimal] = {}
                for raw in raw_provider:
                    row = _normalize_adj_factor_row(raw, source="tushare")
                    key = (row["ts_code"], row["trade_date"])
                    if key[1] != day or key[0] not in series_hashers:
                        continue
                    if key in provider:
                        raise ArtifactReadyCoverageIncomplete("Tushare adj_factor key is duplicated")
                    provider[key] = row["adj_factor"]
                provider_ref = self.cas.put_json(
                    {
                        "schema_version": "dataset_release_adj_factor_provider_snapshot_v1",
                        "provider": "tushare",
                        "trade_date": day.isoformat(),
                        "rows": [_portable_adj_factor_value(key, value) for key, value in sorted(provider.items())],
                        "safety": dict(_ZERO_SAFETY),
                    }
                )
                provider_refs.append(provider_ref)
                for key, provider_value in provider.items():
                    if key in database:
                        overlap_verified += 1
                        if database[key] != provider_value:
                            self._raise_provider_failure(
                                ArtifactReadyMinuteConflict("adj_factor DB/provider overlap differs"),
                                code="BLOCKED_ADJ_FACTOR_PROVIDER_CONFLICT",
                                stage="adj_factor_overlap",
                                subject=f"{key[0]}:{day.isoformat()}",
                            )
                    elif key not in provider_fill:
                        provider_fill[key] = provider_value
                        provider_ref_by_key[key] = provider_ref
                for code in missing:
                    if (code, day) not in provider_fill:
                        self._raise_provider_failure(
                            ArtifactReadyCoverageIncomplete("Tushare adj_factor left required keys missing"),
                            code=ArtifactReadyCoverageIncomplete.code,
                            stage="adj_factor_coverage",
                            subject=f"{code}:{day.isoformat()}",
                        )
            effective = {**database, **provider_fill}
            if not expected.issubset(effective):
                raise ArtifactReadyCoverageIncomplete("adj_factor effective coverage is incomplete")
            for (code, day), value in sorted(effective.items()):
                previous = series_last_dates.get(code)
                if previous is not None and day <= previous:
                    raise ArtifactReadyCoverageIncomplete("adj_factor effective series order regressed")
                series_last_dates[code] = day
                maxima[code] = max(value, maxima.get(code, value))
                series_counts[code] += 1
                series_hashers[code].update(_qfq_canonical_row_bytes(code, day, value))
            effective_rows = [_portable_adj_factor_value(key, value) for key, value in sorted(effective.items())]
            effective_root = digest_named_fields(
                "dataset_release_adj_factor_effective_partition_v1",
                {"partition_key": str(descriptor["partition_key"]), "rows": effective_rows},
            )
            overlay_rows = [
                {
                    **_portable_adj_factor_value(key, value),
                    "provider_ref": provider_ref_by_key[key].as_dict(),
                }
                for key, value in sorted(provider_fill.items())
            ]
            monthly_leaves = _adj_monthly_leaves(effective)
            totals["database_rows"] += len(database)
            totals["provider_fill_rows"] += len(provider_fill)
            totals["overlap_rows_verified"] += overlap_verified
            receipt = {
                "schema_version": ARTIFACT_READY_ADJ_COVERAGE_SCHEMA,
                "raw_partition_identity": _identity(descriptor),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "expected_pit_rows": len(expected),
                "effective_rows": len(effective),
                "database_rows": len(database),
                "provider_fill_rows": len(provider_fill),
                "overlap_rows_verified": overlap_verified,
                "overlay_rows": overlay_rows,
                "effective_content_root": effective_root,
                "monthly_content_leaves": monthly_leaves,
                "source_precedence": "db_then_tushare_missing_keys_conflict_fail_v1",
                "provider_override_rows": 0,
                "overlap_mismatch_cells": 0,
                "safety": dict(_ZERO_SAFETY),
            }
            reference = self.cas.put_json(receipt)
            derived_refs.append(reference)
            entries.append(
                _derived_entry(
                    dataset="adj_factor_coverage",
                    partition_key=str(descriptor["partition_key"]),
                    role="database_tushare_missing_only",
                    row_count=len(effective),
                    reference=reference,
                    content_digest=effective_root,
                    monthly_content_leaves=monthly_leaves,
                    source_table_schema_digest=descriptor.get("source_table_schema_digest"),
                    source_code_membership_digest=descriptor.get("source_code_membership_digest"),
                    min_key=(
                        [effective_rows[0]["ts_code"], effective_rows[0]["trade_date"]] if effective_rows else None
                    ),
                    max_key=(
                        [effective_rows[-1]["ts_code"], effective_rows[-1]["trade_date"]] if effective_rows else None
                    ),
                )
            )
            checkpoint()
        missing_codes = sorted(set(codes).difference(maxima))
        if missing_codes:
            raise ArtifactReadyCoverageIncomplete("adj_factor cannot establish every PIT QFQ denominator")
        per_code_series = tuple((code, series_counts[code], series_hashers[code].hexdigest()) for code in codes)
        global_series_digest = digest_named_fields(
            "dataset_release_qfq_per_code_series_root_v1",
            {"per_code_series": per_code_series},
        )
        authority = QfqDenominatorAuthority(
            cutoff=snapshot.official_cutoff,
            pit_spans_sha256=snapshot.pit_snapshot_digest,
            values=tuple((code, float(maxima[code])) for code in codes),
            per_code_series=per_code_series,
            source_row_count=sum(series_counts.values()),
            source_rows_sha256=global_series_digest,
        )
        qfq_ref = self.cas.put_json(authority.as_dict())
        denominator_digest_by_code = {
            code: digest_named_fields(
                "dataset_release_qfq_code_denominator_v1",
                {
                    "ts_code": code,
                    "cutoff": snapshot.official_cutoff,
                    "denominator": float(maxima[code]),
                    "adj_row_count": series_counts[code],
                    "ordered_adj_series_sha256": series_hashers[code].hexdigest(),
                },
            )
            for code in codes
        }
        summary = {
            "source_precedence": "db_then_tushare_missing_keys_conflict_fail_v1",
            **totals,
            "qfq_authority_complete": True,
            "qfq_code_count": len(codes),
            "qfq_denominator_by_code": {code: float(maxima[code]) for code in codes},
            "qfq_denominator_digest_by_code": denominator_digest_by_code,
            "qfq_ordered_adj_digest_by_code": {code: series_hashers[code].hexdigest() for code in codes},
            "qfq_adj_row_count_by_code": dict(series_counts),
            "qfq_denominator_authority_ref": qfq_ref.as_dict(),
            "qfq_denominator_authority_digest": authority.digest,
        }
        return (
            tuple(entries),
            _dedupe_refs(provider_refs),
            _dedupe_refs(derived_refs),
            summary,
            self.cas.verify(qfq_ref),
        )

    def _daily_stock_keys(
        self,
        view: ArtifactSourceView,
        *,
        start: date,
        end: date,
        codes: frozenset[str],
    ) -> frozenset[tuple[str, date]]:
        """Return sealed daily price keys, including PIT-excluded intervals."""

        keys: set[tuple[str, date]] = set()
        for descriptor in view.descriptors("kline_daily_raw"):
            match = _DATE_PARTITION.fullmatch(str(descriptor.get("partition_key", "")))
            if match is not None:
                descriptor_start = date.fromisoformat(match.group("start"))
                descriptor_end = date.fromisoformat(match.group("end"))
                if descriptor_end < start or descriptor_start > end:
                    continue
            with _managed_partition_rows(view, descriptor) as rows:
                for row in rows:
                    code = str(row.get("ts_code", "")).upper()
                    day = _as_date(row.get("trade_date"), field="daily stock trade_date")
                    if code not in codes or not start <= day <= end:
                        continue
                    key = (code, day)
                    if key in keys:
                        raise ArtifactReadyCoverageIncomplete("sealed daily stock key is duplicated")
                    keys.add(key)
        return frozenset(keys)

    def _scan_minute_database_partition(
        self,
        view: ArtifactSourceView,
        descriptor: Mapping[str, Any],
        *,
        expected: Sequence[tuple[str, date]],
        suspended: frozenset[tuple[str, date]],
        total: dict[str, int],
        derived_refs: list[CASRef],
    ) -> tuple[
        dict[tuple[str, date], dict[str, Any]],
        dict[str, list[tuple[MinuteGap, Sequence[Mapping[str, Any]]]]],
    ]:
        coverage_by_key: dict[tuple[str, date], dict[str, Any]] = {}
        gaps_by_code: dict[
            str,
            list[tuple[MinuteGap, Sequence[Mapping[str, Any]]]],
        ] = {}
        with _managed_partition_rows(view, descriptor) as partition_rows:
            observed = _group_minute_rows(partition_rows)
            observed_item = next(observed, None)
            for key in expected:
                while observed_item is not None and observed_item[0] < key:
                    raise ArtifactReadyCoverageIncomplete("minute partition contains a row outside PIT/calendar scope")
                rows: Sequence[Mapping[str, Any]] = ()
                if observed_item is not None and observed_item[0] == key:
                    rows = observed_item[1]
                    observed_item = next(observed, None)
                total["expected_days"] += 1
                if key in suspended:
                    if rows:
                        raise ArtifactReadyCoverageIncomplete("full-day suspension has minute rows")
                    suspension_ref = self.cas.put_json(
                        {
                            "schema_version": ("dataset_release_minute_suspension_exemption_v1"),
                            "ts_code": key[0],
                            "trade_date": key[1].isoformat(),
                            "suspend_type": "S",
                            "suspend_timing": None,
                            "expected_bars": 0,
                            "database_writes": 0,
                            "production_writes": 0,
                        }
                    )
                    derived_refs.append(suspension_ref)
                    coverage_by_key[key] = {
                        "ts_code": key[0],
                        "trade_date": key[1].isoformat(),
                        "status": "SUSPENDED_FULL_DAY",
                        "database_rows": 0,
                        "final_rows": 0,
                        "effective_content_sha256": digest_named_fields(
                            "dataset_release_suspended_stock_day_v1",
                            {
                                "ts_code": key[0],
                                "trade_date": key[1],
                                "status": "SUSPENDED_FULL_DAY",
                            },
                        ),
                        "evidence_ref": suspension_ref.as_dict(),
                    }
                    total["suspended_full_day"] += 1
                    continue
                gap = MinuteGap(*key)
                try:
                    database = normalize_database_rows(rows, gap)
                except MinuteSourceConflict as exc:
                    self._raise_provider_failure(
                        exc,
                        code=ArtifactReadyMinuteConflict.code,
                        stage="minute_database",
                        subject=f"{key[0]}:{key[1].isoformat()}",
                    )
                if len(database) == gap.expected_bars:
                    effective_digest = _minute_effective_content_digest(gap, rows, ())
                    coverage_by_key[key] = {
                        "ts_code": key[0],
                        "trade_date": key[1].isoformat(),
                        "status": "DATABASE_COMPLETE",
                        "provider": "database_complete",
                        "database_rows": len(database),
                        "provider_rows": 0,
                        "overlay_rows": 0,
                        "final_rows": len(database),
                        "overlap_rows_verified": 0,
                        "effective_content_sha256": effective_digest,
                        "provider_ref": None,
                        "overlay_ref": None,
                        "attempts": [],
                    }
                    total["database_complete"] += 1
                else:
                    gaps_by_code.setdefault(key[0], []).append((gap, rows))
            if observed_item is not None:
                raise ArtifactReadyCoverageIncomplete("minute partition contains rows outside expected coverage")
        return coverage_by_key, gaps_by_code

    def _minute_entries(
        self,
        view: ArtifactSourceView,
        *,
        snapshot: ArtifactSnapshot,
        trading_dates: Sequence[date],
        suspended: frozenset[tuple[str, date]],
        checkpoint: Callable[[], None],
    ) -> tuple[
        tuple[Mapping[str, Any], ...],
        tuple[CASRef, ...],
        tuple[CASRef, ...],
        Mapping[str, Any],
    ]:
        descriptors = view.descriptors("kline_minute_raw")
        if not descriptors:
            raise ArtifactReadyCoverageIncomplete("sealed minute partitions are missing")
        provider_refs: list[CASRef] = []
        derived_refs: list[CASRef] = []
        entries: list[Mapping[str, Any]] = []
        total = {"expected_days": 0, "database_complete": 0, "provider_filled": 0, "suspended_full_day": 0}
        eligibility = _MinuteEligibilityIndex.build(
            trading_dates=trading_dates,
            spans=snapshot.pit_snapshot.spans,
            minute_start_date=self.profile.minute_start_date,
            bucket_count=self.profile.minute_code_bucket_count,
        )
        for descriptor in descriptors:
            match = _MINUTE_PARTITION.fullmatch(str(descriptor.get("partition_key", "")))
            if match is None:
                raise ArtifactReadyCoverageIncomplete("minute partition identity is invalid")
            start = date.fromisoformat(match.group("start"))
            end = date.fromisoformat(match.group("end"))
            bucket = int(match.group("bucket"))
            expected = tuple(
                eligibility.iter_expected(
                    bucket=bucket,
                    start=start,
                    end=end,
                )
            )
            coverage_by_key, gaps_by_code = self._scan_minute_database_partition(
                view,
                descriptor,
                expected=expected,
                suspended=suspended,
                total=total,
                derived_refs=derived_refs,
            )
            for code in sorted(gaps_by_code):
                requests = gaps_by_code[code]
                provider_calls: list[dict[str, Any]] = []

                def fetch_tdx(request_code: str, request_start: date, request_end: date) -> Sequence[Mapping[str, Any]]:
                    values = self._provider_call(
                        self.fetch_tdx_rows,
                        request_code,
                        request_start,
                        request_end,
                    )
                    provider_calls.append(
                        {
                            "provider": "tdx",
                            "start": request_start.isoformat(),
                            "end": request_end.isoformat(),
                            "rows": [_portable_json_row(item) for item in values],
                        }
                    )
                    return values

                def fetch_tushare(request_code: str, request_day: date) -> Sequence[Mapping[str, Any]]:
                    values = self._provider_call(
                        self.fetch_tushare_minute_rows,
                        request_code,
                        request_day,
                    )
                    canonical = _drop_tushare_auction_row(values, request_day)
                    provider_calls.append(
                        {
                            "provider": "tushare",
                            "start": request_day.isoformat(),
                            "end": request_day.isoformat(),
                            "rows": [_portable_json_row(item) for item in canonical],
                        }
                    )
                    return canonical

                overlay = MinuteOverlayBuilder(
                    fetch_tdx_rows=fetch_tdx,
                    fetch_tushare_rows=fetch_tushare,
                    policy=self.profile.resource_policy,
                    cas=None,
                )
                try:
                    results = tuple(overlay.iter_many(requests))
                except MinuteSourceConflict as exc:
                    self._raise_provider_failure(
                        exc,
                        code=ArtifactReadyMinuteConflict.code,
                        stage="minute_overlap",
                        subject=code,
                    )
                except MinuteProviderRateLimitTerminal as exc:
                    self._raise_provider_failure(
                        exc,
                        code="BLOCKED_PROVIDER_TERMINAL_40203",
                        stage="minute_provider",
                        subject=code,
                    )
                except MinuteProviderTerminal as exc:
                    self._raise_provider_failure(
                        exc,
                        code=ArtifactReadyCoverageIncomplete.code,
                        stage="minute_provider",
                        subject=code,
                    )
                first_day = requests[0][0].trade_date
                last_day = requests[-1][0].trade_date
                provider_ref = self.cas.put_json(
                    {
                        "schema_version": "dataset_release_minute_provider_window_v1",
                        "ts_code": code,
                        "start": first_day.isoformat(),
                        "end": last_day.isoformat(),
                        "calls": provider_calls,
                        "safety": dict(_ZERO_SAFETY),
                    }
                )
                provider_refs.append(provider_ref)
                overlay_rows = [_portable_json_row(row) for result in results for row in result.overlay_rows]
                overlay_ref = self.cas.put_json(
                    {
                        "schema_version": "dataset_release_minute_overlay_window_v1",
                        "ts_code": code,
                        "start": first_day.isoformat(),
                        "end": last_day.isoformat(),
                        "rows": overlay_rows,
                        "safety": dict(_ZERO_SAFETY),
                    }
                )
                derived_refs.append(overlay_ref)
                for result in results:
                    key = (result.ts_code, result.trade_date)
                    effective_digest = _minute_effective_content_digest(
                        MinuteGap(*key),
                        next(
                            database_rows
                            for request_gap, database_rows in requests
                            if request_gap.trade_date == result.trade_date
                        ),
                        result.overlay_rows,
                    )
                    coverage_by_key[key] = {
                        "ts_code": result.ts_code,
                        "trade_date": result.trade_date.isoformat(),
                        "status": "PROVIDER_FILLED",
                        "provider": result.provider,
                        "database_rows": result.database_rows,
                        "provider_rows": result.provider_rows,
                        "overlay_rows": len(result.overlay_rows),
                        "final_rows": result.database_rows + len(result.overlay_rows),
                        "overlap_rows_verified": result.overlap_rows_verified,
                        "overlay_content_sha256": result.overlay_content_sha256,
                        "effective_content_sha256": effective_digest,
                        "provider_ref": provider_ref.as_dict(),
                        "overlay_ref": overlay_ref.as_dict(),
                        "attempts": [item.as_dict() for item in result.attempts],
                    }
                    total["provider_filled"] += 1
            if set(coverage_by_key) != set(expected):
                raise ArtifactReadyCoverageIncomplete("minute partition coverage keys are incomplete")
            coverage = [coverage_by_key[key] for key in expected]
            effective_partition_root = digest_named_fields(
                "dataset_release_minute_effective_partition_v1",
                {
                    "partition_key": str(descriptor["partition_key"]),
                    "days": [
                        {
                            "ts_code": item["ts_code"],
                            "trade_date": item["trade_date"],
                            "effective_content_sha256": item["effective_content_sha256"],
                        }
                        for item in coverage
                    ],
                },
            )
            monthly_leaves = _effective_day_monthly_leaves(coverage)
            receipt = {
                "schema_version": ARTIFACT_READY_MINUTE_COVERAGE_SCHEMA,
                "raw_partition_identity": _identity(descriptor),
                "raw_partition_content_digest": descriptor.get("content_digest"),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "bucket": bucket,
                "days": coverage,
                "effective_content_root": effective_partition_root,
                "monthly_content_leaves": monthly_leaves,
                "summary": {
                    "expected_days": len(coverage),
                    "database_complete": sum(item["status"] == "DATABASE_COMPLETE" for item in coverage),
                    "provider_filled": sum(item["status"] == "PROVIDER_FILLED" for item in coverage),
                    "suspended_full_day": sum(item["status"] == "SUSPENDED_FULL_DAY" for item in coverage),
                },
                "safety": dict(_ZERO_SAFETY),
            }
            coverage_ref = self.cas.put_json(receipt)
            derived_refs.append(coverage_ref)
            entries.append(
                _derived_entry(
                    dataset="minute_coverage",
                    partition_key=str(descriptor["partition_key"]),
                    role="coverage_overlay",
                    row_count=len(coverage),
                    reference=coverage_ref,
                    content_digest=effective_partition_root,
                    monthly_content_leaves=monthly_leaves,
                    source_table_schema_digest=descriptor.get("source_table_schema_digest"),
                    source_code_membership_digest=descriptor.get("source_code_membership_digest"),
                    min_key=([coverage[0]["ts_code"], coverage[0]["trade_date"]] if coverage else None),
                    max_key=([coverage[-1]["ts_code"], coverage[-1]["trade_date"]] if coverage else None),
                )
            )
            checkpoint()
        if total["expected_days"] != sum(
            total[field] for field in ("database_complete", "provider_filled", "suspended_full_day")
        ):
            raise ArtifactReadyCoverageIncomplete("minute coverage is not closed")
        return (
            tuple(entries),
            _dedupe_refs(provider_refs),
            _dedupe_refs(derived_refs),
            total,
        )

    def _index_entries(
        self,
        view: ArtifactSourceView,
        *,
        cutoff: date,
        trading_dates: Sequence[date],
        checkpoint: Callable[[], None],
    ) -> tuple[tuple[Mapping[str, Any], ...], tuple[CASRef, ...], tuple[CASRef, ...]]:
        descriptors = view.descriptors("index_daily")
        if not descriptors:
            raise ArtifactReadyCoverageIncomplete("sealed index partitions are missing")
        entries: list[Mapping[str, Any]] = []
        provider_refs: list[CASRef] = []
        derived_refs: list[CASRef] = []
        definitions = {item.daily_code: item for item in DOMESTIC_INDEX_DEFINITIONS}
        for descriptor in descriptors:
            match = _DATE_PARTITION.fullmatch(str(descriptor.get("partition_key", "")))
            if match is None:
                raise ArtifactReadyCoverageIncomplete("index partition identity is invalid")
            start = date.fromisoformat(match.group("start"))
            end = min(cutoff, date.fromisoformat(match.group("end")))
            database_by_code: dict[str, list[Mapping[str, Any]]] = {code: [] for code in definitions}
            with _managed_partition_rows(view, descriptor) as rows:
                for row in rows:
                    code = str(row.get("ts_code", "")).upper()
                    if code not in database_by_code:
                        raise ArtifactReadyCoverageIncomplete("index partition has unknown code")
                    database_by_code[code].append(dict(row))
            merged_rows: list[Mapping[str, Any]] = []
            details: dict[str, Any] = {}
            for code, definition in definitions.items():
                expected_dates = tuple(
                    day for day in trading_dates if max(start, definition.required_from) <= day <= end
                )
                if not expected_dates:
                    for row in database_by_code[code]:
                        row_day = _as_date(row.get("trade_date"), field="index trade_date")
                        if row_day >= definition.required_from:
                            raise ArtifactReadyCoverageIncomplete(
                                f"index partition contains an unexpected calendar key: {code}:{row_day.isoformat()}"
                            )
                    continue
                expected_date_set = set(expected_dates)
                database: list[Mapping[str, Any]] = []
                for row in database_by_code[code]:
                    row_day = _as_date(row.get("trade_date"), field="index trade_date")
                    if row_day in expected_date_set:
                        database.append(row)
                    elif row_day >= definition.required_from:
                        raise ArtifactReadyCoverageIncomplete(
                            f"index partition contains an unexpected calendar key: {code}:{row_day.isoformat()}"
                        )
                database_keys = {_as_date(row.get("trade_date"), field="index trade_date") for row in database}
                provider: Sequence[Mapping[str, Any]] = ()
                if expected_date_set.difference(database_keys):
                    try:
                        provider = self._provider_call(
                            self.fetch_tushare_index_rows,
                            definition,
                            expected_dates[0],
                            expected_dates[-1],
                        )
                    except IndexProviderRateLimitTerminal as exc:
                        self._raise_provider_failure(
                            exc,
                            code="BLOCKED_PROVIDER_TERMINAL_40203",
                            stage="index_provider",
                            subject=code,
                        )
                    except Exception as exc:
                        self._raise_provider_failure(
                            exc,
                            code=ArtifactReadyCoverageIncomplete.code,
                            stage="index_provider",
                            subject=code,
                        )
                if provider:
                    provider_payload = {
                        "schema_version": "dataset_release_index_provider_snapshot_v1",
                        "provider": "tushare",
                        "ts_code": code,
                        "start": expected_dates[0].isoformat(),
                        "end": expected_dates[-1].isoformat(),
                        "rows": [_portable_index_row(row) for row in provider],
                        "safety": dict(_ZERO_SAFETY),
                    }
                    provider_ref = self.cas.put_json(provider_payload)
                    provider_refs.append(provider_ref)
                try:
                    merged, evidence = merge_index_rows_missing_only(database, provider)
                except IndexOverlapConflict as exc:
                    self._raise_provider_failure(
                        exc,
                        code=ArtifactReadyIndexConflict.code,
                        stage="index_overlap",
                        subject=code,
                    )
                merged_keys = {_as_date(row["trade_date"], field="merged index trade_date") for row in merged}
                if merged_keys != expected_date_set:
                    self._raise_provider_failure(
                        ArtifactReadyCoverageIncomplete("index provider left calendar keys missing"),
                        code=ArtifactReadyCoverageIncomplete.code,
                        stage="index_coverage",
                        subject=code,
                    )
                details[code] = {
                    **evidence,
                    "expected_rows": len(expected_dates),
                    "required_from": definition.required_from.isoformat(),
                }
                merged_rows.extend(_portable_index_row(row) for row in merged)
            receipt = {
                "schema_version": ARTIFACT_READY_INDEX_CHUNK_SCHEMA,
                "raw_partition_identity": _identity(descriptor),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "rows": sorted(
                    merged_rows,
                    key=lambda row: (str(row["ts_code"]), str(row["trade_date"])),
                ),
                "details": details,
                "safety": dict(_ZERO_SAFETY),
            }
            effective_root = digest_named_fields(
                "dataset_release_index_effective_partition_v1",
                {
                    "partition_key": str(descriptor["partition_key"]),
                    "rows": receipt["rows"],
                },
            )
            receipt["effective_content_root"] = effective_root
            monthly_leaves = _index_monthly_leaves(receipt["rows"])
            receipt["monthly_content_leaves"] = monthly_leaves
            reference = self.cas.put_json(receipt)
            derived_refs.append(reference)
            entries.append(
                _derived_entry(
                    dataset="index_daily_merged",
                    partition_key=str(descriptor["partition_key"]),
                    role="database_provider_missing_only",
                    row_count=len(merged_rows),
                    reference=reference,
                    content_digest=effective_root,
                    monthly_content_leaves=monthly_leaves,
                    source_table_schema_digest=descriptor.get("source_table_schema_digest"),
                    source_code_membership_digest=descriptor.get("source_code_membership_digest"),
                    min_key=(
                        [receipt["rows"][0]["ts_code"], receipt["rows"][0]["trade_date"]] if receipt["rows"] else None
                    ),
                    max_key=(
                        [receipt["rows"][-1]["ts_code"], receipt["rows"][-1]["trade_date"]] if receipt["rows"] else None
                    ),
                )
            )
            checkpoint()
        return tuple(entries), _dedupe_refs(provider_refs), _dedupe_refs(derived_refs)

    def _raw_entries(self, view: ArtifactSourceView, component: Component) -> tuple[Mapping[str, Any], ...]:
        entries: list[Mapping[str, Any]] = []
        for dataset in _COMPONENT_DATASETS[component]:
            descriptors = view.descriptors(dataset)
            if not descriptors:
                raise ArtifactReadyCoverageIncomplete(
                    f"required sealed dataset is missing: {component.value}:{dataset}"
                )
            for descriptor in descriptors:
                reference = _complete_ref(self.cas, descriptor.get("rows_ref"), field=_identity(descriptor))
                entries.append(
                    {
                        "identity": _identity(descriptor),
                        "dataset": dataset,
                        "partition_key": str(descriptor["partition_key"]),
                        "role": "sealed_database_source",
                        "row_count": int(descriptor["row_count"]),
                        "content_digest": ensure_sha256(str(descriptor["content_digest"]), field="content_digest"),
                        "schema_digest": ensure_sha256(str(descriptor["schema_digest"]), field="schema_digest"),
                        "rows_ref": reference.as_dict(),
                        "monthly_content_leaves": [
                            dict(value) for value in descriptor.get("monthly_content_leaves", ())
                        ],
                        "source_table_schema_digest": descriptor.get("source_table_schema_digest"),
                        "source_code_membership_digest": descriptor.get("source_code_membership_digest"),
                        "min_key": descriptor.get("min_key"),
                        "max_key": descriptor.get("max_key"),
                    }
                )
        return tuple(sorted(entries, key=lambda item: str(item["identity"])))

    def _seal_component_manifest(
        self,
        component: Component,
        *,
        source_content_root: str,
        partitions: Sequence[Mapping[str, Any]],
        details: Mapping[str, Any],
    ) -> CASRef:
        ordered = sorted((dict(item) for item in partitions), key=lambda item: str(item["identity"]))
        identities = [str(item["identity"]) for item in ordered]
        if len(identities) != len(set(identities)):
            raise ArtifactReadySourceError("component source partition identity duplicated")
        provenance_root = digest_named_fields(
            ARTIFACT_READY_COMPONENT_SCHEMA,
            {
                "component": component.value,
                "source_content_root": source_content_root,
                "partitions": ordered,
                "details": dict(details),
            },
        )
        effective_partitions = _effective_partition_projection(component, ordered)
        qfq_digest = None
        qfq_summary = details.get("qfq_source_summary")
        if isinstance(qfq_summary, Mapping):
            qfq_digest = qfq_summary.get("qfq_denominator_authority_digest")
        effective_root = digest_named_fields(
            "dataset_release_artifact_ready_component_effective_v1",
            {
                "component": component.value,
                "partitions": effective_partitions,
                "qfq_denominator_authority_digest": qfq_digest,
            },
        )
        reference = self.cas.put_json(
            {
                "schema_version": ARTIFACT_READY_COMPONENT_SCHEMA,
                "component": component.value,
                "source_content_root": source_content_root,
                "partitions": ordered,
                "effective_partitions": effective_partitions,
                "component_content_root": effective_root,
                "component_effective_content_root": effective_root,
                "component_provenance_root": provenance_root,
                "details": dict(details),
                "safety": dict(_ZERO_SAFETY),
            }
        )
        return self.cas.verify(reference)

    def _provider_call(self, function: Callable[..., Any], *args: Any) -> Any:
        with self._provider_lock:
            self._active_provider_calls += 1
            self.peak_provider_calls = max(self.peak_provider_calls, self._active_provider_calls)
            if self._active_provider_calls != 1:
                raise ArtifactReadySourceError("provider concurrency exceeded one")
            try:
                return function(*args)
            finally:
                self._active_provider_calls -= 1

    def _raise_provider_failure(
        self,
        exc: BaseException,
        *,
        code: str,
        stage: str,
        subject: str,
    ) -> None:
        status = getattr(exc, "status_code", getattr(exc, "status", None))
        receipt = self.cas.put_json(
            {
                "schema_version": PROVIDER_FAILURE_SCHEMA,
                "stage": stage,
                "subject": subject,
                "error_code": code,
                "exception_type": type(exc).__name__,
                "http_status": status if type(status) is int else None,
                "message_sha256": hashlib.sha256(
                    f"{type(exc).__name__}\0{exc}".encode("utf-8", errors="replace")
                ).hexdigest(),
                "safety": dict(_ZERO_SAFETY),
            }
        )
        raise ArtifactReadySourceError(
            "artifact-ready provider evidence failed",
            code=code,
            context={"failure_receipt_ref": receipt.as_dict()},
        ) from exc

    def _fetch_tdx_rows(self, ts_code: str, start: date, end: date) -> Sequence[Mapping[str, Any]]:
        if end < start:
            raise ArtifactReadyProviderTerminal("TDX window is reversed")
        port_text = os.getenv("TDX_HTTP_PORT", str(TDX_DEFAULT_PORT)).strip()
        if not port_text.isdigit() or not 1 <= int(port_text) <= 65_535:
            raise ArtifactReadyProviderTerminal("TDX loopback port is invalid")
        host = "127.0.0.1"
        if not ipaddress.ip_address(host).is_loopback:
            raise ArtifactReadyProviderTerminal("TDX target is not loopback")
        try:
            import requests
        except ImportError as exc:
            raise ArtifactReadyProviderTerminal("TDX HTTP client is unavailable") from exc
        requested_bars = ((end - start).days + 1) * 240
        if requested_bars > MAX_TDX_WINDOW_BARS:
            raise ArtifactReadyProviderTerminal("TDX minute window exceeds bound")
        session = requests.Session()
        session.trust_env = False
        try:
            response = session.get(
                f"http://{host}:{int(port_text)}/api/kline-all/tdx",
                params={
                    "code": ts_code.split(".", 1)[0],
                    "type": "minute1",
                    "limit": max(240, requested_bars),
                },
                timeout=(3.05, 30.0),
                allow_redirects=False,
                stream=True,
            )
            if response.is_redirect or response.is_permanent_redirect:
                raise ArtifactReadyProviderTerminal("TDX redirect is forbidden")
            response.raise_for_status()
            body = bytearray()
            for chunk in response.iter_content(chunk_size=64 * 1024):
                body.extend(chunk)
                if len(body) > MAX_TDX_RESPONSE_BYTES:
                    raise ArtifactReadyProviderTerminal("TDX response exceeds bound")
            try:
                payload = json.loads(body)
            except Exception as exc:
                raise ArtifactReadyProviderTerminal("TDX response is invalid JSON") from exc
            if not isinstance(payload, Mapping) or payload.get("code") != 0:
                raise ArtifactReadyProviderTerminal("TDX response status is not success")
            data = payload.get("data")
            values = data.get("list", ()) if isinstance(data, Mapping) else ()
            if not isinstance(values, list):
                raise ArtifactReadyProviderTerminal("TDX minute payload is invalid")
            rows: list[Mapping[str, Any]] = []
            for value in values:
                if not isinstance(value, Mapping):
                    raise ArtifactReadyProviderTerminal("TDX minute payload contains a non-object row")
                observed = _provider_trade_datetime(value)
                if start <= observed.date() <= end:
                    rows.append(value)
            return rows
        finally:
            session.close()

    def _fetch_tushare_minute_rows(self, ts_code: str, day: date) -> Sequence[Mapping[str, Any]]:
        provider = self._tushare.provider()
        try:
            frame = provider.stk_mins(
                ts_code=ts_code,
                freq="1min",
                start_date=f"{day.isoformat()} 09:30:00",
                end_date=f"{day.isoformat()} 15:00:00",
            )
            return _bounded_tushare_records(
                frame,
                dataset="stk_mins",
                expected_columns=(
                    "ts_code",
                    "trade_time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ),
                date_column="trade_time",
                start=day,
                end=day,
                max_rows=MAX_TUSHARE_MINUTE_ROWS_PER_DAY,
            )
        except Exception as exc:
            if _is_40203(exc):
                raise MinuteProviderRateLimitTerminal("Tushare minute request reached terminal 40203") from exc
            raise MinuteProviderTerminal("Tushare minute request failed") from exc

    def _fetch_tushare_index_rows(
        self, definition: IndexDefinition, start: date, end: date
    ) -> Sequence[Mapping[str, Any]]:
        provider = self._tushare.provider()
        try:
            frame = provider.index_daily(
                ts_code=definition.daily_code,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                fields="ts_code,trade_date," + ",".join(INDEX_SOURCE_VALUE_FIELDS),
            )
            return _bounded_tushare_records(
                frame,
                dataset="index_daily",
                expected_columns=("ts_code", "trade_date", *INDEX_SOURCE_VALUE_FIELDS),
                date_column="trade_date",
                start=start,
                end=end,
                max_rows=(end - start).days + 1,
            )
        except Exception as exc:
            if _is_40203(exc):
                raise IndexProviderRateLimitTerminal("Tushare index request reached terminal 40203") from exc
            raise IndexProviderUnavailable("Tushare index request failed") from exc

    def _fetch_tushare_adj_factor_rows(self, day: date) -> Sequence[Mapping[str, Any]]:
        provider = self._tushare.provider()
        try:
            frame = provider.adj_factor(
                trade_date=day.strftime("%Y%m%d"),
                fields="ts_code,trade_date,adj_factor",
            )
            return _bounded_tushare_records(
                frame,
                dataset="adj_factor",
                expected_columns=("ts_code", "trade_date", "adj_factor"),
                date_column="trade_date",
                start=day,
                end=day,
                max_rows=MAX_TUSHARE_ADJ_FACTOR_ROWS_PER_DAY,
            )
        except Exception as exc:
            if _is_40203(exc):
                raise ArtifactReadyProviderTerminal("Tushare adj_factor request reached terminal 40203") from exc
            raise ArtifactReadyProviderTerminal("Tushare adj_factor request failed") from exc

    def _fetch_tushare_daily_rows(self, ts_code: str, start: date, end: date) -> Sequence[Mapping[str, Any]]:
        provider = self._tushare.provider()
        try:
            import tushare as ts

            frame = ts.pro_bar(
                api=provider,
                ts_code=ts_code,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                freq="D",
                asset="E",
                adj=None,
                factors=None,
                fields="ts_code,trade_date,open,high,low,close,vol,amount",
            )
            records = _bounded_tushare_records(
                frame.loc[:, ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]],
                dataset="daily_pro_bar",
                expected_columns=("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"),
                date_column="trade_date",
                start=start,
                end=end,
                max_rows=MAX_TUSHARE_DAILY_ROWS_PER_CODE,
            )
            return tuple(_normalize_tushare_daily_row(row, expected_code=ts_code) for row in records)
        except Exception as exc:
            if _is_40203(exc):
                raise ArtifactReadyProviderTerminal("Tushare daily request reached terminal 40203") from exc
            if isinstance(exc, ArtifactReadyProviderTerminal):
                raise
            raise ArtifactReadyProviderTerminal("Tushare daily request failed") from exc


def _bounded_tushare_records(
    frame: Any,
    *,
    dataset: str,
    expected_columns: Sequence[str],
    date_column: str,
    start: date,
    end: date,
    max_rows: int,
) -> tuple[Mapping[str, Any], ...]:
    """Validate a bounded provider frame before allocating record dictionaries."""

    if end < start or type(max_rows) is not int or max_rows < 0:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response window is invalid")
    try:
        columns = tuple(str(value) for value in frame.columns)
        row_count = len(frame)
    except Exception as exc:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response is not a frame") from exc
    required = tuple(expected_columns)
    if len(columns) != len(required) or len(set(columns)) != len(columns) or set(columns) != set(required):
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response columns differ")
    if type(row_count) is not int or not 0 <= row_count <= max_rows:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response row count exceeds window bound")
    try:
        frame_bytes = int(frame.memory_usage(index=True, deep=True).sum())
    except Exception as exc:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response memory size is unavailable") from exc
    if not 0 <= frame_bytes <= MAX_TUSHARE_PROVIDER_FRAME_BYTES:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response memory size exceeds bound")
    try:
        date_values = tuple(frame[date_column].tolist())
    except Exception as exc:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response date column is invalid") from exc
    if len(date_values) != row_count:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response date count differs")
    try:
        for value in date_values:
            observed = (
                _provider_trade_datetime({"trade_time": value}).date()
                if date_column == "trade_time"
                else _as_date(value, field=f"Tushare {dataset} response date")
            )
            if not start <= observed <= end:
                raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response row is outside request window")
    except ArtifactReadyProviderTerminal:
        raise
    except Exception as exc:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response date is invalid") from exc

    try:
        raw_records = frame.to_dict(orient="records")
    except Exception as exc:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response record conversion failed") from exc
    if not isinstance(raw_records, list) or len(raw_records) != row_count:
        raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response record count differs")
    records: list[Mapping[str, Any]] = []
    receipt_bytes = 2  # JSON array brackets
    for index, row in enumerate(raw_records):
        if not isinstance(row, Mapping) or set(map(str, row)) != set(required):
            raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response contains an invalid row")
        portable = _portable_json_row(row)
        try:
            encoded = json.dumps(
                portable,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response row is not receipt-safe") from exc
        receipt_bytes += len(encoded) + int(index > 0)
        if receipt_bytes > MAX_TUSHARE_PROVIDER_RECEIPT_BYTES:
            raise ArtifactReadyProviderTerminal(f"Tushare {dataset} response receipt exceeds bound")
        records.append(dict(row))
    return tuple(records)


def _normalize_tushare_daily_row(row: Mapping[str, Any], *, expected_code: str) -> dict[str, Any]:
    code = str(row.get("ts_code", "")).upper()
    if code != expected_code.upper():
        raise ArtifactReadyProviderTerminal("Tushare daily response code differs")

    def scaled(name: str, multiplier: str, *, positive: bool) -> int:
        try:
            value = (Decimal(str(row.get(name))) * Decimal(multiplier)).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ArtifactReadyProviderTerminal(f"Tushare daily {name} is invalid") from exc
        integer = int(value)
        if (positive and integer <= 0) or (not positive and integer < 0):
            raise ArtifactReadyProviderTerminal(f"Tushare daily {name} is outside domain")
        return integer

    return {
        "ts_code": code,
        "trade_date": _as_date(row.get("trade_date"), field="Tushare daily trade_date").isoformat(),
        "open_li": scaled("open", "1000", positive=True),
        "high_li": scaled("high", "1000", positive=True),
        "low_li": scaled("low", "1000", positive=True),
        "close_li": scaled("close", "1000", positive=True),
        "volume_hand": scaled("vol", "1", positive=False),
        # Tushare daily amount is thousand CNY; DB amount_li is 0.001 CNY.
        "amount_li": scaled("amount", "1000000", positive=False),
    }


def _portable_daily_row(row: Mapping[str, Any]) -> dict[str, Any]:
    code = str(row.get("ts_code", "")).upper()
    if re.fullmatch(r"[0-9]{6}\.(?:SH|SZ)", code) is None:
        raise ArtifactReadyCoverageIncomplete("daily row code is invalid")
    output: dict[str, Any] = {
        "ts_code": code,
        "trade_date": _as_date(row.get("trade_date"), field="daily row trade_date").isoformat(),
    }
    for field in ("open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li"):
        try:
            value = Decimal(str(row.get(field)))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ArtifactReadyCoverageIncomplete(f"daily row {field} is invalid") from exc
        integral = value.to_integral_value()
        if value != integral:
            raise ArtifactReadyCoverageIncomplete(f"daily row {field} is not integral")
        integer = int(integral)
        if (field in {"open_li", "high_li", "low_li", "close_li"} and integer <= 0) or (
            field in {"volume_hand", "amount_li"} and integer < 0
        ):
            raise ArtifactReadyCoverageIncomplete(f"daily row {field} is outside domain")
        output[field] = integer
    if output["high_li"] < max(output["open_li"], output["close_li"]) or output["low_li"] > min(
        output["open_li"], output["close_li"]
    ):
        raise ArtifactReadyCoverageIncomplete("daily row OHLC ordering is invalid")
    return output


def _daily_value_tuple(row: Mapping[str, Any]) -> tuple[int, ...]:
    normalized = _portable_daily_row(row)
    return tuple(
        int(normalized[field])
        for field in ("open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li")
    )


def _group_minute_rows(
    rows: Iterable[Mapping[str, Any]],
) -> Iterator[tuple[tuple[str, date], tuple[Mapping[str, Any], ...]]]:
    current_key: tuple[str, date] | None = None
    current: list[Mapping[str, Any]] = []
    previous_time: str | None = None
    for raw in rows:
        row = dict(raw)
        if str(row.get("freq", "1m")) not in {"1m", "1min", "minute1"}:
            raise ArtifactReadyCoverageIncomplete("minute source frequency differs")
        code = str(row.get("ts_code", "")).upper()
        day = _as_date(str(row.get("trade_time", ""))[:10], field="minute trade_time")
        key = (code, day)
        timestamp = str(row.get("trade_time", ""))
        if current_key is not None and key < current_key:
            raise ArtifactReadyCoverageIncomplete("minute source group order regressed")
        if key == current_key and previous_time is not None and timestamp <= previous_time:
            raise ArtifactReadyCoverageIncomplete("minute source timestamp order regressed")
        if current_key is not None and key != current_key:
            yield current_key, tuple(current)
            current = []
            previous_time = None
        current_key = key
        current.append(row)
        previous_time = timestamp
    if current_key is not None:
        yield current_key, tuple(current)


def _minute_effective_content_digest(
    gap: MinuteGap,
    database_rows: Iterable[Mapping[str, Any]],
    overlay_rows: Iterable[Mapping[str, Any]],
) -> str:
    database = normalize_database_rows(database_rows, gap)
    overlay = normalize_database_rows(overlay_rows, gap)
    columns = (
        "open_li",
        "high_li",
        "low_li",
        "close_li",
        "volume_hand",
        "amount_li",
    )
    effective: dict[datetime, tuple[int, ...]] = {}
    for frame, source in ((database, "database"), (overlay, "overlay")):
        for row in frame.itertuples(index=False):
            observed = row.trade_time
            values = tuple(int(getattr(row, column)) for column in columns)
            if observed in effective and effective[observed] != values:
                raise MinuteSourceConflict(f"minute {source} conflicts with immutable effective value")
            effective[observed] = values
    if len(effective) != gap.expected_bars:
        raise MinuteProviderTerminal("fresh database plus immutable overlay is not a complete minute session")
    return digest_named_fields(
        "dataset_release_minute_effective_stock_day_v1",
        {
            "ts_code": gap.ts_code,
            "trade_date": gap.trade_date,
            "rows": [
                {
                    "trade_time": observed.isoformat(sep=" "),
                    **dict(zip(columns, values, strict=True)),
                }
                for observed, values in sorted(effective.items())
            ],
        },
    )


def _minute_bucket(code: str, bucket_count: int) -> int:
    return int(hashlib.sha256(code.encode("utf-8")).hexdigest()[:16], 16) % bucket_count


def _portable_index_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ts_code": str(row["ts_code"]).upper(),
        "trade_date": _as_date(row["trade_date"], field="index trade_date").isoformat(),
        **{field: float(row[field]) for field in INDEX_SOURCE_VALUE_FIELDS},
    }


def _normalize_adj_factor_row(row: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise ArtifactReadyCoverageIncomplete(f"{source} adj_factor row is not an object")
    code = str(row.get("ts_code", "")).strip().upper()
    if not re.fullmatch(r"\d{6}\.(?:SH|SZ)", code):
        raise ArtifactReadyCoverageIncomplete(f"{source} adj_factor instrument is invalid")
    observed = _as_date(row.get("trade_date"), field=f"{source} adj_factor date")
    try:
        value = Decimal(str(row.get("adj_factor")))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ArtifactReadyCoverageIncomplete(f"{source} adj_factor value is invalid") from exc
    if not value.is_finite() or value <= 0:
        raise ArtifactReadyCoverageIncomplete(f"{source} adj_factor value is invalid")
    return {"ts_code": code, "trade_date": observed, "adj_factor": value}


def _portable_adj_factor_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ts_code": str(row["ts_code"]).upper(),
        "trade_date": _as_date(row["trade_date"], field="adj_factor trade_date").isoformat(),
        "adj_factor": float(row["adj_factor"]),
    }


def _portable_adj_factor_value(key: tuple[str, date], value: Decimal) -> dict[str, Any]:
    return {
        "ts_code": key[0],
        "trade_date": key[1].isoformat(),
        "adj_factor": float(value),
    }


def _qfq_canonical_row_bytes(code: str, day: date, value: Decimal) -> bytes:
    return (
        json.dumps(
            [code, day.isoformat(), float(value)],
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("ascii")
        + b"\n"
    )


def _adj_monthly_leaves(
    rows: Mapping[tuple[str, date], Decimal],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[tuple[tuple[str, date], Decimal]]] = {}
    for key, value in sorted(rows.items()):
        grouped.setdefault(key[1].strftime("%Y-%m"), []).append((key, value))
    return [
        _artifact_month_leaf(
            month,
            rows=[_portable_adj_factor_value(key, value) for key, value in values],
            min_key=[values[0][0][0], values[0][0][1].isoformat()],
            max_key=[values[-1][0][0], values[-1][0][1].isoformat()],
        )
        for month, values in sorted(grouped.items())
    ]


def _artifact_month_leaf(
    month: str,
    *,
    rows: Sequence[Mapping[str, Any]],
    min_key: Any,
    max_key: Any,
) -> dict[str, Any]:
    merkle_root = digest_named_fields(
        "dataset_release_artifact_ready_month_merkle_v1",
        {"month": month, "rows": list(rows)},
    )
    content_digest = digest_named_fields(
        "dataset_release_artifact_ready_month_content_v1",
        {
            "month": month,
            "row_count": len(rows),
            "min_key": min_key,
            "max_key": max_key,
            "merkle_root": merkle_root,
        },
    )
    body = {
        "schema_version": SOURCE_MONTH_CONTENT_LEAF_SCHEMA,
        "month": month,
        "row_count": len(rows),
        "min_key": min_key,
        "max_key": max_key,
        "merkle_root": merkle_root,
        "content_digest": content_digest,
    }
    return {
        **body,
        "leaf_identity": digest_named_fields(SOURCE_MONTH_CONTENT_LEAF_SCHEMA, body),
    }


def _effective_day_monthly_leaves(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        canonical = {
            "ts_code": str(row["ts_code"]),
            "trade_date": str(row["trade_date"]),
            "effective_content_sha256": ensure_sha256(
                str(row["effective_content_sha256"]),
                field="effective_stock_day_digest",
            ),
        }
        grouped.setdefault(canonical["trade_date"][:7], []).append(canonical)
    return [
        _artifact_month_leaf(
            month,
            rows=values,
            min_key=[values[0]["ts_code"], values[0]["trade_date"]],
            max_key=[values[-1]["ts_code"], values[-1]["trade_date"]],
        )
        for month, values in sorted(grouped.items())
    ]


def _index_monthly_leaves(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["trade_date"])[:7], []).append(dict(row))
    return [
        _artifact_month_leaf(
            month,
            rows=values,
            min_key=[values[0]["ts_code"], values[0]["trade_date"]],
            max_key=[values[-1]["ts_code"], values[-1]["trade_date"]],
        )
        for month, values in sorted(grouped.items())
    ]


def _portable_json_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return provider market data in deterministic, JSON-safe form."""

    def convert(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime):
            return value.isoformat(sep=" ")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Mapping):
            return {str(key): convert(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [convert(item) for item in value]
        return str(value)

    return {str(key): convert(value) for key, value in row.items()}


def _provider_trade_datetime(row: Mapping[str, Any]) -> datetime:
    raw = next(
        (row[key] for key in ("trade_time", "TradeTime", "time", "Time") if key in row and row[key] is not None),
        None,
    )
    if isinstance(raw, datetime):
        return raw.astimezone(CHINA_TZ).replace(tzinfo=None) if raw.tzinfo is not None else raw
    text = str(raw or "").strip()
    try:
        value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return value.astimezone(CHINA_TZ).replace(tzinfo=None) if value.tzinfo is not None else value
    except ValueError:
        pass
    text = text.replace("T", " ")
    for pattern in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y%m%d %H:%M:%S",
        "%Y%m%d%H%M%S",
    ):
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue
    raise ArtifactReadyProviderTerminal("provider minute timestamp is invalid")


def _drop_tushare_auction_row(rows: Sequence[Mapping[str, Any]], target: date) -> tuple[Mapping[str, Any], ...]:
    """Map official 241-row stk_mins sessions to the 240-bar QE convention."""

    result: list[Mapping[str, Any]] = []
    auction_rows = 0
    for row in rows:
        if not isinstance(row, Mapping):
            raise MinuteProviderTerminal("Tushare returned a non-object minute row")
        observed = _provider_trade_datetime(row)
        if observed.date() == target and observed.hour == 9 and observed.minute == 30:
            auction_rows += 1
            continue
        result.append(row)
    if auction_rows > 1:
        raise MinuteProviderTerminal("Tushare returned duplicate 09:30 auction rows")
    return tuple(result)


def _derived_entry(
    *,
    dataset: str,
    partition_key: str,
    role: str,
    row_count: int,
    reference: CASRef,
    content_digest: str,
    monthly_content_leaves: Sequence[Mapping[str, Any]] = (),
    source_table_schema_digest: str | None = None,
    source_code_membership_digest: str | None = None,
    min_key: Any = None,
    max_key: Any = None,
) -> Mapping[str, Any]:
    return {
        "identity": f"{dataset}:{partition_key}",
        "dataset": dataset,
        "partition_key": partition_key,
        "role": role,
        "row_count": int(row_count),
        "content_digest": ensure_sha256(content_digest, field="content_digest"),
        "schema_digest": digest_named_fields(
            "dataset_release_artifact_ready_derived_schema_v1",
            {"dataset": dataset, "role": role},
        ),
        "rows_ref": reference.as_dict(),
        "monthly_content_leaves": [dict(value) for value in monthly_content_leaves],
        "source_table_schema_digest": source_table_schema_digest,
        "source_code_membership_digest": source_code_membership_digest,
        "min_key": min_key,
        "max_key": max_key,
    }


def _effective_partition_projection(
    component: Component,
    partitions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    replaced_raw = {"adj_factor"}
    if component is Component.DAILY_BIN:
        replaced_raw.add("index_daily")
    if component is Component.MINUTE_BIN:
        replaced_raw.add("kline_minute_raw")
    elif component is Component.DOMESTIC_INDEX_CONTEXT:
        replaced_raw = {"index_daily"}
    elif component not in {
        Component.DAILY_BIN,
        Component.FACTOR_H5_STATIC,
    }:
        replaced_raw = set()
    output: list[dict[str, Any]] = []
    for item in partitions:
        if item.get("role") == "sealed_database_source" and item.get("dataset") in replaced_raw:
            continue
        output.append(
            {
                "identity": str(item["identity"]),
                "dataset": str(item["dataset"]),
                "partition_key": str(item["partition_key"]),
                "row_count": int(item["row_count"]),
                "content_digest": ensure_sha256(str(item["content_digest"]), field="effective content_digest"),
                "schema_digest": ensure_sha256(str(item["schema_digest"]), field="effective schema_digest"),
                "monthly_content_leaves": [dict(value) for value in item.get("monthly_content_leaves", ())],
                "source_table_schema_digest": item.get("source_table_schema_digest"),
                "source_code_membership_digest": item.get("source_code_membership_digest"),
                "min_key": item.get("min_key"),
                "max_key": item.get("max_key"),
            }
        )
    return sorted(output, key=lambda item: str(item["identity"]))


def _identity(descriptor: Mapping[str, Any]) -> str:
    return f"{descriptor.get('dataset')}:{descriptor.get('partition_key')}"


def _complete_ref(cas: CASStore, value: Any, *, field: str) -> CASRef:
    try:
        supplied = CASRef.from_value(value)
    except Exception as exc:
        raise SourceManifestError(f"{field} CAS reference is invalid") from exc
    if supplied.size < 0:
        raise SourceManifestError(f"{field} CAS reference is incomplete")
    verified = cas.verify(supplied)
    if supplied.relative_path != verified.relative_path:
        raise SourceManifestError(f"{field} CAS path is non-canonical")
    return verified


def _snapshot_recheck_partition_expectations(
    snapshot: ArtifactSnapshot,
) -> list[Mapping[str, Any]]:
    values: list[Mapping[str, Any]] = []
    identities: set[str] = set()
    for scope, attribute in (("source", "partitions"), ("pit", "pit_partitions")):
        partitions = getattr(snapshot, attribute, ())
        if not isinstance(partitions, Sequence):
            raise ArtifactReadySourceError("source snapshot recheck partitions are invalid")
        for partition in partitions:
            method = getattr(partition, "as_build_input", None)
            if callable(method):
                raw = method()
            elif isinstance(partition, Mapping):
                raw = dict(partition)
            else:
                raise ArtifactReadySourceError("source snapshot recheck partition is invalid")
            if not isinstance(raw, Mapping):
                raise ArtifactReadySourceError("source snapshot recheck partition descriptor is invalid")
            value = {**dict(raw), "recheck_partition_scope": scope}
            identity = f"{value.get('dataset')}:{value.get('partition_key')}"
            if not all((value.get("dataset"), value.get("partition_key"))) or identity in identities:
                raise ArtifactReadySourceError("source snapshot recheck partition identity is invalid")
            identities.add(identity)
            values.append(value)
    return sorted(
        values,
        key=lambda item: (
            str(item["recheck_partition_scope"]),
            str(item["dataset"]),
            str(item["partition_key"]),
        ),
    )


def _dedupe_refs(values: Iterable[CASRef]) -> tuple[CASRef, ...]:
    unique = {value.sha256: value for value in values}
    return tuple(unique[key] for key in sorted(unique))


def _as_date(value: Any, *, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    try:
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:]))
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ArtifactReadyCoverageIncomplete(f"{field} is invalid") from exc


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ArtifactReadySourceError("artifact-ready timestamp is naive")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _is_40203(exc: BaseException) -> bool:
    return str(getattr(exc, "code", "")) == "40203" or "40203" in str(exc)


def _safe_identity(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", str(value)))


__all__ = [
    "ARTIFACT_READY_COMPONENT_SCHEMA",
    "ARTIFACT_READY_CONTRACT_SCHEMA",
    "ARTIFACT_READY_RECHECK_SCHEMA",
    "ArtifactReadyCoverageIncomplete",
    "ArtifactReadyIndexConflict",
    "ArtifactReadyMinuteConflict",
    "ArtifactReadyProviderTerminal",
    "ArtifactReadyRecheckResult",
    "ArtifactReadySourceBuilder",
    "ArtifactReadySourceBundle",
    "ArtifactReadySourceError",
    "ArtifactReadySourceRevised",
    "ArtifactSourceView",
    "load_artifact_ready_contract",
    "load_artifact_ready_recheck_expectations",
]
