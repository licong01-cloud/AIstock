"""Read-only, allowlisted source authority for monthly dataset resolution.

The authority opens one worker-owned PostgreSQL REPEATABLE READ / READ ONLY
snapshot, validates every required table/field, and seals the exact ordered row
streams into control CAS while computing ``source_manifest`` identities.  It
never imports the backend global pool and exposes no arbitrary SQL or path API.
"""

from __future__ import annotations

import json
import heapq
import math
import re
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, Callable, Iterable, Iterator, Mapping, Protocol, Sequence

from .canonical import (
    canonical_json_bytes,
    digest_named_fields,
    ensure_sha256,
    sha256_hex,
)
from .cas_store import (
    CASHashOnlyMismatch,
    CASPutResult,
    CASRef,
    CASStore,
    CASStoreError,
)
from .contracts import Component
from .errors import DatasetReleaseError, SourceManifestError
from .index_sources import independent_postgres_connection_factory
from .pit import FrozenPitSnapshot, freeze_pit_snapshot, require_canonical_source_snapshot
from .profile import DatasetProfile, ResourcePolicy
from .source_manifest import (
    CanonicalPartitionHasher,
    ColumnKind,
    ColumnSpec,
    PartitionSpec,
    PartitionSummary,
    SourceManifest,
)
from .source_pool import ReadOnlySourcePool
from .source_rows_codec import (
    SOURCE_ROWS_CODEC,
    SOURCE_ROWS_CODEC_IDENTITY,
    SOURCE_ROWS_CODEC_LEVEL,
    SOURCE_ROWS_CODEC_VERSION,
    SOURCE_ROWS_FORMAT,
    StreamingCompressionStats,
    iter_gzip_level1,
    validate_rows_codec_identity,
    validate_rows_envelope,
)
from .sector_enrichment import FrozenSectorEnricher, SECTOR_ENRICHMENT_SCHEMA
from .streaming_artifacts import build_date_chunks


SOURCE_MANIFEST_ARTIFACT_SCHEMA = "dataset_release_source_content_manifest_v1"
SOURCE_PROVENANCE_RECEIPT_SCHEMA = "dataset_release_source_provenance_receipt_v1"
SOURCE_REUSE_MANIFEST_SCHEMA = "dataset_release_source_reuse_manifest_v1"
SOURCE_REFRESH_AUDIT_RECEIPT_SCHEMA = "dataset_release_source_refresh_audit_receipt_v1"
SOURCE_STAGE_RECEIPT_SCHEMA = "dataset_release_source_stage_receipt_v1"
SOURCE_PARTITION_ROWS_SCHEMA = "dataset_release_source_partition_rows_v1"
SOURCE_AUTHORITY_POLICY_VERSION = "qe_monthly_source_authority_v2"
SOURCE_CONSISTENCY_POLICY = "partition_rr_control_bracket_writer_ledger_quiescence_v1"
SOURCE_MVCC_FINGERPRINT_SCHEMA = "dataset_release_partition_mvcc_fingerprint_v1"
SOURCE_MONTH_CONTENT_LEAF_SCHEMA = "dataset_release_source_month_content_leaf_v1"
# Flip only after the exact production PostgreSQL/Timescale permissions and
# xmin behavior have passed the documented capability test.  Fixture injection
# can exercise the contract without silently enabling unverified production reuse.
MVCC_PARTITION_REUSE_PRODUCTION_VALIDATED = False
MAX_SOURCE_STAGE_ARTIFACT_BYTES = 64 * 1024 * 1024
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_STOCK_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")


class SourceAuthorityError(DatasetReleaseError):
    code = "DATASET_RELEASE_SOURCE_AUTHORITY_ERROR"


class SourceConfigurationMissing(SourceAuthorityError):
    code = "BLOCKED_SOURCE_CONFIGURATION_MISSING"


class SourceRequiredTableMissing(SourceAuthorityError):
    code = "BLOCKED_SOURCE_REQUIRED_TABLE_MISSING"


class SourceRequiredFieldMissing(SourceAuthorityError):
    code = "BLOCKED_SOURCE_REQUIRED_FIELD_MISSING"


class OfficialCutoffMismatch(SourceAuthorityError):
    code = "BLOCKED_OFFICIAL_TRADING_CUTOFF_MISMATCH"


class SourceRequiredDatasetEmpty(SourceAuthorityError):
    code = "BLOCKED_SOURCE_REQUIRED_DATASET_EMPTY"


class SourcePartitionRowLimitExceeded(SourceAuthorityError):
    code = "BLOCKED_SOURCE_PARTITION_ROW_LIMIT_EXCEEDED"


class SourceProviderContractError(SourceAuthorityError):
    code = "BLOCKED_SOURCE_PROVIDER_CONTRACT"


class SourceAuditIncomplete(SourceAuthorityError):
    code = "BLOCKED_SOURCE_AUDIT_INCOMPLETE"


class SourceSnapshotDriftBlocked(SourceAuthorityError):
    code = "BLOCKED_SOURCE_SNAPSHOT_DRIFT"


class SourceSnapshotRevised(SourceAuthorityError):
    code = "BLOCKED_SOURCE_REVISED"


@dataclass(slots=True)
class SourceCASBudgetTracker:
    predicted_remaining_new_bytes: int | None = None
    disk_checkpoint: Callable[[int | None], Any] | None = None
    new_bytes: int = 0
    reused_bytes: int = 0
    new_partitions: int = 0
    reused_partitions: int = 0
    snapshots: list[Mapping[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.predicted_remaining_new_bytes is not None and (
            type(self.predicted_remaining_new_bytes) is not int or self.predicted_remaining_new_bytes < 0
        ):
            raise ValueError("predicted source CAS bytes are invalid")
        if self.snapshots is None:
            self.snapshots = []

    def checkpoint(self) -> None:
        if self.disk_checkpoint is None:
            return
        value = self.disk_checkpoint(self.predicted_remaining_new_bytes)
        if is_dataclass(value):
            payload = asdict(value)
        elif isinstance(value, Mapping):
            payload = dict(value)
        else:
            raise SourceAuthorityError("disk checkpoint returned an invalid snapshot")
        assert self.snapshots is not None
        self.snapshots.append(payload)

    def record(self, result: CASPutResult) -> None:
        if result.created:
            self.new_bytes += result.reference.size
            self.new_partitions += 1
        else:
            self.reused_bytes += result.reference.size
            self.reused_partitions += 1
        self._consume(result.reference.size)

    def record_reused(self, reference: CASRef) -> None:
        self.reused_bytes += reference.size
        self.reused_partitions += 1
        self._consume(reference.size)

    def _consume(self, size: int) -> None:
        if self.predicted_remaining_new_bytes is not None:
            self.predicted_remaining_new_bytes = max(0, self.predicted_remaining_new_bytes - int(size))

    def as_dict(self) -> dict[str, Any]:
        assert self.snapshots is not None
        return {
            "new_bytes": self.new_bytes,
            "reused_bytes": self.reused_bytes,
            "new_partitions": self.new_partitions,
            "reused_partitions": self.reused_partitions,
            "predicted_remaining_new_bytes": self.predicted_remaining_new_bytes,
            "disk_snapshots": [dict(item) for item in self.snapshots],
        }


@dataclass(frozen=True, slots=True)
class SourceQuerySpec:
    """One immutable member of the production SQL allowlist."""

    query_id: str
    schema_name: str
    table_name: str
    components: tuple[Component, ...]
    key_columns: tuple[str, ...]
    value_columns: tuple[str, ...]
    derived_value_columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    non_null_value_columns: tuple[str, ...]
    date_expression: str | None
    start_policy: str
    query_version: str
    audit_non_null_value_columns: tuple[str, ...] = ()
    audit_dataset: str | None = None
    audit_eligible_sources: tuple[str, ...] = ()
    audit_eligible_quality_statuses: tuple[str, ...] = ("ok", "empty_valid")
    max_partition_rows: int = 1_000_000
    code_column: str | None = None
    code_policy: str | None = None

    def __post_init__(self) -> None:
        identifiers = (
            self.query_id.replace(".", "_"),
            self.schema_name,
            self.table_name,
            *self.key_columns,
            *self.required_columns,
            *self.derived_value_columns,
            *self.non_null_value_columns,
            *self.audit_non_null_value_columns,
            *((self.audit_dataset,) if self.audit_dataset is not None else ()),
            *self.audit_eligible_sources,
            *self.audit_eligible_quality_statuses,
        )
        if any(not _IDENTIFIER.fullmatch(value) for value in identifiers):
            raise ValueError("source query spec contains a non-allowlist identifier")
        if not self.components or not self.key_columns or not self.value_columns:
            raise ValueError("source query spec requires components and keys")
        if not set(self.key_columns).issubset(self.required_columns):
            raise ValueError("source query keys must be required fields")
        if set(self.derived_value_columns).intersection(self.value_columns):
            raise ValueError("derived source values overlap physical values")
        if not set(self.non_null_value_columns).issubset({*self.value_columns, *self.derived_value_columns}):
            raise ValueError("non-null source values must be projected value fields")
        if not set(self.audit_non_null_value_columns).issubset(self.value_columns):
            raise ValueError("audit non-null values must be physical projected fields")
        if self.start_policy not in {"daily", "minute", "timeless"}:
            raise ValueError("source query start policy is invalid")
        if self.start_policy == "timeless" and self.date_expression is not None:
            raise ValueError("timeless query cannot carry a date expression")
        if (self.audit_dataset is None) != (not self.audit_eligible_sources):
            raise ValueError("dated source audit dataset and eligible sources must be specified together")
        if self.date_expression is None and self.audit_dataset is not None:
            raise ValueError("timeless query cannot require per-date refresh audit")
        if self.date_expression is not None and self.audit_dataset is None:
            raise ValueError("dated query requires an explicit refresh-audit dataset")
        if (self.code_column is None) != (self.code_policy is None):
            raise ValueError("source code column/policy must be specified together")
        if type(self.max_partition_rows) is not int or not 0 < self.max_partition_rows <= 1_000_000:
            raise ValueError("source partition row limit must be in [1,1000000]")

    @property
    def table_identity(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    @property
    def sql(self) -> str:
        alias = "source_row"
        key = ",".join(f"{alias}.{column}" for column in self.key_columns)
        projected = tuple(dict.fromkeys((*self.key_columns, *self.value_columns)))
        payload = "jsonb_build_object(" + ",".join(f"'{column}',{alias}.{column}" for column in projected) + ")"
        if self.query_id == "bak_basic":
            return (
                "SELECT jsonb_build_array(source_row.ts_code,source_row.trade_date)::text "
                "AS row_key, (" + payload + ")::text AS row_payload FROM market.bak_basic AS source_row "
                "JOIN market.stock_basic AS stock ON stock.ts_code=source_row.ts_code "
                "WHERE source_row.trade_date >= %(start)s AND source_row.trade_date <= %(end)s "
                "AND source_row.trade_date >= stock.list_date + interval '365 days' "
                "AND EXISTS (SELECT 1 FROM market.kline_daily_raw AS daily "
                "WHERE daily.ts_code=source_row.ts_code "
                "AND daily.trade_date=source_row.trade_date) "
                "AND source_row.ts_code = ANY(%(codes)s) ORDER BY row_key,row_payload"
            )
        if self.query_id == "sw_index_classify":
            return (
                "SELECT jsonb_build_array(source_row.index_code)::text AS row_key, ("
                + payload
                + ")::text AS row_payload FROM market.sw_index_classify AS source_row "
                "WHERE source_row.level='L2' ORDER BY row_key,row_payload"
            )
        if self.query_id == "trading_calendar":
            return (
                "SELECT jsonb_build_array(source_row.cal_date)::text AS row_key, ("
                + payload
                + ")::text AS row_payload FROM market.trading_calendar AS source_row "
                "WHERE source_row.cal_date >= %(start)s "
                "AND source_row.cal_date <= %(end)s "
                "AND source_row.is_trading = TRUE ORDER BY row_key,row_payload"
            )
        clauses: list[str] = []
        if self.date_expression is not None:
            clauses.extend(
                (
                    f"{self.date_expression} >= %(start)s",
                    f"{self.date_expression} <= %(end)s",
                )
            )
        if self.code_column is not None:
            clauses.append(f"{alias}.{self.code_column} = ANY(%(codes)s)")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return (
            "SELECT jsonb_build_array("
            + key
            + ")::text AS row_key, ("
            + payload
            + ")::text AS row_payload "
            + f"FROM {self.table_identity} AS {alias}{where} "
            + "ORDER BY row_key, row_payload"
        )

    @property
    def fingerprint_sql(self) -> str:
        """Bounded-result MVCC revision fingerprint over the exact row query."""

        sql = self.sql.strip()
        for suffix in (" ORDER BY row_key,row_payload", " ORDER BY row_key, row_payload"):
            if sql.endswith(suffix):
                sql = sql[: -len(suffix)]
                break
        marker = ")::text AS row_payload FROM "
        if marker not in sql:
            raise SourceConfigurationMissing(
                "source query cannot derive its MVCC fingerprint projection",
                context={"query_id": self.query_id},
            )
        projected = sql.replace(
            marker,
            ")::text AS row_payload,source_row.xmin::text::bigint AS row_xmin FROM ",
            1,
        )
        return (
            "WITH source_fingerprint_rows AS (" + projected + ") SELECT COUNT(*)::bigint AS row_count,"
            "MIN(row_key)::text AS min_key,MAX(row_key)::text AS max_key,"
            "MIN(row_xmin)::bigint AS min_row_xmin,"
            "MAX(row_xmin)::bigint AS max_row_xmin,"
            "(SELECT system_identifier::text FROM pg_control_system()) "
            "AS db_system_identifier,"
            "(SELECT timeline_id::bigint FROM pg_control_checkpoint()) AS timeline_id,"
            "(txid_current()::bigint >> 32) AS xid_epoch "
            "FROM source_fingerprint_rows"
        )


def _query(
    query_id: str,
    table_name: str,
    components: Sequence[Component],
    keys: Sequence[str],
    *,
    values: Sequence[str],
    date_expression: str | None = None,
    start_policy: str = "daily",
    required: Sequence[str] = (),
    non_null_values: Sequence[str] = (),
    derived_values: Sequence[str] = (),
    audit_dataset: str | None = None,
    audit_eligible_sources: Sequence[str] = (),
    audit_eligible_quality_statuses: Sequence[str] = ("ok", "empty_valid"),
    audit_non_null_values: Sequence[str] | None = None,
    code_column: str | None = None,
    code_policy: str | None = None,
) -> SourceQuerySpec:
    if code_column is None and keys and keys[0] == "ts_code":
        code_column = "ts_code"
        code_policy = "pit_stock_codes"
    required_columns = tuple(dict.fromkeys((*keys, *values, *required)))
    return SourceQuerySpec(
        query_id=query_id,
        schema_name="market",
        table_name=table_name,
        components=tuple(components),
        key_columns=tuple(keys),
        value_columns=tuple(values),
        derived_value_columns=tuple(derived_values),
        required_columns=required_columns,
        non_null_value_columns=tuple(non_null_values),
        audit_non_null_value_columns=tuple(
            (
                value
                for value in non_null_values
                if value in values
            )
            if audit_non_null_values is None
            else audit_non_null_values
        ),
        date_expression=date_expression,
        start_policy=start_policy,
        query_version=(
            f"{query_id}_canonical_row_code_major_v3"
            + (":derived_l2_v1" if derived_values else "")
            + (":pit_stock_filter_v1" if code_policy == "pit_stock_codes" else "")
        ),
        audit_dataset=audit_dataset,
        audit_eligible_sources=tuple(audit_eligible_sources),
        audit_eligible_quality_statuses=tuple(audit_eligible_quality_statuses),
        code_column=code_column,
        code_policy=code_policy,
    )


_ALL_NON_INDEX = (
    Component.DAILY_BIN,
    Component.MINUTE_BIN,
    Component.FACTOR_H5_STATIC,
)

_OHLCV_RAW_VALUES = (
    "open_li",
    "high_li",
    "low_li",
    "close_li",
    "volume_hand",
    "amount_li",
)
_DAILY_BASIC_VALUES = (
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)
_MONEYFLOW_VALUES = (
    "buy_sm_vol",
    "buy_sm_amount",
    "sell_sm_vol",
    "sell_sm_amount",
    "buy_md_vol",
    "buy_md_amount",
    "sell_md_vol",
    "sell_md_amount",
    "buy_lg_vol",
    "buy_lg_amount",
    "sell_lg_vol",
    "sell_lg_amount",
    "buy_elg_vol",
    "buy_elg_amount",
    "sell_elg_vol",
    "sell_elg_amount",
    "net_mf_vol",
    "net_mf_amount",
)
_BAK_BASIC_VALUES = (
    "pe_dyn",
    "total_assets",
    "liquid_assets",
    "fixed_assets",
    "reserved",
    "reserved_pershare",
    "eps",
    "bvps",
    "undp",
    "per_undp",
    "rev_yoy",
    "profit_yoy",
    "gpr",
    "npr",
    "holder_num",
)
_CYQ_VALUES = (
    "his_low",
    "his_high",
    "cost_5pct",
    "cost_15pct",
    "cost_50pct",
    "cost_85pct",
    "cost_95pct",
    "weight_avg",
    "winner_rate",
)
_SECTOR_VALUES = (
    "sw2_open",
    "sw2_high",
    "sw2_low",
    "sw2_close",
    "sw2_pct_change",
    "sw2_vol",
    "sw2_amount",
    "sw2_pe",
    "sw2_pb",
    "sw2_total_mv",
    "sw2_mf_buy_sm_amt",
    "sw2_mf_sell_sm_amt",
    "sw2_mf_buy_md_amt",
    "sw2_mf_sell_md_amt",
    "sw2_mf_buy_lg_amt",
    "sw2_mf_sell_lg_amt",
    "sw2_mf_buy_elg_amt",
    "sw2_mf_sell_elg_amt",
    "sw2_mf_net_amt",
    "sw2_mf_buy_elg_vol",
    "sw2_mf_sell_elg_vol",
    "sw2_mf_net_vol",
)

_QUERY_SPECS = (
    _query(
        "trading_calendar",
        "trading_calendar",
        tuple(Component),
        ("cal_date",),
        values=("is_trading",),
        non_null_values=("is_trading",),
        date_expression="source_row.cal_date",
        required=("is_trading",),
        audit_dataset="trading_calendar",
        audit_eligible_sources=("physical_audit_seed", "script", "tdx_api"),
    ),
    _query(
        "kline_daily_raw",
        "kline_daily_raw",
        _ALL_NON_INDEX,
        ("ts_code", "trade_date"),
        values=_OHLCV_RAW_VALUES,
        non_null_values=_OHLCV_RAW_VALUES,
        date_expression="source_row.trade_date",
        audit_dataset="kline_daily_raw",
        audit_eligible_sources=("physical_audit_seed", "tdx_api"),
    ),
    _query(
        "adj_factor",
        "adj_factor",
        _ALL_NON_INDEX,
        ("ts_code", "trade_date"),
        values=("adj_factor",),
        non_null_values=("adj_factor",),
        date_expression="source_row.trade_date",
        audit_dataset="adj_factor",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
    ),
    _query(
        "stk_limit",
        "stk_limit",
        (Component.DAILY_BIN, Component.MINUTE_BIN),
        ("ts_code", "trade_date"),
        values=("pre_close", "up_limit", "down_limit"),
        non_null_values=(),
        audit_non_null_values=("pre_close", "up_limit", "down_limit"),
        date_expression="source_row.trade_date",
        audit_dataset="stk_limit",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
        audit_eligible_quality_statuses=("ok", "candidate_repairable"),
    ),
    _query(
        "suspend_d",
        "suspend_d",
        (Component.DAILY_BIN, Component.MINUTE_BIN),
        ("ts_code", "trade_date", "suspend_type"),
        values=("suspend_timing",),
        date_expression="source_row.trade_date",
        audit_dataset="suspend_d",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
    ),
    _query(
        "kline_minute_raw",
        "kline_minute_raw",
        (Component.MINUTE_BIN,),
        ("ts_code", "trade_time", "freq"),
        values=_OHLCV_RAW_VALUES,
        non_null_values=_OHLCV_RAW_VALUES,
        date_expression="source_row.trade_time::date",
        start_policy="minute",
        audit_dataset="kline_minute_raw",
        audit_eligible_sources=("physical_audit_seed", "script", "tdx_api"),
        code_column="ts_code",
        code_policy="pit_minute_code_batch",
    ),
    _query(
        "daily_basic",
        "daily_basic",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code", "trade_date"),
        values=_DAILY_BASIC_VALUES,
        date_expression="source_row.trade_date",
        audit_dataset="daily_basic",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
    ),
    _query(
        "moneyflow_ts",
        "moneyflow_ts",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code", "trade_date"),
        values=_MONEYFLOW_VALUES,
        date_expression="source_row.trade_date",
        audit_dataset="stock_moneyflow_ts",
        audit_eligible_sources=("physical_audit_seed", "script"),
    ),
    _query(
        "bak_basic",
        "bak_basic",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code", "trade_date"),
        values=_BAK_BASIC_VALUES,
        date_expression="source_row.trade_date",
        audit_dataset="bak_basic",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
    ),
    _query(
        "cyq_perf",
        "cyq_perf",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code", "trade_date"),
        values=_CYQ_VALUES,
        date_expression="source_row.trade_date",
        audit_dataset="cyq_perf",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
    ),
    _query(
        "sector_data",
        "sector_data",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code", "trade_date"),
        values=_SECTOR_VALUES,
        derived_values=("l2_code_id",),
        non_null_values=("l2_code_id",),
        date_expression="source_row.trade_date",
        audit_dataset="sector_data",
        audit_eligible_sources=("physical_audit_seed", "sector_builder"),
    ),
    _query(
        "margin_detail",
        "margin_detail",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code", "trade_date"),
        values=("rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"),
        date_expression="source_row.trade_date",
        audit_dataset="margin_detail",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
    ),
    _query(
        "stock_basic",
        "stock_basic",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code",),
        values=("list_date", "list_status", "exchange", "market"),
        start_policy="timeless",
    ),
    _query(
        "sw_index_classify",
        "sw_index_classify",
        (Component.FACTOR_H5_STATIC,),
        ("index_code",),
        values=("level",),
        non_null_values=("level",),
        start_policy="timeless",
    ),
    _query(
        "sw_index_member",
        "sw_index_member",
        (Component.FACTOR_H5_STATIC,),
        ("ts_code", "in_date", "l2_code"),
        values=("out_date",),
        start_policy="timeless",
    ),
    _query(
        "index_daily",
        "index_daily",
        (Component.DOMESTIC_INDEX_CONTEXT,),
        ("ts_code", "trade_date"),
        values=("open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"),
        non_null_values=(
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "pct_chg",
            "vol",
            "amount",
        ),
        date_expression="source_row.trade_date",
        audit_dataset="index_daily",
        audit_eligible_sources=("physical_audit_seed", "tushare"),
        audit_eligible_quality_statuses=("ok", "candidate_repairable"),
        code_column="ts_code",
        code_policy="profile_index_codes",
    ),
)

PRODUCTION_QUERY_SPECS: Mapping[str, SourceQuerySpec] = MappingProxyType({item.query_id: item for item in _QUERY_SPECS})


@dataclass(frozen=True, slots=True)
class SourceTableSchema:
    table_identity: str
    ordered_columns: tuple[str, ...]
    ordered_types: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.ordered_types and len(self.ordered_types) != len(self.ordered_columns):
            raise ValueError("source table schema columns/types differ")

    @property
    def digest(self) -> str:
        return digest_named_fields(
            "dataset_release_source_table_schema_v1",
            {
                "table_identity": self.table_identity,
                "projection": [
                    {
                        "name": name,
                        "type": (self.ordered_types[index] if self.ordered_types else "fixture-untyped"),
                    }
                    for index, name in enumerate(self.ordered_columns)
                ],
            },
        )


class SourceSnapshotSession(Protocol):
    @property
    def snapshot_tokens(self) -> tuple[str, ...]: ...

    def describe(self, query_id: str) -> SourceTableSchema: ...

    def fetch_one(
        self,
        query_id: str,
        params: Mapping[str, Any],
    ) -> Mapping[str, Any] | None: ...

    def stream(
        self,
        query_id: str,
        params: Mapping[str, Any],
        *,
        fetch_rows: int,
    ) -> Iterable[Mapping[str, Any]]: ...

    def partition_fingerprint(
        self,
        query_id: str,
        params: Mapping[str, Any],
        *,
        query_version: str,
        table_schema_digest: str,
    ) -> Mapping[str, Any] | None: ...


SourceSessionFactory = Callable[[ResourcePolicy], AbstractContextManager[SourceSnapshotSession]]


_SCHEMA_SQL = """
SELECT column_name,data_type,udt_name
FROM information_schema.columns
WHERE table_schema = %(schema_name)s AND table_name = %(table_name)s
ORDER BY ordinal_position
"""

_OFFICIAL_CUTOFF_SQL = """
SELECT MAX(cal_date)::date AS official_cutoff
FROM market.trading_calendar
WHERE is_trading = TRUE
  AND cal_date >= date_trunc('month', %(cutoff)s::date)::date
  AND cal_date < (date_trunc('month', %(cutoff)s::date) + interval '1 month')::date
"""

_PIT_STATE_SQL = """
SELECT universe_key,rule_version,scope,start_date,end_date,status,dirty,
       jsonb_build_object(
           'universe_key',universe_key,'rule_version',rule_version,'scope',scope,
           'start_date',start_date,'end_date',end_date,'status',status,'dirty',dirty
       )::text AS state_payload
FROM market.stock_universe_pit_state
WHERE universe_key = %(universe_key)s
"""

_PIT_SPANS_SQL = """
SELECT ts_code,eligible_start,eligible_end,entry_reason,exit_reason,
       jsonb_build_object(
           'universe_key',universe_key,'ts_code',ts_code,
           'eligible_start',eligible_start,'eligible_end',eligible_end,
           'entry_reason',entry_reason,'exit_reason',exit_reason,
           'rule_version',rule_version
       )::text AS semantic_payload
FROM market.stock_universe_pit_spans AS source_row
WHERE universe_key = %(universe_key)s
  AND eligible_end >= %(start)s
  AND eligible_start <= %(end)s
  AND (%(codes)s::text[] IS NULL OR ts_code = ANY(%(codes)s))
ORDER BY ts_code,eligible_start,eligible_end,entry_reason,exit_reason,semantic_payload
"""

_REFRESH_AUDIT_SQL = """
SELECT dataset,trade_date,data_source,job_id,status,row_count,refreshed_at,
       error_message,data_max_at,written_rows,expected_rows,coverage_ratio,
       quality_status,failure_category,metadata::text AS metadata_json
FROM market.dataset_date_refresh_audit AS source_row
WHERE dataset = ANY(%(datasets)s)
  AND trade_date >= %(start)s AND trade_date <= %(end)s
ORDER BY dataset,trade_date,data_source
"""

_TRADING_DATES_SQL = """
SELECT cal_date::date AS trade_date
FROM market.trading_calendar
WHERE is_trading = TRUE AND cal_date >= %(start)s AND cal_date <= %(end)s
ORDER BY cal_date
"""

_SOURCE_WRITER_LEDGER_SQL = """
WITH latest_data_sync_attempt AS (
    SELECT DISTINCT ON (attempt.target_id)
           attempt.attempt_id,attempt.target_id,attempt.attempt_no,
           attempt.status,attempt.created_at,attempt.started_at,
           attempt.finished_at,attempt.error_message,attempt.context_json,
           target.dataset,target.data_source
    FROM market.data_sync_attempts AS attempt
    JOIN market.data_sync_targets AS target ON target.target_id=attempt.target_id
    WHERE target.dataset = ANY(%(datasets)s)
    ORDER BY attempt.target_id,attempt.attempt_no DESC,
             attempt.created_at DESC,attempt.attempt_id DESC
)
SELECT ledger_kind,ledger_identity,status,created_at,started_at,finished_at,
       opaque_payload
FROM (
    SELECT 'ingestion_jobs'::text AS ledger_kind,
           job_id::text AS ledger_identity,status::text,
           created_at,started_at,finished_at,summary::text AS opaque_payload
    FROM market.ingestion_jobs
    WHERE created_at >= %(start)s OR status IN ('queued','pending','running')
    UNION ALL
    SELECT 'data_sync_attempts'::text AS ledger_kind,
           attempt.attempt_id::text AS ledger_identity,attempt.status::text,
           attempt.created_at,attempt.started_at,attempt.finished_at,
           jsonb_build_object(
               'target_id',attempt.target_id,
               'dataset',attempt.dataset,
               'data_source',attempt.data_source,
               'error_message',attempt.error_message,
               'context_json',attempt.context_json
           )::text AS opaque_payload
    FROM latest_data_sync_attempt AS attempt
    WHERE (attempt.created_at >= %(start)s OR attempt.status='started')
) AS writer_ledger
ORDER BY ledger_kind,ledger_identity
"""

_PIT_STATE_REQUIRED = (
    "universe_key",
    "rule_version",
    "scope",
    "start_date",
    "end_date",
    "status",
    "dirty",
)
_PIT_SPANS_REQUIRED = (
    "universe_key",
    "ts_code",
    "eligible_start",
    "eligible_end",
    "entry_reason",
    "exit_reason",
    "rule_version",
)
_REFRESH_AUDIT_REQUIRED = (
    "dataset",
    "trade_date",
    "data_source",
    "job_id",
    "status",
    "row_count",
    "refreshed_at",
    "error_message",
    "data_max_at",
    "written_rows",
    "expected_rows",
    "coverage_ratio",
    "quality_status",
    "failure_category",
    "metadata",
)
_WRITER_LEDGER_REQUIRED = (
    "ledger_kind",
    "ledger_identity",
    "status",
    "created_at",
    "started_at",
    "finished_at",
    "opaque_payload",
)


class PostgresSourceSnapshotSession(AbstractContextManager["PostgresSourceSnapshotSession"]):
    """One coherent snapshot backed by the independent dataset source pool."""

    def __init__(
        self,
        policy: ResourcePolicy,
        *,
        connection_factory: Callable[[], Any] = independent_postgres_connection_factory,
    ) -> None:
        self.policy = policy
        self._pool = ReadOnlySourcePool(connection_factory, policy)
        self._connection_context: Any = None
        self._connection: Any = None
        self._snapshot_tokens: tuple[str, ...] = ()
        self._described: dict[str, SourceTableSchema] = {}

    def __enter__(self) -> "PostgresSourceSnapshotSession":
        self._connection_context = self._pool.connection()
        self._connection = self._connection_context.__enter__()
        cursor = self._connection.cursor()
        try:
            cursor.execute(
                "SELECT txid_current_snapshot()::text,current_setting('transaction_isolation'),"
                "current_setting('transaction_read_only')"
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
        if not row or str(row[2]).lower() not in {"on", "true", "1"}:
            raise SourceConfigurationMissing("PostgreSQL source snapshot did not acknowledge read-only mode")
        token = digest_named_fields(
            "dataset_release_postgres_snapshot_token_v1",
            {
                "snapshot": str(row[0]),
                "isolation": str(row[1]).lower(),
                "read_only": str(row[2]).lower(),
            },
        )
        self._snapshot_tokens = (f"postgres-snapshot-sha256:{token}",)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            if self._connection_context is not None:
                self._connection_context.__exit__(exc_type, exc, traceback)
        finally:
            self._connection = None
            self._pool.close()

    @property
    def snapshot_tokens(self) -> tuple[str, ...]:
        if self._connection is None or not self._snapshot_tokens:
            raise SourceConfigurationMissing("source snapshot session is not open")
        return self._snapshot_tokens

    def describe(self, query_id: str) -> SourceTableSchema:
        if query_id in self._described:
            return self._described[query_id]
        if query_id == "writer_ledger":
            physical = (
                self._describe_table(
                    query_id,
                    "market",
                    "ingestion_jobs",
                    (
                        "job_id",
                        "status",
                        "created_at",
                        "started_at",
                        "finished_at",
                        "summary",
                    ),
                ),
                self._describe_table(
                    query_id,
                    "market",
                    "data_sync_attempts",
                    (
                        "attempt_id",
                        "target_id",
                        "attempt_no",
                        "status",
                        "created_at",
                        "started_at",
                        "finished_at",
                        "error_message",
                        "context_json",
                    ),
                ),
                self._describe_table(
                    query_id,
                    "market",
                    "data_sync_targets",
                    ("target_id", "dataset", "data_source"),
                ),
            )
            physical_digest = digest_named_fields(
                "dataset_release_writer_ledger_physical_schema_v1",
                {value.table_identity: value.digest for value in physical},
            )
            value = SourceTableSchema(
                "market.source_writer_ledger_union_v1",
                _WRITER_LEDGER_REQUIRED,
                tuple(f"projection:{physical_digest}" for _ in _WRITER_LEDGER_REQUIRED),
            )
            self._described[query_id] = value
            return value
        if query_id == "pit_state":
            schema_name, table_name, required = (
                "market",
                "stock_universe_pit_state",
                _PIT_STATE_REQUIRED,
            )
        elif query_id == "pit_spans":
            schema_name, table_name, required = (
                "market",
                "stock_universe_pit_spans",
                _PIT_SPANS_REQUIRED,
            )
        elif query_id == "refresh_audit":
            schema_name, table_name, required = (
                "market",
                "dataset_date_refresh_audit",
                _REFRESH_AUDIT_REQUIRED,
            )
        else:
            spec = _required_query_spec(query_id)
            schema_name, table_name, required = (
                spec.schema_name,
                spec.table_name,
                spec.required_columns,
            )
        value = self._describe_table(query_id, schema_name, table_name, required)
        self._described[query_id] = value
        return value

    def _describe_table(
        self,
        query_id: str,
        schema_name: str,
        table_name: str,
        required: Sequence[str],
    ) -> SourceTableSchema:
        cursor = self._cursor()
        try:
            cursor.execute(
                _SCHEMA_SQL,
                {"schema_name": schema_name, "table_name": table_name},
            )
            schema_rows = tuple(cursor.fetchall())
        finally:
            cursor.close()
        if not schema_rows:
            raise SourceRequiredTableMissing(
                f"required source table is missing: {schema_name}.{table_name}",
                context={"query_id": query_id, "table": f"{schema_name}.{table_name}"},
            )
        available = {str(row[0]): f"{row[1]}:{row[2]}" for row in schema_rows}
        missing = sorted(set(required).difference(available))
        if missing:
            raise SourceRequiredFieldMissing(
                f"required source fields are missing: {schema_name}.{table_name}",
                context={"query_id": query_id, "missing_fields": missing},
            )
        return SourceTableSchema(
            f"{schema_name}.{table_name}",
            tuple(required),
            tuple(available[column] for column in required),
        )

    def fetch_one(
        self,
        query_id: str,
        params: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        sql = _metadata_sql(query_id)
        cursor = self._cursor()
        try:
            cursor.execute(sql, dict(params))
            rows = cursor.fetchmany(2)
            if len(rows) > 1:
                raise SourceManifestError(f"metadata query is ambiguous: {query_id}")
            return _row_mapping(cursor, rows[0]) if rows else None
        finally:
            cursor.close()

    def stream(
        self,
        query_id: str,
        params: Mapping[str, Any],
        *,
        fetch_rows: int,
    ) -> Iterable[Mapping[str, Any]]:
        if not 1 <= int(fetch_rows) <= self.policy.validation_read_chunk_rows:
            raise ValueError("source stream fetch_rows is outside profile policy")
        sql = _stream_sql(query_id)
        connection = self._required_connection()
        try:
            cursor = connection.cursor(name=f"dataset_release_source_{query_id}")
        except TypeError:
            cursor = connection.cursor()
        try:
            if hasattr(cursor, "itersize"):
                cursor.itersize = int(fetch_rows)
            cursor.execute(sql, dict(params))
            while True:
                batch = cursor.fetchmany(int(fetch_rows))
                if not batch:
                    return
                if len(batch) > int(fetch_rows):
                    raise SourceManifestError("source driver exceeded the configured fetch bound")
                for row in batch:
                    yield _row_mapping(cursor, row)
        finally:
            cursor.close()

    def partition_fingerprint(
        self,
        query_id: str,
        params: Mapping[str, Any],
        *,
        query_version: str,
        table_schema_digest: str,
    ) -> Mapping[str, Any] | None:
        query = _required_query_spec(query_id)
        cursor = self._cursor()
        savepoint = "dataset_release_mvcc_fingerprint"
        try:
            cursor.execute(f"SAVEPOINT {savepoint}")
            try:
                cursor.execute(query.fingerprint_sql, dict(params))
                rows = cursor.fetchmany(2)
            except Exception:
                # Unsupported system columns/Timescale layouts/permissions are
                # a conservative full-stream fallback, never a reuse signal.
                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                cursor.execute(f"RELEASE SAVEPOINT {savepoint}")
                return None
            cursor.execute(f"RELEASE SAVEPOINT {savepoint}")
        finally:
            cursor.close()
        if len(rows) != 1:
            raise SourceManifestError("partition fingerprint result is ambiguous")
        raw = _row_mapping_from_names(
            (
                "row_count",
                "min_key",
                "max_key",
                "min_row_xmin",
                "max_row_xmin",
                "db_system_identifier",
                "timeline_id",
                "xid_epoch",
            ),
            rows[0],
        )
        return _validated_mvcc_fingerprint(
            raw,
            query=query,
            params=params,
            query_version=query_version,
            table_schema_digest=table_schema_digest,
        )

    def _cursor(self) -> Any:
        return self._required_connection().cursor()

    def _required_connection(self) -> Any:
        if self._connection is None:
            raise SourceConfigurationMissing("source snapshot session is not open")
        return self._connection


def production_source_session_factory(
    policy: ResourcePolicy,
) -> PostgresSourceSnapshotSession:
    return PostgresSourceSnapshotSession(policy)


@dataclass(frozen=True, slots=True)
class SealedSourcePartition:
    spec: PartitionSpec
    summary: PartitionSummary
    components: tuple[Component, ...]
    rows_ref: CASRef
    source_table_schema: SourceTableSchema
    rows_uncompressed_bytes: int
    rows_compressed_bytes: int
    rows_compression_ratio: str
    monthly_content_leaves: tuple[Mapping[str, Any], ...] = ()
    source_payload_columns: tuple[str, ...] = ()
    source_non_null_value_columns: tuple[str, ...] = ()
    source_snapshot_tokens: tuple[str, ...] = ()
    source_revision_fingerprint: Mapping[str, Any] | None = None
    source_revision_capability: str = "full_stream_no_reuse_v1"
    source_partition_params_digest: str | None = None
    source_code_membership_digest: str | None = None
    refresh_audit_digest: str | None = None
    source_order_keys: tuple[str, ...] = ()

    def as_build_input(self) -> dict[str, Any]:
        return {
            "component": self.components[0].value,
            "consumer_components": [item.value for item in self.components],
            "dataset": self.spec.dataset,
            "partition_key": self.spec.partition_key,
            "query_version": self.spec.query_version,
            "schema_digest": self.spec.schema_digest,
            "columns": [item.as_dict() for item in self.spec.columns],
            "primary_keys": list(self.spec.primary_keys),
            "timezone_name": self.spec.timezone_name,
            "null_marker": self.spec.null_marker,
            "float_non_finite_policy": self.spec.float_non_finite_policy,
            "source_table_identity": self.source_table_schema.table_identity,
            "source_table_schema_digest": self.source_table_schema.digest,
            "source_table_columns": list(self.source_table_schema.ordered_columns),
            "source_table_types": list(self.source_table_schema.ordered_types),
            "source_order_keys": list(self.source_order_keys),
            "source_payload_columns": list(self.source_payload_columns),
            "source_non_null_value_columns": list(self.source_non_null_value_columns),
            "source_partition_params_digest": self.source_partition_params_digest,
            "source_code_membership_digest": self.source_code_membership_digest,
            "cross_partition_merge": "bounded_k_way_merge_v1",
            "ingestion_audit_identity": self.summary.ingestion_audit_identity,
            "row_count": self.summary.row_count,
            "min_key": _portable_key(self.summary.min_key),
            "max_key": _portable_key(self.summary.max_key),
            "required_null_count": self.summary.required_null_count,
            "duplicate_count": self.summary.duplicate_count,
            "content_digest": self.summary.content_digest,
            "merkle_root": self.summary.merkle_root,
            "monthly_content_leaves": [dict(value) for value in self.monthly_content_leaves],
            "rows_ref": self.rows_ref.as_dict(),
            "rows_format": SOURCE_ROWS_FORMAT,
            "rows_codec": SOURCE_ROWS_CODEC,
            "rows_codec_version": SOURCE_ROWS_CODEC_VERSION,
            "rows_codec_level": SOURCE_ROWS_CODEC_LEVEL,
            "rows_codec_identity": SOURCE_ROWS_CODEC_IDENTITY,
            "rows_uncompressed_bytes": self.rows_uncompressed_bytes,
            "rows_compressed_bytes": self.rows_compressed_bytes,
            "rows_compression_ratio": self.rows_compression_ratio,
        }

    def as_reuse_input(self) -> dict[str, Any]:
        return {
            **self.as_build_input(),
            "source_revision_fingerprint": (
                dict(self.source_revision_fingerprint) if self.source_revision_fingerprint is not None else None
            ),
            "source_revision_capability": self.source_revision_capability,
        }


@dataclass(frozen=True, slots=True)
class FrozenSourceAuthoritySnapshot:
    official_cutoff: date
    pit_snapshot: FrozenPitSnapshot
    pit_snapshot_ref: CASRef
    manifest: SourceManifest
    source_manifest_ref: CASRef
    source_reuse_manifest_ref: CASRef
    source_audit_ref: CASRef
    source_provenance_ref: CASRef
    derived_source_receipt_refs: tuple[CASRef, ...]
    partitions: tuple[SealedSourcePartition, ...]
    pit_partitions: tuple[SealedSourcePartition, ...]
    snapshot_tokens: tuple[str, ...]
    observation_provenance_root: str
    source_cas_usage: Mapping[str, Any]
    artifact_ready_contract_ref: CASRef | None = None
    artifact_ready_content_root: str | None = None
    artifact_ready_provenance_root: str | None = None
    provider_receipt_refs: tuple[CASRef, ...] = ()
    artifact_ready_derived_source_receipt_refs: tuple[CASRef, ...] = ()

    @property
    def source_content_root(self) -> str:
        return self.manifest.source_content_root

    @property
    def source_provenance_root(self) -> str:
        return self.observation_provenance_root

    @property
    def stable_source_provenance_root(self) -> str:
        return self.manifest.source_provenance_root

    @property
    def pit_snapshot_digest(self) -> str:
        return self.pit_snapshot.spans_sha256

    def component_partition_roots(self) -> dict[Component, str]:
        return {
            component: digest_named_fields(
                "dataset_release_component_source_partitions_v1",
                {
                    "component": component.value,
                    "partitions": [
                        {
                            "identity": item.spec.identity,
                            "content_digest": item.summary.content_digest,
                            "schema_digest": item.summary.schema_digest,
                        }
                        for item in sorted(
                            (value for value in self.partitions if component in value.components),
                            key=lambda value: value.spec.identity,
                        )
                    ],
                },
            )
            for component in Component
        }


@dataclass(frozen=True, slots=True)
class ExactSourceRecheckSnapshot:
    """In-memory result of a no-write exact partition recheck."""

    official_cutoff: date
    pit_snapshot: FrozenPitSnapshot
    manifest: SourceManifest
    partitions: tuple[SealedSourcePartition, ...]
    pit_partitions: tuple[SealedSourcePartition, ...]
    snapshot_tokens: tuple[str, ...]
    source_cas_usage: Mapping[str, Any]

    @property
    def source_content_root(self) -> str:
        return self.manifest.source_content_root

    @property
    def pit_snapshot_digest(self) -> str:
        return self.pit_snapshot.spans_sha256


@dataclass(frozen=True, slots=True)
class _SourceControlSnapshot:
    schemas: Mapping[str, SourceTableSchema]
    audit: SourceRefreshAuditLedger
    writer_ledger_digest: str
    writer_ledger_receipt: Mapping[str, Any]
    pit_partitions: tuple[SealedSourcePartition, ...]
    pit_snapshot: FrozenPitSnapshot
    snapshot_tokens: tuple[str, ...]
    consistency_digest: str


@dataclass(frozen=True, slots=True)
class _PartitionFingerprintPlan:
    query: SourceQuerySpec
    partition_key: str
    params: Mapping[str, Any]
    table_schema: SourceTableSchema
    fingerprint: Mapping[str, Any]
    revision_dependencies: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class SourceRefreshAuditLedger:
    rows: Mapping[tuple[str, date], tuple[Mapping[str, Any], ...]]
    trading_dates: tuple[date, ...]
    eligible_sources: Mapping[str, tuple[str, ...]]
    eligible_quality_statuses: Mapping[str, tuple[str, ...]]

    def as_receipt(self, *, profile: str, cutoff: date) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_REFRESH_AUDIT_RECEIPT_SCHEMA,
            "profile": profile,
            "cutoff": cutoff.isoformat(),
            "trading_dates": [value.isoformat() for value in self.trading_dates],
            "eligible_sources": {key: list(value) for key, value in sorted(self.eligible_sources.items())},
            "eligible_quality_statuses": {
                key: list(value) for key, value in sorted(self.eligible_quality_statuses.items())
            },
            "rows": [
                {
                    "dataset": dataset,
                    "trade_date": trade_date.isoformat(),
                    "sources": [json.loads(canonical_json_bytes(dict(row)).decode("utf-8")) for row in audit_rows],
                }
                for (dataset, trade_date), audit_rows in sorted(
                    self.rows.items(), key=lambda item: (item[0][0], item[0][1])
                )
            ],
            "safety": _zero_safety(),
        }

    def partition_digest(self, dataset: str, start: date, end: date) -> str:
        expected_dates = tuple(value for value in self.trading_dates if start <= value <= end)
        observed = {
            day: audit_rows for (name, day), audit_rows in self.rows.items() if name == dataset and start <= day <= end
        }
        missing = [value.isoformat() for value in expected_dates if value not in observed]
        if not expected_dates or missing:
            raise SourceAuditIncomplete(
                "dataset/date refresh audit coverage is incomplete",
                context={
                    "dataset": dataset,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "missing_count": len(missing),
                    "missing_sample": missing[:20],
                },
            )
        eligible_sources = set(self.eligible_sources.get(dataset, ()))
        eligible_quality = set(self.eligible_quality_statuses.get(dataset, ()))
        unusable = []
        for value in expected_dates:
            audit_rows = observed[value]
            if not any(
                str(row.get("data_source")) in eligible_sources
                and str(row.get("status", "")).lower() == "success"
                and row.get("error_present") is False
                and str(row.get("quality_status", "")).lower() in eligible_quality
                for row in audit_rows
            ):
                unusable.append(value.isoformat())
        if unusable:
            raise SourceAuditIncomplete(
                "dataset/date refresh audit has no policy-eligible successful authority",
                context={
                    "dataset": dataset,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "unusable_count": len(unusable),
                    "unusable_sample": unusable[:20],
                    "eligible_sources": sorted(eligible_sources),
                    "eligible_quality_statuses": sorted(eligible_quality),
                },
            )
        return digest_named_fields(
            "dataset_release_refresh_audit_partition_v1",
            {
                "dataset": dataset,
                "start": start,
                "end": end,
                "rows": [
                    {
                        "trade_date": value.isoformat(),
                        "sources": [
                            {
                                "data_source": str(row["data_source"]),
                                "status": str(row["status"]).lower(),
                                "quality_status": str(row.get("quality_status", "")).lower(),
                                "audit_payload_sha256": str(row["audit_payload_sha256"]),
                            }
                            for row in observed[value]
                        ],
                    }
                    for value in expected_dates
                ],
            },
        )


class MonthlySourceAuthority:
    """Freeze one exact source/PIT snapshot for a versioned monthly request."""

    def __init__(
        self,
        profile: DatasetProfile,
        cas: CASStore,
        *,
        session_factory: SourceSessionFactory = production_source_session_factory,
        mvcc_reuse_capability: bool = MVCC_PARTITION_REUSE_PRODUCTION_VALIDATED,
    ) -> None:
        self.profile = profile
        self.cas = cas
        self._session_factory = session_factory
        self._mvcc_reuse_capability = bool(mvcc_reuse_capability)

    def freeze(
        self,
        *,
        cutoff: date,
        checkpoint: Callable[[], None] | None = None,
        baseline_partitions: Sequence[Mapping[str, Any]] = (),
        predicted_new_bytes: int | None = None,
        disk_checkpoint: Callable[[int | None], Any] | None = None,
        pressure_rung: int = 0,
        recheck_partition_expectations: Sequence[Mapping[str, Any]] | None = None,
        expected_source_content_root: str | None = None,
        expected_pit_snapshot_digest: str | None = None,
        sample_instruments: Sequence[str] = (),
    ) -> FrozenSourceAuthoritySnapshot | ExactSourceRecheckSnapshot:
        if type(pressure_rung) is not int or not 0 <= pressure_rung < len(self.profile.pressure_ladder["h5_batch"]):
            raise SourceProviderContractError("source pressure rung is invalid")
        pulse = checkpoint or (lambda: None)
        selected_stock_codes = _validated_sample_instruments(sample_instruments)
        read_chunk_rows = max(
            1_000,
            self.profile.resource_policy.validation_read_chunk_rows // (2**pressure_rung),
        )
        budget = SourceCASBudgetTracker(
            predicted_remaining_new_bytes=predicted_new_bytes,
            disk_checkpoint=disk_checkpoint,
        )
        recheck_by_identity = _validate_recheck_partition_expectations(
            recheck_partition_expectations,
            expected_source_content_root=expected_source_content_root,
            expected_pit_snapshot_digest=expected_pit_snapshot_digest,
        )
        budget.checkpoint()
        # Refresh-audit rows are readiness/provenance evidence, never content
        # hashes.  Baseline bytes may be adopted only behind the independently
        # versioned MVCC fingerprint capability gate.
        baseline_by_identity: dict[str, Mapping[str, Any]] = {}
        for raw in baseline_partitions:
            if not isinstance(raw, Mapping):
                raise SourceAuditIncomplete("baseline source partition is invalid")
            identity = f"{raw.get('dataset')}:{raw.get('partition_key')}"
            if not all((raw.get("dataset"), raw.get("partition_key"))) or identity in baseline_by_identity:
                raise SourceAuditIncomplete("baseline source partition identity is invalid or duplicated")
            baseline_by_identity[identity] = dict(raw)
        before = self._capture_control_snapshot(
            cutoff=cutoff,
            pulse=pulse,
            budget=budget,
            read_chunk_rows=read_chunk_rows,
            recheck_by_identity=recheck_by_identity,
            selected_stock_codes=selected_stock_codes,
        )
        token_receipts = [f"control-before:{index}:{token}" for index, token in enumerate(before.snapshot_tokens)]
        writer_ledger_check_count = 1
        sealed: list[SealedSourcePartition] = []
        fingerprint_plans: list[_PartitionFingerprintPlan] = []
        mvcc_reused_partitions = 0
        mvcc_streamed_partitions = 0
        mvcc_unsupported_partitions = 0
        classify_rows: list[Mapping[str, Any]] = []
        member_rows: list[Mapping[str, Any]] = []
        sector_enricher: FrozenSectorEnricher | None = None
        query_order = (
            PRODUCTION_QUERY_SPECS["sw_index_classify"],
            PRODUCTION_QUERY_SPECS["sw_index_member"],
            *(
                value
                for value in PRODUCTION_QUERY_SPECS.values()
                if value.query_id not in {"sw_index_classify", "sw_index_member"}
            ),
        )
        for query in query_order:
            if query.query_id == "sector_data" and sector_enricher is None:
                sector_enricher = FrozenSectorEnricher.build(
                    classify_rows,
                    member_rows,
                )
            schema = before.schemas[query.query_id]
            query_rows = 0
            for partition_key, params in self._partition_requests(
                query,
                cutoff,
                pit_snapshot=before.pit_snapshot,
                selected_stock_codes=selected_stock_codes,
            ):
                pulse()
                audit_digest = None
                if query.date_expression is not None:
                    audit_digest = before.audit.partition_digest(
                        str(query.audit_dataset),
                        _as_date(params["start"]),
                        _as_date(params["end"]),
                    )
                with self._session_factory(self.profile.resource_policy) as session:
                    partition_tokens = self._session_tokens(session)
                    observed_schema = session.describe(query.query_id)
                    if observed_schema.digest != schema.digest:
                        raise SourceSnapshotDriftBlocked(
                            "source table schema changed during partition freeze",
                            context={"query_id": query.query_id},
                        )
                    spec = _query_partition_spec(query, partition_key, schema)
                    fingerprint = self._partition_fingerprint(
                        session,
                        query=query,
                        params=params,
                        spec=spec,
                        table_schema=schema,
                    )
                    revision_dependencies: dict[str, str] = {}
                    if query.query_id == "sector_data":
                        assert sector_enricher is not None
                        revision_dependencies = {
                            "sector_code_map_digest": sector_enricher.code_map_digest,
                            "sector_membership_digest": (sector_enricher.membership_digest),
                        }
                        fingerprint = _bind_revision_dependencies(
                            fingerprint,
                            revision_dependencies,
                        )
                    baseline = baseline_by_identity.get(spec.identity)
                    if (
                        recheck_by_identity is None
                        and fingerprint is not None
                        and baseline is not None
                        and baseline.get("source_revision_fingerprint") == fingerprint
                        and baseline.get("source_revision_capability") == "mvcc_xmin_revision_fingerprint_v1"
                    ):
                        partition = _sealed_partition_from_reuse_baseline(
                            self.cas,
                            baseline,
                            refresh_audit_digest=audit_digest,
                            snapshot_tokens=partition_tokens,
                        )
                        budget.record_reused(partition.rows_ref)
                        mvcc_reused_partitions += 1
                    else:
                        partition = self._seal_query_partition(
                            session,
                            query=query,
                            partition_key=partition_key,
                            params=params,
                            tokens=partition_tokens,
                            table_schema=schema,
                            refresh_audit_digest=audit_digest,
                            checkpoint=pulse,
                            budget=budget,
                            pressure_rung=pressure_rung,
                            read_chunk_rows=read_chunk_rows,
                            payload_enricher=(
                                sector_enricher.enrich
                                if query.query_id == "sector_data" and sector_enricher is not None
                                else None
                            ),
                            payload_observer=(
                                classify_rows.append
                                if recheck_by_identity is not None and query.query_id == "sw_index_classify"
                                else (
                                    member_rows.append
                                    if recheck_by_identity is not None and query.query_id == "sw_index_member"
                                    else None
                                )
                            ),
                            recheck_expectation=(
                                _required_recheck_expectation(recheck_by_identity, spec.identity)
                                if recheck_by_identity is not None
                                else None
                            ),
                        )
                        capability = (
                            "mvcc_xmin_revision_fingerprint_v1"
                            if fingerprint is not None
                            else (
                                "full_stream_fingerprint_unavailable_v1"
                                if self._mvcc_reuse_capability
                                else "full_stream_production_capability_gate_v1"
                            )
                        )
                        partition = replace(
                            partition,
                            source_revision_fingerprint=fingerprint,
                            source_revision_capability=capability,
                        )
                        mvcc_streamed_partitions += 1
                        if fingerprint is None:
                            mvcc_unsupported_partitions += 1
                    if fingerprint is not None:
                        if partition.summary.row_count != int(fingerprint["row_count"]):
                            raise SourceSnapshotDriftBlocked(
                                "source stream row count differs from MVCC fingerprint",
                                context={
                                    "query_id": query.query_id,
                                    "partition_key": partition_key,
                                },
                            )
                        fingerprint_plans.append(
                            _PartitionFingerprintPlan(
                                query=query,
                                partition_key=partition_key,
                                params=dict(params),
                                table_schema=schema,
                                fingerprint=fingerprint,
                                revision_dependencies=revision_dependencies,
                            )
                        )
                token_receipts.extend(
                    f"partition:{partition.spec.identity}:{index}:{token}"
                    for index, token in enumerate(partition_tokens)
                )
                query_rows += partition.summary.row_count
                sealed.append(partition)
                if recheck_by_identity is None and query.query_id in {"sw_index_classify", "sw_index_member"}:
                    from .sealed_source_reader import CASSealedPartitionReader

                    reader = CASSealedPartitionReader(
                        self.cas,
                        [partition.as_build_input()],
                        max_partition_rows=query.max_partition_rows,
                    )
                    target = classify_rows if query.query_id == "sw_index_classify" else member_rows
                    with reader.iter_rows(
                        query.query_id,
                        partition.spec.partition_key,
                    ) as rows:
                        target.extend(rows)
            if query_rows == 0:
                raise SourceRequiredDatasetEmpty(
                    f"required source dataset is empty: {query.query_id}",
                    context={"query_id": query.query_id, "cutoff": cutoff.isoformat()},
                )
            with self._session_factory(self.profile.resource_policy) as ledger_session:
                ledger_tokens = self._session_tokens(ledger_session)
                ledger_digest, _ledger_receipt = self._freeze_writer_ledger(
                    ledger_session,
                    cutoff=cutoff,
                    checkpoint=pulse,
                    read_chunk_rows=read_chunk_rows,
                )
            writer_ledger_check_count += 1
            if ledger_digest != before.writer_ledger_digest:
                raise SourceSnapshotDriftBlocked(
                    "source writer ledger changed during partition freeze",
                    context={"query_id": query.query_id},
                )
            token_receipts.extend(
                f"writer-ledger:{query.query_id}:{index}:{token}" for index, token in enumerate(ledger_tokens)
            )
        for plan in fingerprint_plans:
            with self._session_factory(self.profile.resource_policy) as verify_session:
                verify_tokens = self._session_tokens(verify_session)
                if verify_session.describe(plan.query.query_id).digest != plan.table_schema.digest:
                    raise SourceSnapshotDriftBlocked(
                        "source schema changed before fingerprint readback",
                        context={"query_id": plan.query.query_id},
                    )
                actual_fingerprint = self._partition_fingerprint(
                    verify_session,
                    query=plan.query,
                    params=plan.params,
                    spec=_query_partition_spec(
                        plan.query,
                        plan.partition_key,
                        plan.table_schema,
                    ),
                    table_schema=plan.table_schema,
                )
                actual_fingerprint = _bind_revision_dependencies(
                    actual_fingerprint,
                    plan.revision_dependencies,
                )
            if actual_fingerprint != plan.fingerprint:
                raise SourceSnapshotDriftBlocked(
                    "source partition MVCC fingerprint changed during freeze",
                    context={
                        "query_id": plan.query.query_id,
                        "partition_key": plan.partition_key,
                    },
                )
            token_receipts.extend(
                f"fingerprint-readback:{plan.query.query_id}:{plan.partition_key}:{index}:{token}"
                for index, token in enumerate(verify_tokens)
            )
        after = self._capture_control_snapshot(
            cutoff=cutoff,
            pulse=pulse,
            budget=None,
            read_chunk_rows=read_chunk_rows,
            recheck_by_identity=recheck_by_identity,
            selected_stock_codes=selected_stock_codes,
        )
        if after.consistency_digest != before.consistency_digest:
            raise SourceSnapshotDriftBlocked(
                "source control authority changed during partition freeze",
                context={
                    "before_digest": before.consistency_digest,
                    "after_digest": after.consistency_digest,
                },
            )
        token_receipts.extend(f"control-after:{index}:{token}" for index, token in enumerate(after.snapshot_tokens))
        writer_ledger_check_count += 1
        tokens = tuple(token_receipts)
        audit = before.audit
        pit_partitions = before.pit_partitions
        pit_snapshot = before.pit_snapshot
        if sector_enricher is None:
            raise SourceRequiredDatasetEmpty("sector L2 enrichment authority is missing")
        manifest = SourceManifest(tuple(item.summary for item in sealed))
        if recheck_by_identity is not None:
            _assert_recheck_partition_set(
                recheck_by_identity,
                source_partitions=sealed,
                pit_partitions=pit_partitions,
            )
            expected_source = ensure_sha256(
                str(expected_source_content_root or ""),
                field="expected_source_content_root",
            )
            expected_pit = ensure_sha256(
                str(expected_pit_snapshot_digest or ""),
                field="expected_pit_snapshot_digest",
            )
            if manifest.source_content_root != expected_source:
                raise SourceSnapshotRevised("fresh canonical source content root differs")
            if pit_snapshot.spans_sha256 != expected_pit:
                raise SourceSnapshotRevised("fresh PIT source identity differs")
            return ExactSourceRecheckSnapshot(
                official_cutoff=cutoff,
                pit_snapshot=pit_snapshot,
                manifest=manifest,
                partitions=tuple(sealed),
                pit_partitions=tuple(pit_partitions),
                snapshot_tokens=tokens,
                source_cas_usage={
                    "mode": "exact_partition_hash_only_v1",
                    "new_bytes": 0,
                    "new_partitions": 0,
                    "reused_bytes": sum(item.rows_ref.size for item in (*sealed, *pit_partitions)),
                    "reused_partitions": len(sealed) + len(pit_partitions),
                },
            )
        sector_receipt_ref = self.cas.put_json(
            sector_enricher.receipt(
                classify_partitions=[
                    {
                        "identity": item.spec.identity,
                        "content_digest": item.summary.content_digest,
                        "rows_ref": item.rows_ref.as_dict(),
                    }
                    for item in sealed
                    if item.spec.dataset == "sw_index_classify"
                ],
                member_partitions=[
                    {
                        "identity": item.spec.identity,
                        "content_digest": item.summary.content_digest,
                        "rows_ref": item.rows_ref.as_dict(),
                    }
                    for item in sealed
                    if item.spec.dataset == "sw_index_member"
                ],
            )
        )
        self.cas.verify(sector_receipt_ref)
        derived_source_receipt_refs = (sector_receipt_ref,)
        manifest_payload = {
            "schema_version": SOURCE_MANIFEST_ARTIFACT_SCHEMA,
            "authority_policy_version": SOURCE_AUTHORITY_POLICY_VERSION,
            "source_consistency_policy": SOURCE_CONSISTENCY_POLICY,
            "profile": self.profile.profile,
            "cutoff": cutoff.isoformat(),
            "source_content_root": manifest.source_content_root,
            "partitions": [item.as_build_input() for item in sorted(sealed, key=lambda value: value.spec.identity)],
            "safety": _zero_safety(),
        }
        manifest_ref = self.cas.put_json(manifest_payload)
        self.cas.verify(manifest_ref)
        reuse_manifest_ref = self.cas.put_json(
            {
                "schema_version": SOURCE_REUSE_MANIFEST_SCHEMA,
                "profile": self.profile.profile,
                "cutoff": cutoff.isoformat(),
                "source_content_root": manifest.source_content_root,
                "source_content_manifest_ref": manifest_ref.as_dict(),
                "partitions": [
                    {
                        **item.as_reuse_input(),
                        "refresh_audit_digest": item.refresh_audit_digest,
                    }
                    for item in sorted(sealed, key=lambda value: value.spec.identity)
                ],
                "safety": _zero_safety(),
            }
        )
        self.cas.verify(reuse_manifest_ref)
        audit_ref = self.cas.put_json(audit.as_receipt(profile=self.profile.profile, cutoff=cutoff))
        self.cas.verify(audit_ref)
        pit_ref = self.cas.put_bytes(pit_snapshot.canonical_bytes())
        self.cas.verify(pit_ref)
        observation_provenance_root = digest_named_fields(
            "dataset_release_source_observation_provenance_root_v1",
            {
                "stable_source_provenance_root": manifest.source_provenance_root,
                "source_content_root": manifest.source_content_root,
                "source_consistency_policy": SOURCE_CONSISTENCY_POLICY,
                "control_bracket_digest": before.consistency_digest,
                "writer_ledger_digest": before.writer_ledger_digest,
                "snapshot_tokens": list(tokens),
                "source_reuse_manifest_ref": reuse_manifest_ref.as_dict(),
                "source_refresh_audit_ref": audit_ref.as_dict(),
                "provider_receipt_refs": [],
                "derived_source_receipt_refs": [value.as_dict() for value in derived_source_receipt_refs],
                "pit_snapshot_digest": pit_snapshot.spans_sha256,
                "pit_snapshot_ref": pit_ref.as_dict(),
            },
        )
        provenance_ref = self.cas.put_json(
            {
                "schema_version": SOURCE_PROVENANCE_RECEIPT_SCHEMA,
                "authority_policy_version": SOURCE_AUTHORITY_POLICY_VERSION,
                "source_consistency_policy": SOURCE_CONSISTENCY_POLICY,
                "source_consistency_evidence": ("writer_ledger_quiescent_control_bracket_v1"),
                "untracked_direct_writes_policy": ("platform_write_contract_violation_not_observable_v1"),
                "control_bracket_digest": before.consistency_digest,
                "writer_ledger_evidence": {
                    **dict(before.writer_ledger_receipt),
                    "digest": before.writer_ledger_digest,
                    "check_count": writer_ledger_check_count,
                },
                "mvcc_fingerprint_capability_gate": (
                    "enabled_explicit_capability_v1"
                    if self._mvcc_reuse_capability
                    else "blocked_unvalidated_production_v1"
                ),
                "mvcc_fingerprint_counts": {
                    "reused_partitions": mvcc_reused_partitions,
                    "streamed_partitions": mvcc_streamed_partitions,
                    "unsupported_partitions": mvcc_unsupported_partitions,
                    "readback_partitions": len(fingerprint_plans),
                },
                "profile": self.profile.profile,
                "cutoff": cutoff.isoformat(),
                "source_content_root": manifest.source_content_root,
                "source_content_manifest_ref": manifest_ref.as_dict(),
                "source_reuse_manifest_ref": reuse_manifest_ref.as_dict(),
                "stable_source_provenance_root": manifest.source_provenance_root,
                "source_provenance_root": observation_provenance_root,
                "snapshot_tokens": list(tokens),
                "source_refresh_audit_ref": audit_ref.as_dict(),
                "pit_snapshot_digest": pit_snapshot.spans_sha256,
                "pit_snapshot_ref": pit_ref.as_dict(),
                "provider_receipt_refs": [],
                "derived_source_receipt_refs": [value.as_dict() for value in derived_source_receipt_refs],
                "partition_provenance": [
                    {
                        "identity": item.spec.identity,
                        "ingestion_audit_identity": (item.summary.ingestion_audit_identity),
                        "source_table_schema_digest": (item.source_table_schema.digest),
                        "snapshot_tokens": list(item.source_snapshot_tokens),
                    }
                    for item in sorted(sealed, key=lambda value: value.spec.identity)
                ],
                "pit_partition_provenance": [
                    {
                        "identity": item.spec.identity,
                        "content_digest": item.summary.content_digest,
                        "ingestion_audit_identity": (item.summary.ingestion_audit_identity),
                        "rows_ref": item.rows_ref.as_dict(),
                        "source_table_schema_digest": (item.source_table_schema.digest),
                    }
                    for item in sorted(pit_partitions, key=lambda value: value.spec.identity)
                ],
                "source_cas_usage": budget.as_dict(),
                "safety": _zero_safety(),
            }
        )
        self.cas.verify(provenance_ref)
        return FrozenSourceAuthoritySnapshot(
            official_cutoff=cutoff,
            pit_snapshot=pit_snapshot,
            pit_snapshot_ref=pit_ref,
            manifest=manifest,
            source_manifest_ref=manifest_ref,
            source_reuse_manifest_ref=reuse_manifest_ref,
            source_audit_ref=audit_ref,
            source_provenance_ref=provenance_ref,
            derived_source_receipt_refs=derived_source_receipt_refs,
            partitions=tuple(sealed),
            pit_partitions=tuple(pit_partitions),
            snapshot_tokens=tokens,
            observation_provenance_root=observation_provenance_root,
            source_cas_usage=budget.as_dict(),
        )

    def _capture_control_snapshot(
        self,
        *,
        cutoff: date,
        pulse: Callable[[], None],
        budget: SourceCASBudgetTracker | None,
        read_chunk_rows: int,
        recheck_by_identity: Mapping[str, Mapping[str, Any]] | None = None,
        selected_stock_codes: tuple[str, ...] = (),
    ) -> _SourceControlSnapshot:
        with self._session_factory(self.profile.resource_policy) as session:
            tokens = self._session_tokens(session)
            schemas = self._validate_required_sources(session)
            audit = self._freeze_refresh_audit(
                session,
                cutoff=cutoff,
                checkpoint=pulse,
                read_chunk_rows=read_chunk_rows,
            )
            writer_ledger_digest, writer_ledger_receipt = self._freeze_writer_ledger(
                session,
                cutoff=cutoff,
                checkpoint=pulse,
                read_chunk_rows=read_chunk_rows,
            )
            official = session.fetch_one("official_cutoff", {"cutoff": cutoff})
            if official is None or _as_date(official.get("official_cutoff")) != cutoff:
                raise OfficialCutoffMismatch(
                    "resolved cutoff is not the official final trading day of its month",
                    context={
                        "requested_cutoff": cutoff.isoformat(),
                        "official_cutoff": (str(official.get("official_cutoff")) if official else None),
                    },
                )
            pit_partitions, pit_snapshot = self._freeze_pit(
                session,
                cutoff=cutoff,
                tokens=tokens,
                pulse=pulse,
                budget=budget,
                read_chunk_rows=read_chunk_rows,
                recheck_by_identity=recheck_by_identity,
                selected_stock_codes=selected_stock_codes,
            )
        audit_receipt = audit.as_receipt(profile=self.profile.profile, cutoff=cutoff)
        consistency_digest = digest_named_fields(
            "dataset_release_source_control_bracket_v1",
            {
                "source_consistency_policy": SOURCE_CONSISTENCY_POLICY,
                "official_cutoff": cutoff,
                "schemas": {key: value.digest for key, value in sorted(schemas.items())},
                "refresh_audit": audit_receipt,
                "writer_ledger_digest": writer_ledger_digest,
                "pit_snapshot_sha256": sha256_hex(pit_snapshot.canonical_bytes()),
            },
        )
        return _SourceControlSnapshot(
            schemas=schemas,
            audit=audit,
            writer_ledger_digest=writer_ledger_digest,
            writer_ledger_receipt=writer_ledger_receipt,
            pit_partitions=pit_partitions,
            pit_snapshot=pit_snapshot,
            snapshot_tokens=tokens,
            consistency_digest=consistency_digest,
        )

    def _freeze_writer_ledger(
        self,
        session: SourceSnapshotSession,
        *,
        cutoff: date,
        checkpoint: Callable[[], None],
        read_chunk_rows: int,
    ) -> tuple[str, Mapping[str, Any]]:
        datasets = sorted(
            {str(value.audit_dataset) for value in PRODUCTION_QUERY_SPECS.values() if value.audit_dataset is not None}
        )
        rows: list[Mapping[str, Any]] = []
        active: list[Mapping[str, Any]] = []
        for row_number, row in enumerate(
            session.stream(
                "writer_ledger",
                {
                    "start": cutoff.replace(day=1),
                    "datasets": datasets,
                },
                fetch_rows=read_chunk_rows,
            ),
            start=1,
        ):
            try:
                kind = str(row["ledger_kind"])
                identity = row["ledger_identity"]
                status = str(row["status"]).lower()
            except KeyError as exc:
                raise SourceAuditIncomplete("source writer ledger row is incomplete") from exc
            if kind not in {"ingestion_jobs", "data_sync_attempts"}:
                raise SourceAuditIncomplete("source writer ledger kind is invalid")
            if not _IDENTIFIER.fullmatch(status):
                raise SourceAuditIncomplete("source writer ledger status is invalid")
            safe = {
                "ledger_kind": kind,
                "ledger_identity_sha256": _opaque_audit_digest(identity),
                "status": status,
                "created_at": _audit_timestamp_text(row.get("created_at"), field="writer.created_at"),
                "started_at": _audit_timestamp_text(
                    row.get("started_at"),
                    field="writer.started_at",
                    optional=True,
                ),
                "finished_at": _audit_timestamp_text(
                    row.get("finished_at"),
                    field="writer.finished_at",
                    optional=True,
                ),
                "opaque_payload_sha256": _opaque_audit_digest(row.get("opaque_payload")),
            }
            rows.append(safe)
            if status in {"queued", "pending", "running", "started"}:
                active.append(safe)
            if row_number % read_chunk_rows == 0:
                checkpoint()
        if active:
            raise SourceSnapshotDriftBlocked(
                "active source writer exists during source freeze",
                context={
                    "active_count": len(active),
                    "active_identity_sha256": [str(item["ledger_identity_sha256"]) for item in active[:20]],
                },
            )
        ordered = sorted(
            rows,
            key=lambda value: (
                str(value["ledger_kind"]),
                str(value["ledger_identity_sha256"]),
            ),
        )
        receipt = {
            "schema_version": "dataset_release_source_writer_ledger_v1",
            "platform_write_contract": ("all_source_mutations_must_record_ingestion_or_sync_ledger_v1"),
            "cutoff_month_start": cutoff.replace(day=1).isoformat(),
            "active_writer_count": 0,
            "rows": ordered,
            "safety": _zero_safety(),
        }
        return (
            digest_named_fields("dataset_release_source_writer_ledger_digest_v1", receipt),
            receipt,
        )

    def _session_tokens(
        self,
        session: SourceSnapshotSession,
    ) -> tuple[str, ...]:
        tokens = tuple(session.snapshot_tokens)
        if not tokens or len(tokens) != len(set(tokens)):
            raise SourceConfigurationMissing("source session returned invalid snapshot tokens")
        return tokens

    def _validate_required_sources(
        self,
        session: SourceSnapshotSession,
    ) -> Mapping[str, SourceTableSchema]:
        query_ids = (
            "pit_state",
            "pit_spans",
            "refresh_audit",
            "writer_ledger",
            *PRODUCTION_QUERY_SPECS,
        )
        return {query_id: session.describe(query_id) for query_id in query_ids}

    def _freeze_refresh_audit(
        self,
        session: SourceSnapshotSession,
        *,
        cutoff: date,
        checkpoint: Callable[[], None],
        read_chunk_rows: int | None = None,
    ) -> SourceRefreshAuditLedger:
        fetch_rows = read_chunk_rows or self.profile.resource_policy.validation_read_chunk_rows
        start = min(self.profile.start_date, self.profile.minute_start_date)
        trading_date_values: list[date] = []
        for row_number, row in enumerate(
            session.stream(
                "trading_dates",
                {"start": start, "end": cutoff},
                fetch_rows=fetch_rows,
            ),
            start=1,
        ):
            trading_date_values.append(_as_date(row.get("trade_date")))
            if row_number % fetch_rows == 0:
                checkpoint()
        trading_dates = tuple(trading_date_values)
        if not trading_dates or trading_dates != tuple(sorted(set(trading_dates))):
            raise SourceAuditIncomplete("official trading-date audit scope is invalid")
        dated_specs = tuple(value for value in PRODUCTION_QUERY_SPECS.values() if value.date_expression is not None)
        dated = sorted({str(value.audit_dataset) for value in dated_specs})
        eligible_sources: dict[str, tuple[str, ...]] = {}
        eligible_quality: dict[str, tuple[str, ...]] = {}
        for spec in dated_specs:
            audit_dataset = str(spec.audit_dataset)
            eligible_sources[audit_dataset] = tuple(
                sorted(set(eligible_sources.get(audit_dataset, ())).union(spec.audit_eligible_sources))
            )
            eligible_quality[audit_dataset] = tuple(
                sorted(set(eligible_quality.get(audit_dataset, ())).union(spec.audit_eligible_quality_statuses))
            )
        grouped: dict[tuple[str, date], list[Mapping[str, Any]]] = {}
        seen_authorities: set[tuple[str, date, str]] = set()
        for row_number, row in enumerate(
            session.stream(
                "refresh_audit",
                {"datasets": dated, "start": start, "end": cutoff},
                fetch_rows=fetch_rows,
            ),
            start=1,
        ):
            try:
                dataset = str(row["dataset"])
                trade_date = _as_date(row["trade_date"])
                data_source = str(row["data_source"])
            except KeyError as exc:
                raise SourceAuditIncomplete("refresh audit row is incomplete") from exc
            if dataset not in dated:
                raise SourceAuditIncomplete("refresh audit returned an unrequested dataset")
            if not _IDENTIFIER.fullmatch(data_source):
                raise SourceAuditIncomplete("refresh audit data_source is empty")
            authority_key = (dataset, trade_date, data_source)
            if authority_key in seen_authorities:
                raise SourceAuditIncomplete(
                    "refresh audit dataset/date/source authority is ambiguous",
                    context={
                        "dataset": dataset,
                        "trade_date": trade_date.isoformat(),
                        "data_source": data_source,
                    },
                )
            seen_authorities.add(authority_key)
            key = (dataset, trade_date)
            grouped.setdefault(key, []).append(_sanitize_refresh_audit_row(row))
            if row_number % fetch_rows == 0:
                checkpoint()
        rows = {
            key: tuple(
                sorted(
                    values,
                    key=lambda row: (
                        str(row["data_source"]),
                        str(row.get("status", "")),
                        str(row["audit_payload_sha256"]),
                    ),
                )
            )
            for key, values in grouped.items()
        }
        return SourceRefreshAuditLedger(
            rows=rows,
            trading_dates=trading_dates,
            eligible_sources=eligible_sources,
            eligible_quality_statuses=eligible_quality,
        )

    def _freeze_pit(
        self,
        session: SourceSnapshotSession,
        *,
        cutoff: date,
        tokens: tuple[str, ...],
        pulse: Callable[[], None],
        budget: SourceCASBudgetTracker | None,
        read_chunk_rows: int | None = None,
        recheck_by_identity: Mapping[str, Mapping[str, Any]] | None = None,
        selected_stock_codes: tuple[str, ...] = (),
    ) -> tuple[tuple[SealedSourcePartition, ...], FrozenPitSnapshot]:
        state = session.fetch_one("pit_state", {"universe_key": self.profile.universe_key})
        if state is None:
            raise SourceRequiredDatasetEmpty(
                "required PIT state row is missing",
                context={"universe_key": self.profile.universe_key},
            )
        expected_state = {
            "universe_key": self.profile.universe_key,
            "rule_version": self.profile.universe_rule_version,
            "scope": self.profile.pit_scope,
        }
        mismatch = {
            field: {"expected": expected, "actual": state.get(field)}
            for field, expected in expected_state.items()
            if str(state.get(field)) != expected
        }
        if mismatch:
            raise SourceConfigurationMissing(
                "PIT state identity differs from the dataset profile",
                context=mismatch,
            )
        state_schema = session.describe("pit_state")
        state_spec = PartitionSpec(
            dataset="stock_universe_pit_state",
            partition_key=self.profile.universe_key,
            query_version="pit_state_semantic_v1",
            columns=(
                ColumnSpec("universe_key", ColumnKind.STRING, True),
                ColumnSpec("rule_version", ColumnKind.STRING, True),
                ColumnSpec("scope", ColumnKind.STRING, True),
                ColumnSpec("start_date", ColumnKind.DATE, True),
                ColumnSpec("end_date", ColumnKind.DATE, True),
                ColumnSpec("status", ColumnKind.STRING, True),
                ColumnSpec("dirty", ColumnKind.BOOLEAN, True),
                ColumnSpec("state_payload", ColumnKind.STRING, True),
            ),
            primary_keys=("universe_key",),
        )
        state_partition = self._seal_rows(
            spec=state_spec,
            rows=(_select_row(state, state_spec),),
            components=_ALL_NON_INDEX,
            tokens=tokens,
            table_schema=state_schema,
            checkpoint=pulse,
            budget=budget,
            recheck_expectation=(
                _required_recheck_expectation(recheck_by_identity, state_spec.identity)
                if recheck_by_identity is not None
                else None
            ),
        )
        spans_spec = PartitionSpec(
            dataset="stock_universe_pit_spans",
            partition_key=(
                f"{self.profile.start_date.isoformat()}_{cutoff.isoformat()}"
                + (
                    "_sample-"
                    + digest_named_fields(
                        "dataset_release_pit_sample_codes_v1",
                        {"codes": list(selected_stock_codes)},
                    )[:16]
                    if selected_stock_codes
                    else ""
                )
            ),
            query_version=("pit_spans_semantic_sample_v2" if selected_stock_codes else "pit_spans_semantic_v1"),
            columns=(
                ColumnSpec("ts_code", ColumnKind.STRING, True),
                ColumnSpec("eligible_start", ColumnKind.DATE, True),
                ColumnSpec("eligible_end", ColumnKind.DATE, True),
                ColumnSpec("entry_reason", ColumnKind.STRING, False),
                ColumnSpec("exit_reason", ColumnKind.STRING, False),
                ColumnSpec("semantic_payload", ColumnKind.STRING, True),
            ),
            primary_keys=("ts_code", "eligible_start", "eligible_end"),
        )
        frozen_rows: list[dict[str, Any]] = []

        def observe(row: Mapping[str, Any]) -> None:
            frozen_rows.append(
                {
                    "ts_code": row["ts_code"],
                    "eligible_start": row["eligible_start"],
                    "eligible_end": row["eligible_end"],
                    "entry_reason": row["entry_reason"],
                    "exit_reason": row["exit_reason"],
                }
            )

        spans_schema = session.describe("pit_spans")
        spans_partition = self._seal_rows(
            spec=spans_spec,
            rows=session.stream(
                "pit_spans",
                {
                    "universe_key": self.profile.universe_key,
                    "start": self.profile.start_date,
                    "end": cutoff,
                    "codes": list(selected_stock_codes) if selected_stock_codes else None,
                },
                fetch_rows=(read_chunk_rows or self.profile.resource_policy.validation_read_chunk_rows),
            ),
            components=_ALL_NON_INDEX,
            tokens=tokens,
            table_schema=spans_schema,
            observer=observe,
            checkpoint=pulse,
            budget=budget,
            checkpoint_interval_rows=read_chunk_rows,
            recheck_expectation=(
                _required_recheck_expectation(recheck_by_identity, spans_spec.identity)
                if recheck_by_identity is not None
                else None
            ),
        )
        if not frozen_rows:
            raise SourceRequiredDatasetEmpty("frozen PIT span source is empty")
        if selected_stock_codes and tuple(sorted({str(row["ts_code"]) for row in frozen_rows})) != selected_stock_codes:
            raise SourceRequiredDatasetEmpty(
                "sample PIT source does not cover the exact allowlisted instruments",
                context={"expected": list(selected_stock_codes)},
            )
        exact_pit_source = digest_named_fields(
            "dataset_release_exact_pit_source_v1",
            {
                "state": state_partition.summary.content_digest,
                "spans": spans_partition.summary.content_digest,
            },
        )
        snapshot = freeze_pit_snapshot(
            frozen_rows,
            universe_key=self.profile.universe_key,
            rule_version=self.profile.universe_rule_version,
            scope_start=self.profile.start_date,
            cutoff=cutoff,
            state_identity=state_partition.summary.content_digest,
            source_fingerprint_sha256=exact_pit_source,
            parameter_hash=digest_named_fields(
                "dataset_release_pit_parameters_v1",
                {
                    "universe_key": self.profile.universe_key,
                    "rule_version": self.profile.universe_rule_version,
                    "scope_start": self.profile.start_date,
                    "cutoff": cutoff,
                },
            ),
            state_status=str(state["status"]),
            state_dirty=bool(state["dirty"]),
            state_start=_as_date(state["start_date"]),
            state_end=_as_date(state["end_date"]),
        )
        if self.profile.pit_authority_status == "ACTIVE_CANONICAL":
            require_canonical_source_snapshot(snapshot)
        return (state_partition, spans_partition), snapshot

    def _partition_requests(
        self,
        query: SourceQuerySpec,
        cutoff: date,
        *,
        pit_snapshot: FrozenPitSnapshot,
        selected_stock_codes: tuple[str, ...] = (),
    ) -> Iterable[tuple[str, dict[str, Any]]]:
        codes: list[str] | None = None
        if query.code_policy == "profile_index_codes":
            codes = list(self.profile.index_codes)
        elif query.code_policy in {"pit_stock_codes", "pit_minute_code_batch"}:
            pit_codes = tuple(sorted({span.ts_code for span in pit_snapshot.spans}))
            codes = list(selected_stock_codes or pit_codes)
            if not set(codes).issubset(pit_codes):
                raise SourceProviderContractError("source sample instruments escape the frozen PIT snapshot")
            if not codes:
                raise SourceRequiredDatasetEmpty("frozen PIT has no stock instruments")
        if query.start_policy == "timeless":
            params: dict[str, Any] = {}
            if codes is not None:
                params["codes"] = codes
            yield "all", params
            return
        start = self.profile.minute_start_date if query.start_policy == "minute" else self.profile.start_date
        for chunk in build_date_chunks(
            start,
            cutoff,
            months=self.profile.source_date_chunk_months,
        ):
            if query.code_policy == "pit_minute_code_batch":
                assert codes is not None
                buckets: dict[int, list[str]] = {}
                for code in codes:
                    bucket = int(sha256_hex(code.encode("utf-8"))[:16], 16) % self.profile.minute_code_bucket_count
                    buckets.setdefault(bucket, []).append(code)
                for bucket, values in sorted(buckets.items()):
                    batch = sorted(values)
                    if len(batch) > self.profile.minute_code_bucket_capacity:
                        raise SourceProviderContractError(
                            "minute stable hash bucket exceeded frozen capacity",
                            context={
                                "bucket": bucket,
                                "bucket_count": self.profile.minute_code_bucket_count,
                                "capacity": self.profile.minute_code_bucket_capacity,
                                "observed": len(batch),
                                "partition_schema_version": (self.profile.minute_partition_schema_version),
                            },
                        )
                    batch_digest = digest_named_fields(
                        self.profile.minute_partition_schema_version,
                        {"bucket": bucket, "codes": batch},
                    )
                    yield (
                        f"{chunk.start.isoformat()}_{chunk.end.isoformat()}_bucket-{bucket:04d}",
                        {
                            "start": chunk.start,
                            "end": chunk.end,
                            "codes": batch,
                            "code_membership_digest": batch_digest,
                        },
                    )
                continue
            params = {"start": chunk.start, "end": chunk.end}
            if codes is not None:
                params["codes"] = codes
            yield f"{chunk.start.isoformat()}_{chunk.end.isoformat()}", params

    def _seal_query_partition(
        self,
        session: SourceSnapshotSession,
        *,
        query: SourceQuerySpec,
        partition_key: str,
        params: Mapping[str, Any],
        tokens: tuple[str, ...],
        table_schema: SourceTableSchema,
        refresh_audit_digest: str | None = None,
        checkpoint: Callable[[], None] | None = None,
        budget: SourceCASBudgetTracker | None = None,
        pressure_rung: int = 0,
        read_chunk_rows: int | None = None,
        payload_enricher: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
        payload_observer: Callable[[Mapping[str, Any]], None] | None = None,
        recheck_expectation: Mapping[str, Any] | None = None,
    ) -> SealedSourcePartition:
        spec = _query_partition_spec(query, partition_key, table_schema)
        fetch_rows = read_chunk_rows or self.profile.resource_policy.validation_read_chunk_rows
        return self._seal_rows(
            spec=spec,
            rows=self._iter_execution_rows(
                session,
                query=query,
                semantic_params=params,
                pressure_rung=pressure_rung,
                fetch_rows=fetch_rows,
            ),
            components=query.components,
            tokens=tokens,
            table_schema=table_schema,
            refresh_audit_digest=refresh_audit_digest,
            source_order_keys=query.key_columns,
            source_payload_columns=tuple(
                dict.fromkeys(
                    (
                        *query.key_columns,
                        *query.value_columns,
                        *query.derived_value_columns,
                    )
                )
            ),
            source_non_null_value_columns=query.non_null_value_columns,
            source_partition_params_digest=digest_named_fields(
                "dataset_release_semantic_partition_params_v1",
                dict(params),
            ),
            source_code_membership_digest=(
                str(params["code_membership_digest"]) if "code_membership_digest" in params else None
            ),
            row_transform=lambda raw: _validate_query_row(
                raw,
                query,
                spec,
                payload_enricher=payload_enricher,
                payload_observer=payload_observer,
            ),
            checkpoint=checkpoint,
            budget=budget,
            max_rows=query.max_partition_rows,
            checkpoint_interval_rows=fetch_rows,
            recheck_expectation=recheck_expectation,
        )

    def _partition_fingerprint(
        self,
        session: SourceSnapshotSession,
        *,
        query: SourceQuerySpec,
        params: Mapping[str, Any],
        spec: PartitionSpec,
        table_schema: SourceTableSchema,
    ) -> Mapping[str, Any] | None:
        if not self._mvcc_reuse_capability:
            return None
        method = getattr(session, "partition_fingerprint", None)
        if not callable(method):
            return None
        value = method(
            query.query_id,
            params,
            query_version=spec.query_version,
            table_schema_digest=table_schema.digest,
        )
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise SourceManifestError("partition MVCC fingerprint is invalid")
        expected = {
            "schema_version": SOURCE_MVCC_FINGERPRINT_SCHEMA,
            "query_id": query.query_id,
            "query_version": spec.query_version,
            "table_schema_digest": table_schema.digest,
            "params_digest": digest_named_fields("dataset_release_partition_query_params_v1", dict(params)),
        }
        if any(value.get(key) != expected_value for key, expected_value in expected.items()):
            raise SourceManifestError("partition MVCC fingerprint identity differs")
        try:
            digest = ensure_sha256_text(value["fingerprint_digest"], field="fingerprint_digest")
        except KeyError as exc:
            raise SourceManifestError("partition MVCC fingerprint digest is missing") from exc
        payload = {key: raw for key, raw in value.items() if key != "fingerprint_digest"}
        if digest_named_fields(SOURCE_MVCC_FINGERPRINT_SCHEMA, payload) != digest:
            raise SourceManifestError("partition MVCC fingerprint digest differs")
        return dict(value)

    def _iter_execution_rows(
        self,
        session: SourceSnapshotSession,
        *,
        query: SourceQuerySpec,
        semantic_params: Mapping[str, Any],
        pressure_rung: int,
        fetch_rows: int,
    ) -> Iterable[Mapping[str, Any]]:
        params = self._execution_query_params(
            query,
            semantic_params,
            pressure_rung=pressure_rung,
        )
        streams = tuple(
            iter(
                session.stream(
                    query.query_id,
                    value,
                    fetch_rows=fetch_rows,
                )
            )
            for value in params
        )
        if len(streams) == 1:
            return streams[0]
        return _bounded_merge_projected_rows(streams, query_id=query.query_id)

    def _execution_query_params(
        self,
        query: SourceQuerySpec,
        semantic_params: Mapping[str, Any],
        *,
        pressure_rung: int,
    ) -> tuple[Mapping[str, Any], ...]:
        if pressure_rung < 0:
            raise SourceProviderContractError("source pressure rung is invalid")
        date_params: list[dict[str, Any]] = [dict(semantic_params)]
        if "start" in semantic_params and "end" in semantic_params:
            ladder = self.profile.pressure_ladder["date_chunk_months"]
            months = ladder[min(pressure_rung, len(ladder) - 1)]
            date_params = [
                {
                    **dict(semantic_params),
                    "start": chunk.start,
                    "end": chunk.end,
                }
                for chunk in build_date_chunks(
                    _as_date(semantic_params["start"]),
                    _as_date(semantic_params["end"]),
                    months=months,
                )
            ]
        code_values = semantic_params.get("codes")
        if code_values is None:
            return tuple(date_params)
        codes = tuple(str(item) for item in code_values)
        ladder = self.profile.pressure_ladder["minute_batch"]
        batch_size = ladder[min(pressure_rung, len(ladder) - 1)]
        result: list[Mapping[str, Any]] = []
        for value in date_params:
            for offset in range(0, len(codes), batch_size):
                result.append({**value, "codes": list(codes[offset : offset + batch_size])})
        return tuple(result)

    def _seal_rows(
        self,
        *,
        spec: PartitionSpec,
        rows: Iterable[Mapping[str, Any]],
        components: tuple[Component, ...],
        tokens: tuple[str, ...],
        table_schema: SourceTableSchema,
        observer: Callable[[Mapping[str, Any]], None] | None = None,
        refresh_audit_digest: str | None = None,
        source_order_keys: tuple[str, ...] = (),
        source_payload_columns: tuple[str, ...] = (),
        source_non_null_value_columns: tuple[str, ...] = (),
        source_partition_params_digest: str | None = None,
        source_code_membership_digest: str | None = None,
        row_transform: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
        checkpoint: Callable[[], None] | None = None,
        budget: SourceCASBudgetTracker | None = None,
        max_rows: int = 1_000_000,
        checkpoint_interval_rows: int | None = None,
        recheck_expectation: Mapping[str, Any] | None = None,
    ) -> SealedSourcePartition:
        ingestion_identity = digest_named_fields(
            "dataset_release_source_ingestion_audit_v1",
            {
                "authority_policy_version": SOURCE_AUTHORITY_POLICY_VERSION,
                "partition": spec.identity,
                "query_version": spec.query_version,
                "table_schema_digest": table_schema.digest,
            },
        )
        hasher = CanonicalPartitionHasher(
            spec,
            ingestion_audit_identity=ingestion_identity,
            snapshot_tokens=(),
        )
        monthly_hashers: dict[str, CanonicalPartitionHasher] = {}

        def encoded() -> Iterator[bytes]:
            yield (
                canonical_json_bytes(
                    {
                        "schema_version": SOURCE_PARTITION_ROWS_SCHEMA,
                        "partition_identity": spec.identity,
                        "query_version": spec.query_version,
                        "schema_digest": spec.schema_digest,
                        "columns": [item.as_dict() for item in spec.columns],
                        "primary_keys": list(spec.primary_keys),
                        "source_table_identity": table_schema.table_identity,
                        "source_table_schema_digest": table_schema.digest,
                    }
                )
                + b"\n"
            )
            for row_number, raw in enumerate(rows, start=1):
                if row_number > max_rows:
                    raise SourcePartitionRowLimitExceeded(
                        "source partition exceeded its frozen row limit",
                        context={
                            "partition": spec.identity,
                            "max_rows": max_rows,
                        },
                    )
                selected = row_transform(raw) if row_transform is not None else _select_row(raw, spec)
                normalized = hasher.update(selected)
                month = _canonical_row_month(normalized)
                if month is not None:
                    month_hasher = monthly_hashers.get(month)
                    if month_hasher is None:
                        month_hasher = CanonicalPartitionHasher(
                            replace(spec, partition_key=month),
                            ingestion_audit_identity=ingestion_identity,
                            snapshot_tokens=(),
                        )
                        monthly_hashers[month] = month_hasher
                    month_hasher.update(normalized)
                if observer is not None:
                    observer(normalized)
                yield canonical_json_bytes(normalized) + b"\n"
                if (
                    checkpoint is not None
                    and row_number
                    % (checkpoint_interval_rows or self.profile.resource_policy.validation_read_chunk_rows)
                    == 0
                ):
                    checkpoint()
                    if budget is not None:
                        budget.checkpoint()

        if budget is not None:
            budget.checkpoint()
        compression = StreamingCompressionStats()
        compressed = iter_gzip_level1(encoded(), compression)
        if recheck_expectation is None:
            put_result = self.cas.put_stream_observed(compressed)
            rows_ref = put_result.reference
        else:
            expected_ref, expected_codec_identity = _validate_recheck_expectation_before_stream(
                recheck_expectation,
                spec=spec,
                table_schema=table_schema,
            )
            try:
                rows_ref = self.cas.verify_stream_hash_only(
                    compressed,
                    expected_digest=expected_ref.sha256,
                    expected_size=expected_ref.size,
                    expected_relative_path=expected_ref.relative_path,
                    expected_codec_identity=expected_codec_identity,
                    observed_codec_identity=SOURCE_ROWS_CODEC_IDENTITY,
                )
            except (
                CASHashOnlyMismatch,
                SourceManifestError,
                SourcePartitionRowLimitExceeded,
            ) as exc:
                raise SourceSnapshotRevised(
                    "fresh canonical source partition bytes differ",
                    context={"partition": spec.identity},
                ) from exc
            put_result = None
        envelope = compression.as_descriptor_fields()
        if envelope["rows_compressed_bytes"] != rows_ref.size:
            raise SourceManifestError("source row compression size differs from persisted CAS bytes")
        if budget is not None and put_result is not None:
            budget.record(put_result)
        elif budget is not None:
            budget.record_reused(rows_ref)
        if recheck_expectation is None:
            self.cas.verify(rows_ref)
        summary = hasher.finish()
        monthly_content_leaves = tuple(
            _monthly_content_leaf(month, monthly_hashers[month].finish()) for month in sorted(monthly_hashers)
        )
        if recheck_expectation is not None:
            _assert_recheck_partition_after_stream(
                recheck_expectation,
                summary=summary,
                envelope=envelope,
                monthly_content_leaves=monthly_content_leaves,
                source_order_keys=source_order_keys or spec.primary_keys,
                source_payload_columns=source_payload_columns,
                source_non_null_value_columns=source_non_null_value_columns,
                source_partition_params_digest=source_partition_params_digest,
                source_code_membership_digest=source_code_membership_digest,
            )
        return SealedSourcePartition(
            spec=spec,
            summary=summary,
            components=components,
            rows_ref=rows_ref,
            source_table_schema=table_schema,
            rows_uncompressed_bytes=int(envelope["rows_uncompressed_bytes"]),
            rows_compressed_bytes=int(envelope["rows_compressed_bytes"]),
            rows_compression_ratio=str(envelope["rows_compression_ratio"]),
            monthly_content_leaves=monthly_content_leaves,
            source_payload_columns=source_payload_columns,
            source_non_null_value_columns=source_non_null_value_columns,
            source_snapshot_tokens=tokens,
            source_partition_params_digest=source_partition_params_digest,
            source_code_membership_digest=source_code_membership_digest,
            refresh_audit_digest=refresh_audit_digest,
            source_order_keys=source_order_keys or spec.primary_keys,
        )


def _validate_recheck_partition_expectations(
    values: Sequence[Mapping[str, Any]] | None,
    *,
    expected_source_content_root: str | None,
    expected_pit_snapshot_digest: str | None,
) -> dict[str, Mapping[str, Any]] | None:
    if values is None:
        if expected_source_content_root is not None or expected_pit_snapshot_digest is not None:
            raise SourceSnapshotRevised("recheck roots require complete partition expectations")
        return None
    ensure_sha256(
        str(expected_source_content_root or ""),
        field="expected_source_content_root",
    )
    ensure_sha256(
        str(expected_pit_snapshot_digest or ""),
        field="expected_pit_snapshot_digest",
    )
    if not values:
        raise SourceSnapshotRevised("recheck partition expectations are empty")
    result: dict[str, Mapping[str, Any]] = {}
    scopes: set[str] = set()
    for raw in values:
        if not isinstance(raw, Mapping):
            raise SourceSnapshotRevised("recheck partition expectation is invalid")
        value = dict(raw)
        scope = str(value.get("recheck_partition_scope", ""))
        identity = f"{value.get('dataset')}:{value.get('partition_key')}"
        if scope not in {"source", "pit"} or not all((value.get("dataset"), value.get("partition_key"))):
            raise SourceSnapshotRevised("recheck partition expectation identity is incomplete")
        if identity in result:
            raise SourceSnapshotRevised("recheck partition expectation identity is duplicated")
        try:
            reference = CASRef.from_value(value.get("rows_ref"))
            if reference.size < 0:
                raise ValueError("incomplete reference")
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
                raise ValueError("invalid row count")
            if (
                not isinstance(value.get("columns"), list)
                or not value["columns"]
                or not isinstance(value.get("primary_keys"), list)
                or not value["primary_keys"]
                or not isinstance(value.get("source_table_columns"), list)
                or not isinstance(value.get("source_table_types"), list)
            ):
                raise ValueError("incomplete schema")
        except (
            KeyError,
            TypeError,
            ValueError,
            CASStoreError,
            SourceManifestError,
        ) as exc:
            raise SourceSnapshotRevised(
                "recheck partition expectation contract is incomplete",
                context={"partition": identity},
            ) from exc
        value["rows_ref"] = reference.as_dict()
        result[identity] = value
        scopes.add(scope)
    if scopes != {"source", "pit"}:
        raise SourceSnapshotRevised("recheck source and PIT partition expectations are required")
    return result


def _required_recheck_expectation(values: Mapping[str, Mapping[str, Any]], identity: str) -> Mapping[str, Any]:
    try:
        return values[identity]
    except KeyError as exc:
        raise SourceSnapshotRevised(
            "fresh source planned an unexpected partition",
            context={"partition": identity},
        ) from exc


def _validate_recheck_expectation_before_stream(
    value: Mapping[str, Any],
    *,
    spec: PartitionSpec,
    table_schema: SourceTableSchema,
) -> tuple[CASRef, str]:
    expected = {
        "dataset": spec.dataset,
        "partition_key": spec.partition_key,
        "query_version": spec.query_version,
        "schema_digest": spec.schema_digest,
        "columns": [item.as_dict() for item in spec.columns],
        "primary_keys": list(spec.primary_keys),
        "timezone_name": spec.timezone_name,
        "null_marker": spec.null_marker,
        "float_non_finite_policy": spec.float_non_finite_policy,
        "source_table_identity": table_schema.table_identity,
        "source_table_schema_digest": table_schema.digest,
        "source_table_columns": list(table_schema.ordered_columns),
        "source_table_types": list(table_schema.ordered_types),
    }
    changed = sorted(field for field, expected_value in expected.items() if value.get(field) != expected_value)
    if changed:
        raise SourceSnapshotRevised(
            "fresh source partition schema or query identity differs",
            context={"partition": spec.identity, "changed_fields": changed},
        )
    try:
        reference = CASRef.from_value(value.get("rows_ref"))
        if reference.size < 0:
            raise ValueError("incomplete reference")
        validate_rows_envelope(value, cas_size=reference.size)
        codec_identity = validate_rows_codec_identity(value)
    except (TypeError, ValueError, CASStoreError, SourceManifestError) as exc:
        raise SourceSnapshotRevised(
            "fresh source partition codec expectation differs",
            context={"partition": spec.identity},
        ) from exc
    return reference, codec_identity


def _assert_recheck_partition_after_stream(
    value: Mapping[str, Any],
    *,
    summary: PartitionSummary,
    envelope: Mapping[str, Any],
    monthly_content_leaves: Sequence[Mapping[str, Any]],
    source_order_keys: Sequence[str],
    source_payload_columns: Sequence[str],
    source_non_null_value_columns: Sequence[str],
    source_partition_params_digest: str | None,
    source_code_membership_digest: str | None,
) -> None:
    observed = {
        "row_count": summary.row_count,
        "min_key": _portable_key(summary.min_key),
        "max_key": _portable_key(summary.max_key),
        "required_null_count": summary.required_null_count,
        "duplicate_count": summary.duplicate_count,
        "merkle_root": summary.merkle_root,
        "content_digest": summary.content_digest,
        "ingestion_audit_identity": summary.ingestion_audit_identity,
        "monthly_content_leaves": [dict(item) for item in monthly_content_leaves],
        "source_order_keys": list(source_order_keys),
        "source_payload_columns": list(source_payload_columns),
        "source_non_null_value_columns": list(source_non_null_value_columns),
        "source_partition_params_digest": source_partition_params_digest,
        "source_code_membership_digest": source_code_membership_digest,
        **dict(envelope),
    }
    changed = sorted(field for field, observed_value in observed.items() if value.get(field) != observed_value)
    if changed:
        raise SourceSnapshotRevised(
            "fresh canonical source partition identity differs",
            context={"partition": summary.identity, "changed_fields": changed},
        )


def _assert_recheck_partition_set(
    values: Mapping[str, Mapping[str, Any]],
    *,
    source_partitions: Sequence[SealedSourcePartition],
    pit_partitions: Sequence[SealedSourcePartition],
) -> None:
    expected_source = {
        identity for identity, value in values.items() if value.get("recheck_partition_scope") == "source"
    }
    expected_pit = {identity for identity, value in values.items() if value.get("recheck_partition_scope") == "pit"}
    actual_source = {item.spec.identity for item in source_partitions}
    actual_pit = {item.spec.identity for item in pit_partitions}
    if expected_source != actual_source or expected_pit != actual_pit:
        raise SourceSnapshotRevised(
            "fresh source partition set differs",
            context={
                "expected_source_count": len(expected_source),
                "actual_source_count": len(actual_source),
                "expected_pit_count": len(expected_pit),
                "actual_pit_count": len(actual_pit),
            },
        )


def _canonical_row_month(row: Mapping[str, Any]) -> str | None:
    """Return a stable natural-month key for date-bearing source rows.

    This is intentionally derived from the canonical normalized row consumed by
    the partition hasher.  It is never inferred later from row counts or min/max
    keys, so a moving final three-month window can prove its old monthly prefix
    byte-for-byte before an incremental plan is admitted.
    """

    value = next(
        (row[field] for field in ("trade_date", "cal_date", "trade_time") if field in row and row[field] is not None),
        None,
    )
    if value is None:
        return None
    if isinstance(value, datetime):
        observed = value.date()
    elif isinstance(value, date):
        observed = value
    else:
        text = str(value).strip()
        try:
            observed = (
                date(int(text[:4]), int(text[4:6]), int(text[6:8]))
                if len(text) >= 8 and text[:8].isdigit()
                else date.fromisoformat(text[:10])
            )
        except ValueError as exc:
            raise SourceManifestError("canonical date-bearing row has an invalid month authority") from exc
    return f"{observed.year:04d}-{observed.month:02d}"


def _monthly_content_leaf(
    month: str,
    summary: PartitionSummary,
) -> Mapping[str, Any]:
    body = {
        "schema_version": SOURCE_MONTH_CONTENT_LEAF_SCHEMA,
        "month": month,
        "row_count": summary.row_count,
        "min_key": _portable_key(summary.min_key),
        "max_key": _portable_key(summary.max_key),
        "merkle_root": summary.merkle_root,
        "content_digest": summary.content_digest,
    }
    return {
        **body,
        "leaf_identity": digest_named_fields(SOURCE_MONTH_CONTENT_LEAF_SCHEMA, body),
    }


def _validated_monthly_content_leaves(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise SourceAuditIncomplete("source monthly content leaves are invalid")
    result: list[Mapping[str, Any]] = []
    previous: str | None = None
    required = {
        "schema_version",
        "month",
        "row_count",
        "min_key",
        "max_key",
        "merkle_root",
        "content_digest",
        "leaf_identity",
    }
    for raw in value:
        if not isinstance(raw, Mapping) or set(raw) != required:
            raise SourceAuditIncomplete("source monthly content leaf schema differs")
        month = str(raw["month"])
        try:
            parsed = date.fromisoformat(f"{month}-01")
        except ValueError as exc:
            raise SourceAuditIncomplete("source monthly content leaf month is invalid") from exc
        if month != f"{parsed.year:04d}-{parsed.month:02d}" or (previous is not None and month <= previous):
            raise SourceAuditIncomplete("source monthly content leaves are duplicated or unordered")
        row_count = raw["row_count"]
        if type(row_count) is not int or row_count <= 0:
            raise SourceAuditIncomplete("source monthly content leaf row count is invalid")
        body = {
            "schema_version": SOURCE_MONTH_CONTENT_LEAF_SCHEMA,
            "month": month,
            "row_count": row_count,
            "min_key": raw["min_key"],
            "max_key": raw["max_key"],
            "merkle_root": ensure_sha256_text(raw["merkle_root"], field="monthly merkle_root"),
            "content_digest": ensure_sha256_text(raw["content_digest"], field="monthly content_digest"),
        }
        if raw["schema_version"] != SOURCE_MONTH_CONTENT_LEAF_SCHEMA or raw["leaf_identity"] != digest_named_fields(
            SOURCE_MONTH_CONTENT_LEAF_SCHEMA, body
        ):
            raise SourceAuditIncomplete("source monthly content leaf identity differs")
        result.append(
            {
                **body,
                "leaf_identity": str(raw["leaf_identity"]),
            }
        )
        previous = month
    return tuple(result)


def build_source_authority(
    profile: DatasetProfile,
    cas: CASStore,
    *,
    session_factory: SourceSessionFactory = production_source_session_factory,
) -> MonthlySourceAuthority:
    return MonthlySourceAuthority(profile, cas, session_factory=session_factory)


def seal_source_stage_receipt(
    cas: CASStore,
    snapshot: FrozenSourceAuthoritySnapshot,
    *,
    profile: str,
) -> CASRef:
    """Seal the bounded handoff from the data-bearing child to Worker parent."""

    receipt = {
        "schema_version": SOURCE_STAGE_RECEIPT_SCHEMA,
        "authority_policy_version": SOURCE_AUTHORITY_POLICY_VERSION,
        "profile": profile,
        "official_cutoff": snapshot.official_cutoff.isoformat(),
        "source_content_root": snapshot.source_content_root,
        "stable_source_provenance_root": snapshot.stable_source_provenance_root,
        "source_provenance_root": snapshot.source_provenance_root,
        "pit_snapshot_digest": snapshot.pit_snapshot_digest,
        "source_content_manifest_ref": snapshot.source_manifest_ref.as_dict(),
        "source_reuse_manifest_ref": snapshot.source_reuse_manifest_ref.as_dict(),
        "source_refresh_audit_ref": snapshot.source_audit_ref.as_dict(),
        "source_provenance_ref": snapshot.source_provenance_ref.as_dict(),
        "derived_source_receipt_refs": [value.as_dict() for value in snapshot.derived_source_receipt_refs],
        "pit_snapshot_ref": snapshot.pit_snapshot_ref.as_dict(),
        "snapshot_tokens": list(snapshot.snapshot_tokens),
        "source_cas_usage": dict(snapshot.source_cas_usage),
        "artifact_ready_contract_ref": (
            snapshot.artifact_ready_contract_ref.as_dict() if snapshot.artifact_ready_contract_ref is not None else None
        ),
        "artifact_ready_content_root": snapshot.artifact_ready_content_root,
        "artifact_ready_provenance_root": snapshot.artifact_ready_provenance_root,
        "provider_receipt_refs": [value.as_dict() for value in snapshot.provider_receipt_refs],
        "artifact_ready_derived_source_receipt_refs": [
            value.as_dict() for value in snapshot.artifact_ready_derived_source_receipt_refs
        ],
        "safety": _zero_safety(),
    }
    reference = cas.put_json(receipt)
    return cas.verify(reference)


def load_source_stage_receipt(
    cas: CASStore,
    value: CASRef | Mapping[str, Any] | str,
    *,
    expected_profile: str,
    expected_cutoff: date,
    profile: DatasetProfile | None = None,
) -> FrozenSourceAuthoritySnapshot:
    """Load and cross-check a child-produced source snapshot from bounded CAS."""

    stage_ref = _complete_cas_ref(cas, value, field="source_stage_receipt")
    stage = cas.get_json_bounded(stage_ref, max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    required_fields = {
        "schema_version",
        "authority_policy_version",
        "profile",
        "official_cutoff",
        "source_content_root",
        "stable_source_provenance_root",
        "source_provenance_root",
        "pit_snapshot_digest",
        "source_content_manifest_ref",
        "source_reuse_manifest_ref",
        "source_refresh_audit_ref",
        "source_provenance_ref",
        "derived_source_receipt_refs",
        "pit_snapshot_ref",
        "snapshot_tokens",
        "source_cas_usage",
        "artifact_ready_contract_ref",
        "artifact_ready_content_root",
        "artifact_ready_provenance_root",
        "provider_receipt_refs",
        "artifact_ready_derived_source_receipt_refs",
        "safety",
    }
    if not isinstance(stage, Mapping) or set(stage) != required_fields:
        raise SourceAuditIncomplete("source-stage receipt schema is invalid")
    if (
        stage.get("schema_version") != SOURCE_STAGE_RECEIPT_SCHEMA
        or stage.get("authority_policy_version") != SOURCE_AUTHORITY_POLICY_VERSION
        or stage.get("profile") != expected_profile
        or stage.get("official_cutoff") != expected_cutoff.isoformat()
        or stage.get("safety") != _zero_safety()
    ):
        raise SourceAuditIncomplete("source-stage receipt identity/safety differs")

    manifest_ref = _complete_cas_ref(cas, stage["source_content_manifest_ref"], field="source_content_manifest")
    reuse_ref = _complete_cas_ref(cas, stage["source_reuse_manifest_ref"], field="source_reuse_manifest")
    audit_ref = _complete_cas_ref(cas, stage["source_refresh_audit_ref"], field="source_refresh_audit")
    provenance_ref = _complete_cas_ref(cas, stage["source_provenance_ref"], field="source_provenance")
    raw_derived_refs = stage["derived_source_receipt_refs"]
    if not isinstance(raw_derived_refs, list) or not raw_derived_refs:
        raise SourceAuditIncomplete("source-stage derived receipts are missing")
    derived_refs = tuple(_complete_cas_ref(cas, value, field="derived_source_receipt") for value in raw_derived_refs)
    pit_ref = _complete_cas_ref(cas, stage["pit_snapshot_ref"], field="pit_snapshot")
    manifest_payload = cas.get_json_bounded(manifest_ref, max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    reuse_payload = cas.get_json_bounded(reuse_ref, max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    provenance = cas.get_json_bounded(provenance_ref, max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    pit_payload = cas.get_json_bounded(pit_ref, max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    _ = cas.get_json_bounded(audit_ref, max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    if (
        not isinstance(manifest_payload, Mapping)
        or manifest_payload.get("schema_version") != SOURCE_MANIFEST_ARTIFACT_SCHEMA
        or manifest_payload.get("profile") != expected_profile
        or manifest_payload.get("cutoff") != expected_cutoff.isoformat()
        or manifest_payload.get("source_consistency_policy") != SOURCE_CONSISTENCY_POLICY
        or not isinstance(reuse_payload, Mapping)
        or reuse_payload.get("schema_version") != SOURCE_REUSE_MANIFEST_SCHEMA
        or reuse_payload.get("source_content_manifest_ref") != manifest_ref.as_dict()
        or not isinstance(provenance, Mapping)
        or provenance.get("schema_version") != SOURCE_PROVENANCE_RECEIPT_SCHEMA
        or provenance.get("source_consistency_policy") != SOURCE_CONSISTENCY_POLICY
        or provenance.get("source_consistency_evidence") != "writer_ledger_quiescent_control_bracket_v1"
        or provenance.get("untracked_direct_writes_policy") != "platform_write_contract_violation_not_observable_v1"
        or provenance.get("source_content_manifest_ref") != manifest_ref.as_dict()
        or provenance.get("source_reuse_manifest_ref") != reuse_ref.as_dict()
        or provenance.get("source_refresh_audit_ref") != audit_ref.as_dict()
        or provenance.get("pit_snapshot_ref") != pit_ref.as_dict()
        or provenance.get("derived_source_receipt_refs") != [value.as_dict() for value in derived_refs]
    ):
        raise SourceAuditIncomplete("source-stage referenced receipt graph is inconsistent")
    content_rows = manifest_payload.get("partitions")
    reuse_rows = reuse_payload.get("partitions")
    if not isinstance(content_rows, list) or not isinstance(reuse_rows, list):
        raise SourceAuditIncomplete("source-stage partition manifests are invalid")
    reuse_by_identity: dict[str, Mapping[str, Any]] = {}
    for raw in reuse_rows:
        if not isinstance(raw, Mapping):
            raise SourceAuditIncomplete("source reuse partition is invalid")
        identity = f"{raw.get('dataset')}:{raw.get('partition_key')}"
        if identity in reuse_by_identity:
            raise SourceAuditIncomplete("source reuse partition identity is ambiguous")
        reuse_by_identity[identity] = raw
    partitions = tuple(_sealed_partition_from_stage(cas, raw, reuse_by_identity) for raw in content_rows)
    if len(derived_refs) != 1:
        raise SourceAuditIncomplete("source-stage derived receipt set differs")
    sector_receipt = cas.get_json_bounded(derived_refs[0], max_bytes=MAX_SOURCE_STAGE_ARTIFACT_BYTES)
    if (
        not isinstance(sector_receipt, Mapping)
        or sector_receipt.get("schema_version") != SECTOR_ENRICHMENT_SCHEMA
        or sector_receipt.get("safety") != _zero_safety()
    ):
        raise SourceAuditIncomplete("source-stage sector enrichment receipt is invalid")
    expected_classify = [
        {
            "identity": item.spec.identity,
            "content_digest": item.summary.content_digest,
            "rows_ref": item.rows_ref.as_dict(),
        }
        for item in partitions
        if item.spec.dataset == "sw_index_classify"
    ]
    expected_member = [
        {
            "identity": item.spec.identity,
            "content_digest": item.summary.content_digest,
            "rows_ref": item.rows_ref.as_dict(),
        }
        for item in partitions
        if item.spec.dataset == "sw_index_member"
    ]
    if (
        sector_receipt.get("classify_partitions") != expected_classify
        or sector_receipt.get("member_partitions") != expected_member
    ):
        raise SourceAuditIncomplete("source-stage sector enrichment lineage differs")
    manifest = SourceManifest(tuple(item.summary for item in partitions))
    source_content_root = ensure_sha256_text(stage.get("source_content_root"), field="source_content_root")
    stable_provenance_root = ensure_sha256_text(
        stage.get("stable_source_provenance_root"),
        field="stable_source_provenance_root",
    )
    observation_provenance_root = ensure_sha256_text(
        stage.get("source_provenance_root"), field="source_provenance_root"
    )
    if (
        manifest.source_content_root != source_content_root
        or manifest.source_provenance_root != stable_provenance_root
        or manifest_payload.get("source_content_root") != source_content_root
        or reuse_payload.get("source_content_root") != source_content_root
        or provenance.get("source_content_root") != source_content_root
        or provenance.get("stable_source_provenance_root") != stable_provenance_root
        or provenance.get("source_provenance_root") != observation_provenance_root
    ):
        raise SourceAuditIncomplete("source-stage content/provenance roots differ")
    snapshot_tokens = tuple(str(item) for item in stage.get("snapshot_tokens", ()))
    if (
        not snapshot_tokens
        or len(snapshot_tokens) != len(set(snapshot_tokens))
        or list(snapshot_tokens) != provenance.get("snapshot_tokens")
    ):
        raise SourceAuditIncomplete("source-stage snapshot tokens are invalid")
    try:
        control_bracket_digest = ensure_sha256_text(
            provenance["control_bracket_digest"], field="control_bracket_digest"
        )
        writer_evidence = provenance["writer_ledger_evidence"]
        writer_digest = ensure_sha256_text(writer_evidence["digest"], field="writer_ledger_digest")
        if (
            not isinstance(writer_evidence, Mapping)
            or writer_evidence.get("schema_version") != "dataset_release_source_writer_ledger_v1"
            or writer_evidence.get("active_writer_count") != 0
            or type(writer_evidence.get("check_count")) is not int
            or writer_evidence["check_count"] < 2
        ):
            raise SourceAuditIncomplete("source writer-ledger evidence is invalid")
        writer_receipt = {key: value for key, value in writer_evidence.items() if key not in {"digest", "check_count"}}
        if digest_named_fields("dataset_release_source_writer_ledger_digest_v1", writer_receipt) != writer_digest:
            raise SourceAuditIncomplete("source writer-ledger digest differs")
    except (KeyError, TypeError) as exc:
        raise SourceAuditIncomplete("source-stage consistency evidence is incomplete") from exc
    expected_observation_root = digest_named_fields(
        "dataset_release_source_observation_provenance_root_v1",
        {
            "stable_source_provenance_root": stable_provenance_root,
            "source_content_root": source_content_root,
            "source_consistency_policy": SOURCE_CONSISTENCY_POLICY,
            "control_bracket_digest": control_bracket_digest,
            "writer_ledger_digest": writer_digest,
            "snapshot_tokens": list(snapshot_tokens),
            "source_reuse_manifest_ref": reuse_ref.as_dict(),
            "source_refresh_audit_ref": audit_ref.as_dict(),
            "provider_receipt_refs": [],
            "derived_source_receipt_refs": [value.as_dict() for value in derived_refs],
            "pit_snapshot_digest": stage.get("pit_snapshot_digest"),
            "pit_snapshot_ref": pit_ref.as_dict(),
        },
    )
    if expected_observation_root != observation_provenance_root:
        raise SourceAuditIncomplete("source-stage observation provenance root differs")
    pit_snapshot = _pit_snapshot_from_stage(pit_payload)
    if (
        pit_snapshot.cutoff != expected_cutoff
        or pit_snapshot.spans_sha256
        != ensure_sha256_text(stage.get("pit_snapshot_digest"), field="pit_snapshot_digest")
        or provenance.get("pit_snapshot_digest") != pit_snapshot.spans_sha256
    ):
        raise SourceAuditIncomplete("source-stage PIT identity differs")
    source_cas_usage = _validated_source_cas_usage(stage.get("source_cas_usage"))
    artifact_ref: CASRef | None = None
    artifact_content_root: str | None = None
    artifact_provenance_root: str | None = None
    provider_refs: tuple[CASRef, ...] = ()
    artifact_derived_refs: tuple[CASRef, ...] = ()
    raw_artifact_ref = stage.get("artifact_ready_contract_ref")
    if raw_artifact_ref is None:
        if (
            stage.get("artifact_ready_content_root") is not None
            or stage.get("artifact_ready_provenance_root") is not None
            or stage.get("provider_receipt_refs") != []
            or stage.get("artifact_ready_derived_source_receipt_refs") != []
        ):
            raise SourceAuditIncomplete("source-stage artifact-ready fields are partially populated")
    else:
        if profile is None or profile.profile != expected_profile:
            raise SourceAuditIncomplete("source-stage artifact-ready validation requires exact profile")
        raw_provider_refs = stage.get("provider_receipt_refs")
        raw_artifact_derived = stage.get("artifact_ready_derived_source_receipt_refs")
        if not isinstance(raw_provider_refs, list) or not isinstance(raw_artifact_derived, list):
            raise SourceAuditIncomplete("source-stage artifact-ready evidence refs are invalid")
        artifact_ref = _complete_cas_ref(cas, raw_artifact_ref, field="artifact_ready_contract")
        provider_refs = tuple(
            _complete_cas_ref(cas, value, field="artifact_ready_provider_receipt") for value in raw_provider_refs
        )
        artifact_derived_refs = tuple(
            _complete_cas_ref(cas, value, field="artifact_ready_derived_receipt") for value in raw_artifact_derived
        )
        from .artifact_ready_source import load_artifact_ready_contract

        loaded_artifact = load_artifact_ready_contract(
            cas,
            profile,
            artifact_ref,
            expected_source_content_root=source_content_root,
            expected_pit_snapshot_digest=pit_snapshot.spans_sha256,
        )
        artifact_content_root = ensure_sha256_text(
            stage.get("artifact_ready_content_root"),
            field="artifact_ready_content_root",
        )
        artifact_provenance_root = ensure_sha256_text(
            stage.get("artifact_ready_provenance_root"),
            field="artifact_ready_provenance_root",
        )
        if (
            loaded_artifact.artifact_ready_effective_content_root != artifact_content_root
            or loaded_artifact.artifact_ready_provenance_root != artifact_provenance_root
            or loaded_artifact.payload.get("provider_receipt_refs") != [value.as_dict() for value in provider_refs]
            or loaded_artifact.payload.get("derived_source_receipt_refs")
            != [value.as_dict() for value in artifact_derived_refs]
        ):
            raise SourceAuditIncomplete("source-stage artifact-ready lineage differs")
    return FrozenSourceAuthoritySnapshot(
        official_cutoff=expected_cutoff,
        pit_snapshot=pit_snapshot,
        pit_snapshot_ref=pit_ref,
        manifest=manifest,
        source_manifest_ref=manifest_ref,
        source_reuse_manifest_ref=reuse_ref,
        source_audit_ref=audit_ref,
        source_provenance_ref=provenance_ref,
        derived_source_receipt_refs=derived_refs,
        partitions=partitions,
        pit_partitions=(),
        snapshot_tokens=snapshot_tokens,
        observation_provenance_root=observation_provenance_root,
        source_cas_usage=source_cas_usage,
        artifact_ready_contract_ref=artifact_ref,
        artifact_ready_content_root=artifact_content_root,
        artifact_ready_provenance_root=artifact_provenance_root,
        provider_receipt_refs=provider_refs,
        artifact_ready_derived_source_receipt_refs=artifact_derived_refs,
    )


def _sealed_partition_from_stage(
    cas: CASStore,
    raw: Any,
    reuse_by_identity: Mapping[str, Mapping[str, Any]],
) -> SealedSourcePartition:
    if not isinstance(raw, Mapping):
        raise SourceAuditIncomplete("source content partition is invalid")
    try:
        dataset = str(raw["dataset"])
        partition_key = str(raw["partition_key"])
        identity = f"{dataset}:{partition_key}"
        columns = tuple(
            ColumnSpec(
                str(item["name"]),
                ColumnKind(str(item["kind"])),
                bool(item["required"]),
            )
            for item in raw["columns"]
        )
        spec = PartitionSpec(
            dataset=dataset,
            partition_key=partition_key,
            query_version=str(raw["query_version"]),
            columns=columns,
            primary_keys=tuple(str(item) for item in raw["primary_keys"]),
            timezone_name=str(raw["timezone_name"]),
            null_marker=str(raw["null_marker"]),
            float_non_finite_policy=str(raw["float_non_finite_policy"]),
        )
        schema = SourceTableSchema(
            str(raw["source_table_identity"]),
            tuple(str(item) for item in raw["source_table_columns"]),
            tuple(str(item) for item in raw["source_table_types"]),
        )
        summary = PartitionSummary(
            dataset=dataset,
            partition_key=partition_key,
            query_version=spec.query_version,
            schema_digest=ensure_sha256_text(raw["schema_digest"], field="schema_digest"),
            row_count=int(raw["row_count"]),
            min_key=_restore_portable_key(raw.get("min_key")),
            max_key=_restore_portable_key(raw.get("max_key")),
            required_null_count=int(raw["required_null_count"]),
            duplicate_count=int(raw["duplicate_count"]),
            merkle_root=ensure_sha256_text(raw["merkle_root"], field="merkle_root"),
            content_digest=ensure_sha256_text(raw["content_digest"], field="content_digest"),
            ingestion_audit_identity=ensure_sha256_text(
                raw["ingestion_audit_identity"], field="ingestion_audit_identity"
            ),
            snapshot_tokens=(),
        )
        components = tuple(Component(str(item)) for item in raw["consumer_components"])
        rows_ref = _complete_cas_ref(cas, raw["rows_ref"], field=identity)
        envelope = validate_rows_envelope(raw, cas_size=rows_ref.size)
        monthly_content_leaves = _validated_monthly_content_leaves(raw["monthly_content_leaves"])
        payload_columns = tuple(str(item) for item in raw["source_payload_columns"])
        non_null_value_columns = tuple(str(item) for item in raw["source_non_null_value_columns"])
        partition_params_digest = ensure_sha256_text(
            raw["source_partition_params_digest"],
            field="source_partition_params_digest",
        )
        raw_membership_digest = raw.get("source_code_membership_digest")
        code_membership_digest = (
            ensure_sha256_text(
                raw_membership_digest,
                field="source_code_membership_digest",
            )
            if raw_membership_digest is not None
            else None
        )
        query = _required_query_spec(dataset)
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceAuditIncomplete("source content partition fields are invalid") from exc
    if (
        spec.schema_digest != summary.schema_digest
        or schema.digest != raw.get("source_table_schema_digest")
        or raw.get("cross_partition_merge") != "bounded_k_way_merge_v1"
        or not components
        or payload_columns
        != tuple(
            dict.fromkeys(
                (
                    *query.key_columns,
                    *query.value_columns,
                    *query.derived_value_columns,
                )
            )
        )
        or non_null_value_columns != query.non_null_value_columns
        or (query.code_policy == "pit_minute_code_batch") != (code_membership_digest is not None)
    ):
        raise SourceAuditIncomplete("source content partition contract differs")
    reuse = reuse_by_identity.get(identity)
    if reuse is None:
        raise SourceAuditIncomplete("source reuse partition is missing")
    stable_fields = dict(raw)
    compared_reuse = dict(reuse)
    refresh_audit_digest = compared_reuse.pop("refresh_audit_digest", None)
    revision_fingerprint = compared_reuse.pop("source_revision_fingerprint", None)
    revision_capability = str(
        compared_reuse.pop(
            "source_revision_capability",
            "full_stream_no_reuse_v1",
        )
    )
    if compared_reuse != stable_fields:
        raise SourceAuditIncomplete("source content/reuse partition differs")
    return SealedSourcePartition(
        spec=spec,
        summary=summary,
        components=components,
        rows_ref=rows_ref,
        source_table_schema=schema,
        rows_uncompressed_bytes=int(envelope["rows_uncompressed_bytes"]),
        rows_compressed_bytes=int(envelope["rows_compressed_bytes"]),
        rows_compression_ratio=str(envelope["rows_compression_ratio"]),
        monthly_content_leaves=monthly_content_leaves,
        source_payload_columns=payload_columns,
        source_non_null_value_columns=non_null_value_columns,
        source_revision_fingerprint=(dict(revision_fingerprint) if isinstance(revision_fingerprint, Mapping) else None),
        source_revision_capability=revision_capability,
        source_partition_params_digest=partition_params_digest,
        source_code_membership_digest=code_membership_digest,
        refresh_audit_digest=(
            ensure_sha256_text(refresh_audit_digest, field="refresh_audit_digest")
            if refresh_audit_digest is not None
            else None
        ),
        source_order_keys=tuple(str(item) for item in raw["source_order_keys"]),
    )


def _sealed_partition_from_reuse_baseline(
    cas: CASStore,
    raw: Mapping[str, Any],
    *,
    refresh_audit_digest: str | None,
    snapshot_tokens: tuple[str, ...],
) -> SealedSourcePartition:
    content = dict(raw)
    try:
        fingerprint = content.pop("source_revision_fingerprint")
        capability = str(content.pop("source_revision_capability"))
        content.pop("refresh_audit_digest", None)
    except KeyError as exc:
        raise SourceAuditIncomplete("baseline partition lacks MVCC reuse evidence") from exc
    identity = f"{content.get('dataset')}:{content.get('partition_key')}"
    reuse = {
        **content,
        "refresh_audit_digest": refresh_audit_digest,
    }
    partition = _sealed_partition_from_stage(cas, content, {identity: reuse})
    return replace(
        partition,
        refresh_audit_digest=refresh_audit_digest,
        source_snapshot_tokens=snapshot_tokens,
        source_revision_fingerprint=(dict(fingerprint) if isinstance(fingerprint, Mapping) else None),
        source_revision_capability=capability,
    )


def _pit_snapshot_from_stage(raw: Any) -> FrozenPitSnapshot:
    if not isinstance(raw, Mapping) or raw.get("schema_version") != "dataset_release_frozen_pit_v1":
        raise SourceAuditIncomplete("source-stage PIT artifact schema is invalid")
    try:
        scope = raw["scope"]
        snapshot = freeze_pit_snapshot(
            raw["spans"],
            universe_key=str(raw["universe_key"]),
            rule_version=str(raw["rule_version"]),
            scope_start=date.fromisoformat(str(scope["start"])),
            cutoff=date.fromisoformat(str(scope["cutoff"])),
            state_identity=str(raw["state_identity"]),
            source_fingerprint_sha256=ensure_sha256_text(
                raw["source_fingerprint_sha256"], field="source_fingerprint_sha256"
            ),
            parameter_hash=ensure_sha256_text(raw["parameter_hash"], field="parameter_hash"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceAuditIncomplete("source-stage PIT artifact fields are invalid") from exc
    if (
        snapshot.spans_sha256 != raw.get("spans_sha256")
        or len(snapshot.spans) != int(raw.get("span_count", -1))
        or snapshot.unique_instruments != int(raw.get("instrument_count", -1))
    ):
        raise SourceAuditIncomplete("source-stage PIT artifact digest/count differs")
    return snapshot


def _validated_source_cas_usage(raw: Any) -> dict[str, Any]:
    fields = {
        "new_bytes",
        "reused_bytes",
        "new_partitions",
        "reused_partitions",
        "predicted_remaining_new_bytes",
        "disk_snapshots",
    }
    if not isinstance(raw, Mapping) or set(raw) != fields:
        raise SourceAuditIncomplete("source-stage CAS usage receipt is invalid")
    for field in (
        "new_bytes",
        "reused_bytes",
        "new_partitions",
        "reused_partitions",
    ):
        if type(raw[field]) is not int or raw[field] < 0:
            raise SourceAuditIncomplete("source-stage CAS usage count is invalid")
    remaining = raw["predicted_remaining_new_bytes"]
    if remaining is not None and (type(remaining) is not int or remaining < 0):
        raise SourceAuditIncomplete("source-stage remaining-byte estimate is invalid")
    snapshots = raw["disk_snapshots"]
    if not isinstance(snapshots, list) or not all(isinstance(item, Mapping) for item in snapshots):
        raise SourceAuditIncomplete("source-stage disk snapshots are invalid")
    return {
        **{field: raw[field] for field in fields if field != "disk_snapshots"},
        "disk_snapshots": [dict(item) for item in snapshots],
    }


def _required_query_spec(query_id: str) -> SourceQuerySpec:
    try:
        return PRODUCTION_QUERY_SPECS[query_id]
    except KeyError as exc:
        raise SourceConfigurationMissing(
            "source query id is not allowlisted",
            context={"query_id": str(query_id)[:100]},
        ) from exc


def _query_partition_spec(
    query: SourceQuerySpec,
    partition_key: str,
    table_schema: SourceTableSchema,
) -> PartitionSpec:
    return PartitionSpec(
        dataset=query.query_id,
        partition_key=partition_key,
        query_version=(f"{query.query_version}:table_schema_sha256:{table_schema.digest}"),
        columns=(
            ColumnSpec("row_key", ColumnKind.STRING, True),
            ColumnSpec("row_payload", ColumnKind.STRING, True),
        ),
        primary_keys=("row_key",),
    )


def _complete_cas_ref(cas: CASStore, value: Any, *, field: str) -> CASRef:
    try:
        reference = CASRef.from_value(value)
    except Exception as exc:
        raise SourceAuditIncomplete(f"{field} baseline CAS reference is invalid") from exc
    if reference.size < 0:
        raise SourceAuditIncomplete(f"{field} baseline CAS reference lacks size")
    verified = cas.verify(reference)
    if reference.relative_path != verified.relative_path:
        raise SourceAuditIncomplete(f"{field} baseline CAS path is not canonical")
    return verified


def ensure_sha256_text(value: Any, *, field: str) -> str:
    try:
        return ensure_sha256(str(value), field=field)
    except Exception as exc:
        raise SourceAuditIncomplete(f"baseline {field} is invalid") from exc


def _validated_sample_instruments(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise SourceProviderContractError("sample instruments must be a bounded sequence")
    raw = tuple(values)
    normalized = tuple(sorted({str(value).strip().upper() for value in raw}))
    if len(normalized) != len(raw):
        raise SourceProviderContractError("sample instruments are duplicated or empty")
    if len(normalized) > 20 or any(not _STOCK_CODE.fullmatch(value) for value in normalized):
        raise SourceProviderContractError("sample instruments exceed the bounded SH/SZ allowlist contract")
    return normalized


def _restore_portable_key(value: Any) -> tuple[Any, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise SourceAuditIncomplete("baseline partition key bound is invalid")
    return tuple(value)


def _metadata_sql(query_id: str) -> str:
    if query_id == "official_cutoff":
        return _OFFICIAL_CUTOFF_SQL
    if query_id == "pit_state":
        return _PIT_STATE_SQL
    raise SourceConfigurationMissing("metadata query id is not allowlisted", context={"query_id": query_id})


def _stream_sql(query_id: str) -> str:
    if query_id == "pit_spans":
        return _PIT_SPANS_SQL
    if query_id == "refresh_audit":
        return _REFRESH_AUDIT_SQL
    if query_id == "trading_dates":
        return _TRADING_DATES_SQL
    if query_id == "writer_ledger":
        return _SOURCE_WRITER_LEDGER_SQL
    return _required_query_spec(query_id).sql


def _row_mapping(cursor: Any, row: Sequence[Any]) -> dict[str, Any]:
    description = cursor.description or ()
    names = [str(item[0]) for item in description]
    if len(names) != len(row) or len(names) != len(set(names)):
        raise SourceManifestError("source query returned an invalid row schema")
    return dict(zip(names, row))


def _row_mapping_from_names(
    names: Sequence[str],
    row: Sequence[Any],
) -> dict[str, Any]:
    if len(names) != len(row) or len(names) != len(set(names)):
        raise SourceManifestError("source query returned an invalid fixed row schema")
    return dict(zip(names, row))


def _validated_mvcc_fingerprint(
    raw: Mapping[str, Any],
    *,
    query: SourceQuerySpec,
    params: Mapping[str, Any],
    query_version: str,
    table_schema_digest: str,
) -> Mapping[str, Any]:
    try:
        row_count = int(raw["row_count"])
        min_row_xmin = int(raw["min_row_xmin"]) if raw.get("min_row_xmin") is not None else None
        max_row_xmin = int(raw["max_row_xmin"]) if raw.get("max_row_xmin") is not None else None
        timeline_id = int(raw["timeline_id"])
        xid_epoch = int(raw["xid_epoch"])
        system_identifier = str(raw["db_system_identifier"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceManifestError("partition MVCC fingerprint fields are invalid") from exc
    if (
        row_count < 0
        or timeline_id < 0
        or xid_epoch < 0
        or not system_identifier.isdigit()
        or (row_count == 0)
        != (raw.get("min_key") is None and raw.get("max_key") is None and min_row_xmin is None and max_row_xmin is None)
        or (
            row_count > 0
            and (
                not isinstance(raw.get("min_key"), str)
                or not isinstance(raw.get("max_key"), str)
                or min_row_xmin is None
                or max_row_xmin is None
                or min_row_xmin > max_row_xmin
            )
        )
    ):
        raise SourceManifestError("partition MVCC fingerprint contract differs")
    payload = {
        "schema_version": SOURCE_MVCC_FINGERPRINT_SCHEMA,
        "query_id": query.query_id,
        "query_version": query_version,
        "table_schema_digest": table_schema_digest,
        "params_digest": digest_named_fields("dataset_release_partition_query_params_v1", dict(params)),
        "db_system_identifier": system_identifier,
        "timeline_id": timeline_id,
        "xid_epoch": xid_epoch,
        "row_count": row_count,
        "min_key": raw.get("min_key"),
        "max_key": raw.get("max_key"),
        "min_row_xmin": min_row_xmin,
        "max_row_xmin": max_row_xmin,
    }
    return {
        **payload,
        "fingerprint_digest": digest_named_fields(SOURCE_MVCC_FINGERPRINT_SCHEMA, payload),
    }


def _bind_revision_dependencies(
    fingerprint: Mapping[str, Any] | None,
    dependencies: Mapping[str, str],
) -> Mapping[str, Any] | None:
    if fingerprint is None:
        return None
    if not dependencies:
        return dict(fingerprint)
    for name, digest in dependencies.items():
        ensure_sha256(str(digest), field=name)
    payload = {key: value for key, value in fingerprint.items() if key != "fingerprint_digest"}
    payload["revision_dependencies"] = {key: dependencies[key] for key in sorted(dependencies)}
    return {
        **payload,
        "fingerprint_digest": digest_named_fields(
            SOURCE_MVCC_FINGERPRINT_SCHEMA,
            payload,
        ),
    }


def _bounded_merge_projected_rows(
    streams: Sequence[Iterator[Mapping[str, Any]]],
    *,
    query_id: str,
) -> Iterator[Mapping[str, Any]]:
    """K-way merge pressure subqueries with one buffered row per cursor."""

    heap: list[tuple[str, str, int, Mapping[str, Any]]] = []
    for ordinal, stream in enumerate(streams):
        try:
            row = next(stream)
        except StopIteration:
            continue
        key = _projected_row_sort_key(row, query_id=query_id)
        heapq.heappush(heap, (*key, ordinal, row))
    while heap:
        _row_key, _payload, ordinal, row = heapq.heappop(heap)
        yield row
        try:
            following = next(streams[ordinal])
        except StopIteration:
            continue
        key = _projected_row_sort_key(following, query_id=query_id)
        heapq.heappush(heap, (*key, ordinal, following))


def _projected_row_sort_key(
    row: Mapping[str, Any],
    *,
    query_id: str,
) -> tuple[str, str]:
    try:
        row_key = row["row_key"]
        payload = row["row_payload"]
    except KeyError as exc:
        raise SourceRequiredFieldMissing(
            "source pressure subquery omitted projected envelope fields",
            context={"query_id": query_id},
        ) from exc
    if not isinstance(row_key, str) or not isinstance(payload, str):
        raise SourceManifestError(f"source pressure subquery envelope type differs: {query_id}")
    return row_key, payload


def _select_row(row: Mapping[str, Any], spec: PartitionSpec) -> dict[str, Any]:
    expected = tuple(item.name for item in spec.columns)
    missing = [name for name in expected if name not in row]
    if missing:
        raise SourceRequiredFieldMissing(
            "source query row is missing required projected fields",
            context={"partition": spec.identity, "missing_fields": missing},
        )
    return {name: row[name] for name in expected}


_TEXT_PAYLOAD_COLUMNS = frozenset(
    {
        "ts_code",
        "trade_date",
        "trade_time",
        "freq",
        "cal_date",
        "suspend_type",
        "suspend_timing",
        "list_date",
        "list_status",
        "exchange",
        "market",
        "index_code",
        "level",
        "in_date",
        "l2_code",
        "out_date",
    }
)
_BOOLEAN_PAYLOAD_COLUMNS = frozenset({"is_trading"})


def _validate_query_row(
    row: Mapping[str, Any],
    query: SourceQuerySpec,
    spec: PartitionSpec,
    *,
    payload_enricher: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    payload_observer: Callable[[Mapping[str, Any]], None] | None = None,
) -> Mapping[str, Any]:
    """Validate the SQL JSON envelope before it enters semantic hashing/CAS."""

    selected = _select_row(row, spec)
    row_key = _load_json_value(selected["row_key"], field="row_key", partition=spec.identity)
    payload = _load_json_value(selected["row_payload"], field="row_payload", partition=spec.identity)
    if not isinstance(row_key, list) or len(row_key) != len(query.key_columns) or not isinstance(payload, dict):
        raise SourceManifestError(f"source query JSON envelope shape differs: {spec.identity}")
    physical_projected = tuple(dict.fromkeys((*query.key_columns, *query.value_columns)))
    if set(payload) != set(physical_projected):
        raise SourceManifestError(f"source query payload fields differ: {spec.identity}")
    for index, column in enumerate(query.key_columns):
        key_value = row_key[index]
        payload_value = payload[column]
        if key_value is None or payload_value is None:
            raise SourceManifestError(f"source query key is NULL: {spec.identity}:{column}")
        _validate_payload_type(column, key_value, partition=spec.identity)
        _validate_payload_type(column, payload_value, partition=spec.identity)
        if canonical_json_bytes(key_value) != canonical_json_bytes(payload_value):
            raise SourceManifestError(f"source query key/payload identity differs: {spec.identity}:{column}")
    non_null = set(query.non_null_value_columns)
    for column in query.value_columns:
        value = payload[column]
        if column in non_null and value is None:
            raise SourceManifestError(f"source query required value is NULL: {spec.identity}:{column}")
        if value is not None:
            _validate_payload_type(column, value, partition=spec.identity)
    if query.derived_value_columns:
        if payload_enricher is None:
            raise SourceManifestError(f"source query derived payload enricher is missing: {spec.identity}")
        enriched = payload_enricher(payload)
        if not isinstance(enriched, Mapping):
            raise SourceManifestError(f"source query derived payload is invalid: {spec.identity}")
        payload = dict(enriched)
    final_projected = tuple(
        dict.fromkeys(
            (
                *query.key_columns,
                *query.value_columns,
                *query.derived_value_columns,
            )
        )
    )
    if set(payload) != set(final_projected):
        raise SourceManifestError(f"source query derived payload fields differ: {spec.identity}")
    for column in query.derived_value_columns:
        value = payload[column]
        if column in non_null and value is None:
            raise SourceManifestError(f"source query required derived value is NULL: {spec.identity}:{column}")
        if value is not None:
            _validate_payload_type(column, value, partition=spec.identity)
    if payload_observer is not None:
        payload_observer(dict(payload))
    # Re-encode parsed JSON so whitespace/key-order/number formatting cannot
    # become a false source-content revision.
    return {
        "row_key": canonical_json_bytes(row_key).decode("utf-8"),
        "row_payload": canonical_json_bytes(payload).decode("utf-8"),
    }


def _load_json_value(value: Any, *, field: str, partition: str) -> Any:
    if not isinstance(value, str):
        raise SourceManifestError(f"source query {field} is not JSON text: {partition}")

    def reject_constant(_value: str) -> Any:
        raise ValueError("non-finite JSON number")

    try:
        return json.loads(value, parse_constant=reject_constant)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SourceManifestError(f"source query {field} is invalid JSON: {partition}") from exc


def _validate_payload_type(column: str, value: Any, *, partition: str) -> None:
    if column in _TEXT_PAYLOAD_COLUMNS:
        if not isinstance(value, str) or not value.strip():
            raise SourceManifestError(f"source query text value is invalid: {partition}:{column}")
        return
    if column in _BOOLEAN_PAYLOAD_COLUMNS:
        if not isinstance(value, bool):
            raise SourceManifestError(f"source query boolean value is invalid: {partition}:{column}")
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SourceManifestError(f"source query numeric value is invalid: {partition}:{column}")
    if isinstance(value, float) and not math.isfinite(value):
        raise SourceManifestError(f"source query numeric value is non-finite: {partition}:{column}")


def _sanitize_refresh_audit_row(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """Retain readiness fields while replacing free-form text with digests."""

    error = row.get("error_message")
    metadata = row.get("metadata_json", row.get("metadata"))
    failure_category = row.get("failure_category")
    job_id = row.get("job_id")
    raw_identity = {str(key): value for key, value in row.items()}
    return {
        "data_source": str(row["data_source"]),
        "status": str(row.get("status", "")).lower(),
        "quality_status": str(row.get("quality_status", "")).lower(),
        "row_count": _audit_optional_int(row.get("row_count"), field="row_count"),
        "written_rows": _audit_optional_int(row.get("written_rows"), field="written_rows"),
        "expected_rows": _audit_optional_int(row.get("expected_rows"), field="expected_rows"),
        "coverage_ratio": _audit_numeric_text(row.get("coverage_ratio"), field="coverage_ratio"),
        "refreshed_at": _audit_timestamp_text(row.get("refreshed_at"), field="refreshed_at"),
        "data_max_at": _audit_timestamp_text(row.get("data_max_at"), field="data_max_at", optional=True),
        "job_id_sha256": _opaque_audit_digest(job_id),
        "error_present": error not in {None, ""},
        "error_message_sha256": (_opaque_audit_digest(error) if error not in {None, ""} else None),
        "failure_category_present": failure_category not in {None, ""},
        "failure_category_sha256": (
            _opaque_audit_digest(failure_category) if failure_category not in {None, ""} else None
        ),
        "metadata_sha256": _opaque_audit_digest(metadata),
        "audit_payload_sha256": sha256_hex(canonical_json_bytes(raw_identity)),
    }


def _opaque_audit_digest(value: Any) -> str:
    return sha256_hex(canonical_json_bytes(value))


def _audit_optional_int(value: Any, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise SourceAuditIncomplete(f"refresh audit {field} is invalid")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise SourceAuditIncomplete(f"refresh audit {field} is invalid") from exc
    if result < 0:
        raise SourceAuditIncomplete(f"refresh audit {field} is invalid")
    return result


def _audit_numeric_text(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    text = str(value)
    try:
        number = float(text)
    except (TypeError, ValueError) as exc:
        raise SourceAuditIncomplete(f"refresh audit {field} is invalid") from exc
    if not math.isfinite(number):
        raise SourceAuditIncomplete(f"refresh audit {field} is invalid")
    return text


def _audit_timestamp_text(
    value: Any,
    *,
    field: str,
    optional: bool = False,
) -> str | None:
    if value is None:
        if optional:
            return None
        raise SourceAuditIncomplete(f"refresh audit {field} is missing")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise SourceAuditIncomplete(f"refresh audit {field} is naive")
        return value.isoformat()
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SourceAuditIncomplete(f"refresh audit {field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SourceAuditIncomplete(f"refresh audit {field} is naive")
    return parsed.isoformat()


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise SourceManifestError(f"source returned an invalid date: {value!r}") from exc


def _portable_key(value: tuple[Any, ...] | None) -> Any:
    if value is None:
        return None
    return json.loads(canonical_json_bytes(list(value)).decode("utf-8"))


def _zero_safety() -> dict[str, int]:
    return {
        "database_writes": 0,
        "provider_database_writes": 0,
        "production_writes": 0,
        "production_deletes": 0,
        "production_pointer_changes": 0,
        "service_process_controls": 0,
        "candidate_writes": 0,
    }


__all__ = [
    "ExactSourceRecheckSnapshot",
    "FrozenSourceAuthoritySnapshot",
    "MonthlySourceAuthority",
    "OfficialCutoffMismatch",
    "PRODUCTION_QUERY_SPECS",
    "PostgresSourceSnapshotSession",
    "SOURCE_AUTHORITY_POLICY_VERSION",
    "SOURCE_MANIFEST_ARTIFACT_SCHEMA",
    "SOURCE_PROVENANCE_RECEIPT_SCHEMA",
    "SOURCE_REUSE_MANIFEST_SCHEMA",
    "SealedSourcePartition",
    "SourceAuthorityError",
    "SourceConfigurationMissing",
    "SourceProviderContractError",
    "SourceQuerySpec",
    "SourceRequiredDatasetEmpty",
    "SourceRequiredFieldMissing",
    "SourceRequiredTableMissing",
    "SourceSessionFactory",
    "SourceSnapshotSession",
    "SourceSnapshotRevised",
    "SourceTableSchema",
    "build_source_authority",
    "production_source_session_factory",
]
