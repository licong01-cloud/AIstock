"""Read-only Phase 1D capacity planning contracts and PostgreSQL probe."""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import psycopg2.extras
import psycopg2.sql
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonical_json_text, canonicalize
from backend.services.advisory_phase1.source_observer import (
    SOURCE_QUERY_TEMPLATES,
    SourceObserverConfigBundle,
    SourceObserverError,
    SourceQueryTemplate,
)


CAPACITY_REQUEST_SCHEMA_VERSION = "advisory_phase1_capacity_request_v1"
CAPACITY_RECEIPT_SCHEMA_VERSION = "advisory_phase1_capacity_receipt_v1"
REASON_CAPACITY_REQUEST_INVALID = "ADVISORY_PHASE1_CAPACITY_REQUEST_INVALID"
REASON_CAPACITY_STATS_UNAVAILABLE = "ADVISORY_PHASE1_CAPACITY_STATS_UNAVAILABLE"
REASON_CAPACITY_BUDGET_INSUFFICIENT = "ADVISORY_PHASE1_CAPACITY_BUDGET_INSUFFICIENT"
REASON_CAPACITY_RECEIPT_CONFLICT = "ADVISORY_PHASE1_CAPACITY_RECEIPT_CONFLICT"
REASON_PARQUET_MEASUREMENT_UNAVAILABLE = "ADVISORY_PHASE1_CAPACITY_PARQUET_MEASUREMENT_UNAVAILABLE"

CAPACITY_LOGICAL_ROLES = (
    "canonical_signals",
    "stage_candidates",
    "outcome_labels",
    "universe_outcomes",
    "source_revisions",
)
CAPACITY_APP_RELATIONS = (
    "advisory_source_availability_event",
    "advisory_capture_batch",
    "advisory_signal_observation",
    "advisory_signal_observation_version",
    "advisory_outcome_label",
    "advisory_outcome_label_payload",
    "advisory_dataset_build",
    "advisory_dataset_build_attempt",
    "advisory_dataset_snapshot",
    "advisory_dataset_snapshot_file",
    "advisory_source_observer_cursor",
    "advisory_source_observation_receipt",
)

ConnFactory = Callable[[], Iterator[Any]]


def _read_only_conn_factory() -> Iterator[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class CapacityStatus(str, Enum):
    MEASURED = "MEASURED"
    PARTIAL = "PARTIAL"
    INSUFFICIENT = "INSUFFICIENT"


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone")
    return value.astimezone(UTC)


def _sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


class CapacityPlanningRequest(BaseModel):
    """Explicit workload assumptions. No capacity default is hidden in code."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    observer_config_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    as_of_ts: datetime
    history_start_trade_date: date
    history_end_trade_date: date
    program_count_by_style: dict[str, int] = Field(min_length=1)
    candidate_depth_by_program: dict[str, int] = Field(min_length=1)
    universe_size_p50: int = Field(ge=0)
    universe_size_p95: int = Field(ge=0)
    universe_size_max: int = Field(ge=0)
    horizons: tuple[int, ...] = Field(min_length=1)
    projection_count: int = Field(ge=1)
    stage_projection_factor: int = Field(ge=1)
    revision_multiplier_p50: float = Field(ge=1.0)
    revision_multiplier_p95: float = Field(ge=1.0)
    revision_multiplier_max: float = Field(ge=1.0)
    retained_snapshot_count: int = Field(ge=1)
    concurrent_build_count: int = Field(ge=1)
    staging_copy_count: int = Field(ge=1)
    parquet_target_file_bytes: int = Field(ge=1)
    memory_budget_bytes: int = Field(ge=1)
    worker_memory_overheads: dict[str, int] = Field(min_length=3)
    store_available_bytes: int = Field(ge=0)
    orphan_reserve_bytes: int = Field(ge=0)
    concurrent_build_bytes: int = Field(ge=0)
    manifest_overhead_bytes_per_snapshot: int = Field(ge=0)
    parquet_measurement_snapshot_limit: int = Field(ge=1, le=100)
    parquet_measurement_file_limit: int = Field(ge=1, le=100_000)

    @field_validator("observer_config_hash", "query_registry_hash")
    @classmethod
    def _validate_hashes(cls, value: str) -> str:
        return _sha256(value, field_name="capacity hash")

    @field_validator("as_of_ts")
    @classmethod
    def _validate_as_of(cls, value: datetime) -> datetime:
        return _aware(value)

    @model_validator(mode="after")
    def _validate_ranges(self) -> "CapacityPlanningRequest":
        if self.history_start_trade_date > self.history_end_trade_date:
            raise ValueError("history_start_trade_date must not be after history_end_trade_date")
        if not (self.universe_size_p50 <= self.universe_size_p95 <= self.universe_size_max):
            raise ValueError("universe size percentiles must be monotonic")
        if not (self.revision_multiplier_p50 <= self.revision_multiplier_p95 <= self.revision_multiplier_max):
            raise ValueError("revision multipliers must be monotonic")
        if tuple(sorted(set(self.horizons))) != self.horizons or any(item <= 0 for item in self.horizons):
            raise ValueError("horizons must be positive, sorted, and duplicate-free")
        if any(value < 0 for value in self.program_count_by_style.values()):
            raise ValueError("program_count_by_style values must be non-negative")
        if any(value <= 0 for value in self.candidate_depth_by_program.values()):
            raise ValueError("candidate_depth_by_program values must be positive")
        if set(self.program_count_by_style) != set(self.candidate_depth_by_program):
            raise ValueError("program_count_by_style and candidate_depth_by_program keys must match")
        required_memory_keys = {"arrow_builder_bytes", "hash_buffer_bytes", "verifier_bytes"}
        if set(self.worker_memory_overheads) != required_memory_keys or any(value < 0 for value in self.worker_memory_overheads.values()):
            raise ValueError("worker_memory_overheads must contain the three explicit non-negative overhead keys")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": CAPACITY_REQUEST_SCHEMA_VERSION,
            **self.model_dump(mode="python"),
        }

    @property
    def request_hash(self) -> str:
        return canonical_json_sha256(self.canonical_payload())


class CapacityMeasurements(BaseModel):
    """Only observed/read-only measurements accepted by the projection formula."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    database_observed_at: datetime
    database_version: str = Field(min_length=1)
    trading_days: int = Field(ge=0)
    observed_partitions: int = Field(ge=0)
    source_role_count: int = Field(ge=0)
    relation_size_summary: dict[str, Any] = Field(default_factory=dict)
    row_distribution_summary: dict[str, Any] = Field(default_factory=dict)
    measured_role_row_widths: dict[str, float] = Field(default_factory=dict)
    measured_role_parquet_bytes_per_row_p95: dict[str, float] = Field(default_factory=dict)
    parquet_measurement_provenance: dict[str, Any] = Field(default_factory=dict)
    observed_partitions_by_role: dict[str, int] = Field(default_factory=dict)
    changed_partition_ratio_by_tier: dict[str, float] = Field(default_factory=dict)
    source_fetch_peak_bytes: int | None = Field(default=None, ge=0)
    missing_measurements: tuple[str, ...] = ()

    @field_validator("database_observed_at")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _aware(value)

    @model_validator(mode="after")
    def _validate_values(self) -> "CapacityMeasurements":
        if any(value <= 0 for value in self.measured_role_row_widths.values()):
            raise ValueError("measured role row widths must be positive")
        if any(value <= 0 for value in self.measured_role_parquet_bytes_per_row_p95.values()):
            raise ValueError("measured parquet bytes per row must be positive")
        if any(value < 0 for value in self.observed_partitions_by_role.values()):
            raise ValueError("observed partition counts must be non-negative")
        if set(self.changed_partition_ratio_by_tier) - {"p50", "p95", "max"} or any(
            value < 0 or value > 1 for value in self.changed_partition_ratio_by_tier.values()
        ):
            raise ValueError("changed partition ratios must be p50/p95/max values in [0, 1]")
        if tuple(sorted(set(self.missing_measurements))) != self.missing_measurements:
            raise ValueError("missing_measurements must be sorted and duplicate-free")
        return self


class CapacityPlanningReceipt(BaseModel):
    """Content-addressed capacity evidence; it is neither approval nor runtime authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_hash: str = Field(min_length=64, max_length=64)
    observer_config_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    database_observed_at: datetime
    database_version: str = Field(min_length=1)
    source_coverage_summary: dict[str, Any]
    relation_size_summary: dict[str, Any]
    row_distribution_summary: dict[str, Any]
    role_projection_summary: dict[str, Any]
    parquet_measurement_summary: dict[str, Any]
    db_transaction_budget_summary: dict[str, Any]
    memory_budget_summary: dict[str, Any]
    staging_store_summary: dict[str, Any]
    durable_store_summary: dict[str, Any]
    status: CapacityStatus
    reason_codes: tuple[str, ...] = ()
    missing_measurements: tuple[str, ...] = ()

    @field_validator("request_hash", "observer_config_hash", "query_registry_hash")
    @classmethod
    def _validate_hashes(cls, value: str) -> str:
        return _sha256(value, field_name="capacity receipt hash")

    @field_validator("database_observed_at")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _aware(value)

    @model_validator(mode="after")
    def _validate_status(self) -> "CapacityPlanningReceipt":
        if self.status is CapacityStatus.MEASURED and (self.reason_codes or self.missing_measurements):
            raise ValueError("MEASURED receipt cannot contain missing measurements or reasons")
        if self.status is CapacityStatus.PARTIAL and not self.missing_measurements:
            raise ValueError("PARTIAL receipt requires missing measurements")
        if self.status is CapacityStatus.INSUFFICIENT and REASON_CAPACITY_BUDGET_INSUFFICIENT not in self.reason_codes:
            raise ValueError("INSUFFICIENT receipt requires budget reason")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": CAPACITY_RECEIPT_SCHEMA_VERSION,
            **self.model_dump(mode="python", exclude={"reason_codes", "missing_measurements"}),
            "reason_codes": list(self.reason_codes),
            "missing_measurements": list(self.missing_measurements),
        }

    @property
    def receipt_hash(self) -> str:
        return canonical_json_sha256(self.canonical_payload())


def build_capacity_receipt(*, request: CapacityPlanningRequest, measurements: CapacityMeasurements) -> CapacityPlanningReceipt:
    """Project p50/p95/max storage from explicit workload and measured row sizes."""

    styles = tuple(sorted(request.program_count_by_style))
    program_days_by_style = {
        style: measurements.trading_days * request.program_count_by_style[style] for style in styles
    }
    signal_rows = sum(program_days_by_style[style] * request.candidate_depth_by_program[style] for style in styles)
    horizon_count = len(request.horizons)
    candidate_label_rows = signal_rows * horizon_count * request.projection_count
    stage_rows = signal_rows * request.stage_projection_factor
    missing = set(measurements.missing_measurements)
    tier_inputs = {
        "p50": (request.universe_size_p50, request.revision_multiplier_p50),
        "p95": (request.universe_size_p95, request.revision_multiplier_p95),
        "max": (request.universe_size_max, request.revision_multiplier_max),
    }
    for role in CAPACITY_LOGICAL_ROLES:
        if role not in measurements.measured_role_row_widths:
            missing.add(f"logical_row_width:{role}")
        if role not in measurements.measured_role_parquet_bytes_per_row_p95:
            missing.add(f"parquet_bytes_per_row_p95:{role}")
    for tier in tier_inputs:
        if tier not in measurements.changed_partition_ratio_by_tier:
            missing.add(f"changed_partition_ratio:{tier}")

    projection_by_tier: dict[str, Any] = {}
    for tier, (universe_size, revision_multiplier) in tier_inputs.items():
        universe_outcome_rows = (
            measurements.trading_days
            * sum(request.program_count_by_style.values())
            * universe_size
            * horizon_count
            * request.projection_count
        )
        source_event_rows = int(
            math.ceil(sum(measurements.observed_partitions_by_role.values()) * revision_multiplier)
        )
        role_rows = {
            "canonical_signals": signal_rows,
            "stage_candidates": stage_rows,
            "outcome_labels": candidate_label_rows,
            "universe_outcomes": universe_outcome_rows,
            "source_revisions": source_event_rows,
        }
        logical_bytes = {
            role: int(math.ceil(row_count * measurements.measured_role_row_widths[role]))
            for role, row_count in role_rows.items()
            if role in measurements.measured_role_row_widths
        }
        parquet_bytes = {
            role: int(math.ceil(row_count * measurements.measured_role_parquet_bytes_per_row_p95[role]))
            for role, row_count in role_rows.items()
            if role in measurements.measured_role_parquet_bytes_per_row_p95
        }
        projected_parquet_bytes = sum(parquet_bytes.values())
        changed_ratio = measurements.changed_partition_ratio_by_tier.get(tier)
        changed_snapshot_bytes = (
            int(math.ceil(projected_parquet_bytes * changed_ratio)) if changed_ratio is not None else 0
        )
        retained_store_bytes = (
            projected_parquet_bytes
            + changed_snapshot_bytes * max(0, request.retained_snapshot_count - 1)
            + request.manifest_overhead_bytes_per_snapshot * request.retained_snapshot_count
        )
        staging_peak_bytes = projected_parquet_bytes * request.staging_copy_count
        required_free_bytes = (
            staging_peak_bytes
            + request.concurrent_build_bytes * request.concurrent_build_count
            + request.orphan_reserve_bytes
        )
        projection_by_tier[tier] = {
            "role_rows": role_rows,
            "logical_uncompressed_bytes": logical_bytes,
            "projected_parquet_bytes_by_role": parquet_bytes,
            "projected_parquet_bytes": projected_parquet_bytes,
            "projected_file_count": (
                math.ceil(projected_parquet_bytes / request.parquet_target_file_bytes)
                if projected_parquet_bytes
                else 0
            ),
            "changed_partition_ratio": changed_ratio,
            "changed_snapshot_bytes": changed_snapshot_bytes,
            "staging_peak_bytes": staging_peak_bytes,
            "retained_store_bytes": retained_store_bytes,
            "required_free_bytes": required_free_bytes,
        }

    if measurements.source_fetch_peak_bytes is None:
        missing.add("source_fetch_peak_bytes")
        estimated_peak_memory = None
    else:
        estimated_peak_memory = (
            measurements.source_fetch_peak_bytes
            + request.worker_memory_overheads["arrow_builder_bytes"]
            + request.worker_memory_overheads["hash_buffer_bytes"]
            + request.worker_memory_overheads["verifier_bytes"]
        )
    concurrent_peak_memory = (
        estimated_peak_memory * request.concurrent_build_count if estimated_peak_memory is not None else None
    )
    reasons: tuple[str, ...] = ()
    max_projection = projection_by_tier["max"]
    if max_projection["required_free_bytes"] > request.store_available_bytes or (
        concurrent_peak_memory is not None and concurrent_peak_memory > request.memory_budget_bytes
    ):
        status = CapacityStatus.INSUFFICIENT
        reasons = (REASON_CAPACITY_BUDGET_INSUFFICIENT,)
    elif missing:
        status = CapacityStatus.PARTIAL
    else:
        status = CapacityStatus.MEASURED
    ordered_missing = tuple(sorted(missing))
    return CapacityPlanningReceipt(
        request_hash=request.request_hash,
        observer_config_hash=request.observer_config_hash,
        query_registry_hash=request.query_registry_hash,
        database_observed_at=measurements.database_observed_at,
        database_version=measurements.database_version,
        source_coverage_summary={
            "trading_days": measurements.trading_days,
            "observed_partitions": measurements.observed_partitions,
            "source_role_count": measurements.source_role_count,
            "observed_partitions_by_role": measurements.observed_partitions_by_role,
        },
        relation_size_summary=canonicalize(measurements.relation_size_summary),
        row_distribution_summary=canonicalize(measurements.row_distribution_summary),
        role_projection_summary={
            "program_days_by_style": program_days_by_style,
            "tiers": projection_by_tier,
        },
        parquet_measurement_summary={
            "logical_bytes_per_row_p95": measurements.measured_role_row_widths,
            "parquet_bytes_per_row_p95": measurements.measured_role_parquet_bytes_per_row_p95,
            "provenance": measurements.parquet_measurement_provenance,
        },
        db_transaction_budget_summary={"parquet_target_file_bytes": request.parquet_target_file_bytes},
        memory_budget_summary={
            "budget_bytes": request.memory_budget_bytes,
            "estimated_peak_bytes_per_worker": estimated_peak_memory,
            "concurrent_build_count": request.concurrent_build_count,
            "estimated_concurrent_peak_bytes": concurrent_peak_memory,
        },
        staging_store_summary={
            tier: {
                "staging_peak_bytes": value["staging_peak_bytes"],
                "required_free_bytes": value["required_free_bytes"],
            }
            for tier, value in projection_by_tier.items()
        },
        durable_store_summary={
            "store_available_bytes": request.store_available_bytes,
            "manifest_overhead_bytes_per_snapshot": request.manifest_overhead_bytes_per_snapshot,
            "retained_store_bytes_by_tier": {
                tier: value["retained_store_bytes"] for tier, value in projection_by_tier.items()
            },
        },
        status=status,
        reason_codes=reasons,
        missing_measurements=ordered_missing,
    )


class AdvisoryPhase1CapacityProbe:
    """Read only database inventory; source tables are never written or locked for update."""

    def __init__(self, *, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _read_only_conn_factory

    def probe(
        self,
        *,
        request: CapacityPlanningRequest,
        config: SourceObserverConfigBundle,
        registry: Mapping[str, SourceQueryTemplate] = SOURCE_QUERY_TEMPLATES,
    ) -> CapacityPlanningReceipt:
        if request.observer_config_hash != config.config_hash(registry) or request.query_registry_hash != config.query_registry_hash(registry):
            raise SourceObserverError(
                REASON_CAPACITY_REQUEST_INVALID,
                "capacity request does not bind the selected observer config and query registry",
            )
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cur.execute("SELECT clock_timestamp() AS observed_at, version() AS database_version")
                database = cur.fetchone()
                measurements = self._collect_measurements(
                    conn=conn,
                    cur=cur,
                    request=request,
                    config=config,
                    registry=registry,
                    database=database,
                )
        return build_capacity_receipt(request=request, measurements=measurements)

    @staticmethod
    def _collect_measurements(
        *,
        conn: Any,
        cur: Any,
        request: CapacityPlanningRequest,
        config: SourceObserverConfigBundle,
        registry: Mapping[str, SourceQueryTemplate],
        database: Mapping[str, Any],
    ) -> CapacityMeasurements:
        relation_sizes: dict[str, Any] = {}
        sample_row_widths: dict[str, float] = {}
        missing: set[str] = set()
        for spec in config.dataset_specs:
            template = registry.get(spec.query_template_id)
            if template is None:
                missing.add(f"query_template:{spec.query_template_id}")
                continue
            cur.execute(
                """
                SELECT c.reltuples::bigint AS estimated_rows,
                       pg_total_relation_size(c.oid)::bigint AS total_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (template.schema_name, template.table_name),
            )
            row = cur.fetchone()
            key = f"{template.schema_name}.{template.table_name}"
            if row is None:
                missing.add(f"relation:{key}")
                continue
            estimated_rows = max(0, int(row["estimated_rows"] or 0))
            total_bytes = max(0, int(row["total_bytes"] or 0))
            cur.execute(
                psycopg2.sql.SQL(
                    "SELECT min(trade_date) AS min_trade_date, max(trade_date) AS max_trade_date "
                    "FROM {}.{} WHERE trade_date >= %s AND trade_date <= %s"
                ).format(
                    psycopg2.sql.Identifier(template.schema_name), psycopg2.sql.Identifier(template.table_name)
                ),
                (request.history_start_trade_date, request.history_end_trade_date),
            )
            date_range = cur.fetchone()
            sample_date = date_range["max_trade_date"] if date_range is not None else None
            if sample_date is None:
                missing.add(f"source_sample:{key}")
                sample_summary: dict[str, Any] = {"sample_trade_date": None, "sample_row_count": 0, "sample_canonical_bytes": 0}
            else:
                sample_summary = AdvisoryPhase1CapacityProbe._measure_partition_sample(
                    conn=conn,
                    template=template,
                    trade_date=sample_date,
                    fetch_rows=config.source_fetch_rows,
                )
                if sample_summary["sample_row_count"] == 0:
                    missing.add(f"source_sample:{key}")
                else:
                    sample_row_widths[key] = sample_summary["sample_canonical_bytes"] / sample_summary["sample_row_count"]
            relation_sizes[key] = {
                "estimated_rows": estimated_rows,
                "total_bytes": total_bytes,
                "estimated_row_width": (total_bytes / estimated_rows) if estimated_rows > 0 else None,
                "min_trade_date": date_range["min_trade_date"] if date_range is not None else None,
                "max_trade_date": date_range["max_trade_date"] if date_range is not None else None,
                **sample_summary,
            }
        for table_name in CAPACITY_APP_RELATIONS:
            cur.execute(
                """
                SELECT c.reltuples::bigint AS estimated_rows,
                       pg_total_relation_size(c.oid)::bigint AS total_bytes,
                       pg_indexes_size(c.oid)::bigint AS index_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'app' AND c.relname = %s
                """,
                (table_name,),
            )
            row = cur.fetchone()
            key = f"app.{table_name}"
            if row is None:
                missing.add(f"relation:{key}")
                continue
            relation_sizes[key] = {
                "estimated_rows": max(0, int(row["estimated_rows"] or 0)),
                "total_bytes": max(0, int(row["total_bytes"] or 0)),
                "index_bytes": max(0, int(row["index_bytes"] or 0)),
            }
        datasets = tuple(spec.dataset_name for spec in config.dataset_specs)
        cur.execute(
            """
            SELECT dataset, data_source, count(*)::bigint AS observed_partitions,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY row_count) AS row_count_p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY row_count) AS row_count_p95,
                   max(row_count)::bigint AS row_count_max
            FROM market.dataset_date_refresh_audit
            WHERE dataset = ANY(%s)
              AND status = 'success'
              AND refreshed_at <= %s
              AND trade_date >= %s
              AND trade_date <= %s
            GROUP BY dataset, data_source
            """,
            (
                list(datasets),
                request.as_of_ts,
                request.history_start_trade_date,
                request.history_end_trade_date,
            ),
        )
        audit_rows = tuple(dict(row) for row in cur.fetchall())
        if not audit_rows:
            missing.add("dataset_date_refresh_audit")
        audit_by_scope = {
            (str(row["dataset"]), str(row["data_source"])): row
            for row in audit_rows
        }
        observed_partitions_by_role: dict[str, int] = {}
        row_counts_p50: list[float] = []
        row_counts_p95: list[float] = []
        row_counts_max: list[int] = []
        observed_partitions = 0
        for spec in config.dataset_specs:
            dataset_partition_count = 0
            for data_source in spec.allowed_data_sources:
                row = audit_by_scope.get((spec.dataset_name, data_source))
                if row is None:
                    missing.add(f"audit_coverage:{spec.dataset_name}:{data_source}")
                    continue
                partition_count = int(row["observed_partitions"] or 0)
                dataset_partition_count += partition_count
                observed_partitions += partition_count
                if row["row_count_p50"] is not None:
                    row_counts_p50.append(float(row["row_count_p50"]))
                if row["row_count_p95"] is not None:
                    row_counts_p95.append(float(row["row_count_p95"]))
                row_counts_max.append(int(row["row_count_max"] or 0))
            for source_role in spec.source_roles:
                observed_partitions_by_role[source_role] = (
                    observed_partitions_by_role.get(source_role, 0) + dataset_partition_count
                )
        cur.execute(
            """
            SELECT count(*)::bigint AS trading_days,
                   min(cal_date) AS min_trade_date,
                   max(cal_date) AS max_trade_date
            FROM market.trading_calendar
            WHERE is_trading = TRUE AND cal_date >= %s AND cal_date <= %s
            """,
            (request.history_start_trade_date, request.history_end_trade_date),
        )
        calendar = dict(cur.fetchone() or {})
        trading_days = int(calendar.get("trading_days") or 0)
        if trading_days == 0:
            missing.add("trading_day_coverage")
        parquet_measurement = AdvisoryPhase1CapacityProbe._collect_parquet_measurements(
            cur=cur,
            snapshot_limit=request.parquet_measurement_snapshot_limit,
            file_limit=request.parquet_measurement_file_limit,
        )
        missing.update(parquet_measurement["missing"])
        estimated_widths = list(sample_row_widths.values())
        source_fetch_peak_bytes = None
        if estimated_widths:
            source_fetch_peak_bytes = int(max(estimated_widths) * config.source_fetch_rows)
        else:
            missing.add("source_fetch_peak_bytes")
        return CapacityMeasurements(
            database_observed_at=database["observed_at"],
            database_version=str(database["database_version"]),
            trading_days=trading_days,
            observed_partitions=observed_partitions,
            source_role_count=sum(len(spec.source_roles) for spec in config.dataset_specs),
            relation_size_summary=relation_sizes,
            row_distribution_summary={
                "row_count_p50": max(row_counts_p50) if row_counts_p50 else None,
                "row_count_p95": max(row_counts_p95) if row_counts_p95 else None,
                "row_count_max": max(row_counts_max) if row_counts_max else 0,
                "calendar_min_trade_date": calendar.get("min_trade_date"),
                "calendar_max_trade_date": calendar.get("max_trade_date"),
            },
            measured_role_row_widths=parquet_measurement["logical_widths"],
            measured_role_parquet_bytes_per_row_p95=parquet_measurement["parquet_widths"],
            parquet_measurement_provenance=parquet_measurement["provenance"],
            observed_partitions_by_role=observed_partitions_by_role,
            changed_partition_ratio_by_tier=parquet_measurement["changed_ratios"],
            source_fetch_peak_bytes=source_fetch_peak_bytes,
            missing_measurements=tuple(sorted(missing)),
        )

    @staticmethod
    def _collect_parquet_measurements(*, cur: Any, snapshot_limit: int, file_limit: int) -> dict[str, Any]:
        cur.execute(
            """
            SELECT snapshot_id, snapshot_content_hash, manifest_sha256, writer_version,
                   base_snapshot_id, sealed_at
            FROM app.advisory_dataset_snapshot
            WHERE snapshot_state = 'SEALED'
            ORDER BY sealed_at DESC, snapshot_id
            LIMIT %s
            """,
            (snapshot_limit,),
        )
        snapshots = tuple(dict(row) for row in cur.fetchall())
        if not snapshots:
            return {
                "logical_widths": {},
                "parquet_widths": {},
                "changed_ratios": {},
                "provenance": {},
                "missing": {"sealed_snapshot_measurement"},
            }
        snapshot_ids = {str(row["snapshot_id"]) for row in snapshots}
        snapshot_ids.update(str(row["base_snapshot_id"]) for row in snapshots if row.get("base_snapshot_id"))
        cur.execute(
            """
            SELECT snapshot_id, logical_path, logical_role, sha256, size_bytes, row_count, content_uri
            FROM app.advisory_dataset_snapshot_file
            WHERE snapshot_id = ANY(%s) AND row_count > 0
            ORDER BY snapshot_id, logical_role, logical_path
            LIMIT %s
            """,
            (sorted(snapshot_ids), file_limit + 1),
        )
        files = tuple(dict(row) for row in cur.fetchall())
        if len(files) > file_limit:
            return {
                "logical_widths": {},
                "parquet_widths": {},
                "changed_ratios": {},
                "provenance": {},
                "missing": {"parquet_measurement_file_limit"},
            }

        logical_samples: dict[str, list[float]] = {role: [] for role in CAPACITY_LOGICAL_ROLES}
        parquet_samples: dict[str, list[float]] = {role: [] for role in CAPACITY_LOGICAL_ROLES}
        missing: set[str] = set()
        selected_ids = {str(row["snapshot_id"]) for row in snapshots}
        selected_files = [row for row in files if str(row["snapshot_id"]) in selected_ids]
        for row in selected_files:
            role = str(row["logical_role"])
            if role not in logical_samples:
                continue
            row_count = int(row["row_count"])
            size_bytes = int(row["size_bytes"])
            try:
                parquet_rows, uncompressed_bytes = AdvisoryPhase1CapacityProbe._parquet_metadata(
                    uri=str(row["content_uri"]),
                    expected_size_bytes=size_bytes,
                )
            except (OSError, ValueError):
                missing.add(f"parquet_metadata:{role}")
                continue
            if parquet_rows != row_count or parquet_rows <= 0:
                missing.add(f"parquet_metadata:{role}")
                continue
            logical_samples[role].append(uncompressed_bytes / parquet_rows)
            parquet_samples[role].append(size_bytes / parquet_rows)

        logical_widths: dict[str, float] = {}
        parquet_widths: dict[str, float] = {}
        for role in CAPACITY_LOGICAL_ROLES:
            if logical_samples[role] and parquet_samples[role]:
                logical_widths[role] = _nearest_rank(logical_samples[role], 0.95)
                parquet_widths[role] = _nearest_rank(parquet_samples[role], 0.95)
            else:
                missing.add(f"parquet_role_measurement:{role}")

        files_by_snapshot: dict[str, dict[str, dict[str, Any]]] = {}
        for row in files:
            files_by_snapshot.setdefault(str(row["snapshot_id"]), {})[str(row["logical_path"])] = row
        changed_samples: list[float] = []
        for snapshot in snapshots:
            snapshot_id = str(snapshot["snapshot_id"])
            current = files_by_snapshot.get(snapshot_id, {})
            total_bytes = sum(int(row["size_bytes"]) for row in current.values())
            if total_bytes <= 0:
                missing.add(f"snapshot_file_measurement:{snapshot_id}")
                continue
            base_snapshot_id = snapshot.get("base_snapshot_id")
            base = files_by_snapshot.get(str(base_snapshot_id), {}) if base_snapshot_id else {}
            changed_bytes = sum(
                int(row["size_bytes"])
                for path, row in current.items()
                if path not in base or str(base[path]["sha256"]) != str(row["sha256"])
            )
            changed_samples.append(changed_bytes / total_bytes)
        changed_ratios = (
            {
                "p50": _nearest_rank(changed_samples, 0.50),
                "p95": _nearest_rank(changed_samples, 0.95),
                "max": max(changed_samples),
            }
            if changed_samples
            else {}
        )
        if not changed_ratios:
            missing.add("changed_partition_ratio")
        provenance_rows = [
            {
                "snapshot_id": str(row["snapshot_id"]),
                "snapshot_content_hash": str(row["snapshot_content_hash"]),
                "manifest_sha256": str(row["manifest_sha256"]),
                "writer_version": str(row["writer_version"]),
            }
            for row in snapshots
        ]
        provenance = {
            "measurement_source": "app.advisory_dataset_snapshot_file:SEALED",
            "snapshot_count": len(snapshots),
            "file_count": len(selected_files),
            "snapshot_set_hash": canonical_json_sha256(provenance_rows),
            "snapshots": provenance_rows,
        }
        return {
            "logical_widths": logical_widths,
            "parquet_widths": parquet_widths,
            "changed_ratios": changed_ratios,
            "provenance": provenance,
            "missing": missing,
        }

    @staticmethod
    def _parquet_metadata(*, uri: str, expected_size_bytes: int) -> tuple[int, int]:
        parsed = urlparse(uri)
        if parsed.scheme not in {"", "file"}:
            raise ValueError("capacity measurement only accepts local verified Parquet URIs")
        raw_path = unquote(parsed.path) if parsed.scheme == "file" else uri
        if len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
            raw_path = raw_path[1:]
        path = Path(raw_path)
        if path.stat().st_size != expected_size_bytes:
            raise ValueError("verified Parquet size differs from snapshot evidence")
        import pyarrow.parquet as pq

        metadata = pq.ParquetFile(path).metadata
        if metadata is None:
            raise ValueError("Parquet metadata is unavailable")
        uncompressed_bytes = sum(metadata.row_group(index).total_byte_size for index in range(metadata.num_row_groups))
        return int(metadata.num_rows), int(uncompressed_bytes)

    @staticmethod
    def _measure_partition_sample(
        *,
        conn: Any,
        template: SourceQueryTemplate,
        trade_date: date,
        fetch_rows: int,
    ) -> dict[str, Any]:
        cursor_name = f"advisory_capacity_{template.table_name}_{trade_date:%Y%m%d}"
        source_cur = conn.cursor(name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            source_cur.itersize = fetch_rows
            source_cur.execute(template.sql, (trade_date,))
            rows = source_cur.fetchmany(fetch_rows)
            expected_columns = tuple(column.name for column in template.columns)
            actual_columns = tuple(description.name for description in source_cur.description or ())
            if actual_columns != expected_columns:
                raise SourceObserverError(
                    REASON_CAPACITY_STATS_UNAVAILABLE,
                    "capacity sample projection differs from compiled source template",
                    context={"template_id": template.template_id, "expected": expected_columns, "actual": actual_columns},
                )
            canonical_bytes = sum(
                len(canonical_json_text(canonicalize({column: row[column] for column in expected_columns})).encode("utf-8")) + 1
                for row in rows
            )
            return {
                "sample_trade_date": trade_date,
                "sample_row_count": len(rows),
                "sample_canonical_bytes": canonical_bytes,
            }
        finally:
            source_cur.close()


def _nearest_rank(values: list[float], percentile: float) -> float:
    if not values or percentile <= 0 or percentile > 1:
        raise ValueError("nearest-rank percentile requires values and percentile in (0, 1]")
    ordered = sorted(values)
    return float(ordered[max(0, math.ceil(percentile * len(ordered)) - 1)])
