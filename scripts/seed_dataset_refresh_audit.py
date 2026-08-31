"""Plan, apply, and verify historical dataset readiness reconstruction.

The monthly dataset release path intentionally treats
``market.dataset_date_refresh_audit`` as a required readiness authority.  This
operator utility reconstructs only the missing ``physical_audit_seed`` rows
from already-present physical tables.  It never downloads source data, changes
candidate datasets, or relaxes the dataset-release source contract.

Safety defaults:

* ``--mode plan`` is read-only and is the default;
* every database target is explicit (``dev`` or ``production``);
* ``apply`` requires a non-secret authorization reference;
* ``validate-dml`` exercises one DEV upsert/readback and always rolls back;
* production ``apply`` additionally requires a matching successful DEV DML
  validation receipt (legacy full DEV apply receipts remain readable);
* the complete requested range is planned before the first production write;
  sparse legal empties and registered candidate-local repairs do not become
  false hard blockers;
* writes use the one registered ``physical_audit_seed`` authority and are
  followed by exact readback in the same transaction.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.dataset_release.profile import load_dataset_profile  # noqa: E402
from backend.services.dataset_release.source_authority import (  # noqa: E402
    POSTGRES_NUMERIC_NON_FINITE_MARKERS,
    PRODUCTION_QUERY_SPECS,
    STK_LIMIT_REPAIRABLE_NUMERIC_COLUMNS,
)


SCHEMA_VERSION = "dataset_release_physical_audit_seed_v2"
RECEIPT_SCHEMA_VERSION = "dataset_release_physical_audit_seed_receipt_v2"
AUTHORITY = "physical_audit_seed"
VERIFY_BATCH_SIZE = 200
MAX_EXPECTED_DATES = 10_000
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_AUTHORIZATION_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/#-]{2,255}$")
_TARGETS = {"dev", "production"}
_MODES = {"plan", "apply", "verify", "validate-dml"}
_SPARSE_DATASETS = frozenset({"bak_basic", "suspend_d"})
_CANDIDATE_REPAIRABLE_DATASETS = frozenset({"index_daily", "stk_limit"})
DEV_DML_CONTRACT_DIGEST = hashlib.sha256(
    b"dataset_refresh_audit_dev_transactional_readback_v1"
).hexdigest()


class AuditSeedError(RuntimeError):
    """Fail-closed operator error with no credential payload."""


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    target: str
    host: str
    port: int
    user: str
    password: str
    dbname: str
    credential_location: str

    @property
    def identity_digest(self) -> str:
        return _digest(
            {
                "schema_version": "dataset_release_database_target_identity_v1",
                "target": self.target,
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "dbname": self.dbname,
            }
        )


@dataclass(frozen=True, slots=True)
class AuditSeedSpec:
    query_id: str
    dataset: str
    schema_name: str
    table_name: str
    date_expression: str
    start_policy: str
    sparse_ok: bool
    candidate_repairable: bool
    non_null_columns: tuple[str, ...]
    code_policy: str | None
    eligible_sources: tuple[str, ...]
    eligible_quality_statuses: tuple[str, ...]

    @property
    def table_identity(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


@dataclass(frozen=True, slots=True)
class PlannedAuditRow:
    dataset: str
    trade_date: dt.date
    row_count: int
    quality_status: str
    table_identity: str

    def canonical(self) -> Mapping[str, Any]:
        return {
            "dataset": self.dataset,
            "trade_date": self.trade_date.isoformat(),
            "row_count": self.row_count,
            "quality_status": self.quality_status,
            "table_identity": self.table_identity,
        }


@dataclass(frozen=True, slots=True)
class PhysicalDayObservation:
    row_count: int
    invalid_rows: int = 0
    missing_required_codes: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return self.row_count > 0 and self.invalid_rows == 0 and not self.missing_required_codes


@dataclass(frozen=True, slots=True)
class DatasetPlan:
    spec: AuditSeedSpec
    start_date: dt.date
    end_date: dt.date
    expected_dates: int
    existing_ready_dates: int
    planned_rows: tuple[PlannedAuditRow, ...]
    blocked_dates: tuple[dt.date, ...]

    def summary(self) -> Mapping[str, Any]:
        return {
            "dataset": self.spec.dataset,
            "table": self.spec.table_identity,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "expected_dates": self.expected_dates,
            "existing_ready_dates": self.existing_ready_dates,
            "planned_rows": len(self.planned_rows),
            "blocked_dates": len(self.blocked_dates),
            "blocked_sample": [value.isoformat() for value in self.blocked_dates[:20]],
            "sparse_ok": self.spec.sparse_ok,
        }


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _registered_specs() -> Mapping[str, AuditSeedSpec]:
    result: dict[str, AuditSeedSpec] = {}
    for source in PRODUCTION_QUERY_SPECS.values():
        if source.audit_dataset is None:
            continue
        if AUTHORITY not in source.audit_eligible_sources:
            raise AuditSeedError(f"{source.query_id}: physical_audit_seed is not a registered authority")
        dataset = str(source.audit_dataset)
        if dataset in result:
            raise AuditSeedError(f"{dataset}: duplicate source-audit registration")
        physical_non_null_columns = tuple(source.audit_non_null_value_columns)
        if not all(
            _IDENTIFIER.fullmatch(value)
            for value in (
                dataset,
                source.schema_name,
                source.table_name,
                source.query_id,
                *physical_non_null_columns,
            )
        ):
            raise AuditSeedError("source-audit registration contains an unsafe identifier")
        if source.date_expression is None:
            raise AuditSeedError(f"{dataset}: dated audit registration lacks date expression")
        result[dataset] = AuditSeedSpec(
            query_id=source.query_id,
            dataset=dataset,
            schema_name=source.schema_name,
            table_name=source.table_name,
            date_expression=str(source.date_expression),
            start_policy=str(source.start_policy),
            sparse_ok=dataset in _SPARSE_DATASETS,
            candidate_repairable=dataset in _CANDIDATE_REPAIRABLE_DATASETS,
            non_null_columns=physical_non_null_columns,
            code_policy=source.code_policy,
            eligible_sources=tuple(source.audit_eligible_sources),
            eligible_quality_statuses=tuple(source.audit_eligible_quality_statuses),
        )
    if not result:
        raise AuditSeedError("dataset-release source authority has no dated audit registrations")
    return dict(sorted(result.items()))


SPECS = _registered_specs()


def _load_database_config(target: str, env_file: Path) -> DatabaseConfig:
    if target not in _TARGETS:
        raise AuditSeedError("database target must be dev or production")
    resolved = env_file.resolve(strict=True)
    values = dotenv_values(resolved)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    names = {
        "host": f"{prefix}HOST",
        "port": f"{prefix}PORT",
        "user": f"{prefix}USER",
        "password": f"{prefix}PASSWORD",
        "dbname": f"{prefix}NAME",
    }
    missing = [key for key, name in names.items() if not str(values.get(name) or "").strip()]
    if missing:
        raise AuditSeedError(f"{target} database credential location is missing required keys: {missing}")
    try:
        port = int(str(values[names["port"]]))
    except (TypeError, ValueError) as exc:
        raise AuditSeedError(f"{target} database port is invalid") from exc
    if not 1 <= port <= 65535:
        raise AuditSeedError(f"{target} database port is invalid")
    return DatabaseConfig(
        target=target,
        host=str(values[names["host"]]).strip(),
        port=port,
        user=str(values[names["user"]]).strip(),
        password=str(values[names["password"]]),
        dbname=str(values[names["dbname"]]).strip(),
        credential_location=str(resolved),
    )


def _connect(config: DatabaseConfig) -> Any:
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.dbname,
        application_name="AIstock-dataset-release-audit-seed",
        options=(
            "-c client_encoding=utf8 "
            "-c statement_timeout=300000 "
            "-c max_parallel_workers_per_gather=0"
        ),
    )


def _expected_dates(conn: Any, start: dt.date, end: dt.date) -> tuple[dt.date, ...]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT cal_date::date
            FROM market.trading_calendar
            WHERE is_trading = TRUE AND cal_date >= %s AND cal_date <= %s
            ORDER BY cal_date
            """,
            (start, end),
        )
        result = tuple(row[0] for row in cursor.fetchall())
    if not result or len(result) > MAX_EXPECTED_DATES:
        raise AuditSeedError("trading-calendar range is empty or exceeds the bounded audit limit")
    return result


def _physical_counts(
    conn: Any,
    spec: AuditSeedSpec,
    start: dt.date,
    end: dt.date,
    *,
    profile: Any,
) -> Mapping[dt.date, PhysicalDayObservation]:
    if spec.query_id == "kline_minute_raw":
        predicate = "source_row.trade_time >= %s::date AND source_row.trade_time < %s::date"
    else:
        predicate = f"{spec.date_expression} >= %s AND {spec.date_expression} < %s"
    if spec.dataset == "trading_calendar":
        predicate += " AND source_row.is_trading = TRUE"
    invalid_terms: list[str] = []
    for column in spec.non_null_columns:
        invalid_terms.append(f"source_row.{column} IS NULL")
        if spec.dataset == "stk_limit" and column in STK_LIMIT_REPAIRABLE_NUMERIC_COLUMNS:
            markers = ",".join(f"'{value}'" for value in POSTGRES_NUMERIC_NON_FINITE_MARKERS)
            invalid_terms.append(f"source_row.{column}::text IN ({markers})")
    invalid_expression = " OR ".join(invalid_terms) or "FALSE"
    if spec.code_policy == "profile_index_codes":
        sql = f"""
            SELECT ({spec.date_expression})::date AS trade_date,
                   COUNT(*)::bigint,
                   ARRAY_AGG(DISTINCT source_row.ts_code ORDER BY source_row.ts_code),
                   COUNT(*) FILTER (WHERE {invalid_expression})::bigint
            FROM {spec.table_identity} AS source_row
            WHERE {predicate} AND source_row.ts_code = ANY(%s)
            GROUP BY ({spec.date_expression})::date
            ORDER BY ({spec.date_expression})::date
        """
    elif spec.code_policy in {"pit_stock_codes", "pit_minute_code_batch"}:
        sql = f"""
            SELECT ({spec.date_expression})::date AS trade_date,
                   COUNT(*)::bigint,
                   NULL::text[],
                   COUNT(*) FILTER (WHERE {invalid_expression})::bigint
            FROM {spec.table_identity} AS source_row
            WHERE {predicate}
              AND EXISTS (
                  SELECT 1
                  FROM market.stock_universe_pit_spans AS pit
                  WHERE pit.universe_key = %s
                    AND pit.ts_code = source_row.ts_code
                    AND ({spec.date_expression})::date
                        BETWEEN pit.eligible_start AND pit.eligible_end
              )
            GROUP BY ({spec.date_expression})::date
            ORDER BY ({spec.date_expression})::date
        """
    else:
        sql = f"""
            SELECT ({spec.date_expression})::date AS trade_date,
                   COUNT(*)::bigint,
                   NULL::text[],
                   COUNT(*) FILTER (WHERE {invalid_expression})::bigint
            FROM {spec.table_identity} AS source_row
            WHERE {predicate}
            GROUP BY ({spec.date_expression})::date
            ORDER BY ({spec.date_expression})::date
        """
    result: dict[dt.date, PhysicalDayObservation] = {}
    chunk_months = int(profile.source_date_chunk_months)
    if not 1 <= chunk_months <= 3:
        raise AuditSeedError("profile source date chunk exceeds the audit seed hard boundary")
    for chunk_start, chunk_end in _date_chunks(start, end, months=chunk_months):
        if spec.code_policy == "profile_index_codes":
            parameters = (
                chunk_start,
                chunk_end + dt.timedelta(days=1),
                [value.daily_code for value in profile.indices],
            )
        elif spec.code_policy in {"pit_stock_codes", "pit_minute_code_batch"}:
            parameters = (
                chunk_start,
                chunk_end + dt.timedelta(days=1),
                profile.universe_key,
            )
        else:
            parameters = (chunk_start, chunk_end + dt.timedelta(days=1))
        with conn.cursor() as cursor:
            cursor.execute(sql, parameters)
            rows = cursor.fetchall()
        if len(rows) > MAX_EXPECTED_DATES:
            raise AuditSeedError(f"{spec.dataset}: physical count result exceeds bounded date limit")
        for trade_date, row_count, observed_codes, invalid_count in rows:
            if trade_date in result:
                raise AuditSeedError(f"{spec.dataset}: chunked physical counts overlap")
            missing_required_codes: tuple[str, ...] = ()
            if spec.code_policy == "profile_index_codes":
                required = {
                    value.daily_code for value in profile.indices if value.required_from <= trade_date
                }
                missing_required_codes = tuple(sorted(required.difference(set(observed_codes or ()))))
            result[trade_date] = PhysicalDayObservation(
                row_count=int(row_count),
                invalid_rows=int(invalid_count),
                missing_required_codes=missing_required_codes,
            )
    return result


def _date_chunks(start: dt.date, end: dt.date, *, months: int) -> tuple[tuple[dt.date, dt.date], ...]:
    chunks: list[tuple[dt.date, dt.date]] = []
    cursor = start
    while cursor <= end:
        zero_based = cursor.year * 12 + (cursor.month - 1) + months
        next_boundary = dt.date(zero_based // 12, zero_based % 12 + 1, 1)
        chunk_end = min(end, next_boundary - dt.timedelta(days=1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + dt.timedelta(days=1)
    return tuple(chunks)


def _existing_ready_dates(
    conn: Any,
    spec: AuditSeedSpec,
    start: dt.date,
    end: dt.date,
) -> frozenset[dt.date]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT trade_date
            FROM market.dataset_date_refresh_audit
            WHERE dataset = %s
              AND trade_date >= %s AND trade_date <= %s
              AND data_source = ANY(%s)
              AND status = 'success'
              AND quality_status = ANY(%s)
            """,
            (
                spec.dataset,
                start,
                end,
                [value for value in spec.eligible_sources if value != AUTHORITY],
                list(spec.eligible_quality_statuses),
            ),
        )
        return frozenset(row[0] for row in cursor.fetchall())


def _build_dataset_plan(
    spec: AuditSeedSpec,
    *,
    start: dt.date,
    end: dt.date,
    expected_dates: Sequence[dt.date],
    physical_counts: Mapping[dt.date, PhysicalDayObservation | int],
    existing_ready_dates: Iterable[dt.date],
) -> DatasetPlan:
    ready = frozenset(existing_ready_dates)
    planned: list[PlannedAuditRow] = []
    blocked: list[dt.date] = []
    for trade_date in expected_dates:
        if trade_date in ready:
            continue
        raw_observation = physical_counts.get(trade_date, 0)
        observation = (
            raw_observation
            if isinstance(raw_observation, PhysicalDayObservation)
            else PhysicalDayObservation(row_count=int(raw_observation))
        )
        if observation.complete:
            planned.append(
                PlannedAuditRow(
                    dataset=spec.dataset,
                    trade_date=trade_date,
                    row_count=observation.row_count,
                    quality_status="ok",
                    table_identity=spec.table_identity,
                )
            )
        elif spec.candidate_repairable and (
            spec.dataset == "stk_limit" or observation.invalid_rows == 0
        ):
            planned.append(
                PlannedAuditRow(
                    dataset=spec.dataset,
                    trade_date=trade_date,
                    row_count=observation.row_count,
                    quality_status="candidate_repairable",
                    table_identity=spec.table_identity,
                )
            )
        elif spec.sparse_ok and observation.row_count == 0 and observation.invalid_rows == 0:
            planned.append(
                PlannedAuditRow(
                    dataset=spec.dataset,
                    trade_date=trade_date,
                    row_count=0,
                    quality_status="empty_valid",
                    table_identity=spec.table_identity,
                )
            )
        else:
            blocked.append(trade_date)
    return DatasetPlan(
        spec=spec,
        start_date=start,
        end_date=end,
        expected_dates=len(expected_dates),
        existing_ready_dates=len(ready.intersection(expected_dates)),
        planned_rows=tuple(planned),
        blocked_dates=tuple(blocked),
    )


def _profile_start(profile: Any, spec: AuditSeedSpec) -> dt.date:
    return profile.minute_start_date if spec.start_policy == "minute" else profile.start_date


def build_plan(
    conn: Any,
    *,
    profile: Any,
    end_date: dt.date,
    datasets: Sequence[str],
) -> tuple[DatasetPlan, ...]:
    plans: list[DatasetPlan] = []
    expected_cache: dict[dt.date, tuple[dt.date, ...]] = {}
    for dataset in datasets:
        spec = SPECS[dataset]
        start = _profile_start(profile, spec)
        if start > end_date:
            raise AuditSeedError(f"{dataset}: profile start is after requested end date")
        expected = expected_cache.get(start)
        if expected is None:
            expected = _expected_dates(conn, start, end_date)
            expected_cache[start] = expected
        plans.append(
            _build_dataset_plan(
                spec,
                start=start,
                end=end_date,
                expected_dates=expected,
                physical_counts=_physical_counts(conn, spec, start, end_date, profile=profile),
                existing_ready_dates=_existing_ready_dates(conn, spec, start, end_date),
            )
        )
    return tuple(plans)


def _plan_digest(profile: Any, end_date: dt.date, plans: Sequence[DatasetPlan]) -> str:
    return _digest(
        {
            "schema_version": SCHEMA_VERSION,
            "profile": profile.profile,
            "profile_config_digest": profile.config_digest,
            "semantic_profile_digest": profile.semantic_profile_digest,
            "end_date": end_date.isoformat(),
            "authority": AUTHORITY,
            "datasets": [
                {
                    "summary": plan.summary(),
                    "planned_rows": [row.canonical() for row in plan.planned_rows],
                    "blocked_dates": [value.isoformat() for value in plan.blocked_dates],
                }
                for plan in plans
            ],
        }
    )


def _all_planned_rows(plans: Sequence[DatasetPlan]) -> tuple[PlannedAuditRow, ...]:
    return tuple(row for plan in plans for row in plan.planned_rows)


def _require_apply_authorization(
    *,
    target: str,
    authorization_ref: str | None,
    dev_receipt: Path | None,
    profile: Any,
    end_date: dt.date,
    datasets: Sequence[str],
) -> None:
    if not authorization_ref or not _AUTHORIZATION_REF.fullmatch(authorization_ref):
        raise AuditSeedError("apply requires a bounded non-secret --authorization-ref")
    if target != "production":
        return
    if dev_receipt is None:
        raise AuditSeedError("production apply requires --dev-receipt from successful DEV DML validation")
    value = json.loads(dev_receipt.resolve(strict=True).read_text(encoding="utf-8"))
    common = (
        value.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or value.get("database_target") != "dev"
        or value.get("status") != "PASS"
        or value.get("profile") != profile.profile
        or value.get("profile_config_digest") != profile.config_digest
        or value.get("semantic_profile_digest") != profile.semantic_profile_digest
        or value.get("required_failures") != 0
    )
    legacy_full_apply = (
        value.get("mode") == "apply"
        and value.get("end_date") == end_date.isoformat()
        and value.get("dataset_names") == list(datasets)
    )
    transactional_validation = (
        value.get("mode") == "validate-dml"
        and value.get("dev_dml_contract_digest") == DEV_DML_CONTRACT_DIGEST
        and value.get("transaction_rolled_back") is True
        and value.get("rows_changed") == 1
    )
    if common or not (legacy_full_apply or transactional_validation):
        raise AuditSeedError("DEV receipt does not authorize this production contract")


def _dev_validation_row(conn: Any, *, end_date: dt.date) -> PlannedAuditRow:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT MAX(cal_date)::date
            FROM market.trading_calendar
            WHERE is_trading = TRUE AND cal_date <= %s
            """,
            (end_date,),
        )
        row = cursor.fetchone()
    if row is None or row[0] is None:
        raise AuditSeedError("DEV DML validation has no trading-calendar row")
    return PlannedAuditRow(
        dataset="trading_calendar",
        trade_date=row[0],
        row_count=1,
        quality_status="ok",
        table_identity="market.trading_calendar",
    )


def _apply_rows(conn: Any, rows: Sequence[PlannedAuditRow], *, metadata: Mapping[str, Any]) -> int:
    values = [
        (
            row.dataset,
            row.trade_date,
            AUTHORITY,
            "success",
            row.row_count,
            0,
            row.quality_status,
            psycopg2.extras.Json({**metadata, "table": row.table_identity}),
        )
        for row in rows
    ]
    if not values:
        return 0
    with conn.cursor() as cursor:
        changed = psycopg2.extras.execute_values(
            cursor,
            """
            INSERT INTO market.dataset_date_refresh_audit AS existing (
                dataset,trade_date,data_source,status,row_count,written_rows,quality_status,metadata
            ) VALUES %s
            ON CONFLICT (dataset,trade_date,data_source) DO UPDATE SET
                status=EXCLUDED.status,
                row_count=EXCLUDED.row_count,
                written_rows=EXCLUDED.written_rows,
                quality_status=EXCLUDED.quality_status,
                failure_category=NULL,
                error_message=NULL,
                refreshed_at=NOW(),
                metadata=EXCLUDED.metadata
            WHERE (existing.status,existing.row_count,existing.written_rows,
                   existing.quality_status,existing.failure_category,existing.error_message,existing.metadata)
                  IS DISTINCT FROM
                  (EXCLUDED.status,EXCLUDED.row_count,EXCLUDED.written_rows,
                   EXCLUDED.quality_status,NULL,NULL,EXCLUDED.metadata)
            RETURNING existing.dataset,existing.trade_date
            """,
            values,
            page_size=VERIFY_BATCH_SIZE,
            fetch=True,
        )
        return len(changed)


def _verify_rows(conn: Any, rows: Sequence[PlannedAuditRow]) -> int:
    failures = 0
    for offset in range(0, len(rows), VERIFY_BATCH_SIZE):
        batch = rows[offset : offset + VERIFY_BATCH_SIZE]
        datasets = [row.dataset for row in batch]
        dates = [row.trade_date for row in batch]
        expected = {(row.dataset, row.trade_date): row for row in batch}
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH requested(dataset,trade_date) AS (
                    SELECT * FROM unnest(%s::text[],%s::date[])
                )
                SELECT a.dataset,a.trade_date,a.status,a.row_count,a.quality_status
                FROM requested r
                LEFT JOIN market.dataset_date_refresh_audit a
                  ON a.dataset=r.dataset AND a.trade_date=r.trade_date
                 AND a.data_source='physical_audit_seed'
                """,
                (datasets, dates),
            )
            observed = {
                (row[0], row[1]): (row[2], int(row[3]), row[4])
                for row in cursor.fetchall()
                if row[0] is not None
            }
        for key, planned in expected.items():
            if observed.get(key) != ("success", planned.row_count, planned.quality_status):
                failures += 1
    return failures


def _receipt(
    *,
    mode: str,
    target: DatabaseConfig,
    profile: Any,
    end_date: dt.date,
    plans: Sequence[DatasetPlan],
    plan_digest: str,
    authorization_ref: str | None,
    rows_changed: int,
    required_failures: int,
) -> Mapping[str, Any]:
    blocked = sum(len(plan.blocked_dates) for plan in plans)
    planned = sum(len(plan.planned_rows) for plan in plans)
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mode": mode,
        "status": "PASS" if blocked == 0 and required_failures == 0 else "BLOCKED",
        "database_target": target.target,
        "database_identity_digest": target.identity_digest,
        "credential_location": target.credential_location,
        "credential_values_recorded": False,
        "profile": profile.profile,
        "profile_config_digest": profile.config_digest,
        "semantic_profile_digest": profile.semantic_profile_digest,
        "end_date": end_date.isoformat(),
        "authority": AUTHORITY,
        "plan_digest": plan_digest,
        "authorization_ref": authorization_ref,
        "dataset_count": len(plans),
        "dataset_names": [plan.spec.dataset for plan in plans],
        "planned_rows": planned,
        "apply_required": planned > 0,
        "coverage_ready_before_apply": planned == 0 and blocked == 0,
        "blocked_dates": blocked,
        "rows_changed": rows_changed,
        "committed_rows_changed": 0 if mode == "validate-dml" else rows_changed,
        "required_failures": required_failures,
        "dev_dml_contract_digest": DEV_DML_CONTRACT_DIGEST if mode == "validate-dml" else None,
        "transaction_rolled_back": mode == "validate-dml",
        "datasets": [plan.summary() for plan in plans],
        "safety": {
            "candidate_writes": 0,
            "production_pointer_changes": 0,
            "source_table_writes": 0,
            "audit_table_rows_changed": 0 if mode == "validate-dml" else rows_changed,
            "database_dml_executed": mode in {"apply", "validate-dml"},
            "provider_calls": 0,
            "service_process_controls": 0,
        },
    }


def _write_receipt(path: Path, value: Mapping[str, Any], *, allowed_root: Path) -> None:
    resolved = path.resolve(strict=False)
    root = allowed_root.resolve(strict=False)
    if resolved.parent != root:
        raise AuditSeedError("receipt path must be a direct child of the profile operator_receipts root")
    root.mkdir(parents=True, exist_ok=True)
    if resolved.exists():
        raise AuditSeedError("receipt path already exists; receipts are immutable")
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{resolved.name}.", suffix=".partial", dir=resolved.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=str(REPO_ROOT / "configs/datasets/qe_backtest_monthly_v2.yaml"))
    parser.add_argument("--database", choices=sorted(_TARGETS), required=True)
    parser.add_argument("--mode", choices=sorted(_MODES), default="plan")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--dataset", action="append", choices=sorted(SPECS), default=[])
    parser.add_argument("--env-file", default=str(REPO_ROOT / ".env"))
    parser.add_argument("--authorization-ref")
    parser.add_argument("--dev-receipt")
    parser.add_argument("--receipt-path")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    connection_factory: Callable[[DatabaseConfig], Any] = _connect,
) -> int:
    args = _parser().parse_args(argv)
    profile = load_dataset_profile(args.profile)
    end_date = dt.date.fromisoformat(args.end_date)
    datasets = tuple(args.dataset or sorted(SPECS))
    target = _load_database_config(args.database, Path(args.env_file))
    if args.mode in {"apply", "validate-dml"}:
        if args.mode == "validate-dml" and args.database != "dev":
            raise AuditSeedError("validate-dml is restricted to the existing DEV database")
        _require_apply_authorization(
            target=args.database,
            authorization_ref=args.authorization_ref,
            dev_receipt=Path(args.dev_receipt) if args.dev_receipt else None,
            profile=profile,
            end_date=end_date,
            datasets=datasets,
        )

    conn = connection_factory(target)
    rows_changed = 0
    required_failures = 0
    try:
        conn.set_session(readonly=args.mode not in {"apply", "validate-dml"}, autocommit=False)
        if args.mode == "validate-dml":
            validation_row = _dev_validation_row(conn, end_date=end_date)
            validation_spec = SPECS["trading_calendar"]
            plans = (
                DatasetPlan(
                    spec=validation_spec,
                    start_date=validation_row.trade_date,
                    end_date=validation_row.trade_date,
                    expected_dates=1,
                    existing_ready_dates=0,
                    planned_rows=(validation_row,),
                    blocked_dates=(),
                ),
            )
        else:
            plans = build_plan(conn, profile=profile, end_date=end_date, datasets=datasets)
        digest = _plan_digest(profile, end_date, plans)
        blocked = sum(len(plan.blocked_dates) for plan in plans)
        rows = _all_planned_rows(plans)
        if args.mode == "validate-dml":
            rows_changed = _apply_rows(
                conn,
                rows,
                metadata={
                    "schema_version": SCHEMA_VERSION,
                    "mode": "dev_transactional_dml_validation",
                    "profile": profile.profile,
                    "profile_config_digest": profile.config_digest,
                    "semantic_profile_digest": profile.semantic_profile_digest,
                    "dev_dml_contract_digest": DEV_DML_CONTRACT_DIGEST,
                    "validation_run_id": uuid.uuid4().hex,
                    "database_target": target.target,
                    "database_identity_digest": target.identity_digest,
                    "authorization_ref": args.authorization_ref,
                    "script": "scripts/seed_dataset_refresh_audit.py",
                },
            )
            required_failures = _verify_rows(conn, rows)
            if rows_changed != 1 or required_failures:
                raise AuditSeedError("DEV transactional DML validation readback differs")
            conn.rollback()
        elif args.mode == "apply":
            if blocked:
                raise AuditSeedError("physical source gaps block the entire audit seed transaction")
            rows_changed = _apply_rows(
                conn,
                rows,
                metadata={
                    "schema_version": SCHEMA_VERSION,
                    "mode": "physical_audit_seed",
                    "profile": profile.profile,
                    "profile_config_digest": profile.config_digest,
                    "semantic_profile_digest": profile.semantic_profile_digest,
                    "plan_digest": digest,
                    "database_target": target.target,
                    "database_identity_digest": target.identity_digest,
                    "authorization_ref": args.authorization_ref,
                    "script": "scripts/seed_dataset_refresh_audit.py",
                },
            )
            required_failures = _verify_rows(conn, rows)
            if required_failures:
                raise AuditSeedError("audit seed readback differs; transaction rolled back")
            conn.commit()
        elif args.mode == "verify":
            required_failures = _verify_rows(conn, rows)
            conn.rollback()
        else:
            conn.rollback()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()

    value = _receipt(
        mode=args.mode,
        target=target,
        profile=profile,
        end_date=end_date,
        plans=plans,
        plan_digest=digest,
        authorization_ref=args.authorization_ref,
        rows_changed=rows_changed,
        required_failures=required_failures,
    )
    if args.receipt_path:
        _write_receipt(
            Path(args.receipt_path),
            value,
            allowed_root=Path(profile.control_root) / "operator_receipts",
        )
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0 if value["status"] == "PASS" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AuditSeedError, OSError, ValueError, json.JSONDecodeError, psycopg2.Error) as exc:
        envelope = {
            "schema_version": "dataset_release_physical_audit_seed_error_v1",
            "error_code": "AUDIT_SEED_BLOCKED",
            "exception_type": type(exc).__name__,
            "message_sha256": hashlib.sha256(f"{type(exc).__name__}\0{exc}".encode()).hexdigest(),
            "safety": {
                "candidate_writes": 0,
                "production_pointer_changes": 0,
                "service_process_controls": 0,
            },
        }
        sys.stderr.write(json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n")
        raise SystemExit(2)
