"""QE-only compact persistence for F-014 Phase 3 evaluation receipts."""

from __future__ import annotations

import base64
import json
import math
import re
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Callable, Mapping, Sequence

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.long_trend_artifact_store import (
    ALLOWED_ARTIFACT_SCHEMAS,
    MANIFEST_SCHEMA,
    artifact_uri,
    evaluation_uri,
    required_artifact_matrix,
)
from backend.services.quantevolver.long_trend_evaluation_contract import canonical_sha256
from backend.services.quantevolver.long_trend_evaluation_control_repository import (
    QE_RUN_TYPES,
    QE_SOURCE_SYSTEMS,
    QELongTrendControlLease,
)
from backend.services.quantevolver.qe_dataset_contract import QE_DATASET_CONTRACT_ID

METRIC_TABLE = "qe_archive.run_evaluation_metric"
ARTIFACT_TABLE = "qe_archive.run_evaluation_artifact"
CONTROL_TABLE = "qe_archive.run_evaluation"
SCHEMA_VERSION = "qe_archive_v5_20260728"
TERMINAL_SCHEMA = "qe_long_trend_worker_terminal_v1"
PUBLISHED_SCHEMA = "qe_long_trend_published_compact_v1"
DIMENSION_SCHEMA = "qelt_metric_dimension_v2"
ARTIFACT_METADATA_SCHEMA = "qelt_evaluation_artifact_metadata_v1"
MAX_VALUE_JSON_BYTES = 64_000
MAX_METADATA_JSON_BYTES = 256_000
INSERT_PAGE_SIZE = 500
MAX_CURSOR_LENGTH = 4096
ALLOWED_HORIZONS = frozenset({20, 40, 60, 120, 180})
ALLOWED_SLICES = frozenset({"all_oos", "last_252_signal_days", "last_126_signal_days"})
ALLOWED_BARRIERS = frozenset({0.30, 0.50, 0.70})
ALLOWED_FAMILY_STATUSES = frozenset(
    {"COMPUTED", "COMPUTED_WITH_LIMITATIONS", "NOT_COMPUTABLE", "NOT_VERIFIABLE"}
)
ALLOWED_ENTRY_EXECUTION_STATUSES = frozenset(
    {"filled_t1", "partial_fill_t1", "delayed_fill", "never_filled", "not_attempted_by_strategy", "not_verifiable"}
)
ALLOWED_EXIT_EXECUTION_STATUSES = frozenset(
    {"filled_on_exit_signal_day", "delayed_exit", "never_exited", "not_attempted_by_strategy", "not_verifiable"}
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

ALLOWED_METRIC_SCOPES = frozenset(
    {"signal_path", "position_episode", "portfolio_result", "order_fill", "execution_cause", "sector_regime"}
)
ALLOWED_QUALITY_FLAGS = frozenset(
    {"ok", "computed_with_limitations", "insufficient_maturity", "not_computable", "not_verifiable", "censored_only"}
)
ALLOWED_METRIC_KEYS = frozenset(
    {
        "label_parity",
        "maturity",
        "rank_ic",
        "ordered_trend_stage_survival",
        "topk_return_distribution",
        "barrier_capture",
        "top50_sector_concentration",
        "sector_signal_path",
        "episode_capture_summary",
        "authoritative_portfolio_summary",
        "entry_execution_summary",
    }
)
METRIC_SEMANTICS: Mapping[str, tuple[str | None, str | None]] = {
    "rank_ic": ("ratio", "higher_better"),
    "authoritative_portfolio_summary": ("ratio", "higher_better"),
    "label_parity": ("ratio", "lower_better"),
}
METRIC_SCOPES: Mapping[str, str] = {
    "label_parity": "signal_path",
    "maturity": "signal_path",
    "rank_ic": "signal_path",
    "ordered_trend_stage_survival": "signal_path",
    "topk_return_distribution": "signal_path",
    "barrier_capture": "signal_path",
    "top50_sector_concentration": "sector_regime",
    "sector_signal_path": "sector_regime",
    "episode_capture_summary": "position_episode",
    "authoritative_portfolio_summary": "portfolio_result",
    "entry_execution_summary": "order_fill",
}
HORIZON_REQUIRED_METRICS = frozenset(
    {
        "label_parity", "maturity", "rank_ic", "ordered_trend_stage_survival",
        "topk_return_distribution", "barrier_capture", "sector_signal_path",
    }
)
SCALAR_ALLOWED_METRICS = frozenset({"rank_ic", "authoritative_portfolio_summary"})


class QELongTrendResultRepositoryError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str = "QELT_RESULT_PERSISTENCE_CONFLICT") -> None:
        super().__init__(message)
        self.reason_code = reason_code


class QELongTrendResultSchemaNotReady(QELongTrendResultRepositoryError):
    def __init__(self, message: str) -> None:
        super().__init__(message, reason_code="QELT_RESULT_SCHEMA_NOT_READY")


class QELongTrendResultPersistenceUnavailable(QELongTrendResultRepositoryError):
    def __init__(self, message: str) -> None:
        super().__init__(message, reason_code="QELT_RESULT_PERSISTENCE_UNAVAILABLE")


class QELongTrendResultQueryError(QELongTrendResultRepositoryError):
    def __init__(self, message: str) -> None:
        super().__init__(message, reason_code="QELT_QUERY_INVALID")


@dataclass(frozen=True)
class RunEvaluationMetricRecord:
    evaluation_id: str
    metric_key: str
    metric_scope: str
    period_start: date | None
    period_end: date | None
    horizon: int | None
    sector_code: str | None
    dimension_key: str
    dimension_json: Mapping[str, Any]
    value_num: float | None
    value_text: str | None
    value_json: Mapping[str, Any] | None
    unit: str | None
    direction: str | None
    source_payload_path: str
    quality_flag: str


@dataclass(frozen=True)
class RunEvaluationArtifactRecord:
    evaluation_id: str
    artifact_type: str
    artifact_uri: str
    sha256: str
    schema_sha256: str | None
    size_bytes: int | None
    row_count: int | None
    status: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class PersistedEvaluationReceipt:
    evaluation_id: str
    metric_count: int
    artifact_count: int
    control_row: Mapping[str, Any]
    replayed: bool


def build_metric_records(
    *, evaluation_id: str, worker_terminal: Mapping[str, Any]
) -> tuple[RunEvaluationMetricRecord, ...]:
    _require_terminal_identity(evaluation_id, worker_terminal)
    raw_metrics = worker_terminal.get("metrics")
    if not isinstance(raw_metrics, list):
        raise QELongTrendResultRepositoryError("worker terminal metrics must be an array")
    records: list[RunEvaluationMetricRecord] = []
    seen: set[tuple[str, str]] = set()
    for index, raw in enumerate(raw_metrics):
        if not isinstance(raw, Mapping):
            raise QELongTrendResultRepositoryError(f"worker metric at index {index} must be an object")
        metric = dict(raw)
        metric_key = str(metric.get("metric_key") or "")
        metric_scope = str(metric.get("metric_scope") or "")
        quality_flag = str(metric.get("quality_flag") or "")
        if metric_key not in ALLOWED_METRIC_KEYS:
            raise QELongTrendResultRepositoryError(f"unknown long-trend metric_key: {metric_key!r}")
        if metric_scope not in ALLOWED_METRIC_SCOPES:
            raise QELongTrendResultRepositoryError(f"invalid long-trend metric_scope: {metric_scope!r}")
        if quality_flag not in ALLOWED_QUALITY_FLAGS:
            raise QELongTrendResultRepositoryError(f"invalid long-trend quality_flag: {quality_flag!r}")
        horizon = _optional_int(metric.get("horizon"), field="horizon")
        if horizon is not None and horizon not in ALLOWED_HORIZONS:
            raise QELongTrendResultRepositoryError(f"invalid long-trend metric horizon: {horizon}")
        sector_code = _optional_text(metric.get("sector_code"), field="sector_code", max_length=128)
        period_start = _optional_date(metric.get("period_start"), field="period_start")
        period_end = _optional_date(metric.get("period_end"), field="period_end")
        if period_start is not None and period_end is not None and period_start > period_end:
            raise QELongTrendResultRepositoryError(f"metric {metric_key} period_start is after period_end")
        slice_name = _optional_text(metric.get("slice"), field="slice", max_length=128)
        barrier = _optional_number(metric.get("barrier"), field="barrier")
        k = _optional_int(metric.get("k"), field="k")
        entry_status = _optional_text(
            metric.get("entry_execution_status"), field="entry_execution_status", max_length=128
        )
        exit_status = _optional_text(
            metric.get("exit_execution_status"), field="exit_execution_status", max_length=128
        )
        dimension_json = {
            "schema_version": DIMENSION_SCHEMA,
            "metric_scope": metric_scope,
            "period_start": period_start.isoformat() if period_start is not None else None,
            "period_end": period_end.isoformat() if period_end is not None else None,
            "slice": slice_name,
            "horizon": horizon,
            "sector_code": sector_code,
            "barrier": barrier,
            "k": k,
            "entry_execution_status": entry_status,
            "exit_execution_status": exit_status,
        }
        dimension_key = canonical_sha256(dimension_json)
        identity = (metric_key, dimension_key)
        if identity in seen:
            raise QELongTrendResultRepositoryError(
                f"duplicate long-trend metric identity: metric_key={metric_key} dimension_key={dimension_key}"
            )
        seen.add(identity)
        value_num = _optional_number(metric.get("value_num"), field="value_num")
        value_text = _optional_text(metric.get("value_text"), field="value_text", max_length=8192)
        raw_json = metric.get("value_json")
        value_json = None if raw_json is None else _bounded_object(raw_json, field="value_json", limit=MAX_VALUE_JSON_BYTES)
        if value_text is not None and (value_num is not None or value_json is not None):
            raise QELongTrendResultRepositoryError(
                f"metric {metric_key} value_text cannot coexist with value_num/value_json"
            )
        if quality_flag in {"ok", "computed_with_limitations"} and all(
            value is None for value in (value_num, value_text, value_json)
        ):
            raise QELongTrendResultRepositoryError(f"computed metric {metric_key} has no value")
        _validate_registered_metric_contract(
            metric_key=metric_key,
            metric_scope=metric_scope,
            slice_name=slice_name,
            horizon=horizon,
            sector_code=sector_code,
            barrier=barrier,
            k=k,
            entry_status=entry_status,
            exit_status=exit_status,
            value_num=value_num,
            value_text=value_text,
            value_json=value_json,
        )
        unit, direction = METRIC_SEMANTICS.get(metric_key, (None, None))
        records.append(
            RunEvaluationMetricRecord(
                evaluation_id=evaluation_id,
                metric_key=metric_key,
                metric_scope=metric_scope,
                period_start=period_start,
                period_end=period_end,
                horizon=horizon,
                sector_code=sector_code,
                dimension_key=dimension_key,
                dimension_json=dimension_json,
                value_num=value_num,
                value_text=value_text,
                value_json=value_json,
                unit=unit,
                direction=direction,
                source_payload_path=f"$.metrics[{index}]",
                quality_flag=quality_flag,
            )
        )
    return tuple(records)


def build_artifact_records(
    *,
    evaluation_id: str,
    worker_terminal: Mapping[str, Any],
    manifest: Mapping[str, Any],
    published_meta: Mapping[str, Any],
) -> tuple[RunEvaluationArtifactRecord, ...]:
    _require_terminal_identity(evaluation_id, worker_terminal)
    if str(manifest.get("schema_version") or "") != MANIFEST_SCHEMA:
        raise QELongTrendResultRepositoryError("unsupported long-trend artifact manifest schema")
    if str(manifest.get("evaluation_id") or "") != evaluation_id:
        raise QELongTrendResultRepositoryError("artifact manifest evaluation identity mismatch")
    manifest_content = {
        key: value for key, value in manifest.items() if key not in {"artifact_manifest_sha256", "uri", "published_at"}
    }
    manifest_sha = str(manifest.get("artifact_manifest_sha256") or "")
    if not _SHA256_RE.fullmatch(manifest_sha) or canonical_sha256(manifest_content) != manifest_sha:
        raise QELongTrendResultRepositoryError("artifact manifest content hash mismatch")
    terminal_sha = canonical_sha256(dict(worker_terminal))
    if str(manifest.get("worker_terminal_sha256") or "") != terminal_sha:
        raise QELongTrendResultRepositoryError("artifact manifest worker terminal hash mismatch")
    manifest_uri = str(manifest.get("uri") or "")
    if manifest_uri != evaluation_uri(evaluation_id):
        raise QELongTrendResultRepositoryError("artifact manifest URI is outside the QE-only namespace")

    records: list[RunEvaluationArtifactRecord] = []
    seen_types: set[str] = set()
    raw_items = manifest.get("artifacts")
    if not isinstance(raw_items, list):
        raise QELongTrendResultRepositoryError("artifact manifest artifacts must be an array")
    for index, raw in enumerate(raw_items):
        if not isinstance(raw, Mapping):
            raise QELongTrendResultRepositoryError(f"artifact manifest item {index} must be an object")
        item = dict(raw)
        artifact_type = str(item.get("artifact_type") or "")
        if artifact_type not in ALLOWED_ARTIFACT_SCHEMAS or artifact_type == "published_compact_receipt":
            raise QELongTrendResultRepositoryError(f"unknown long-trend artifact type: {artifact_type!r}")
        if artifact_type in seen_types:
            raise QELongTrendResultRepositoryError(f"duplicate long-trend artifact type: {artifact_type}")
        seen_types.add(artifact_type)
        if str(item.get("schema_version") or "") != ALLOWED_ARTIFACT_SCHEMAS[artifact_type]:
            raise QELongTrendResultRepositoryError(f"artifact schema mismatch: {artifact_type}")
        expected_uri = artifact_uri(evaluation_id, artifact_type)
        if str(item.get("uri") or "") != expected_uri:
            raise QELongTrendResultRepositoryError(f"artifact URI mismatch: {artifact_type}")
        sha256 = _require_sha256(item.get("sha256"), field=f"artifacts[{index}].sha256")
        schema_sha256 = _optional_sha256(item.get("schema_sha256"), field=f"artifacts[{index}].schema_sha256")
        metadata = _artifact_metadata(item)
        records.append(
            RunEvaluationArtifactRecord(
                evaluation_id=evaluation_id,
                artifact_type=artifact_type,
                artifact_uri=expected_uri,
                sha256=sha256,
                schema_sha256=schema_sha256,
                size_bytes=_optional_nonnegative_int(item.get("size_bytes"), field="size_bytes"),
                row_count=_optional_nonnegative_int(item.get("row_count"), field="row_count"),
                status="published",
                metadata=metadata,
            )
        )

    required, typed_absence = required_artifact_matrix(worker_terminal)
    manifest_required = manifest.get("required_artifacts")
    manifest_absence = manifest.get("typed_absence")
    if (
        not isinstance(manifest_required, list)
        or any(not isinstance(value, str) for value in manifest_required)
        or set(manifest_required) != required
    ):
        raise QELongTrendResultRepositoryError(
            "artifact manifest required_artifacts differ from worker family status"
        )
    if not isinstance(manifest_absence, Mapping) or canonical_sha256(manifest_absence) != canonical_sha256(
        typed_absence
    ):
        raise QELongTrendResultRepositoryError(
            "artifact manifest typed_absence differs from worker family status"
        )
    if not required.issubset(seen_types):
        raise QELongTrendResultRepositoryError("artifact manifest is missing worker-required artifacts")
    if seen_types.intersection(typed_absence):
        raise QELongTrendResultRepositoryError("artifact manifest contains typed-absent artifacts")

    records.append(
        RunEvaluationArtifactRecord(
            evaluation_id=evaluation_id,
            artifact_type="artifact_manifest",
            artifact_uri=manifest_uri,
            sha256=manifest_sha,
            schema_sha256=canonical_sha256({"schema_version": MANIFEST_SCHEMA}),
            size_bytes=None,
            row_count=None,
            status="published",
            metadata=_manifest_metadata(manifest, terminal_sha=terminal_sha),
        )
    )
    published_type = str(published_meta.get("artifact_type") or "")
    if published_type != "published_compact_receipt":
        raise QELongTrendResultRepositoryError("published compact receipt metadata is missing")
    published_uri = str(published_meta.get("uri") or "")
    if published_uri != artifact_uri(evaluation_id, published_type):
        raise QELongTrendResultRepositoryError("published compact receipt URI mismatch")
    if published_type in seen_types:
        raise QELongTrendResultRepositoryError("published compact receipt is duplicated in the manifest")
    records.append(
        RunEvaluationArtifactRecord(
            evaluation_id=evaluation_id,
            artifact_type=published_type,
            artifact_uri=published_uri,
            sha256=_require_sha256(published_meta.get("sha256"), field="published_compact_receipt.sha256"),
            schema_sha256=canonical_sha256({"schema_version": PUBLISHED_SCHEMA}),
            size_bytes=_optional_nonnegative_int(published_meta.get("size_bytes"), field="size_bytes"),
            row_count=None,
            status="published",
            metadata=_bounded_object(
                {
                    "schema_version": ARTIFACT_METADATA_SCHEMA,
                    "artifact_schema_version": PUBLISHED_SCHEMA,
                },
                field="published compact metadata",
                limit=MAX_METADATA_JSON_BYTES,
            ),
        )
    )
    return tuple(records)


class QELongTrendEvaluationResultRepository:
    def __init__(self, connection_provider: Callable[[], Any] | None = None) -> None:
        self._uses_default_connection_provider = connection_provider is None
        self._connection_provider = connection_provider or get_conn

    @contextmanager
    def _connection(self, *, transactional: bool = False):
        try:
            provider = (
                get_conn(autocommit=not transactional, manage_transaction=transactional)
                if self._uses_default_connection_provider
                else self._connection_provider()
            )
            with provider as conn:
                yield conn
        except QELongTrendResultRepositoryError:
            raise
        except Exception as exc:
            raise QELongTrendResultPersistenceUnavailable(
                "F-014 result database operation is unavailable"
            ) from exc

    def ensure_schema_ready(self) -> None:
        required = {
            METRIC_TABLE: {
                "evaluation_metric_id": ("int8", "NO"), "evaluation_id": ("text", "NO"),
                "metric_key": ("text", "NO"), "metric_scope": ("text", "NO"),
                "period_start": ("date", "YES"), "period_end": ("date", "YES"),
                "horizon": ("int4", "YES"), "sector_code": ("text", "YES"),
                "dimension_key": ("text", "NO"), "dimension_json": ("jsonb", "NO"),
                "value_num": ("float8", "YES"), "value_text": ("text", "YES"),
                "value_json": ("jsonb", "YES"), "unit": ("text", "YES"),
                "direction": ("text", "YES"), "source_payload_path": ("text", "NO"),
                "quality_flag": ("text", "NO"), "created_at": ("timestamptz", "NO"),
            },
            ARTIFACT_TABLE: {
                "evaluation_artifact_id": ("int8", "NO"), "evaluation_id": ("text", "NO"),
                "artifact_type": ("text", "NO"), "artifact_uri": ("text", "NO"),
                "sha256": ("text", "NO"), "schema_sha256": ("text", "YES"),
                "size_bytes": ("int8", "YES"), "row_count": ("int8", "YES"),
                "status": ("text", "NO"), "metadata": ("jsonb", "NO"),
                "created_at": ("timestamptz", "NO"),
            },
        }
        try:
            with self._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM qe_archive.schema_version WHERE version = %s",
                        (SCHEMA_VERSION,),
                    )
                    if cur.fetchone() is None:
                        raise QELongTrendResultSchemaNotReady(
                            f"{SCHEMA_VERSION} is not registered; apply the versioned F-014 Phase 3 migration"
                        )
                    for table_name, required_columns in required.items():
                        schema, table = table_name.split(".", 1)
                        cur.execute("SELECT to_regclass(%s)", (table_name,))
                        row = cur.fetchone()
                        if not row or row[0] is None:
                            raise QELongTrendResultSchemaNotReady(
                                f"{table_name} is missing; apply the versioned F-014 Phase 3 migration"
                            )
                        cur.execute(
                            "SELECT column_name, udt_name, is_nullable FROM information_schema.columns "
                            "WHERE table_schema = %s AND table_name = %s",
                            (schema, table),
                        )
                        columns = {
                            str(item[0]): (str(item[1]), str(item[2]))
                            for item in cur.fetchall()
                        }
                        mismatched = sorted(
                            name
                            for name, contract in required_columns.items()
                            if columns.get(name) != contract
                        )
                        if mismatched:
                            raise QELongTrendResultSchemaNotReady(
                                f"{table_name} has missing or incompatible columns: {mismatched}"
                            )
        except QELongTrendResultRepositoryError:
            raise

    def persist_published_receipt(
        self,
        *,
        evaluation_id: str,
        worker_terminal: Mapping[str, Any],
        manifest: Mapping[str, Any],
        published_meta: Mapping[str, Any],
        lease: QELongTrendControlLease | None = None,
    ) -> PersistedEvaluationReceipt:
        metrics = build_metric_records(evaluation_id=evaluation_id, worker_terminal=worker_terminal)
        artifacts = build_artifact_records(
            evaluation_id=evaluation_id,
            worker_terminal=worker_terminal,
            manifest=manifest,
            published_meta=published_meta,
        )
        self.ensure_schema_ready()
        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM {CONTROL_TABLE} WHERE evaluation_id = %s FOR UPDATE", (evaluation_id,))
                control = cur.fetchone()
                if control is None:
                    raise QELongTrendResultRepositoryError(f"evaluation control row does not exist: {evaluation_id}")
                self._verify_qe_parent(cur, control=control)
                _verify_control_receipt_identity(
                    control,
                    worker_terminal=worker_terminal,
                    manifest=manifest,
                    published_meta=published_meta,
                    lease=lease,
                )
                if lease is not None:
                    if (
                        str(control.get("owner_id") or "") != lease.owner_id
                        or int(control.get("fencing_token") or -1) != lease.fencing_token
                        or int(control.get("row_version") or -1) != lease.row_version
                        or str(control.get("status") or "") != "collecting"
                    ):
                        raise QELongTrendResultRepositoryError(
                            "result persistence lost control owner/fencing/row-version CAS"
                        )
                replayed = self._write_or_verify_metrics(cur, evaluation_id=evaluation_id, records=metrics)
                replayed = self._write_or_verify_artifacts(
                    cur, evaluation_id=evaluation_id, records=artifacts
                ) and replayed
                delivery = dict(control.get("platform_delivery_status_json") or {})
                expected_delivery = {
                    **delivery,
                    "db": "published",
                    "db_metric_count": len(metrics),
                    "db_artifact_count": len(artifacts),
                    "db_manifest_sha256": str(manifest["artifact_manifest_sha256"]),
                }
                if delivery != expected_delivery:
                    cur.execute(
                        f"""
                        UPDATE {CONTROL_TABLE}
                        SET platform_delivery_status_json = %s,
                            row_version = row_version + 1,
                            updated_at = clock_timestamp()
                        WHERE evaluation_id = %s AND row_version = %s
                        RETURNING *
                        """,
                        (Json(expected_delivery), evaluation_id, int(control["row_version"])),
                    )
                    updated = cur.fetchone()
                    if updated is None:
                        raise QELongTrendResultRepositoryError("result delivery status row-version CAS failed")
                    control = updated
                conn.commit()
        return PersistedEvaluationReceipt(
            evaluation_id=evaluation_id,
            metric_count=len(metrics),
            artifact_count=len(artifacts),
            control_row=dict(control),
            replayed=replayed,
        )

    def get_evaluation(
        self,
        evaluation_id: str,
        *,
        metric_limit: int = 100,
        metric_cursor: str | None = None,
    ) -> dict[str, Any] | None:
        bounded = _bounded_limit(metric_limit)
        cursor_sql = ""
        metric_params: list[Any] = [evaluation_id]
        if metric_cursor:
            values = _decode_text_cursor(metric_cursor, expected=2)
            cursor_sql = " AND (metric_key > %s OR (metric_key = %s AND dimension_key > %s))"
            metric_params.extend([values[0], values[0], values[1]])
        metric_params.append(bounded + 1)
        self.ensure_schema_ready()
        # Keep the control row and both child collections on one publication
        # boundary.  The writer locks this row FOR UPDATE while publishing;
        # FOR SHARE therefore makes the detail response entirely pre-publish
        # or entirely post-publish instead of mixing three autocommit reads.
        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT evaluation_id, run_id, parent_task_id, parent_loop_index,
                           evaluation_type, profile_id, profile_sha256, evaluator_version,
                           evaluator_source_sha256, execution_environment_snapshot_id,
                           execution_environment_manifest_sha256, bundle_sha256,
                           qe_dataset_contract_id,
                           feature_dataset_snapshot_id, feature_dataset_manifest_sha256,
                           outcome_dataset_snapshot_id, outcome_dataset_manifest_sha256,
                           input_manifest_sha256, request_sha,
                           worker_terminal_sha256, artifact_manifest_sha256,
                           node_id, status, family_status_json, platform_delivery_status_json,
                           data_action_plan_json, reason_code,
                           created_at, started_at, completed_at, updated_at
                    FROM {CONTROL_TABLE}
                    WHERE evaluation_id = %s
                    FOR SHARE
                    """,
                    (evaluation_id,),
                )
                control = cur.fetchone()
                if control is None:
                    return None
                cur.execute(
                    f"""
                    SELECT evaluation_metric_id, evaluation_id, metric_key, metric_scope,
                           period_start, period_end, horizon, sector_code, dimension_key,
                           dimension_json, value_num, value_text, value_json, unit, direction,
                           source_payload_path, quality_flag, created_at
                    FROM {METRIC_TABLE}
                    WHERE evaluation_id = %s {cursor_sql}
                    ORDER BY metric_key, dimension_key
                    LIMIT %s
                    """,
                    metric_params,
                )
                metrics = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    f"""
                    SELECT evaluation_artifact_id, evaluation_id, artifact_type, artifact_uri,
                           sha256, schema_sha256, size_bytes, row_count, status, metadata, created_at
                    FROM {ARTIFACT_TABLE}
                    WHERE evaluation_id = %s
                    ORDER BY artifact_type
                    """,
                    (evaluation_id,),
                )
                artifacts = [dict(row) for row in cur.fetchall()]
        has_more = len(metrics) > bounded
        metric_items = metrics[:bounded]
        next_cursor = None
        if has_more and metric_items:
            last = metric_items[-1]
            next_cursor = _encode_cursor(last["metric_key"], last["dimension_key"])
        return {
            "evaluation": dict(control),
            "metrics": metric_items,
            "metric_next_cursor": next_cursor,
            "artifacts": artifacts,
        }

    def list_evaluations(
        self,
        *,
        task_id: str,
        loop_index: int,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        bounded = _bounded_limit(limit)
        params: list[Any] = [task_id, int(loop_index)]
        cursor_sql = ""
        if cursor:
            cursor_values = _decode_cursor(cursor, expected=2)
            cursor_sql = " AND (created_at < %s OR (created_at = %s AND evaluation_id > %s))"
            params.extend([cursor_values[0], cursor_values[0], cursor_values[1]])
        params.append(bounded + 1)
        with self._connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT evaluation_id, run_id, parent_task_id, parent_loop_index, profile_id,
                           feature_dataset_snapshot_id, outcome_dataset_snapshot_id, node_id,
                           status, family_status_json, platform_delivery_status_json,
                           data_action_plan_json, reason_code,
                           created_at, started_at, completed_at, updated_at
                    FROM {CONTROL_TABLE}
                    WHERE parent_task_id = %s AND parent_loop_index = %s
                    {cursor_sql}
                    ORDER BY created_at DESC, evaluation_id ASC
                    LIMIT %s
                    """,
                    params,
                )
                rows = [dict(row) for row in cur.fetchall()]
        has_more = len(rows) > bounded
        items = rows[:bounded]
        next_cursor = None
        if has_more and items:
            last = items[-1]
            next_cursor = _encode_cursor(last["created_at"], last["evaluation_id"])
        return {"items": items, "next_cursor": next_cursor, "limit": bounded}

    def find_materializable_candidates(
        self,
        *,
        run_id: str,
        task_id: str,
        loop_index: int,
        profile_id: str,
        outcome_dataset_snapshot_id: str,
    ) -> tuple[dict[str, Any], ...]:
        """Return at most two exact terminal CAS candidates for conflict detection."""

        self.ensure_schema_ready()
        with self._connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT evaluation_id, run_id, parent_task_id, parent_loop_index,
                           profile_id, feature_dataset_snapshot_id,
                           outcome_dataset_snapshot_id, status,
                           worker_terminal_sha256, artifact_manifest_sha256,
                           platform_delivery_status_json
                    FROM {CONTROL_TABLE}
                    WHERE run_id = %s
                      AND parent_task_id = %s
                      AND parent_loop_index = %s
                      AND evaluation_type = 'long_trend'
                      AND profile_id = %s
                      AND outcome_dataset_snapshot_id = %s
                      AND status IN ('succeeded', 'partial', 'failed', 'cancelled')
                      AND worker_terminal_sha256 IS NOT NULL
                      AND artifact_manifest_sha256 IS NOT NULL
                      AND platform_delivery_status_json->>'cas' = 'published'
                    ORDER BY evaluation_id
                    LIMIT 2
                    """,
                    (
                        run_id,
                        task_id,
                        int(loop_index),
                        profile_id,
                        outcome_dataset_snapshot_id,
                    ),
                )
                rows = tuple(dict(row) for row in cur.fetchall())
        return rows

    def query_quality(
        self,
        *,
        evaluation_id: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        loop_index: int | None = None,
        model_type: str | None = None,
        label_horizon: int | None = None,
        evaluation_asof: date | None = None,
        outcome_dataset_snapshot_id: str | None = None,
        horizon: int | None = None,
        sector_code: str | None = None,
        family_status: str | None = None,
        entry_execution_status: str | None = None,
        exit_execution_status: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        _validate_query_filters(
            model_type=model_type,
            label_horizon=label_horizon,
            horizon=horizon,
            sector_code=sector_code,
            family_status=family_status,
            entry_execution_status=entry_execution_status,
            exit_execution_status=exit_execution_status,
        )
        bounded = _bounded_limit(limit)
        clauses = ["1=1"]
        params: list[Any] = []

        def add(sql: str, value: Any) -> None:
            clauses.append(sql)
            params.append(value)

        if evaluation_id:
            add("e.evaluation_id = %s", evaluation_id)
        if run_id:
            add("e.run_id = %s", run_id)
        if task_id:
            add("e.parent_task_id = %s", task_id)
        if loop_index is not None:
            add("e.parent_loop_index = %s", int(loop_index))
        if model_type:
            add("r.model_type = %s", model_type)
        if label_horizon is not None:
            add("r.label_horizon = %s", int(label_horizon))
        if evaluation_asof is not None:
            add("(e.request_json->>'evaluation_asof')::date = %s", evaluation_asof)
        if outcome_dataset_snapshot_id:
            add("e.outcome_dataset_snapshot_id = %s", outcome_dataset_snapshot_id)
        if horizon is not None:
            add("m.horizon = %s", int(horizon))
        if sector_code:
            add("m.sector_code = %s", sector_code)
        if family_status:
            add(
                "EXISTS (SELECT 1 FROM jsonb_each(e.family_status_json) family "
                "WHERE family.value->>'status' = %s)",
                family_status,
            )
        if entry_execution_status:
            clauses.append(
                "(m.dimension_json->>'entry_execution_status' = %s OR "
                "(m.metric_key = 'entry_execution_summary' AND "
                "COALESCE(m.value_json->'entry_status_counts', '{}'::jsonb) ? %s))"
            )
            params.extend([entry_execution_status, entry_execution_status])
        if exit_execution_status:
            clauses.append(
                "(m.dimension_json->>'exit_execution_status' = %s OR "
                "(m.metric_key = 'entry_execution_summary' AND "
                "COALESCE(m.value_json->'exit_status_counts', '{}'::jsonb) ? %s))"
            )
            params.extend([exit_execution_status, exit_execution_status])
        if cursor:
            values = _decode_cursor(cursor, expected=4)
            try:
                cursor_asof = date.fromisoformat(values[0])
            except ValueError as exc:
                raise QELongTrendResultQueryError("quality cursor evaluation_asof is invalid") from exc
            clauses.append(
                "(COALESCE((e.request_json->>'evaluation_asof')::date, DATE '0001-01-01') < %s OR "
                "(COALESCE((e.request_json->>'evaluation_asof')::date, DATE '0001-01-01') = %s AND "
                "(e.evaluation_id > %s OR (e.evaluation_id = %s AND "
                "(m.metric_key > %s OR (m.metric_key = %s AND m.dimension_key > %s))))))"
            )
            params.extend([cursor_asof, cursor_asof, values[1], values[1], values[2], values[2], values[3]])
        params.append(bounded + 1)
        self.ensure_schema_ready()
        # ALGO-COMPLEXITY-001: this compact warehouse query joins only one
        # evaluation parent to its indexed metric rows. Keyset pagination and
        # limit <= 100 bound returned rows; it never expands signal/episode Parquet.
        with self._connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT e.evaluation_id, e.run_id, e.parent_task_id, e.parent_loop_index,
                           e.profile_id, e.status AS evaluation_status,
                           e.feature_dataset_snapshot_id, e.outcome_dataset_snapshot_id,
                           e.request_json->>'evaluation_asof' AS evaluation_asof,
                           COALESCE((e.request_json->>'evaluation_asof')::date, DATE '0001-01-01')
                               AS evaluation_asof_sort,
                           e.family_status_json, e.platform_delivery_status_json,
                           e.created_at, r.model_type, r.label_horizon,
                           m.metric_key, m.metric_scope, m.horizon, m.sector_code,
                           m.dimension_key, m.dimension_json, m.value_num, m.value_text,
                           m.value_json, m.unit, m.direction, m.quality_flag
                    FROM {CONTROL_TABLE} e
                    JOIN {METRIC_TABLE} m ON m.evaluation_id = e.evaluation_id
                    LEFT JOIN qe_archive.run r ON r.run_id = e.run_id
                    WHERE {' AND '.join(clauses)}
                    ORDER BY evaluation_asof_sort DESC, e.evaluation_id ASC,
                             m.metric_key ASC, m.dimension_key ASC
                    LIMIT %s
                    """,
                    params,
                )
                rows = [dict(row) for row in cur.fetchall()]
        has_more = len(rows) > bounded
        items = rows[:bounded]
        next_cursor = None
        if has_more and items:
            last = items[-1]
            next_cursor = _encode_cursor(
                last["evaluation_asof_sort"], last["evaluation_id"], last["metric_key"], last["dimension_key"]
            )
        for item in items:
            item.pop("evaluation_asof_sort", None)
        return {"items": items, "next_cursor": next_cursor, "limit": bounded}

    @staticmethod
    def _write_or_verify_metrics(
        cur: Any, *, evaluation_id: str, records: Sequence[RunEvaluationMetricRecord]
    ) -> bool:
        cur.execute(
            f"""
            SELECT metric_key, metric_scope, period_start, period_end, horizon, sector_code,
                   dimension_key, dimension_json, value_num, value_text, value_json,
                   unit, direction, source_payload_path, quality_flag
            FROM {METRIC_TABLE}
            WHERE evaluation_id = %s
            ORDER BY metric_key, dimension_key
            """,
            (evaluation_id,),
        )
        existing = [dict(row) for row in cur.fetchall()]
        expected = [_metric_db_payload(record) for record in records]
        expected.sort(key=lambda row: (row["metric_key"], row["dimension_key"]))
        if existing:
            if _canonical_rows(existing) != _canonical_rows(expected):
                raise QELongTrendResultRepositoryError(
                    "existing long-trend metric rows conflict with the published receipt"
                )
            return True
        values = [
            (
                    evaluation_id,
                    row["metric_key"], row["metric_scope"], row["period_start"], row["period_end"],
                    row["horizon"], row["sector_code"], row["dimension_key"], Json(row["dimension_json"]),
                    row["value_num"], row["value_text"], Json(row["value_json"]) if row["value_json"] is not None else None,
                    row["unit"], row["direction"], row["source_payload_path"], row["quality_flag"],
            )
            for row in expected
        ]
        _execute_multirow_insert(
            cur,
            table=METRIC_TABLE,
            columns=(
                "evaluation_id", "metric_key", "metric_scope", "period_start", "period_end",
                "horizon", "sector_code", "dimension_key", "dimension_json", "value_num",
                "value_text", "value_json", "unit", "direction", "source_payload_path", "quality_flag",
            ),
            values=values,
        )
        return False

    @staticmethod
    def _write_or_verify_artifacts(
        cur: Any, *, evaluation_id: str, records: Sequence[RunEvaluationArtifactRecord]
    ) -> bool:
        cur.execute(
            f"""
            SELECT artifact_type, artifact_uri, sha256, schema_sha256, size_bytes,
                   row_count, status, metadata
            FROM {ARTIFACT_TABLE}
            WHERE evaluation_id = %s
            ORDER BY artifact_type
            """,
            (evaluation_id,),
        )
        existing = [dict(row) for row in cur.fetchall()]
        expected = [_artifact_db_payload(record) for record in records]
        expected.sort(key=lambda row: row["artifact_type"])
        if existing:
            if _canonical_rows(existing) != _canonical_rows(expected):
                raise QELongTrendResultRepositoryError(
                    "existing long-trend artifact rows conflict with the published manifest"
                )
            return True
        values = [
            (
                    evaluation_id, row["artifact_type"], row["artifact_uri"], row["sha256"],
                    row["schema_sha256"], row["size_bytes"], row["row_count"], row["status"], Json(row["metadata"]),
            )
            for row in expected
        ]
        _execute_multirow_insert(
            cur,
            table=ARTIFACT_TABLE,
            columns=(
                "evaluation_id", "artifact_type", "artifact_uri", "sha256", "schema_sha256",
                "size_bytes", "row_count", "status", "metadata",
            ),
            values=values,
        )
        return False

    @staticmethod
    def _verify_qe_parent(cur: Any, *, control: Mapping[str, Any]) -> None:
        if str(control.get("evaluation_type") or "") != "long_trend":
            raise QELongTrendResultRepositoryError(
                "evaluation control row is not an F-014 long-trend evaluation",
                reason_code="QELT_NON_QE_SOURCE_REJECTED",
            )
        if str(control.get("qe_dataset_contract_id") or "") != QE_DATASET_CONTRACT_ID:
            raise QELongTrendResultRepositoryError(
                "evaluation control row uses a different QE dataset contract",
                reason_code="QELT_NON_QE_SOURCE_REJECTED",
            )
        run_id = str(control.get("run_id") or "").strip()
        if run_id:
            cur.execute(
                """
                SELECT source_system, run_type, task_id, loop_index
                FROM qe_archive.run
                WHERE run_id = %s
                FOR SHARE
                """,
                (run_id,),
            )
            parent = cur.fetchone()
            if (
                parent is None
                or str(parent.get("source_system") or "") not in QE_SOURCE_SYSTEMS
                or str(parent.get("run_type") or "") not in QE_RUN_TYPES
                or str(parent.get("task_id") or "") != str(control.get("parent_task_id") or "")
                or int(parent.get("loop_index") or 0) != int(control.get("parent_loop_index") or 0)
            ):
                raise QELongTrendResultRepositoryError(
                    "evaluation control row is not bound to the matching QE Archive run",
                    reason_code="QELT_NON_QE_SOURCE_REJECTED",
                )
            return
        cur.execute(
            """
            SELECT 1
            FROM qe_evolution_loops l
            JOIN qe_evolution_tasks t ON t.task_id = l.task_id
            WHERE l.task_id = %s AND l.loop_index = %s
            FOR SHARE OF l
            """,
            (control.get("parent_task_id"), int(control.get("parent_loop_index") or 0)),
        )
        if cur.fetchone() is None:
            raise QELongTrendResultRepositoryError(
                "evaluation control row has no authoritative QE task/Loop parent",
                reason_code="QELT_NON_QE_SOURCE_REJECTED",
            )


def _metric_db_payload(record: RunEvaluationMetricRecord) -> dict[str, Any]:
    row = asdict(record)
    row.pop("evaluation_id", None)
    return row


def _validate_registered_metric_contract(
    *,
    metric_key: str,
    metric_scope: str,
    slice_name: str | None,
    horizon: int | None,
    sector_code: str | None,
    barrier: float | None,
    k: int | None,
    entry_status: str | None,
    exit_status: str | None,
    value_num: float | None,
    value_text: str | None,
    value_json: Mapping[str, Any] | None,
) -> None:
    expected_scope = METRIC_SCOPES[metric_key]
    if metric_scope != expected_scope:
        raise QELongTrendResultRepositoryError(
            f"metric {metric_key} requires metric_scope={expected_scope}"
        )
    if slice_name is None:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} requires a registered slice")
    if slice_name not in ALLOWED_SLICES:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} uses an unregistered slice")
    if metric_key in HORIZON_REQUIRED_METRICS and horizon is None:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} requires horizon")
    if metric_key not in HORIZON_REQUIRED_METRICS and metric_key != "episode_capture_summary" and horizon is not None:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} does not allow horizon")
    if metric_key == "episode_capture_summary" and horizon != 180:
        raise QELongTrendResultRepositoryError("metric episode_capture_summary requires horizon=180")
    if (sector_code is not None) != (metric_key == "sector_signal_path"):
        raise QELongTrendResultRepositoryError(
            f"metric {metric_key} has an invalid sector_code dimension"
        )
    if (barrier is not None) != (metric_key == "barrier_capture"):
        raise QELongTrendResultRepositoryError(f"metric {metric_key} has an invalid barrier dimension")
    if barrier is not None and barrier not in ALLOWED_BARRIERS:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} uses an unregistered barrier")
    if metric_key in {"topk_return_distribution", "barrier_capture"}:
        if k is None or k <= 0 or k > 100:
            raise QELongTrendResultRepositoryError(f"metric {metric_key} requires registered k in 1..100")
    elif metric_key == "sector_signal_path":
        if k != 50:
            raise QELongTrendResultRepositoryError("metric sector_signal_path requires k=50")
    elif k is not None:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} does not allow k")
    if (entry_status is not None or exit_status is not None) and metric_key != "entry_execution_summary":
        raise QELongTrendResultRepositoryError(
            f"metric {metric_key} does not allow execution-status dimensions"
        )
    if entry_status is not None and entry_status not in ALLOWED_ENTRY_EXECUTION_STATUSES:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} has invalid entry_execution_status")
    if exit_status is not None and exit_status not in ALLOWED_EXIT_EXECUTION_STATUSES:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} has invalid exit_execution_status")
    if value_text is not None:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} does not allow value_text")
    if value_json is None:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} requires value_json")
    if value_num is not None and metric_key not in SCALAR_ALLOWED_METRICS:
        raise QELongTrendResultRepositoryError(f"metric {metric_key} does not allow value_num")


def _execute_multirow_insert(
    cur: Any,
    *,
    table: str,
    columns: Sequence[str],
    values: Sequence[Sequence[Any]],
) -> None:
    """Insert immutable receipt rows in bounded pages within the caller transaction."""
    if not values:
        return
    # ALGO-COMPLEXITY-001: receipt rows are already compact and validated.
    # INSERT_PAGE_SIZE bounds placeholder/parameter growth, while the single
    # caller transaction preserves exact-set atomicity across all pages.
    column_sql = ", ".join(columns)
    row_placeholder = "(" + ", ".join(["%s"] * len(columns)) + ")"
    for start in range(0, len(values), INSERT_PAGE_SIZE):
        page = values[start : start + INSERT_PAGE_SIZE]
        placeholders = ", ".join([row_placeholder] * len(page))
        params = [value for row in page for value in row]
        cur.execute(f"INSERT INTO {table} ({column_sql}) VALUES {placeholders}", params)


def _artifact_db_payload(record: RunEvaluationArtifactRecord) -> dict[str, Any]:
    row = asdict(record)
    row.pop("evaluation_id", None)
    return row


def _canonical_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    return canonical_sha256([_json_safe(dict(row)) for row in rows])


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, date):
        return value.isoformat()
    return value


def _require_terminal_identity(evaluation_id: str, worker_terminal: Mapping[str, Any]) -> None:
    if str(worker_terminal.get("schema_version") or "") != TERMINAL_SCHEMA:
        raise QELongTrendResultRepositoryError("unsupported worker terminal schema")
    if str(worker_terminal.get("evaluation_id") or "") != evaluation_id:
        raise QELongTrendResultRepositoryError("worker terminal evaluation identity mismatch")


def _artifact_metadata(item: Mapping[str, Any]) -> Mapping[str, Any]:
    raw_columns = item.get("columns") or []
    if not isinstance(raw_columns, list) or any(not isinstance(value, str) for value in raw_columns):
        raise QELongTrendResultRepositoryError("artifact columns must be an array of strings")
    metadata = {
        "schema_version": ARTIFACT_METADATA_SCHEMA,
        "artifact_schema_version": str(item.get("schema_version") or ""),
        "row_group_count": _optional_nonnegative_int(item.get("row_group_count"), field="row_group_count"),
        "columns": list(raw_columns),
    }
    return _bounded_object(metadata, field="artifact metadata", limit=MAX_METADATA_JSON_BYTES)


def _manifest_metadata(manifest: Mapping[str, Any], *, terminal_sha: str) -> Mapping[str, Any]:
    required = manifest.get("required_artifacts") or []
    typed_absence = manifest.get("typed_absence") or {}
    if not isinstance(required, list) or any(not isinstance(value, str) for value in required):
        raise QELongTrendResultRepositoryError("manifest required_artifacts must be an array of strings")
    if not isinstance(typed_absence, Mapping):
        raise QELongTrendResultRepositoryError("manifest typed_absence must be an object")
    return _bounded_object(
        {
            "schema_version": ARTIFACT_METADATA_SCHEMA,
            "artifact_schema_version": MANIFEST_SCHEMA,
            "worker_terminal_sha256": terminal_sha,
            "required_artifacts": list(required),
            "typed_absence": dict(typed_absence),
        },
        field="artifact manifest metadata",
        limit=MAX_METADATA_JSON_BYTES,
    )


def _verify_control_receipt_identity(
    control: Mapping[str, Any],
    *,
    worker_terminal: Mapping[str, Any],
    manifest: Mapping[str, Any],
    published_meta: Mapping[str, Any],
    lease: QELongTrendControlLease | None,
) -> None:
    terminal_sha = canonical_sha256(dict(worker_terminal))
    manifest_sha = str(manifest.get("artifact_manifest_sha256") or "")
    published_sha = str(published_meta.get("sha256") or "")
    if lease is not None:
        for field, expected in (
            ("worker_terminal_sha256", terminal_sha),
            ("artifact_manifest_sha256", manifest_sha),
        ):
            stored = str(control.get(field) or "")
            if stored and stored != expected:
                raise QELongTrendResultRepositoryError(f"control row {field} conflicts with published receipt")
        return

    if str(control.get("status") or "") not in {"succeeded", "partial", "failed", "cancelled"}:
        raise QELongTrendResultRepositoryError("CAS materialization requires a terminal evaluation control row")
    if str(control.get("worker_terminal_sha256") or "") != terminal_sha:
        raise QELongTrendResultRepositoryError("control row worker terminal hash differs from CAS")
    if str(control.get("artifact_manifest_sha256") or "") != manifest_sha:
        raise QELongTrendResultRepositoryError("control row artifact manifest hash differs from CAS")
    stats = control.get("stats_json") or {}
    if not isinstance(stats, Mapping):
        raise QELongTrendResultRepositoryError("control row stats_json must be an object")
    published = stats.get("published_compact_receipt") or {}
    if not isinstance(published, Mapping) or str(published.get("sha256") or "") != published_sha:
        raise QELongTrendResultRepositoryError("control row published compact hash differs from CAS")


def _bounded_object(value: Any, *, field: str, limit: int) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise QELongTrendResultRepositoryError(f"{field} must be an object")
    payload = dict(value)
    try:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise QELongTrendResultRepositoryError(f"{field} is not canonical JSON: {exc}") from exc
    if len(encoded) > limit:
        raise QELongTrendResultRepositoryError(f"{field} exceeds the bounded payload limit: {len(encoded)} > {limit}")
    return payload


def _optional_text(value: Any, *, field: str, max_length: int) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    if len(text) > max_length:
        raise QELongTrendResultRepositoryError(f"{field} exceeds maximum length {max_length}")
    return text


def _optional_number(value: Any, *, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise QELongTrendResultRepositoryError(f"{field} must be numeric, not boolean")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise QELongTrendResultRepositoryError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise QELongTrendResultRepositoryError(f"{field} must be finite")
    return result


def _optional_int(value: Any, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise QELongTrendResultRepositoryError(f"{field} must be an integer, not boolean")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise QELongTrendResultRepositoryError(f"{field} must be an integer") from exc
    if str(value).strip() not in {str(result), f"{result}.0"} and not isinstance(value, int):
        raise QELongTrendResultRepositoryError(f"{field} must not lose precision")
    return result


def _optional_nonnegative_int(value: Any, *, field: str) -> int | None:
    result = _optional_int(value, field=field)
    if result is not None and result < 0:
        raise QELongTrendResultRepositoryError(f"{field} must be non-negative")
    return result


def _optional_date(value: Any, *, field: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise QELongTrendResultRepositoryError(f"{field} must be an ISO date") from exc


def _require_sha256(value: Any, *, field: str) -> str:
    result = str(value or "").lower()
    if not _SHA256_RE.fullmatch(result):
        raise QELongTrendResultRepositoryError(f"{field} must be a SHA-256 digest")
    return result


def _optional_sha256(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    return _require_sha256(value, field=field)


def _bounded_limit(limit: int) -> int:
    try:
        value = int(limit)
    except (TypeError, ValueError) as exc:
        raise QELongTrendResultQueryError("limit must be an integer") from exc
    if value < 1 or value > 100:
        raise QELongTrendResultQueryError("limit must be between 1 and 100")
    return value


def _validate_query_filters(
    *,
    model_type: str | None,
    label_horizon: int | None,
    horizon: int | None,
    sector_code: str | None,
    family_status: str | None,
    entry_execution_status: str | None,
    exit_execution_status: str | None,
) -> None:
    for field, value in (("label_horizon", label_horizon), ("horizon", horizon)):
        if value is not None:
            if isinstance(value, bool):
                raise QELongTrendResultQueryError(f"{field} must be one of {sorted(ALLOWED_HORIZONS)}")
            try:
                parsed = int(value)
            except (TypeError, ValueError) as exc:
                raise QELongTrendResultQueryError(
                    f"{field} must be one of {sorted(ALLOWED_HORIZONS)}"
                ) from exc
            if parsed not in ALLOWED_HORIZONS or str(value).strip() not in {str(parsed), f"{parsed}.0"}:
                raise QELongTrendResultQueryError(f"{field} must be one of {sorted(ALLOWED_HORIZONS)}")
    for field, value, allowed in (
        ("family_status", family_status, ALLOWED_FAMILY_STATUSES),
        ("entry_execution_status", entry_execution_status, ALLOWED_ENTRY_EXECUTION_STATUSES),
        ("exit_execution_status", exit_execution_status, ALLOWED_EXIT_EXECUTION_STATUSES),
    ):
        if value is not None and value not in allowed:
            raise QELongTrendResultQueryError(f"{field} is invalid: {value}")
    for field, value, maximum in (("model_type", model_type, 128), ("sector_code", sector_code, 128)):
        if value is not None and (not value.strip() or len(value) > maximum):
            raise QELongTrendResultQueryError(f"{field} must contain 1 to {maximum} characters")


def _encode_cursor(*values: Any) -> str:
    payload = [_json_safe(value) for value in values]
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str, *, expected: int) -> list[str]:
    if len(cursor) > MAX_CURSOR_LENGTH:
        raise QELongTrendResultQueryError("cursor exceeds maximum length")
    try:
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode((cursor + padding).encode("ascii")))
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise QELongTrendResultQueryError("cursor is invalid") from exc
    if not isinstance(payload, list) or len(payload) != expected or any(not isinstance(value, str) for value in payload):
        raise QELongTrendResultQueryError("cursor has an invalid shape")
    if not payload[0]:
        raise QELongTrendResultQueryError("cursor timestamp is empty")
    try:
        datetime.fromisoformat(payload[0])
    except ValueError as exc:
        raise QELongTrendResultQueryError("cursor timestamp is invalid") from exc
    return payload


def _decode_text_cursor(cursor: str, *, expected: int) -> list[str]:
    if len(cursor) > MAX_CURSOR_LENGTH:
        raise QELongTrendResultQueryError("cursor exceeds maximum length")
    try:
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode((cursor + padding).encode("ascii")))
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise QELongTrendResultQueryError("cursor is invalid") from exc
    if not isinstance(payload, list) or len(payload) != expected or any(not isinstance(value, str) for value in payload):
        raise QELongTrendResultQueryError("cursor has an invalid shape")
    return payload
