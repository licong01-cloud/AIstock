"""PostgreSQL residue and concurrency evidence for Phase 1G G5 L3."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import traceback
from typing import Any, Callable

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.control_binding import (
    ControlBindingRequest,
    PostgresControlBindingRepository,
)

from .phase1g_dev_evidence_contract import (
    Phase1GDevEvidenceError,
    Phase1GDevResidueCheck,
    REASON_L3_CONCURRENCY_FAILED,
    REASON_L3_RESIDUE_DETECTED,
)


ConnectionFactory = Callable[[], Any]
LOGGER = logging.getLogger(__name__)


def _log_sanitized_exception(message: str, exc: Exception) -> None:
    frames = " > ".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}"
        for frame in traceback.extract_tb(exc.__traceback__)
    )
    LOGGER.error(
        message + " exception_type=%s redacted_traceback=%s",
        type(exc).__name__,
        frames,
    )


@dataclass(frozen=True)
class Phase1GDevResidueProbe:
    relation_name: str
    key_columns: tuple[str, ...]
    identities: tuple[tuple[Any, ...], ...]

    @property
    def identity_set_hash(self) -> str:
        return canonical_json_sha256(
            [
                {
                    "columns": list(self.key_columns),
                    "values": [_json_value(value) for value in identity],
                }
                for identity in self.identities
            ]
        )


_RELATION_KEYS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("app.advisory_phase1_control_binding_event", ("binding_event_hash",)),
    ("app.advisory_capture_batch", ("capture_batch_id",)),
    ("app.advisory_capture_plan", ("capture_batch_id", "plan_hash")),
    (
        "app.advisory_capture_batch_evidence_membership",
        ("capture_batch_id", "evidence_role", "evidence_id"),
    ),
    ("app.advisory_source_revision_set", ("source_revision_set_id",)),
    (
        "app.advisory_source_revision_member",
        ("source_revision_set_id", "member_key"),
    ),
    ("app.advisory_selection_stage_trace_outbox", ("trace_outbox_id",)),
    (
        "app.advisory_selection_stage_trace_delivery_event",
        ("delivery_event_id",),
    ),
    ("app.advisory_signal_observation", ("canonical_signal_id",)),
    ("app.advisory_signal_observation_version", ("observation_version_id",)),
    ("app.advisory_signal_observation_lineage_identity", ("lineage_id",)),
    (
        "app.advisory_signal_observation_lineage_payload",
        ("decision_as_of_trade_date", "lineage_id"),
    ),
    ("app.advisory_signal_stage_evidence", ("stage_evidence_id",)),
    (
        "app.advisory_signal_stage_candidate_identity",
        ("stage_evidence_id", "symbol"),
    ),
    (
        "app.advisory_signal_stage_candidate_payload",
        ("decision_as_of_trade_date", "stage_evidence_id", "symbol"),
    ),
)


def capture_current_transaction_residue_probes(
    *,
    cursor: Any,
) -> tuple[Phase1GDevResidueProbe, ...]:
    probes: list[Phase1GDevResidueProbe] = []
    for relation_name, key_columns in _RELATION_KEYS:
        schema_name, table_name = relation_name.split(".", 1)
        query = sql.SQL("SELECT {} FROM {}.{} WHERE xmin::text::bigint = txid_current() ORDER BY {}").format(
            sql.SQL(", ").join(sql.Identifier(value) for value in key_columns),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(value) for value in key_columns),
        )
        cursor.execute(query)
        identities = tuple(
            tuple(row[column] for column in key_columns)
            for row in cursor.fetchall()
        )
        probes.append(
            Phase1GDevResidueProbe(
                relation_name=relation_name,
                key_columns=key_columns,
                identities=identities,
            )
        )
    return tuple(probes)


def verify_zero_residue(
    *,
    connection_factory: ConnectionFactory,
    probes: tuple[Phase1GDevResidueProbe, ...],
) -> tuple[Phase1GDevResidueCheck, ...]:
    connection = connection_factory()
    checks: list[Phase1GDevResidueCheck] = []
    try:
        connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for probe in probes:
                residue_count = 0
                schema_name, table_name = probe.relation_name.split(".", 1)
                for identity in probe.identities:
                    predicates = sql.SQL(" AND ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(column))
                        for column in probe.key_columns
                    )
                    query = sql.SQL("SELECT 1 FROM {}.{} WHERE {} LIMIT 1").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                        predicates,
                    )
                    cur.execute(query, identity)
                    if cur.fetchone() is not None:
                        residue_count += 1
                checks.append(
                    Phase1GDevResidueCheck(
                        relation_name=probe.relation_name,
                        identity_set_hash=probe.identity_set_hash,
                        checked_identity_count=len(probe.identities),
                        residue_count=residue_count,
                    )
                )
        connection.rollback()
    finally:
        connection.close()
    if any(item.residue_count for item in checks):
        raise Phase1GDevEvidenceError(
            REASON_L3_RESIDUE_DETECTED,
            "fresh DEV connection detected rollback residue",
            context={
                "relations": [
                    item.relation_name for item in checks if item.residue_count
                ]
            },
        )
    return tuple(checks)


def run_control_binding_concurrency_probe(
    *,
    connection_factory: ConnectionFactory,
    request: ControlBindingRequest,
    lock_timeout_ms: int,
) -> str:
    if lock_timeout_ms <= 0:
        raise Phase1GDevEvidenceError(
            REASON_L3_CONCURRENCY_FAILED,
            "concurrency lock timeout must be positive",
        )
    connection_a = None
    connection_b = None
    event_hash: str | None = None
    expected_conflict = False
    baseline_count = 0
    cleanup_error: Exception | None = None
    try:
        connection_a = connection_factory()
        connection_b = connection_factory()
        for connection in (connection_a, connection_b):
            if bool(getattr(connection, "autocommit", False)):
                raise Phase1GDevEvidenceError(
                    REASON_L3_CONCURRENCY_FAILED,
                    "concurrency probe connections must disable autocommit",
                )
        with connection_a.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur_a:
            cur_a.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cur_a.execute(
                "SELECT count(*) AS row_count FROM app.advisory_phase1_control_binding_event WHERE binding_chain_key = %s",
                (request.binding_chain_key,),
            )
            baseline_count = int(cur_a.fetchone()["row_count"])
            event = PostgresControlBindingRepository.append_in_transaction(
                cur_a, request
            )
            event_hash = event.binding_event_hash
            with connection_b.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur_b:
                cur_b.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cur_b.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (str(lock_timeout_ms),),
                )
                cur_b.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(max(lock_timeout_ms * 2, lock_timeout_ms + 100)),),
                )
                try:
                    PostgresControlBindingRepository.append_in_transaction(
                        cur_b, request
                    )
                except (
                    psycopg2.errors.LockNotAvailable,
                    psycopg2.errors.QueryCanceled,
                    psycopg2.errors.UniqueViolation,
                ):
                    expected_conflict = True
        connection_b.rollback()
        connection_a.rollback()
    except Phase1GDevEvidenceError:
        raise
    except Exception as exc:
        raise Phase1GDevEvidenceError(
            REASON_L3_CONCURRENCY_FAILED,
            "control binding concurrency probe failed unexpectedly",
            context={"exception_type": type(exc).__name__},
        ) from exc
    finally:
        for connection in (
            item for item in (connection_b, connection_a) if item is not None
        ):
            try:
                connection.rollback()
            except Exception as exc:
                _log_sanitized_exception(
                    "phase1g G5 concurrency probe cleanup rollback failed",
                    exc,
                )
                cleanup_error = cleanup_error or exc
            try:
                connection.close()
            except Exception as exc:
                _log_sanitized_exception(
                    "phase1g G5 concurrency probe connection close failed",
                    exc,
                )
                cleanup_error = cleanup_error or exc
    if cleanup_error is not None:
        raise Phase1GDevEvidenceError(
            REASON_L3_CONCURRENCY_FAILED,
            "control binding concurrency probe cleanup failed",
            context={"exception_type": type(cleanup_error).__name__},
        ) from cleanup_error
    if not expected_conflict or event_hash is None:
        raise Phase1GDevEvidenceError(
            REASON_L3_CONCURRENCY_FAILED,
            "concurrency probe did not observe the expected lock or unique conflict",
        )
    verification = connection_factory()
    try:
        verification.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with verification.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_phase1_control_binding_event WHERE binding_chain_key = %s",
                (request.binding_chain_key,),
            )
            residue = int(cur.fetchone()[0])
        verification.rollback()
    finally:
        verification.close()
    if residue != baseline_count:
        raise Phase1GDevEvidenceError(
            REASON_L3_CONCURRENCY_FAILED,
            "concurrency probe left control binding residue",
        )
    return canonical_json_sha256(
        {
            "binding_chain_key_hash": canonical_json_sha256(
                request.binding_chain_key
            ),
            "event_hash": event_hash,
            "expected_conflict": expected_conflict,
            "baseline_count": baseline_count,
            "post_rollback_count": residue,
        }
    )


def _json_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
