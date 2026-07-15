from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest

from backend.services.advisory_phase0a.policy import canonicalize
from backend.services.advisory_phase1.historical_trace_projection_postgres import (
    PHASE1G_G2_HISTORICAL_SQL_REGISTRY,
    Phase1GPostgresHistoricalProjection,
    Phase1GPostgresHistoricalSnapshot,
    Phase1GPostgresReadOnlyError,
    project_phase1g_target_snapshot,
)
from backend.services.advisory_phase1.phase1g_historical_trace_contract import (
    Phase1GHistoricalTraceError,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    Phase1GSourceReplayError,
    REASON_G2_UNEXPECTED_ERROR,
    parse_phase1g_source_operation,
    replay_phase1g_source_operation,
)
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)
from backend.services.advisory_phase1.source_revision_postgres import (
    PostgresSourceRevisionRepository,
)
from backend.tests.advisory_phase1.test_phase1g_historical_trace_projection import (
    historical_raw_empty_case,
)


pytest_plugins = ("backend.tests.advisory_phase1.test_release_schema_dev_db",)


BASELINE_G2_SQL = """
CREATE SCHEMA selection;
CREATE SCHEMA strategy_pkg;
CREATE TABLE selection.daily_selection_evidence (
    evidence_id TEXT PRIMARY KEY,
    target_trade_date DATE NOT NULL,
    cutoff_date DATE,
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    runtime_profile_version_id TEXT NOT NULL,
    runtime_profile_hash TEXT NOT NULL,
    source_type TEXT NOT NULL,
    data_source TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    excluded_count INTEGER NOT NULL,
    artifact_hash TEXT NOT NULL,
    evidence_payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE strategy_pkg.selection_score_artifact (
    artifact_id TEXT PRIMARY KEY,
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    trade_date DATE NOT NULL,
    data_source TEXT NOT NULL,
    runtime_config_hash TEXT NOT NULL,
    scores_json JSONB NOT NULL,
    artifact_sha256 TEXT,
    score_count INTEGER NOT NULL,
    universe_count INTEGER NOT NULL,
    top_score_symbol TEXT,
    status TEXT NOT NULL,
    metadata JSONB NOT NULL,
    artifact_contract_version TEXT,
    artifact_payload_sha256 TEXT,
    artifact_input_context_hash TEXT,
    source_revision_set_hash TEXT,
    asset_closure_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE strategy_pkg.package (
    package_id TEXT PRIMARY KEY,
    manifest_json JSONB NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    alpha_mode TEXT NOT NULL
);
CREATE TABLE app.advisory_strategy_binding_version (
    binding_version_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL,
    package_mode TEXT NOT NULL,
    package_ids JSONB NOT NULL,
    runtime_config_json JSONB NOT NULL,
    effective_from_trade_date DATE,
    effective_to_trade_date DATE,
    activation_status TEXT NOT NULL,
    binding_payload_json JSONB NOT NULL
);
"""


def test_historical_sql_registry_is_exact_read_only_and_has_no_latest_fallback() -> (
    None
):
    for sql in PHASE1G_G2_HISTORICAL_SQL_REGISTRY.values():
        normalized = " ".join(sql.upper().split())
        assert normalized.startswith("SELECT ")
        assert "SELECT *" not in normalized
        assert not any(
            token in normalized
            for token in (" INSERT ", " UPDATE ", " DELETE ", " MERGE ")
        )
        assert " ORDER BY " not in normalized
        assert " LIMIT " not in normalized
    assert "WHERE EVIDENCE_ID = %S" in " ".join(
        PHASE1G_G2_HISTORICAL_SQL_REGISTRY["dse_exact"].upper().split()
    )


def test_unexpected_projection_error_logs_useful_traceback_without_exception_payload(
    caplog,
) -> None:  # type: ignore[no-untyped-def]
    case = historical_raw_empty_case()
    secret = "".join(("credential", "-token-must-not-leak"))

    @contextmanager
    def failing_factory():  # type: ignore[no-untyped-def]
        raise RuntimeError(secret)
        yield

    with caplog.at_level("ERROR"), pytest.raises(Phase1GSourceReplayError) as error:
        project_phase1g_target_snapshot(
            conn_factory=failing_factory,
            phase1e_plan=case["plan"],
            target_request=case["target"],
        )
    assert error.value.reason_code == REASON_G2_UNEXPECTED_ERROR
    assert "credential-token-must-not-leak" not in caplog.text
    assert case["plan"].plan_id in caplog.text
    assert "RuntimeError" in caplog.text
    assert "redacted unexpected PostgreSQL projection failure" in caplog.text


class _StateCursor:
    def __init__(
        self, *, read_only: str = "on", isolation: str = "repeatable read"
    ) -> None:
        self.read_only = read_only
        self.isolation = isolation

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _sql, _params=None):  # type: ignore[no-untyped-def]
        return None

    def fetchone(self):  # type: ignore[no-untyped-def]
        return {
            "transaction_read_only": self.read_only,
            "transaction_isolation": self.isolation,
        }


class _StateConnection:
    def __init__(self, cursor: _StateCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):  # type: ignore[no-untyped-def]
        return self._cursor


def test_snapshot_rejects_non_read_only_or_wrong_isolation_and_exact_reads_report_not_found() -> (
    None
):
    for cursor in (
        _StateCursor(read_only="off"),
        _StateCursor(isolation="read committed"),
    ):

        @contextmanager
        def factory(cursor=cursor):  # type: ignore[no-untyped-def]
            yield _StateConnection(cursor)

        with pytest.raises(Phase1GPostgresReadOnlyError):
            with Phase1GPostgresHistoricalProjection(factory).snapshot():
                pass

    class _MissingCursor:
        def execute(self, _sql, _params=None):  # type: ignore[no-untyped-def]
            return None

        def fetchone(self):  # type: ignore[no-untyped-def]
            return None

    snapshot = Phase1GPostgresHistoricalSnapshot(_MissingCursor())
    for read in (
        lambda: snapshot.dse("missing-dse"),
        lambda: snapshot.artifact("missing-artifact"),
        lambda: snapshot.package("missing-package", "f" * 64),
        lambda: snapshot.binding("missing-binding", "missing-program"),
    ):
        with pytest.raises(Phase1GHistoricalTraceError) as error:
            read()
        assert getattr(error.value, "reason_code", None)


@pytest.mark.usefixtures("disposable_postgres")
def test_disposable_postgres_snapshot_projection_and_caller_owned_freeze_matrix(database_factory) -> None:  # type: ignore[no-untyped-def]
    config = database_factory()
    case = historical_raw_empty_case()
    migrations = (
        Path(
            "backend/db/migrations/add_advisory_source_availability_ledger_20260712.sql"
        ),
        Path(
            "backend/db/migrations/add_advisory_phase1c2_source_revision_cutoff_20260713.sql"
        ),
    )
    conn = psycopg2.connect(**config.connect_kwargs())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for migration in migrations:
                cur.execute(migration.read_text(encoding="utf-8"))
            cur.execute(BASELINE_G2_SQL)
            cur.execute(
                "ALTER TABLE app.advisory_source_availability_event DISABLE TRIGGER USER"
            )
            event = case["event"]
            item = event.input
            cur.execute(
                """
                INSERT INTO app.advisory_source_availability_event (
                    availability_event_id, append_request_hash, dataset_name, source_role,
                    partition_key, partition_key_hash, partition_chain_key, revision_id,
                    event_revision_no, event_type, predecessor_event_hash, provider_job_id,
                    refresh_job_id, provider_published_at, first_observed_at,
                    formal_available_at, schema_fingerprint, row_count,
                    partition_content_hash, quality_status, reason_codes,
                    event_content_hash, created_by_service_principal
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event.availability_event_id,
                    item.append_request_hash,
                    item.dataset_name,
                    item.source_role,
                    psycopg2.extras.Json(item.partition_key),
                    item.partition_key_hash,
                    item.partition_chain_key,
                    item.revision_id,
                    item.event_revision_no,
                    item.event_type.value,
                    item.predecessor_event_hash,
                    item.provider_job_id,
                    item.refresh_job_id,
                    item.provider_published_at,
                    item.first_observed_at,
                    item.formal_available_at,
                    item.schema_fingerprint,
                    item.row_count,
                    item.partition_content_hash,
                    item.quality_status,
                    psycopg2.extras.Json(list(item.reason_codes)),
                    event.event_content_hash,
                    item.created_by_service_principal,
                ),
            )
            cur.execute(
                "ALTER TABLE app.advisory_source_availability_event ENABLE TRIGGER USER"
            )
            dse = case["dse_row"]
            cur.execute(
                """
                INSERT INTO selection.daily_selection_evidence
                    (evidence_id, target_trade_date, cutoff_date, package_id, manifest_sha256,
                     runtime_profile_version_id, runtime_profile_hash, source_type, data_source,
                     candidate_count, excluded_count, artifact_hash, evidence_payload_json, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dse["evidence_id"],
                    dse["target_trade_date"],
                    dse["cutoff_date"],
                    dse["package_id"],
                    dse["manifest_sha256"],
                    dse["runtime_profile_version_id"],
                    dse["runtime_profile_hash"],
                    dse["source_type"],
                    dse["data_source"],
                    dse["candidate_count"],
                    dse["excluded_count"],
                    dse["artifact_hash"],
                    psycopg2.extras.Json(dse["evidence_payload_json"]),
                    dse["created_at"],
                ),
            )
            artifact = case["artifact_row"]
            cur.execute(
                """
                INSERT INTO strategy_pkg.selection_score_artifact
                    (artifact_id, package_id, manifest_sha256, trade_date, data_source,
                     runtime_config_hash, scores_json, artifact_sha256, score_count,
                     universe_count, top_score_symbol, status, metadata,
                     artifact_contract_version, artifact_payload_sha256,
                     artifact_input_context_hash, source_revision_set_hash, asset_closure_hash, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    artifact["artifact_id"],
                    artifact["package_id"],
                    artifact["manifest_sha256"],
                    artifact["trade_date"],
                    artifact["data_source"],
                    artifact["runtime_config_hash"],
                    psycopg2.extras.Json(artifact["scores_json"]),
                    artifact["artifact_sha256"],
                    artifact["score_count"],
                    artifact["universe_count"],
                    artifact["top_score_symbol"],
                    artifact["status"],
                    psycopg2.extras.Json(canonicalize(artifact["metadata"])),
                    artifact["artifact_contract_version"],
                    artifact["artifact_payload_sha256"],
                    artifact["artifact_input_context_hash"],
                    artifact["source_revision_set_hash"],
                    artifact["asset_closure_hash"],
                    artifact["created_at"],
                ),
            )
            package = case["package_row"]
            cur.execute(
                "INSERT INTO strategy_pkg.package VALUES (%s, %s, %s, %s)",
                (
                    package["package_id"],
                    psycopg2.extras.Json(package["manifest_json"]),
                    package["manifest_sha256"],
                    package["alpha_mode"],
                ),
            )
            binding = case["binding_row"]
            cur.execute(
                """
                INSERT INTO app.advisory_strategy_binding_version
                    (binding_version_id, program_id, package_mode, package_ids,
                     runtime_config_json, effective_from_trade_date, effective_to_trade_date,
                     activation_status, binding_payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    binding["binding_version_id"],
                    binding["program_id"],
                    binding["package_mode"],
                    psycopg2.extras.Json(binding["package_ids"]),
                    psycopg2.extras.Json(binding["runtime_config_json"]),
                    binding["effective_from_trade_date"],
                    binding["effective_to_trade_date"],
                    binding["activation_status"],
                    psycopg2.extras.Json(binding["binding_payload_json"]),
                ),
            )
    finally:
        conn.close()

    @contextmanager
    def conn_factory():  # type: ignore[no-untyped-def]
        connection = psycopg2.connect(**config.connect_kwargs())
        try:
            yield connection
        finally:
            connection.rollback()
            connection.close()

    snapshot = project_phase1g_target_snapshot(
        conn_factory=conn_factory,
        phase1e_plan=case["plan"],
        target_request=case["target"],
    )
    assert snapshot.projected_candidate_rows == 0
    assert snapshot.source_revision_freeze_intent.source_revision_set.members

    verify = psycopg2.connect(**config.connect_kwargs())
    try:
        with verify.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM app.advisory_source_revision_set")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM app.advisory_source_revision_member")
            assert cur.fetchone()[0] == 0
    finally:
        verify.close()

    operation = parse_phase1g_source_operation(
        phase1e_plan=case["plan"], target_request=case["target"]
    )
    replay = replay_phase1g_source_operation(
        projection=operation, availability_events=(case["event"],)
    )
    writer = psycopg2.connect(**config.connect_kwargs())
    try:
        with writer.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            PostgresSourceRevisionRepository().freeze_in_transaction(
                cur, replay.source_revision_set
            )
        writer.rollback()
        with writer.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM app.advisory_source_revision_set")
            assert cur.fetchone()[0] == 0

        def freeze_once() -> str:
            concurrent = psycopg2.connect(**config.connect_kwargs())
            try:
                with concurrent.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cur:
                    frozen = PostgresSourceRevisionRepository().freeze_in_transaction(
                        cur, replay.source_revision_set
                    )
                concurrent.commit()
                return frozen.source_revision_set_hash
            finally:
                concurrent.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            hashes = list(executor.map(lambda _index: freeze_once(), range(2)))
        assert hashes == [
            replay.source_revision_set.source_revision_set_hash,
            replay.source_revision_set.source_revision_set_hash,
        ]
        with writer.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            PostgresSourceRevisionRepository().freeze_in_transaction(
                cur, replay.source_revision_set
            )
        writer.commit()
        with writer.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            assert (
                PostgresSourceRevisionRepository.read_exact_in_transaction(
                    cur, replay.source_revision_set.source_revision_set_hash
                )
                == replay.source_revision_set
            )
        writer.rollback()
    finally:
        writer.close()

    # A later committed event is invisible to a snapshot that already read the chain.
    with Phase1GPostgresHistoricalProjection(conn_factory).snapshot() as read_snapshot:
        before = read_snapshot.source_events.load_events(operation.requirement_set)
        late = SourceAvailabilityEvent.from_request(
            SourceAvailabilityEventRequest(
                dataset_name=case["event"].input.dataset_name,
                source_role=case["event"].input.source_role,
                partition_key=case["event"].input.partition_key,
                revision_id="revision-2",
                event_revision_no=2,
                event_type=SourceAvailabilityEventType.CORRECTED,
                predecessor_event_hash=case["event"].event_content_hash,
                schema_fingerprint=case["event"].input.schema_fingerprint,
                row_count=case["event"].input.row_count,
                partition_content_hash="8" * 64,
                quality_status="PASS",
                created_by_service_principal="fixture-observer",
            ),
            first_observed_at=datetime.now(timezone.utc),
        )
        append = psycopg2.connect(**config.connect_kwargs())
        append.autocommit = True
        try:
            with append.cursor() as cur:
                item = late.input
                cur.execute(
                    """
                    INSERT INTO app.advisory_source_availability_event
                        (availability_event_id, append_request_hash, dataset_name, source_role,
                         partition_key, partition_key_hash, partition_chain_key, revision_id,
                         event_revision_no, event_type, predecessor_event_hash, provider_job_id,
                         refresh_job_id, provider_published_at, first_observed_at, formal_available_at,
                         schema_fingerprint, row_count, partition_content_hash, quality_status,
                         reason_codes, event_content_hash, created_by_service_principal)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        late.availability_event_id,
                        item.append_request_hash,
                        item.dataset_name,
                        item.source_role,
                        psycopg2.extras.Json(item.partition_key),
                        item.partition_key_hash,
                        item.partition_chain_key,
                        item.revision_id,
                        item.event_revision_no,
                        item.event_type.value,
                        item.predecessor_event_hash,
                        item.provider_job_id,
                        item.refresh_job_id,
                        item.provider_published_at,
                        item.first_observed_at,
                        item.formal_available_at,
                        item.schema_fingerprint,
                        item.row_count,
                        item.partition_content_hash,
                        item.quality_status,
                        psycopg2.extras.Json([]),
                        late.event_content_hash,
                        item.created_by_service_principal,
                    ),
                )
        finally:
            append.close()
        after = read_snapshot.source_events.load_events(operation.requirement_set)
        assert after == before == (case["event"],)
