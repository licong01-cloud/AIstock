"""Explicitly authorized DEV-DB L4 for the Batch C additive migration.

It is skipped unless the caller explicitly opts in.  It never reads `.env`
credentials until that opt-in is set and rejects every target except the local
development database used by the existing Phase 1C L4 suite.
"""

from __future__ import annotations

from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest

from backend.services.advisory_phase0a.policy import canonicalize
from backend.services.advisory_phase1.dataset_build import build_id_for, logical_build_key
from backend.tests.advisory_phase1.test_capture_foundation_dev_db import _apply_sql, _dev_dsn
from backend.tests.advisory_phase1.test_dataset_build import _request


_CAPTURE_MIGRATION = Path("backend/db/migrations/add_advisory_phase1_capture_foundation_20260713.sql")
_CAPTURE_ROLLBACK = Path("backend/db/migrations/add_advisory_phase1_capture_foundation_20260713.rollback.sql")
_MIGRATION = Path("backend/db/migrations/add_advisory_phase1c3_label_snapshot_foundation_20260713.sql")
_ROLLBACK = Path("backend/db/migrations/add_advisory_phase1c3_label_snapshot_foundation_20260713.rollback.sql")


def test_phase1c3_batch_c_l4_apply_readback_and_rollback() -> None:
    conn = psycopg2.connect(**_dev_dsn(), connect_timeout=5)
    conn.autocommit = True
    applied_capture = False
    applied_batch_c = False
    try:
        _apply_sql(conn, _CAPTURE_ROLLBACK)
        _apply_sql(conn, _CAPTURE_MIGRATION)
        applied_capture = True
        _apply_sql(conn, _MIGRATION)
        applied_batch_c = True
        _apply_sql(conn, _MIGRATION)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'app' AND table_name = 'advisory_capture_batch'
                   AND column_name IN ('capture_request_schema_version', 'capture_purpose')
                ORDER BY column_name
                """
            )
            assert [row[0] for row in cur.fetchall()] == ["capture_purpose", "capture_request_schema_version"]
            for table_name in (
                "advisory_outcome_label",
                "advisory_outcome_label_payload",
                "advisory_dataset_build",
                "advisory_dataset_build_attempt",
                "advisory_dataset_attempt_file",
                "advisory_dataset_snapshot",
                "advisory_dataset_snapshot_blob_ref",
            ):
                cur.execute("SELECT to_regclass(%s)", (f"app.{table_name}",))
                assert cur.fetchone()[0] == f"app.{table_name}"
            cur.execute(
                """
                SELECT tgname FROM pg_trigger
                 WHERE tgrelid IN (
                    'app.advisory_dataset_build'::regclass,
                    'app.advisory_dataset_build_attempt'::regclass,
                    'app.advisory_dataset_snapshot'::regclass,
                    'app.advisory_outcome_label'::regclass
                 ) AND NOT tgisinternal
                """
            )
            triggers = {row[0] for row in cur.fetchall()}
            assert {
                "trg_verify_advisory_dataset_build_transition",
                "trg_verify_advisory_dataset_build_predecessor",
                "trg_verify_advisory_dataset_build_attempt_closure_build",
                "trg_verify_advisory_outcome_label_owner",
                "trg_verify_advisory_dataset_snapshot_closure",
            } <= triggers

        conn.autocommit = False
        request = _request()
        logical_key = logical_build_key(request)
        build_id = build_id_for(logical_key, 1)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO app.advisory_dataset_build (
                    build_id, logical_build_key_sha256, build_generation, build_request_hash,
                    build_request_payload_jsonb, snapshot_source_revision_set_hash, capture_set_hash,
                    handoff_readiness_hash, admission_scope_set_hash, query_registry_hash,
                    date_start, date_end, builder_version, code_commit, writer_version,
                    partition_policy_hash, compression_config_hash, lifecycle_status, checkpoint,
                    current_fencing_token, row_version
                ) VALUES (
                    %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, 'ACTIVE', 'REQUESTED', 1, 1
                )
                """,
                (
                    build_id, logical_key, request.build_request_hash,
                    psycopg2.extras.Json(canonicalize(request.model_dump(mode="json"))),
                    request.snapshot_source_revision_set_hash, request.capture_set_hash,
                    request.handoff_readiness_hash, request.admission_scope_set_hash,
                    request.query_registry_hash, request.date_start, request.date_end,
                    request.builder_version, request.code_commit, request.writer_version,
                    request.partition_policy_hash, request.compression_config_hash,
                ),
            )

            cur.execute("SAVEPOINT illegal_checkpoint_jump")
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1C3_BUILD_TRANSITION_INVALID"):
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build SET lifecycle_status = 'SEALED', checkpoint = 'SEALED',
                        materialized_attempt_id = 'm', materialize_receipt_hash = %s, materialized_file_set_hash = %s,
                        verified_attempt_id = 'v', verify_receipt_hash = %s, verified_file_set_hash = %s,
                        verification_contract_version = 'PHASE1C3_BATCH_D_FULL_PARQUET_V1',
                        promoted_attempt_id = 'p', promotion_receipt_hash = %s, promoted_manifest_hash = %s,
                        sealed_attempt_id = 's', seal_receipt_hash = %s, sealed_snapshot_id = 'snapshot', row_version = 2
                     WHERE build_id = %s
                    """,
                    ("1" * 64, "2" * 64, "3" * 64, "4" * 64, "5" * 64, "6" * 64, "7" * 64, build_id),
                )
            cur.execute("ROLLBACK TO SAVEPOINT illegal_checkpoint_jump")

            cur.execute("SAVEPOINT orphan_active_attempt")
            cur.execute(
                """
                INSERT INTO app.advisory_dataset_build_attempt (
                    attempt_id, build_id, attempt_no, operation, attempt_state, lease_owner_id,
                    lease_token, fencing_token, expected_build_row_version, expected_checkpoint,
                    acquired_at, heartbeat_at, expires_at, started_at, operation_request_hash
                ) VALUES (
                    'orphan-attempt', %s, 1, 'MATERIALIZE', 'ACTIVE', 'test', 'lease', 2, 1,
                    'REQUESTED', clock_timestamp(), clock_timestamp(), clock_timestamp() + interval '1 minute',
                    clock_timestamp(), %s
                )
                """,
                (build_id, "8" * 64),
            )
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1C3_BUILD_ATTEMPT_CLOSURE_INVALID"):
                cur.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cur.execute("ROLLBACK TO SAVEPOINT orphan_active_attempt")

            cur.execute(
                "INSERT INTO app.advisory_dataset_blob (store_backend_hash, blob_sha256, size_bytes) VALUES (%s, %s, 1)",
                ("9" * 64, "a" * 64),
            )
            cur.execute("SAVEPOINT mutate_blob")
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1C3_APPEND_ONLY"):
                cur.execute(
                    "UPDATE app.advisory_dataset_blob SET size_bytes = 2 WHERE store_backend_hash = %s AND blob_sha256 = %s",
                    ("9" * 64, "a" * 64),
                )
            cur.execute("ROLLBACK TO SAVEPOINT mutate_blob")

            cur.execute("SAVEPOINT incomplete_snapshot")
            cur.execute(
                """
                INSERT INTO app.advisory_dataset_snapshot (
                    snapshot_id, snapshot_content_hash, snapshot_state, manifest_core_sha256,
                    manifest_sha256, promotion_receipt_uri, promotion_receipt_hash, build_id,
                    snapshot_schema_version, snapshot_source_revision_set_hash, capture_set_hash,
                    handoff_readiness_hash, admission_scope_set_hash, query_registry_hash,
                    builder_version, code_commit, writer_version, partition_policy_hash,
                    policy_compatibility_hash, dataset_capability_manifest,
                    dataset_capability_manifest_hash, schema_fingerprint, file_count, row_count,
                    total_bytes, label_maturity_event_summary
                ) VALUES (
                    'incomplete-snapshot', %s, 'SEALED', %s, %s, 'file:///promotion.json', %s, %s,
                    'snapshot-v1', %s, %s, %s, %s, %s, 'builder', 'commit', 'writer', %s,
                    %s, '{}'::jsonb, %s, %s, 1, 1, 1, '{}'::jsonb
                )
                """,
                tuple(f"{index:x}" * 64 for index in range(1, 13))[:4]
                + (build_id,)
                + tuple(f"{index:x}" * 64 for index in range(5, 14)),
            )
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1C3_SNAPSHOT_BUILD_CLOSURE_INVALID"):
                cur.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cur.execute("ROLLBACK TO SAVEPOINT incomplete_snapshot")
        conn.rollback()
        conn.autocommit = True
        _apply_sql(conn, _ROLLBACK)
        applied_batch_c = False
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM information_schema.columns
                 WHERE table_schema = 'app' AND table_name = 'advisory_capture_batch'
                   AND column_name IN ('capture_request_schema_version', 'capture_purpose')
                """
            )
            assert cur.fetchone()[0] == 0
    finally:
        if applied_batch_c:
            _apply_sql(conn, _ROLLBACK)
        if applied_capture:
            _apply_sql(conn, _CAPTURE_ROLLBACK)
        conn.close()
