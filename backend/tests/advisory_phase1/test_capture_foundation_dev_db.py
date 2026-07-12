"""Explicitly authorized rollback-only DEV-DB L4 for Phase 1C capture foundation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest
from dotenv import load_dotenv

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchRequest,
    CaptureMembership,
    CapturePlan,
    PostgresCaptureBatchRepository,
    PostgresTraceAdmissionValidator,
    PostgresTraceCaptureGapRepository,
)
from backend.services.advisory_phase1.control_binding import (
    ControlBindingRequest,
    ControlType,
    PostgresControlBindingRepository,
)
from backend.services.advisory_phase1.observation_capture import expected_evidence_bundle_hash
from backend.services.advisory_phase1.stage_trace import (
    PHASE1_STAGE_TRACE_SCHEMA_VERSION,
    StageTraceEnvelope,
    TraceCaptureBinding,
    TraceCapturePolicy,
)
from backend.services.advisory_phase1.trace_outbox import ExpectedTraceIdentity, PostgresTraceOutboxRepository


_ENV_FILE = Path("F:/Dev/AIstock/.env")
_MIGRATION = Path("backend/db/migrations/add_advisory_phase1_capture_foundation_20260713.sql")
_ROLLBACK = Path("backend/db/migrations/add_advisory_phase1_capture_foundation_20260713.rollback.sql")
_NOW = datetime(2026, 7, 13, 2, 0, tzinfo=timezone.utc)


def _dev_dsn() -> dict[str, Any]:
    if os.getenv("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("set AISTOCK_DEV_DB_E2E=1 to authorize the DEV-DB stateful L4 gate")
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)
    dsn = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": int(os.getenv("TDX_DB_DEV_PORT", "0")),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in str(dsn["dbname"] or "").lower():
        raise AssertionError(f"refusing Phase 1C L4 target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.skip("DEV DB credentials are unavailable")
    return dsn


def _binding(*, control_binding_event_hash: str = "0" * 64) -> TraceCaptureBinding:
    return TraceCaptureBinding(
        control_binding_event_hash=control_binding_event_hash,
        binding_id="phase1c-l4-binding",
        binding_version="1",
        handoff_readiness_hash="a" * 64,
        admission_scope_id="phase1c-l4-scope",
        admission_scope_hash="b" * 64,
        capture_batch_id="phase1c-l4-batch",
        capture_fencing_token=1,
        capture_policy=TraceCapturePolicy(
            policy_id="phase1c-l4-policy",
            policy_version="1",
            max_candidates=10,
            max_bytes=100_000,
            max_capture_ms=1_000,
        ),
    )


def _plan(*, evidence_bundle_hash: str) -> CapturePlan:
    return CapturePlan(
        selection_run_id="phase1c-l4-selection",
        package_id="phase1c-l4-package",
        manifest_sha256="c" * 64,
        decision_as_of_trade_date=date(2026, 7, 10).isoformat(),
        selection_as_of_trade_date=date(2026, 7, 10).isoformat(),
        target_trade_date=date(2026, 7, 13).isoformat(),
        decision_cutoff_ts=_NOW,
        alpha_mode="single_alpha",
        selection_runtime_semantics_hash="d" * 64,
        package_effective_config_hash="e" * 64,
        calendar_version="market.trading_calendar.v1",
        calendar_hash="f" * 64,
        stable_signal_semantics_hash="1" * 64,
        canonical_signal_scope_hash="2" * 64,
        phase0a_audit_id="phase1c-l4-audit",
        phase0a_audit_manifest_hash="3" * 64,
        handoff_readiness_hash="a" * 64,
        admission_scope_id="phase1c-l4-scope",
        admission_scope_hash="b" * 64,
        signal_source_revision_set_id="phase1c-l4-source-set",
        signal_source_revision_set_hash="4" * 64,
        phase0a_signal_context_hash="5" * 64,
        evidence_bundle_hash=evidence_bundle_hash,
        selection_evidence_id="phase1c-l4-evidence",
        selection_evidence_hash="6" * 64,
        selection_run_content_hash="7" * 64,
        selection_score_artifact_id="phase1c-l4-artifact",
        selection_score_artifact_hash="8" * 64,
        runtime_profile_version_id="phase1c-l4-profile",
        runtime_profile_version_hash="9" * 64,
        hmm_snapshot_status="NOT_APPLICABLE",
        risk_policy_hash="a" * 64,
        universe_policy_hash="b" * 64,
        symbol_normalization_policy_hash="c" * 64,
        valid_no_candidate=True,
        evidence_available_at=_NOW,
        audit_target_id="phase1c-l4-target",
        target_scope_hash="d" * 64,
        capability="HISTORICAL_RESEARCH",
        oos_interval_id="phase1c-l4-oos",
        oos_interval_hash="e" * 64,
        evidence_scope="RETROSPECTIVE_RESEARCH_ONLY",
        signal_evidence_level="RETROSPECTIVE_RESEARCH_ONLY",
        effective_cutoff_date=date(2026, 7, 10).isoformat(),
        program_id="phase1c-l4-program",
        binding_version_id="phase1c-l4-binding-version",
        source_run_id="phase1c-l4-source-run",
        lineage_source_type="PHASE0A_AUDIT",
    )


def _envelope(binding: TraceCaptureBinding) -> StageTraceEnvelope:
    content = canonicalize(
        {
            "schema_version": PHASE1_STAGE_TRACE_SCHEMA_VERSION,
            "selection_identity": {
                "selection_run_id": "phase1c-l4-selection",
                "package_id": "phase1c-l4-package",
                "manifest_sha256": "c" * 64,
                "decision_as_of_trade_date": date(2026, 7, 10).isoformat(),
                "data_source": "DB_HISTORICAL",
                "execution_origin": "ADVISORY_RUN",
                "research_scope": "HISTORICAL_RESEARCH_ONLY",
                "execution_prohibited": True,
            },
            "trace_capture_binding": binding.model_dump(mode="json"),
            "raw_score_artifact": {"artifact_payload_sha256": "8" * 64, "scores_json": []},
            "stage_trace": [],
            "hmm_metadata": {},
            "risk_metadata": {},
            "universe_metadata": {},
            "component_capability": "NOT_APPLICABLE",
        }
    )
    digest = canonical_json_sha256(content)
    return StageTraceEnvelope(
        trace_outbox_id=f"sto_{digest[:20]}",
        trace_content_hash=digest,
        trace_content=content,
        candidate_count=0,
        size_bytes=len(json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")),
    )


def _apply_sql(conn: Any, path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))


def _insert_signal(cur: Any, *, signal_id: str, scope_hash: str, target_trade_date: date, plan: CapturePlan) -> None:
    cur.execute(
        """
        INSERT INTO app.advisory_signal_observation (
            canonical_signal_id, signal_schema_version, stable_signal_semantics_hash,
            canonical_signal_scope_hash, decision_as_of_trade_date, selection_as_of_trade_date,
            target_trade_date, decision_cutoff_ts, package_id, manifest_sha256, alpha_mode,
            selection_runtime_semantics_hash, package_effective_config_hash, calendar_version, calendar_hash
        ) VALUES (%s, 'advisory_canonical_signal_v1', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            signal_id,
            plan.stable_signal_semantics_hash,
            scope_hash,
            plan.decision_as_of_trade_date,
            plan.selection_as_of_trade_date,
            target_trade_date,
            plan.decision_cutoff_ts,
            plan.package_id,
            plan.manifest_sha256,
            plan.alpha_mode,
            plan.selection_runtime_semantics_hash,
            plan.package_effective_config_hash,
            plan.calendar_version,
            plan.calendar_hash,
        ),
    )


def _insert_version(
    cur: Any,
    *,
    version_id: str,
    signal_id: str,
    revision_no: int,
    supersedes: str | None,
    content_hash: str,
    batch_id: str,
    plan: CapturePlan,
) -> None:
    cur.execute(
        """
        INSERT INTO app.advisory_signal_observation_version (
            observation_version_id, canonical_signal_id, observation_schema_version,
            observation_revision_no, supersedes_observation_version_id,
            signal_source_revision_set_id, signal_source_revision_set_hash,
            phase0a_signal_context_hash, evidence_bundle_hash, stage_evidence_bundle_hash,
            selection_evidence_id, selection_evidence_hash, selection_run_id,
            selection_run_content_hash, selection_score_artifact_id, selection_score_artifact_hash,
            runtime_profile_version_id, runtime_profile_version_hash,
            hmm_snapshot_id, hmm_snapshot_hash, hmm_snapshot_status,
            risk_policy_hash, universe_policy_hash, symbol_normalization_policy_hash,
            valid_no_candidate, observation_status, evidence_available_at,
            observation_content_hash, created_by_capture_batch_id
        ) VALUES (
            %s, %s, 'advisory_signal_observation_version_v1', %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            NULL, NULL, %s, %s, %s, %s, %s, 'COMPLETE', %s, %s, %s
        )
        """,
        (
            version_id,
            signal_id,
            revision_no,
            supersedes,
            plan.signal_source_revision_set_id,
            plan.signal_source_revision_set_hash,
            plan.phase0a_signal_context_hash,
            plan.evidence_bundle_hash,
            "1" * 64,
            plan.selection_evidence_id,
            plan.selection_evidence_hash,
            plan.selection_run_id,
            plan.selection_run_content_hash,
            plan.selection_score_artifact_id,
            plan.selection_score_artifact_hash,
            plan.runtime_profile_version_id,
            plan.runtime_profile_version_hash,
            plan.hmm_snapshot_status,
            plan.risk_policy_hash,
            plan.universe_policy_hash,
            plan.symbol_normalization_policy_hash,
            plan.valid_no_candidate,
            plan.evidence_available_at,
            content_hash,
            batch_id,
        ),
    )


def test_capture_foundation_l4_dev_db_apply_readback_and_rollback() -> None:
    conn = psycopg2.connect(**_dev_dsn(), connect_timeout=5)
    conn.autocommit = True
    applied = False
    try:
        _apply_sql(conn, _ROLLBACK)
        _apply_sql(conn, _MIGRATION)
        applied = True
        conn.autocommit = False

        @contextmanager
        def conn_factory() -> Iterator[Any]:
            yield conn

        provisional_binding = _binding()
        controls = PostgresControlBindingRepository(conn_factory=conn_factory)
        control_payload = provisional_binding.model_dump(mode="json", exclude={"control_binding_event_hash"})
        control_event = controls.append(
            ControlBindingRequest(
                control_type=ControlType.TRACE_CAPTURE,
                environment="DEV",
                admission_scope_set_hash=provisional_binding.admission_scope_hash,
                config_source="phase1c-l4",
                config_payload=control_payload,
                config_or_store_backend_hash=canonical_json_sha256(control_payload),
                enabled=True,
                binding_event_revision_no=1,
                created_by_service_principal="phase1c-l4",
            )
        )
        binding = _binding(control_binding_event_hash=control_event.binding_event_hash)
        envelope = _envelope(binding)
        provisional_plan = _plan(evidence_bundle_hash="0" * 64)
        plan_payload = provisional_plan.model_dump(mode="python", exclude={"plan_hash"})
        plan_payload["evidence_bundle_hash"] = expected_evidence_bundle_hash(
            plan=provisional_plan,
            trace_content_hash=envelope.trace_content_hash,
        )
        plan = CapturePlan(**plan_payload)
        capture = PostgresCaptureBatchRepository(conn_factory=conn_factory)
        planned = capture.create(CaptureBatchRequest(capture_batch_id=binding.capture_batch_id, binding=binding, plans=(plan,)))
        running = capture.acquire(
            capture_batch_id=planned.request.capture_batch_id,
            expected_row_version=planned.row_version,
            lease_seconds=60,
        )
        outbox = PostgresTraceOutboxRepository(
            conn_factory=conn_factory,
            admission_validator=PostgresTraceAdmissionValidator(),
        )
        trace = outbox.append(envelope, binding=binding)
        captured = capture.add_membership(
            capture_batch_id=running.request.capture_batch_id,
            expected_row_version=running.row_version,
            fencing_token=running.fencing_token,
            membership=CaptureMembership(
                evidence_role="trace_outbox",
                evidence_id=trace.trace_outbox_id,
                evidence_content_hash=envelope.trace_content_hash,
            ),
        )
        complete = capture.complete(
            capture_batch_id=captured.request.capture_batch_id,
            expected_row_version=captured.row_version,
            fencing_token=captured.fencing_token,
        )
        assert complete.capture_receipt_hash
        assert outbox.append(envelope, binding=binding) == trace
        identity = ExpectedTraceIdentity.from_envelope(envelope, binding=binding)
        gaps = PostgresTraceCaptureGapRepository(conn_factory=conn_factory)
        assert gaps.record(identity=identity, reason_code="ADVISORY_PHASE1_TRACE_CAPTURE_LOST") == gaps.record(
            identity=identity,
            reason_code="ADVISORY_PHASE1_TRACE_CAPTURE_LOST",
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MIN(cal_date) FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date > %s",
                (plan.decision_as_of_trade_date,),
            )
            next_trade_date = cur.fetchone()[0]
            assert next_trade_date is not None
            _insert_signal(
                cur,
                signal_id="phase1c-l4-signal-1",
                scope_hash="2" * 64,
                target_trade_date=next_trade_date,
                plan=plan,
            )
            _insert_signal(
                cur,
                signal_id="phase1c-l4-signal-2",
                scope_hash="3" * 64,
                target_trade_date=next_trade_date,
                plan=plan,
            )
            _insert_version(
                cur,
                version_id="phase1c-l4-version-1",
                signal_id="phase1c-l4-signal-1",
                revision_no=1,
                supersedes=None,
                content_hash="4" * 64,
                batch_id=binding.capture_batch_id,
                plan=plan,
            )
            _insert_version(
                cur,
                version_id="phase1c-l4-version-2",
                signal_id="phase1c-l4-signal-1",
                revision_no=2,
                supersedes="phase1c-l4-version-1",
                content_hash="5" * 64,
                batch_id=binding.capture_batch_id,
                plan=plan,
            )

            cur.execute("SAVEPOINT invalid_revision_chain")
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1_OBSERVATION_REVISION_CHAIN_INVALID"):
                _insert_version(
                    cur,
                    version_id="phase1c-l4-cross-signal-version",
                    signal_id="phase1c-l4-signal-2",
                    revision_no=2,
                    supersedes="phase1c-l4-version-1",
                    content_hash="6" * 64,
                    batch_id=binding.capture_batch_id,
                    plan=plan,
                )
            cur.execute("ROLLBACK TO SAVEPOINT invalid_revision_chain")

            cur.execute("SAVEPOINT invalid_calendar")
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1_OBSERVATION_CALENDAR_INVALID"):
                _insert_signal(
                    cur,
                    signal_id="phase1c-l4-invalid-calendar",
                    scope_hash="7" * 64,
                    target_trade_date=next_trade_date + date.resolution,
                    plan=plan,
                )
            cur.execute("ROLLBACK TO SAVEPOINT invalid_calendar")

            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1_CAPTURE_BATCH_IMMUTABLE"):
                cur.execute(
                    "UPDATE app.advisory_capture_batch SET capture_request_hash = %s WHERE capture_batch_id = %s",
                    ("f" * 64, binding.capture_batch_id),
                )
            conn.rollback()
    finally:
        try:
            if applied:
                conn.rollback()
                conn.autocommit = True
                _apply_sql(conn, _ROLLBACK)
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass('app.advisory_capture_batch')")
                    assert cur.fetchone() == (None,)
        finally:
            conn.close()
