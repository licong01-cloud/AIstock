"""Explicitly authorized rollback-only DEV-DB L4 for Phase 1B trace outbox."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
import os
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest
from dotenv import load_dotenv

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.control_binding import (
    ControlBindingRequest,
    ControlType,
    PostgresControlBindingRepository,
)
from backend.services.advisory_phase1.stage_trace import (
    PHASE1_STAGE_TRACE_SCHEMA_VERSION,
    StageTraceEnvelope,
    TraceCaptureBinding,
    TraceCapturePolicy,
)
from backend.services.advisory_phase1.trace_outbox import (
    PostgresTraceOutboxRepository,
    TraceDeliveryEventRequest,
    TraceDeliveryEventType,
)


_ENV_FILE = Path("F:/Dev/AIstock/.env")


class _FixtureAdmissionValidator:
    """Explicit test fixture until Phase 1C owns persisted capture-batch validation."""

    def __init__(self) -> None:
        self.call_count = 0

    def validate(self, *, envelope, binding, conn=None) -> None:  # type: ignore[no-untyped-def]
        self.call_count += 1
        assert conn is not None
        assert envelope.trace_content["trace_capture_binding"]["binding_hash"] == binding.binding_hash


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
        raise AssertionError(f"refusing trace-outbox L4 target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.skip("DEV DB credentials are unavailable")
    return dsn


def _binding(*, control_binding_event_hash: str = "7" * 64) -> TraceCaptureBinding:
    return TraceCaptureBinding(
        control_binding_event_hash=control_binding_event_hash,
        binding_id="l4-trace-binding",
        binding_version="1",
        handoff_readiness_hash="a" * 64,
        admission_scope_id="l4-scope",
        admission_scope_hash="b" * 64,
        capture_batch_id="l4-batch",
        capture_fencing_token=1,
        capture_policy=TraceCapturePolicy(
            policy_id="l4-trace-policy",
            policy_version="1",
            max_candidates=10,
            max_bytes=100_000,
            max_capture_ms=1_000,
        ),
    )


def _control_request(binding: TraceCaptureBinding) -> ControlBindingRequest:
    config_payload = binding.model_dump(mode="json", exclude={"control_binding_event_hash"})
    return ControlBindingRequest(
        control_type=ControlType.TRACE_CAPTURE,
        environment="DEV",
        admission_scope_set_hash=binding.admission_scope_hash,
        config_source="l4-fixture",
        config_payload=config_payload,
        config_or_store_backend_hash=canonical_json_sha256(config_payload),
        enabled=True,
        binding_event_revision_no=1,
        created_by_service_principal="dev-db-l4",
    )


def _envelope(binding: TraceCaptureBinding) -> StageTraceEnvelope:
    content = canonicalize(
        {
            "schema_version": PHASE1_STAGE_TRACE_SCHEMA_VERSION,
            "selection_identity": {
                "selection_run_id": "l4-selection-run",
                "package_id": "l4-package",
                "manifest_sha256": "c" * 64,
                "decision_as_of_trade_date": date(2026, 7, 10).isoformat(),
                "data_source": "DB_HISTORICAL",
                "execution_origin": "ADVISORY_RUN",
                "research_scope": "HISTORICAL_RESEARCH_ONLY",
                "execution_prohibited": True,
            },
            "trace_capture_binding": binding.model_dump(mode="json"),
            "raw_score_artifact": {"artifact_payload_sha256": "d" * 64, "scores_json": []},
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
        size_bytes=len(json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")),
    )


def test_trace_outbox_l4_dev_db_is_immutable_idempotent_and_rolls_back() -> None:
    conn = psycopg2.connect(**_dev_dsn(), connect_timeout=5)
    conn.autocommit = False
    try:
        @contextmanager
        def conn_factory() -> Iterator[Any]:
            yield conn

        provisional_binding = _binding()
        controls = PostgresControlBindingRepository(conn_factory=conn_factory)
        control_event = controls.append(_control_request(provisional_binding))
        binding = _binding(control_binding_event_hash=control_event.binding_event_hash)
        envelope = _envelope(binding)
        validator = _FixtureAdmissionValidator()
        repository = PostgresTraceOutboxRepository(conn_factory=conn_factory, admission_validator=validator)
        first = repository.append(envelope, binding=binding)
        assert repository.append(envelope, binding=binding) == first
        assert validator.call_count == 1
        failed = repository.append_delivery(
            TraceDeliveryEventRequest(
                trace_outbox_id=first.trace_outbox_id,
                delivery_event_no=1,
                event_type=TraceDeliveryEventType.OBSERVATION_WRITE_FAILED,
                writer_attempt_no=1,
                reason_codes=("L4_WRITER_DOWN",),
            )
        )
        assert repository.append_delivery(
            TraceDeliveryEventRequest(
                trace_outbox_id=first.trace_outbox_id,
                delivery_event_no=2,
                predecessor_event_hash=failed.delivery_event_hash,
                event_type=TraceDeliveryEventType.OBSERVATION_WRITTEN,
                writer_attempt_no=2,
            )
        ).request.event_type is TraceDeliveryEventType.OBSERVATION_WRITTEN
        with conn.cursor() as cur:
            with pytest.raises(psycopg2.Error, match="ADVISORY_PHASE1_TRACE_IMMUTABLE"):
                cur.execute(
                    "UPDATE app.advisory_selection_stage_trace_outbox SET candidate_count = 1 WHERE trace_outbox_id = %s",
                    (first.trace_outbox_id,),
                )
            conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM app.advisory_selection_stage_trace_outbox WHERE trace_outbox_id = %s",
                (first.trace_outbox_id,),
            )
            assert cur.fetchone() == (0,)
    finally:
        conn.rollback()
        conn.close()
