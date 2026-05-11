"""Regression tests for backend/services/qe_archive/handlers/contract.py.

Covers Codex T14a review fix round 2 (P1.1 + P1.2) plus the additional
handler-contract test ideas Codex flagged:

  P1.1: ArchiveHandler subclass definition smoke (was crashing on
        __abstractmethods__ access in __init_subclass__).
  P1.2: ArchiveResult status invariants (4 cases — string coercion, identity,
        and FAILED-without-message rejection).
  Extra: can_handle routing_class gate (paper.daemon.* telemetry rejected).
  Extra: validate_payload schema_version failure (missing / unknown rejected).

These tests do not touch the database or any worker machinery; they exercise
the in-memory contract.py classes only. Run via:

    pytest backend/tests/qe_archive/test_handler_contract.py -v
"""
from __future__ import annotations

import pytest

from backend.services.qe_archive.handlers.contract import (
    PAYLOAD_ROUTING_CLASS_KEY,
    PAYLOAD_SCHEMA_VERSION_KEY,
    ArchiveHandler,
    ArchiveResult,
    HandlerStatus,
    PayloadValidationError,
    ROUTING_CLASS_ARCHIVE,
)
from backend.services.qe_archive.models import ArchiveJobRecord, ClaimedOutboxEvent


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_event(
    event_type: str = "paper.portfolio_run.completed",
    routing_class: str | None = ROUTING_CLASS_ARCHIVE,
    schema_version: int | str | None = 1,
    extra_payload: dict | None = None,
) -> ClaimedOutboxEvent:
    payload: dict = {}
    if schema_version is not None:
        payload[PAYLOAD_SCHEMA_VERSION_KEY] = schema_version
    if routing_class is not None:
        payload[PAYLOAD_ROUTING_CLASS_KEY] = routing_class
    if extra_payload:
        payload.update(extra_payload)
    return ClaimedOutboxEvent(
        event_id="evt_test",
        event_type=event_type,
        source_system="paper_v2",
        source_id="src_test",
        source_sub_id=None,
        payload=payload,
        retry_count=0,
    )


class _DummyHandler(ArchiveHandler):
    """Minimal concrete subclass for P1.1 smoke + can_handle/validate tests."""

    event_type = "paper.portfolio_run.completed"
    supported_schema_versions = (1,)

    def handle(self, event, archive_job):
        return ArchiveResult(status=HandlerStatus.SUCCESS)


# ---------------------------------------------------------------------------
# P1.1: subclass definition must not crash on __abstractmethods__ access
# ---------------------------------------------------------------------------

class TestSubclassDefinitionSmoke:
    def test_concrete_subclass_can_be_defined(self):
        """The class statement above for _DummyHandler MUST have completed
        without raising AttributeError; if we got here, it did. This test
        also exercises the post-import path one more time by defining a
        fresh subclass inline."""

        class _AnotherHandler(ArchiveHandler):
            event_type = "factor.recompute.completed"
            supported_schema_versions = (1,)

            def handle(self, event, archive_job):
                return ArchiveResult(status=HandlerStatus.NOOP)

        assert _AnotherHandler.event_type == "factor.recompute.completed"

    def test_subclass_missing_event_type_raises(self):
        with pytest.raises(TypeError, match="event_type"):
            class _BadHandler(ArchiveHandler):
                # event_type intentionally omitted (inherits empty string)
                supported_schema_versions = (1,)

                def handle(self, event, archive_job):
                    return ArchiveResult(status=HandlerStatus.SUCCESS)

    def test_subclass_missing_schema_versions_raises(self):
        with pytest.raises(TypeError, match="supported_schema_versions"):
            class _BadHandler(ArchiveHandler):
                event_type = "x.y.z"
                # supported_schema_versions intentionally omitted

                def handle(self, event, archive_job):
                    return ArchiveResult(status=HandlerStatus.SUCCESS)


# ---------------------------------------------------------------------------
# P1.2: ArchiveResult status invariants (the 4 cases from the brief)
# ---------------------------------------------------------------------------

class TestArchiveResultStatusInvariants:
    def test_string_failed_without_message_rejected(self):
        """Case 1 from brief: ArchiveResult(status='failed') without
        error_message MUST raise. Previously the string bypassed enum
        identity check and silently accepted."""
        with pytest.raises(ValueError, match="FAILED status requires error_message"):
            ArchiveResult(status="failed")

    def test_string_failed_with_message_accepted(self):
        """Case 2: ArchiveResult(status='failed', error_message='x') OK."""
        r = ArchiveResult(status="failed", error_message="boom")
        assert r.status is HandlerStatus.FAILED
        assert r.error_message == "boom"

    def test_enum_failed_with_message_accepted(self):
        """Case 3: ArchiveResult(status=HandlerStatus.FAILED, error_message='x') OK.
        Previously the identity comparison `status is HandlerStatus.FAILED` was
        sound when the caller used the enum directly, but the symmetric string
        path failed silently. After fix both paths normalize to the enum first."""
        r = ArchiveResult(status=HandlerStatus.FAILED, error_message="boom")
        assert r.status is HandlerStatus.FAILED

    def test_enum_success_no_message(self):
        """Case 4: ArchiveResult(status=HandlerStatus.SUCCESS) OK."""
        r = ArchiveResult(status=HandlerStatus.SUCCESS)
        assert r.status is HandlerStatus.SUCCESS
        assert r.error_message is None

    def test_string_success_normalized(self):
        """String 'success' must coerce to HandlerStatus.SUCCESS, not stay a string."""
        r = ArchiveResult(status="success")
        assert r.status is HandlerStatus.SUCCESS
        assert isinstance(r.status, HandlerStatus)

    def test_invalid_string_status_rejected(self):
        with pytest.raises(ValueError, match="invalid status string"):
            ArchiveResult(status="ok_ish")

    def test_non_string_non_enum_status_rejected(self):
        with pytest.raises((ValueError, TypeError)):
            ArchiveResult(status=42)  # type: ignore[arg-type]

    def test_message_without_failed_status_rejected(self):
        with pytest.raises(ValueError, match="error_message only valid"):
            ArchiveResult(status=HandlerStatus.SUCCESS, error_message="leak")

    def test_negative_row_counts_rejected(self):
        with pytest.raises(ValueError, match="row counts must be non-negative"):
            ArchiveResult(status=HandlerStatus.SUCCESS, rows_inserted=-1)


# ---------------------------------------------------------------------------
# Extra: can_handle routing_class gate (paper.daemon.* telemetry rejected)
# ---------------------------------------------------------------------------

class TestCanHandleRoutingClass:
    def setup_method(self):
        self.handler = _DummyHandler()

    def test_archive_routing_accepted(self):
        evt = _make_event(routing_class=ROUTING_CLASS_ARCHIVE)
        assert self.handler.can_handle(evt) is True

    def test_telemetry_routing_rejected(self):
        """paper-v2 T13 added routing_class='telemetry' on paper.daemon.*
        events; archive handlers MUST never accept those."""
        evt = _make_event(routing_class="telemetry")
        assert self.handler.can_handle(evt) is False

    def test_missing_routing_class_rejected(self):
        evt = _make_event(routing_class=None)
        assert self.handler.can_handle(evt) is False

    def test_arbitrary_routing_class_rejected(self):
        evt = _make_event(routing_class="anything_else")
        assert self.handler.can_handle(evt) is False

    def test_event_type_mismatch_rejected(self):
        evt = _make_event(event_type="factor.recompute.completed")
        # _DummyHandler.event_type = paper.portfolio_run.completed
        assert self.handler.can_handle(evt) is False


# ---------------------------------------------------------------------------
# Extra: validate_payload schema_version failure modes
# ---------------------------------------------------------------------------

class TestValidatePayload:
    def setup_method(self):
        self.handler = _DummyHandler()

    def test_valid_payload_accepted(self):
        evt = _make_event(schema_version=1)
        # Should not raise
        self.handler.validate_payload(evt.payload)

    def test_missing_schema_version_rejected(self):
        evt = _make_event(schema_version=None)
        with pytest.raises(PayloadValidationError, match="schema_version"):
            self.handler.validate_payload(evt.payload)

    def test_unknown_schema_version_rejected(self):
        evt = _make_event(schema_version=999)
        with pytest.raises(PayloadValidationError, match="unsupported schema_version"):
            self.handler.validate_payload(evt.payload)

    def test_zero_schema_version_rejected(self):
        """0 is falsy and the validator treats falsy as missing per the
        no-silent-fallback rule."""
        evt = _make_event(schema_version=0)
        with pytest.raises(PayloadValidationError, match="schema_version"):
            self.handler.validate_payload(evt.payload)

    def test_routing_class_telemetry_rejected_at_validate(self):
        """validate_payload also enforces routing_class — defense in depth so
        a handler that bypasses can_handle still cannot persist telemetry."""
        evt = _make_event(routing_class="telemetry")
        with pytest.raises(PayloadValidationError, match="routing_class"):
            self.handler.validate_payload(evt.payload)

    def test_non_mapping_payload_rejected(self):
        with pytest.raises(PayloadValidationError, match="must be a mapping"):
            self.handler.validate_payload("not a dict")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Smoke: handle() round-trip via _DummyHandler
# ---------------------------------------------------------------------------

class TestHandleRoundTrip:
    def test_dummy_handle_returns_success(self):
        handler = _DummyHandler()
        evt = _make_event()
        job = ArchiveJobRecord(event_id="evt_test", job_type="paper_v2_capture")
        result = handler.handle(evt, job)
        assert result.status is HandlerStatus.SUCCESS
        assert result.rows_inserted == 0
        assert result.error_message is None
