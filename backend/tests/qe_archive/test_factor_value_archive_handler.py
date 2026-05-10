"""Integration tests for FactorValueArchiveHandler against dev DB qe_archive."""
from __future__ import annotations

from datetime import date

import pytest

from backend.services.qe_archive.handlers.contract import (
    HandlerStatus,
    PayloadValidationError,
)
from backend.services.qe_archive.handlers.factor_value_archive_handler import (
    FactorValueArchiveHandler,
)
from backend.services.qe_archive.models import ArchiveJobRecord, ClaimedOutboxEvent


# ---------------------------------------------------------------------------
# pure validation
# ---------------------------------------------------------------------------

class TestValidatePayload:
    def setup_method(self):
        self.h = FactorValueArchiveHandler(source_loader=lambda p: [])

    def test_missing_factor_name(self):
        with pytest.raises(PayloadValidationError, match="factor_name"):
            self.h.validate_payload({
                "schema_version": 1, "routing_class": "archive",
                "code_text_hash": "abc",
            })

    def test_missing_code_text_hash(self):
        with pytest.raises(PayloadValidationError, match="code_text_hash"):
            self.h.validate_payload({
                "schema_version": 1, "routing_class": "archive",
                "factor_name": "f",
            })

    def test_unknown_schema_version_rejected(self):
        with pytest.raises(PayloadValidationError, match="schema_version"):
            self.h.validate_payload({
                "schema_version": 2, "routing_class": "archive",
                "factor_name": "f", "code_text_hash": "h",
            })

    def test_telemetry_routing_class_rejected(self):
        with pytest.raises(PayloadValidationError, match="routing_class"):
            self.h.validate_payload({
                "schema_version": 1, "routing_class": "telemetry",
                "factor_name": "f", "code_text_hash": "h",
            })


def _make_event(factor_name: str, code_text_hash: str, **extra) -> ClaimedOutboxEvent:
    payload = {
        "schema_version": 1,
        "routing_class": "archive",
        "factor_name": factor_name,
        "code_text_hash": code_text_hash,
        "data_start": "2018-01-01",
        "data_end": "2026-05-01",
        "snapshot_date": "2026-05-09",
    }
    payload.update(extra)
    return ClaimedOutboxEvent(
        event_id=f"evt_{factor_name}_{code_text_hash[:8]}",
        event_type="factor.recompute.completed",
        source_system="factor_pipeline",
        source_id=factor_name,
        source_sub_id=code_text_hash,
        payload=payload,
    )


@pytest.mark.usefixtures("cleanup_qe_archive")
class TestFactorValueHandler:
    SAMPLE_ROWS = [
        {"trade_date": date(2026, 5, 5), "code": "000001.SZ", "value": 0.123},
        {"trade_date": date(2026, 5, 5), "code": "000002.SZ", "value": 0.456},
        {"trade_date": date(2026, 5, 6), "code": "000001.SZ", "value": 0.234},
        # cross-month to exercise DEFAULT partition (P1.5)
        {"trade_date": date(2026, 4, 30), "code": "000001.SZ", "value": 0.111},
    ]

    def test_handle_factor_recompute_happy_path(self, dev_conn_provider):
        handler = FactorValueArchiveHandler(
            connection_provider=dev_conn_provider,
            source_loader=lambda payload: self.SAMPLE_ROWS,
        )
        evt = _make_event("test_factor_a", "sha256:abc123")
        job = ArchiveJobRecord(event_id=evt.event_id, job_type="factor_value_capture")
        result = handler.handle(evt, job)

        assert result.status is HandlerStatus.SUCCESS
        assert result.rows_inserted == len(self.SAMPLE_ROWS)
        assert result.rows_upserted == 0

        # Verify rows landed (including cross-month one — DEFAULT partition coverage)
        with dev_conn_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*) FROM qe_archive.factor_value
                       WHERE factor_name = %s AND code_text_hash = %s""",
                    ("test_factor_a", "sha256:abc123"),
                )
                assert cur.fetchone()[0] == len(self.SAMPLE_ROWS)

                cur.execute(
                    """SELECT COUNT(*) FROM qe_archive.factor_value
                       WHERE trade_date = '2026-04-30'"""
                )
                # cross-month row landed in DEFAULT partition (P1.5 verified)
                assert cur.fetchone()[0] >= 1

    def test_idempotency_replay_no_new_inserts(self, dev_conn_provider):
        handler = FactorValueArchiveHandler(
            connection_provider=dev_conn_provider,
            source_loader=lambda payload: self.SAMPLE_ROWS,
        )
        evt = _make_event("test_factor_b", "sha256:def456")
        job = ArchiveJobRecord(event_id=evt.event_id, job_type="factor_value_capture")

        first = handler.handle(evt, job)
        assert first.status is HandlerStatus.SUCCESS
        assert first.rows_inserted == len(self.SAMPLE_ROWS)

        second = handler.handle(evt, job)
        assert second.status is HandlerStatus.SUCCESS
        assert second.rows_inserted == 0
        assert second.rows_upserted == len(self.SAMPLE_ROWS)

        with dev_conn_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*) FROM qe_archive.factor_value
                       WHERE factor_name = %s""",
                    ("test_factor_b",),
                )
                assert cur.fetchone()[0] == len(self.SAMPLE_ROWS)

    def test_different_code_text_hash_creates_separate_versions(self, dev_conn_provider):
        """Per design §7.3: same factor_name with different code_text_hash
        keeps both versions (multi-version comparison)."""
        handler = FactorValueArchiveHandler(
            connection_provider=dev_conn_provider,
            source_loader=lambda payload: self.SAMPLE_ROWS,
        )
        e1 = _make_event("test_factor_c", "sha256:v1")
        e2 = _make_event("test_factor_c", "sha256:v2")
        job = ArchiveJobRecord(event_id="x", job_type="factor_value_capture")

        handler.handle(e1, job)
        handler.handle(e2, job)

        with dev_conn_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COUNT(DISTINCT code_text_hash) FROM qe_archive.factor_value
                       WHERE factor_name = %s""",
                    ("test_factor_c",),
                )
                assert cur.fetchone()[0] == 2

    def test_empty_source_returns_noop(self, dev_conn_provider):
        handler = FactorValueArchiveHandler(
            connection_provider=dev_conn_provider,
            source_loader=lambda payload: [],
        )
        evt = _make_event("test_factor_empty", "sha256:zzz")
        job = ArchiveJobRecord(event_id=evt.event_id, job_type="factor_value_capture")
        result = handler.handle(evt, job)
        assert result.status is HandlerStatus.NOOP

    def test_partition_routing_default_partition_works(self, dev_conn_provider):
        """Specifically verify P1.5 DEFAULT partition catches dates outside
        the example y2026m05 partition without 'no partition for row' errors."""
        out_of_range = [
            {"trade_date": date(2018, 1, 15), "code": "000001.SZ", "value": 0.5},
            {"trade_date": date(2099, 12, 31), "code": "000001.SZ", "value": 0.7},
        ]
        handler = FactorValueArchiveHandler(
            connection_provider=dev_conn_provider,
            source_loader=lambda p: out_of_range,
        )
        evt = _make_event("test_factor_partition", "sha256:partition")
        job = ArchiveJobRecord(event_id=evt.event_id, job_type="factor_value_capture")
        result = handler.handle(evt, job)
        assert result.status is HandlerStatus.SUCCESS, \
            f"DEFAULT partition should accept any trade_date; got: {result.error_message}"
        assert result.rows_inserted == 2
