from __future__ import annotations

import datetime as dt

from backend.services.stock_universe_pit_service import (
    DEFAULT_ST_PIT_RULE_VERSION,
    StockUniversePitService,
)


def test_needs_rebuild_when_state_is_dirty() -> None:
    service = StockUniversePitService()

    needs, reason = service._needs_rebuild(
        state={
            "status": "ready",
            "dirty": True,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 4, 30),
            "source_fingerprint_sha256": "abc",
            "last_build_summary": {"validation": {}},
        },
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 4, 30),
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        source_sha="abc",
    )

    assert needs is True
    assert reason == "dirty"


def test_needs_rebuild_rejects_failed_validation() -> None:
    service = StockUniversePitService()

    needs, reason = service._needs_rebuild(
        state={
            "status": "ready",
            "dirty": False,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 4, 30),
            "source_fingerprint_sha256": "abc",
            "last_build_summary": {"validation": {"overlap_error_count": 1}},
        },
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 4, 30),
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        source_sha="abc",
    )

    assert needs is True
    assert reason == "last_validation_failed"


def test_needs_rebuild_ready_state_passes() -> None:
    service = StockUniversePitService()

    needs, reason = service._needs_rebuild(
        state={
            "status": "ready",
            "dirty": False,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 4, 30),
            "source_fingerprint_sha256": "abc",
            "last_build_summary": {"validation": {"overlap_error_count": 0}},
        },
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 4, 30),
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        source_sha="abc",
    )

    assert needs is False
    assert reason == "ready"
