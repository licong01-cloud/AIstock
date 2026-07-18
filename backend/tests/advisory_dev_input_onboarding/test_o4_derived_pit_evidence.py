from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.advisory_dev_input_onboarding.phase1e_derived_pit import (
    AdvisoryDerivedPitEvidenceProbe,
    DerivedPitEvidenceRequest,
    DerivedPitEvidenceStatus,
    REASON_DERIVED_PIT_STATE_CONFLICT,
    REASON_DERIVED_PIT_UPSTREAM_AUDIT_MISSING,
    _stock_universe_fingerprint_hash,
    build_derived_pit_observation_decisions,
)
from backend.services.advisory_phase1.source_ledger import InMemorySourceAvailabilityLedger
from backend.services.advisory_phase1.source_observer import (
    ObservationOutcome,
    SOURCE_QUERY_TEMPLATES,
    o4_advisory_input_source_observer_config,
)


SHA_A = "a" * 64


class _Cursor:
    def __init__(self, *, state: dict[str, Any] | None, audits: dict[str, list[dict[str, Any]]], spans: list[dict[str, Any]]) -> None:
        self.state = state
        self.audits = audits
        self.spans = spans
        self.sql = ""
        self.params: tuple[Any, ...] = ()

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.sql = " ".join(sql.split())
        self.params = params or ()

    def fetchone(self) -> dict[str, Any] | None:
        if "FROM market.stock_universe_pit_state" in self.sql:
            return self.state
        raise AssertionError(f"unexpected fetchone SQL: {self.sql}")

    def fetchall(self) -> list[dict[str, Any]]:
        if "FROM market.dataset_date_refresh_audit" in self.sql:
            return self.audits.get(str(self.params[0]), [])
        if "FROM market.stock_universe_pit_spans" in self.sql:
            return self.spans
        raise AssertionError(f"unexpected fetchall SQL: {self.sql}")


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def cursor(self, **_kwargs: Any) -> _Cursor:
        return self._cursor


def _factory(cursor: _Cursor):
    @contextmanager
    def factory():
        yield _Connection(cursor)

    return factory


def _request() -> DerivedPitEvidenceRequest:
    config = o4_advisory_input_source_observer_config()
    return DerivedPitEvidenceRequest(
        target_database_identity_hash=SHA_A,
        program_id="program_single",
        decision_trade_date=date(2026, 7, 18),
        universe_key="shsz_st_pit_active_v1",
        decision_cutoff_ts=datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc),
        observer_config_hash=config.config_hash(SOURCE_QUERY_TEMPLATES),
        query_registry_hash=config.query_registry_hash(SOURCE_QUERY_TEMPLATES),
    )


def _state(*, dirty: bool = False) -> dict[str, Any]:
    fingerprint = {
        "fingerprint_end_date": "2026-07-18",
        "stock_basic": {"listed_asof_count": 2},
        "stock_st": {"row_count": 1},
        "stock_st_events": {"row_count": 1},
        "trading_calendar": {"max_trading_day": "2026-07-18"},
    }
    return {
        "universe_key": "shsz_st_pit_active_v1",
        "rule_version": "st_pit_v1",
        "scope": "st_only_active",
        "start_date": date(2020, 1, 1),
        "end_date": date(2026, 7, 18),
        "status": "ready",
        "dirty": dirty,
        "source_fingerprint": fingerprint,
        "source_fingerprint_sha256": _stock_universe_fingerprint_hash(fingerprint),
        "last_build_summary": {"validation": {"overlap_error_count": 0}},
        "last_error": None,
        "generated_at": datetime(2026, 7, 18, 6, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 7, 18, 6, 0, tzinfo=timezone.utc),
    }


def _audit(dataset: str, *, refreshed_at: datetime | None = None) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "trade_date": date(2026, 7, 18),
        "data_source": "physical_audit_seed",
        "job_id": f"job_{dataset}",
        "status": "success",
        "row_count": 2,
        "refreshed_at": refreshed_at or datetime(2026, 7, 18, 5, 0, tzinfo=timezone.utc),
        "error_message": None,
        "metadata": {},
        "data_max_at": None,
        "written_rows": 2,
        "expected_rows": 2,
        "coverage_ratio": 1.0,
        "quality_status": "ok",
        "failure_category": None,
    }


def _spans() -> list[dict[str, Any]]:
    return [
        {
            "universe_key": "shsz_st_pit_active_v1",
            "ts_code": symbol,
            "eligible_start": date(2026, 1, 1),
            "eligible_end": date(2026, 12, 31),
            "entry_reason": "ipo_365d",
            "exit_reason": "generation_end",
            "base_list_date": date(2020, 1, 1),
            "ipo_eligible_date": date(2021, 1, 1),
            "entry_event_date": None,
            "exit_event_date": None,
            "terminal_exit": False,
            "rule_version": "st_pit_v1",
            "generated_at": datetime(2026, 7, 18, 5, 30, tzinfo=timezone.utc),
            "metadata": {},
        }
        for symbol in ("000001.SZ", "600000.SH")
    ]


def _probe(*, state: dict[str, Any] | None, audits: dict[str, list[dict[str, Any]]], spans: list[dict[str, Any]]):
    cursor = _Cursor(state=state, audits=audits, spans=spans)
    return AdvisoryDerivedPitEvidenceProbe(conn_factory=_factory(cursor)).probe(
        request=_request(),
        config=o4_advisory_input_source_observer_config(),
    )


def test_derived_pit_ready_evidence_builds_idempotent_ledger_decisions() -> None:
    result = _probe(
        state=_state(),
        audits={dataset: [_audit(dataset)] for dataset in ("stock_basic", "stock_st_events", "trading_calendar")},
        spans=_spans(),
    )

    assert result.receipt.status is DerivedPitEvidenceStatus.READY
    assert result.receipt.spans_row_count == 2
    assert set(result.receipt.upstream_audit_row_hashes) == {
        "stock_basic",
        "stock_st_events",
        "trading_calendar",
    }

    first = build_derived_pit_observation_decisions(
        evidence=result,
        terminal_events={"pit_universe": None, "pit_universe_build_state": None},
    )
    assert {item.outcome for item in first} == {ObservationOutcome.EVENT_APPENDED}
    assert all(item.partition_key["universe_key"] == "shsz_st_pit_active_v1" for item in first)

    ledger = InMemorySourceAvailabilityLedger(
        now_provider=lambda: datetime(2026, 7, 18, 7, 0, tzinfo=timezone.utc)
    )
    terminal = {
        item.event_request.source_role: ledger.append(item.event_request)
        for item in first
        if item.event_request is not None
    }
    second = build_derived_pit_observation_decisions(evidence=result, terminal_events=terminal)
    assert {item.outcome for item in second} == {ObservationOutcome.UNCHANGED}


def test_derived_pit_missing_upstream_audit_is_pending() -> None:
    result = _probe(
        state=_state(),
        audits={
            "stock_basic": [_audit("stock_basic")],
            "stock_st_events": [_audit("stock_st_events")],
        },
        spans=_spans(),
    )

    assert result.receipt.status is DerivedPitEvidenceStatus.PENDING
    assert result.receipt.reason_codes == (REASON_DERIVED_PIT_UPSTREAM_AUDIT_MISSING,)


def test_derived_pit_accepts_legacy_fingerprint_without_rebuilding_state() -> None:
    state = _state()
    state["source_fingerprint"].pop("fingerprint_end_date")
    state["source_fingerprint_sha256"] = _stock_universe_fingerprint_hash(state["source_fingerprint"])

    result = _probe(
        state=state,
        audits={dataset: [_audit(dataset)] for dataset in ("stock_basic", "stock_st_events", "trading_calendar")},
        spans=_spans(),
    )

    assert result.receipt.status is DerivedPitEvidenceStatus.READY


def test_derived_pit_dirty_state_is_blocked_without_querying_spans() -> None:
    result = _probe(state=_state(dirty=True), audits={}, spans=[])

    assert result.receipt.status is DerivedPitEvidenceStatus.BLOCKED
    assert result.receipt.reason_codes == (REASON_DERIVED_PIT_STATE_CONFLICT,)


def test_derived_pit_module_has_no_build_or_write_path() -> None:
    source = Path(
        "backend/services/advisory_dev_input_onboarding/phase1e_derived_pit.py"
    ).read_text(encoding="utf-8")

    assert "from backend.services.stock_universe_pit_service import" not in source
    assert "import backend.services.stock_universe_pit_service" not in source
    assert ".ensure_" not in source
    assert " INSERT " not in source.upper()
    assert " UPDATE " not in source.upper()
    assert " DELETE " not in source.upper()
