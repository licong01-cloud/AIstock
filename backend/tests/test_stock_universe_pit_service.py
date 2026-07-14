from __future__ import annotations

import datetime as dt
import inspect

import pytest

from scripts import build_stock_universe_pit_spans as pit_builder

from backend.services.stock_universe_pit_service import (
    DEFAULT_ST_PIT_RULE_VERSION,
    StockUniversePitError,
    StockUniversePitService,
    _fingerprint_sha256,
)


QE_SNAPSHOT_KEY = "shsz_st_pit_qe_dataset_test_20180801_20260630_v1"


def _ready_immutable_state() -> dict[str, object]:
    return {
        "universe_key": QE_SNAPSHOT_KEY,
        "status": "ready",
        "dirty": False,
        "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
        "scope": "st_only_active",
        "start_date": dt.date(2018, 8, 1),
        "end_date": dt.date(2026, 6, 30),
        "source_fingerprint_sha256": "dataset-fingerprint",
        "last_build_summary": {"validation": {"overlap_error_count": 0}},
    }


def test_immutable_dataset_snapshot_reuses_existing_state_without_source_refresh(monkeypatch) -> None:
    service = StockUniversePitService()
    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "get_status", lambda **_kwargs: _ready_immutable_state())
    monkeypatch.setattr(
        service,
        "compute_source_fingerprint",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("immutable snapshot must not refresh source")),
    )
    monkeypatch.setattr(
        service,
        "rebuild_st_pit_universe",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("immutable snapshot must not rebuild")),
    )

    result = service.ensure_immutable_dataset_snapshot(
        universe_key=QE_SNAPSHOT_KEY,
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 6, 30),
    )

    assert result["rebuilt"] is False
    assert result["source_fingerprint_sha256"] == "dataset-fingerprint"


def test_immutable_dataset_snapshot_bootstraps_only_a_missing_exact_key(monkeypatch) -> None:
    service = StockUniversePitService()
    source = {"fingerprint_end_date": "2026-06-30"}
    states = iter([{"status": "missing", "dirty": True}, _ready_immutable_state()])
    captured: dict[str, object] = {}
    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "get_status", lambda **_kwargs: next(states))
    monkeypatch.setattr(service, "compute_source_fingerprint", lambda **_kwargs: source)
    monkeypatch.setattr(service, "rebuild_st_pit_universe", lambda **kwargs: captured.update(kwargs) or {})

    result = service.ensure_immutable_dataset_snapshot(
        universe_key=QE_SNAPSHOT_KEY,
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 6, 30),
    )

    assert result["rebuilt"] is True
    assert captured["write_mode"] == "replace"
    assert captured["incremental_from"] is None
    assert captured["end_date"] == dt.date(2026, 6, 30)


def test_immutable_dataset_snapshot_never_repairs_or_extends_existing_key(monkeypatch) -> None:
    service = StockUniversePitService()
    invalid_state = _ready_immutable_state()
    invalid_state["dirty"] = True
    invalid_state["end_date"] = dt.date(2026, 7, 13)
    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "get_status", lambda **_kwargs: invalid_state)
    monkeypatch.setattr(
        service,
        "rebuild_st_pit_universe",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("existing immutable key must never rebuild")),
    )

    with pytest.raises(StockUniversePitError, match="publish a new dataset contract"):
        service.ensure_immutable_dataset_snapshot(
            universe_key=QE_SNAPSHOT_KEY,
            start_date=dt.date(2018, 8, 1),
            end_date=dt.date(2026, 6, 30),
        )


def test_wait_for_ready_state_retries_initial_missing_state_until_peer_is_ready(monkeypatch) -> None:
    service = StockUniversePitService()
    building_state = {
        "status": "building",
        "dirty": True,
        "source_fingerprint_sha256": "dataset-fingerprint",
    }
    states = iter(
        [
            {"status": "missing", "dirty": True},
            building_state,
            _ready_immutable_state(),
        ]
    )
    monkeypatch.setattr(service, "get_status", lambda **_kwargs: next(states))
    monkeypatch.setattr("backend.services.stock_universe_pit_service.time.sleep", lambda _seconds: None)

    state, reason = service._wait_for_ready_state(
        universe_key=QE_SNAPSHOT_KEY,
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 6, 30),
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        source_sha="dataset-fingerprint",
        refresh_policy="coverage",
        timeout_seconds=1.0,
        retryable_reasons=frozenset({"missing_state", "status_building"}),
    )

    assert state == _ready_immutable_state()
    assert reason == "ready"


def test_wait_for_ready_state_keeps_terminal_contract_failures_loud(monkeypatch) -> None:
    service = StockUniversePitService()
    invalid_state = _ready_immutable_state()
    invalid_state["rule_version"] = "unexpected-rule"
    calls = 0

    def get_status(**_kwargs):
        nonlocal calls
        calls += 1
        return invalid_state

    monkeypatch.setattr(service, "get_status", get_status)

    state, reason = service._wait_for_ready_state(
        universe_key=QE_SNAPSHOT_KEY,
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 6, 30),
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        source_sha="dataset-fingerprint",
        refresh_policy="coverage",
        timeout_seconds=180.0,
        retryable_reasons=frozenset({"missing_state", "status_building"}),
    )

    assert state is None
    assert reason == "rule_version_changed"
    assert calls == 1


def test_rebuild_lock_loser_waits_across_initial_missing_state(monkeypatch) -> None:
    service = StockUniversePitService()
    captured: dict[str, object] = {}

    class LockCursor:
        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            return False

        def execute(self, _query, _params):
            return None

        def fetchone(self):
            return (False,)

    class LockConnection:
        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            return False

        def cursor(self):
            return LockCursor()

    def wait_for_ready_state(**kwargs):
        captured.update(kwargs)
        return _ready_immutable_state(), "ready"

    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "get_status", lambda **_kwargs: {"status": "missing", "dirty": True})
    monkeypatch.setattr(service, "_wait_for_ready_state", wait_for_ready_state)
    monkeypatch.setattr("backend.services.stock_universe_pit_service.get_conn", lambda: LockConnection())

    result = service.rebuild_st_pit_universe(
        universe_key=QE_SNAPSHOT_KEY,
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 6, 30),
        source_fingerprint={"fingerprint_end_date": "2026-06-30"},
        source_fingerprint_sha256="dataset-fingerprint",
        skip_if_ready=True,
        refresh_policy="coverage",
        lock_wait_seconds=180.0,
    )

    assert result["rebuilt"] is False
    assert result["reason"] == "built_by_peer:ready"
    assert captured["retryable_reasons"] == frozenset({"missing_state", "status_building"})


def test_needs_rebuild_ignores_dirty_for_coverage_policy() -> None:
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

    assert needs is False
    assert reason == "coverage_ready_source_changed_ignored"


def test_needs_rebuild_when_state_is_dirty_for_source_fingerprint_policy() -> None:
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
        refresh_policy="source_fingerprint",
    )

    assert needs is True
    assert reason == "dirty"


def test_needs_rebuild_when_source_fingerprint_changed_for_source_policy() -> None:
    service = StockUniversePitService()

    needs, reason = service._needs_rebuild(
        state={
            "status": "ready",
            "dirty": False,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 4, 30),
            "source_fingerprint_sha256": "old",
            "last_build_summary": {"validation": {}},
        },
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 4, 30),
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        source_sha="new",
        refresh_policy="source_fingerprint",
    )

    assert needs is True
    assert reason == "source_fingerprint_changed"


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


def test_pit_builder_db_config_preserves_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("TDX_DB_HOST", "127.0.0.1")
    monkeypatch.setenv("TDX_DB_PORT", "5433")
    monkeypatch.setenv("TDX_DB_NAME", "aistock_dev")
    monkeypatch.setenv("TDX_DB_USER", "dev_user")
    monkeypatch.setenv("TDX_DB_PASSWORD", "dev_password")

    cfg = pit_builder._db_config()

    assert cfg["host"] == "127.0.0.1"
    assert cfg["port"] == 5433
    assert cfg["dbname"] == "aistock_dev"
    assert cfg["user"] == "dev_user"


def test_ensure_uses_incremental_write_for_end_extension(monkeypatch) -> None:
    service = StockUniversePitService()
    captured: dict[str, object] = {}
    source = {"source": "fingerprint"}

    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "compute_source_fingerprint", lambda *, end_date=None: source)
    monkeypatch.setattr(
        service,
        "get_status",
        lambda *, universe_key="shsz_st_pit_active_v1": {
            "status": "ready",
            "dirty": False,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 4, 28),
            "source_fingerprint_sha256": _fingerprint_sha256(source),
            "last_build_summary": {"validation": {"overlap_error_count": 0}},
        },
    )

    def fake_rebuild(**kwargs):
        captured.update(kwargs)
        return {"universe_key": kwargs["universe_key"], "status": "ready", "rebuilt": True}

    monkeypatch.setattr(service, "rebuild_st_pit_universe", fake_rebuild)

    result = service.ensure_st_pit_universe(end_date=dt.date(2026, 5, 13))

    assert result["status"] == "ready"
    assert captured["write_mode"] == "incremental"
    assert captured["incremental_from"] == dt.date(2026, 4, 29)
    assert captured["refresh_policy"] == "coverage"


def test_ensure_reuses_same_range_even_when_source_fingerprint_changes(monkeypatch) -> None:
    service = StockUniversePitService()
    source = {"source": "new-fingerprint"}
    called = {"rebuild": False}

    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "compute_source_fingerprint", lambda *, end_date=None: source)
    monkeypatch.setattr(
        service,
        "get_status",
        lambda *, universe_key="shsz_st_pit_active_v1": {
            "status": "ready",
            "dirty": True,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 4, 30),
            "source_fingerprint_sha256": "old-fingerprint",
            "last_build_summary": {"validation": {"overlap_error_count": 0}},
        },
    )

    def fake_rebuild(**kwargs):
        called["rebuild"] = True
        return {"universe_key": kwargs["universe_key"], "status": "ready", "rebuilt": True}

    monkeypatch.setattr(service, "rebuild_st_pit_universe", fake_rebuild)

    result = service.ensure_st_pit_universe(end_date=dt.date(2026, 4, 30))

    assert result["status"] == "ready"
    assert result["rebuilt"] is False
    assert result["reason"] == "coverage_ready_source_changed_ignored"
    assert called["rebuild"] is False


def test_ensure_rebuilds_dirty_same_range_for_source_fingerprint_policy(monkeypatch) -> None:
    service = StockUniversePitService()
    source = {"source": "new-fingerprint"}
    captured: dict[str, object] = {}

    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "compute_source_fingerprint", lambda *, end_date=None: source)
    monkeypatch.setattr(
        service,
        "get_status",
        lambda *, universe_key="shsz_st_pit_active_v1": {
            "status": "ready",
            "dirty": True,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 4, 30),
            "source_fingerprint_sha256": "old-fingerprint",
            "last_build_summary": {"validation": {"overlap_error_count": 0}},
        },
    )

    def fake_rebuild(**kwargs):
        captured.update(kwargs)
        return {"universe_key": kwargs["universe_key"], "status": "ready", "rebuilt": True}

    monkeypatch.setattr(service, "rebuild_st_pit_universe", fake_rebuild)

    result = service.ensure_st_pit_universe(
        end_date=dt.date(2026, 4, 30),
        refresh_policy="source_fingerprint",
    )

    assert result["status"] == "ready"
    assert result["reason"] == "dirty"
    assert captured["write_mode"] == "replace"
    assert captured["incremental_from"] is None
    assert captured["refresh_policy"] == "source_fingerprint"


def test_ensure_preserves_later_shared_coverage_for_shorter_refresh_request(monkeypatch) -> None:
    service = StockUniversePitService()
    captured: dict[str, object] = {}
    source = {"source": "current-live-fingerprint"}

    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(
        service,
        "compute_source_fingerprint",
        lambda *, end_date=None: source if end_date == dt.date(2026, 7, 13) else (_ for _ in ()).throw(
            AssertionError(f"fingerprint requested for regressed end_date={end_date}")
        ),
    )
    monkeypatch.setattr(
        service,
        "get_status",
        lambda *, universe_key="shsz_st_pit_active_v1": {
            "status": "ready",
            "dirty": True,
            "rule_version": DEFAULT_ST_PIT_RULE_VERSION,
            "scope": "st_only_active",
            "start_date": dt.date(2018, 8, 1),
            "end_date": dt.date(2026, 7, 13),
            "source_fingerprint_sha256": "old-fingerprint",
            "last_build_summary": {"validation": {"overlap_error_count": 0}},
        },
    )

    def fake_rebuild(**kwargs):
        captured.update(kwargs)
        return {"universe_key": kwargs["universe_key"], "status": "ready", "rebuilt": True}

    monkeypatch.setattr(service, "rebuild_st_pit_universe", fake_rebuild)

    result = service.ensure_st_pit_universe(
        end_date=dt.date(2026, 6, 30),
        refresh_policy="source_fingerprint",
    )

    assert result["status"] == "ready"
    assert captured["end_date"] == dt.date(2026, 7, 13)


def test_pit_rebuild_paths_do_not_refresh_global_data_stats() -> None:
    assert "refresh_data_stats" not in inspect.getsource(StockUniversePitService.rebuild_st_pit_universe)
    assert "refresh_data_stats" not in inspect.getsource(pit_builder.build)
