from __future__ import annotations

import datetime as dt
import inspect

from scripts import build_stock_universe_pit_spans as pit_builder

from backend.services.stock_universe_pit_service import (
    DEFAULT_ST_PIT_RULE_VERSION,
    StockUniversePitService,
    _fingerprint_sha256,
)


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
