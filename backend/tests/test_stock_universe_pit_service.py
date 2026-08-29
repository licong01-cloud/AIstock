from __future__ import annotations

import datetime as dt
import inspect

import pytest

from scripts import build_stock_universe_pit_spans as pit_builder

from backend.services.stock_universe_pit_service import (
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    CANONICAL_PIT_UNIVERSE_KEY,
    DEFAULT_ST_PIT_UNIVERSE_KEY,
    DEFAULT_ST_PIT_RULE_VERSION,
    StockUniversePitError,
    StockUniversePitService,
    _fingerprint_sha256,
    require_live_st_pit_universe_key,
    require_qe_immutable_st_pit_universe_key,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT,
    PitAuthorityStatus,
    PitConsumerBinding,
    canonical_rule_parameters_digest,
)


def test_canonical_source_fingerprint_uses_builder_terminal_evidence_contract() -> None:
    source = inspect.getsource(StockUniversePitService.compute_source_fingerprint)

    assert "CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT" in source
    assert "evidence->>'terminal_evidence_contract' = %s" in source
    assert "evidence#>>'{{issuer_binding,schema_version}}' = 'announcement_issuer_binding_v1'" in source
    assert "evidence#>>'{{issuer_binding,status}}' = 'EXACT'" in source
    assert "evidence#>>'{{issuer_binding,actionable}}' = 'true'" in source
    assert "evidence#>>'{{issuer_binding,resolved_ts_code}}' = ts_code" in source
    assert "evidence#>>'{{terminal_cross_check,matched}}'" in source
    assert "evidence#>>'{{terminal_cross_check,terminal}}'" in source
    assert "evidence#>>'{{st_cross_check,matched}}'" in source
    assert "evidence#>>'{{st_cross_check,terminal}}'" in source
    assert "COALESCE" in source
    assert "FROM market.stock_namechange" in source
    assert 'fingerprint["stock_namechange"]' in source
    assert CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT == "issuer_bound_stock_delisting_v2"


QE_SNAPSHOT_KEY = "shsz_st_pit_qe_dataset_test_20180801_20260630_v1"


def test_st_pit_namespace_contract_is_bidirectional() -> None:
    assert require_live_st_pit_universe_key(DEFAULT_ST_PIT_UNIVERSE_KEY) == DEFAULT_ST_PIT_UNIVERSE_KEY
    assert require_qe_immutable_st_pit_universe_key(QE_SNAPSHOT_KEY) == QE_SNAPSHOT_KEY

    with pytest.raises(StockUniversePitError, match="live Selection/Paper/simulation"):
        require_live_st_pit_universe_key(QE_SNAPSHOT_KEY)
    with pytest.raises(StockUniversePitError, match="QE ST PIT must use an immutable"):
        require_qe_immutable_st_pit_universe_key(DEFAULT_ST_PIT_UNIVERSE_KEY)


def test_live_st_pit_service_paths_reject_qe_namespace_before_database_access() -> None:
    service = StockUniversePitService()

    with pytest.raises(StockUniversePitError, match="authoritative rolling universe"):
        service.mark_dirty(reason="test", universe_key=QE_SNAPSHOT_KEY)
    with pytest.raises(StockUniversePitError, match="authoritative rolling universe"):
        service.ensure_st_pit_universe(universe_key=QE_SNAPSHOT_KEY)
    with pytest.raises(StockUniversePitError, match="authoritative rolling universe"):
        service.rebuild_st_pit_universe(universe_key=QE_SNAPSHOT_KEY)
    with pytest.raises(StockUniversePitError, match="authoritative rolling universe"):
        service.get_eligible_codes(
            trade_date=dt.date(2026, 6, 30),
            universe_key=QE_SNAPSHOT_KEY,
            ensure=False,
        )


def test_canonical_query_requires_resolver_binding_before_database_access() -> None:
    service = StockUniversePitService()
    with pytest.raises(StockUniversePitError, match="resolver-issued authority_binding"):
        service.get_eligible_codes(
            trade_date=dt.date(2026, 7, 31),
            universe_key=CANONICAL_PIT_UNIVERSE_KEY,
            ensure=False,
            consumer="selection",
        )


def test_canonical_query_accepts_validated_binding(monkeypatch) -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            assert params[0] == CANONICAL_PIT_UNIVERSE_KEY

        def fetchall(self):
            return [("000001.SZ",), ("600000.SH",)]

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return Cursor()

    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=1,
        activation_envelope_digest="a" * 64,
        coverage_start=dt.date(2018, 8, 1),
        coverage_end=dt.date(2026, 7, 31),
    )
    class Resolver:
        def resolve_live_binding(self):
            return binding

    service = StockUniversePitService(authority_resolver=Resolver())
    monkeypatch.setattr(
        service,
        "ensure_canonical_pit_universe",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("canonical read path must not rebuild")),
    )
    monkeypatch.setattr("backend.services.stock_universe_pit_service.get_conn", lambda: Conn())
    assert service.get_eligible_codes(
        trade_date=dt.date(2026, 7, 31),
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        ensure=True,
        authority_binding=binding,
        consumer="selection",
    ) == ["000001.SZ", "600000.SH"]


def test_canonical_query_rejects_stale_binding_before_span_query() -> None:
    current = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=2,
        activation_envelope_digest="b" * 64,
        coverage_start=dt.date(2018, 8, 1),
        coverage_end=dt.date(2026, 7, 31),
    )

    class Resolver:
        def resolve_live_binding(self):
            return current

    stale = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=1,
        activation_envelope_digest="a" * 64,
        coverage_start=dt.date(2018, 8, 1),
        coverage_end=dt.date(2026, 7, 31),
    )
    with pytest.raises(StockUniversePitError, match="stale relative"):
        StockUniversePitService(authority_resolver=Resolver()).get_eligible_codes(
            trade_date=dt.date(2026, 7, 31),
            universe_key=CANONICAL_PIT_UNIVERSE_KEY,
            ensure=False,
            authority_binding=stale,
            consumer="selection",
        )


def test_canonical_query_rejects_date_outside_live_coverage() -> None:
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=1,
        activation_envelope_digest="a" * 64,
        coverage_start=dt.date(2018, 8, 1),
        coverage_end=dt.date(2026, 7, 31),
    )

    class Resolver:
        def resolve_live_binding(self):
            return binding

    with pytest.raises(StockUniversePitError, match="outside the live authority coverage"):
        StockUniversePitService(authority_resolver=Resolver()).get_eligible_codes(
            trade_date=dt.date(2026, 8, 1),
            universe_key=CANONICAL_PIT_UNIVERSE_KEY,
            ensure=False,
            authority_binding=binding,
            consumer="selection",
        )


def test_canonical_query_rejects_empty_authoritative_pool(monkeypatch) -> None:
    binding = PitConsumerBinding(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=1,
        activation_envelope_digest="a" * 64,
        coverage_start=dt.date(2018, 8, 1),
        coverage_end=dt.date(2026, 7, 31),
    )

    class Resolver:
        def resolve_live_binding(self):
            return binding

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            pass

        def fetchall(self):
            return []

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setattr("backend.services.stock_universe_pit_service.get_conn", lambda: Conn())
    with pytest.raises(StockUniversePitError, match="empty authoritative stock pool"):
        StockUniversePitService(authority_resolver=Resolver()).get_eligible_codes(
            trade_date=dt.date(2026, 7, 31),
            universe_key=CANONICAL_PIT_UNIVERSE_KEY,
            ensure=False,
            authority_binding=binding,
            consumer="selection",
        )


def test_mark_canonical_dirty_uses_v2_identity(monkeypatch) -> None:
    service = StockUniversePitService()
    captured = {}
    monkeypatch.setattr(service, "_mark_dirty", lambda **kwargs: captured.update(kwargs) or kwargs)
    service.mark_canonical_dirty(reason="source_refresh", source_dataset="stock_st_events")
    assert captured["universe_key"] == CANONICAL_PIT_UNIVERSE_KEY
    assert captured["rule_version"] == CANONICAL_PIT_RULE_VERSION
    assert captured["scope"] == CANONICAL_PIT_SCOPE


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
        "_rebuild_st_pit_universe",
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
    monkeypatch.setattr(service, "_rebuild_st_pit_universe", lambda **kwargs: captured.update(kwargs) or {})

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
        "_rebuild_st_pit_universe",
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
        universe_key=DEFAULT_ST_PIT_UNIVERSE_KEY,
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


def test_canonical_ensure_uses_exact_v2_scope_and_source_policy(monkeypatch) -> None:
    service = StockUniversePitService()
    captured: dict[str, object] = {}
    source = {"confirmed_delisting_events": {"row_count": 12}}
    monkeypatch.setattr(service, "ensure_tables", lambda: None)
    monkeypatch.setattr(service, "get_status", lambda **_kwargs: {"status": "missing", "dirty": True})

    def fingerprint(**kwargs):
        assert kwargs["include_canonical_terminal_events"] is True
        return source

    monkeypatch.setattr(service, "compute_source_fingerprint", fingerprint)
    monkeypatch.setattr(
        service,
        "rebuild_canonical_pit_universe",
        lambda **kwargs: captured.update(kwargs) or {"status": "ready", "rebuilt": True},
    )

    result = service.ensure_canonical_pit_universe(
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 7, 31),
    )

    assert result["status"] == "ready"
    assert captured["source_fingerprint"] == source


def test_canonical_monthly_plan_is_readonly_and_reports_exact_rebuild_reason(monkeypatch) -> None:
    service = StockUniversePitService()
    state = {
        "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "scope": CANONICAL_PIT_SCOPE,
        "start_date": dt.date(2018, 8, 1),
        "end_date": dt.date(2026, 7, 31),
        "status": "ready",
        "dirty": False,
        "source_fingerprint_sha256": "old",
        "last_build_summary": {"validation": {}},
    }
    monkeypatch.setattr(
        service,
        "ensure_tables",
        lambda: (_ for _ in ()).throw(AssertionError("plan must not ensure tables")),
    )
    monkeypatch.setattr(service, "get_status_readonly", lambda **_kwargs: state)
    monkeypatch.setattr(
        service,
        "compute_source_fingerprint",
        lambda **kwargs: {
            "fingerprint_end_date": kwargs["end_date"].isoformat(),
            "confirmed_delisting_events": {"row_count": 1},
        },
    )

    result = service.plan_canonical_pit_universe(
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 8, 31),
    )

    assert result["zero_write"] is True
    assert result["needs_rebuild"] is True
    assert result["reason"] == "end_coverage_insufficient"
    assert result["decision"] == "REBUILD_REQUIRED"
    assert result["requested_end_date"] == dt.date(2026, 8, 31)
    assert result["effective_end_date"] == dt.date(2026, 8, 31)


def test_canonical_monthly_plan_rejects_inverted_window_before_database_access() -> None:
    with pytest.raises(StockUniversePitError, match="end_date must be on or after"):
        StockUniversePitService().plan_canonical_pit_universe(
            start_date=dt.date(2026, 8, 31),
            end_date=dt.date(2026, 8, 1),
        )


def test_get_status_readonly_does_not_create_state_table(monkeypatch) -> None:
    executed: list[str] = []

    class Cursor:
        def __init__(self):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, _params=None):
            executed.append(sql)
            self.calls += 1

        def fetchone(self):
            if self.calls == 1:
                return {
                    "state_table": "market.stock_universe_pit_state",
                    "spans_table": "market.stock_universe_pit_spans",
                    "events_table": "market.stock_universe_pit_events",
                }
            return {
                "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
                "status": "ready",
                "dirty": False,
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return Cursor()

    service = StockUniversePitService()
    monkeypatch.setattr(
        service,
        "ensure_tables",
        lambda: (_ for _ in ()).throw(AssertionError("readonly status must not ensure tables")),
    )
    monkeypatch.setattr("backend.services.stock_universe_pit_service.get_conn", lambda: Connection())

    result = service.get_status_readonly(universe_key=CANONICAL_PIT_UNIVERSE_KEY)

    assert result["status"] == "ready"
    assert all("CREATE " not in sql.upper() and "INSERT " not in sql.upper() for sql in executed)


def test_get_status_readonly_reports_missing_schema_without_creating_it(monkeypatch) -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, _params=None):
            return None

        def fetchone(self):
            return {
                "state_table": "market.stock_universe_pit_state",
                "spans_table": None,
                "events_table": "market.stock_universe_pit_events",
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return Cursor()

    monkeypatch.setattr("backend.services.stock_universe_pit_service.get_conn", lambda: Connection())

    result = StockUniversePitService().get_status_readonly(
        universe_key=CANONICAL_PIT_UNIVERSE_KEY
    )

    assert result["status"] == "missing"
    assert result["reason"] == "schema_contract_missing"
    assert result["missing_tables"] == ["spans_table"]


def test_canonical_rebuild_passes_exact_builder_contract(monkeypatch) -> None:
    service = StockUniversePitService()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        service,
        "_rebuild_st_pit_universe",
        lambda **kwargs: captured.update(kwargs) or {"status": "ready"},
    )
    service.rebuild_canonical_pit_universe(
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 7, 31),
    )
    assert captured["universe_key"] == CANONICAL_PIT_UNIVERSE_KEY
    assert captured["rule_version"] == CANONICAL_PIT_RULE_VERSION
    assert captured["scope"] == CANONICAL_PIT_SCOPE
    assert captured["ipo_filter_days"] == 252
    assert captured["ipo_filter_unit"] == "trading_sessions"
    assert captured["include_canonical_terminal_events"] is True
