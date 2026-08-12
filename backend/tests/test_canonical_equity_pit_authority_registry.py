from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

import backend.services.canonical_equity_pit as pit


def _active_row(**overrides):
    values = {
        "authority_id": pit.CANONICAL_PIT_AUTHORITY_ID,
        "authority_status": pit.PitAuthorityStatus.ACTIVE_CANONICAL.value,
        "universe_key": pit.CANONICAL_PIT_UNIVERSE_KEY,
        "rule_version": pit.CANONICAL_PIT_RULE_VERSION,
        "rule_parameters_digest": pit.canonical_rule_parameters_digest(),
        "activation_generation": 1,
        "activation_envelope_digest": "a" * 64,
        "expected_source_commit": "deadbeef",
        "state_source_digest": "b" * 64,
        "state_status": "ready",
        "dirty": False,
        "state_rule_version": pit.CANONICAL_PIT_RULE_VERSION,
        "scope": pit.CANONICAL_PIT_SCOPE,
        "start_date": dt.date(2018, 8, 1),
        "end_date": dt.date(2026, 7, 31),
        "has_current_rule_span": True,
        "all_spans_current_rule": True,
        "last_build_summary": {
            "rule_parameters_digest": pit.canonical_rule_parameters_digest(),
            "exception_ledger_status": "ready",
            "exception_ledger": {"status": "ready", "unresolved_exception_count": 0},
            "st_snapshot_continuity": {"status": "ready"},
            "terminal_evidence": {"status": "ready", "missing_terminal_evidence_count": 0},
            "validation": {
                "invalid_span_count": 0,
                "overlap_error_count": 0,
                "event_action_violation_count": 0,
                "terminal_reentry_violation_count": 0,
            },
        },
    }
    values.update(overrides)
    return values


def _target(**overrides):
    values = {
        "authority_id": pit.CANONICAL_PIT_AUTHORITY_ID,
        "target_rule_version": pit.CANONICAL_PIT_RULE_VERSION,
        "target_rolling_key": pit.CANONICAL_PIT_UNIVERSE_KEY,
        "rule_parameters_digest": pit.canonical_rule_parameters_digest(),
        "candidate_bundle_digest": "c" * 64,
        "activation_envelope_digest": "a" * 64,
        "expected_source_commit": "deadbeef",
        "expected_previous_generation": 0,
        "expected_previous_key": pit.LEGACY_PIT_UNIVERSE_KEY,
    }
    values.update(overrides)
    return pit.PitActivationTarget(**values)


def test_resolver_accepts_only_complete_active_canonical_state() -> None:
    binding = pit.CanonicalPitAuthorityResolver._binding_from_row(_active_row())
    assert binding.authority_status is pit.PitAuthorityStatus.ACTIVE_CANONICAL
    assert binding.activation_generation == 1
    with pytest.raises(pit.CanonicalPitContractError, match="ready and clean"):
        pit.CanonicalPitAuthorityResolver._binding_from_row(_active_row(dirty=True))
    with pytest.raises(pit.CanonicalPitContractError, match="non-current rule"):
        pit.CanonicalPitAuthorityResolver._binding_from_row(_active_row(all_spans_current_rule=False))
    with pytest.raises(pit.CanonicalPitContractError, match="exception_ledger_status"):
        pit.CanonicalPitAuthorityResolver._binding_from_row(
            _active_row(
                last_build_summary={
                    "rule_parameters_digest": pit.canonical_rule_parameters_digest(),
                    "exception_ledger_status": "blocked",
                }
            )
        )
    incomplete = _active_row()
    incomplete["last_build_summary"] = dict(incomplete["last_build_summary"])
    incomplete["last_build_summary"]["validation"] = {}
    with pytest.raises(pit.CanonicalPitContractError, match="validation receipt contains failures"):
        pit.CanonicalPitAuthorityResolver._binding_from_row(incomplete)


def test_activation_target_rejects_identity_or_digest_drift() -> None:
    with pytest.raises(pit.CanonicalPitContractError, match="target_rule_version"):
        pit._validate_activation_target_shape(_target(target_rule_version="other"))
    with pytest.raises(pit.CanonicalPitContractError, match="candidate_bundle_digest"):
        pit._validate_activation_target_shape(_target(candidate_bundle_digest="not-a-digest"))


def test_activation_default_connection_is_managed_transaction(monkeypatch) -> None:
    captured = {}
    sentinel = object()
    monkeypatch.setattr(pit, "get_conn", lambda **kwargs: captured.update(kwargs) or sentinel)
    assert pit._transactional_connection() is sentinel
    assert captured == {"autocommit": False, "manage_transaction": True}


class _ActivationCursor:
    def __init__(self, rowcounts, target_state=None):
        self.rowcounts = iter(rowcounts)
        self.rowcount = -1
        self._fetch_value = None
        self.target_state = target_state or {
            "universe_key": pit.CANONICAL_PIT_UNIVERSE_KEY,
            "rule_version": pit.CANONICAL_PIT_RULE_VERSION,
            "scope": pit.CANONICAL_PIT_SCOPE,
            "status": "ready",
            "dirty": False,
            "source_fingerprint_sha256": "b" * 64,
            "has_current_rule_span": True,
            "all_spans_current_rule": True,
            "last_build_summary": _active_row()["last_build_summary"],
        }

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        if "stock_universe_pit_authority_pointer" in sql and "FOR UPDATE" in sql:
            self._fetch_value = {
                "current_rule_version": pit.LEGACY_PIT_RULE_VERSION,
                "current_rolling_key": pit.LEGACY_PIT_UNIVERSE_KEY,
                "activation_generation": 0,
            }
            self.rowcount = 1
        elif "FROM market.stock_universe_pit_state s" in sql:
            self._fetch_value = self.target_state
            self.rowcount = 1
        elif "pg_advisory_xact_lock" in sql:
            self.rowcount = 1
        else:
            self.rowcount = next(self.rowcounts)

    def fetchone(self):
        value = self._fetch_value
        self._fetch_value = None
        return value


class _ActivationConnection:
    def __init__(self, rowcounts, target_state=None):
        self.cursor_value = _ActivationCursor(rowcounts, target_state=target_state)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self, cursor_factory=None):
        return self.cursor_value


def test_activation_is_unit_row_count_cas_and_returns_final_readback(monkeypatch) -> None:
    final_binding = pit.PitConsumerBinding(
        authority_id=pit.CANONICAL_PIT_AUTHORITY_ID,
        authority_status=pit.PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=pit.CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=pit.CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=pit.canonical_rule_parameters_digest(),
        activation_generation=1,
    )
    monkeypatch.setattr(pit.CanonicalPitAuthorityResolver, "require_activation_target", lambda self, target: target)
    monkeypatch.setattr(pit.CanonicalPitAuthorityResolver, "resolve_live_binding", lambda self: final_binding)
    result = pit.activate_canonical_pit_authority(
        _target(),
        operator_intent="authorized activation",
        independent_receipt_digest="d" * 64,
        connection_factory=lambda: _ActivationConnection([1, 1, 1, 1]),
    )
    assert result == final_binding

    with pytest.raises(pit.CanonicalPitContractError, match="pointer CAS"):
        pit.activate_canonical_pit_authority(
            _target(),
            operator_intent="authorized activation",
            independent_receipt_digest="d" * 64,
            connection_factory=lambda: _ActivationConnection([1, 1, 0]),
        )


def test_activation_revalidates_locked_target_state_after_preflight(monkeypatch) -> None:
    monkeypatch.setattr(pit.CanonicalPitAuthorityResolver, "require_activation_target", lambda self, target: target)
    drifted = {
        "universe_key": pit.CANONICAL_PIT_UNIVERSE_KEY,
        "rule_version": pit.CANONICAL_PIT_RULE_VERSION,
        "scope": pit.CANONICAL_PIT_SCOPE,
        "status": "building",
        "dirty": True,
        "has_current_rule_span": True,
        "all_spans_current_rule": True,
        "last_build_summary": _active_row()["last_build_summary"],
    }
    with pytest.raises(pit.CanonicalPitContractError, match="ready/clean"):
        pit.activate_canonical_pit_authority(
            _target(),
            operator_intent="authorized activation",
            independent_receipt_digest="d" * 64,
            connection_factory=lambda: _ActivationConnection([], target_state=drifted),
        )


def test_registry_migration_installs_legacy_pointer_without_v2_activation() -> None:
    migration = (
        Path(__file__).parents[1]
        / "migrations"
        / "stock_universe_pit_authority_registry_20260813.sql"
    ).read_text(encoding="utf-8")
    assert "DEPLOYED_LEGACY_PENDING_MIGRATION" in migration
    assert "activation_generation = 0" in migration
    assert "PIT_AUTHORITY_FORWARD_MUST_NOT_ACTIVATE_V2" in migration
    assert "WHERE status = 'ACTIVE_CANONICAL'" in migration
