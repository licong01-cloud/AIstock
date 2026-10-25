from __future__ import annotations

from datetime import date

import pytest

from scripts import activate_canonical_pit_default as op


@pytest.fixture
def legacy():
    return op.PitConsumerBinding(
        authority_id=op.CANONICAL_PIT_AUTHORITY_ID,
        authority_status=op.PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION,
        universe_key=op.LEGACY_PIT_UNIVERSE_KEY,
        rule_version=op.LEGACY_PIT_RULE_VERSION,
        rule_parameters_digest=op.legacy_rule_parameters_digest(), activation_generation=0,
    )


def test_initial_activation_uses_real_source_and_exact_previous_pointer():
    live = op.PitConsumerBinding(
        authority_id=op.CANONICAL_PIT_AUTHORITY_ID,
        authority_status=op.PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION,
        universe_key=op.LEGACY_PIT_UNIVERSE_KEY,
        rule_version=op.LEGACY_PIT_RULE_VERSION,
        rule_parameters_digest=op.legacy_rule_parameters_digest(),
        activation_generation=0,
    )
    target, envelope = op.activation_target(
        live, bundle_digest="b" * 64, source_commit="a" * 40,
        expected_generation=0, expected_key=op.LEGACY_PIT_UNIVERSE_KEY,
        authorization_ref="user-explicit-global-pit-activation",
    )
    assert target.candidate_bundle_digest == "b" * 64
    assert target.activation_envelope_digest == op._digest(envelope)
    assert target.target_rule_version == op.CANONICAL_PIT_RULE_VERSION
    assert envelope["dataset_profile_activation"] is False
    with pytest.raises(ValueError, match="previous pointer"):
        op.activation_target(live, bundle_digest="b" * 64, source_commit="a" * 40,
                             expected_generation=1, expected_key=op.LEGACY_PIT_UNIVERSE_KEY,
                             authorization_ref="user-test")


def test_dev_receipt_is_digest_bound_to_actual_rehearsal():
    receipt = op.seal({
        "schema_version": op.SCHEMA, "status": "PASS", "mode": "rehearse",
        "database_target": "dev", "operator_contract_digest": op.CONTRACT_DIGEST,
        "source_commit": "a" * 40, "cutoff": "2026-10-19",
        "rule_parameters_digest": op.canonical_rule_parameters_digest(),
        "transaction_dml_rolled_back": True, "pointer_restored": True,
        "activated_binding": {"activation_generation": 1, "authority_status": "ACTIVE_CANONICAL"},
    })
    assert op.validate_dev_receipt(receipt, cutoff=date(2026, 10, 19), source_commit="a" * 40)
    for key, value in (("status", "BLOCKED"), ("pointer_restored", False),
                       ("cutoff", "2026-09-30"), ("source_commit", "c" * 40),
                       ("mode", "plan")):
        with pytest.raises(ValueError, match="DEV"):
            op.validate_dev_receipt(op.seal({**receipt, key: value}),
                                    cutoff=date(2026, 10, 19), source_commit="a" * 40)
    with pytest.raises(ValueError, match="DEV"):
        op.validate_dev_receipt({**receipt, "pointer_restored": False},
                                cutoff=date(2026, 10, 19), source_commit="a" * 40)


def test_nonzero_sql_errors_cannot_be_reported_as_ready():
    good = {"span_count": 2, "symbol_count": 2, "invalid_count": 0,
            "overlap_count": 0, "wrong_rule_count": 0}
    op.require_sql_audit(good)
    for field in ("invalid_count", "overlap_count", "wrong_rule_count"):
        with pytest.raises(ValueError, match="SQL"):
            op.require_sql_audit({**good, field: 1})
    with pytest.raises(ValueError, match="SQL"):
        op.require_sql_audit({**good, "span_count": 0})


def test_readonly_modes_cannot_apply_and_production_requires_dev():
    for target, mode in (("production", "rehearse"), ("dev", "apply")):
        with pytest.raises(ValueError):
            op.require_mode(target, mode, authorization_ref=None, dev_receipt=None)
    for auth, receipt in ((None, None), ("user-authorization", None)):
        with pytest.raises(ValueError):
            op.require_mode("production", "apply", authorization_ref=auth, dev_receipt=receipt)


class Connection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.queries = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, args):
        self.queries.append(sql)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.mark.parametrize("mode", ["rehearse", "apply", "plan"])
def test_transaction_boundary_and_rollback(monkeypatch, legacy, mode):
    from dataclasses import replace
    conn = Connection()
    monkeypatch.setattr(op.CanonicalPitAuthorityResolver, "resolve_live_binding", lambda self: legacy)
    monkeypatch.setattr(op.CanonicalPitAuthorityResolver, "require_activation_target", lambda self, target: target)
    monkeypatch.setattr(op, "capture", lambda *args: ({"snapshot": "actual-test-readback"}, {"span_count": 2}))
    monkeypatch.setattr(op, "activate_canonical_pit_authority", lambda *args, **kwargs:
                        replace(legacy, activation_generation=1, authority_status=op.PitAuthorityStatus.ACTIVE_CANONICAL))
    receipt = op.execute(conn, mode=mode, cutoff=date(2026, 10, 19), source_commit="a" * 40,
        database_identity="b" * 64, expected_generation=0, expected_key=op.LEGACY_PIT_UNIVERSE_KEY,
        authorization_ref="user-test-authorization", dev_receipt_digest="c" * 64)
    assert receipt["status"] == "PASS"
    assert conn.commits == int(mode == "apply")
    assert conn.rollbacks == int(mode == "rehearse")
    if mode == "plan":
        assert conn.queries == []
    else:
        assert "FOR UPDATE" in conn.queries[0]
        assert "pg_advisory_xact_lock" in conn.queries[1]


def test_failed_shared_cas_rolls_back_without_commit(monkeypatch, legacy):
    conn = Connection()
    monkeypatch.setattr(op.CanonicalPitAuthorityResolver, "resolve_live_binding", lambda self: legacy)
    monkeypatch.setattr(op.CanonicalPitAuthorityResolver, "require_activation_target", lambda self, target: target)
    monkeypatch.setattr(op, "capture", lambda *args: ({"snapshot": "actual-test-readback"}, {}))

    def fail(*args, **kwargs):
        raise ValueError("CAS conflict")

    monkeypatch.setattr(op, "activate_canonical_pit_authority", fail)
    with pytest.raises(ValueError, match="CAS conflict"):
        op.execute(conn, mode="apply", cutoff=date(2026, 10, 19), source_commit="a" * 40,
            database_identity="b" * 64, expected_generation=0, expected_key=op.LEGACY_PIT_UNIVERSE_KEY,
            authorization_ref="user-test", dev_receipt_digest="c" * 64)
    assert conn.commits == 0
    assert conn.rollbacks == 1
