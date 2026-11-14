"""Rolling PIT compatibility preserves the original native publication binding."""
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import date
import json
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first import lstm_spo_shadow_source_v1 as source
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY,
    CanonicalPitAuthorityResolver, CanonicalPitContractError, PitAuthorityStatus,
    PitConsumerBinding, canonical_rule_parameters_digest,
)
from backend.services.data_integrity.contracts import digest
from backend.services.data_integrity.inputs import InputReadinessError


@pytest.fixture
def native(monkeypatch):
    published = PitConsumerBinding(authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL, universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION, rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=1, activation_envelope_digest="a" * 64, expected_source_commit="source-commit",
        state_source_digest="b" * 64, coverage_start=date(2018, 8, 1), coverage_end=date(2026, 11, 12))
    pit = json.loads(json.dumps(asdict(published), default=str))
    pkg, manifest = source.TARGET["package_id"], source.TARGET["manifest_sha256"]
    original = dict(binding_id=pkg, manifest_sha256=manifest, global_pit=pit)
    original["binding_sha256"] = digest(original)
    n = SimpleNamespace(published=published, current=published, proof=True, calls=[], original=original,
        repo=SimpleNamespace(get_program=lambda *_: SimpleNamespace(status="ENABLED", package_ids=[pkg]),
            get_active_binding_version=lambda *_: SimpleNamespace(binding_version_id=source.BINDING)))

    def resolve(_self):
        if isinstance(n.current, Exception):
            raise n.current
        return n.current

    def verify(_self, **kwargs):
        n.calls.append(kwargs)
        if isinstance(n.proof, Exception):
            raise n.proof
        return n.proof

    monkeypatch.setattr(CanonicalPitAuthorityResolver, "resolve_live_binding", resolve)
    monkeypatch.setattr(CanonicalPitAuthorityResolver, "verifies_rolling_predecessor", verify)

    class Connection:
        @contextmanager
        def cursor(self):
            yield self

        def execute(self, sql, args):
            assert sql.startswith("SELECT manifest_sha256,package_status") and args == (pkg,)

        def fetchone(self):
            return manifest, "READY"

    n.connection = Connection()
    return n


def check(n):
    before = deepcopy(n.original)
    try:
        source.verify_native_liveness(n.connection, n.repo, {source.TARGET["package_id"]: n.original})
    finally:
        assert n.original == before


def test_exact_pit_reuses_identity_without_predecessor_lookup(native):
    check(native)
    assert not native.calls


@pytest.mark.parametrize("digest_changed", [False, True])
def test_verified_roll_accepts_only_the_original_prefix_without_rebinding(native, digest_changed):
    native.current = replace(native.published, coverage_end=date(2026, 11, 13),
        state_source_digest="c" * 64 if digest_changed else native.published.state_source_digest)
    check(native)
    assert native.calls == [dict(current=native.current, source_digest="b" * 64,
        coverage_start=date(2018, 8, 1), coverage_end=date(2026, 11, 12))]
    assert native.original["global_pit"]["coverage_end"] == "2026-11-12"


@pytest.mark.parametrize("proof", [False, None, 1, {"verified": True}, RuntimeError("unavailable")])
def test_unproven_or_unavailable_prefix_remains_waiting(native, proof):
    native.current = replace(native.published, coverage_end=date(2026, 11, 13))
    native.proof = proof
    with pytest.raises(InputReadinessError) as error:
        check(native)
    assert error.value.error_code == "ADVISORY_INPUT_NOT_READY"
    if isinstance(proof, Exception):
        assert error.value.__cause__ is proof


@pytest.mark.parametrize("field,value", [
    ("authority_id", "other"), ("authority_status", PitAuthorityStatus.EMERGENCY_LEGACY_ROLLBACK),
    ("universe_key", "other"), ("rule_version", "other"), ("rule_parameters_digest", "d" * 64),
    ("activation_generation", 2), ("activation_envelope_digest", "d" * 64),
    ("expected_source_commit", "other"), ("coverage_start", date(2018, 8, 2)),
    ("snapshot_digest", "d" * 64), ("cutoff", date(2026, 11, 12)),
    ("release_id", "other"), ("reproduction_mode", True),
])
def test_verified_proof_cannot_override_policy_or_other_identity_changes(native, field, value):
    native.current = replace(native.published, coverage_end=date(2026, 11, 13), **{field: value})
    with pytest.raises(InputReadinessError):
        check(native)
    assert not native.calls


@pytest.mark.parametrize("end", [date(2026, 11, 12), date(2026, 11, 11)])
def test_same_window_restatement_or_coverage_shrink_is_not_a_roll(native, end):
    native.current = replace(native.published, coverage_end=end, state_source_digest="c" * 64)
    with pytest.raises(InputReadinessError):
        check(native)
    assert not native.calls


@pytest.mark.parametrize("change", ["unknown", "missing", "bad_date", "bad_digest", "bad_generation"])
def test_malformed_published_identity_is_not_repaired_or_rebound(native, change):
    pit = native.original["global_pit"]
    if change == "unknown":
        pit["unknown"] = "value"
    elif change == "missing":
        del pit["coverage_end"]
    elif change == "bad_date":
        pit["coverage_end"] = "invalid"
    elif change == "bad_digest":
        pit["state_source_digest"] = "not-a-sha"
    else:
        pit["activation_generation"] = True
    native.original["binding_sha256"] = digest({k: v for k, v in native.original.items() if k != "binding_sha256"})
    native.current = replace(native.published, coverage_end=date(2026, 11, 13))
    with pytest.raises(InputReadinessError):
        check(native)
    assert not native.calls


def test_unavailable_authority_preserves_waiting_contract(native):
    native.current = CanonicalPitContractError("unavailable")
    with pytest.raises(InputReadinessError):
        check(native)
    assert not native.calls
