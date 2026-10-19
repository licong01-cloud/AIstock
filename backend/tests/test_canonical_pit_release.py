from dataclasses import replace
from datetime import date
import json

import pytest

from backend.services import canonical_equity_pit as policy
from backend.services import canonical_pit_release as releases
from backend.services.dataset_release.pit import freeze_pit_snapshot


def version():
    snapshot = freeze_pit_snapshot(
        [("000001.SZ", date(2026, 7, 1), date(2026, 8, 31), "IPO_WARMUP", None)],
        universe_key=policy.CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=policy.CANONICAL_PIT_RULE_VERSION,
        scope_start=date(2026, 7, 1), cutoff=date(2026, 8, 31),
        state_identity="a" * 64, source_fingerprint_sha256="b" * 64,
        parameter_hash=policy.canonical_rule_parameters_digest(),
    )
    return releases.build_release(snapshot, calendar=[date(2026, 7, 1), date(2026, 8, 31)],
                                  source_commit="c" * 40)


def test_pit_versions_need_no_price_release_and_are_create_exclusive(tmp_path):
    first = version()
    path = releases.publish_release(first, tmp_path)
    assert releases.read_release(path) == first
    with pytest.raises(FileExistsError):
        releases.publish_release(first, tmp_path)
    successor = releases.build_release(
        first.snapshot, calendar=first.payload["trading_calendar"], source_commit="d" * 40,
        predecessor_digest=first.digest,
    )
    assert releases.read_release(releases.publish_release(successor, tmp_path)).digest != first.digest
    assert releases.read_release(path).digest == first.digest
    assert successor.payload["predecessor_digest"] == first.digest
    assert len(releases.list_releases(tmp_path)) == 2
    with pytest.raises(ValueError, match="predecessor"):
        releases.publish_release(releases.build_release(first.snapshot,
            calendar=first.payload["trading_calendar"], source_commit="e" * 40,
            predecessor_digest="f" * 64), tmp_path)


@pytest.mark.parametrize("field,value", [
    ("authority_id", "module_private_pit"), ("rule_parameters_digest", "e" * 64),
    ("version_id", "pit_forged"), ("extra", "not allowed"),
])
def test_release_rejects_identity_and_schema_tampering(field, value):
    damaged = {**version().payload, field: value}
    with pytest.raises(ValueError):
        releases.validate_release(damaged)


def test_binding_pairs_one_fact_release_with_independent_pit_versions():
    first = version()
    binding = releases.bind_fact_release(first, dataset_manifest_sha256="f" * 64,
                                       start=date(2026, 7, 1), cutoff=date(2026, 8, 31))
    assert binding["pit_release_digest"] == first.digest
    assert binding["dataset_manifest_sha256"] == "f" * 64
    assert binding["pit_binding"]["frozen_snapshot_digest"] == first.snapshot.spans_sha256
    with pytest.raises(ValueError, match="coverage"):
        releases.bind_fact_release(first, dataset_manifest_sha256="f" * 64,
                                  start=date(2026, 6, 30), cutoff=date(2026, 8, 31))
    with pytest.raises(ValueError):
        releases.build_release(replace(first.snapshot, parameter_hash="e" * 64),
                               calendar=first.payload["trading_calendar"], source_commit="c" * 40)


def test_read_rejects_non_version_filename(tmp_path):
    first = version()
    path = tmp_path / "active.json"
    path.write_text(json.dumps(first.payload), encoding="utf-8")
    with pytest.raises(ValueError, match="filename"):
        releases.read_release(path)


def test_independent_pit_binding_does_not_require_provider_catalog_names():
    live = policy.PitConsumerBinding(
        policy.CANONICAL_PIT_AUTHORITY_ID, policy.PitAuthorityStatus.ACTIVE_CANONICAL,
        policy.CANONICAL_PIT_UNIVERSE_KEY, policy.CANONICAL_PIT_RULE_VERSION,
        policy.canonical_rule_parameters_digest(), activation_generation=1,
        activation_envelope_digest="a" * 64, expected_source_commit="b" * 40,
        state_source_digest="c" * 64, coverage_start=date(2018, 8, 1), coverage_end=date(2026, 9, 30),
    )
    pair = releases.bind_fact_release(version(), dataset_manifest_sha256="f" * 64,
                                     start=date(2026, 7, 1), cutoff=date(2026, 8, 31))
    profile = {"cutoff": "2026-08-31", "pit_binding": pair["pit_binding"], "components": {
        "day_pins": {"universe_key": "physical_quote_catalog"},
        "minute_pins": {"universe_key": "all_real_quotes"},
    }}
    assert policy.require_dataset_runtime_pit_policy(profile, live)["rule_version"] == live.rule_version
    profile["pit_binding"]["rule_parameters_digest"] = "e" * 64
    with pytest.raises(policy.CanonicalPitContractError, match="independent PIT"):
        policy.require_dataset_runtime_pit_policy(profile, live)


def test_rehearsal_rolls_back_source_drift_before_any_pointer_write(monkeypatch):
    class Connection:
        rolled_back = False
        def rollback(self):
            self.rolled_back = True
    conn = Connection()
    monkeypatch.setattr(releases.CanonicalPitAuthorityResolver, "resolve_live_binding", lambda _: object())
    changed = releases.build_release(version().snapshot, calendar=version().payload["trading_calendar"],
                                     source_commit="e" * 40)
    monkeypatch.setattr(releases, "capture_release", lambda *args, **kwargs: changed)
    monkeypatch.setattr(releases, "activate_canonical_pit_authority",
                        lambda *args, **kwargs: pytest.fail("source drift must precede pointer DML"))
    with pytest.raises(ValueError, match="source drifted"):
        releases.rehearse_activation(conn, version())
    assert conn.rolled_back


def test_wrong_calendar_and_unregistered_predecessors_fail_without_publication(tmp_path):
    with pytest.raises(ValueError, match="calendar"):
        releases.build_release(version().snapshot, calendar=[date(2026, 9, 1)], source_commit="c" * 40)
    assert list(tmp_path.iterdir()) == []


def test_new_shared_runtime_defaults_use_pointer_without_rewriting_saved_versions():
    from backend.services.selection_center.runtime_profile import attach_default_runtime_profile_binding
    from backend.services.selection_center.canonical_pit_runtime import CANONICAL_PIT_RUNTIME_PROFILE_KEY
    original = {"runtime_profile": {"risk_policy": {"st_universe_key": "shsz_st_pit_active_v1"}}}
    fresh = attach_default_runtime_profile_binding(original)
    assert fresh[CANONICAL_PIT_RUNTIME_PROFILE_KEY]["authority_id"] == policy.CANONICAL_PIT_AUTHORITY_ID
    assert "st_universe_key" not in fresh["runtime_profile"]["risk_policy"]
    assert original["runtime_profile"]["risk_policy"]["st_universe_key"] == "shsz_st_pit_active_v1"
    saved = {**original, "runtime_profile_activation": {"source": "runtime_config_activation",
             "profile_version_id": "saved-version", "config_sha256": "f" * 64, "trade_enabled": True}}
    assert attach_default_runtime_profile_binding(saved) == saved


def test_new_saved_profile_versions_inherit_global_pointer():
    from backend.tests.paper_trading_v2.test_runtime_profile import _portfolio_fixture
    from backend.services.selection_center.canonical_pit_runtime import CANONICAL_PIT_RUNTIME_PROFILE_KEY
    _, _, service, _, portfolio = _portfolio_fixture()
    profile, first = service.create_runtime_profile(portfolio_id=portfolio.portfolio_id,
        profile_name="new global default", config_json={"top_k": 2})
    second = service.create_runtime_profile_version(portfolio_id=portfolio.portfolio_id,
        profile_id=profile.profile_id, config_json={"top_k": 1})
    for version in (first, second):
        assert version.config_json[CANONICAL_PIT_RUNTIME_PROFILE_KEY]["authority_id"] == policy.CANONICAL_PIT_AUTHORITY_ID
        assert "st_universe_key" not in version.config_json["runtime_profile"]["risk_policy"]
