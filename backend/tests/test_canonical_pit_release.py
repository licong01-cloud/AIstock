from copy import deepcopy
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
    return releases.build_release(
        snapshot, calendar=[date(2026, 7, 1), date(2026, 8, 31)], source_commit="c" * 40,
    )


def test_independent_versions_do_not_export_or_activate_prices(tmp_path):
    first = version()
    path = releases.publish_release(first, tmp_path)
    original = path.read_bytes()
    assert releases.read_release(path) == first
    with pytest.raises(FileExistsError):
        releases.publish_release(first, tmp_path)
    successor = releases.build_release(
        first.snapshot, calendar=first.payload["trading_calendar"], source_commit="d" * 40,
        predecessor_digest=first.digest,
    )
    releases.publish_release(successor, tmp_path)
    assert len(releases.list_releases(tmp_path)) == 2
    assert path.read_bytes() == original
    assert not (tmp_path / "active.json").exists()
    binding = releases.bind_fact_release(first, dataset_manifest_sha256="f" * 64)
    assert binding["pit_binding"]["rule_version"] == "shsz_a_252td_st_delist_asof_v2"
    assert binding["pit_binding"]["frozen_snapshot_digest"] == first.snapshot.spans_sha256
    assert policy.CANONICAL_PIT_UNIVERSE_KEY == "aistock_equity_pit_canonical_v2"
    with pytest.raises(ValueError, match="predecessor"):
        releases.publish_release(releases.build_release(
            first.snapshot, calendar=first.payload["trading_calendar"], source_commit="e" * 40,
            predecessor_digest="f" * 64,
        ), tmp_path)


@pytest.mark.parametrize("field,value", [
    ("authority_id", "private_pit"), ("rule_parameters_digest", "e" * 64),
    ("version_id", "pit_forged"), ("calendar_sha256", "e" * 64),
    ("trading_calendar", ["2026-08-31", "2026-07-01"]),
    ("trading_calendar", ["2026-07-01", "2026-07-01"]),
    ("trading_calendar", ["2026-06-30"]), ("source_commit", "short"),
    ("extra", True),
])
def test_release_rejects_tampering(field, value):
    damaged = {**version().payload, field: value}
    with pytest.raises(ValueError):
        releases.validate_release(damaged)


def test_snapshot_parameter_and_file_identity_remain_strict(tmp_path):
    damaged = deepcopy(version().payload)
    damaged["snapshot"]["parameter_hash"] = "e" * 64
    with pytest.raises(ValueError):
        releases.validate_release(damaged)
    path = tmp_path / "active.json"
    path.write_text(json.dumps(version().payload), encoding="utf-8")
    with pytest.raises(ValueError, match="filename"):
        releases.read_release(path)
    path.write_text('{"schema_version":1,"schema_version":2}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        releases.read_release(path)


def test_atomic_publish_failure_does_not_leave_registered_partial_version(tmp_path, monkeypatch):
    def failed(*args, **kwargs):
        raise OSError("link failed")
    monkeypatch.setattr(releases.os, "link", failed)
    with pytest.raises(OSError, match="link failed"):
        releases.publish_release(version(), tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_capture_requires_read_only_consistent_connection_before_queries():
    class UnsafeConnection:
        readonly = False
        isolation_level = 2
        def cursor(self, **kwargs):
            pytest.fail("unsafe connection must never query")
    with pytest.raises(ValueError, match="read-only"):
        releases.capture_release(
            UnsafeConnection(), start=date(2026, 7, 1), cutoff=date(2026, 8, 31),
            source_commit="c" * 40,
        )


def test_offline_cli_can_verify_prior_version_without_live_source_or_default_change(tmp_path, monkeypatch, capsys):
    from scripts import manage_canonical_pit_release as cli
    path = releases.publish_release(version(), tmp_path)
    monkeypatch.setattr(cli, "_load_database_config", lambda *args: pytest.fail("offline verify accessed DB"))
    assert cli.main(["--action", "verify", "--release", str(path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "FILE_VERIFIED"
    assert result["database_read"] is False
    assert result["source_freshness_checked"] is False
    assert cli.main(["--action", "list", "--directory", str(tmp_path)]) == 0
    assert len(json.loads(capsys.readouterr().out)["versions"]) == 1


@pytest.mark.parametrize("mutation", ["none", "digest", "cutoff", "scope", "legacy_without_binding", "legacy_null_binding"])
def test_independent_binding_compares_policy_not_physical_quote_catalog(mutation):
    live = policy.PitConsumerBinding(
        policy.CANONICAL_PIT_AUTHORITY_ID, policy.PitAuthorityStatus.ACTIVE_CANONICAL,
        policy.CANONICAL_PIT_UNIVERSE_KEY, policy.CANONICAL_PIT_RULE_VERSION,
        policy.canonical_rule_parameters_digest(), activation_generation=1,
        activation_envelope_digest="a" * 64, expected_source_commit="b" * 40,
        state_source_digest="c" * 64, coverage_start=date(2018, 8, 1), coverage_end=date(2026, 9, 30),
    )
    pair = releases.bind_fact_release(version(), dataset_manifest_sha256="f" * 64)
    profile = {"cutoff": "2026-08-31", "pit_binding": pair["pit_binding"], "components": {
        "day_pins": {"universe_key": "physical_quote_catalog"},
        "minute_pins": {"universe_key": "all_real_quotes"},
    }}
    if mutation == "digest":
        profile["pit_binding"]["rule_parameters_digest"] = "e" * 64
    elif mutation == "cutoff":
        profile["cutoff"] = "2026-09-01"
    elif mutation == "scope":
        profile["pit_binding"]["scope"] = "sample"
    elif mutation == "legacy_without_binding":
        del profile["pit_binding"]
    elif mutation == "legacy_null_binding":
        profile["pit_binding"] = None
        profile["components"] = {name: {"universe_key": live.universe_key, "rule_version": live.rule_version}
                                 for name in ("day_pins", "minute_pins")}
    if mutation in {"none", "legacy_null_binding"}:
        assert policy.require_dataset_runtime_pit_policy(profile, live)["rule_version"] == live.rule_version
    else:
        with pytest.raises(policy.CanonicalPitContractError):
            policy.require_dataset_runtime_pit_policy(profile, live)
