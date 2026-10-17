from dataclasses import asdict

import pytest

from backend.services.position_timing import service as service_module
from backend.services.position_timing.contracts import canonical_sha256
from backend.services.position_timing.policy import (
    EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
    EXIT_GUARD_SNAPSHOT_ENVELOPE_V1,
    POLICY_SOURCE_REPOSITORY_COMMIT,
    PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
    PRICE_GUARD_SNAPSHOT_ENVELOPE_V1,
    assert_shared_guard_defaults_unmodified,
    frozen_exit_guard_policy,
    frozen_price_guard_policy,
)
from backend.services.trading_core.exit_guard import ExitGuardPolicy
from backend.services.trading_core.price_guard import PriceGuardPolicy


def test_frozen_guard_snapshots_match_audited_shared_defaults() -> None:
    assert_shared_guard_defaults_unmodified()
    current_price = asdict(PriceGuardPolicy())
    current_exit = asdict(ExitGuardPolicy())
    current_price.pop("policy_sha256")
    current_exit.pop("policy_sha256")
    assert current_price == PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1
    assert current_exit == EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1
    assert PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1["buy"]["max_chase_bps"] == 100.0
    assert EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1["stop_loss"]["max_loss_bps"] == 600.0
    assert frozen_price_guard_policy().policy_sha256 == PRICE_GUARD_SNAPSHOT_ENVELOPE_V1["provenance"][
        "timing_policy_sha256"
    ]
    assert frozen_exit_guard_policy().policy_sha256 == EXIT_GUARD_SNAPSHOT_ENVELOPE_V1["provenance"][
        "timing_policy_sha256"
    ]


def test_snapshot_provenance_is_hash_bound_to_fixed_source_commit() -> None:
    for envelope in (PRICE_GUARD_SNAPSHOT_ENVELOPE_V1, EXIT_GUARD_SNAPSHOT_ENVELOPE_V1):
        provenance = envelope["provenance"]
        assert provenance["source_repository_commit"] == POLICY_SOURCE_REPOSITORY_COMMIT
        assert provenance["source_defaults_sha256"] == canonical_sha256(envelope["policy"])
        assert len(provenance["timing_policy_sha256"]) == 64


def test_source_commit_resolution_prefers_explicit_process_identity(monkeypatch) -> None:
    expected = "b" * 40
    monkeypatch.setenv("AISTOCK_GIT_COMMIT", expected)

    def fail_if_git_is_called(*args, **kwargs):  # pragma: no cover - assertion helper
        raise AssertionError("git must not be called when explicit process identity is valid")

    monkeypatch.setattr(service_module.subprocess, "run", fail_if_git_is_called)
    assert service_module._resolve_source_commit() == expected


def test_source_commit_provider_does_not_reread_mutable_checkout(monkeypatch) -> None:
    captured = service_module._source_commit()
    monkeypatch.delenv("AISTOCK_GIT_COMMIT", raising=False)

    def fail_if_git_is_called(*args, **kwargs):  # pragma: no cover - assertion helper
        raise AssertionError("running process must not reread mutable checkout HEAD")

    monkeypatch.setattr(service_module.subprocess, "run", fail_if_git_is_called)
    assert service_module._source_commit() == captured


def test_source_commit_provider_fails_closed_when_import_capture_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(service_module, "_PROCESS_SOURCE_REPOSITORY_COMMIT", None)
    monkeypatch.setattr(service_module, "_PROCESS_SOURCE_REPOSITORY_COMMIT_ERROR", "CalledProcessError")

    with pytest.raises(RuntimeError, match="unavailable when process code loaded: CalledProcessError"):
        service_module._source_commit()
