from dataclasses import asdict

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
