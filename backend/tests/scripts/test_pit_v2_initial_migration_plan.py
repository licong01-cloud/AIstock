from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from backend.services.dataset_release.errors import ProfileValidationError
from backend.services.dataset_release.profile import (
    CANONICAL_INITIAL_MIGRATION_PLAN_ID,
    load_dataset_profile,
    load_initial_migration_plan,
)


ROOT = Path(__file__).resolve().parents[3]
PROFILE_PATH = ROOT / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
PLAN_PATH = ROOT / "configs" / "datasets" / "migrations" / "pit_v2_initial_20260731_v1.yaml"


def test_initial_migration_plan_is_fixed_allowlisted_and_digest_bound() -> None:
    profile = load_dataset_profile(PROFILE_PATH)
    first = load_initial_migration_plan(PLAN_PATH)
    second = load_initial_migration_plan(PLAN_PATH)

    assert profile.initial_migration_plan_ids == (CANONICAL_INITIAL_MIGRATION_PLAN_ID,)
    assert first.plan_id in profile.initial_migration_plan_ids
    assert first.cutoff.isoformat() == "2026-07-31"
    assert first.allowed_scopes == ("sample", "full")
    assert first.sample_instruments == (
        "000001.SZ",
        "300379.SZ",
        "600462.SH",
        "600930.SH",
        "688981.SH",
    )
    assert len(first.event_windows) == 10
    assert len(first.index_windows) == 2
    assert first.plan_digest == second.plan_digest
    assert len(first.plan_digest) == 64


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("cutoff", "2026-08-31", "cutoff must remain"),
        ("allowed_scopes", ["sample"], "scopes differ"),
        ("source_identity_policy", "trust_previous_receipt", "source identity policy differs"),
    ],
)
def test_initial_migration_plan_drift_fails_closed(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    payload = yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))
    payload[field] = value
    target = tmp_path / PLAN_PATH.name
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ProfileValidationError, match=message):
        load_initial_migration_plan(target)


def test_initial_migration_event_window_cannot_change_under_same_plan_id(tmp_path: Path) -> None:
    payload = yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))
    payload["event_windows"][0]["start"] = "2026-07-28"
    target = tmp_path / PLAN_PATH.name
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ProfileValidationError, match="event windows differ"):
        load_initial_migration_plan(target)
