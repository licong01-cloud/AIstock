from datetime import date
from types import SimpleNamespace

import pytest

from scripts import seed_dataset_refresh_audit as seed


def test_monthly_plan_bounds_all_queries_and_keeps_real_empty_day_blocked(monkeypatch):
    start, end = date(2026, 9, 1), date(2026, 9, 30)
    days = (start, end)
    calls = []
    profile = SimpleNamespace(start_date=date(2018, 8, 1), minute_start_date=date(2018, 8, 1))
    monkeypatch.setattr(seed, "_expected_dates", lambda _, lo, hi: calls.append((lo, hi)) or days)
    monkeypatch.setattr(seed, "_existing_ready_dates", lambda *_: frozenset())
    monkeypatch.setattr(seed, "_physical_counts", lambda *args, **kwargs: {start: seed.PhysicalDayObservation(1)})
    (plan,) = seed.build_plan(None, profile=profile, start_date=start, end_date=end, datasets=("margin_detail",))
    assert calls == [(start, end)]
    assert plan.start_date == start
    assert [row.trade_date for row in plan.planned_rows] == [start]
    assert plan.blocked_dates == (end,)


def test_valid_seed_authority_is_preserved_not_replanned():
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params):
            assert "physical_audit_seed" in params[3]
            assert "error_message" in sql

        def fetchall(self):
            return [(date(2026, 9, 1),)]

    connection = SimpleNamespace(cursor=Cursor)
    ready = seed._existing_ready_dates(connection, seed.SPECS["trading_calendar"], date(2026, 9, 1), date(2026, 9, 30))
    plan = seed._build_dataset_plan(
        seed.SPECS["trading_calendar"],
        start=date(2026, 9, 1),
        end=date(2026, 9, 1),
        expected_dates=tuple(ready),
        physical_counts={},
        existing_ready_dates=ready,
    )
    assert plan.planned_rows == plan.blocked_dates == ()


@pytest.mark.parametrize("start", ["2026-10-01", "2018-07-31"])
def test_invalid_explicit_range_fails_before_database_open(monkeypatch, start):
    monkeypatch.setattr(
        seed, "_load_database_config", lambda *_: pytest.fail("database configuration must not be loaded")
    )
    with pytest.raises(seed.AuditSeedError, match="start date"):
        seed.main(["--database", "production", "--mode", "plan", "--start-date", start, "--end-date", "2026-09-30"])


def test_legacy_dev_apply_receipt_cannot_authorize_different_explicit_range(tmp_path):
    import json

    path = tmp_path / "dev.json"
    profile = SimpleNamespace(profile="qe_hmm_full_v2", config_digest="a", semantic_profile_digest="b")
    value = {
        "schema_version": seed.RECEIPT_SCHEMA_VERSION,
        "database_target": "dev",
        "status": "PASS",
        "profile": profile.profile,
        "profile_config_digest": "a",
        "semantic_profile_digest": "b",
        "required_failures": 0,
        "mode": "apply",
        "end_date": "2026-09-30",
        "dataset_names": ["trading_calendar"],
    }
    path.write_text(json.dumps(value))
    with pytest.raises(seed.AuditSeedError, match="DEV receipt"):
        seed._require_apply_authorization(
            target="production",
            authorization_ref="approved-exact-plan",
            dev_receipt=path,
            profile=profile,
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 30),
            datasets=("trading_calendar",),
        )


def test_plan_digest_distinguishes_range_even_if_no_rows_needed():
    spec = seed.SPECS["trading_calendar"]
    profile = SimpleNamespace(profile="qe_hmm_full_v2", config_digest="a", semantic_profile_digest="b")
    end = date(2026, 9, 30)
    full = seed.DatasetPlan(spec, date(2018, 8, 1), end, 2, 2, (), ())
    month = seed.DatasetPlan(spec, date(2026, 9, 1), end, 2, 2, (), ())
    assert seed._plan_digest(profile, end, (full,)) != seed._plan_digest(profile, end, (month,))
