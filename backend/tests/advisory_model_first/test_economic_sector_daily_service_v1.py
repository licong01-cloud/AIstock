"""Service composition/budgets; original model math has separate actual parity tests."""

from datetime import date
import json
from types import SimpleNamespace as NS

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_sector_daily_service_v1 as m
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def test_not_configured_has_no_io_and_is_not_a_negative_price_recommendation(monkeypatch):
    monkeypatch.delenv("AISTOCK_ADVISORY_SECTOR_PRICE_CONFIG", raising=False)

    def forbidden(*a, **k):
        pytest.fail("unconfigured consumer must not read assets or DB")

    result = m.AdvisorySectorDailyServiceV1(config_loader=forbidden, session_factory=forbidden).read_day(program_id="p")
    assert result["status"] == "NOT_CONFIGURED" and result["candidates"] == []
    assert result["economic_effectiveness"] == "NOT_CONFIRMED" and result["package_qualification_rechecked"] is False


@pytest.mark.parametrize("kind", ["duplicate_json", "nonfinite_json", "unknown_key", "wrong_family"])
def test_real_configuration_header_rejects_corruption_before_model_or_database(monkeypatch, kind):
    config = dict.fromkeys(m.CONFIG_KEYS)
    config.update(schema_version="economic_sector_daily_config_v1", model_family=m.FAMILY, price_context_mode="LIVE_DB")
    if kind == "unknown_key":
        config["qualified_pointer"] = "not_a_new_gate"
    if kind == "wrong_family":
        config["model_family"] = "legacy_v3"
    body = json.dumps(config).encode()
    if kind == "duplicate_json":
        body = b'{"model_family": "one", "model_family": "two"}'
    if kind == "nonfinite_json":
        body = b'{"weights": NaN}'
    monkeypatch.setattr(m, "_read_file", lambda *_: body)

    def forbidden(**_):
        pytest.fail("bad configuration must not open a model or database")

    monkeypatch.setattr(m, "load_sector_research_bundle_v1", forbidden)
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        m.load_sector_daily_configuration_v1("X:/unit/config.json")


@pytest.fixture
def composition():
    state = NS(closed=False, loads=0, family_calls=0, config_checks=0, mutate=None)
    scope = {key: "a" * 64 for key in m.SCOPE_KEYS}
    scope.update(program_id="p", package_id="pkg", universe_selection={"mode": "stock_universe", "pool_ids": []})

    def verify():
        state.config_checks += 1

    config = NS(
        bundle=NS(scope=scope),
        roles={"lstm": "a", "fund": "b"},
        weights={"a": 0.6, "b": 0.4},
        price_context_mode="LIVE_DB",
        pit_universe_key=None,
        config_sha256="b" * 64,
        verify_unchanged=verify,
    )

    def load(_):
        state.loads += 1
        return config

    def close():
        state.closed = True

    session = NS(close=close)

    class Reader:
        def __init__(self, **k):
            assert k["read_session"] is session

        def load_day(self, *, target_date, **k):
            target = target_date or date(2026, 8, 28)
            decision = date(2026, 8, 27)
            receipt = dict(
                decision_date=decision.isoformat(),
                target_date=target.isoformat(),
                source_review_policy_sha256="d" * 64,
                unmodeled_items=[],
            )
            return dict(
                candidates=pd.DataFrame({"instrument": ["000001.SZ"]}),
                calendar=(decision, target),
                price_contexts={},
                scope=scope,
                candidate_receipt=receipt,
            )

    class Family:
        def predict_batch(self, *, packets):
            assert state.closed, "DB read budget must end before full batch CPU computation"
            state.family_calls += 1
            results = [
                dict(
                    model_family=m.FAMILY,
                    scope=packet["scope"],
                    decision_date=packet["calendar"][-2].isoformat(),
                    target_date=packet["calendar"][-1].isoformat(),
                    candidates=[{"instrument": "000001.SZ"}],
                    status="COMPUTED",
                    model_sha256="c" * 64,
                    bundle_sha256="e" * 64,
                )
                for packet in packets
            ]
            if state.mutate:
                state.mutate(results)
            return results

    state.service = m.AdvisorySectorDailyServiceV1(
        config_path="F:/unit/config.json",
        config_loader=load,
        session_factory=lambda: session,
        list_source_factory=Reader,
        family_factory=lambda _: Family(),
    )
    return state


def test_batch_loads_config_once_uses_one_family_call_and_different_policy_hashes(composition):
    state = composition
    results = state.service.read_batch(program_id="p", target_dates=[date(2026, 8, 28), date(2026, 8, 31)])
    assert len(results) == 2 and state.loads == state.family_calls == state.config_checks == 1
    assert all(row["source_review_policy_sha256"] != row["model_parent_policy_identity"] for row in results)
    assert all(row["database_written"] is row["outcomes_read"] is row["deployable"] is False for row in results)


@pytest.mark.parametrize("targets", [[], ["2026-08-28"] * 2, ["2026-08-28"] * 21, ["not-a-date"]])
def test_bad_batch_does_not_load_assets(composition, targets):
    with pytest.raises(AdvisoryModelFirstError):
        composition.service.read_batch(program_id="p", target_dates=targets)
    assert composition.loads == 0


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: rows.pop(),
        lambda rows: rows[0].update(target_date="2027-01-01"),
        lambda rows: rows[0]["candidates"].append({"instrument": "foreign"}),
        lambda rows: rows[0].update(model_family="foreign"),
    ],
)
def test_family_roster_or_identity_drift_is_not_presented_as_success(composition, mutation):
    composition.mutate = mutation
    with pytest.raises(AdvisoryModelFirstError):
        composition.service.read_day(program_id="p", target_date=date(2026, 8, 28))
    assert composition.closed
