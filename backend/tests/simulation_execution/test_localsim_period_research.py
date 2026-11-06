"""Real native account fixtures for the new period adapter, never economic evidence."""

from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim import period_continuation as preparation
from backend.services.simulation_execution.localsim import period_research as period
from backend.services.simulation_execution.localsim import timing_campaign as campaign
from backend.services.simulation_execution.localsim.research_performance import ResearchPerformanceOptions
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import publish, read_json
from backend.tests.simulation_execution.test_localsim_period_continuation import continuation as inherited_continuation

continuation = inherited_continuation


def put(path, value):
    publish(path, value)
    return ref(path)


@pytest.fixture
def period_fixture(continuation, monkeypatch):
    root = continuation["root"] / "account_adapter"
    h = deepcopy(continuation["handoff"])
    for e in h["prior_endpoints"]:
        day = read_json(Path(e["day"]["path"]))
        e.update({k: day[k] for k in ("rule", "delay_seconds")})
    native = h["native_projections"][0]
    projection = read_json(Path(native["projection"]["path"]))
    for weight, row in zip((0.2, 0.8), projection["aggregate_results"], strict=True):
        row["target_weight"] = weight
    native["projection"] = put(root / "signals/2026-09-23/projection.json", projection)
    native["completion"] = put(
        root / "signals/2026-09-23/completion.json", read_json(Path(native["completion"]["path"]))
    )
    native["target_payload_sha256"] = digest(projection["aggregate_results"])
    h_ref = FileRef.model_validate(put(root / "handoff.json", h))
    prepared = preparation.prepare_period(
        **{**continuation["kwargs"], "handoff_ref": h_ref, "output_root": root / "prepared"}
    )
    pf_ref = FileRef.model_validate(prepared["preflight"])
    pf = read_json(Path(pf_ref.path))
    original_source = period.read(continuation["request"]["source_request"])
    old_templates = period.read(original_source["price_templates"])
    last_events = period.read(old_templates["days"][-1]["events"])
    # Events are an array in the existing public price-template contract.
    new_events = json.loads(json.dumps(last_events).replace("2026-09-22", "2026-09-23"))
    events_ref = put(root / "price_events.json", new_events)
    policy = period.read(old_templates["policy_document"])
    policy.update(
        schema_version="position_timing_shared_price_policy_v1",
        preflight=pf_ref.model_dump(),
        period_preparation=pf["preparation_request"],
        opening_accounts=pf["initial_accounts"],
        historical_sell_symbols=pf["historical_sell_symbols"],
        start_trade_date="2026-09-23",
        end_trade_date="2026-09-23",
        selection_inventory_sha256=pf["selection_inventory"]["sha256"],
    )
    # Like the real October producer Counter, omit the zero UNKNOWN category.
    policy["reference_audit"] = put(
        root / "reference_audit.json",
        {
            "schema_version": "position_timing_shared_price_reference_audit_v1",
            "price_policy_id": policy["price_policy_id"],
            "outcomes_read": False,
            "unknown_references": [],
            "trade_day_count": 1,
            "stock_count": len({e["symbol"] for e in new_events}),
            "counts": {
                "BUY": sum(e["side"] == "BUY" for e in new_events),
                "SELL": sum(e["side"] == "SELL" for e in new_events),
                "AVAILABLE": len(new_events),
            },
        },
    )
    policy_ref = put(root / "price_policy.json", policy)
    templates = {
        **old_templates,
        "selection_inventory_sha256": pf["selection_inventory"]["sha256"],
        "policy_document": policy_ref,
        "days": [{"trade_date": "2026-09-23", "events": events_ref}],
    }
    tpl_ref = FileRef.model_validate(put(root / "templates.json", templates))
    manifest_ref = FileRef.model_validate(
        put(
            root / "price_manifest.json",
            {"spec": {"preflight": pf_ref.model_dump()}, "templates": tpl_ref.model_dump()},
        )
    )

    # The preparation-only fixture never used the calendar in a new-day replay.
    # Shift its calendar too, so the full native account runner sees one day.
    class Reader(preparation.CandidateReader):
        def market(self, symbol, day):
            market = super().market(symbol, day)
            shift = timedelta(days=(day - market.calendar.trade_date).days)
            return market.model_copy(
                update={
                    "calendar": market.calendar.model_copy(
                        update={
                            "trade_date": day,
                            "available_at": market.calendar.available_at + shift,
                        }
                    ),
                }
            )

    monkeypatch.setattr(period, "CandidateReader", Reader)
    monkeypatch.setattr(campaign, "CandidateReader", Reader)
    # Replace only owner-side file construction in this synthetic fixture.
    # LocalSIM source, native account/day, fee, core and journal paths are real.
    monkeypatch.setattr(period, "inspect_price_manifest", lambda manifest: {"templates": tpl_ref.model_dump()})
    frozen = period.freeze_period(
        preflight_ref=pf_ref,
        templates_ref=tpl_ref,
        manifest_ref=manifest_ref,
        output_root=continuation["root"].parent.parent / (continuation["root"].parent.name + "-account-studies"),
        producer_commit="4" * 40,
    )
    return FileRef.model_validate(frozen["request"]), root, prepared


def test_real_inherited_accounts_inspect_and_one_full_economic_retry(period_fixture, monkeypatch):
    request, _, prepared = period_fixture
    original = dual._compute_account_day
    calls = []

    def counted(job):
        assert (Path(request.path).parent / "raw_source_inspection.json").is_file()
        calls.append(job["initial"]["account_id"])
        return original(job)

    monkeypatch.setattr(dual, "_compute_account_day", counted)
    options = ResearchPerformanceOptions(optimized=True)
    first = period.run_period(request, performance=options)
    assert first["computed_account_days"] == 13 and len(calls) == 13
    facts = period.read(first["facts"])
    assert len(facts["accounts"]) == 13 and all(len(a["days"]) == 1 for a in facts["accounts"])
    openings = [period.read(r) for r in period.read(prepared["preflight"])["initial_accounts"]]
    for account, opening in zip(facts["accounts"], openings, strict=True):
        day = period.read(account["days"][0])
        assert day["initial_account"]["cash_cny"] == opening["cash_cny"]
        assert day["initial_account"]["positions"] == opening["positions"]
        for parent in day["parents"]:
            audits = ([parent["session"]] if "session" in parent else []) + parent.get("sessions", [])
            for audit in audits:
                for event in audit["events"]:
                    assert event["event_time"].startswith(day["trade_date"])
                    if event["fill"] is not None:
                        assert event["event_time"] == event["fill"]["trade_time"]
    assert any(period.read(a["days"][0])["parents"] for a in facts["accounts"])
    before = {p: p.read_bytes() for p in Path(request.path).parent.rglob("*.json")}
    checked = period.inspect_period(request)
    assert checked["verified_account_days"] == 13 and len(calls) == 13
    retried = period.run_period(request, performance=options, economic_retry=True)
    assert retried["status"] == "EXACT_RETRY_VERIFIED" and retried["recomputed_account_days"] == 13
    assert len(calls) == 26 and all(p.read_bytes() == b for p, b in before.items())
    again = period.run_period(request, performance=options, economic_retry=True)
    assert again["status"] == "ALREADY_VERIFIED" and again["recomputed_account_days"] == 0 and len(calls) == 26
    with monkeypatch.context() as guarded:

        def bad_source(*args):
            raise ValueError("FUNCTIONAL_SOURCE_DRIFT")

        guarded.setattr(period, "_sources", bad_source)
        guarded.setattr(period, "_inspect_completed", lambda *args: pytest.fail("outcomes read before sources"))
        with pytest.raises(ValueError, match="FUNCTIONAL_SOURCE_DRIFT"):
            period.inspect_period(request)


def test_partial_day_prefix_resumes_without_duplicate_economics(period_fixture):
    request, _, _ = period_fixture
    first = period.run_period(
        request, stop_after_account_days=1, performance=ResearchPerformanceOptions(optimized=True)
    )
    assert first["status"] == "CHECKPOINTED" and first["computed_account_days"] == 1
    root = Path(request.path).parent
    before = {p: p.read_bytes() for p in (root / "journals").rglob("*.json")}
    resumed = period.run_period(request, performance=ResearchPerformanceOptions(optimized=True))
    assert resumed["computed_account_days"] == 12 and all(p.read_bytes() == b for p, b in before.items())
    assert period.inspect_period(request)["verified_account_days"] == 13
    all_days = period.read(resumed["facts"])["accounts"]
    total = []
    for a in all_days:
        day = period.read(a["days"][0])
        journal = period.read(day["account_receipt"])["payload"]
        total.extend(journal["facts"]["fills"])
    assert len(total) == len(set(total)) and total


def test_economic_divergence_preserves_prior_results_and_rejects_success(period_fixture, monkeypatch):
    request, _, _ = period_fixture
    period.run_period(request, performance=ResearchPerformanceOptions(optimized=True))
    root = Path(request.path).parent
    before = {p: p.read_bytes() for p in root.rglob("*.json")}
    original = dual._compute_account_day

    def changed(job):
        result = original(job)
        result["account"]["cash_cny"] = "999999999.99"
        return result

    monkeypatch.setattr(dual, "_compute_account_day", changed)
    with pytest.raises(ValueError, match="RECOMPUTE_ACCOUNT_CONFLICT"):
        period.run_period(request, economic_retry=True, performance=ResearchPerformanceOptions(optimized=True))
    assert all(p.read_bytes() == b for p, b in before.items())
    assert not (root / "economic_retry_receipt.json").exists()
    assert list((root / "failures").glob("*.json"))


def test_wrong_v1_request_is_rejected_before_any_write(tmp_path):
    path = tmp_path / "old/request.json"
    source = FileRef.model_validate(put(path, {"schema_version": "localsim_dual_mode_study_v1"}))
    before = {p: p.read_bytes() for p in tmp_path.rglob("*.json")}
    with pytest.raises(ValueError, match="REQUEST_SCOPE_CONFLICT"):
        period.run_period(source)
    assert before == {p: p.read_bytes() for p in tmp_path.rglob("*.json")}
    assert not (path.parent / "study-writer.lock").exists()
