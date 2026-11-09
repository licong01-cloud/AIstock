"""Business boundaries for monthly diagnostics; reuse the existing four-account fixture."""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import adverse_wait_diagnostics as diag
from backend.services.position_timing.intraday import adverse_wait_results as ar
from backend.services.position_timing.intraday.contracts import payload
from backend.services.position_timing.intraday.period_context import read
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.store import IntradayError
from backend.tests.position_timing.test_intraday_adverse_results import p2_history  # noqa: F401
from backend.tests.position_timing.test_intraday_wait_history import history  # noqa: F401
from backend.tests.position_timing.test_intraday_wait_control import period, three_arm  # noqa: F401


@pytest.fixture
def diagnostic(request):
    original, root = request.getfixturevalue("p2_history")
    report = ar.analyze(original, root/"original")["report"]
    return diag.DiagnosticSpec(analysis_spec=freeze(root/"analysis-spec.json", original), analysis_report=report), root


def test_full_population_months_reconcile_without_replaying_or_altering_old_report(diagnostic):
    spec, root = diagnostic
    before = ar.implementation_identity()
    result = diag.analyze(spec, root/"diagnostics")
    assert diag.analyze(spec, root/"diagnostics") == result | {"status": "ALREADY_MATERIALIZED"}
    report, original = read(result["report"]), read(spec.analysis_report)
    assert {p["arm_id"] for p in report["account_panels"]} == {"B0", "W15", "C2", "P2"}
    months = sorted({d[:7] for d in original["trade_dates"]})
    assert len(report["account_panels"]) == 4*(len(months)+1)
    all_panels = [p for p in report["account_panels"] if p["label"] == "ALL"]
    for panel, old in zip(all_panels, original["diagnostics"], strict=True):
        assert {s["side"]: s["parent_denominator"] for s in panel["sides"]} == old["parent_denominators"]
    for pair in original["comparisons"]:
        groups = [p for p in report["comparison_panels"] if p["baseline_arm_id"] == pair["baseline_arm_id"]]
        assert sum(Decimal(p["period_net_increment_cny"]) for p in groups if p["label"] != "ALL") == Decimal(pair["period_net_increment_cny"])
        assert all(p["side_pnl_attribution"].startswith("UNAVAILABLE") for p in groups)
    assert all(x["status"] == "UNAVAILABLE" for x in report["time_weighted_exposure"].values())
    assert not report["quantity_matched"] and not report["account_replay_started"] and not report["economic_confirmation"]
    assert ar.implementation_identity() == before


@pytest.mark.parametrize("source", ["analysis_report", "late_source"])
def test_source_corruption_is_rejected_before_diagnostic_economics(diagnostic, monkeypatch, source):
    spec, root = diagnostic
    original = read(spec.analysis_spec)
    delivery = read(original["source_delivery"])
    damaged = spec.analysis_report if source == "analysis_report" else delivery["ordered_account_days"][-1]["day"]
    Path(payload(damaged)["path"]).write_text("{}", encoding="utf-8")
    monkeypatch.setattr(diag, "diagnose", lambda *args: pytest.fail("diagnostics before complete source preflight"))
    with pytest.raises(IntradayError):
        diag.analyze(spec, root/"rejected")
    assert not (root/"rejected").exists()


def test_source_change_after_diagnostics_cannot_publish(diagnostic, monkeypatch):
    spec, root = diagnostic
    original = diag.diagnose
    def drift(*args):
        result = original(*args)
        Path(spec.analysis_spec.path).write_text("{}", encoding="utf-8")
        return result
    monkeypatch.setattr(diag, "diagnose", drift)
    with pytest.raises(IntradayError, match="SOURCE_IDENTITY_MISMATCH"):
        diag.analyze(spec, root/"rejected")
    assert not (root/"rejected").exists()


def test_monthly_increment_must_match_frozen_account_endpoints(diagnostic):
    spec, _ = diagnostic
    _, accounts, parent, _ = diag.source_preflight(spec)
    parent["period_comparisons"][0]["comparisons"][0]["period_net_increment_cny"] = "999999"
    with pytest.raises(IntradayError, match="NET_RECONCILIATION"):
        diag.diagnose(accounts, parent)


def test_side_samples_keep_unfilled_and_unknown_fees_and_conditional_match_bias():
    parent = {"plan_id": "p", "symbol": "000001.SZ", "side": "SELL", "status": "PARTIAL",
        "target_quantity": 100, "filled_quantity": 10, "average_price_raw": "10"}
    day = {"trade_date": "2026-08-03", "terminal_as_of": "2026-08-03T14:57:00+08:00", "parents": [parent],
        "human_operation_state": {"actions": []}, "fees_cny": "5"}
    result = diag.side_panel([day], "SELL")
    assert result["parent_denominator"] == result["parent_status"]["PARTIAL"] == 1
    assert result["filled_parent_count"] == 1 and result["first_fill_delay_sample_count"] == 0
    assert result["fees"]["status"] == "UNAVAILABLE" and result["fees"]["cny"] is None
    assert result["ready_to_dispatch_seconds_median"] is None and result["early_parent_count"] == 0
    key = (day["trade_date"], parent["symbol"], "SELL", 100)
    a, b = deepcopy(parent), deepcopy(parent)
    a.update(status="FILLED", filled_quantity=100, average_price_raw="11")
    b.update(status="FILLED", filled_quantity=100, average_price_raw="10")
    matched = diag.matched_panel({key: a}, {key: b}, "SELL")
    assert matched["conditional_price_improvement_excluding_fees_cny"] == 100
    assert matched["selection_bias"].startswith("CONDITIONAL_SUBSET")
    assert diag.matched_panel({key: parent}, {key: b}, "SELL")["both_fully_filled_pair_count"] == 0


@pytest.mark.parametrize("revision_count", [1, 2])
def test_early_opportunity_does_not_imply_early_execution_or_revision_specific_attribution(revision_count):
    def stamp(t):
        return "2026-08-03T"+t+":00+08:00"
    signals = [{"revision": i+1, "batch_started_at": stamp("09:30"), "first_outer_opportunity_at": stamp("09:35")}
        for i in range(revision_count)]
    parent = {"plan_id": "p", "side": "BUY", "status": "FILLED", "target_quantity": 100, "filled_quantity": 100,
        "research_timing": {"first_dispatch_at": stamp("09:50"), "first_fill_at": stamp("09:51"), "signal_revisions": signals}}
    day = {"trade_date": "2026-08-03", "terminal_as_of": stamp("14:57"), "parents": [parent], "fees_cny": "5",
        "fees_by_side_cny": {"BUY": "5", "SELL": "0"}, "human_operation_state": {"actions": [{"plan_id": "p",
            "kind": "SUBMIT", "status": "COMPLETED", "visible_at": stamp("09:35"), "ready_at": stamp("09:36"),
            "dispatch_at": stamp("09:50")}]}}
    panel = diag.side_panel([day], "BUY")
    assert panel["early_parent_count"] == 1 and panel["early_revision_count"] == revision_count
    assert panel["single_revision_early_parent_denominator"] == (revision_count == 1)
    assert panel["single_revision_dispatch_before_15m"] == panel["single_revision_first_fill_before_15m"] == 0
    assert panel["ready_to_dispatch_seconds_median"] == 840 and panel["dispatch_to_first_fill_seconds_median"] == 60
    assert panel["multi_revision_execution_attribution"] == "UNAVAILABLE"
