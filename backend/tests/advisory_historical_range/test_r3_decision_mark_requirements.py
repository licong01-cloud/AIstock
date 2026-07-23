from __future__ import annotations

from backend.services.advisory_historical_range.requirement_planner import HistoricalRangeSourceRequirementPlanner
from backend.tests.advisory_historical_range.conftest import digest, resolved_request


def test_r3_planner_adds_one_unfiltered_decision_mark_source_pair_per_day() -> None:
    resolved = resolved_request()
    plan = HistoricalRangeSourceRequirementPlanner().build(
        request=resolved.request,
        date_plan=resolved.date_plan,
        frozen_programs=resolved.frozen_programs,
        calendar_identity_hash=digest("calendar"),
        code_release_hash=resolved.frozen_programs[0].code_release_hash,
    )

    daily_market = [item for item in plan.requirements if item.source_role == "decision_mark_daily_market"]
    market_state = [item for item in plan.requirements if item.source_role == "decision_mark_market_state"]

    assert len(daily_market) == len(resolved.date_plan.ordered_trade_dates)
    assert len(market_state) == len(resolved.date_plan.ordered_trade_dates)
    assert all(item.package_id is None and item.component_id is None for item in daily_market + market_state)
    assert {item.query_template_id for item in daily_market} == {"historical_decision_mark_daily_market"}
    assert {item.query_template_id for item in market_state} == {"historical_decision_mark_market_state"}
