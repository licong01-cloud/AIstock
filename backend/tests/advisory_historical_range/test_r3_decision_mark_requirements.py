from __future__ import annotations

from copy import deepcopy

import pytest

from backend.services.advisory_historical_range.requirement_planner import HistoricalRangeSourceRequirementPlanner
from backend.tests.advisory_historical_range.conftest import digest, resolved_request
from backend.tests.advisory_historical_range.test_task_input_sources import authority as authority, _task


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


@pytest.mark.parametrize("mode", ["DATABASE_ONLY", "FILE_ONLY", "FILE_THEN_DATABASE"])
def test_planned_mark_state_pins_native_pit_while_quotes_remain_unfiltered(
    tmp_path, monkeypatch, authority, mode,
) -> None:
    from backend.services.advisory_historical_range import input_source_plan as sources
    from backend.services.advisory_historical_range.task_input_sources import freeze_program_input_sources
    from backend.tests.advisory_historical_range.test_input_source_plan import _canonical_policy, _snapshot

    dates = ("2026-01-20", "2026-01-21") if mode == "FILE_ONLY" else (
        "2026-01-20", "2026-01-21", "2026-01-22",
    )
    request, program, date_plan = _task(mode, dates=dates)
    if mode != "DATABASE_ONLY":
        snapshot = _snapshot(tmp_path, policy=_canonical_policy())
        monkeypatch.setattr(sources, "freeze_file_source", lambda **_: deepcopy(snapshot))
    programs = freeze_program_input_sources(request=request, date_plan=date_plan, frozen_programs=(program,))
    assert programs[0].admitted_package_projection.canonical_pit_binding is None
    before = programs[0].model_dump(mode="json")
    plan = HistoricalRangeSourceRequirementPlanner().build(
        request=request, date_plan=date_plan, frozen_programs=programs,
        calendar_identity_hash=digest("calendar"), code_release_hash=program.code_release_hash,
    )
    states = [item for item in plan.requirements if item.source_role == "decision_mark_market_state"]
    assert len(states) == len(dates)
    assert {item.parameter_template["universe_key"] for item in states} == {
        programs[0].selection_input_source_plan["native_policy"]["universe_key"],
    }
    assert all("shsz_st_pit_active_v1" not in item.partition_ref_template for item in states)
    quotes = [item for item in plan.requirements if item.source_role == "decision_mark_daily_market"]
    assert all("universe_key" not in item.parameter_template for item in quotes)
    assert programs[0].model_dump(mode="json") == before
