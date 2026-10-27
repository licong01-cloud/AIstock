from datetime import date, timedelta

import pytest

from backend.services.advisory_workbench.projection import (
    WorkbenchError, calendar_context, coverage, daily_projection, membership, run_state, streak_counts,
)


def item(symbol, action="HOLD", reason="PENDING_TARGET_OPEN_REVIEW", state="ACTIVE"):
    return dict(symbol=symbol, action=action, reason_code=reason, item_state=state)


@pytest.mark.parametrize("row,expected", [
    (item("A"), "IN_SCOPE"),
    (item("A", "WATCH", "PENDING_TARGET_OPEN_ENTRY", "WATCH"), "IN_SCOPE"),
    (item("A", "WATCH", "OUTSIDE_ENTRY_THRESHOLD", "WATCH"), "OBSERVATION"),
    (item("A", "WAITING", state="WAITING"), "OUTSIDE_PENDING"),
    (item("A", "EXIT", "STOP_LOSS", "EXITED"), "OUTSIDE_EXIT"),
    (item("A", "WATCH", "NEW_UNRECOGNIZED_REASON", "WATCH"), "UNKNOWN"),
])
def test_membership_never_treats_every_watch_or_waiting_as_entry_or_exit(row, expected):
    assert membership(row) == expected


def test_change_is_set_membership_not_action_or_return_direction():
    previous = [item("A"), item("B"), item("C")]
    current = [item("A", "WATCH", "PENDING_TARGET_OPEN_ENTRY", "WATCH"), item("D"),
               item("B", "WAITING", state="WAITING")]
    rows, counts = daily_projection(current, previous, prior_known=True)
    by_symbol = {r["symbol"]: r for r in rows}
    assert by_symbol["A"]["change"] == "CONTINUING"  # prior member, although now awaiting entry
    assert by_symbol["D"]["change"] == "NEW"  # HOLD alone is not continuity proof
    assert by_symbol["B"]["change"] == "REMOVED"
    assert by_symbol["B"]["progress"] == "WAITING_REVIEW"
    assert by_symbol["C"]["reason_code"] == "REMOVAL_WITHOUT_EXIT_DECISION"
    assert counts["in_scope"] == counts["new"] + counts["continuing"] + counts["unknown_change"] == 2
    assert counts["previous_count"] - counts["removed"] + counts["new"] == counts["in_scope"]


def test_missing_predecessor_preserves_unknown_instead_of_inventing_new():
    rows, counts = daily_projection([item("A")], [], prior_known=False)
    assert rows[0]["change"] == "UNKNOWN"
    assert counts["new"] == 0 and counts["previous_count"] is None


def test_duplicate_and_unrecognized_rows_remain_visible():
    rows, counts = daily_projection([item("A"), item("A")], [item("A")], prior_known=True)
    assert counts["in_scope"] == 0 and counts["unknown"] == 2
    assert counts["comparison_known"] is False and counts["previous_count"] is None
    assert all(r["membership_reason"] == "DUPLICATE_SYMBOL" for r in rows)


def test_asset_blockage_outweighs_old_success_but_not_existing_publication():
    failure = {"publication_status": "WAITING_DATA", "last_reason_code": "DATA_UNAVAILABLE",
        "last_error_json": {"context": {"reason_code": "multi_alpha_parent_leg_runtime_assets_incomplete"}}}
    assert run_state(failure, has_target=False, configured="ENABLED")["state"] == "ASSET_BLOCKED"
    failure.update(publication_status="PUBLISHED", settlement_status="WAITING_DATA")
    state = run_state(failure, has_target=True, configured="ENABLED")
    assert state["state"] == "PUBLISHED" and state["settlement"] == "WAITING_DATA"


def test_calendar_does_not_invent_weekdays_and_first_date_exceeds_twenty_versions():
    start = date(2026, 8, 14)
    calendar = [{"cal_date": start + timedelta(days=i), "is_trading": i % 7 not in {1, 2}} for i in range(60)]
    versions = [{"trade_date": r["cal_date"], "version_status": "PUBLISHED"} for r in calendar if r["is_trading"]]
    assert len(versions) > 20
    report = coverage(versions, calendar, str(calendar[-1]["cal_date"]))
    assert report["first_published_target_date"] == "2026-08-14" and report["status"] == "COMPLETE"
    assert coverage(versions, calendar[1:], str(calendar[-1]["cal_date"]))["status"] == "CALENDAR_INCOMPLETE"
    with pytest.raises(WorkbenchError, match="交易日历"):
        calendar_context([], date(2026, 10, 28))


def test_streak_stops_at_gap_and_does_not_reuse_another_binding():
    versions = [dict(list_version_id="v3", binding_version_id="b", trade_date="2026-10-28", version_status="PUBLISHED"),
                dict(list_version_id="v1", binding_version_id="b", trade_date="2026-10-26", version_status="PUBLISHED")]
    calendar = [dict(cal_date=date(2026, 10, d), is_trading=True) for d in (26, 27, 28)]
    data = {"v3": [item("A")], "v1": [item("A")]}
    assert streak_counts(versions[0], versions, data, calendar)["A"] is None
    versions.append(dict(list_version_id="v2", binding_version_id="b", trade_date="2026-10-27", version_status="PUBLISHED"))
    data["v2"] = [item("A")]
    assert streak_counts(versions[0], versions, data, calendar)["A"] == 3
    versions[1]["binding_version_id"] = "old-binding"
    assert streak_counts(versions[0], versions, data, calendar)["A"] is None
