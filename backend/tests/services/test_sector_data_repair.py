"""Only real dependency revisions invalidate existing sector dates."""
from datetime import date

import pytest

from backend.services.sector_data_repair import membership_revision_spans


def row(end=None, name="old"):
    return dict(ts_code="002086.SZ", l2_code="801153.SI", in_date="20220729", out_date=end, name=name)


def test_identical_or_name_only_refresh_does_not_rebuild_history():
    assert membership_revision_spans([row()], [row(name="new")]) == []


def test_closing_member_invalidates_only_removed_dates_not_entire_history():
    assert membership_revision_spans([row()], [row("20260630")]) == [(date(2026, 7, 1), None)]


def test_new_membership_invalidates_its_actual_closed_interval():
    assert membership_revision_spans([], [row("20260630")]) == [(date(2022, 7, 29), date(2026, 6, 30))]


def test_historical_boundary_correction_invalidates_only_symmetric_difference():
    assert membership_revision_spans([row("20260702")], [row("20260630")]) == [
        (date(2026, 7, 1), date(2026, 7, 2))
    ]


def test_missing_source_row_is_reported_not_used_to_close_membership():
    from backend.services.sector_data_repair import projection_revision_plan

    plan = projection_revision_plan([row()], [], date(2026, 7, 1), date(2026, 7, 3))
    assert not plan["changes"]
    assert len(plan["unverified_existing_identities"]) == 1


def test_revision_marker_requires_source_transaction_and_retains_racing_future_date():
    from backend.services.sector_data_repair import mark_revision_dates

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            calls.append((sql, params))

        def fetchone(self):
            return (date(2026, 7, 3),)

    class Connection:
        autocommit = True

        def cursor(self):
            return Cursor()

    calls = []
    connection = Connection()
    with pytest.raises(ValueError, match="source transaction"):
        mark_revision_dates(connection, [date(2026, 7, 1)], "sw_daily")
    assert not calls
    connection.autocommit = False
    mark_revision_dates(connection, [date(2026, 7, 1), date(2026, 7, 1), date(2026, 7, 6)], "sw_daily")
    assert calls[-1][1] == ("sw_daily", [date(2026, 7, 1), date(2026, 7, 6)])


def test_dated_projection_rejects_source_hash_drift_and_never_reopens_history():
    from backend.services.sector_data_repair import projection_revision_plan
    from backend.services.sw_industry_history_store import digest

    value = dict(row(), l1_code="801150.SI", l3_code="851534.SI", is_new="Y")
    payload = dict(row=value, endpoint="index_member_all", source_row_sha256=digest(value))
    fact = dict(payload=payload, fact_sha256=digest(payload))
    with pytest.raises(ValueError, match="reopened"):
        projection_revision_plan([row("20260630")], [fact], date(2026, 7, 1), date(2026, 7, 3))
    fact["fact_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="identity differs"):
        projection_revision_plan([], [fact], date(2026, 7, 1), date(2026, 7, 3))


def test_approved_classification_boundary_requires_real_successor_and_is_idempotent():
    from backend.services.sector_data_repair import projection_revision_plan
    from backend.services.sw_industry_history_store import digest

    current = dict(row(), l1_code="801010.SI", l2_code="801012.SI", l3_code="850154.SI",
                   in_date="20260701", is_new="Y")
    payload = dict(row=current, endpoint="index_member_all", source_row_sha256=digest(current))
    fact = dict(payload=payload, fact_sha256=digest(payload))
    history = dict(canonical_symbol="002086.SZ", published_l2_code="801153.SI",
                   effective_from="2022-07-29", effective_to="2026-06-30")
    approved = dict(bundle_sha256="a" * 64, payload=history,
                    fact_sha256=digest(dict(bundle_sha256="a" * 64, row=history)))
    plan = projection_revision_plan([row()], [fact], "2026-06-30", "2026-07-03", approved_history=[approved])
    assert any(item.get("resolution") == "CLOSE_BY_APPROVED_DATED_CLASSIFICATION_AND_SUCCESSOR" for item in plan["changes"])
    assert not plan["unverified_existing_identities"]
    assert not projection_revision_plan([row("20260630"), current], [fact], "2026-06-30", "2026-07-03", approved_history=[approved])["changes"]
    without_successor = projection_revision_plan([row()], [], "2026-06-30", "2026-07-03", approved_history=[approved])
    assert not without_successor["changes"] and without_successor["unverified_existing_identities"]


def test_operator_rejects_wrong_dev_target_before_connect_and_preserves_decimal(monkeypatch):
    from decimal import Decimal
    import scripts.repair_sw_sector_revision as operator

    monkeypatch.setattr(operator, "dotenv_values", lambda _: {"TDX_DB_DEV_NAME": "aistock"})
    monkeypatch.setattr(operator.psycopg2, "connect", lambda **kwargs: pytest.fail("must not connect"))
    with pytest.raises(ValueError, match="target differs"):
        operator.connect("unused", "dev", readonly=False)
    assert operator.receipt_value({"value": Decimal("12345678.0010")}) == {"value": "12345678.0010"}


def test_pit_geometry_extension_marks_only_tail_and_label_changes_mark_nothing():
    from backend.services.sector_data_repair import eligibility_revision_spans

    previous = dict(ts_code="002086.SZ", eligible_start="2024-06-12", eligible_end="2026-08-31")
    same = dict(previous, exit_reason="different provenance label")
    assert eligibility_revision_spans([previous], [same]) == []
    assert eligibility_revision_spans([previous], [dict(previous, eligible_end="2026-09-30")]) == [
        (date(2026, 9, 1), date(2026, 9, 30))
    ]


def test_pit_geometry_detects_real_removed_dates_and_ignores_equivalent_splits():
    from backend.services.sector_data_repair import eligibility_revision_spans

    a = dict(ts_code="002086.SZ", eligible_start="2026-07-01", eligible_end="2026-07-10")
    split = [dict(a, eligible_end="2026-07-03"), dict(a, eligible_start="2026-07-04")]
    assert eligibility_revision_spans([a], split) == []
    shortened = [dict(a, eligible_end="2026-07-03")]
    assert eligibility_revision_spans([a], shortened) == [(date(2026, 7, 4), date(2026, 7, 10))]
