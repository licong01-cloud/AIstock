"""Historical SW facts must survive provider truncation and source revisions."""

from copy import deepcopy
from datetime import date

import pytest

from backend.services.sw_industry_history_store import (
    canonical_bytes,
    prepare_member_refresh,
    source_fact,
)


def member(symbol="000001.SZ", start="20210730", end=None, current="Y"):
    return dict(
        l1_code="801780.SI",
        l1_name="银行",
        l2_code="801783.SI",
        l2_name="股份制银行Ⅱ",
        l3_code="851783.SI",
        l3_name="股份制银行Ⅲ",
        ts_code=symbol,
        name="测试",
        in_date=start,
        out_date=end,
        is_new=current,
    )


def test_partial_history_does_not_take_down_old_rows():
    old = member(start="20180101", end="20210729", current="N")
    new = member()
    assert prepare_member_refresh([old, new], [new], "801783.SI") == [new]


def test_disappearing_current_member_is_not_an_inferred_end_date():
    with pytest.raises(ValueError, match="unexplained active"):
        prepare_member_refresh([member()], [member("000002.SZ")], "801783.SI")


def test_explicit_ended_provider_row_allows_real_transition():
    old = member()
    ended = member(end="20261012", current="N")
    assert prepare_member_refresh([old], [ended], "801783.SI") == [ended]
    assert old["out_date"] is None


def test_daily_refresh_cannot_reopen_repaired_ended_history():
    old = member(end="20261012", current="N")
    with pytest.raises(ValueError, match="reopened"):
        prepare_member_refresh([old], [member()], "801783.SI")


@pytest.mark.parametrize(
    "bad",
    [
        {"l2_code": "801011.SI"},
        {"in_date": "20260230"},
        {"out_date": "20210729", "is_new": "N"},
        {"is_new": "N"},
        {"out_date": "20261012"},
        {"ts_code": None},
    ],
)
def test_invalid_provider_rows_fail_before_writing(bad):
    row = member()
    row.update(bad)
    with pytest.raises(ValueError):
        prepare_member_refresh([], [row], "801783.SI")


def test_duplicates_dedup_but_conflicting_payloads_fail():
    row = member()
    assert prepare_member_refresh([], [row, deepcopy(row)], "801783.SI") == [row]
    conflicting = dict(row, name="changed")
    with pytest.raises(ValueError, match="conflicting"):
        prepare_member_refresh([], [row, conflicting], "801783.SI")


def test_inclusive_end_requires_next_day_handoff():
    old = member(start="20180101", end="20210729", current="N")
    assert len(prepare_member_refresh([], [old, member()], "801783.SI")) == 2
    old["out_date"] = "20210730"
    with pytest.raises(ValueError, match="overlap"):
        prepare_member_refresh([], [old, member()], "801783.SI")


def test_source_revision_creates_new_fact_and_never_changes_old_fact():
    row = member()
    before = source_fact("index_member_all", "SW2021", row, {"is_new": "Y"})
    after = source_fact("index_member_all", "SW2021", dict(row, out_date="20261012", is_new="N"), {"is_new": "N"})
    assert before["fact_sha256"] != after["fact_sha256"]
    assert before["payload"]["row"]["out_date"] is None
    assert len(before["source_sha256"]) == 64
    assert b'"observed_at"' not in canonical_bytes(before["payload"])


def test_canonical_dates_and_taxonomy_version_are_identity():
    a = source_fact("index_member_all", "SW2021", dict(in_date=date(2021, 7, 30)), {})
    b = source_fact("index_member_all", "SW2014", dict(in_date=date(2021, 7, 30)), {})
    assert a["fact_sha256"] != b["fact_sha256"]
    assert a["payload"]["row"]["in_date"] == "2021-07-30"
    with pytest.raises(ValueError):
        canonical_bytes({"value": float("nan")})


def recovery():
    catalog = [
        dict(
            taxonomy_version="SW2021",
            taxonomy_code=str(level),
            level=level,
            parent_code=str(level - 1) if level > 1 else None,
            name=str(level),
            published_index_code="801783.SI" if level == 2 else None,
        )
        for level in (1, 2, 3)
    ]
    interval = dict(
        canonical_symbol="000001.SZ",
        taxonomy_version="SW2021",
        effective_from="2021-07-30",
        effective_to="2026-08-31",
        published_l2_code="801783.SI",
        source_sha256="a" * 64,
        source_row_sha256="b" * 64,
        known_from=None,
        knowledge_time_quality="FIRST_PUBLICATION_UNVERIFIED",
    )
    for level in (1, 2, 3):
        interval.update({f"l{level}_code": str(level), f"l{level}_name": str(level)})
    return dict(
        schema_version="sw_industry_recovery_bundle_v1",
        known_from_inferred=False,
        source_facts=[dict(source_version="official", source_sha256="a" * 64, source_row_sha256="b" * 64)],
        taxonomy_catalog=catalog,
        effective_intervals=[interval],
        retained_source_gaps=[],
    )


def test_recovery_sql_is_append_only_and_source_bound():
    from scripts.import_sw_industry_history import import_sql

    bundle = recovery()
    sql, identity, summary, expected = import_sql(bundle)
    assert summary["interval_rows"] == 1 and len(identity) == 64
    assert all(expected.values())
    assert "ON CONFLICT (fact_sha256) DO NOTHING" in sql
    assert not any(word in sql for word in ("DELETE ", "UPDATE ", "TRUNCATE ", "market.sw_index_member"))


@pytest.mark.parametrize("fault", ["backcast", "overlap", "parent", "source", "known_from"])
def test_recovery_rejects_unproved_history(fault):
    from scripts.import_sw_industry_history import validate_bundle

    bundle = recovery()
    row = bundle["effective_intervals"][0]
    if fault == "backcast":
        row["effective_from"] = "2018-08-01"
    if fault == "overlap":
        bundle["effective_intervals"].append(deepcopy(row))
    if fault == "parent":
        bundle["taxonomy_catalog"][1]["parent_code"] = "wrong"
    if fault == "source":
        row["source_row_sha256"] = "c" * 64
    if fault == "known_from":
        row["known_from"] = "2021-07-30"
    with pytest.raises(ValueError):
        validate_bundle(bundle)
