"""Small original roster fixture: fixed calendar, normal unknowns and contradictions."""
from copy import deepcopy
from datetime import date, timedelta

import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import build_generic_price_5td_labels_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


@pytest.fixture
def packet():
    days = [date(2024, 1, 2)+timedelta(days=k) for k in (0, 1, 2, 5, 6, 7, 8)]
    d, t = days[:2]
    key = dict(decision_as_of_trade_date=d, target_trade_date=t, instrument="000001.SZ")
    roster = pd.DataFrame([{**key, "selection_effective_rank": 1, "candidate_group_size": 1}])
    prices = pd.DataFrame([dict(decision_as_of_trade_date=d, trade_date=day, instrument="000001.SZ",
                               open=10., high=11.5, low=9.+i/10, close=10.+i/5, d_anchor_factor=1.,
                               suspended=False, tradability_unknown=False, up_limit=12., down_limit=8.)
                           for i, day in enumerate(days[1:6])])
    reference = pd.DataFrame([{**key, "reference_cny": 10., "reference_visible_through": d}])
    return dict(candidates=roster, decision_dates=[d], calendar=days, prices=prices, references=reference,
                source_context=dict(calendar_sha256="a"*64, prices_sha256="b"*64, references_sha256="c"*64,
                                    label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence="NON_VINTAGE"))


def test_five_original_sessions_hand_values_hash_and_no_mutation(packet):
    before = deepcopy(packet)
    labels, receipt = build_generic_price_5td_labels_v1(**packet)
    row = labels.iloc[0]
    assert row.label_information_end.date() == packet["calendar"][5]
    assert row.gross_terminal_ratio == pytest.approx(1.08) and row.path_min_ratio == pytest.approx(.9)
    assert row.observed_gap_bps == 0 and row.label_status == "AVAILABLE"
    assert row.policy_sha256 == POLICY_SHA256 and row.label_contract == "GENERIC_ENTRY_FIXED_5TD_V1"
    assert receipt["candidate_count"] == 1 and not receipt["new_native_receipt"]
    for field in ("candidates", "prices", "references"):
        pd.testing.assert_frame_equal(packet[field], before[field])


@pytest.mark.parametrize("mode,reason", [("missing", "UNKNOWN_PATH"), ("halt", "UNKNOWN_PATH"),
    ("entry_halt", "ENTRY_SUSPENDED"), ("entry_limit", "ENTRY_AT_UP_LIMIT"),
    ("exit_limit", "UNKNOWN_ENDPOINT_EXECUTION"), ("unknown_law", "UNKNOWN_TRADABILITY"),
    ("unknown_factor", "UNKNOWN_PATH"), ("unknown_reference_clock", "UNKNOWN_REFERENCE")])
def test_normal_missing_no_drop_fill_or_horizon_extension(packet, mode, reason):
    if mode == "missing":
        packet["prices"] = packet["prices"].drop(index=2)
    elif mode in ("halt", "entry_halt"):
        packet["prices"].loc[2 if mode == "halt" else 0, "suspended"] = True
    elif mode == "entry_limit":
        packet["prices"].loc[0, "up_limit"] = 10.
    elif mode == "exit_limit":
        packet["prices"].loc[4, "down_limit"] = 10.8
    elif mode == "unknown_law":
        packet["prices"].loc[0, "tradability_unknown"] = True
    elif mode == "unknown_factor":
        packet["prices"].loc[1, "d_anchor_factor"] = None
    else:
        packet["references"]["reference_visible_through"] = None
    labels, receipt = build_generic_price_5td_labels_v1(**packet)
    assert len(labels) == 1 and labels.iloc[0].label_reason == reason
    assert labels.iloc[0].label_information_end.date() == packet["calendar"][5]
    assert labels.gross_terminal_ratio.isna().all() and receipt["candidate_count"] == 1


@pytest.mark.parametrize("mode", ["future_reference", "duplicate_quote", "foreign", "beyond_horizon",
                                 "bool_price", "inf", "ohlc", "rank", "wrong_t"])
def test_known_contradictions_fail_closed(packet, mode):
    if mode == "future_reference":
        packet["references"].loc[0, "reference_visible_through"] = packet["calendar"][1]
    elif mode == "duplicate_quote":
        packet["prices"] = pd.concat([packet["prices"], packet["prices"].iloc[:1]])
    elif mode == "foreign":
        packet["prices"].loc[0, "instrument"] = "600000.SH"
    elif mode == "beyond_horizon":
        packet["prices"].loc[0, "trade_date"] = packet["calendar"][-1]
    elif mode in ("bool_price", "inf", "ohlc"):
        packet["prices"]["open"] = packet["prices"].open.astype(object)
        packet["prices"].loc[0, "open"] = {"bool_price": True, "inf": float("inf"), "ohlc": 20.}[mode]
    elif mode == "rank":
        packet["candidates"].loc[0, "selection_effective_rank"] = 2
    else:
        packet["candidates"].loc[0, "target_trade_date"] = packet["calendar"][2]
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        build_generic_price_5td_labels_v1(**packet)


def test_short_calendar_is_immature_empty_schedule_preserved(packet):
    packet["calendar"] = packet["calendar"][:4]
    packet["prices"] = packet["prices"].iloc[:3]
    labels, _ = build_generic_price_5td_labels_v1(**packet)
    assert labels.iloc[0].label_status == "IMMATURE" and pd.isna(labels.iloc[0].label_information_end)
    for field in ("candidates", "prices", "references"):
        packet[field] = packet[field].iloc[:0].astype(object)
    labels, receipt = build_generic_price_5td_labels_v1(**packet)
    assert labels.empty and receipt["decision_dates"] == [packet["decision_dates"][0].isoformat()]


def test_same_raw_quote_different_D_anchor_not_reused(packet):
    packet["prices"]["d_anchor_factor"] = 2.
    packet["references"]["reference_cny"] = 20.
    labels, _ = build_generic_price_5td_labels_v1(**packet)
    assert labels.iloc[0].gross_terminal_ratio == pytest.approx(1.08)
    assert labels.iloc[0].observed_gap_bps == pytest.approx(0.)


@pytest.mark.parametrize("mode,expected_ratio,exit_state", [
    ("holding_halt", 1.08, "NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF"),
    ("resume_adjustment", 1.08, "NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF"),
    ("endpoint_halt", 1.06, "PENDING_EXIT_SUSPENDED"),
    ("endpoint_limit", 1.08, "EXIT_UNPROVEN_LIMIT_DOWN"),
    ("endpoint_unknown_limit", 1.08, "UNKNOWN_EXIT_LIMIT"),
])
def test_v2_known_valuation_never_upgrades_original_execution(packet, mode, expected_ratio, exit_state):
    from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import (
        VALUATION_POLICY_SHA256, build_generic_price_5td_valuation_v2,
    )
    if mode in {"holding_halt", "endpoint_halt", "resume_adjustment"}:
        index = 4 if mode == "endpoint_halt" else 2
        packet["prices"].loc[index, "suspended"] = True
        packet["prices"].loc[index, ["open", "high", "low", "close", "d_anchor_factor"]] = None
        if mode == "resume_adjustment":
            packet["prices"].loc[3:, ["open", "high", "low", "close", "up_limit", "down_limit"]] /= 2
            packet["prices"].loc[3:, "d_anchor_factor"] = 2.
    else:
        packet["prices"].loc[4, "down_limit"] = 10.8 if mode == "endpoint_limit" else None
    before = deepcopy(packet)
    legacy, _ = build_generic_price_5td_labels_v1(**packet)
    labels, receipt = build_generic_price_5td_valuation_v2(**packet)
    pd.testing.assert_frame_equal(labels.loc[:, legacy.columns], legacy)
    row = labels.iloc[0]
    assert row.label_status == "UNKNOWN" and row.valuation_status == "AVAILABLE"
    assert row.valuation_gross_terminal_ratio == pytest.approx(expected_ratio)
    assert row.mark_to_market_net_bps == pytest.approx(10000*(expected_ratio/1.000095-1))
    assert row.hypothetical_liquidation_net_bps == pytest.approx(
        10000*(expected_ratio*.999405/1.000095-1))
    assert row.exit_execution_status == exit_state
    assert not row.actual_fill_proven and pd.isna(row.realized_return_bps)
    assert row.valuation_policy_sha256 == VALUATION_POLICY_SHA256 != POLICY_SHA256
    assert receipt["legacy_execution_labels_unchanged"] and not receipt["activation_evidence"]
    assert row.label_information_end.date() == packet["calendar"][5]
    for field in ("candidates", "prices", "references"):
        pd.testing.assert_frame_equal(packet[field], before[field])


@pytest.mark.parametrize("mode", ["missing", "unknown_tradability", "unknown_coordinate"])
def test_v2_unexplained_gaps_are_not_carried_or_deleted(packet, mode):
    from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import (
        build_generic_price_5td_valuation_v2,
    )
    if mode == "missing":
        packet["prices"] = packet["prices"].drop(index=2)
    else:
        field = "tradability_unknown" if mode == "unknown_tradability" else "d_anchor_factor"
        packet["prices"].loc[2, field] = True if mode == "unknown_tradability" else None
    labels, _ = build_generic_price_5td_valuation_v2(**packet)
    assert len(labels) == 1 and labels.iloc[0].valuation_status == "UNKNOWN"
    assert labels.hypothetical_liquidation_net_bps.isna().all()
    assert labels.realized_return_bps.isna().all()
    assert labels.iloc[0].label_information_end.date() == packet["calendar"][5]


def test_v2_entry_unexecutable_is_cash_without_fake_exit_or_fees(packet):
    from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import (
        build_generic_price_5td_valuation_v2,
    )
    packet["prices"].loc[0, "suspended"] = True
    labels, _ = build_generic_price_5td_valuation_v2(**packet)
    row = labels.iloc[0]
    assert row.valuation_status == "CASH_ENTRY_NOT_EXECUTABLE"
    assert row.hypothetical_liquidation_net_bps == row.mark_to_market_net_bps == 0
    assert row.exit_execution_status == "NOT_HELD" and pd.isna(row.realized_return_bps)


def test_v2_ordinary_marks_match_v1_and_do_not_extend_empty_or_immature_schedule(packet):
    from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import (
        build_generic_price_5td_valuation_v2,
    )
    labels, _ = build_generic_price_5td_valuation_v2(**packet)
    assert labels.iloc[0].valuation_gross_terminal_ratio == labels.iloc[0].gross_terminal_ratio
    packet["calendar"] = packet["calendar"][:4]
    packet["prices"] = packet["prices"].iloc[:3]
    labels, _ = build_generic_price_5td_valuation_v2(**packet)
    assert labels.iloc[0].valuation_status == "IMMATURE"
    for field in ("candidates", "prices", "references"):
        packet[field] = packet[field].iloc[:0].astype(object)
    labels, receipt = build_generic_price_5td_valuation_v2(**packet)
    assert labels.empty and receipt["legacy_receipt"]["decision_dates"] == [packet["decision_dates"][0].isoformat()]


@pytest.mark.parametrize("case", ["ordinary", "unknown", "rank_mismatch", "group_mismatch"])
def test_v2_original_five_slots_and_policy_identity(packet, case):
    from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY
    from backend.services.advisory_model_first.generic_price_5td_revaluation_v2 import account_frozen_fixed5_valuations
    from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import build_generic_price_5td_valuation_v2
    if case == "unknown":
        packet["prices"] = packet["prices"].drop(index=2)
    labels, _ = build_generic_price_5td_valuation_v2(**packet)
    labels["package_id"] = "pkg_test"
    predictions = labels.loc[:, ["package_id", *KEY, "selection_effective_rank", "candidate_group_size", "policy_sha256"]].copy()
    predictions["status"] = "AVOID"
    if case in {"rank_mismatch", "group_mismatch"}:
        predictions["selection_effective_rank" if case == "rank_mismatch" else "candidate_group_size"] = 2
        with pytest.raises(ValueError, match="rank/group identity"):
            account_frozen_fixed5_valuations(valuations=labels, predictions=predictions,
                                            decision_dates=packet["decision_dates"])
        return
    days = packet["calendar"][:2]
    daily, _ = account_frozen_fixed5_valuations(valuations=labels, predictions=predictions, decision_dates=days)
    assert len(daily) == 2 and daily.iloc[1].original_slots == 0
    assert daily.iloc[1].baseline_valuation_bps == 0
    assert daily.iloc[0].model_valuation_bps == 0
    if case == "unknown":
        assert pd.isna(daily.iloc[0].baseline_valuation_bps) and pd.isna(daily.iloc[0].increment_bps)
    else:
        assert daily.iloc[0].baseline_valuation_bps == pytest.approx(labels.iloc[0].hypothetical_liquidation_net_bps/5)


@pytest.mark.parametrize("case", ["complete", "candidate_transfer", "matched_anchor", "unknown_arm",
    "duplicate_unit", "changed_family", "changed_reference", "changed_prediction_policy", "source_as_output"])
def test_v2_frozen_consumer_atomic_no_queries_no_fits_or_source_overwrite(packet, tmp_path, case):
    import json
    from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, file_sha256
    from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY
    from backend.services.advisory_model_first.generic_price_5td_revaluation_v2 import revalue_frozen_cross_package_fixed5_v2
    source, output = tmp_path/"source", tmp_path/"output"
    source.mkdir()
    packet["prices"].loc[2, "suspended"] = True
    model_id, model_sha = "a"*64, "b"*64
    arm = case if case in {"candidate_transfer", "matched_anchor", "unknown_arm"} else "candidate"
    def write(relative, value):
        path = source/relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(value, pd.DataFrame):
            value.to_parquet(path, index=False)
        else:
            path.write_bytes(_json_bytes(value))
        return file_sha256(path)
    calendar_sha = write("calendar.json", [str(day) for day in packet["calendar"]])
    plan_sha = write("plan.json", dict(calendar_sha256=calendar_sha,
        study_type="EXPLORATORY_SCREEN", decision_use="NAVIGATION_ONLY",
        decision_dates=[str(packet["decision_dates"][0])], settlement_cutoff=str(packet["calendar"][5])))
    roster = packet["candidates"].copy()
    for field in KEY[:2]:
        roster[field] = pd.to_datetime(roster[field])
    roster["package_id"] = "pkg_test"
    refs = packet["references"].copy()
    for field in KEY[:2]:
        refs[field] = pd.to_datetime(refs[field])
    refs["d_anchor_factor"] = 1.
    roster_sha = write("shared_daily/rosters.parquet", roster)
    reference_sha = write("shared_daily/references.parquet", refs)
    write("shared_daily/receipt.json", dict(plan_sha256=plan_sha,
        files={"rosters.parquet": roster_sha, "references.parquet": reference_sha}))
    quotes = packet["prices"].drop(columns=[KEY[0]]).rename(columns={
        **{name: "raw_"+name+"_cny" for name in ("open", "high", "low", "close")},
        "d_anchor_factor": "adj_factor"})
    quotes["trade_date"] = pd.to_datetime(quotes.trade_date)
    quote_sha = write("settlement_coordinates/quotes.parquet", quotes)
    write("settlement_coordinates/receipt.json", dict(quotes_sha256=quote_sha,
        until=str(packet["calendar"][5]), source_evidence="NON_VINTAGE"))
    legacy, _ = build_generic_price_5td_labels_v1(**packet)
    legacy["package_id"] = "pkg_test"
    label_sha = write("fixed5_settlement/labels.parquet", legacy)
    write("fixed5_settlement/receipt.json", dict(labels_sha256=label_sha))
    unit = dict(model_id=model_id, arm=arm, model_sha256=model_sha, family="frozen_test")
    spec_sha = write("fixed5_query_spec.json", dict(plan_sha256=plan_sha, policy_sha256=POLICY_SHA256,
        physical_fit_count=0, outcomes_read=False, units=[unit, unit] if case == "duplicate_unit" else [unit]))
    predictions = roster.copy()
    predictions["status"], predictions["model_sha256"] = "ACCEPTABLE", model_sha
    predictions["policy_sha256"] = "c"*64 if case == "changed_prediction_policy" else POLICY_SHA256
    prediction_sha = write(f"fixed5_predictions/{model_id}/{arm}/predictions.parquet", predictions)
    write(f"fixed5_predictions/{model_id}/{arm}/receipt.json", dict(spec_sha256=spec_sha,
        model_id=model_id, model_sha256=model_sha, arm=arm,
        family="changed" if case == "changed_family" else unit["family"], physical_fit_count=0,
        H_outcomes_used=False, original_policy_preserved=True, predictions_sha256=prediction_sha, rows=1))
    if case == "changed_reference":
        refs["reference_cny"] = 20.
        write("shared_daily/references.parquet", refs)
    before = {str(p.relative_to(source)): file_sha256(p) for p in source.rglob("*") if p.is_file()}
    if case not in {"complete", "candidate_transfer", "matched_anchor"}:
        with pytest.raises(ValueError):
            revalue_frozen_cross_package_fixed5_v2(source_root=source,
                output_root=source if case == "source_as_output" else output)
        assert not output.exists()
        return
    destination, result = revalue_frozen_cross_package_fixed5_v2(source_root=source, output_root=output)
    values = pd.read_parquet(destination/"valuations.parquet")
    assert values.iloc[0].valuation_status == "AVAILABLE" and values.iloc[0].label_status == "UNKNOWN"
    assert result["comparisons"][0]["valuation_paired_days"] == 1
    spec = json.loads((destination/"specification.json").read_text(encoding="utf-8"))
    assert spec["physical_fit_count"] == spec["new_predictions"] == spec["database_reads"] == 0
    assert not spec["activation_evidence"] and not spec["realized_profit_claimed"]
    assert revalue_frozen_cross_package_fixed5_v2(source_root=source, output_root=output)[0] == destination
    assert before == {str(p.relative_to(source)): file_sha256(p) for p in source.rglob("*") if p.is_file()}
