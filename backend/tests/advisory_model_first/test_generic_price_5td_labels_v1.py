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
