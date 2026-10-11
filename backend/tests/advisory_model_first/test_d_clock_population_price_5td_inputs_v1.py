"""D mark eligibility, missing-data semantics and one-variable sampling contract."""
from datetime import date
import hashlib
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import ARMS, FEATURES, KEY, MARK_POLICY_SHA256, ROSTER
from backend.services.advisory_model_first.d_clock_population_price_5td_inputs_v1 import build_d_clock_mark_labels_v1, build_training_encoding_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def mark_packet():
    calendar = list(pd.bdate_range("2025-02-03", periods=6).date)
    d, t = map(pd.Timestamp, calendar[:2])
    roster = pd.DataFrame([[d, t, "000001.SZ", 1, 1]], columns=ROSTER)
    prices = pd.DataFrame([dict(decision_as_of_trade_date=d, trade_date=pd.Timestamp(day), instrument="000001.SZ",
        open=10., high=11., low=10., close=10. if i == 0 else 10.5, d_anchor_factor=1.,
        suspended=False, tradability_unknown=False, up_limit=11., down_limit=9.) for i, day in enumerate(calendar)])
    references = roster.loc[:, KEY].assign(reference_cny=10., reference_visible_through=d.date())
    return dict(candidates=roster, decision_dates=[d.date()], calendar=calendar, prices=prices, references=references,
        cutoff=calendar[-1], source_context=dict(calendar_sha256=sha([v.isoformat() for v in calendar]),
            prices_sha256=hashlib.sha256(parquet_bytes(prices)).hexdigest(),
            references_sha256=hashlib.sha256(parquet_bytes(references)).hexdigest(),
            label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence="CURRENT_DATABASE_NON_VINTAGE"))


def mark_labels(packet):
    packet["source_context"]["prices_sha256"] = hashlib.sha256(parquet_bytes(packet["prices"])).hexdigest()
    packet["source_context"]["references_sha256"] = hashlib.sha256(parquet_bytes(packet["references"])).hexdigest()
    return build_d_clock_mark_labels_v1(**packet)


@pytest.mark.parametrize("entry", ["up_limit", "suspended", "missing_open"])
def test_future_entry_does_not_control_mark_target(entry):
    packet = mark_packet()
    p = packet["prices"]
    if entry == "up_limit":
        p.loc[1, ["open", "high", "low", "close"]] = [11., 11., 10., 10.5]
    elif entry == "suspended":
        p.loc[1, "suspended"] = True
        p.loc[1, ["open", "high", "low", "close"]] = np.nan
    else:
        p.loc[1, "open"] = np.nan
    labels, receipt = mark_labels(packet)
    row = labels.iloc[0]
    assert row.mark_label_status == "AVAILABLE" and row.terminal_mark_ratio == 1.05
    assert row.path_mark_min_ratio == 1. and not row.actual_fill_proven
    assert row.carried_mark_sessions == (entry == "suspended")
    assert receipt["native_receipt_recovered"] is False


@pytest.mark.parametrize("unknown", ["missing_bar", "bridge", "suspension"])
def test_unknown_path_remains_an_original_unknown_row(unknown):
    packet = mark_packet()
    if unknown == "missing_bar":
        packet["prices"] = packet["prices"].drop(index=2)
    elif unknown == "bridge":
        packet["prices"].loc[2, "d_anchor_factor"] = np.nan
    else:
        packet["prices"].loc[2, "tradability_unknown"] = True
    labels, _ = mark_labels(packet)
    assert len(labels) == 1 and labels.iloc[0].mark_label_status == "UNKNOWN"
    assert pd.isna(labels.iloc[0].terminal_mark_ratio)


def test_maturity_reference_clock_and_endpoint_execution_are_separate():
    packet = mark_packet()
    packet["prices"].loc[5, "down_limit"] = 10.5
    labels, _ = mark_labels(packet)
    assert labels.iloc[0].endpoint_execution_status == "EXIT_UNPROVEN_LIMIT_DOWN"
    packet["cutoff"] = packet["calendar"][-2]
    packet["prices"] = packet["prices"].iloc[:-1]
    labels, _ = mark_labels(packet)
    assert labels.iloc[0].mark_label_status == "IMMATURE"
    packet = mark_packet()
    packet["references"]["reference_visible_through"] = packet["calendar"][1]
    with pytest.raises(AdvisoryModelFirstError, match="future visibility"):
        mark_labels(packet)
    packet = mark_packet()
    packet["source_context"]["prices_sha256"] = "0"*64
    with pytest.raises(AdvisoryModelFirstError, match="source digest"):
        build_d_clock_mark_labels_v1(**packet)


def synthetic_training_rows():
    plan = SimpleNamespace(train_start=date(2024, 7, 4), train_end=date(2025, 5, 30),
        estimation_first_D=date(2024, 12, 24), evaluation_start=date(2025, 6, 3), evaluation_end=date(2025, 9, 30))
    records = []
    for i, d in enumerate(pd.bdate_range("2024-11-01", "2025-02-03")):
        for stock in range(6):
            t, h, symbol = d+pd.offsets.BDay(1), d+pd.offsets.BDay(5), f"{stock+1:06d}.SZ"
            for package in ("a", "b"):
                records.append(dict(package_id=package, decision_as_of_trade_date=d, target_trade_date=t,
                    instrument=symbol, label_information_end=h, reference_cny=10.,
                    terminal_mark_ratio=1.04+.01*stock, path_mark_min_ratio=.94+.005*stock,
                    mark_label_status="AVAILABLE", mark_policy_sha256=MARK_POLICY_SHA256,
                    observed_gap_bps=np.nan if stock == 5 else 0., legacy_valuation_status="CASH_ENTRY_NOT_EXECUTABLE" if stock == 5 else "AVAILABLE",
                    legacy_valuation_policy_sha256=VALUATION_POLICY_SHA256,
                    label_cluster=sha([d.date().isoformat(), t.date().isoformat(), symbol, MARK_POLICY_SHA256]),
                    **{n: float(stock)/10+float(i % 5)/100 for n in FEATURES}))
    return pd.DataFrame(records), plan


def test_common_medians_deduplicate_and_ignore_future_episode_and_held_poison():
    rows, plan = synthetic_training_rows()
    encoding = build_training_encoding_v1(rows=rows, plan=plan)
    assert encoding["contrast_status"] == "PREPARED_IDENTIFIABLE_NO_FIT"
    assert encoding["pools"][ARMS[0]]["STRUCTURE"] > encoding["pools"][ARMS[1]]["STRUCTURE"]
    held = rows.iloc[:2].copy()
    held[KEY[0]], held["label_information_end"] = pd.Timestamp("2025-06-03"), pd.Timestamp("2025-06-10")
    held[list(FEATURES)] = "unreadable evaluation finance"
    held["terminal_mark_ratio"] = "unreadable target"
    poisoned = pd.concat([rows, held], ignore_index=True)
    actual = build_training_encoding_v1(rows=poisoned, plan=plan)
    assert actual == encoding
    rows.loc[rows.package_id.eq("b") & rows.instrument.eq("000001.SZ"), "ret_1"] = np.nan
    with pytest.raises(AdvisoryModelFirstError, match="contradictory"):
        build_training_encoding_v1(rows=rows, plan=plan)


def test_identical_post_purge_populations_have_zero_fit_contract():
    rows, plan = synthetic_training_rows()
    rows["observed_gap_bps"], rows["legacy_valuation_status"] = 0., "AVAILABLE"
    encoding = build_training_encoding_v1(rows=rows, plan=plan)
    assert encoding["contrast_status"] == "NOT_IDENTIFIABLE_SAMPLING_CONTRAST"
    assert encoding["physical_fit_count"] == 0


def test_parent_projection_never_decodes_finance_and_known_hash_change_is_rejected(tmp_path):
    import json
    from backend.services.advisory_model_first.d_clock_population_price_5td_inputs_v1 import verified_parent_rosters_v1
    from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, file_sha256, publish_stage
    packet = mark_packet()
    rows = pd.concat([packet["candidates"].assign(package_id=p, manifest_sha256=p*64, run_id=p, source_id=p)
        for p in ("a", "b")], ignore_index=True)
    rows["hypothetical_liquidation_net_bps"] = "opaque forbidden outcome column"
    days = pd.DataFrame([dict(package_id=p, decision_date=pd.Timestamp(packet["calendar"][0]), target_date=pd.Timestamp(packet["calendar"][1]),
        horizon_end=pd.Timestamp(packet["calendar"][5]), original_candidates=1, source_status="KNOWN_ROSTER") for p in ("a", "b")])
    parent = publish_stage(study_root=tmp_path, stage="prepared", plan_sha256="a"*64, parent_sha256="b"*64,
        artifacts={"rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days),
            "calendar.json": _json_bytes([d.isoformat() for d in packet["calendar"]])})
    manifest = json.loads((parent/"manifest.json").read_bytes())
    def ref(name):
        p = parent/name
        return SimpleNamespace(artifact_uri=str(p), sha256=file_sha256(p), size_bytes=p.stat().st_size)
    plan = SimpleNamespace(parent_manifest_ref=ref("manifest.json"), parent_rows_ref=ref("rows.parquet"),
        parent_days_ref=ref("days.parquet"), calendar_ref=ref("calendar.json"), parent_plan_sha256="a"*64,
        parent_stage_sha256=manifest["stage_sha256"], train_start=packet["calendar"][0], train_end=packet["calendar"][0],
        evaluation_start=packet["calendar"][0], evaluation_end=packet["calendar"][0],
        sources=[SimpleNamespace(package_id=p, manifest_sha256=p*64, run_id=p, source_id=p) for p in ("a", "b")])
    actual, _, _ = verified_parent_rosters_v1(plan)
    assert "hypothetical_liquidation_net_bps" not in actual and len(actual) == 2
    plan.parent_rows_ref.sha256 = "f"*64
    with pytest.raises(AdvisoryModelFirstError, match="reference changed"):
        verified_parent_rosters_v1(plan)
