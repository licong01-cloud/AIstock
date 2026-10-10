"""Synthetic-only time/identity contracts; no actual study, DB or financial source."""
from datetime import date
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_contracts_v1 import (
    FEATURES, KEY, POLICY, VALUATION_POLICY_SHA256, OriginalSlotHurdleStudyPlanV1,
)
from backend.services.advisory_model_first import original_slot_hurdle_price_5td_inputs_v1 as inputs
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_pipeline_v1 import implementation_sha256
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes


def source_case(root, *, poison=False):
    calendar = pd.bdate_range("2024-07-04", "2026-02-10")
    calendar = calendar[calendar.weekday != 2]  # Synthetic sessions, not the real calendar.
    sources = [dict(source_id=f"source_{b}", package_id=f"package_{b}", manifest_sha256=str(b+1)*64,
        run_id=f"run_{b}", context_bit=b, prediction_source=dict(package_id=f"package_{b}",
            manifest_sha256=str(b+1)*64, run_id=f"run_{b}", status="FROZEN_SCORE_AVAILABLE",
            descriptor=dict(sha256=str(b+3)*64, size_bytes=100))) for b in (0, 1)]
    fields = dict(sources=sources, train_start=date(2024, 7, 4), train_end=date(2025, 5, 30),
        evaluation_start=date(2025, 6, 3), evaluation_end=date(2025, 9, 30), source_commit="a"*40,
        implementation_sha256=implementation_sha256(), node_python_uri="/existing/unit/python",
        library_versions={n: "synthetic-only" for n in ("numpy", "pandas", "scikit-learn", "scipy", "pyarrow", "pydantic")},
        temporary_root_uri="X:/AIstock_temp/advisory_original_slot_hurdle_source_20261011/unit_temp",
        qe_api_base="http://127.0.0.1:8001")
    dates = calendar[calendar <= pd.Timestamp(fields["evaluation_end"])]
    d, t, h = (np.repeat(v, 6) for v in (dates, calendar[1:len(dates)+1], calendar[5:len(dates)+5]))
    i, rank = np.repeat(np.arange(len(dates)), 6), np.tile(np.arange(1, 7), len(dates))
    rng = np.random.default_rng(20261011)
    r = np.where(rng.random(len(d)) < .55, rng.gamma(3., 150., len(d)), -10000*rng.beta(2., 9., len(d)))
    r = np.where(d >= np.datetime64(fields["evaluation_start"]), np.where(rank < 4, 800., -2000.), r)
    g = (-200+((i+rank) % 7)*80).astype(float)
    y = (1+r/10000)*(1+g/10000)*(1+POLICY["buy_bps"]/10000)/(1-POLICY["sell_bps"]/10000)
    outside = (h > np.datetime64(fields["train_end"])) | (rank > 5)
    values = dict(zip(FEATURES, [.005*np.sin(i), .01*rank, .02, .03, np.where(i % 2, .01, -.01),
        .01*rank, .5, 1.+rank/20, np.nan], strict=True))
    base = pd.DataFrame({**dict(zip(KEY, [d, t, [f"stock_{v}" for v in rank]], strict=True)), **values,
        "selection_effective_rank": rank, "candidate_group_size": 6, "parent_score": "FORBIDDEN_SCORE_COLUMN",
        "observed_gap_bps": g, "valuation_status": "AVAILABLE", "valuation_policy_sha256": VALUATION_POLICY_SHA256,
        "label_information_end": np.where(poison & outside, np.datetime64("1900-01-01"), h),
        "valuation_gross_terminal_ratio": np.where(poison & outside, np.inf, y),
        "valuation_path_min_ratio": np.where(poison & outside, np.inf, y*.95),
        "hypothetical_liquidation_net_bps": np.where(poison & outside, np.inf, r),
        "mark_to_market_net_bps": 10000*(y/((1+g/10000)*(1+POLICY["buy_bps"]/10000))-1),
        "label_status": "AVAILABLE", "exit_execution_status": "UNKNOWN_EXECUTION"})
    schedule = pd.DataFrame(dict(decision_date=dates, target_date=calendar[1:len(dates)+1], horizon_end=calendar[5:len(dates)+5],
        original_candidates=6, source_status="KNOWN_ROSTER"))
    rows = pd.concat([base.assign(**{n: s[n] for n in ("package_id", "manifest_sha256", "run_id")}) for s in sources], ignore_index=True)
    days = pd.concat([schedule.assign(package_id=s["package_id"]) for s in sources], ignore_index=True)
    source_root = publish_stage(study_root=root/"source", stage="prepared", plan_sha256="f"*64, parent_sha256="e"*64,
        artifacts={"rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days),
            "calendar.json": _json_bytes([d.date().isoformat() for d in calendar])})
    header = json.loads((source_root/"manifest.json").read_bytes())
    fields["source_prepared"] = dict(artifact_uri=str(source_root), plan_sha256="f"*64,
        parent_sha256="e"*64, stage_sha256=header["stage_sha256"], files=header["files"])
    plan = OriginalSlotHurdleStudyPlanV1(**fields)
    return plan, inputs.prepare_hurdle_inputs_v1(plan=plan), rows


@pytest.fixture(scope="session")
def prepared_study(tmp_path_factory):
    return source_case(tmp_path_factory.mktemp("hurdle_source"))


def test_top5_calendar_projection_never_decodes_poisoned_future(tmp_path, monkeypatch):
    calls, original = [], inputs.read_projection_v1
    def observed(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)
    monkeypatch.setattr(inputs, "read_projection_v1", observed)
    plan, prepared, rows = source_case(tmp_path, poison=True)
    assert len(prepared["roster"]) == len(rows) and "parent_score" not in prepared["roster"]
    assert not set(inputs.TRAIN_FINANCE).intersection(prepared["roster"])
    assert prepared["domain"].selection_effective_rank.max() == 5
    assert prepared["domain"].label_information_end.max() <= pd.Timestamp(plan.train_end)
    assert np.isfinite(prepared["domain"].net_return_bps).all()
    financial = [v for v in calls if "valuation_gross_terminal_ratio" in v["columns"]]
    assert len(financial) == 1 and financial[0]["top5"]
    assert set(financial[0]["dates"]) == set(prepared["encoding"]["clocks"]["train_dates"])


def test_unique_cluster_mass_and_label_independent_encoding(prepared_study):
    _, prepared, _ = prepared_study
    domain, encoding = prepared["domain"], prepared["encoding"]
    assert domain.groupby("label_cluster").cluster_mass.sum().eq(1).all()
    assert domain.cluster_mass.eq(.5).all()
    assert encoding["preprocessing_unique_clusters"] == domain.label_cluster.nunique()
    assert encoding["label_based_preprocessing"] is False
    assert "market_up_ratio" in encoding["entirely_unknown_features"]
    altered = domain.copy()
    altered["net_return_bps"] = np.nan
    from backend.services.advisory_model_first.original_slot_hurdle_price_5td_model_v1 import fit_encoding_v1
    plain = fit_encoding_v1(altered, encoding["intervals_bps"])
    assert plain["numeric_mean"] == encoding["numeric_mean"] and plain["medians"] == encoding["medians"]


@pytest.mark.parametrize("corruption", ["duplicate", "rank"])
def test_roster_identity_contradictions_fail_closed(prepared_study, corruption):
    plan, p, _ = prepared_study
    rows, days = p["roster"].copy(), p["days"].copy()
    if corruption == "duplicate":
        rows = pd.concat([rows, rows.iloc[[0]]], ignore_index=True)
    elif corruption == "rank":
        rows.loc[0, "selection_effective_rank"] = 50
    with pytest.raises(ValueError):
        inputs.validate_roster_v1(rows, days, plan, p["calendar"])


def test_financial_clock_and_cluster_contradictions(prepared_study):
    _, p, _ = prepared_study
    for column, value in (("label_information_end", pd.Timestamp("1900-01-01")),
            ("valuation_gross_terminal_ratio", 999.), ("valuation_policy_sha256", "0"*64)):
        rows = p["domain"].copy()
        rows.loc[rows.index[0], column] = value
        with pytest.raises(ValueError):
            inputs.validate_finance_v1(rows, p["calendar"])


def test_source_hash_and_forbidden_projection(prepared_study, tmp_path):
    plan, _, _ = prepared_study
    with pytest.raises(ValueError, match="parent score"):
        inputs.read_projection_v1(plan=plan, dates=[plan.train_start], columns=("parent_score",))
    for field, value in (("temporary_root_uri", "C:/temp"), ("evaluation_end", "2025-10-20")):
        with pytest.raises(ValueError):
            OriginalSlotHurdleStudyPlanV1.model_validate({**plan.model_dump(mode="json"), field: value})
    for dates, fields, top5, message in ((["2025-10-09"], ("valuation_gross_terminal_ratio",), True, "sealed"),
            ([plan.train_end], ("valuation_gross_terminal_ratio",), True, "immature"),
            ([plan.train_start], ("observed_gap_bps",), False, "Top5"),
            ([plan.evaluation_start], ("valuation_gross_terminal_ratio",), True, "frozen forecasts")):
        with pytest.raises(ValueError, match=message):
            inputs.read_projection_v1(plan=plan, dates=dates, columns=fields, top5=top5)
    plan2, _, _ = source_case(tmp_path)
    target = inputs.node_path(plan2.source_prepared.artifact_uri)/"calendar.json"
    target.write_bytes(target.read_bytes()+b" ")
    with pytest.raises(Exception, match="hash mismatch"):
        inputs.checked_source(plan2)
