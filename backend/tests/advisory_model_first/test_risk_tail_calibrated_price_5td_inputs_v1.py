"""Small synthetic consumer contracts; no database or real experiment inputs."""
from datetime import date
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import RiskTailStudyPlanV1
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import (
    BASE_FINANCE, CAL_FINANCE, ROSTER_KEY, checked_source, clocks_v1, prepare_risk_tail_inputs_v1, read_projection_v1,
)
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_pipeline_v1 import implementation_sha256


def source_case(root, *, poison=False, missing_cal_day=False):
    calendar = pd.bdate_range("2024-07-04", "2025-11-05")
    sources = [dict(source_id=f"source_{b}", package_id=f"package_{b}", manifest_sha256=str(b+1)*64,
        run_id=f"run_{b}", context_bit=b, prediction_source=dict(package_id=f"package_{b}",
            manifest_sha256=str(b+1)*64, run_id=f"run_{b}", status="FROZEN_SCORE_AVAILABLE",
            descriptor=dict(sha256=str(b+3)*64, size_bytes=100))) for b in (0, 1)]
    fields = dict(sources=sources, train_start=date(2024, 7, 4), train_end=date(2025, 5, 30),
        evaluation_start=date(2025, 6, 3), evaluation_end=date(2025, 9, 30), source_commit="a"*40,
        implementation_sha256=implementation_sha256(), node_python_uri="/existing/unit/python",
        library_versions={n: "unit-only" for n in ("numpy", "pandas", "scikit-learn", "pyarrow", "pydantic")},
        qe_api_base="http://127.0.0.1:8001")
    from types import SimpleNamespace
    clocks = clocks_v1(SimpleNamespace(**fields), calendar)
    dates = calendar[(calendar >= pd.Timestamp(fields["train_start"])) & (calendar <= pd.Timestamp(fields["evaluation_end"]))]
    rows, days = [], []
    for source in sources:
        for d in dates:
            i = calendar.get_loc(d)
            t, h = calendar[i+1], calendar[i+5]
            missing = missing_cal_day and source["context_bit"] == 1 and d == pd.Timestamp(clocks["calibration_dates"][0])
            count = 0 if missing else 6
            days.append(dict(package_id=source["package_id"], decision_date=d, target_date=t, horizon_end=h,
                original_candidates=count, source_status="UNKNOWN_ROSTER" if missing else "KNOWN_ROSTER"))
            for rank in range(1, count+1):
                path = .985 if d.date().isoformat() in clocks["calibration_dates"] else .88
                y = .98 if d >= pd.Timestamp(fields["evaluation_start"]) else 1.02
                mature = h <= pd.Timestamp(fields["evaluation_end"])
                raw = y/(1+POLICY["buy_bps"]/10000)
                values = dict(zip(FEATURES, [.01, .02, .03, .02, .01, .01, .5, 1., np.nan], strict=True))
                if poison and d >= pd.Timestamp(clocks["calibration_first_D"]):
                    y, path = np.inf, np.inf
                rows.append(dict(package_id=source["package_id"], manifest_sha256=source["manifest_sha256"],
                    run_id=source["run_id"], **dict(zip(KEY, [d, t, f"stock_{rank}"], strict=True)),
                    selection_effective_rank=rank, candidate_group_size=count, parent_score="never decode this field",
                    **values, observed_gap_bps=0., label_information_end=h, valuation_policy_sha256=VALUATION_POLICY_SHA256,
                    valuation_status="AVAILABLE" if mature else "IMMATURE", valuation_gross_terminal_ratio=y,
                    valuation_path_min_ratio=path, hypothetical_liquidation_net_bps=10000*(raw*(1-POLICY["sell_bps"]/10000)-1) if mature else np.nan,
                    mark_to_market_net_bps=10000*(raw-1) if mature else np.nan,
                    label_status="AVAILABLE" if mature else "IMMATURE", exit_execution_status="UNKNOWN_EXECUTION"))
    rows, days = pd.DataFrame(rows), pd.DataFrame(days)
    original = publish_stage(study_root=root/"source"/"old", stage="prepared", plan_sha256="f"*64,
        parent_sha256="e"*64, artifacts={"rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days),
            "calendar.json": _json_bytes([d.isoformat() for d in calendar])})
    header = json.loads((original/"manifest.json").read_bytes())
    fields["source_prepared"] = dict(artifact_uri=str(original), plan_sha256="f"*64, stage_sha256=header["stage_sha256"])
    plan = RiskTailStudyPlanV1(**fields)
    return plan, prepare_risk_tail_inputs_v1(plan=plan), rows


@pytest.fixture(scope="session")
def synthetic_study(tmp_path_factory):
    return source_case(tmp_path_factory.mktemp("risk_synthetic"))


def test_projection_three_pools_and_poisoned_future(tmp_path):
    plan, prepared, _ = source_case(tmp_path, poison=True)
    base, cal, enc = (prepared[n] for n in ("base", "calibration_roster", "encoding"))
    assert set(base.pool) == {"STRUCTURE", "ESTIMATION"}
    assert base.label_information_end.max() < pd.Timestamp(enc["calibration_first_D"])
    assert base.loc[base.pool.eq("STRUCTURE"), "label_information_end"].max() < pd.Timestamp(enc["estimation_first_D"])
    assert cal[KEY[0]].nunique() == 60 and cal.selection_effective_rank.max() == 5
    assert "parent_score" not in base and set(BASE_FINANCE).issubset(base)
    assert base.valuation_gross_terminal_ratio.max() == 1.02
    assert np.allclose(base.groupby("label_cluster").cluster_mass.sum(), 1.)
    assert cal.original_weight.sum() == pytest.approx(1.)
    with pytest.raises(AdvisoryModelFirstError, match="numeric value"):
        from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import validate_finance_v1
        validate_finance_v1(read_projection_v1(plan=plan, dates=enc["calibration_dates"], columns=CAL_FINANCE, top5=True),
            enc["calendar"], CAL_FINANCE)


def test_original_missing_day_mass_is_not_redistributed(tmp_path):
    _, prepared, _ = source_case(tmp_path, missing_cal_day=True)
    cal = prepared["calibration_roster"]
    first = pd.Timestamp(prepared["encoding"]["calibration_first_D"])
    assert len(cal.loc[cal[KEY[0]].eq(first)]) == 5
    assert cal.loc[cal[KEY[0]].eq(first), "original_weight"].isna().all()
    assert cal.original_weight.sum() == pytest.approx(59/60)
    assert prepared["days"].source_status.eq("UNKNOWN_ROSTER").sum() == 1


@pytest.mark.parametrize("change", ["source_hash", "rank", "duplicate", "day"])
def test_original_source_identity_and_population_are_fail_closed(synthetic_study, change):
    plan, prepared, _ = synthetic_study
    from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import validate_roster_v1
    rows, days = prepared["roster"].copy(), prepared["days"].copy()
    if change == "source_hash":
        with pytest.raises(ValueError, match="identity"):
            checked_source(plan.model_copy(update={"source_prepared": plan.source_prepared.model_copy(update={"stage_sha256": "0"*64})}))
        return
    if change == "rank":
        rows.loc[0, "selection_effective_rank"] = 6
    elif change == "duplicate":
        rows = pd.concat([rows, rows.iloc[:1]], ignore_index=True)
    else:
        days = days.iloc[1:]
    with pytest.raises(ValueError, match="original|identity"):
        validate_roster_v1(rows, days, plan, prepared["calendar"])
    assert not rows.empty and set(ROSTER_KEY).issubset(rows)
