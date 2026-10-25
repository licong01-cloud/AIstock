"""Small frozen source reuse, immutable stages and complete non-NAV cohorts."""
from datetime import date
import json

import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY_SHA256, ROSTER
from backend.services.advisory_model_first.generic_price_5td_pipeline_v1 import (
    GenericPrice5TDPlanV1, evaluate_generic_price_5td_cohorts_v1, implementation_sha256,
    prepare_generic_price_5td_v1, preregister_generic_price_5td_v1, train_generic_price_5td_study_v1,
)
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_generic_price_5td_models_v1 import fitted_packet as fitted_packet


@pytest.fixture
def source_packet(tmp_path):
    days = pd.bdate_range("2024-01-02", periods=40)
    key = dict(decision_as_of_trade_date=days[20], target_trade_date=days[21], instrument="000001.SZ")
    roster = pd.DataFrame([{**key, "selection_effective_rank": 1, "candidate_group_size": 1}])
    raw = pd.DataFrame([dict(trade_date=day, instrument="000001.SZ",
        raw_open_cny=10., raw_high_cny=11., raw_low_cny=9.8, raw_close_cny=10.2,
        adj_factor=1., up_limit=12., down_limit=8., suspended=False, tradability_unknown=False) for day in days])
    volume = raw.loc[:, ["trade_date", "instrument"]].assign(volume_hand=100.)
    index = pd.DataFrame(dict(trade_date=days, instrument="000300.SH", close=4000.))
    paths = {}
    for name, frame in (("roster", roster), ("raw_daily", raw), ("volume_daily", volume), ("index_daily", index)):
        paths[name] = tmp_path/(name+".parquet")
        frame.to_parquet(paths[name], index=False)
    paths["calendar"] = tmp_path/"calendar.json"
    paths["calendar"].write_text(json.dumps([day.date().isoformat() for day in days]), encoding="utf-8")
    config = dict(train_start="2024-01-01", train_end="2024-03-01", validation_start="2024-03-04",
                  validation_end="2024-03-29", test_start="2024-04-01", test_end="2024-04-30", label_cutoff="2024-05-10")
    plan = GenericPrice5TDPlanV1(configuration=config, inputs={name: evidence_reference_for_file(path, role=name)
        for name, path in paths.items()}, dataset_identity="existing-frozen-development",
        parent_lineage=("existing-source",), decision_dates=(days[20].date(), days[21].date()),
        implementation_sha256=implementation_sha256(), source_evidence="NON_VINTAGE")
    return plan, tmp_path/"studies"


def test_prepare_reuses_complete_original_D_input_and_five_day_label_immutable(source_packet):
    plan, root = source_packet
    preregister_generic_price_5td_v1(plan=plan, output_root=root)
    prepared = prepare_generic_price_5td_v1(plan=plan, output_root=root)
    rows = pd.read_parquet(prepared/"rows.parquet")
    assert len(rows) == 1 and list(rows.loc[:, ROSTER].instrument) == ["000001.SZ"]
    assert rows.iloc[0].gross_terminal_ratio == pytest.approx(1.)
    assert rows.iloc[0].market_up_ratio != rows.iloc[0].market_up_ratio  # unknown original breadth definition
    assert rows.iloc[0].policy_sha256 == POLICY_SHA256
    receipt = json.loads((prepared/"receipt.json").read_text(encoding="utf-8"))
    assert not receipt["database_written"] and not receipt["selection_regenerated"]
    assert len(receipt["decision_dates"]) == 2
    assert prepare_generic_price_5td_v1(plan=plan, output_root=root) == prepared
    assert len((root/"trial_registry.jsonl").read_text(encoding="utf-8").splitlines()) == 2


def test_source_drift_and_qe_busy_fail_before_physical_fit(source_packet):
    plan, root = source_packet
    preregister_generic_price_5td_v1(plan=plan, output_root=root)
    prepare_generic_price_5td_v1(plan=plan, output_root=root)
    with pytest.raises(ValueError, match="QE idle"):
        train_generic_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=False)
    path = root/plan.experiment_id/"fit_attempt.json"
    assert not path.exists()
    Path = type(path)
    original = Path(plan.inputs["calendar"].artifact_uri)
    original.write_bytes(original.read_bytes()+b" ")
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
    with pytest.raises(AdvisoryModelFirstError, match="identity changed"):
        prepare_generic_price_5td_v1(plan=plan, output_root=root)


def test_partial_fit_has_durable_attempt_and_no_implicit_retry(source_packet, monkeypatch):
    plan, root = source_packet
    preregister_generic_price_5td_v1(plan=plan, output_root=root)
    prepare_generic_price_5td_v1(plan=plan, output_root=root)
    def failure(**kwargs):
        kwargs["before_fit"]("candidate_mean")
        raise ValueError("injected interruption")
    import backend.services.advisory_model_first.generic_price_5td_pipeline_v1 as module
    monkeypatch.setattr(module, "train_generic_price_5td_v1", failure)
    with pytest.raises(ValueError, match="interruption"):
        train_generic_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)
    study = root/plan.experiment_id
    journal = [json.loads(line) for line in (study/"fit_journal.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(journal) == 1 and journal[0]["kind"] == "PHYSICAL_FIT"
    with pytest.raises(FileExistsError):
        train_generic_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)


def test_complete_cohorts_no_compounding_unknown_dates_and_no_top6(fitted_packet):
    fitted, source, _ = fitted_packet
    rows = source.iloc[:6].copy()
    rows["selection_effective_rank"] = range(1, 7)
    rows.loc[0, "label_status"] = "UNKNOWN"
    result = evaluate_generic_price_5td_cohorts_v1(rows=rows, fitted=fitted,
        decision_dates=[rows.iloc[0].decision_as_of_trade_date.date(), date(2024, 2, 29)])
    assert len(result["cohorts"]) == 2 and result["cumulative_nav"] is None
    assert result["cohorts"][0]["baseline"]["net_bps"] is None
    assert result["cohorts"][0]["baseline"]["unsettled"] == 1
    assert result["cohorts"][1]["baseline"]["net_bps"] == 0
    assert result["cohorts"][0]["candidate"]["true_take"] <= 4
    assert not result["economic_confirmation"] and not result["deployable"]
