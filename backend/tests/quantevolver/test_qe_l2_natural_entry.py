"""Contract-level money, identity and real reader checks for natural new buys."""

# ruff: noqa: E402 -- use the same worker import root as node execution.
from copy import deepcopy
from pathlib import Path
import sys
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import qe_l2_natural_entry_admission as admission
from qe_l2_natural_entry_consumer import NaturalEntryInputs, adjust_natural_entry_budgets
from backend.services.quantevolver.l2_natural_entry_consumer import (
    apply_natural_entry,
    bind_natural_entry_hook,
    extract_natural_entry,
)
from backend.tests.quantevolver.test_qe_l2_relative import data as relative_data  # noqa: F401
from backend.tests.quantevolver.qe_l2_continuous_fixture import _write


def context(stocks, *, day="2026-09-30", prior="2026-09-29"):
    rows = {
        str(i): {
            "sector_code": str(i),
            "trade_date": day,
            "as_of_date": prior,
            "availability": "available",
            "reason_code": None,
            "expected_loss": i / 130,
        }
        for i in range(131)
    }
    return {"sector_rows": rows, "stock_sectors": stocks}


def adjust(base, ordered, risk, *, cap=10_000_000.0):
    return adjust_natural_entry_budgets(
        "2026-09-30",
        "2026-09-29",
        ordered,
        list(base),
        base,
        100_000_000.0,
        {"max_weight": 0.05, "max_single_order_value": cap},
        risk,
    )


def test_money_oracle_and_groups_use_entire_target_list():
    # held names determine group boundaries; J must not be split in halves.
    ordered = ["a", "held1", "b", "held2", "c", "held3"]
    base = {"a": 3_000_000.0, "b": 1_000_000.0, "c": 2_000_000.0}
    risk = context({"a": "0", "b": "130", "c": "100"})
    before = deepcopy((base, ordered, risk))
    result, audit = adjust(base, ordered, risk)
    assert result == pytest.approx({"a": 3_075_000.0, "b": 925_000.0, "c": 2_000_000.0})
    assert [g["new_buys"] for g in audit["groups"]] == [["a", "b"], ["c"]]
    assert audit["groups"][0]["adjusted_budget_cny"] == pytest.approx(4_000_000.0, abs=1e-4)
    assert (base, ordered, risk) == before and len(audit["risk_inputs"]) == 131
    assert adjust(base, ordered, None)[0] == base


@pytest.mark.parametrize(
    "base,order,sectors",
    [
        ({}, [], {}),
        ({"a": 0.0}, ["a"], {"a": "0"}),
        ({"a": 2e6}, ["a"], {"a": "130"}),
        ({"a": 1e6, "b": 2e6}, ["a", "b", "held"], {"a": "10", "b": "10"}),
    ],
)
def test_degenerate_cases_are_identity(base, order, sectors):
    assert adjust(base, order, context(sectors))[0] == base


@pytest.mark.parametrize(
    "fault",
    ["missing_sector", "na", "nonfinite", "wrong_clock", "no_mapping", "negative", "extra_buy", "cap", "single_cap"],
)
def test_invalid_inputs_fail_without_clipping_or_safe_defaults(fault):
    base = {"a": 3e6, "b": 1e6}
    ordered = ["a", "b", "held"]
    risk = context({"a": "0", "b": "130"})
    cap = 1e7
    if fault == "missing_sector":
        risk["sector_rows"].pop("50")
    if fault == "na":
        risk["sector_rows"]["50"].update(availability="unavailable", expected_loss=None, reason_code="missing")
    if fault == "nonfinite":
        risk["sector_rows"]["50"]["expected_loss"] = float("nan")
    if fault == "wrong_clock":
        risk["sector_rows"]["50"]["as_of_date"] = "2026-09-30"
    if fault == "no_mapping":
        risk["stock_sectors"].pop("a")
    if fault == "negative":
        base["a"] = -1.0
    if fault == "extra_buy":
        ordered.remove("a")
    if fault == "cap":
        base["a"] = 5e6
    if fault == "single_cap":
        cap = 3e6
    with pytest.raises(ValueError):
        adjust(base, ordered, risk, cap=cap)


def test_tied_sector_losses_use_average_rank_and_empty_buys_still_check_131():
    risk = context({"a": "0", "b": "1"})
    risk["sector_rows"]["0"]["expected_loss"] = risk["sector_rows"]["1"]["expected_loss"]
    _, audit = adjust({"a": 1e6, "b": 1e6}, ["a", "b", "held"], risk)
    assert audit["stock_ranks"] == {"a": 0.5 / 130, "b": 0.5 / 130}
    empty = context({})
    empty["sector_rows"].pop("1")
    with pytest.raises(ValueError, match="FULL_POPULATION"):
        adjust({}, [], empty)


@pytest.fixture
def data(relative_data, monkeypatch):  # noqa: F811 -- pytest fixture injection
    d = relative_data
    m = d.manifest
    m.update(
        contract=admission.CONTRACT,
        schema_version=admission.VERSION,
        arm="R-entry",
        base_source_sha256=admission.BASE_SOURCE_SHA,
    )
    record = deepcopy(d.relative_record)
    record["base_source_sha256"] = admission.BASE_SOURCE_SHA
    monkeypatch.setattr(admission, "ADMISSIONS", {m["asset_admission_version"]: record})
    monkeypatch.setattr(admission, "validate_projection", lambda *a, **k: None)
    _write(d.root / "natural.json", m)
    return d


def test_full_reader_end_date_and_b0_never_reads_loss(data):
    inputs = NaturalEntryInputs(data.root / "natural.json")
    assert len(inputs.as_of) == 125 and len(inputs.risk) == 16375
    assert inputs.as_of["2026-09-30"] == "2026-09-29"
    assert len(inputs.risk_context("2026-09-30", [])["sector_rows"]) == 131
    with pytest.raises(ValueError, match="MAPPING_MISSING"):
        inputs.risk_context("2026-09-30", ["unknown"])
    data.manifest["arm"] = "B0"
    data.manifest["assets"]["loss"]["path"] = str(data.root / "must_not_be_opened.json")
    _write(data.root / "natural.json", data.manifest)
    control = NaturalEntryInputs(data.root / "natural.json")
    assert control.risk == {} and control.risk_context("2026-09-30", ["unknown"]) is None


def test_independent_admission_forbids_old_schedule_seed_or_source_drift(data):
    admission.validate_admission(data.manifest)
    for change in [
        {"arm": "R"},
        {"seed": 2024},
        {"kappa": 0.2},
        {"base_source_sha256": "0" * 64},
        {"target_source": {}},
    ]:
        with pytest.raises(ValueError):
            admission.validate_admission({**data.manifest, **change})


def test_noop_default_and_all_overlay_combinations(data):
    original = {"unchanged": object()}
    assert apply_natural_entry(original, None, scripts_dir=ROOT / "scripts") is original
    assert extract_natural_entry({"ordinary": 1}) == ({"ordinary": 1}, None)
    for key in [
        "qe_terminal_replay",
        "qe_l2_relative_manifest",
        "qe_l2_continuous_manifest",
        "qe_l2_study_manifest",
        "qe_stock_probability_study",
        "qe_l2_risk_enabled",
        "enable_sector_hmm",
        "enable_hmm_risk_gate",
        "sector_risk_overlay_enabled",
    ]:
        with pytest.raises(ValueError, match="OTHER_OVERLAY"):
            extract_natural_entry({"qe_l2_natural_entry_manifest": data.manifest, key: {"enabled": True}})


def test_only_reviewed_source_and_budget_sites_are_adapted():
    source = (ROOT / "scripts/score_weighted_strategy_v2_deterministic_v1.py").read_text(encoding="utf-8")
    adapted, receipt = bind_natural_entry_hook(source)
    compile(adapted, "custom_strategy.py", "exec")
    assert receipt["base_ast_restored"] and receipt["new_order_generators"] == 0
    assert adapted.index("_qe_entry_budgets =") < adapted.index("for sid in actual_buys:")
    with pytest.raises(ValueError, match="BASE_SOURCE_DRIFT"):
        bind_natural_entry_hook(source + "\n# change")


def test_never_trains_or_rebinds_prediction():
    import asyncio
    from backend.tests.unified_engine.test_qe_prediction_replay import _config, _context, _executor, _source
    from backend.services.quantevolver.executors.backtest import BacktestMode

    cfg = _config()
    cfg.model_params_base = {"qe_l2_natural_entry_manifest": {"assets": {"prediction": {"sha256": "wrong"}}}}
    for mode, error in [
        (BacktestMode.FULL_TRAIN, "REQUIRES_PREDICTION_REPLAY"),
        (BacktestMode.PREDICTION_REPLAY, "SOURCE_PREDICTION_MISMATCH"),
    ]:
        executor, coordinator = _executor()
        with pytest.raises(ValueError, match=error):
            asyncio.run(executor.submit(cfg, _context(_source()), mode=mode))
        executor.composer.compose_experiment_in_memory.assert_not_called()
        assert coordinator.payload is None
