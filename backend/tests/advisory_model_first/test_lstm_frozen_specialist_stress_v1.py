"""Current specialist, fixed consumed axis, and explicit no-activation decisions."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_frozen_specialist_stress_v1 as app
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_lstm_complementarity_v1 import example


@pytest.fixture
def trial():
    frame, inner, _ = example()
    days = [*pd.bdate_range(app.prior.FIRST, periods=117), pd.Timestamp(app.prior.LAST)]
    frames = []
    for day in days:
        part = frame.iloc[:50].copy()
        part[app.DAY], part["target_trade_date"] = day, day + pd.offsets.BDay(1)
        part["label_information_end"] = day + pd.offsets.BDay(5)
        frames.append(part)
    inputs = pd.concat(frames, ignore_index=True)
    inner["cutoff"] = app.CONTRACT["cutoff"]
    inner["model_payload_sha256"] = app.spo.sha({k: v for k, v in inner.items() if k != "model_payload_sha256"})
    packet = dict(schema=app.specialist.CONTRACT["schema"], contract_sha256=app.spo.sha(app.specialist.CONTRACT),
        target=app.prior.LSTM, fold=3, model=inner)
    packet["payload_sha256"] = app.spo.sha(packet)
    axis = inputs[app.AXIS].copy()
    finance = inputs[[*app.KEY, *app.prior.FINANCE]].copy()
    future = finance.label_information_end.gt(pd.Timestamp(app.prior.LAST))
    finance.loc[future, ["valuation_status", "exit_execution_status"]] = "IMMATURE"
    finance.loc[future, ["hypothetical_liquidation_net_bps", "valuation_gross_terminal_ratio", "observed_gap_bps"]] = np.nan
    return inputs, packet, axis, finance


def test_no_fit_or_original_fold_guard_is_called(trial, monkeypatch):
    inputs, packet, axis, _ = trial
    monkeypatch.setattr(app.spo, "fit", lambda *_a, **_k: pytest.fail("no model fitting"))
    monkeypatch.setattr(app.specialist, "predict_specialist", lambda *_a: pytest.fail("do not fake the original fold dates"))
    before = deepcopy(packet)
    result = app.predict(inputs, packet, axis)
    assert len(result) == 5900 and result[app.DAY].nunique() == 118
    assert result.groupby(app.DAY).selected.sum().eq(10).all()
    assert packet == before


def test_original_p18_text_axis_and_original_row_order(trial):
    inputs, _packet, axis, _ = trial
    text_axis = axis.astype(str)
    assert len(app.validate_inputs(inputs, text_axis)) == 118
    with pytest.raises(AssertionError):
        app.validate_inputs(inputs.iloc[::-1].reset_index(drop=True), text_axis)


@pytest.mark.parametrize("problem", ["missing", "duplicate", "wrong_manifest", "wrong_axis", "infinite", "wrong_fold", "wrong_cutoff", "corrupt_weight"])
def test_population_and_frozen_model_drift_fail(problem, trial):
    inputs, packet, axis, _ = trial
    if problem == "missing":
        inputs = inputs.iloc[:-1]
    elif problem == "duplicate":
        inputs = pd.concat([inputs, inputs.iloc[:1]])
    elif problem == "wrong_manifest":
        inputs["manifest_sha256"] = "wrong"
    elif problem == "wrong_axis":
        axis.loc[0, "instrument"] = "other"
    elif problem == "infinite":
        inputs.loc[0, app.spo.FEATURES[0]] = np.inf
    elif problem == "wrong_fold":
        packet["fold"] = 2
    elif problem == "wrong_cutoff":
        packet["model"]["cutoff"] = "2026-03-12"
    else:
        packet["model"]["coef"][0] += 1
    with pytest.raises((ValueError, AssertionError)):
        app.predict(inputs, packet, axis)


@pytest.mark.parametrize("classification,mean,action,target", [
    ("STRESS_NOT_SUPPORTED_CURRENT_PAIR", -20., "STOP_ADDITIONAL_DEVELOPMENT_THIS_FROZEN_MODEL", "POINT_ESTIMATE_BELOW_TARGET"),
    ("STRESS_NOT_SUPPORTED_CURRENT_PAIR", 120., "STOP_ADDITIONAL_DEVELOPMENT_THIS_FROZEN_MODEL", "POINT_ESTIMATE_AT_OR_ABOVE_TARGET"),
    ("STRESS_SUPPORTS_LSTM_PROSPECTIVE", 57., "KEEP_FROZEN_OBSERVATION_NO_EXPANSION", "POINT_ESTIMATE_BELOW_TARGET"),
    ("STRESS_SUPPORTS_LSTM_PROSPECTIVE", 100., "KEEP_FROZEN_OBSERVATION_NO_EXPANSION", "POINT_ESTIMATE_AT_OR_ABOVE_TARGET"),
    ("STRESS_INCONCLUSIVE", None, "INCONCLUSIVE_NO_NEW_FIT_OR_EXPANSION", "NOT_EVALUATED"),
])
def test_branches_do_not_mutate_formal_or_production_state(classification, mean, action, target):
    result = app.decision(dict(classification=classification, statistics=dict(mean_bps=mean)))
    assert result["action"] == action and result["target_status"] == target
    assert not any(result[k] for k in ("v2_changed", "p20_stopped", "production_ranking_replaced", "independent_economic_confirmation"))
    assert result["extra_fit_budget"] == 0


def test_invalid_decision_is_not_success():
    with pytest.raises(ValueError):
        app.decision(dict(classification="OTHER", statistics=dict(mean_bps=1.)))
    with pytest.raises(ValueError):
        app.decision(dict(classification="STRESS_INCONCLUSIVE", statistics=dict(mean_bps=np.inf)))


def test_unknown_shared_arm_does_not_filter_specialist_primary(trial):
    inputs, packet, axis, finance = trial
    specialist = app.predict(inputs, packet, axis)
    shared = specialist.copy()
    first = inputs[app.DAY].min()
    group = shared.loc[shared[app.DAY].eq(first)]
    native_ids = set(group.loc[group.selection_effective_rank.le(10), "instrument"])
    special_ids = set(group.loc[group.advisory_rank.le(10), "instrument"])
    unknown_symbol = next(s for s in group.instrument if s not in native_ids | special_ids)
    available = group.loc[group.selected].index[0]
    outsider = group.loc[group.instrument.eq(unknown_symbol)].index[0]
    a, b = shared.loc[available, "advisory_rank"], shared.loc[outsider, "advisory_rank"]
    shared.loc[available, "advisory_rank"], shared.loc[outsider, "advisory_rank"] = b, a
    shared["selected"] = shared.advisory_rank.le(10)
    mask = finance[app.DAY].eq(first) & finance.instrument.eq(unknown_symbol)
    finance.loc[mask, "valuation_status"] = "UNKNOWN"
    finance.loc[mask, "hypothetical_liquidation_net_bps"] = np.nan
    old_panel = app.spo.base.cohort(shared, finance)
    old_report = dict(lstm_stress=dict(statistics=dict(mean_bps=float(old_panel.delta.mean()))))
    report, panel, wide = app.evaluate(specialist, finance, shared, old_panel, old_report, axis)
    assert report["common_days"] == report["three_arm_common"]["days"] + 1
    assert len(panel) == 118 and len(wide) == 118
    assert pd.notna(panel.iloc[0].delta) and not wide.iloc[0].three_arm_complete
    assert report["original_slot_status_counts"]["UNKNOWN"] == 1
    assert report["new_candidates"] == 0


@pytest.mark.parametrize("problem", ["future", "known_nan", "wrong_fee", "duplicate"])
def test_invalid_finance_is_rejected(trial, problem):
    inputs, packet, axis, finance = trial
    predictions = app.predict(inputs, packet, axis)
    if problem == "future":
        finance.loc[0, "label_information_end"] = pd.Timestamp("2026-09-01")
    elif problem == "known_nan":
        finance.loc[0, "hypothetical_liquidation_net_bps"] = np.nan
    elif problem == "wrong_fee":
        finance.loc[0, "hypothetical_liquidation_net_bps"] += 1
    else:
        finance = pd.concat([finance, finance.iloc[:1]], ignore_index=True)
    with pytest.raises(ValueError):
        app.evaluate(predictions, finance, predictions, pd.DataFrame(), {}, axis)


def test_missing_prediction_stage_prevents_financial_decode(tmp_path, monkeypatch):
    from scripts import advisory_lstm_frozen_specialist_stress as cli
    monkeypatch.setattr(cli, "OUTPUT", tmp_path)
    monkeypatch.setattr(cli, "verify", lambda _p: None)
    monkeypatch.setattr(cli, "financial_inputs", lambda _p: pytest.fail("must not decode before predictions are frozen"))
    with pytest.raises(AdvisoryModelFirstError, match="not complete/readable"):
        cli.evaluated(tmp_path / "study", dict(sha256="a" * 64, output_root=str(tmp_path / "study")))


def test_prediction_uses_real_immutable_publisher_and_reuses_stage(trial, tmp_path, monkeypatch):
    from scripts import advisory_lstm_frozen_specialist_stress as cli
    inputs, packet, axis, _ = trial
    monkeypatch.setattr(cli, "OUTPUT", tmp_path)
    monkeypatch.setattr(cli, "verify", lambda _p: None)
    root = tmp_path / "study"
    inputs.to_parquet(tmp_path / "inputs.parquet", index=False)
    axis.to_json(tmp_path / "axis.json", orient="records", date_format="iso")
    (tmp_path / "model.json").write_text(json.dumps(packet), encoding="utf-8")
    plan = dict(sha256="a" * 64, output_root=str(root), sources=dict(
        p18_inputs=dict(path=str(tmp_path / "inputs.parquet")),
        p18_axis=dict(path=str(tmp_path / "axis.json")), model3=dict(path=str(tmp_path / "model.json"))))
    first = cli.predicted(root, plan)
    assert first["stage"] == "trained"
    receipt = json.loads((root / "trained/receipt.json").read_bytes())
    assert receipt["operation"] == "FROZEN_WEIGHT_INFERENCE_ZERO_FIT" and not receipt["financial_payload_decoded"]
    monkeypatch.setattr(cli.pd, "read_parquet", lambda *_a, **_k: pytest.fail("must reuse frozen predictions"))
    assert cli.predicted(root, plan)["stage_sha256"] == first["stage_sha256"]
    with pytest.raises(ValueError, match="differs from the frozen protocol"):
        cli.predicted(tmp_path / "another", plan)
