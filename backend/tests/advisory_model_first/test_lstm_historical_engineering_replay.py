"""Offline replay preserves production math while time and storage stay isolated."""
from copy import deepcopy
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_historical_engineering_replay as app
from backend.tests.advisory_model_first.test_lstm_spo_shadow_v1 import trial as trial


@pytest.fixture
def history(trial, tmp_path):
    day = "2025-04-01"
    trial.rows[app.shadow.KEY[0]], trial.rows[app.shadow.KEY[1]] = pd.Timestamp(day), pd.Timestamp("2025-04-02")
    model = deepcopy(trial.packet)
    model["fold"], model["model"]["cutoff"] = 1, "2025-03-31"
    inner = model["model"]
    inner["model_payload_sha256"] = app.shadow.spo.sha({k:v for k,v in inner.items() if k != "model_payload_sha256"})
    model["payload_sha256"] = app.shadow.spo.sha({k:v for k,v in model.items() if k != "payload_sha256"})
    saved = app.specialist.predict_specialist(trial.rows, model)
    finance = trial.rows[app.KEY].copy()
    finance["label_information_end"] = pd.Timestamp("2025-04-09")
    finance["valuation_policy_sha256"] = app.shadow.RELEASE["valuation_policy_sha256"]
    finance["valuation_status"] = "AVAILABLE"
    finance["valuation_gross_terminal_ratio"] = 1. + np.arange(50) / 1000
    finance["observed_gap_bps"] = np.arange(50) - 25.
    finance["hypothetical_liquidation_net_bps"] = 10000 * (finance.valuation_gross_terminal_ratio / (1 + finance.observed_gap_bps/10000) * (1 - 5.95/10000) / (1 + .95/10000) - 1)
    finance["exit_execution_status"] = "EXECUTABLE"
    info = dict(fold=1, fold_end="2025-05-30", horizon_end="2025-04-09", target_date="2025-04-02")
    source = SimpleNamespace(days=[day], ready=True, revision=7, inputs=lambda _d: trial.rows,
        model=lambda _f: model, saved=lambda _d: saved, day=lambda _d: info,
        finance_at=lambda _d, _at: finance.copy(deep=True))

    def token():
        if not source.ready:
            raise app.InputReadinessError("waiting for the consumed input version")
        return str(source.revision)

    def verify(value):
        if value != str(source.revision):
            raise app.InputReadinessError("source revision changed during computation")

    source.token, source.verify = token, verify
    now = [app.shadow.clock(day, 16, 31)]
    engine = app.HistoricalReplay(root=tmp_path / "replay", source=source,
        clock=lambda: now[0], plan_sha256="plan")
    return SimpleNamespace(engine=engine, source=source, now=now, day=day, inputs=trial.rows,
        finance=finance, model=model, info=info)


@pytest.mark.parametrize("date,hour,minute", [("2025-04-01", 21, 5), ("2025-04-02", 0, 15), ("2025-04-02", 9, 29)])
def test_real_clock_is_unnecessary_for_late_or_midnight_replay(history, date, hour, minute):
    h = history
    h.source.ready = False
    assert h.engine.capture(h.day)["status"] == "WAITING_DATA"
    assert not h.engine.path(h.day).exists()
    h.source.ready = True
    h.now[0] = app.shadow.clock(date, hour, minute)
    assert h.engine.capture(h.day)["status"] == "PREDICTED"
    raw = h.engine.path(h.day).read_bytes()
    h.source.inputs = lambda _d: pytest.fail("frozen replay must not reread altered inputs")
    h.now[0] = app.shadow.clock("2025-04-02", 10)
    assert h.engine.capture(h.day)["status"] == "PREDICTION_REUSED"
    assert h.engine.path(h.day).read_bytes() == raw


def test_open_deadline_and_computation_crossing_open_never_create_prediction(history):
    h = history
    h.now[0] = app.shadow.clock("2025-04-02", 9, 29)
    def slow(_d):
        h.now[0] = app.shadow.clock("2025-04-02", 9, 30)
        return h.inputs
    h.source.inputs = slow
    with pytest.raises(ValueError, match="crossed target open"):
        h.engine.capture(h.day)
    assert not h.engine.path(h.day).exists()
    h.source.inputs = lambda _d: pytest.fail("post-open replay must not read features")
    assert h.engine.capture(h.day)["status"] == "MISSED_CAPTURE_WINDOW"


def test_post_compute_revision_change_waits_without_publishing(history):
    h = history
    original = h.source.verify
    def changed(token):
        h.source.revision = 8
        original(token)
    h.source.verify = changed
    assert h.engine.capture(h.day)["status"] == "WAITING_DATA"
    assert not h.engine.path(h.day).exists()
    h.source.verify = original
    assert h.engine.capture(h.day)["status"] == "PREDICTED"


def test_before_h_and_original_cutoff_precede_all_financial_payload_reads(history):
    h = history
    h.engine.capture(h.day)
    h.source.finance_at = lambda *_: pytest.fail("H not closed; no monetary payload may be read")
    h.now[0] = app.shadow.clock("2025-04-09", 14, 59)
    assert h.engine.evaluate(h.day)["status"] == "IMMATURE"
    assert h.engine.status(h.day)["selected"]["business_review"]["native_net_bps"] is None
    h.info["fold_end"] = "2025-04-08"
    h.now[0] = app.shadow.clock("2026-11-02", 15)
    assert h.engine.evaluate(h.day)["status"] == "IMMATURE"


def test_first_complete_reuses_original_and_virtual_clock_cannot_expose_future_evaluation(history):
    h = history
    h.engine.capture(h.day)
    h.now[0] = app.shadow.clock("2025-04-09", 15)
    assert h.engine.evaluate(h.day)["status"] == "COMPLETE"
    first = h.engine.evaluation(h.day)["sha256"]
    h.source.finance_at = lambda *_: pytest.fail("first complete historical evaluation must be immutable")
    assert h.engine.evaluate(h.day)["status"] == "EVALUATION_REUSED"
    assert h.engine.evaluation(h.day)["sha256"] == first
    h.now[0] = app.shadow.clock("2025-04-09", 14, 59)
    assert h.engine.evaluate(h.day)["status"] == "IMMATURE"
    assert h.engine.evaluation(h.day) is None


@pytest.mark.parametrize("failure", ["missing", "duplicate", "infinite_feature", "future_fold"])
def test_invalid_original_population_or_future_fold_is_rejected(history, failure):
    h = history
    if failure == "missing":
        h.source.inputs = lambda _d: h.inputs.iloc[:-1]
    elif failure == "duplicate":
        h.source.inputs = lambda _d: pd.concat([h.inputs, h.inputs.iloc[:1]], ignore_index=True)
    elif failure == "infinite_feature":
        h.inputs.loc[0, "ret_1"] = np.inf
    else:
        h.model["fold"] = 3
    with pytest.raises(ValueError):
        h.engine.capture(h.day)
    assert not h.engine.path(h.day).exists()


@pytest.mark.parametrize("failure", ["nan_known", "duplicate", "missing", "wrong_policy", "wrong_fee"])
def test_invalid_saved_valuation_cannot_become_a_successful_business_result(history, failure):
    h = history
    h.engine.capture(h.day)
    h.now[0] = app.shadow.clock("2025-04-09", 15)
    finance = h.finance.copy()
    if failure == "nan_known":
        finance.loc[0, "hypothetical_liquidation_net_bps"] = np.nan
    elif failure == "duplicate":
        finance = pd.concat([finance, finance.iloc[:1]], ignore_index=True)
    elif failure == "missing":
        finance = finance.iloc[:-1]
    elif failure == "wrong_policy":
        finance["valuation_policy_sha256"] = "wrong"
    else:
        finance.loc[0, "hypothetical_liquidation_net_bps"] += 1
    h.source.finance_at = lambda *_: finance
    with pytest.raises(ValueError):
        h.engine.evaluate(h.day)
    assert h.engine.evaluation(h.day) is None


def test_unknown_and_cash_keep_original_fifty_rows_and_fixed_slots(history):
    h = history
    h.engine.capture(h.day)
    h.finance.loc[0, ["valuation_status", "hypothetical_liquidation_net_bps"]] = ["CASH_ENTRY_NOT_EXECUTABLE", 0]
    h.finance.loc[49, ["valuation_status", "hypothetical_liquidation_net_bps"]] = ["UNKNOWN", np.nan]
    h.now[0] = app.shadow.clock("2025-04-09", 15)
    h.source.finance_at = lambda *_: h.finance
    assert h.engine.evaluate(h.day)["status"] == "UNKNOWN"
    status = h.engine.status(h.day)
    review = status["selected"]["business_review"]
    assert len(review["rows"]) == 50 and review["native_cash_slots"] == 1
    assert review["specialist_unknown_slots"] == 1 and review["delta_bps"] is None
    assert all(v is None for v in review["components"].values())
    assert status["replay_context"]["clock_is_simulated"] is True
    assert status["independent_economic_confirmation"] is False


@pytest.mark.parametrize("gap", [np.nan, -10000.])
def test_invalid_entry_gap_cannot_pass_independent_fee_audit(history, gap):
    finance = history.finance.copy()
    finance.loc[0, "observed_gap_bps"] = gap
    with pytest.raises(ValueError, match="entry gap"):
        app.verify_fees(finance)


def test_hash_valid_evaluation_with_wrong_parent_is_rejected(history):
    h = history
    h.engine.capture(h.day)
    h.now[0] = app.shadow.clock("2025-04-09", 15)
    h.engine.evaluate(h.day)
    path = next((h.engine.path(h.day).parent / "evaluations").glob("*.json"))
    packet = json.loads(path.read_bytes())
    packet["prediction_sha256"] = "b" * 64
    packet["sha256"] = app.shadow.spo.sha({k: v for k, v in packet.items() if k != "sha256"})
    path.write_bytes(json.dumps(packet).encode())
    with pytest.raises(ValueError, match="parent differs"):
        h.engine.evaluation(h.day)
