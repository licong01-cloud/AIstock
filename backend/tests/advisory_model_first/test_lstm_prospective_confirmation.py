"""Prospective clock, full population, immutable results and independent arithmetic."""
from copy import deepcopy
from contextlib import contextmanager
from datetime import date, timedelta
import json
from statistics import NormalDist

import numpy as np
import pytest

from scripts import advisory_lstm_prospective_confirmation as c


def calendar():
    start = date.fromisoformat(c.RULE["first_d"])
    return dict(schema="confirmation_calendar_v1", source="market.trading_calendar_READ_ONLY",
        observed_at="2026-11-01T12:00:00+08:00", database_write=False,
        rows=[[(start + timedelta(days=i)).isoformat(), (start + timedelta(days=i)).weekday() < 5]
              for i in range(300)])


def pair(window):
    records, finance = [], []
    for i in range(1, 51):
        key = dict(package_id=c.shadow.TARGET["package_id"], decision_as_of_trade_date=window[0],
                   target_trade_date=window[1], instrument=f"{i:06}.SZ")
        records.append(dict(**key, selection_effective_rank=i, advisory_rank=51-i))
        finance.append(dict(**key, valuation_policy_sha256=c.shadow.RELEASE["valuation_policy_sha256"],
            valuation_status="AVAILABLE", hypothetical_liquidation_net_bps=float(i)))
    prediction = dict(sha256="p", predictions=records)
    result = c.shadow.paired_result(c.shadow.frame(records), c.shadow.frame(finance))
    packet = dict(prediction_sha256="p", result=result, finance=finance,
        observed_at=c.shadow.clock(window[-1], 16).isoformat(), source_receipt=dict(calendar=window,
        horizon_end=window[-1], read_after_maturity=True, source="CURRENT_DATABASE_NON_VINTAGE",
        database_write=False, actual_fill_proven=False))
    return prediction, packet


def test_hac_matches_independent_full_kernel_matrix():
    x = np.sin(np.arange(150) * .21) * 123 + np.arange(150) / 9
    lag = 10
    distances = np.abs(np.arange(len(x))[:, None] - np.arange(len(x))[None, :])
    kernel = np.maximum(1 - distances / (lag + 1), 0.)
    residual = x - x.mean()
    se = np.sqrt(residual @ kernel @ residual / (150 * 149))
    result = c.hac(x, lag)
    assert result["se_bps"] == pytest.approx(se, abs=1e-12)
    assert result["lower_one_sided95_bps"] == pytest.approx(x.mean() - NormalDist().inv_cdf(.95) * se)


def test_budget_is_conditional_and_does_not_assume_old_effect_is_true():
    result = c.budget(dict(statistics=dict(paired_days=102, se_bps=27.291809958209306,
                                         mean_bps=57.277529801029665)))
    assert not result["guaranteed_power"] and result["business_minimum_effect_bps"] is None
    assert result["mde80_bps_by_noise"]["1.0"] == pytest.approx(55.95911887099532)
    assert [r["required_complete_days"] for r in result["scenarios"] if r["noise_multiplier"] == 1.][2] == 144
    assert result["new_fits"] == 0


def test_direction_inconclusive_and_degenerate_do_not_activate():
    wave = np.sin(np.arange(150)) * 20
    assert c.decision(100 + wave)["statistical_support"]
    for values in (-100 + wave, wave, np.ones(150) * 100):
        assert not c.decision(values)["statistical_support"]
    result = c.decision(100 + wave)
    assert not result["economic_confirmation"] and not result["automatic_activation"]
    assert [v["lag"] for v in result["sensitivity"]] == [4, 20]
    with pytest.raises(ValueError, match="drop"):
        c.decision(wave[:-1])
    with pytest.raises(ValueError, match="finite"):
        c.decision(np.r_[wave[:-1], np.nan])


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "int_bool", "wrong_role"])
def test_calendar_rejects_gaps_duplicates_and_untrusted_role(mutation):
    packet = calendar()
    if mutation == "duplicate":
        packet["rows"][1] = packet["rows"][0]
    elif mutation == "missing":
        packet["rows"].pop(10)
    elif mutation == "int_bool":
        packet["rows"][0][1] = 1
    else:
        packet["source"] = "WEEKDAY_GUESS"
    with pytest.raises(ValueError):
        c.scheduled(packet)


def test_schedule_uses_original_sessions_and_fixed_repair_grace():
    packet = calendar()
    schedule = c.scheduled(packet)
    dates = [d for d, value in packet["rows"] if value]
    assert schedule["decision_dates"] == dates[:150]
    assert schedule["windows"][dates[149]] == dates[149:155]
    assert schedule["look_at"] == c.shadow.clock(dates[159], 15)
    packet["rows"] = packet["rows"][:60]
    assert c.scheduled(packet) is None


def test_finalize_and_inspect_never_open_evaluations_before_fixed_look(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "OUTPUT", tmp_path)
    monkeypatch.setattr(c, "LIVE", tmp_path / "live")
    monkeypatch.setattr(c, "verify", lambda _: None)
    monkeypatch.setattr(c, "now_utc", lambda: c.instant("2026-11-01T18:00:00+08:00"))
    monkeypatch.setattr(c, "collect", lambda *_: pytest.fail("outcome access before look"))
    (c.LIVE / "days/2026-10-30/evaluations").mkdir(parents=True)
    (c.LIVE / "days/2026-10-30/evaluations/not_json.json").write_text("FORBIDDEN")
    path = tmp_path / "calendar.json"
    c.shadow.publish(path, calendar())
    protocol = dict(prior_pilot_excluded="2026-10-30", study_root=str(tmp_path / "result"))
    assert c.finalize(protocol, path, tmp_path / "result")["status"] == "AWAITING_FIXED_LOOK"
    assert c.inspect(protocol)["evaluation_files_read"] == 0


def test_unknown_preserved_and_result_tamper_rejected():
    window = c.scheduled(calendar())["windows"][c.RULE["first_d"]]
    prediction, packet = pair(window)
    assert c.check_evaluation(packet, prediction, window)["delta_bps"] == 40
    packet["result"]["delta_bps"] += 1
    with pytest.raises(ValueError, match="arithmetic"):
        c.check_evaluation(packet, prediction, window)
    prediction, packet = pair(window)
    packet["finance"][0]["valuation_status"] = "UNKNOWN"
    packet["finance"][0]["hypothetical_liquidation_net_bps"] = None
    packet["result"] = c.shadow.paired_result(c.shadow.frame(prediction["predictions"]), c.shadow.frame(packet["finance"]))
    result = c.check_evaluation(packet, prediction, window)
    assert result["status"] == "UNKNOWN" and result["delta_bps"] is None
    assert result["native_unknown_slots"] == 1


@pytest.mark.parametrize("kind", ["parent", "calendar", "clock", "db_write"])
def test_evaluation_identity_and_maturity_are_not_bypassed(kind):
    window = c.scheduled(calendar())["windows"][c.RULE["first_d"]]
    prediction, packet = pair(window)
    if kind == "parent":
        packet["prediction_sha256"] = "other"
    elif kind == "calendar":
        packet["source_receipt"]["calendar"] = window[:-1]
    elif kind == "clock":
        packet["observed_at"] = c.shadow.clock(window[-1], 14).isoformat()
    else:
        packet["source_receipt"]["database_write"] = True
    with pytest.raises(ValueError):
        c.check_evaluation(packet, prediction, window)


def test_prediction_recompute_and_open_deadline(tmp_path, monkeypatch):
    window = c.scheduled(calendar())["windows"][c.RULE["first_d"]]
    prediction, _ = pair(window)
    expected = c.shadow.frame(prediction["predictions"])
    monkeypatch.setattr(c.shadow, "predict_current", lambda *_: expected)
    packet = dict(prediction, schema=c.shadow.SCHEMA, release_sha256=c.RELEASE_SHA,
        model_sha256=c.shadow.MODEL_SHA, physical_fit_count=0, native_policy_changed=False, inputs=prediction["predictions"],
        publication=dict(program_id=c.shadow.PROGRAM, binding_version_id=c.shadow.BINDING,
            decision_date=window[0], target_date=window[1], created_at=c.shadow.clock(window[0], 20).isoformat()),
        observed_at=c.shadow.clock(window[0], 21).isoformat())
    packet.pop("sha256")
    protocol = dict(program_id=c.shadow.PROGRAM, binding_version_id=c.shadow.BINDING)
    path = tmp_path / "prediction.json"
    c.shadow.publish(path, packet)
    assert c.check_prediction(path, protocol, window, {})
    late = deepcopy(packet)
    late["observed_at"] = c.shadow.clock(window[1], 9, 30).isoformat()
    c.shadow.publish(tmp_path / "late.json", late)
    with pytest.raises(ValueError, match="open"):
        c.check_prediction(tmp_path / "late.json", protocol, window, {})
    changed = deepcopy(packet)
    changed["predictions"][0]["advisory_rank"] = 49
    c.shadow.publish(tmp_path / "changed.json", changed)
    with pytest.raises(AssertionError):
        c.check_prediction(tmp_path / "changed.json", protocol, window, {})


def test_first_complete_before_fixed_cutoff_is_used_and_files_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "LIVE", tmp_path)
    (tmp_path / "model.json").write_text("placeholder-for-stubbed-model-reader")
    monkeypatch.setattr(c.shadow, "checked_model", lambda _: {})
    schedule = c.scheduled(calendar())
    day = schedule["decision_dates"][0]
    window = schedule["windows"][day]
    prediction, packet = pair(window)
    directory = tmp_path / "days" / day
    directory.mkdir(parents=True)
    (directory / "prediction.json").write_text("placeholder-for-stubbed-prediction-reader")
    monkeypatch.setattr(c, "check_prediction", lambda *_: prediction)
    unknown = deepcopy(packet)
    unknown["result"]["status"] = "UNKNOWN"
    unknown["finance"][0]["valuation_status"] = "UNKNOWN"
    unknown["finance"][0]["hypothetical_liquidation_net_bps"] = None
    unknown["result"] = c.shadow.paired_result(c.shadow.frame(prediction["predictions"]), c.shadow.frame(unknown["finance"]))
    c.shadow.publish(directory / "evaluations/a.json", unknown)
    packet["observed_at"] = c.shadow.clock(window[-1], 17).isoformat()
    c.shadow.publish(directory / "evaluations/b.json", packet)
    late = deepcopy(packet)
    late["observed_at"] = (schedule["look_at"] + timedelta(seconds=1)).isoformat()
    late["result"]["delta_bps"] = 999999
    c.shadow.publish(directory / "evaluations/c.json", late)
    rows, consumed, reads = c.collect({}, schedule)
    assert len(rows) == 150 and rows[0]["delta_bps"] == 40 and reads == 3 and len(consumed) == 4
    assert sum(r["status"] == "MISSING_PREDICTION" for r in rows) == 149


def test_final_full_and_missing_population_publish_once_without_live_calls(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "OUTPUT", tmp_path)
    monkeypatch.setattr(c, "verify", lambda _: None)
    schedule = c.scheduled(calendar())
    monkeypatch.setattr(c, "now_utc", lambda: schedule["look_at"] + timedelta(hours=1))
    monkeypatch.setattr(c.shadow.LstmSpoShadowService, "run_once", lambda *_: pytest.fail("live scheduler"))
    monkeypatch.setattr(c.shadow.LstmSpoShadowService, "_source", lambda *_: pytest.fail("live finance source"))
    monkeypatch.setattr(c.shadow.spo, "fit", lambda *_: pytest.fail("new fitting"))
    rows = [dict(decision_date=d, status="COMPLETE", delta_bps=100 + np.sin(i),
                 result=dict(native_net_bps=float(i - 50), specialist_net_bps=float(i + 50 + np.sin(i)),
                             intervention_count=7))
            for i, d in enumerate(schedule["decision_dates"])]
    monkeypatch.setattr(c, "collect", lambda *_: (rows, {}, 150))
    path = tmp_path / "calendar.json"
    c.shadow.publish(path, calendar())
    protocol = dict(study_root=str(tmp_path / "full"))
    result = c.finalize(protocol, path, tmp_path / "full")
    assert result["statistical_support"] and result["evaluation_files_read"] == 150
    assert result["descriptive_economics"]["native"]["worst_cohort_bps"] == -50
    assert result["descriptive_economics"]["mean_entries_per_day"] == 7
    monkeypatch.setattr(c, "collect", lambda *_: pytest.fail("must reuse final result"))
    assert c.finalize(protocol, path, tmp_path / "full")["status"] == "FINAL_RESULT_REUSED"
    with pytest.raises(ValueError, match="unique registered"):
        c.finalize(protocol, path, tmp_path / "second_formal_result")
    rows[20]["status"], rows[20]["delta_bps"] = "UNKNOWN", None
    monkeypatch.setattr(c, "collect", lambda *_: (rows, {}, 150))
    result = c.finalize(dict(study_root=str(tmp_path / "unknown")), path, tmp_path / "unknown")
    assert result["status"] == "NOT_EVALUABLE" and result["unresolved_dates"] == 1
    assert len(json.loads((tmp_path / "unknown/evaluated/cohorts.json").read_bytes())) == 150


def test_absolute_deadline_stops_without_waiting_for_calendar(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "OUTPUT", tmp_path)
    monkeypatch.setattr(c, "verify", lambda _: None)
    monkeypatch.setattr(c, "now_utc", lambda: c.instant(c.RULE["absolute_close"]))
    monkeypatch.setattr(c, "collect", lambda *_: pytest.fail("incomplete calendar cannot open outcomes"))
    packet = calendar()
    packet["rows"] = packet["rows"][:20]
    path = tmp_path / "short_calendar.json"
    c.shadow.publish(path, packet)
    assert c.finalize(dict(study_root=str(tmp_path / "expired")), path, tmp_path / "expired")["status"] == "NOT_EVALUABLE"


def test_source_normalization_data_hash_and_output_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "OUTPUT", tmp_path / "allowed")
    assert c.files.code_sha(b"a\r\nb") == c.files.code_sha(b"a\nb")
    path = tmp_path / "physical.txt"
    path.write_bytes(b"a\r\nb")
    before = c.ref(path)
    path.write_bytes(b"a\nb")
    with pytest.raises(ValueError, match="source changed"):
        c.check_ref(before, path)
    for forbidden in (tmp_path / "other", c.LIVE / "overwritten.json"):
        with pytest.raises(ValueError):
            c.destination(forbidden)
    assert c.destination(tmp_path / "allowed" / "result") == tmp_path / "allowed/result"


def test_calendar_collector_queries_only_calendar_and_no_outcomes(tmp_path, monkeypatch):
    from backend.services.advisory_model_first.lstm_spo_shadow_source_v1 import LstmSpoShadowSource
    packet = calendar()
    calls = []
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, sql, params):
            calls.append((sql, params))

        def fetchall(self):
            return [(date.fromisoformat(d), value) for d, value in packet["rows"]]

    class Connection:
        def cursor(self):
            return Cursor()

    @contextmanager
    def connection(_):
        yield Connection()

    monkeypatch.setattr(c, "OUTPUT", tmp_path)
    monkeypatch.setattr(LstmSpoShadowSource, "connection", connection)
    result = c.calendar_snapshot(tmp_path / "calendar.json")
    assert result["rows"] == packet["rows"]
    assert len(calls) == 1 and calls[0][0].lstrip().startswith("SELECT cal_date,is_trading FROM market.trading_calendar")
    assert calls[0][1] == (c.RULE["first_d"], "2027-09-30")


def test_protocol_identity_clock_and_data_drift_rejected(tmp_path, monkeypatch):
    paths = {name: tmp_path / name for name in ("model", "release", "aggregate")}
    for path in paths.values():
        path.write_text("immutable physical input")
    monkeypatch.setattr(c, "references", lambda: paths)
    monkeypatch.setattr(c, "OUTPUT", tmp_path)
    monkeypatch.setattr(c, "source_closure", lambda **_: {"code": "lf-hash"})
    protocol = dict(schema=c.SCHEMA, rule=deepcopy(c.RULE), target=c.shadow.TARGET,
        program_id=c.shadow.PROGRAM, binding_version_id=c.shadow.BINDING, release_sha256=c.RELEASE_SHA,
        source_closure={"code": "lf-hash"}, registered_at="2026-11-01T12:00:00+08:00",
        sources={name: c.ref(path) for name, path in paths.items()}, study_root=str(tmp_path / "study"))
    c.verify(protocol)
    for key, bad in (("program_id", "other"), ("registered_at", "2026-11-02T00:00:00+08:00"),
                     ("source_closure", {"code": "changed"})):
        changed = {**protocol, key: bad}
        with pytest.raises(ValueError):
            c.verify(changed)
    paths["aggregate"].write_text("different source")
    with pytest.raises(ValueError, match="source changed"):
        c.verify(protocol)


def test_new_registration_after_first_d_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "OUTPUT", tmp_path)
    monkeypatch.setattr(c, "now_utc", lambda: c.shadow.clock(c.RULE["first_d"], 0))
    with pytest.raises(ValueError, match="registered after"):
        c.freeze(tmp_path / "late")
