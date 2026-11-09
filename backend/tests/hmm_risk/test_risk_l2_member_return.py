"""Direct approved signed-target contracts; synthetic unit fits are not research."""

from copy import deepcopy
import math

import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_member_return as h
from backend.services.hmm_risk.contracts import StateModelSetError, canonical_sha256
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from scripts.hmm_risk import run_risk_l2_member_return as cli


HEAD = "a" * 40
NUMERIC = dict(
    versions=h.loss.EXPECTED_VERSIONS,
    thread_variables=dict.fromkeys(h.THREAD_VARIABLES, "1"),
    thread_pools=[dict(num_threads=1)],
)


def seal(value):
    return receipt({k: v for k, v in value.items() if k != "receipt_sha256"})


@pytest.fixture
def packet(monkeypatch):
    catalog = [f"801{i:03d}.SI" for i in range(131)]
    plan = dict(
        train=["2026-03-13", "2026-03-16", "2026-03-31"],
        fit=["2026-03-13", "2026-03-16"],
        predict=["2026-04-01", "2026-04-02"],
    )
    monkeypatch.setattr(h, "schedule", lambda _, _v: plan)
    rows = [
        [d, c, [i / 131, j] + [0.0] * 18, (i - 65) / 500 + j / 100, i / 1310]
        for j, d in enumerate(plan["fit"])
        for i, c in enumerate(catalog)
    ]
    return receipt(
        dict(
            schema_version=h.VERSION + "_train",
            contract=h.CONTRACT,
            vintage=h.VINTAGES[0],
            calendar=pd.bdate_range("2026-03-12", "2026-04-20").strftime("%Y-%m-%d").tolist(),
            catalog=catalog,
            rows=rows,
            excluded={"OUTCOME_NOT_MATURE": 131},
            mask_sha256=canonical_sha256([r[:2] for r in rows]),
            feature_definition_sha256=h.PINS["feature_definition"],
            feature_sha256="f" * 64,
            target_facts_sha256="e" * 64,
            release_binding=dict(manifest_sha256=h.PINS["manifest"]),
            source_commit=HEAD,
        )
    )


def fitted(packet, tmp_path):
    return h.fit_packet(packet, None, tmp_path / "models", tmp_path / "attempt", HEAD, NUMERIC)


@pytest.mark.parametrize("s,c", [(-0.3, 0.4), (0.0, 0.1), (0.8, 0.05), (1.2, 0.1)])
def test_signed_algebra_no_upper_clip(s, c):
    assert h.signed_label(dict(B=max(0, -s), C=c, upside=s + c)) == pytest.approx((s, c))
    with pytest.raises(FormalStateError):
        h.signed_label(dict(B=0.9, C=c, upside=s + c))


def test_legacy_loss_still_rejects_signed_and_nonfinite():
    rows = [["a", "x", [0.0] * 20, -0.2], ["b", "x", [1.0] * 20, 1.2]]
    params, stats = h.loss._fit_weighted_ridge(rows)
    assert math.isfinite(params["intercept"]) and stats["training_target_mean"] == pytest.approx(0.5)
    with pytest.raises(FormalStateError):
        h.loss.fit_parameters(rows)
    rows[0][3] = float("nan")
    with pytest.raises(FormalStateError):
        h.loss._fit_weighted_ridge(rows)


def test_quarter_maturity_and_forecast_windows():
    calendar = pd.bdate_range("2022-01-01", "2026-10-30").strftime("%Y-%m-%d").tolist()
    for vintage in h.VINTAGES:
        plan = h.schedule(calendar, vintage)
        assert len(plan["train"]) - len(plan["fit"]) == 10
        assert calendar[calendar.index(plan["fit"][-1]) + 10] == vintage["train_end"]
        assert plan["predict"][0] > vintage["train_end"]


def test_no_immature_label_read_and_no_mask_widening(packet):
    codes, days = packet["catalog"], h.schedule(None, None)["fit"]
    features = dict(calendar=packet["calendar"], catalog=codes, rows={d: {} for d in days})
    labels, original = {}, {}
    for d in days:
        labels[d] = {c: dict(status="AVAILABLE", reason=None, B=0, C=0.1, upside=0.3) for c in codes}
        original[d] = dict.fromkeys(codes, "AVAILABLE")
        features["rows"][d] = {c: dict(features=[0.0] * 20) for c in codes}
    labels[days[0]][codes[0]] = dict(
        status="UNAVAILABLE", reason="FIXED_COHORT_ENDPOINT_NA", B=None, C=None, upside=None
    )
    original[days[0]][codes[1]] = "OUTCOME_LEGAL_NA"
    rows, excluded = h.packet_rows(features, labels, original, packet["vintage"])
    assert len(rows) == 260 and excluded == {
        "OUTCOME_NOT_MATURE": 131,
        "FIXED_COHORT_ENDPOINT_NA": 1,
        "ORIGINAL_COMMON_MASK_EXCLUDED": 1,
    }
    labels[days[0]][codes[0]]["reason"] = "UNKNOWN"
    with pytest.raises(FormalStateError):
        h.packet_rows(features, labels, original, packet["vintage"])


@pytest.mark.parametrize("kind", ["duplicate", "late", "nonfinite", "wrong_release", "hash"])
def test_packet_fails_closed(packet, kind):
    p = deepcopy(packet)
    if kind == "duplicate":
        p["rows"][1] = p["rows"][0]
    elif kind == "late":
        p["rows"][0][0] = "2026-03-31"
    elif kind == "nonfinite":
        p["rows"][0][3] = -1
    elif kind == "wrong_release":
        p["release_binding"]["manifest_sha256"] = "0" * 64
    else:
        p["mask_sha256"] = "0" * 64
    with pytest.raises(FormalStateError):
        h.validate_packet(seal(p))


def test_synthetic_fit_pair_scaler_and_attempt_reentry(packet, tmp_path):
    first = fitted(packet, tmp_path)
    assert first["fits"] == 2
    for k in ("mean", "scale", "active"):
        assert first["models"]["S"]["parameters"][k] == first["models"]["C"]["parameters"][k]
    assert first["models"]["S"]["training_target_mean"] != first["models"]["C"]["training_target_mean"]
    with pytest.raises((FormalStateError, FileExistsError)):
        h.fit_packet(packet, None, tmp_path / "other", tmp_path / "attempt", HEAD, NUMERIC)


def test_failed_attempt_is_consumed_without_second_fit(packet, tmp_path, monkeypatch):
    calls = []

    def broken(_):
        calls.append(1)
        raise RuntimeError("injected failure")

    monkeypatch.setattr(h.loss, "_fit_weighted_ridge", broken)
    with pytest.raises(RuntimeError):
        fitted(packet, tmp_path)
    with pytest.raises((FormalStateError, FileExistsError)):
        fitted(packet, tmp_path)
    assert calls == [1]


def test_q4_identity_mismatch_stops_before_fit(packet, monkeypatch):
    p = seal({**packet, "vintage": h.release.Q4})
    monkeypatch.setattr(h.release, "validate_model", lambda _: None)
    with pytest.raises(FormalStateError, match="Q4 control"):
        h.validate_q4_control(p, dict(model_sha256="0" * 64))


def test_signed_prediction_is_raw_and_preserves_na(packet, tmp_path, monkeypatch):
    model = fitted(packet, tmp_path)["models"]["S"]
    model["parameters"]["coef"] = [0.0, 0.0]
    model["parameters"]["intercept"] = -1.2
    model["model_sha256"] = canonical_sha256(model["parameters"])
    model = seal(model)
    days, codes = h.schedule(None, None)["predict"], packet["catalog"]
    f = dict(
        calendar=packet["calendar"],
        catalog=codes,
        as_of={d: packet["calendar"][packet["calendar"].index(d) - 1] for d in days},
        rows={d: {c: dict(features=[0.0] * 20, reason_code=None) for c in codes} for d in days},
    )
    f["rows"][days[0]][codes[0]] = dict(features=None, reason_code="hmm_risk_legal_na")
    monkeypatch.setattr(h, "validate_prediction_features", lambda _: None)
    rows = h.predict_model(model, f)
    assert len(rows) == 262 and rows[0]["expected_member_return"] is None
    assert rows[1]["expected_member_return"] == -1.2 and rows[0]["reason_code"] == "hmm_risk_legal_na"


def test_fractional_ties_and_real_session_hac():
    assert h.extreme_mean([0, 1, 1, 1, 2], [0, 10, 20, 30, 40], high=True) == 40
    assert h.extreme_mean([1] * 7, list(range(7)), high=True) == 3
    cal = [str(i) for i in range(30)]
    result = h.hac({"0": 1, "10": -1}, cal)
    assert result["variance"] == 0.5
    assert h.hac({"0": 1}, cal)["mean"] is None
    # S/C main means must share the paired date set, even when C is constant on another date.
    days = {
        "0": dict(S=0.9, C=None, delta=None),
        "1": dict(S=0.2, C=0.1, delta=0.1),
        "2": dict(S=0.4, C=0.1, delta=0.3),
    }
    assert h.summarize_daily(days, cal)["full"]["S"]["mean"] == pytest.approx(0.3)


def test_stopping_rule_does_not_promote_negative_or_inconclusive():
    def stats(s, d):
        return dict(S=dict(mean=s), delta=dict(mean=d))

    assert h.decision(stats(None, None), stats(1, 1)) == "INSUFFICIENT_EVALUABLE_EVIDENCE"
    assert h.decision(stats(-0.1, 0.1), stats(1, 1)) == "STOP_NO_INCREMENTAL_ORDERING"
    assert h.decision(stats(0.1, 0.1), stats(None, None)) == "INCONCLUSIVE_TRANSFER"
    assert h.decision(stats(0.1, 0.1), stats(0.1, -0.1)) == "STOP_NO_TRANSFER_LEAD"
    assert h.decision(stats(0.1, 0.1), stats(0.1, 0.1)) == "RESEARCH_LEAD_REQUIRES_VALUE_REVIEW"


def test_evaluation_requires_sealed_predictions_before_any_source_read(monkeypatch):
    monkeypatch.setattr(h, "load_request", lambda *_: pytest.fail("must not open evaluation source"))
    sealed = receipt(dict(schema_version=h.VERSION + "_prediction_acceptance", status="PASS", fresh_processes=1))
    with pytest.raises(FormalStateError):
        h.evaluate({}, {}, {}, sealed)


def test_guard_poison_database_network_and_unapproved_fit():
    import psycopg2
    import socket
    from sklearn.linear_model import Ridge

    with cli.guard(allow_ridge=False):
        for forbidden in (
            lambda: psycopg2.connect(""),
            lambda: socket.create_connection(("localhost", 8001)),
            lambda: Ridge().fit([[0], [1]], [0, 1]),
        ):
            with pytest.raises(FormalStateError):
                forbidden()


def test_cohort_dedup_and_metrics(packet, monkeypatch):
    days, codes = ["2026-04-01", "2026-04-02"], packet["catalog"][:3]
    features = dict(
        prediction_dates=days,
        catalog=codes,
        calendar=packet["calendar"],
        rows={d: {c: dict(features=[0, i] + [0] * 18) for i, c in enumerate(codes)} for d in days},
    )
    predictions = {
        a: [
            dict(trade_date=d, sector_code=c, **{field: (i - 1) / 100 if a == "S" else i / 100})
            for d in days
            for i, c in enumerate(codes)
        ]
        for a, field in (("S", "expected_member_return"), ("C", "expected_member_loss"))
    }
    prediction = receipt(dict(predictions=predictions))
    labels = {
        d: {
            c: dict(status="AVAILABLE", reason=None, B=max(0, (1 - i) / 100), C=0.02, upside=0.02 + (i - 1) / 100)
            for i, c in enumerate(codes)
        }
        for d in days
    }
    cohort = dict(
        rows=[
            dict(date=d, stock=str(i), code=c, stock_return=(i - 1) / 100, reason=None, seed=s)
            for s in (42, 2024, 2026)
            for d in days
            for i, c in enumerate(codes)
        ],
        secondary_rows=[{"do_not_use": True}],
    )
    acceptance = dict(models={"2026Q2": dict(S=dict(training_target_mean=0))})
    result = h.score_outcomes(prediction, labels, features, acceptance, cohort)
    assert result["cohort_unique_keys"] == 6 and result["cohort_source_rows"] == 18
    assert result["cohort"]["full"]["S"]["mean"] == pytest.approx(1)
    assert result["l2"]["full"]["delta"]["mean"] == pytest.approx(2)
    assert result["auxiliary"]["full"]["errors"]["mse"] < 1e-30
    assert result["status"] == "RESEARCH_LEAD_REQUIRES_VALUE_REVIEW"
    cohort["rows"][-1]["stock_return"] = 0.9
    with pytest.raises(FormalStateError, match="cross-seed"):
        h.score_outcomes(prediction, labels, features, acceptance, cohort)


def test_full_prediction_denominator_and_asof():
    calendar = pd.bdate_range("2023-01-01", "2026-10-30").strftime("%Y-%m-%d").tolist()
    future = [d for d in calendar if "2026-04-01" <= d <= "2026-10-30"]
    remove = set(future[1 : len(future) - 141])
    calendar = [d for d in calendar if d not in remove]
    days = [d for d in calendar if "2026-04-01" <= d]
    codes = [f"801{i:03d}.SI" for i in range(131)]
    asof = {d: calendar[calendar.index(d) - 1] for d in days}
    f = receipt(
        dict(
            schema_version=h.VERSION + "_features",
            contract=h.CONTRACT,
            calendar=calendar,
            catalog=codes,
            prediction_dates=days,
            as_of=asof,
            rows={d: {c: dict(features=[0.0] * 20, reason_code=None, as_of_date=asof[d]) for c in codes} for d in days},
            cached_features_sha256=h.PINS["features"],
            release_binding=dict(manifest_sha256=h.PINS["manifest"]),
            inference_identity={},
        )
    )
    h.validate_prediction_features(f)
    assert len(f["rows"]) * len(codes) == 18602
    f["as_of"][days[0]] = days[0]
    with pytest.raises(FormalStateError, match="as-of"):
        h.validate_prediction_features(seal(f))
    f["as_of"][days[0]] = calendar[calendar.index(days[0]) - 1]
    del f["rows"][days[-1]][codes[-1]]
    with pytest.raises(FormalStateError):
        h.validate_prediction_features(seal(f))


def test_q4_reuses_control_without_second_fit(packet, tmp_path, monkeypatch):
    p = seal({**packet, "vintage": h.release.Q4})
    monkeypatch.setitem(h.PINS, "mask", p["mask_sha256"])
    p = seal({**p, "feature_sha256": h.PINS["features"], "target_facts_sha256": h.PINS["training"]})
    crows = [r[:3] + [r[4]] for r in p["rows"]]
    params, stats = h.loss.fit_parameters(crows)
    control = dict(
        arm="C",
        parameters=params,
        model_sha256=h.PINS["control_model"],
        **stats,
        feature_sha256=h.PINS["features"],
        training_label_sha256=h.PINS["training"],
        release_binding=p["release_binding"],
    )
    monkeypatch.setattr(h.release, "validate_model", lambda _: None)
    monkeypatch.setattr(h.loss, "fit_parameters", lambda _: pytest.fail("Q4 C must not fit"))
    result = h.fit_packet(p, control, tmp_path / "fit", tmp_path / "attempts", HEAD, NUMERIC)
    assert result["fits"] == 1 and result["models"]["C"] == control
    assert not (tmp_path / "attempts" / "2026Q4.C.json").exists()


def test_neutral_kernel_two_fresh_processes(tmp_path):
    import os
    import subprocess
    import sys

    code = """from backend.services.hmm_risk import risk_l2_loss_severity as h
from backend.services.hmm_risk.contracts import canonical_sha256
rows=[['a','x',[0.0]*20,-0.2],['b','x',[1.0]*20,1.2]]
print(canonical_sha256(h._fit_weighted_ridge(rows)))
"""
    env = {**os.environ, **dict.fromkeys(h.THREAD_VARIABLES, "1"), "PYTHONPATH": str(cli.ROOT)}
    results = [subprocess.check_output([sys.executable, "-c", code], env=env, cwd=tmp_path) for _ in (1, 2)]
    assert results[0] == results[1] and len(results[0].strip()) == 64


def test_explicit_relocated_provider_same_identity_only(tmp_path):
    from pathlib import Path
    import json

    original = Path(h.__file__).parent / "manifests" / "provider_absence_v1.json"
    relocated = tmp_path / "authority.json"
    relocated.write_bytes(original.read_bytes())
    pin = canonical_sha256(json.loads(original.read_text(encoding="utf-8")))
    cached = dict(
        provider_absence_manifest="Z:/retired-worktree/provider.json",
        provider_absence_sha256=pin,
        work_parent="Z:/old-work",
        candidate_root="X:/same-frozen-release",
    )
    current = {**cached, "provider_absence_manifest": str(relocated), "work_parent": str(tmp_path)}
    h.validate_cached_source(current, cached)
    for changed in (
        {"provider_absence_sha256": "0" * 64},
        {"candidate_root": "X:/other-release"},
        {"provider_absence_manifest": str(tmp_path / "absent.json")},
    ):
        with pytest.raises((FormalStateError, ValueError, FileNotFoundError)):
            h.validate_cached_source({**current, **changed}, cached)
    relocated.write_text("{}", encoding="utf-8")
    with pytest.raises((FormalStateError, StateModelSetError)):
        h.validate_cached_source(current, cached)
