"""Economic target, causal source and paired execution protections."""

from copy import deepcopy
from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_member_loss as h
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from backend.services.hmm_risk.formal_state_executor import write_once


def member(symbol, weight, value, reason=None):
    return dict(symbol=symbol, weight=weight, value=value, reason=reason)


def test_offsetting_winners_do_not_erase_member_loss_and_singleton_is_equal():
    label = h.pair_target([member("000001.SZ", 3.0, -0.2), member("000002.SZ", 1.0, 0.6)])
    assert label["B"] == pytest.approx(0)
    assert label["C"] == pytest.approx(0.15)
    assert label["upside"] == pytest.approx(0.15)
    assert h.pair_target([member("000001.SZ", 1.0, -0.2)])["B"] == pytest.approx(0.2)
    assert h.pair_target([])["reason"] == "NO_ASOF_MEMBER"


def test_legal_suspension_is_label_na_without_dropping_or_reweighting():
    out = h.pair_target(
        [member("000001.SZ", 9.0, -0.1), member("000002.SZ", 1.0, None, "LEGAL_SUSPENSION_ENDPOINT_NA")]
    )
    assert out["B"] is None and out["C"] is None
    assert out["missing_members"] == ["000002.SZ"] and out["missing_weight_fraction"] == pytest.approx(0.1)
    for bad in [
        member("000001.SZ", 1.0, None, "UNKNOWN_MISSING"),
        member("000001.SZ", 0.0, 0.1),
        member("000001.SZ", True, 0.1),
        member("000001.SZ", 1.0, float("nan")),
    ]:
        with pytest.raises(FormalStateError):
            h.pair_target([bad])


def test_circ_mv_keeps_invalid_latest_event_and_never_uses_future():
    frame = pd.DataFrame(
        {"db_circ_mv": [10.0, np.nan, 99.0]}, index=pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-05"])
    )
    values = h.prior_values(frame["db_circ_mv"], [date(2025, 12, 31), date(2026, 1, 1), date(2026, 1, 3)])
    assert np.isnan(values[0]) and values[1] == 10 and np.isnan(values[2])


def test_maturity_is_resolved_before_source_endpoint_reads():
    calendar = pd.bdate_range("2026-03-01", "2026-04-30").strftime("%Y-%m-%d").tolist()
    plan = h.target_dates(calendar, ["2026-03-16", "2026-03-31"], "2026-03-31")
    assert plan["mature"] == ["2026-03-16"] and plan["immature"] == ["2026-03-31"]
    assert plan["last_outcome"] == "2026-03-30"


def test_paired_fit_shares_mask_and_scaler_and_counts_failed_fit():
    rows = [
        ("a", "x", [0.0] + [1.0] * 19, 0.0, 0.1),
        ("b", "x", [1.0] + [1.0] * 19, 0.1, 0.2),
        ("b", "y", [2.0] + [1.0] * 19, 0.2, 0.3),
    ]
    attempts = []
    params, stats = h.fit_pair(rows, lambda arm: attempts.append(arm))
    assert attempts == ["B", "C"]
    assert params["B"]["mean"] == params["C"]["mean"] and params["B"]["scale"] == params["C"]["scale"]
    assert stats["B"]["sample_weight_sum"] == pytest.approx(3)
    assert stats["C"]["training_loss_mean"] > stats["B"]["training_loss_mean"]


def test_paired_ic_uses_same_dates_and_does_not_fill_undefined():
    rows = [
        dict(date="a", stock="x", code="s", loss=0.1, B=0.1, C=0.3, A=0.2, stock_return=-0.1, seed=42),
        dict(date="a", stock="y", code="t", loss=0.3, B=0.3, C=0.1, A=0.4, stock_return=-0.3, seed=42),
        dict(date="b", stock="x", code="s", loss=0.0, B=0.1, C=0.3, A=0.2, stock_return=0.1, seed=42),
    ]
    result = h.cohort_metrics(rows)
    assert result["common_defined_days"] == 1 and result["delta_ic"] == pytest.approx(-2)
    assert result["undefined_dates"] == ["b"]
    assert result["daily"][1]["undefined_reasons"] == dict(B="INSUFFICIENT_STOCKS", C="INSUFFICIENT_STOCKS")
    assert result["old_A_same_sample"]["defined_dates"] == 1
    assert result["old_A_same_sample"]["delta_C_minus_A"] == pytest.approx(-2)
    assert result["disposition"] == "STOP_NO_MEMBER_LOSS_INCREMENT"
    duplicate = deepcopy(rows[0])
    duplicate["seed"] = 2024
    assert h.cohort_metrics(rows + [duplicate])["unique_rows"] == 3
    duplicate["code"] = "unknown"
    with pytest.raises(FormalStateError):
        h.cohort_metrics(rows + [duplicate])


def test_empty_repeat_or_budget_mismatch_cannot_pass():
    first = receipt(
        dict(
            contract=h.CONTRACT,
            fits=4,
            source_commit="a" * 40,
            prepared_sha256="b" * 64,
            sealed=[],
            evaluation={},
            numeric_environment={},
        )
    )
    with pytest.raises(FormalStateError):
        h.close_processes(first, first, "a" * 40, "b" * 64)
    second = receipt({**{k: v for k, v in first.items() if k != "receipt_sha256"}, "fits": 3})
    with pytest.raises(FormalStateError):
        h.close_processes(first, second, "a" * 40, "b" * 64)


@pytest.fixture
def sample(monkeypatch):
    codes = [f"801{i:03d}.SI" for i in range(131)]
    plans = {
        "2026Q2": dict(train=["2026-03-16", "2026-03-31"], fit=["2026-03-16"], predict=["2026-04-01"]),
        "2026Q3": dict(train=["2026-06-15", "2026-06-30"], fit=["2026-06-15"], predict=["2026-07-01"]),
    }
    monkeypatch.setattr(h.rolling, "schedule", lambda _, v: plans[v["id"]])
    cal = pd.bdate_range("2026-03-13", "2026-07-20").strftime("%Y-%m-%d").tolist()
    ds = sorted({d for p in plans.values() for d in p["train"] + p["predict"]})
    binding = dict(manifest_sha256=h.loss.MANIFEST_PIN)
    features = receipt(
        dict(
            calendar=cal,
            catalog=codes,
            as_of={d: cal[cal.index(d) - 1] for d in ds},
            source=dict(release_binding=binding),
            model_identity=dict(feature_definition_sha256="b" * 64),
            rows={
                d: {
                    c: dict(as_of_date=cal[cal.index(d) - 1], features=[i / 131] + [0.0] * 19, reason_code=None)
                    for i, c in enumerate(codes)
                }
                for d in ds
            },
        )
    )
    monkeypatch.setattr(h.loss, "FEATURE_PIN", features["receipt_sha256"])
    source = dict(
        release_identity=dict(dataset_manifest_sha256=h.loss.MANIFEST_PIN),
        target_contract=h.TARGET,
        read_inventory_sha256="c" * 64,
        source_inventory_sha256="d" * 64,
        industry_authority_sha256="e" * 64,
    )
    labels = {}
    for v in h.rolling.VINTAGES:
        p = plans[v["id"]]
        labels[v["id"]] = receipt(
            dict(
                schema_version=h.VERSION + "_train",
                contract=h.CONTRACT,
                vintage=v,
                feature_sha256=features["receipt_sha256"],
                original_label_sha256=h.loss.LABEL_PINS[v["id"]],
                labels={
                    p["train"][0]: {
                        c: dict(status="AVAILABLE", reason=None, B=i / 1310, C=i / 1310 + 0.01)
                        for i, c in enumerate(codes)
                    },
                    p["train"][1]: {
                        c: dict(status="OUTCOME_NOT_MATURE", reason="OUTCOME_NOT_MATURE", B=None, C=None) for c in codes
                    },
                },
                original_status={
                    p["train"][0]: dict.fromkeys(codes, "AVAILABLE"),
                    p["train"][1]: dict.fromkeys(codes, "OUTCOME_NOT_MATURE"),
                },
            )
        )
    return features, labels, source


def fit(sample):
    f, t, s = sample
    return h.fit_vintage(f, t["2026Q2"], h.rolling.VINTAGES[0], "a" * 40, lambda _: None, member_source_identity=s)


def test_models_keep_target_identity_and_full_131_and_na(sample):
    f, _, _ = sample
    pair = fit(sample)
    assert pair["B"]["model"]["training_mask_sha256"] == pair["C"]["model"]["training_mask_sha256"]
    for arm in h.ARMS:
        model = pair[arm]["model"]
        assert len(pair[arm]["predictions"]) == 131
        assert "probability" not in pair[arm]["predictions"][0]
        with pytest.raises(FormalStateError):
            h.loss.validate_model(model)
    rows = deepcopy({d: f["rows"][d] for d in ["2026-04-01"]})
    rows["2026-04-01"][f["catalog"][0]].update(features=None, reason_code="hmm_risk_quote_unavailable")
    out, _ = h.predict(pair["C"]["model"], rows, ["2026-04-01"], f["as_of"], f["calendar"])
    assert out[0]["expected_member_loss"] is None and len(out) == 131
    with pytest.raises(FormalStateError):
        h.predict(pair["C"]["model"], rows, ["2026-07-01"], f["as_of"], f["calendar"])


def test_future_features_and_unmature_labels_cannot_enter_fit(sample):
    f, t, s = sample
    first = fit(sample)
    changed = deepcopy(f)
    changed["rows"]["2026-07-01"][f["catalog"][0]]["features"][0] = 1e8
    second = h.fit_vintage(
        changed, t["2026Q2"], h.rolling.VINTAGES[0], "a" * 40, lambda _: None, member_source_identity=s
    )
    assert first["C"]["model"]["parameters"] == second["C"]["model"]["parameters"]
    bad = deepcopy(t["2026Q2"])
    bad["labels"]["2026-03-31"][f["catalog"][0]]["B"] = 0.1
    bad = receipt({k: v for k, v in bad.items() if k != "receipt_sha256"})
    attempts = []
    with pytest.raises(FormalStateError):
        h.fit_vintage(f, bad, h.rolling.VINTAGES[0], "a" * 40, lambda a: attempts.append(a), member_source_identity=s)
    assert attempts == []


def test_kernel_failure_is_counted_once_and_no_second_arm_runs(monkeypatch):
    attempts = []

    def fail(_):
        raise RuntimeError("kernel failed")

    monkeypatch.setattr(h.loss, "fit_parameters", fail)
    with pytest.raises(RuntimeError):
        h.fit_pair([("d", "a", [0.0] * 20, 0.1, 0.2), ("d", "b", [1.0] * 20, 0.2, 0.3)], attempts.append)
    assert attempts == ["B"]


def test_source_resolves_shared_alias_at_asof_without_renumbering():
    source = h.FrozenMemberSource.__new__(h.FrozenMemberSource)
    source.calendar = ["2026-01-02", "2026-01-05"]
    source.positions = {d: i for i, d in enumerate(source.calendar)}
    source.catalog = ["801783.SI"]
    source.as_of_pos = {"2026-01-02": 0}
    source.suspended = set()
    source.spans = {"000001.SZ": ((date(2020, 1, 1), date(2026, 9, 30)),)}
    source.projection = SimpleNamespace(resolve=lambda *_: SimpleNamespace(status="resolved", l2_code="801783.SI"))
    source.security = SimpleNamespace(resolve=lambda *_: SimpleNamespace(source_ts_code="OLD_CODE"))
    source.cap = {"OLD_CODE": np.asarray([10.0])}
    source.source_aliases = h.Counter()
    assert source.members("2026-01-05") == {"801783.SI": [("000001.SZ", 10.0)]}
    assert source.source_aliases == {"000001.SZ->OLD_CODE": 1}
    source.cap["OLD_CODE"][0] = np.nan
    with pytest.raises(FormalStateError):
        source.members("2026-01-05")


@pytest.mark.parametrize("mode", ["frozen_file", "database_daily"])
def test_dual_entry_validates_source_and_never_refits(sample, tmp_path, monkeypatch, mode):
    from backend.services.hmm_risk import risk_l2_database_inference as db

    f, _, _ = sample
    model = fit(sample)["C"]["model"]
    request = dict(decision_dates=["2026-04-01"], source=dict(candidate_root=str(tmp_path), calendar_sha256="b" * 64))
    bundle = receipt(
        dict(
            rows={"2026-04-01": f["rows"]["2026-04-01"]},
            catalog=f["catalog"],
            as_of=f["as_of"],
            model_identity=f["model_identity"],
            inference_identity=dict(calendar=f["calendar"]),
        )
    )

    def validate(_r, b, pin):
        h.core.verify_receipt(b, pin)

    consumer = h.core if mode == "frozen_file" else db
    monkeypatch.setattr(consumer, "validate_bundle", validate)
    monkeypatch.setattr(h.core, "date_plan", lambda _: None)
    calendar = tmp_path / "calendar.txt"
    calendar.write_text("\n".join(f["calendar"]), encoding="utf-8")
    monkeypatch.setattr(h.core.inputs.reader, "_require_file", lambda *_: calendar)
    result = h.infer_prepared(
        model,
        request,
        bundle,
        source_mode=mode,
        expected_model_sha256=model["receipt_sha256"],
        expected_bundle_sha256=bundle["receipt_sha256"],
    )
    assert len(result["predictions"]) == 131 and result["fits"] == 0 and not result["outcomes_read"]
    with pytest.raises(FormalStateError):
        h.infer_prepared(
            model,
            request,
            bundle,
            source_mode=mode,
            expected_model_sha256="f" * 64,
            expected_bundle_sha256=bundle["receipt_sha256"],
        )


def test_four_fit_sealing_precedes_evaluation_and_failed_kernel_does_not_retry(sample, tmp_path, monkeypatch):
    f, labels, source = sample
    locs = {}
    for vintage, body in labels.items():
        path = tmp_path / f"{vintage}.json"
        write_once(path, body)
        locs[vintage] = dict(path=str(path), sha256=body["receipt_sha256"])
    facts = receipt(dict(unit="evaluation"))
    factpath = tmp_path / "outcomes.json"
    write_once(factpath, facts)
    prepared = dict(
        training_labels=locs,
        evaluation_path=str(factpath),
        evaluation_sha256=facts["receipt_sha256"],
        old_a_path="unit",
        member_source_identity=source,
    )
    monkeypatch.setattr(h, "load_prepared", lambda *_: (prepared, f))
    monkeypatch.setattr(h, "pinned_json", lambda *_: {})
    monkeypatch.setattr(h, "evaluate", lambda *_: {})
    output = tmp_path / "out"
    read = h.core.inputs._asset

    def audited(path, pin):
        if path == locs["2026Q3"]["path"]:
            assert (output / "2026Q2_B.sealed.json").exists() and (output / "2026Q2_C.sealed.json").exists()
        if path == str(factpath):
            assert len(list(output.glob("*.sealed.json"))) == 4
        return read(path, pin)

    monkeypatch.setattr(h.core.inputs, "_asset", audited)
    assert h.execute_child("unit", "b" * 64, output, "a" * 40)["fits"] == 4
    assert len(list(output.glob("*.attempt.json"))) == 4

    def fail(_):
        raise RuntimeError("kernel failure")

    monkeypatch.setattr(h.loss, "fit_parameters", fail)
    failed = tmp_path / "failed"
    with pytest.raises(RuntimeError):
        h.execute_child("unit", "b" * 64, failed, "a" * 40)
    assert len(list(failed.glob("*.attempt.json"))) == 1 and not list(failed.glob("*.sealed.json"))


def test_bin_price_reader_uses_existing_integer_li_and_preserves_legal_missing(tmp_path):
    source = h.FrozenMemberSource.__new__(h.FrozenMemberSource)
    source.positions = {"2026-01-02": 0, "2026-01-05": 1, "2026-01-06": 2}
    source.quote_arrays = {}
    source.spans = {"000001.SZ": ((date(2020, 1, 1), date(2026, 9, 30)),)}
    source.suspended = {(date(2026, 1, 6), "000001.SZ")}
    source.encoding = SimpleNamespace(scale=lambda *_: 1.0)
    for field, values in [("close", [0, 2.468, float("nan"), float("nan")]), ("factor", [0, 2, 2, 2])]:
        (tmp_path / f"{field}.day.bin").write_bytes(np.asarray(values, dtype="<f4").tobytes())
    source.asset = lambda rel: tmp_path / rel.rsplit("/", 1)[-1]
    assert source.price("000001.SZ", "2026-01-02") == (1.234, None)
    assert source.price("000001.SZ", "2026-01-06") == (None, "LEGAL_SUSPENSION_ENDPOINT_NA")
    with pytest.raises(FormalStateError):
        source.price("000001.SZ", "2026-01-05")


def test_old_prediction_grid_must_be_complete_before_fit(sample):
    f, _, _ = sample
    rows = [dict(trade_date=d, sector_code=c) for d in ["2026-04-01", "2026-07-01"] for c in f["catalog"]]
    h.validate_old_a(receipt(dict(contract=h.loss.CONTRACT, predictions=rows)), f)
    with pytest.raises(FormalStateError):
        h.validate_old_a(receipt(dict(contract=h.loss.CONTRACT, predictions=rows[:-1])), f)


def test_acceptance_requires_four_real_models_same_mask_and_complete_grid(sample, monkeypatch):
    f, t, s = sample
    sealed = [
        h.fit_vintage(f, t[v["id"]], v, "a" * 40, lambda _: None, member_source_identity=s) for v in h.rolling.VINTAGES
    ]
    repeat = receipt(
        dict(
            contract=h.CONTRACT,
            fits=4,
            source_commit="a" * 40,
            prepared_sha256="b" * 64,
            sealed=sealed,
            evaluation={},
            numeric_environment={},
        )
    )
    monkeypatch.setattr(h, "EXPECTED_PREDICTION_DAYS", 2)
    assert h.close_processes(repeat, repeat, "a" * 40, "b" * 64)["completed_fits"] == 8
    bad = deepcopy(sealed)
    bad[0]["B"]["predictions"].pop()
    bad[0]["B"] = receipt({k: v for k, v in bad[0]["B"].items() if k != "receipt_sha256"})
    broken = receipt({k: v for k, v in repeat.items() if k != "receipt_sha256"} | dict(sealed=bad))
    with pytest.raises(FormalStateError):
        h.close_processes(broken, broken, "a" * 40, "b" * 64)
