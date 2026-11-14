"""Direct approved earnings contract, not historical snapshot assertions."""

from datetime import date, timedelta
import hashlib
import json
import math
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.hmm_risk import earnings_information_input as inp
from backend.services.hmm_risk import risk_l2_earnings_information as h
from backend.services.hmm_risk.formal_state_executor import read_json, write_once
from backend.services.hmm_risk.formal_state_model import receipt
from scripts.hmm_risk import run_risk_l2_earnings_information as cli
from scripts.hmm_risk.run_risk_l2_loss_severity import guard


def event(ann="2023-04-28", period="2022-12-31", value=10.0, seen="2026-05-01T00:00:00+00:00", symbol="000001.SZ"):
    return dict(symbol=symbol, ann=ann, period=period, value=value, first_seen=seen, lineage="a")


def test_tied_versions_and_later_revision():
    rows = [event(), event(value=99, seen="2026-06-01T00:00:00+00:00")]
    assert inp.choose_version(rows)["value"] == 10
    assert inp.choose_version([event(), event(value=11)])["reason"] == "AMBIGUOUS_EARNINGS_VERSION"
    assert inp.choose_version([event(), event(value=None)])["reason"] == "AMBIGUOUS_EARNINGS_VERSION"
    assert inp.choose_version([event(), event()])["value"] == 10


def test_clock_and_latest_missing_does_not_backfill():
    calendar = ["2023-04-27", "2023-04-28", "2023-05-04", "2023-05-05"]
    timeline = inp.Disclosures([event(), event(ann="2023-04-29", period="2023-03-31", value=None)], calendar)
    assert timeline.at("000001.SZ", "2023-04-28")["reason"] == "NO_EARNINGS_DISCLOSURE"
    assert timeline.at("000001.SZ", "2023-05-04")["reason"] == "MISSING_EARNINGS_FIELD"


def test_stock_rank_capital_weight_and_single_member():
    members = {"a": [("s1", 1), ("s2", 3)], "b": [("s3", 1)], "c": []}
    observations = {
        "s1": {"value": -100, "reason": None},
        "s2": {"value": 0, "reason": None},
        "s3": {"value": 1e8, "reason": None},
    }
    result = h.aggregate(members, observations)
    assert result["a"]["value"] == -0.125
    assert result["b"]["value"] == 0.5
    assert result["c"]["value"] is None
    for v in observations.values():
        v["value"] = 0
    assert h.aggregate(members, observations)["a"]["value"] == 0


def test_matched_design_and_date_equal_weights():
    rows = [
        ["2023-01-01", "a", [1.0] * 20, 0.0, 0.1],
        ["2023-01-01", "b", [2.0] * 20, 0.0, 0.2],
        ["2023-01-02", "a", [3.0] * 20, 0.0, 0.0],
    ]
    a = h.design(rows, [-0.5, 0, 0.5], "C20")
    b = h.design(rows, [-0.5, 0, 0.5], "E21")
    assert all(a[3][k] == b[3][k][:20] for k in a[3])
    assert a[2].tolist() == [0.75, 0.75, 1.5]
    assert math.isclose(float(b[0][:, -1].mean()), 0, abs_tol=1e-15)


def test_pair_unknown_cancels_but_is_not_imputed():
    label = {"a": dict(status="UNAVAILABLE", reason="LEGAL_SUSPENSION_ENDPOINT_NA")}
    assert h.paired({"a": 1}, {"a": 1}, label)["J"] == 0
    assert h.paired({"a": 1}, {"a": 0}, label)["J"] is None


@pytest.mark.parametrize(
    "symbol,day,expected", [("300114.SZ", "2025-02-16", "302132.SZ"), ("302132.SZ", "2022-01-01", "302132.SZ")]
)
def test_explicit_financial_alias(symbol, day, expected):
    assert inp.canonical_symbol(symbol, date.fromisoformat(day)) == expected


def test_alias_wrong_side_fails():
    with pytest.raises(ValueError, match="alias"):
        inp.canonical_symbol("300114.SZ", date(2025, 2, 17))


@pytest.mark.parametrize("age,expected", [(252, None), (253, "STALE_EARNINGS_DISCLOSURE")])
def test_report_age_not_renewed_by_reannouncement(age, expected):
    calendar = [(date(2022, 12, 31) + timedelta(days=n)).isoformat() for n in range(260)]
    state = inp.Disclosures([event(ann=calendar[age - 1])], calendar)
    assert state.at("000001.SZ", calendar[age])["reason"] == expected


def test_event_order_is_not_raw_file_order():
    rows = [
        event(),
        event(ann="2023-05-01", period="2021-12-31", value=500),
        event(ann="2023-05-02", period="2023-03-31", value=-10),
    ]
    calendar = ["2023-05-04"]
    a, b = inp.Disclosures(rows, calendar), inp.Disclosures(list(reversed(rows)), calendar)
    assert a.events == b.events
    assert a.at("000001.SZ", "2023-05-04")["value"] == -10


def test_post_cutoff_and_invalid_fina_report_never_selected():
    assert inp.choose_version([event(seen="2026-10-31T00:00:00+08:00")]) is None
    state = inp.Disclosures([event(), event(ann="2023-05-01", period="2023-06-30", value=1000)], ["2023-05-04"])
    assert state.at("000001.SZ", "2023-05-04")["value"] == 10


@pytest.mark.parametrize(
    "ann,asof,available",
    [
        ("2023-04-28", "2023-04-28", False),
        ("2023-04-28", "2023-05-04", True),
        ("2023-04-30", "2023-05-04", True),
        ("2023-05-04", "2023-05-04", False),
    ],
)
def test_holiday_calendar_lag(ann, asof, available):
    state = inp.Disclosures([event(ann=ann)], ["2023-04-28", "2023-05-04"])
    assert (state.at("000001.SZ", asof)["value"] is not None) == available


def test_canonical_aliases_group_as_one_event():
    rows = [event(symbol=inp.canonical_symbol(s, date(2024, 1, 1))) for s in ["300114.SZ", "302132.SZ"]]
    state = inp.Disclosures(rows, ["2023-05-04"])
    assert len(state.events) == 1
    assert state.at("302132.SZ", "2023-05-04")["value"] == 10


def test_input_path_hash_schema_namespace_fail_closed(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="namespace"):
        inp.load(tmp_path, tmp_path / "a", {}, [])
    with pytest.raises(Exception, match="unavailable"):
        inp.load(tmp_path, tmp_path / "a", inp.PROJECTION, [])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(dict(schema_version="unknown")), encoding="utf8")
    with pytest.raises(ValueError, match="SHA"):
        inp.load(tmp_path, tmp_path / "a", inp.PROJECTION, [])
    monkeypatch.setattr(inp, "PACKAGE_PIN", hashlib.sha256(manifest.read_bytes()).hexdigest())
    with pytest.raises(ValueError, match="schema"):
        inp.load(tmp_path, tmp_path / "a", inp.PROJECTION, [])
    files = inp._Files(tmp_path)
    with pytest.raises(Exception, match="relative"):
        files.path("../escape")


def test_rank_population_and_missing_not_neutral():
    m = {"a": [("s", 1)], "b": [("t", 2)]}
    o = {"s": dict(value=0, reason=None), "t": dict(value=None, reason="AMBIGUOUS_EARNINGS_VERSION")}
    assert all(r["value"] is None for r in h.aggregate(m, o).values())
    with pytest.raises(Exception, match="duplicate"):
        h.aggregate({"a": [("s", 1)], "b": [("s", 2)]}, {"s": dict(value=0, reason=None)})


def sample_rows():
    return [["2023-01-01", "a", [1.0] * 20, 0.0, 0.1], ["2023-01-02", "a", [2.0] * 20, 0.0, 0.2]]


def test_constant_extra_and_nonfinite_fail_closed():
    z, _, _, s = h.design(sample_rows(), [0, 0], "E21")
    assert s["active"][-1] is False and s["scale"][-1] == 0 and z.shape[1] == 20
    with pytest.raises(Exception, match="earnings"):
        h.design(sample_rows(), [0, float("nan")], "E21")
    with pytest.raises(Exception, match="empty"):
        h.design([], [], "C20")


def test_failed_fit_consumes_attempt_and_cannot_retry(tmp_path, monkeypatch):
    wrapper = receipt(dict(vintage=h.prior.VINTAGES[0], source_commit="a" * 40, earnings_values=[0, 0.5]))
    monkeypatch.setattr(h, "load_packet", lambda _: sample_rows())
    monkeypatch.setattr(h.prior, "validate_numeric", lambda _: None)
    calls = []

    def fail(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("fit injected failure")

    monkeypatch.setattr("sklearn.linear_model.Ridge.fit", fail)
    with pytest.raises(RuntimeError, match="injected"):
        h.fit_packet(wrapper, tmp_path, "a" * 40, {})
    ledger = tmp_path / "2026Q2.C20.json"
    assert read_json(ledger)["fit_kernel_attempts"] == 1
    with pytest.raises(Exception):
        h.fit_packet(wrapper, tmp_path, "a" * 40, {})
    assert len(calls) == 1 and len(h.attempts()) == 12


def test_poison_and_no_output_overwrite(tmp_path):
    from sklearn.linear_model import Ridge
    import socket

    with guard(allow_ridge=False):
        with pytest.raises(Exception, match="unauthorized"):
            Ridge().fit([[1], [2]], [1, 2])
        with pytest.raises(Exception, match="unauthorized|forbidden"):
            socket.create_connection(("127.0.0.1", 9))
    p = tmp_path / "receipt.json"
    write_once(p, receipt(dict(ok=True)))
    before = p.read_bytes()
    with pytest.raises(Exception):
        write_once(p, receipt(dict(ok=False)))
    assert p.read_bytes() == before


def test_budget_and_dirty_source_checks_before_fit(tmp_path, monkeypatch):
    monkeypatch.setattr(
        cli,
        "asset",
        lambda *_: receipt(
            dict(
                schema_version=h.VERSION + "_preflight",
                contract=h.CONTRACT,
                status="PASS",
                source_commit="a" * 40,
                fits=0,
                evaluation_outcomes_read=False,
                attempts=h.attempts()[:-1],
                fit_ledger_root=h.FIT_LEDGER_ROOT,
            )
        ),
    )
    with pytest.raises(Exception, match="preflight"):
        cli.preflight(SimpleNamespace(preflight=tmp_path / "p", preflight_sha256="x"), "a" * 40)


def test_fractional_boundary_ties_same_exposure_and_pair_algebra():
    scores = {"a": 3, "b": 2, "c": 2, "d": 1, "e": 0, "f": -1}
    weights = h.risk_weights(scores)
    assert weights["a"] == 1 and weights["b"] == weights["c"] == 0.5 and sum(weights.values()) == 2
    labels = {c: dict(status="AVAILABLE", reason=None, C=0.1, upside=0.03, B=0.07) for c in scores}
    control = dict.fromkeys(scores, 2 / 6)
    pair = h.paired(weights, control, labels)
    assert abs(pair["J"]) < 1e-18  # Identical outcomes, not return-selected groups.
    assert h.risk_weights(dict(a=1, b=1)) is None


def test_full_grid_coverage_and_undefined_day_retained():
    days = ["2026-04-01", "2026-04-02"]
    catalog = [f"{i:06d}.SI" for i in range(131)]
    rows = [
        dict(trade_date=d, sector_code=c, C20=1.0, E21=None if i == 0 else 1.0)
        for d in days
        for i, c in enumerate(catalog)
    ]
    groups = h.freeze_groups(rows, days, catalog)
    assert len(groups) == 2 and all(g["E21"] is None and len(g["X"]) == 130 for g in groups.values())
    with pytest.raises(Exception, match="grid"):
        h.freeze_groups(rows[:-1], days, catalog)


def test_evaluation_fails_before_loading_any_outcome(monkeypatch):
    def forbidden(*a, **k):
        pytest.fail("unsealed evaluation touched source")

    monkeypatch.setattr(h, "load_source", forbidden)
    with pytest.raises(Exception):
        h.evaluate(receipt({}), receipt({}), receipt({}), receipt({}), "a" * 40)


def test_hac_uses_actual_session_gaps():
    calendar = [f"2023-01-{n:02d}" for n in range(1, 25)]
    compact = h.prior.hac({calendar[0]: 0.0, calendar[1]: 2.0}, calendar)
    gap = h.prior.hac({calendar[0]: 0.0, calendar[20]: 2.0}, calendar)
    assert compact["variance"] != gap["variance"] and gap["mean"] == 1


def test_train_maturity_and_fixed_windows():
    calendar = pd.bdate_range("2020-01-01", "2026-10-30").strftime("%Y-%m-%d").tolist()
    for v in h.prior.VINTAGES:
        plan = h.prior.schedule(calendar, v)
        assert all(calendar[calendar.index(d) + 10] <= v["train_end"] for d in plan["fit"])
        assert len(set(plan["train"]) - set(plan["fit"])) == 10


def test_dual_process_mismatch_stops():
    with pytest.raises(Exception, match="fresh-process"):
        h.close_fits([{"a": 1}, {"a": 2}], {}, {}, "a" * 40)


def test_single_pair_mean_does_not_add_a_significance_gate():
    stats = h.statistics({"2026-04-01": 0.01}, ["2026-04-01"])
    assert stats["mean"] == 0.01 and stats["interval"] is None


def test_predict_complete_142_by_131_grid_and_clipped_projection(monkeypatch):
    days = pd.bdate_range("2026-04-01", periods=142).strftime("%Y-%m-%d").tolist()
    catalog = [f"{i:06d}.SI" for i in range(131)]
    as_of = {d: (pd.Timestamp(d) - pd.offsets.BDay()).strftime("%Y-%m-%d") for d in days}
    features = receipt(
        dict(
            calendar=sorted(set(days) | set(as_of.values())),
            catalog=catalog,
            prediction_dates=days,
            as_of=as_of,
            rows={
                d: {c: dict(as_of_date=as_of[d], features=[0.0] * 20, reason_code=None) for c in catalog} for d in days
            },
        )
    )
    extra = receipt(
        dict(
            contract=h.CONTRACT,
            calendar=features["calendar"],
            catalog=catalog,
            as_of=as_of,
            rows={d: {c: dict(value=0.0, reason=None) for c in catalog} for d in days},
        )
    )
    models = {}
    for v in h.prior.VINTAGES:
        models[v["id"]] = {}
        for arm in h.ARMS:
            n = 21 if arm == "E21" else 20
            p = dict(mean=[0.0] * n, scale=[1.0] * n, active=[True] * n, coef=[0.0] * n, intercept=2.0)
            models[v["id"]][arm] = receipt(
                dict(
                    schema_version=h.VERSION + "_model",
                    contract=h.CONTRACT,
                    arm=arm,
                    vintage=v,
                    source_commit="a" * 40,
                    model_sha256=h.canonical_sha256(p),
                    parameters=p,
                    packet_sha256="b" * 64,
                    mask_sha256="c" * 64,
                    feature_sha256=extra["receipt_sha256"],
                    fit_rows=2,
                    fit_dates=2,
                )
            )
    accepted = receipt(
        dict(
            schema_version=h.VERSION + "_fit_acceptance",
            contract=h.CONTRACT,
            status="PASS",
            source_commit="a" * 40,
            fresh_processes=2,
            completed_fits=12,
            models=models,
        )
    )
    monkeypatch.setattr(h.prior, "validate_prediction_features", lambda _: None)
    result = h.predict(accepted, features, extra, "a" * 40)
    assert len(result["rows"]) == 18602
    assert all(r["raw_E21"] == 2 and r["E21"] == 1 and r["availability"]["C20"] == "AVAILABLE" for r in result["rows"])
    assert all(g["E21"] is None for g in result["ex_ante_groups"].values())
