from __future__ import annotations

from datetime import date
from fractions import Fraction
import math
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_margin_information as h
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES
from backend.services.hmm_risk.formal_state_model import FormalStateError
from scripts.hmm_risk import run_risk_l2_margin_information as cli


@pytest.fixture(autouse=True)
def no_external_actions():
    def forbidden(*_a, **_kw):
        raise AssertionError("unit test must not access DB/network or run a real fit")

    with (
        patch("socket.socket.connect", forbidden),
        patch("psycopg2.connect", forbidden),
        patch("sklearn.linear_model.Ridge.fit", forbidden),
    ):
        yield


def rows():
    return [
        [
            "2026-01-02" if i < 3 else "2026-01-05",
            f"801{i:03}.SI",
            [float(i * (j + 1)) for j in range(20)],
            0.2,
            0.01 * (i + 1),
        ]
        for i in range(4)
    ]


def label(c=0.1, u=0.2):
    return dict(status="AVAILABLE", reason=None, B=max(0.0, c - u), C=c, upside=u)


def test_observed_subset_currency_missing_and_zero_are_not_interchanged():
    result = h.aggregate_ratio([("A", 10000.0), ("B", 30000.0)], {"A": 200.0, "FUND": 9000}, unit="CNY")
    assert result["value"] == 0.02 and result["observed"] == 1 and result["observed_capital_fraction"] == 0.25
    zero = h.aggregate_ratio([("A", 10000.0)], {"A": 0.0}, unit="CNY")
    assert zero["value"] == 0 and zero["reason"] is None
    missing = h.aggregate_ratio([("A", 10000.0)], {}, unit="CNY")
    assert missing["value"] is None and missing["reason"] == "NO_OBSERVED_MARGIN_MEMBER"
    assert missing["member_sha256"] == zero["member_sha256"]
    with pytest.raises(FormalStateError, match="CNY"):
        h.aggregate_ratio([("A", 1)], {"A": 200}, unit="TEN_THOUSAND_CNY")


@pytest.mark.parametrize(
    "members,balances",
    [([("A", 0)], {"A": 1}), ([("A", 1), ("A", 2)], {}), ([("A", 1)], {"A": float("nan")}), ([("A", 1)], {"A": -1})],
)
def test_invalid_observed_facts_fail_instead_of_being_dropped(members, balances):
    with pytest.raises(FormalStateError):
        h.aggregate_ratio(members, balances, unit="CNY")


def test_financing_clock_uses_t_minus_two_and_cny_conversion_once(monkeypatch):
    calendar = ["2026-04-01", "2026-04-02", "2026-04-03"]
    source = SimpleNamespace(
        calendar=calendar,
        positions={d: i for i, d in enumerate(calendar)},
        catalog=["801011.SI"],
        members=lambda day: {"801011.SI": [("000001.SZ", 2.0)]},
        unchanged=lambda: None,
        identity=lambda: {"source": "synthetic"},
    )
    binding = SimpleNamespace(unchanged=lambda: None, evidence=lambda: {"identity": "synthetic"})
    frame = pd.DataFrame(
        {"md_rzye": [100.0]},
        index=pd.MultiIndex.from_tuples([(pd.Timestamp(calendar[0]), "000001.SZ")], names=["datetime", "instrument"]),
    )

    def observed(_binding, days):
        assert days == [calendar[0]]
        return frame

    monkeypatch.setattr(h, "margin_frame", observed)
    result = h.financing_features(source, binding, [calendar[-1]])
    assert result["source_dates"][calendar[-1]] == calendar[0]
    assert result["as_of"][calendar[-1]] == calendar[1]
    assert result["rows"][calendar[-1]]["801011.SI"]["value"] == 100 / 20000


@pytest.mark.parametrize("bad", [None, "duplicate", "nonfinite", "negative", "wrong-side"])
def test_native_h5_reader_preserves_zeros_and_checks_effective_source(tmp_path, bad):
    path = tmp_path / "margin.h5"
    source_day = pd.Timestamp("2025-02-14")
    codes = ["300114.SZ", "000001.SZ"]
    values = [0.0, 20.0]
    if bad == "duplicate":
        codes = ["300114.SZ", "300114.SZ"]
    if bad == "nonfinite":
        values[0] = float("nan")
    if bad == "negative":
        values[0] = -1.0
    if bad == "wrong-side":
        codes[0] = "302132.SZ"
    pd.DataFrame(
        {"md_rzye": values},
        index=pd.MultiIndex.from_tuples([(source_day, c) for c in codes], names=["datetime", "instrument"]),
    ).to_hdf(path, key="data", format="table", data_columns=True)
    calls = []

    def canonical(code, day):
        calls.append((code, day))
        h.require(code == "300114.SZ" and day == date(2025, 2, 14), "wrong effective source")
        return "302132.SZ"

    binding = SimpleNamespace(
        margin_path=path,
        canonical_code=canonical,
        unchanged=lambda: None,
        identity=SimpleNamespace(
            rows=[
                SimpleNamespace(
                    source_dataset=h.MARGIN_SOURCE, source_ts_code="300114.SZ", canonical_ts_code="302132.SZ"
                )
            ]
        ),
    )
    if bad is not None:
        with pytest.raises(FormalStateError):
            h.margin_frame(binding, ["2025-02-14"])
    else:
        result = h.margin_frame(binding, ["2025-02-14"])
        assert result.loc[(source_day, "302132.SZ"), "md_rzye"] == 0
        assert calls == [("300114.SZ", date(2025, 2, 14))]


def test_matched_20d_scalers_weights_and_single_new_dimension_are_exact():
    original, values = rows(), [0.1, 0.2, 0.4, 0.3]
    z0, y0, w0, s0 = h.design(original, values, "C20")
    z1, y1, w1, s1 = h.design(original, values, "M")
    assert h.CONTRACT["feature_names"] == [*ALL_CORE_FEATURES, "margin_balance_ratio"] and len(ALL_CORE_FEATURES) == 20
    assert np.array_equal(z0, z1[:, :20]) and np.array_equal(y0, y1) and np.array_equal(w0, w1)
    assert w0[3] == 3 * w0[0]
    assert all(s0[k] == s1[k][:20] for k in s0)
    assert s1["mean"][-1] == np.mean(values) and s1["scale"][-1] == np.std(values, ddof=0)
    assert h.design(original, [0.1] * 4, "M")[3]["active"][-1] is False
    with pytest.raises(FormalStateError, match="financing feature"):
        h.design(original, [0.1, 0.2, float("nan"), 0.3], "M")


def test_fit_uses_identical_c_labels_and_ledger_stops_retries(tmp_path, monkeypatch):
    calls = []

    class FakeRidge:
        def __init__(self, **parameters):
            assert parameters == h.loss.PARAMS

        def fit(self, z, y, sample_weight):
            calls.append((z.copy(), y.copy(), sample_weight.copy()))
            self.coef_, self.intercept_ = np.ones(z.shape[1]), 0.02
            return self

    monkeypatch.setattr("sklearn.linear_model.Ridge", FakeRidge)
    monkeypatch.setattr(h, "load_packet", lambda _: rows())
    monkeypatch.setattr(h.prior, "validate_numeric", lambda _: None)
    wrapper = h.receipt(
        dict(
            vintage=h.prior.VINTAGES[0],
            source_commit="a" * 40,
            margin_values=[0.1, 0.2, 0.4, 0.3],
            reused_control=None,
            finance_sha256="b" * 64,
            mask_sha256="c" * 64,
        )
    )
    result = h.fit_packet(wrapper, tmp_path / "out", tmp_path / "attempts", "a" * 40, {})
    assert result["fits"] == 2 and len(calls) == 2 and np.array_equal(calls[0][1], calls[1][1])
    assert np.array_equal(calls[0][2], calls[1][2])
    with pytest.raises(FileExistsError):
        h.fit_packet(wrapper, tmp_path / "other-out", tmp_path / "attempts", "a" * 40, {})
    assert len(calls) == 2


def test_failed_kernel_consumes_attempt_before_fit_and_cannot_retry(tmp_path, monkeypatch):
    calls = []

    class FailedRidge:
        def __init__(self, **_kw):
            pass

        def fit(self, *_a, **_kw):
            calls.append(1)
            raise RuntimeError("injected failure")

    monkeypatch.setattr("sklearn.linear_model.Ridge", FailedRidge)
    monkeypatch.setattr(h, "load_packet", lambda _: rows())
    monkeypatch.setattr(h.prior, "validate_numeric", lambda _: None)
    wrapper = h.receipt(
        dict(
            vintage=h.prior.VINTAGES[0],
            source_commit="a" * 40,
            margin_values=[0.1, 0.2, 0.4, 0.3],
            reused_control=None,
            finance_sha256="b" * 64,
            mask_sha256="c" * 64,
        )
    )
    for target, error in [("one", RuntimeError), ("two", FileExistsError)]:
        with pytest.raises(error):
            h.fit_packet(wrapper, tmp_path / target, tmp_path / "attempts", "a" * 40, {})
    assert len(calls) == 1


def test_budget_is_request_bound_not_output_directory(tmp_path):
    p = dict(
        fit_ledger_root=str(tmp_path / "ledger"),
        request_sha256="a" * 64,
        receipt_sha256="b" * 64,
        attempts=[dict(process=1, quarter="2026Q2", arm="M")],
    )
    cli.claim_budget(p, "a" * 40)
    with pytest.raises(FileExistsError):
        cli.claim_budget(p, "a" * 40)


def test_fractional_boundary_ties_and_opportunity_cost_algebra():
    scores = {str(i): 2.0 if i < 3 else 1.0 for i in range(7)}
    weights = h.risk_weights(scores)
    assert sum(weights.values()) == 2 and all(weights[str(i)] == 2 / 3 for i in range(3))
    result = h.paired_value(
        {"M": {"A": 1, "B": 0}, "C20": {"A": 0, "B": 1}}, {"A": label(0.2, 0), "B": label(0.05, 0.3)}
    )
    assert result["avoided_loss"] == pytest.approx(0.15 / 131)
    assert result["missed_upside"] == pytest.approx(-0.3 / 131)
    assert result["J"] == pytest.approx(0.45 / 131)


def test_unavailable_and_immature_labels_do_not_reselect_or_manufacture_zero():
    weights = {"M": {"A": 1, "B": 0, "C": 0}, "C20": {"A": 0, "B": 1, "C": 0}}
    na = dict(status="UNAVAILABLE", reason="FIXED_COHORT_ENDPOINT_NA", B=None, C=None, upside=None)
    result = h.paired_value(weights, {"A": na, "B": label(), "C": na})
    assert result["J"] is None and result["reason"] == "FIXED_COHORT_ENDPOINT_NA"
    cancelled = {"M": {"A": 1, "C": 0}, "C20": {"A": 1, "C": 0}}
    assert h.paired_value(cancelled, {"A": label(), "C": na})["J"] == 0
    immature = {k: dict(status="OUTCOME_NOT_MATURE", reason="OUTCOME_NOT_MATURE") for k in cancelled["M"]}
    assert h.paired_value(cancelled, immature)["J"] is None
    assert h.risk_weights({"A": 1.0, "B": 1.0}) is None


def test_full_grid_and_ex_ante_common_set_do_not_consult_outcomes():
    pred = dict(
        rows=[
            dict(trade_date="D", sector_code=c, M=m, C20=s)
            for c, m, s in [("A", 3.0, 1.0), ("B", 1.0, 3.0), ("C", None, 2.0)]
        ]
    )
    groups = h.freeze_groups(pred, ["D"], ["A", "B", "C"])
    assert set(groups["D"]["M"]) == {"A", "B"}
    assert groups["D"]["M"] == {"A": 1.0, "B": 0.0}
    with pytest.raises(FormalStateError, match="grid"):
        h.freeze_groups(dict(rows=pred["rows"][:-1]), ["D"], ["A", "B", "C"])


def test_all_l2_denominators_ties_use_exact_boundary_not_float_residual():
    for n in range(2, 132):
        scores = {str(i): float(i // 7) for i in range(n)}
        if len(set(scores.values())) == 1:
            continue
        weights = h.risk_weights(scores)
        quota = math.ceil(0.20 * n)
        higher = 0
        for score in sorted(set(scores.values()), reverse=True):
            keys = [key for key, value in scores.items() if value == score]
            expected = Fraction(min(max(0, quota - higher), len(keys)), len(keys))
            assert all(weights[key] == float(expected) for key in keys)
            higher += len(keys)


def test_unapproved_request_stops_before_loading_legacy_or_market_data(monkeypatch):
    monkeypatch.setattr(h.member, "pinned_json", lambda *_a: dict(schema_version=h.VERSION + "_request", contract={}))
    monkeypatch.setattr(h.prior, "load_prepared", lambda *_a: pytest.fail("must reject before reading carriers"))
    with pytest.raises(FormalStateError, match="unapproved"):
        h.load_request("unused", "a" * 64)


def test_unsealed_evaluation_stops_before_any_source_read(monkeypatch):
    monkeypatch.setattr(h, "load_request", lambda *_a: pytest.fail("must not read evaluation sources"))
    with pytest.raises((FormalStateError, KeyError)):
        h.evaluate({}, {}, h.receipt({}), h.receipt(dict(status="PENDING")), "a" * 40)
