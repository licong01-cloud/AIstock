"""Direct approved reference invariants; synthetic arithmetic is not economics."""

from copy import deepcopy
from datetime import date
from fractions import Fraction
import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_holding_vix_reference as study


def calendar():
    return pd.bdate_range("2026-03-31", periods=20).strftime("%Y-%m-%d").tolist()


def stocks(ranks=(0, 0.5, 1), ends=(8, 12, 9)):
    return [
        dict(
            symbol=str(i),
            quantity=10.0,
            prior_close=10.0,
            open=10.0,
            restore_open=float(end),
            closes=[10.0, float(end)],
            rank=rank,
            missing=[],
            sector_code="801011.SI",
            no_overlay_reason=None,
            suspension_dates=[],
            limit_fill_status="UNKNOWN_NOT_SIMULATED",
        )
        for i, (rank, end) in enumerate(zip(ranks, ends, strict=True))
    ]


@pytest.mark.parametrize("ends", [(8, 12, 9), (12, 13, 11), (8, 7, 9), (10, 10, 10)])
@pytest.mark.parametrize("ranks", [(0, 0.5, 1), (0.5, 0.5, 0.5), (None, 0.5, 1)])
def test_cash_amount_cost_and_independent_fraction_arithmetic(ends, ranks):
    rows = stocks(ranks, ends)
    result = study.evaluate_basket(rows, ["2026-04-01", "2026-04-02"])
    cuts = [Fraction(0) if r is None else Fraction(str(r)) / 10 for r in ranks]
    removed = sum(100 * c for c in cuts)
    eligible = sum(100 for r in ranks if r is not None)
    lam = removed / eligible
    b = sum(10 * Fraction(end) for end in ends)
    # Independently value residual inventory + cash, rather than repeat delta formula.
    rv = sum(10 * (1 - c) * end for c, end in zip(cuts, ends, strict=True)) + removed
    xv = sum(10 * (1 - (lam if r is not None else 0)) * end for r, end in zip(ranks, ends, strict=True)) + removed
    for bp in study.COST_BPS:
        fee = removed * Fraction(bp, 10000)
        observed = result["sensitivity"][str(bp)]
        assert observed["R_minus_B"] == pytest.approx(float((rv - b - fee) / 300), abs=1e-14)
        assert observed["X_minus_B"] == pytest.approx(float((xv - b - fee) / 300), abs=1e-14)
        assert observed["R_minus_X"] == pytest.approx(float((rv - xv) / 300), abs=1e-14)
    for row, end in zip(result["restoration_10bp"], ends, strict=True):
        for arm in row["arms"].values():
            assert arm["restored_encoded_shares"] * end + arm["roundtrip_cost_10bp"] == pytest.approx(
                arm["released_cash"]
            )
            assert arm["external_cash"] == 0
    assert result == study.evaluate_basket(list(reversed(rows)), ["2026-04-01", "2026-04-02"])
    if len(set(ranks)) == 1:
        assert result["metrics"]["R_minus_X"] == pytest.approx(0, abs=1e-14)
    assert result["execution_status"] == "NOT_SIMULATED"


def collection():
    cal = calendar()
    return dict(
        schema_version="hmm_member_prediction_transport_collection_v1",
        arm="C",
        catalog=["a", "b", "c"],
        quarters=[
            dict(model_vintage=dict(id="Q"), decision_start=cal[1], decision_end=cal[-1], model_parameter_sha256="hash")
        ],
        predictions=[
            dict(
                trade_date=cal[1],
                as_of_date=cal[0],
                sector_code=c,
                vintage_id="Q",
                model_sha256="hash",
                availability="available",
                reason_code=None,
                expected_member_loss=v,
            )
            for c, v in zip(["a", "b", "c"], [0.1, 0.1, 0.5], strict=True)
        ],
    )


def test_full_catalog_average_ranks_ties_and_no_held_subset():
    obj = collection()
    ranks = study.signal_ranks(obj, calendar(), obj["catalog"])[calendar()[1]]
    assert {k: v["rank"] for k, v in ranks.items()} == {"a": 0.25, "b": 0.25, "c": 1}
    obj["predictions"].reverse()
    assert ranks == study.signal_ranks(obj, calendar(), obj["catalog"])[calendar()[1]]
    for row in obj["predictions"]:
        row["expected_member_loss"] = 0.2
    assert all(r["rank"] == 0.5 for r in study.signal_ranks(obj, calendar(), obj["catalog"])[calendar()[1]].values())
    for row in obj["predictions"][1:]:
        row.update(availability="unavailable", expected_member_loss=None, reason_code="NO_ASOF_MEMBER")
    assert all(r["rank"] is None for r in study.signal_ranks(obj, calendar(), obj["catalog"])[calendar()[1]].values())


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "asof", "model", "nonfinite", "unknown"])
def test_prediction_fail_closed(mutation):
    obj = collection()
    if mutation == "duplicate":
        obj["predictions"].append(deepcopy(obj["predictions"][0]))
    elif mutation == "missing":
        obj["predictions"].pop()
    else:
        key, value = {
            "asof": ("as_of_date", calendar()[1]),
            "model": ("model_sha256", "other"),
            "nonfinite": ("expected_member_loss", float("nan")),
            "unknown": ("sector_code", "outside"),
        }[mutation]
        obj["predictions"][0][key] = value
    with pytest.raises(ValueError):
        study.signal_ranks(obj, calendar(), obj["catalog"])


def test_episode_clock_shared_no_repeat_observation_no_restore_day_reentry_and_late():
    cal = calendar()
    days = cal[1:]
    context = {d: dict(change=0.1, us_date="same" if i < 9 else str(i)) for i, d in enumerate(days)}
    episodes, audit = study.episode_schedule(cal, days, context)
    assert [e["start"] for e in episodes] == [days[0], days[9], days[15]]
    assert [a["reason"] for a in audit[:8]] == ["START"] + ["ACTIVE_EPISODE"] * 4 + ["RESTORE"] + [
        "REUSED_VIX_OBSERVATION"
    ] * 2
    assert episodes[-1]["restore"] is None
    assert len(audit) == len(days)


def test_vix_dst_actual_publication_calendar_and_0915_no_future():
    frame = pd.DataFrame(
        dict(trade_date=[date(2026, 3, 5), date(2026, 3, 6), date(2026, 3, 9)], close=[10.0, 12.0, 9.0])
    )
    ctx = study.vix_context(frame, ["2026-03-09", "2026-03-10"])
    assert ctx["2026-03-09"]["us_date"] == "2026-03-06"
    assert ctx["2026-03-09"]["age_hours"] == 52
    assert ctx["2026-03-10"]["age_hours"] == 5
    assert ctx["2026-03-10"]["change"] == -0.25
    assert ctx["2026-03-10"]["publication_time_quality"] == "UNKNOWN"
    assert not ctx["2026-03-10"]["historical_as_known"]
    assert ctx == study.vix_context(frame.iloc[::-1], ["2026-03-09", "2026-03-10"])
    with pytest.raises(ValueError):
        study.vix_context(frame.iloc[:1], ["2026-03-10"])


def test_legal_na_and_unavailable_holdings_not_deleted_or_assumed_safe():
    rows = stocks((None, 0.5, 1))
    rows[0]["missing"] = [dict(day="2026-04-01", reason="LEGAL_SUSPENSION_ENDPOINT_NA")]
    result = study.evaluate_basket(rows, ["a", "b"])
    assert result["status"] == "PAIRED_VALUATION_UNAVAILABLE" and len(result["stocks"]) == 3
    assert result["metrics"] is None
    assert study.evaluate_basket([], ["a"])["status"] == "NO_PRIOR_HOLDING"
    assert study.evaluate_basket(stocks((None, None, None)), ["a", "b"])["status"] == "NO_ELIGIBLE_REDUCTION"
    with pytest.raises(ValueError):
        study.evaluate_basket(stocks((0, 0.5, 1.1)), ["a", "b"])


def test_only_common_seed_episode_mean_and_diagnostic_exclusion():
    result = study.evaluate_basket(stocks(), ["a", "b"])
    records = [dict(seed=seed, start=day, **result) for day in ("2026-04-01", "2026-07-01") for seed in study.SEEDS]
    records[0] = dict(records[0], status="NO_PRIOR_HOLDING", metrics=None)
    summary = study.summarize(records, [dict(start=d) for d in ("2026-04-01", "2026-07-01")])
    assert summary["common_paired_episodes"] == 1
    assert summary["segments"]["Q2"] is None
    assert summary["remove_largest_absolute_R_minus_X"]["means"] is None
    assert summary["economic_adoption"] is False


def test_price_missing_is_not_filled_and_unknown_fails():
    class Source:
        positions = {"d": 0}

        def _arrays(self, symbol):
            return 0, np.array([0.0, np.nan]), np.array([0.0, 1.0])

        def price(self, symbol, day, adjusted):
            return None, "LEGAL_SUSPENSION_ENDPOINT_NA"

    source = Source()
    prices = study.ReferencePrices(source)
    prices.opening["s"] = np.array([0.0, np.nan])
    assert prices.open("s", "d") == (None, "LEGAL_SUSPENSION_ENDPOINT_NA")
    source.price = lambda *args: (1.0, None)
    with pytest.raises(ValueError, match="unexplained open"):
        prices.open("s", "d")


def test_cli_zero_compute_poison_guard():
    from scripts.hmm_risk.run_risk_l2_holding_vix_reference import zero_compute
    from sklearn.linear_model import Ridge
    import psycopg2

    with zero_compute():
        for operation in (
            lambda: Ridge().fit([[1]], [1]),
            lambda: Ridge().predict([[1]]),
            lambda: psycopg2.connect(""),
            lambda: study.member.FrozenMemberSource.targets(None, [], None),
        ):
            with pytest.raises((ValueError, RuntimeError)):
                operation()


def test_held_pit_exit_preserved_and_classification_conflict_fails():
    cal = calendar()
    projection = SimpleNamespace(
        status="unavailable", reason_code="classification:outside_authority", classification_receipt_hash="pin"
    )
    source = SimpleNamespace(
        calendar=cal,
        positions={d: i for i, d in enumerate(cal)},
        spans={"s": []},
        suspended=set(),
        catalog=["a"],
        projection=SimpleNamespace(resolve=lambda *args: projection),
        price=lambda *args: (10.0, None),
    )
    prices = study.ReferencePrices(source)
    prices.open = lambda *args: (10.0, None)
    holdings = [dict(symbol="s", quantity=1.0, prior_close=10.0)]
    rows, path = prices.stocks(holdings, cal[1], cal[6], {"a": dict(rank=1.0, reason=None)})
    assert len(path) == 5 and rows[0]["rank"] is None
    assert rows[0]["no_overlay_reason"] == "HELD_CLASSIFICATION_UNAVAILABLE"
    assert study.evaluate_basket(rows, path)["status"] == "NO_ELIGIBLE_REDUCTION"
    projection.status = "ambiguous"
    with pytest.raises(ValueError, match="ambiguous"):
        prices.stocks(holdings, cal[1], cal[6], {})
    projection.status = "resolved"
    projection.l2_code = "a"
    projection.reason_code = None
    rows, path = prices.stocks(holdings, cal[1], cal[6], {"a": dict(rank=1.0, reason=None)})
    assert rows[0]["rank"] == 1.0  # PIT exit is not a reason to delete a still-held stock.
    holdings[0]["prior_close"] = 20.0
    rows, path = prices.stocks(holdings, cal[1], cal[6], {"a": dict(rank=1.0, reason=None)})
    assert rows[0]["missing"][0]["reason"] == "VALUATION_BASIS_UNRESOLVED"


class _Position:
    def get_stock_list(self):
        return ["s"]

    def get_stock_amount(self, symbol):
        return 2.0

    def get_stock_price(self, symbol):
        return 10.0


def test_native_previous_close_not_current_buy_and_hash_before_pickle(tmp_path, monkeypatch):
    root = tmp_path / "account"
    ap = root / "mlruns" / "1" / "id" / "artifacts" / "portfolio_analysis"
    ap.mkdir(parents=True)
    dates = pd.bdate_range("2026-04-01", periods=141)
    pd.to_pickle({t: _Position() for t in dates}, ap / "positions_normal_1day.pkl")
    pd.DataFrame(index=dates).to_pickle(ap / "report_normal_1day.pkl")
    for name in ("indicators_normal_1day_obj.pkl", "indicators_normal_1day.pkl"):
        (ap / name).write_bytes(b"pinned, not deserialized")
    (root / "mapping.parquet").write_bytes(b"original mapping unused for held classification")
    meta = dict(
        arm="B0",
        risk_binding=dict(release_binding=dict(manifest_sha256=study.MANIFEST)),
        assets=dict(mapping=dict(path="mapping.parquet")),
    )
    (root / "qe_l2_member_loss_manifest.json").write_text(json.dumps(meta))
    (root / "qe_current_recorder.json").write_text(json.dumps(dict(experiment_id="1", recorder_id="id")))
    (root / "qe_l2_member_loss_actions.jsonl").write_text(
        "\n".join(
            json.dumps(
                dict(
                    trade_date=str(t.date()),
                    as_of_date="previous",
                    arm="B0",
                    risk_applied=False,
                    actual_fill_claimed=False,
                    held_before=[] if i == 0 else ["s"],
                )
            )
            for i, t in enumerate(dates)
        )
    )
    paths = {
        k: ap / f
        for k, f in dict(
            positions="positions_normal_1day.pkl",
            report="report_normal_1day.pkl",
            indicators="indicators_normal_1day_obj.pkl",
            aggregate="indicators_normal_1day.pkl",
        ).items()
    }
    paths.update(actions=root / "qe_l2_member_loss_actions.jsonl", mapping=root / "mapping.parquet")

    def sha(p):
        return hashlib.sha256(p.read_bytes()).hexdigest()

    expected = {k: sha(p) for k, p in paths.items()}
    pin = dict(
        workspace=str(root),
        manifest_sha256=sha(root / "qe_l2_member_loss_manifest.json"),
        recorder=dict(experiment_id="1", recorder_id="id"),
        dates=[str(t.date()) for t in dates],
        seed=42,
        loop=7,
    )
    result = study._native_extract(pin, expected, study.MANIFEST)
    assert len(result["days"]) == 141
    assert result["days"][0]["holdings"] == []
    assert result["days"][1]["holdings"] == [dict(symbol="s", quantity=2.0, prior_close=10.0)]
    (ap / "positions_normal_1day.pkl").write_bytes(b"tampered")
    monkeypatch.setattr(pd, "read_pickle", lambda *args: pytest.fail("must validate hash before deserialization"))
    with pytest.raises(ValueError, match="hash differs"):
        study._native_extract(pin, expected, study.MANIFEST)


def test_request_unknown_parameter_and_frozen_asset_hash_fail_closed(tmp_path):
    with pytest.raises(ValueError, match="unexpected request"):
        study.execute(dict(holding_sessions=10))
    wrong = tmp_path / "wrong.json"
    wrong.write_text("{}")
    request = dict(
        schema_version=study.VERSION + "_request",
        contract=study.CONTRACT,
        features=str(wrong),
        features_file_sha256="0" * 64,
        vix_root=str(tmp_path),
        provider_absence_manifest=str(wrong),
        native_access={},
        **{k: str(wrong) for k in study.PINS},
    )
    with pytest.raises((ValueError, RuntimeError), match="identity differs"):
        study.execute(request)
