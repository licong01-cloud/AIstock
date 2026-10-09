"""Focused rolling-candidate contracts; synthetic tests are not formal fits."""

from copy import deepcopy
from datetime import date, timedelta
import json
import math
import os
from pathlib import Path
import subprocess
import sys

import pytest

from backend.services.hmm_risk import rotation_l2_rolling_return as subject
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk.contracts import canonical_sha256


def trading_calendar():
    start, end = date(2025, 1, 1), date(2026, 8, 31)
    holidays = {date(2026, 4, 6), date(2026, 5, 1), date(2026, 5, 4), date(2026, 5, 5), date(2026, 6, 19)}
    return [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5 and start + timedelta(days=i) not in holidays
    ]


def test_causal_monthly_maturity_ledger():
    calendar = trading_calendar()
    months = subject.monthly_schedule(calendar)
    assert len(months) == 5
    assert months[1]["origin"] == "2026-05-06"
    for month in months:
        i = calendar.index(date.fromisoformat(month["origin"]))
        train = [date.fromisoformat(d) for d in month["train_days"]]
        assert len(train) == 126
        assert train[-1] == calendar[i - 11]
        assert calendar[calendar.index(train[-1]) + 10] == date.fromisoformat(month["as_of"])


def reseal(value, field="input_hash"):
    return ridge.seal({k: v for k, v in value.items() if k != field}, field)


def synthetic_bundle():
    """A synthetic 131-code panel; never used as real source/preflight evidence."""
    calendar = trading_calendar()
    catalog = [f"801{i:03d}.SI" for i in range(131)]
    months = subject.monthly_schedule(calendar)
    positions = {d.isoformat(): i for i, d in enumerate(calendar)}

    def rows(days, count=4):
        output = []
        for day in days:
            t = positions[day]
            delta = {code: math.sin(j * 0.22 + t * 0.007) for j, code in enumerate(catalog)}
            scores, states = baseline._score_and_states(delta)
            for j, code in enumerate(catalog):
                output.append(
                    {
                        "trade_date": day,
                        "as_of_date": calendar[t - 1].isoformat(),
                        "sector_level": "L2",
                        "sector_code": code,
                        "sector_name": code,
                        "availability": "available",
                        "reason_code": None,
                        "structural_eligible": True,
                        "feature_eligible": True,
                        "forecast_state": states[code],
                        "rotation_score": scores[code],
                        "feature_contributions": {"moneyflow_intensity_delta_5d_rank": scores[code]},
                        "feature_diagnostics": {},
                        "x": [
                            j / 130 - 0.5,
                            scores[code],
                            math.cos(j * 0.19 + t * 0.004) * 0.4,
                            math.sin(j * 0.31 + t * 0.002) * 0.4,
                        ][:count],
                    }
                )
        return output

    output = []
    for plan in months:
        fact_days = [d.isoformat() for d in calendar if plan["train_days"][0] <= d.isoformat() <= plan["as_of"]]
        facts = {
            "sector_returns": [
                {
                    "trade_date": d,
                    "sector_code": c,
                    "quote_available": True,
                    "pct_change": (j - 65) * 0.001 + math.sin(positions[d] * 0.019) * 0.01,
                }
                for d in fact_days
                for j, c in enumerate(catalog)
            ],
            "benchmark_close": [{"trade_date": d, "close": 100.0} for d in fact_days],
        }
        output.append(
            {
                "schedule": plan,
                "training_rows": rows(plan["train_days"]),
                "training_facts": facts,
                "prediction_rows": rows(plan["prediction_days"]),
            }
        )
    fixed = ridge.seal(
        {
            "contract_hash": subject.fixed.MODEL_CONTRACT_HASH,
            "feature_names": list(subject.FEATURE_NAMES),
            "coefficients": [0.01, 0.002, -0.003, 0.001],
            "intercept": 0.0001,
            "training_sha256": "1" * 64,
            "train_rows": 100,
            "usable_train_dates": 126,
        },
        "parameter_sha256",
    )
    return ridge.seal(
        {
            "schema_version": subject.VERSION + "_input",
            "contract": subject.CONTRACT,
            "calendar": [d.isoformat() for d in calendar],
            "catalog": catalog,
            "months": output,
            "delta_rows": rows([d.isoformat() for d in calendar if subject.START <= d <= subject.END], 2),
            "source_identity": {"frozen_binding_sha256": subject.SOURCE_BINDING_SHA, "release": {"test": True}},
            "fixed_parameters": fixed,
            "numeric_environment": {"test": "synthetic_not_formal"},
            "request_sha256": "2" * 64,
            "source_commit": "c" * 40,
        },
        "input_hash",
    )


@pytest.fixture(scope="module")
def prepared():
    return synthetic_bundle()


@pytest.fixture
def panel(prepared, monkeypatch):
    monkeypatch.setattr(subject, "FIXED_PARAMETER_SHA", prepared["fixed_parameters"]["parameter_sha256"])
    monkeypatch.setattr(subject, "numeric_environment", lambda: prepared["numeric_environment"])
    return prepared


def test_training_causal_weights_and_future_labels(panel):
    calendar = [date.fromisoformat(d) for d in panel["calendar"]]
    month = panel["months"][0]
    training = subject.month_training(month, calendar, panel["catalog"])
    assert (training["usable_dates"], training["planned_dates"]) == (126, 126)
    assert math.fsum(r["weight"] for r in training["entries"]) == pytest.approx(1, abs=1e-12)
    changed = deepcopy(month)
    changed["prediction_rows"][0]["x"] = [1234.0] * 4
    assert subject.month_training(changed, calendar, panel["catalog"]) == training
    changed["training_facts"]["sector_returns"].append({"trade_date": "2026-04-01"})
    with pytest.raises(RuntimeError, match="future labels"):
        subject.month_training(changed, calendar, panel["catalog"])
    assert any(abs(r["y"]) < 0.01 for r in training["entries"])


@pytest.mark.parametrize(
    "mutation",
    [
        "calendar",
        "unknown_code",
        "future_asof",
        "nonfinite",
        "quote_finite_na",
        "quote_missing",
        "duplicate",
        "release",
        "monthly_drift",
        "evaluation_field",
    ],
)
def test_identity_and_rehashed_input_drift_fail_closed(panel, mutation):
    changed = deepcopy(panel)
    month = changed["months"][0]
    if mutation == "calendar":
        changed["calendar"].reverse()
    elif mutation == "unknown_code":
        month["training_rows"][0]["sector_code"] = "unknown"
    elif mutation == "future_asof":
        month["training_rows"][0]["as_of_date"] = month["training_rows"][0]["trade_date"]
    elif mutation == "nonfinite":
        month["training_rows"][0]["x"] = [None] * 4
    elif mutation == "quote_finite_na":
        month["training_facts"]["sector_returns"][0]["quote_available"] = False
    elif mutation == "quote_missing":
        month["training_facts"]["sector_returns"][0]["pct_change"] = None
    elif mutation == "duplicate":
        month["training_facts"]["sector_returns"].append(month["training_facts"]["sector_returns"][0])
    elif mutation == "release":
        changed["source_identity"]["frozen_binding_sha256"] = "0" * 64
    elif mutation == "monthly_drift":
        month["schedule"]["as_of"] = "2026-04-01"
    else:
        changed["evaluation_outcomes"] = []
    with pytest.raises((RuntimeError, ValueError)):
        subject.validate_input(reseal(changed))


@pytest.fixture(scope="module")
def synthetic_report(prepared):
    old_pin, old_numeric = subject.FIXED_PARAMETER_SHA, subject.numeric_environment
    try:
        subject.FIXED_PARAMETER_SHA = prepared["fixed_parameters"]["parameter_sha256"]
        subject.numeric_environment = lambda: prepared["numeric_environment"]
        first = subject.run_process(prepared, process_index=1)
        return first, reseal({**first, "process_index": 2}, "report_sha256")
    finally:
        subject.FIXED_PARAMETER_SHA, subject.numeric_environment = old_pin, old_numeric


def test_parent_authority_and_no_parent_or_control_fit(panel, synthetic_report, monkeypatch):
    from sklearn.linear_model import Ridge

    def forbidden(*a, **kw):
        raise AssertionError("parent/control attempted fit or source read")

    monkeypatch.setattr(Ridge, "fit", forbidden)
    monkeypatch.setattr(subject, "_source", forbidden)
    first, second = synthetic_report
    rows = subject.verify_processes(first, second, input_bundle=panel)
    assert len(rows) == 104 * 131
    assert first["completed_fits"] == 5
    assert len({m["parameters"]["parameter_sha256"] for m in first["models"]}) == 5
    for row in rows:
        assert row["model_as_of"] < row["fit_origin"] <= row["trade_date"]


@pytest.mark.parametrize(
    "mutation", ["coefficients", "feature_drift", "borrowed_month", "budget", "prediction", "numeric"]
)
def test_child_self_hash_is_not_authority(panel, synthetic_report, mutation):
    first, second = map(deepcopy, synthetic_report)
    for child in (first, second):
        if mutation in {"coefficients", "feature_drift"}:
            p = child["models"][0]["parameters"]
            if mutation == "coefficients":
                p["coefficients"][0] += 0.1
            else:
                p["feature_names"][0] = "fake"
            child["models"][0]["parameters"] = reseal(p, "parameter_sha256")
        elif mutation == "borrowed_month":
            child["models"][1] = child["models"][0]
        elif mutation == "budget":
            child["started_fits"] = True
        elif mutation == "prediction":
            child["predictions"][0]["rotation_score"] += 0.01
            child["prediction_sha256"] = canonical_sha256(child["predictions"])
        else:
            child["numeric_environment"] = {}
    with pytest.raises(RuntimeError):
        subject.verify_processes(reseal(first, "report_sha256"), reseal(second, "report_sha256"), input_bundle=panel)


def synthetic_outcomes(panel):
    days = [d for d in panel["calendar"] if subject.START.isoformat() <= d <= subject.END.isoformat()]
    quotes = [
        {"trade_date": d, "sector_code": c, "quote_available": True, "pct_change": (j - 65) * 0.001}
        for d in days
        for j, c in enumerate(panel["catalog"])
    ]
    return ridge.seal(
        {
            "schema_version": subject.VERSION + "_outcomes",
            "input_hash": panel["input_hash"],
            "sector_returns": quotes,
            "benchmark_close": [{"trade_date": d, "close": 100.0} for d in days],
            "synthetic_returns": {d: {c: (j - 65) * 0.00001 for j, c in enumerate(panel["catalog"])} for d in days},
            "synthetic_outcome_sha256": "3" * 64,
        },
        "outcome_sha256",
    )


def test_evaluation_full_reference_matrix_and_separate_objects(panel, synthetic_report):
    final = subject.close_processes(*synthetic_report, input_bundle=panel, facts=synthetic_outcomes(panel))
    assert final["completed_fits"] == 10
    assert set(final["reference_summary"]) == set(subject.ARMS)
    assert all(set(costs) == {"0", "5", "10", "20"} for costs in final["reference_summary"].values())
    assert len(final["paired_reference"]) == 5
    assert final["valuation_basis"] == "GROSS_SYNTHETIC_L2_REFERENCE"
    assert final["net_value_status"] == "UNASSESSED" and final["forward_confirmed"] is False
    assert all(final[k] is False for k in ("database_write", "dataset_write", "runtime_action", "QE_action"))
    for summary in final["reference_summary"].values():
        assert summary["0"]["date_count"] == 104
        assert summary["0"]["status"] == "AVAILABLE"


def test_evaluation_months_do_not_become_coverage_and_gate(panel, synthetic_report):
    rows = deepcopy(synthetic_report[0]["predictions"])
    june_days = sorted({r["trade_date"] for r in rows if r["trade_date"].startswith("2026-06")})[:4]
    for row in rows:
        if row["trade_date"] in june_days:
            row.update(
                availability="unavailable",
                feature_eligible=False,
                rotation_score=None,
                forecast_state=None,
                reason_code="hmm_risk_rotation_l2_price_history_unavailable",
            )
    result = subject.evaluate(rows, synthetic_outcomes(panel), [date.fromisoformat(d) for d in panel["calendar"]])
    june = next(b for b in result["metrics"]["diagnostic_blocks"] if b["name"] == "6")
    assert june["coverage_pass_day_share"] < 0.90
    assert result["metrics"]["coverage_sufficient"] is True
    assert result["effect_status"] == "DEVELOPMENT_EFFECT_QUALIFIED"


def test_reference_first_return_costs_and_held_na():
    from scripts.hmm_risk.rotation_l2_reference_value import cohort_reference_path, summarize_for_blocks

    days = [d.isoformat() for d in trading_calendar() if subject.START <= d <= subject.END]
    groups = {d: ["801001.SI"] for d in days[:-10]}
    quotes = {(d, "801001.SI"): 0.001 for d in days}
    quotes[days[0], "801001.SI"] = 0.9  # Entry-day return must never be booked.
    zero = cohort_reference_path(days, days[:-10], groups, quotes, cost_bps=0)
    paid = cohort_reference_path(days, days[:-10], groups, quotes, cost_bps=20)
    assert zero[0]["nav"] == pytest.approx(1.0)
    assert zero[1]["nav"] == pytest.approx(1.0001)
    assert paid[-1]["nav"] < zero[-1]["nav"]
    quotes[days[1], "801001.SI"] = None
    missing = cohort_reference_path(days, days[:-10], groups, quotes, cost_bps=0)
    result = summarize_for_blocks(missing, blocks=())
    assert result["status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert result["cumulative_return"] is None
    assert all(r["nav"] is None for r in missing[1:])


def test_synthetic_capital_depletion_not_silent_success():
    from scripts.hmm_risk.rotation_l2_reference_value import cohort_reference_path, summarize_for_blocks

    days = [d.isoformat() for d in trading_calendar() if subject.START <= d <= subject.END][:40]
    groups = {d: ["801001.SI"] for d in days[:-10]}
    quotes = {(d, "801001.SI"): -0.9999999999999999 for d in days}
    rows = cohort_reference_path(
        days, days[:-10], groups, quotes, cost_bps=0, valuation_basis="GROSS_SYNTHETIC_L2_REFERENCE"
    )
    assert rows[-1]["valuation_status"] == "REFERENCE_CAPITAL_DEPLETED"
    assert summarize_for_blocks(rows, blocks=())["cumulative_return"] is None


def test_delta_native_population_is_not_price_population(panel, synthetic_report):
    changed = deepcopy(panel)
    code = changed["catalog"][-1]
    for month in changed["months"]:
        for row in month["prediction_rows"]:
            if row["sector_code"] == code:
                row.update(
                    availability="unavailable",
                    reason_code="hmm_risk_rotation_l2_price_history_unavailable",
                    x=None,
                    rotation_score=None,
                    forecast_state=None,
                    feature_eligible=False,
                )
    changed = reseal(changed)
    first, second = map(deepcopy, synthetic_report)
    for child in (first, second):
        child["input_hash"] = changed["input_hash"]
        for row in child["predictions"]:
            if row["sector_code"] == code:
                row.update(
                    availability="unavailable",
                    reason_code="hmm_risk_rotation_l2_price_history_unavailable",
                    rotation_score=None,
                    forecast_state=None,
                    feature_eligible=False,
                    feature_contributions=None,
                )
        # Parent recomputes whole E+ projections: dropping one code changes all
        # remaining rank scores/states, so rebuild, do not fake a matching child.
        child["predictions"] = []
        for month, model in zip(changed["months"], child["models"], strict=True):
            rows = ridge.linear_predictions_for_rows(
                month["prediction_rows"], model["parameters"], term_names=subject.price.LINEAR_TERMS
            )
            for row in rows:
                row["fit_origin"] = month["schedule"]["origin"]
                row["model_as_of"] = month["schedule"]["as_of"]
            child["predictions"].extend(rows)
        child["prediction_sha256"] = canonical_sha256(child["predictions"])
    facts = synthetic_outcomes(changed)
    final = subject.close_processes(
        reseal(first, "report_sha256"), reseal(second, "report_sha256"), input_bundle=changed, facts=facts
    )
    delta = final["native_metrics"]["delta"]["evaluated_rows"]
    assert all(r["availability"] == "available" for r in delta if r["sector_code"] == code)
    assert all(n == 130 for n in final["common_population_by_date"].values())


@pytest.mark.parametrize("mode", ["source_failure", "after_ten_fits", "collision"])
def test_cli_durable_typed_failure_and_collision_are_separate(tmp_path, monkeypatch, mode):
    from scripts.hmm_risk import run_rotation_l2_rolling_return as cli
    from backend.services.hmm_risk.formal_state_executor import read_json

    output = tmp_path / "owned_run"

    def source(args):
        if mode == "source_failure":
            raise subject.fail("source differs")

    def runner(args, request, counters):
        if mode == "collision":
            subject.require(not output.exists(), "run output already exists")
        counters.update(started_fits=10, completed_fits=10)
        raise OSError("parent finalization failed")

    monkeypatch.setattr(cli, "_source", source)
    monkeypatch.setattr(cli, "_request", lambda args: {})
    monkeypatch.setattr(cli, "_run", runner)
    if mode == "collision":
        output.mkdir()
        existing = output / "preserved.json"
        existing.write_bytes(b"original")  # Test fixture, not repository editing.
    result = cli.main(
        [
            "run",
            "--request",
            str(tmp_path / "request.json"),
            "--request-sha256",
            "1" * 64,
            "--executor-commit",
            "c" * 40,
            "--input",
            str(tmp_path / "input.json"),
            "--input-sha256",
            "2" * 64,
            "--output",
            str(output),
        ]
    )
    assert result == 1
    failure = read_json(tmp_path / "owned_run.parent.failure.json")
    assert failure["status"] == "FAILED" and failure["reason_code"]
    assert failure["completed_fits"] == (10 if mode == "after_ten_fits" else 0)
    if mode == "collision":
        assert list(output.iterdir()) == [existing] and existing.read_bytes() == b"original"


def test_fixed_control_uses_own_return_model_pins(monkeypatch, tmp_path):
    from scripts.hmm_risk.rotation_l2_reference_value import RETURN_PINS

    seen = []

    def reader(path, *, variant, pins):
        assert path == tmp_path / "acceptance.json" and variant is subject.fixed
        assert pins == RETURN_PINS and pins != subject.fixed.REFERENCE_PINS
        seen.append(True)
        return {
            "acceptance_sha256": subject.FIXED_ACCEPTANCE_SHA,
            "model_hash": subject.FIXED_MODEL_SHA,
            "parameters": {"verified_fixture": True},
        }, {}

    monkeypatch.setattr(subject.price, "_reference", reader)
    assert subject._fixed_control(tmp_path / "acceptance.json") == {"verified_fixture": True}
    assert seen == [True]


def test_cli_child_fit_only_external_actions_poisoned(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from sklearn.linear_model import Ridge
    from backend.db import pg_pool
    from backend.services.hmm_risk import formal_state_model, rotation_l1_input_bundle
    from scripts.hmm_risk import run_rotation_l2_rolling_return as cli
    import psycopg2
    import socket

    fit = Ridge.fit
    observed = []

    def runner(bundle, *, process_index, progress):
        assert Ridge.fit is fit and process_index == 1
        for action in (
            pg_pool.get_conn,
            psycopg2.connect,
            socket.create_connection,
            formal_state_model.fit_entry,
            formal_state_model.causal_filter,
            rotation_l1_input_bundle.load_active_hmm_dataset_identity,
        ):
            with pytest.raises(formal_state_model.FormalStateError, match="forbidden"):
                action()
        progress("started", 1, "2026-04-01")
        progress("completed", 1, "2026-04-01")
        return {"unit_fixture": True}

    monkeypatch.setattr(cli, "_input", lambda args, request: {})
    monkeypatch.setattr(cli, "_source", lambda args: None)
    monkeypatch.setattr(cli.model, "run_process", runner)
    monkeypatch.setattr(cli, "write_once", lambda path, result: observed.append(result))
    counters = {"started_fits": 0, "completed_fits": 0}
    cli._child(SimpleNamespace(process_index=1, output=tmp_path / "unused.json"), {}, counters)
    assert counters == {"started_fits": 1, "completed_fits": 1}
    assert observed == [{"unit_fixture": True}] and Ridge.fit is fit


def test_no_side_effect_fresh_process_synthetic_math():
    # CI may use a different Python than the formal pinned environment. This
    # checks real fresh-process math on synthetic data, not formal environment acceptance.
    code = """
import json
from backend.tests.hmm_risk.test_rotation_l2_rolling_return import synthetic_bundle
from backend.services.hmm_risk import rotation_l2_rolling_return as model
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
bundle = synthetic_bundle()
model.FIXED_PARAMETER_SHA = bundle['fixed_parameters']['parameter_sha256']
model.numeric_environment = lambda: bundle['numeric_environment']
result = model.run_process(bundle, process_index=1)
print(json.dumps([result['prediction_sha256'], [m['parameters']['parameter_sha256'] for m in result['models']]]))
"""
    root = Path(__file__).resolve().parents[3]
    env = {**os.environ, **{k: "1" for k in subject.THREADS}, "PYTHONPATH": str(root)}
    first, second = [
        subprocess.check_output([sys.executable, "-c", code], cwd=root, env=env, text=True) for _ in range(2)
    ]
    assert json.loads(first) == json.loads(second)
