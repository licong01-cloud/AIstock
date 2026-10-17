"""Synthetic contract tests; these are not the approved formal two-fit run."""

from copy import deepcopy
from datetime import date
import json
import math
import os
from pathlib import Path
import subprocess
import sys

import pytest
from threadpoolctl import threadpool_limits

from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as subject
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk.formal_state_executor import write_once
from backend.services.hmm_risk.rotation_l2_prediction import rows_from_acceptance, _validate_batch, _validate_row
from backend.tests.hmm_risk import test_rotation_l2_moneyflow_supervised as existing_tests

panel, completed = existing_tests.panel, existing_tests.completed


def reseal(bundle):
    bundle["price_features"] = ridge.seal(
        {k: v for k, v in bundle["price_features"].items() if k != "price_sha256"}, "price_sha256"
    )
    bundle["source"] = ridge.seal({k: v for k, v in bundle["source"].items() if k != "input_hash"}, "input_hash")
    return ridge.seal({k: v for k, v in bundle.items() if k != "input_hash"}, "input_hash")


@pytest.fixture(scope="module")
def reference_assets(completed, tmp_path_factory):
    old_first, old_second, _, acceptance = completed
    reference = tmp_path_factory.mktemp("synthetic_reference")
    for name, payload in (
        ("acceptance.json", acceptance),
        ("process_1.json", old_first),
        ("process_2.json", old_second),
    ):
        write_once(reference / name, payload)
    return reference


@pytest.fixture
def price_panel(panel, completed, reference_assets, monkeypatch):
    old_first, _, _, acceptance = completed
    reference = reference_assets
    pins = {k: acceptance[k] for k in subject.REFERENCE_PINS if k != "prediction_sha256"}
    pins["prediction_sha256"] = old_first["prediction_sha256"]
    monkeypatch.setattr(subject, "REFERENCE_PINS", pins)
    identity = panel["source"]["identity"]
    monkeypatch.setattr(subject, "APPROVED_INPUT", {k: identity.get(k) for k in subject.APPROVED_INPUT})
    for key in subject.THREADS:
        monkeypatch.setenv(key, "1")
    with threadpool_limits(limits=1):
        environment = subject.numeric_environment()
    # Synthetic unit/subprocess fixtures use the test interpreter, not the
    # separately authorized Conda-base formal experiment environment.
    monkeypatch.setattr(subject, "APPROVED_NUMERIC", {k: environment[k] for k in ("python", "versions")})
    source = deepcopy(panel["source"])
    binding = source["evaluation_source_binding"]
    binding.update(
        id_to_code={str(1 + 2 * i): r["sector_code"] for i, r in enumerate(source["catalog"])},
        sector={"path": "sector.h5", "sha256": "7" * 64},
        index={"path": "index.h5", "sha256": "8" * 64},
        quote_entries={r["sector_code"]: [[source["calendar"][0], source["calendar"][-1]]] for r in source["catalog"]},
    )
    view = {
        "schema_version": "hmm_risk_rotation_l2_official_price_features_v1",
        "start": source["calendar"][0],
        "end": source["calendar"][-2],
        "source_pins": {k: binding[k] for k in ("sector", "index")},
        "sector_returns": [
            {
                "trade_date": day,
                "sector_code": r["sector_code"],
                "quote_available": True,
                "pct_change": (i - 65) / 100 + (j % 3) * 0.01,
            }
            for j, day in enumerate(source["calendar"][:-1])
            for i, r in enumerate(source["catalog"])
        ],
        "benchmark_close": [{"trade_date": day, "close": 1000.0 + j} for j, day in enumerate(source["calendar"][:-1])],
    }
    return reseal(
        {
            "schema_version": subject.INPUT_SCHEMA,
            "contract": subject.CONTRACT,
            "source": source,
            "price_features": view,
            "numeric_environment": environment,
            "reference": {"path": str(reference / "acceptance.json"), "pins": pins, "input_identity": identity},
        }
    )


def test_formal_numeric_contract_remains_fixed():
    assert subject.APPROVED_NUMERIC == {
        "python": "3.13.5",
        "versions": {"numpy": "2.3.3", "scipy": "1.16.3", "scikit-learn": "1.8.0", "threadpoolctl": "3.6.0"},
    }


@pytest.mark.parametrize("reference_drift", [False, True])
def test_cold_preflight_captures_the_same_numeric_pools_as_fit_without_fitting(reference_drift):
    """Isolate cold imports: module-level fixtures otherwise mask lazy sklearn loading."""
    root = Path(__file__).resolve().parents[3]
    env = {**os.environ, **{key: "1" for key in subject.THREADS}, "PYTHONPATH": str(root)}
    control = """
import json
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as m
from sklearn.linear_model import Ridge
from threadpoolctl import threadpool_limits
with threadpool_limits(limits=1):
    print(json.dumps(m.numeric_environment()))
"""
    reference = subprocess.run(
        [sys.executable, "-c", control], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert reference.returncode == 0, reference.stderr
    expected = json.loads(reference.stdout)
    if reference_drift:
        expected["versions"]["scikit-learn"] = "0.0.0"
    program = """
import json, sys
from pathlib import Path
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as m
from backend.services.hmm_risk import rotation_l2_input as reader
assert 'sklearn.linear_model' not in sys.modules, 'test must begin with a cold model import'
expected = json.loads(sys.argv[1])
# Only file construction and unrelated shape validation are stubbed. The actual
# prepare_inputs numeric initialization, thread limit and comparison remain live.
m.ridge.prepare_inputs = lambda **kwargs: {'source': {}}
m._reference = lambda path: ({'numeric_environment': expected, 'input_identity': {}}, {})
reader.bounded_price_features = lambda source: {}
m.validate_input = lambda bundle: None
def forbid_fit(frame, event, arg):
    if event == 'call' and frame.f_code.co_name == 'fit':
        raise AssertionError('preflight must never fit')
sys.setprofile(forbid_fit)
try:
    bundle = m.prepare_inputs(reference_acceptance_path=Path('reference.json'))
finally:
    sys.setprofile(None)
assert bundle['numeric_environment'] == expected
print(json.dumps({'matches_fit_environment': True, 'fits': 0}))
"""
    result = subprocess.run(
        [sys.executable, "-c", program, json.dumps(expected)],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if reference_drift:
        assert result.returncode != 0
        assert "preflight numeric payload differs from the approved prior environment" in result.stderr
        assert "preflight must never fit" not in result.stderr
    else:
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"matches_fit_environment": True, "fits": 0}


def test_numeric_drift_is_rejected_even_after_rehash(price_panel):
    for key, value in (
        ("python", "0.0.0"),
        ("versions", {**price_panel["numeric_environment"]["versions"], "numpy": "0.0.0"}),
        ("thread_variables", {}),
        ("thread_pools", []),
        ("thread_pools", [{"num_threads": 2}]),
    ):
        changed = deepcopy(price_panel)
        changed["numeric_environment"][key] = value
        with pytest.raises(RuntimeError, match="request numeric contract differs"):
            subject.validate_input(reseal(changed))


def test_formulas_first_anchor_e0_ranks_and_zero_downside(price_panel):
    rows, train, pred = subject.feature_rows(price_panel)
    assert (len(train), len(pred), len(rows)) == (126, 232, 358 * 131)
    old, _, _ = ridge.feature_rows(subject._old_bundle(price_panel))
    assert [r["x"][:2] for r in rows if r["availability"] == "available"] == [
        r["x"] for r in old if r["availability"] == "available"
    ]
    first = next(r for r in rows if r["availability"] == "available")
    calendar = price_panel["source"]["calendar"]
    start = calendar.index(first["trade_date"])
    assert calendar[start - 20] == "2024-08-20" and calendar[start - 21] == "2024-08-19"
    quotes = {
        r["trade_date"]: r["pct_change"] / 100
        for r in price_panel["price_features"]["sector_returns"]
        if r["sector_code"] == first["sector_code"]
    }
    closes = {r["trade_date"]: r["close"] for r in price_panel["price_features"]["benchmark_close"]}
    returns = [quotes[day] for day in calendar[start - 20 : start]]
    market = [closes[calendar[j]] / closes[calendar[j - 1]] - 1 for j in range(start - 20, start)]
    assert first["feature_diagnostics"]["relative_momentum_20d"] == math.prod(1 + r for r in returns) - math.prod(
        1 + r for r in market
    )
    assert first["feature_diagnostics"]["relative_downside_20d"] == math.sqrt(
        math.fsum(min(a - b, 0) ** 2 for a, b in zip(returns, market)) / 20
    )
    assert any((r["feature_diagnostics"] or {}).get("relative_downside_20d") == 0 for r in rows)


def test_price_exclusion_does_not_rerank_moneyflow_and_small_cross_section_is_na(price_panel):
    original = subject.feature_rows(price_panel)[0]
    bundle = deepcopy(price_panel)
    for code in [r["sector_code"] for r in bundle["source"]["catalog"]][2:]:
        bundle["source"]["evaluation_source_binding"]["quote_entries"][code] = []
        for row in bundle["price_features"]["sector_returns"]:
            if row["sector_code"] == code:
                row.update(quote_available=False, pct_change=None)
    result = subject.feature_rows(reseal(bundle))[0]
    assert len(result) == len(original)
    assert all(
        r["availability"] == "unavailable" and r["feature_eligible"] is False and r["rotation_score"] is None
        for r in result
    )


@pytest.mark.parametrize(
    "mutation", ["duplicate", "missing", "nan", "negative_domain", "tail", "benchmark", "pin", "alias"]
)
def test_rehashed_invalid_price_view_fails_closed(price_panel, mutation):
    bundle = deepcopy(price_panel)
    rows = bundle["price_features"]["sector_returns"]
    if mutation == "duplicate":
        rows.append(rows[0].copy())
    elif mutation == "missing":
        rows.pop()
    elif mutation == "nan":
        rows[0]["pct_change"] = None  # canonical JSON rejects NaN earlier as well
    elif mutation == "negative_domain":
        rows[0]["pct_change"] = -100
    elif mutation == "tail":
        rows[-1]["trade_date"] = "2026-04-01"
    elif mutation == "benchmark":
        bundle["price_features"]["benchmark_close"][0]["close"] = 0
    elif mutation == "pin":
        bundle["price_features"]["source_pins"]["sector"]["sha256"] = "9" * 64
    else:
        rows[0]["sector_code"] = "999999.SI"
    with pytest.raises((ValueError, RuntimeError)):
        subject.validate_input(reseal(bundle))


def test_future_feature_changes_cannot_change_training_matrix(price_panel):
    original = subject.training_matrix(price_panel, subject.feature_rows(price_panel)[0])
    modified = deepcopy(price_panel)
    for row in modified["price_features"]["sector_returns"]:
        if row["trade_date"] >= "2025-04-01":
            row["pct_change"] += 0.1
    modified = reseal(modified)
    assert subject.training_matrix(modified, subject.feature_rows(modified)[0]) == original
    assert math.isclose(math.fsum(r["weight"] for r in original["entries"]), 1, abs_tol=1e-12)


def test_parent_zero_fit_joint_drift_and_complete_product(price_panel, completed, monkeypatch):
    first = subject.run_process(price_panel, process_index=1)
    second = ridge.seal(
        {**{k: v for k, v in first.items() if k != "report_sha256"}, "process_index": 2}, "report_sha256"
    )
    facts = ridge.seal(
        {
            **{k: v for k, v in completed[2].items() if k != "outcome_sha256"},
            "schema_version": subject.VERSION + "_outcomes",
            "input_hash": price_panel["input_hash"],
        },
        "outcome_sha256",
    )
    from sklearn.linear_model import Ridge

    monkeypatch.setattr(Ridge, "fit", lambda *a, **k: pytest.fail("parent/reference must not fit"))
    final = subject.close_processes(first, second, input_bundle=price_panel, facts=facts)
    subject.validate_acceptance(final)
    assert len(final["predictions"]) == 30392
    assert final["metrics"]["mature_day_count"] == 222
    assert final["paired_increment"]["population"] == "three_way_common_mature_eligible"
    assert final["paired_increment"]["candidate_minus_delta"]["valid_date_count"] == 222
    assert final["paired_increment"]["candidate_minus_old_ridge"]["valid_date_count"] == 222
    products = rows_from_acceptance(final)
    _validate_batch(products)
    available = next(r for r in products if r["availability"] == "available")
    assert set(available["feature_contributions"]) == {
        "raw_prediction",
        "intercept",
        *subject.LINEAR_TERMS,
        "average_rank_score",
        "daily_rank_group",
        "model_parameter_sha256",
    }
    available["feature_contributions"]["relative_downside_linear_term"] += 1
    with pytest.raises(RuntimeError):
        _validate_row(available)
    for report in (first, second):
        parameters = {k: v for k, v in report["parameters"].items() if k != "parameter_sha256"}
        parameters["coefficients"][0] += 0.01
        report["parameters"] = ridge.seal(parameters, "parameter_sha256")
        report.update(ridge.seal({k: v for k, v in report.items() if k != "report_sha256"}, "report_sha256"))
    with pytest.raises(RuntimeError):
        subject.verify_processes(first, second, input_bundle=price_panel)


def test_three_way_common_rank_not_subtracting_native_ic():
    def evaluation(scores):
        return {
            "evaluated_rows": [
                {
                    "trade_date": "2025-04-16",
                    "sector_code": str(i),
                    "availability": "available" if score is not None else "unavailable",
                    "outcome_status": "available",
                    "relative_return_10d": float(i),
                    "rotation_score": score,
                    "forecast_state": "trending" if i == 3 else "fading" if i == 1 else "neutral",
                }
                for i, score in enumerate(scores)
            ]
        }

    result = subject._paired(
        [date(2025, 4, 16)], [evaluation([10, 1, 2, 3]), evaluation([None, 3, 2, 1]), evaluation([None, 1, 2, 3])]
    )
    daily = result["daily"]["2025-04-16"]
    assert daily["common_count"] == 3 and daily["rank_ic"] == [1, -1, 1]
    assert result["candidate_minus_delta"]["daily_ic_difference"]["2025-04-16"] == 2
    assert result["candidate_minus_old_ridge"]["daily_ic_difference"]["2025-04-16"] == 0


def test_real_fresh_process_import_and_fixed_prior_hashes():
    root = Path(__file__).resolve().parents[3]
    env = {**os.environ, "PYTHONPATH": str(root)}
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as r; from backend.routers import hmm_risk, health; print(r.MODEL_CONTRACT_HASH, r.EVALUATION_HASH)",
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "6f7b6593b77219919bb46b8ef8f5bf2c7e2391a55de9ebf52628794b88ef5e19" in result.stdout
    assert "1e1397392b170aeac9e0fa9ca0d17d183a89652f6eacc284e46261be89d6fe7e" in result.stdout


def test_reference_drift_is_not_silently_dropped(price_panel):
    path = Path(price_panel["reference"]["path"])
    value = json.loads(path.read_text(encoding="utf-8"))
    value["model_hash"] = "9" * 64
    other = path.parent.parent / "drift" / "acceptance.json"
    other.parent.mkdir()
    write_once(other, ridge.seal({k: v for k, v in value.items() if k != "acceptance_sha256"}, "acceptance_sha256"))
    for i in (1, 2):
        write_once(
            other.parent / f"process_{i}.json",
            json.loads((path.parent / f"process_{i}.json").read_text(encoding="utf-8")),
        )
    with pytest.raises(RuntimeError):
        subject._reference(other)


def test_synthetic_two_fresh_processes_reproduce_without_reference_refit(price_panel, tmp_path):
    """Actual subprocesses on synthetic data, not the formal candidate experiment."""
    root = Path(__file__).resolve().parents[3]
    env = {**os.environ, **{k: "1" for k in subject.THREADS}, "PYTHONPATH": str(root)}
    # Freeze a cold process environment before running either child. Unrelated
    # test collection may load the same native pools in a different order.
    cold = subprocess.run(
        [
            sys.executable,
            "-c",
            "from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as m; from sklearn.linear_model import Ridge; import json; print(json.dumps(m.numeric_environment()))",
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    price_panel = deepcopy(price_panel)
    price_panel["numeric_environment"] = json.loads(cold.stdout)
    price_panel = reseal(price_panel)
    path = tmp_path / "synthetic_input.json"
    write_once(path, price_panel)
    program = (
        "import sys; from pathlib import Path; "
        "from backend.services.hmm_risk.formal_state_executor import read_json,write_once; "
        "from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as m; "
        "b=read_json(Path(sys.argv[1])); m.REFERENCE_PINS=b['reference']['pins']; "
        "m.APPROVED_INPUT={k:b['source']['identity'].get(k) for k in m.APPROVED_INPUT}; "
        "m.APPROVED_NUMERIC={k:b['numeric_environment'][k] for k in ('python','versions')}; "
        "write_once(Path(sys.argv[2]),m.run_process(b,process_index=int(sys.argv[3])))"
    )
    reports = []
    for index in (1, 2):
        output = tmp_path / f"synthetic_process_{index}.json"
        result = subprocess.run(
            [sys.executable, "-c", program, str(path), str(output), str(index)],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        reports.append(json.loads(output.read_text(encoding="utf-8")))
    subject.verify_processes(*reports, input_bundle=price_panel)
    assert reports[0]["prediction_sha256"] == reports[1]["prediction_sha256"]


def test_bounded_official_reader_has_no_decision_or_tail_numeric_read(price_panel, monkeypatch):
    from backend.services.hmm_risk import rotation_l2_input as reader

    reads = []
    monkeypatch.setattr(reader, "_require_file", lambda root, path, sha: root / path)

    def sector(path, **kwargs):
        reads.append(("sector", kwargs["start"], kwargs["end"]))
        return price_panel["price_features"]["sector_returns"]

    def benchmark(path, *, start, end):
        reads.append(("benchmark", start, end))
        return price_panel["price_features"]["benchmark_close"]

    monkeypatch.setattr(reader, "_sector_returns", sector)
    monkeypatch.setattr(reader, "bounded_benchmark_close", benchmark)
    view = reader.bounded_price_features(price_panel["source"])
    assert reads == [
        ("sector", date(2024, 8, 13), date(2026, 3, 30)),
        ("benchmark", date(2024, 8, 13), date(2026, 3, 30)),
    ]
    ridge.verify(view, "price_sha256")


def test_cli_source_failure_is_durable_and_does_not_train(price_panel, tmp_path, monkeypatch):
    from scripts.hmm_risk import run_rotation_l2_moneyflow_price_supervised as cli

    path, output = tmp_path / "input.json", tmp_path / "run"
    write_once(path, price_panel)
    monkeypatch.setattr(subject, "run_process", lambda *a, **k: pytest.fail("no fit on source drift"))
    assert cli.main(["run", "--input-bundle", str(path), "--output", str(output)]) == 1
    failure = json.loads(output.with_name("run.failure.json").read_text(encoding="utf-8"))
    assert failure["completed_fits_known"] == 0 and failure["tail_accessed"] is False
    assert not output.exists()
