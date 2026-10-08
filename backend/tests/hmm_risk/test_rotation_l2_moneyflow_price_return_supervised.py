"""Direct synthetic target/causality/identity tests, not formal experiment fits."""

from copy import deepcopy
from datetime import date
import math
import os
from pathlib import Path
import subprocess
import sys

import pytest
from threadpoolctl import threadpool_limits

from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_price_return_supervised as subject
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as price
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json, write_once
from backend.tests.hmm_risk import test_rotation_l2_moneyflow_price_supervised as previous

panel, completed, reference_assets, price_panel = (
    previous.panel,
    previous.completed,
    previous.reference_assets,
    previous.price_panel,
)


def reseal(value, field="input_hash"):
    return ridge.seal({k: v for k, v in value.items() if k != field}, field)


@pytest.fixture
def candidate(price_panel, completed, candidate_cache, tmp_path_factory, monkeypatch):
    # Generate the unchanged synthetic comparison once, not once per mutation.
    if not candidate_cache:
        directory = tmp_path_factory.mktemp("return_target_reference")
        original_path = directory / "original.json"
        write_once(original_path, price_panel)
        first = price.run_process(price_panel, process_index=1)
        second = reseal({**first, "process_index": 2}, "report_sha256")
        facts = reseal(
            {**completed[2], "schema_version": price.VERSION + "_outcomes", "input_hash": price_panel["input_hash"]},
            "outcome_sha256",
        )
        acceptance = price.close_processes(first, second, input_bundle=price_panel, facts=facts)
        for name, payload in (("acceptance.json", acceptance), ("process_1.json", first), ("process_2.json", second)):
            write_once(directory / name, payload)
        candidate_cache.update(original=price_panel, directory=directory, first=first, acceptance=acceptance)
    original = candidate_cache["original"]
    first, acceptance = candidate_cache["first"], candidate_cache["acceptance"]
    directory = candidate_cache["directory"]
    monkeypatch.setattr(subject, "ORIGINAL_INPUT_HASH", original["input_hash"])
    pins = {k: acceptance[k] for k in subject.REFERENCE_PINS if k != "prediction_sha256"}
    pins["prediction_sha256"] = first["prediction_sha256"]
    monkeypatch.setattr(subject, "REFERENCE_PINS", pins)
    bundle = subject.prepare_inputs(
        original_input_path=directory / "original.json",
        reference_acceptance_path=directory / "acceptance.json",
        source_commit="c" * 40,
    )
    return bundle, original, acceptance


@pytest.fixture(scope="module")
def candidate_cache():
    return {}


def test_raw_decimal_target_and_original_training_population_are_exact(candidate):
    bundle, original, _ = candidate
    rows, train, pred = subject.feature_rows(bundle)
    raw = subject.training_matrix(bundle, rows)
    ranked = price.training_matrix(original, rows)
    assert (len(train), len(pred), raw["usable_dates"]) == (126, 232, 126)
    assert [{k: v for k, v in r.items() if k != "y"} for r in raw["entries"]] == [
        {k: v for k, v in r.items() if k != "y"} for r in ranked["entries"]
    ]
    row = raw["entries"][0]
    source = next(r for r in original["source"]["sector_returns"] if r["sector_code"] == row["sector_code"])
    assert row["y"] == pytest.approx((1 + source["pct_change"] / 100) ** 10 - 1, abs=1e-15)
    assert row["y"] != ranked["entries"][0]["y"]
    assert math.fsum(r["weight"] for r in raw["entries"]) == pytest.approx(1, abs=1e-12)
    assert subject.CONTRACT["alpha"] == price.CONTRACT["alpha"] == 0.01


@pytest.mark.parametrize(
    "mutation", ["input_pin", "release", "tail_label", "bool_label", "null_label", "extra_outcomes", "root", "numeric"]
)
def test_rehashed_input_drift_fails_closed(candidate, monkeypatch, mutation):
    bundle, original, _ = candidate
    changed = deepcopy(bundle)
    if mutation == "input_pin":
        changed["original_input"]["input_hash"] = "0" * 64
    elif mutation in {"release", "tail_label", "bool_label", "null_label"}:
        original = deepcopy(original)
        if mutation == "release":
            original["source"]["identity"]["generation"] = "wrong"
        else:
            row = next(
                r
                for r in original["source"]["sector_returns"]
                if r["trade_date"] > "2024-09-19"
                and r["sector_code"] != original["source"]["catalog"][0]["sector_code"]
            )
            if mutation == "tail_label":
                row["trade_date"] = "2026-04-01"
            else:
                row["pct_change"] = True if mutation == "bool_label" else None
        original = previous.reseal(original)
        modified_path = Path(changed["original_input"]["path"]).with_name(f"modified_original_{mutation}.json")
        write_once(modified_path, original)
        changed["original_input"]["path"] = str(modified_path)
        monkeypatch.setattr(subject, "ORIGINAL_INPUT_HASH", original["input_hash"])
        changed["original_input"]["input_hash"] = original["input_hash"]
    elif mutation == "extra_outcomes":
        changed["evaluation_outcomes"] = []
    elif mutation == "root":
        changed["source"]["evaluation_source_binding"]["root"] += "-other"
    else:
        changed["numeric_environment"]["versions"]["numpy"] = "0"
    with pytest.raises((RuntimeError, ValueError)):
        subject.training_matrix(reseal(changed), price.feature_rows(candidate[1])[0])


def test_training_never_reads_prediction_outcomes_or_source_reader(candidate, monkeypatch):
    bundle, original, _ = candidate

    def forbidden(*args, **kwargs):
        raise AssertionError("evaluation/source reader used during training")

    monkeypatch.setattr(subject, "_reference", forbidden)
    monkeypatch.setattr(price, "read_evaluation_facts", forbidden)
    monkeypatch.setattr(ridge, "read_evaluation_facts", forbidden)
    rows = subject.feature_rows(bundle)[0]
    raw = subject.training_matrix(bundle, rows)
    assert raw["planned_dates"] == 126
    altered = deepcopy(rows)
    for row in altered:
        if row["trade_date"] >= "2025-04-16":
            row["x"] = [1000] * 4
    assert subject.training_matrix(bundle, altered) == raw
    assert original["input_hash"] != bundle["input_hash"]


def test_parent_normal_equations_sealed_outcomes_and_zero_old_refits(candidate, monkeypatch):
    bundle, _, old = candidate
    first = subject.run_process(bundle, process_index=1)
    second = reseal({**first, "process_index": 2}, "report_sha256")

    def forbidden(*args, **kwargs):
        raise AssertionError("old model/source/parent fit used")

    monkeypatch.setattr(price, "run_process", forbidden)
    monkeypatch.setattr(price, "read_evaluation_facts", forbidden)
    from backend.db import pg_pool
    import socket

    monkeypatch.setattr(pg_pool, "get_conn", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    from sklearn.linear_model import Ridge

    monkeypatch.setattr(Ridge, "fit", forbidden)
    facts = subject.read_evaluation_facts(bundle)
    final = subject.close_processes(first, second, input_bundle=bundle, facts=facts)
    subject.validate_acceptance(final)
    assert final["model_hash"] != old["model_hash"]
    assert len(final["predictions"]) == 232 * 131
    assert final["metrics"]["mature_day_count"] == 222
    assert final["rank_target_metrics"] == old["metrics"]
    assert final["baseline_metrics"] == old["baseline_metrics"]
    assert final["database_write"] is final["runtime_action"] is final["tail_accessed"] is False
    assert final["net_value_status"] == "UNASSESSED"
    assert "spread" in final["paired_increment"]["candidate_minus_rank_target"]
    for report in (first, second):
        parameters = deepcopy(report["parameters"])
        parameters["coefficients"][0] += 0.01
        report["parameters"] = reseal(parameters, "parameter_sha256")
        report.update(reseal(report, "report_sha256"))
    with pytest.raises(RuntimeError, match="weighted Ridge objective"):
        subject.verify_processes(first, second, input_bundle=bundle)


def test_reference_and_fact_drift_cannot_be_self_rehashed(candidate, tmp_path):
    bundle, _, _ = candidate
    reference = Path(bundle["reference"]["path"])
    broken = read_json(reference)
    broken["predictions"][1]["relative_return_10d"] += 0.01
    write_once(tmp_path / "acceptance.json", reseal(broken, "acceptance_sha256"))
    for i in (1, 2):
        write_once(tmp_path / f"process_{i}.json", read_json(reference.parent / f"process_{i}.json"))
    bundle = reseal({**bundle, "reference": {**bundle["reference"], "path": str(tmp_path / "acceptance.json")}})
    with pytest.raises(RuntimeError):
        subject.read_evaluation_facts(bundle)


def test_same_statistic_summary_and_illegal_outcome_maturity(candidate):
    bundle, _, old = candidate
    calendar = subject.validate_input(bundle)["calendar"]
    result = baseline.summarize_evaluated_predictions(
        calendar=calendar,
        evaluated_rows=old["predictions"],
        decision_start=ridge.PREDICTION_START,
        decision_end=ridge.PREDICTION_END,
        outcome_end=ridge.PREDICTION_END,
        report_blocks=ridge.BLOCKS,
    )
    assert result["metrics"] == old["metrics"]
    assert result["evaluated_rows"] == old["predictions"]
    changed = deepcopy(old["predictions"])
    changed[-1].update(outcome_status="available", relative_return_10d=0.1)
    with pytest.raises(RuntimeError, match="maturity"):
        baseline.summarize_evaluated_predictions(
            calendar=calendar,
            evaluated_rows=changed,
            decision_start=ridge.PREDICTION_START,
            decision_end=ridge.PREDICTION_END,
            outcome_end=ridge.PREDICTION_END,
            report_blocks=ridge.BLOCKS,
        )


def test_two_real_synthetic_fresh_children_match(candidate, tmp_path):
    bundle, _, _ = candidate
    path = tmp_path / "request.json"
    write_once(path, bundle)
    root = Path(__file__).resolve().parents[3]
    env = {**os.environ, **{key: "1" for key in price.THREADS}, "PYTHONPATH": str(root)}
    program = """
import json, sys
from pathlib import Path
from backend.services.hmm_risk import rotation_l2_moneyflow_price_return_supervised as s
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as p
from backend.services.hmm_risk.formal_state_executor import read_json, write_once
b = read_json(Path(sys.argv[1]))
original = read_json(Path(b['original_input']['path']))
s.ORIGINAL_INPUT_HASH = b['original_input']['input_hash']
s.REFERENCE_PINS = b['reference']['pins']
p.REFERENCE_PINS = original['reference']['pins']
p.APPROVED_INPUT = {k: original['source']['identity'].get(k) for k in p.APPROVED_INPUT}
p.APPROVED_NUMERIC = {k: original['numeric_environment'][k] for k in ('python', 'versions')}
def forbid_reference(*args, **kwargs):
    raise AssertionError('child must not read evaluation outcomes')
s._reference = forbid_reference
write_once(Path(sys.argv[2]), s.run_process(b, process_index=int(sys.argv[3])))
"""
    reports = []
    for index in (1, 2):
        output = tmp_path / f"fresh_{index}.json"
        result = subprocess.run(
            [sys.executable, "-c", program, str(path), str(output), str(index)],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        reports.append(read_json(output))
    with threadpool_limits(limits=1):
        subject.verify_processes(*reports, input_bundle=bundle)
    assert reports[0]["parameters"] == reports[1]["parameters"]
    assert reports[0]["prediction_sha256"] == reports[1]["prediction_sha256"]


def test_cli_failure_is_durable_without_fit_or_database(tmp_path, monkeypatch):
    from scripts.hmm_risk import run_rotation_l2_moneyflow_price_return_supervised as cli

    output = tmp_path / "preflight.json"
    status = cli.main(
        [
            "preflight",
            "--original-input",
            str(tmp_path / "missing.json"),
            "--reference-acceptance",
            str(tmp_path / "acceptance.json"),
            "--output",
            str(output),
        ]
    )
    assert status == 1 and not output.exists()
    failure = read_json(tmp_path / "preflight.json.failure.json")
    assert failure["completed_fits_known"] == 0
    assert failure["database_write"] is failure["runtime_action"] is failure["tail_accessed"] is False
    assert failure["reason_code"].startswith("hmm_risk_rotation_l2_supervised_")


def test_common_spread_uses_frozen_state_not_reranked_subset():
    rows = [
        {
            "trade_date": "2025-04-16",
            "sector_code": str(i),
            "availability": "available",
            "outcome_status": "available",
            "rotation_score": i,
            "relative_return_10d": 0.01 * i,
            "forecast_state": "fading" if i == 0 else "trending" if i == 3 else "neutral",
        }
        for i in range(4)
    ]
    evaluations = [{"evaluated_rows": deepcopy(rows)} for _ in range(3)]
    evaluations[1]["evaluated_rows"][0]["availability"] = "unavailable"
    paired = price._paired(
        [date(2025, 4, 16)], evaluations, comparison_names=("candidate_minus_rank_target", "candidate_minus_delta")
    )
    subject._spread_increments(paired, [date(2025, 4, 16)])
    assert paired["daily"]["2025-04-16"]["common_count"] == 3
    assert paired["daily"]["2025-04-16"]["spread"] == [None] * 3
    assert paired["candidate_minus_rank_target"]["spread"]["valid_date_count"] == 0
    assert canonical_sha256(price.CONTRACT) == price.MODEL_CONTRACT_HASH
