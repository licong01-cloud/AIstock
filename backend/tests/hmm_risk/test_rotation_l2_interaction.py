"""Focused paired-hypothesis contracts; synthetic fits are not formal research."""

from copy import deepcopy
import os
from pathlib import Path
import subprocess
import sys

import pytest
from threadpoolctl import threadpool_limits

from backend.services.hmm_risk import rotation_l2_interaction as subject
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as price
from backend.tests.hmm_risk import test_rotation_l2_moneyflow_supervised as existing_tests

panel = existing_tests.panel


def reseal(value, field="input_hash"):
    return ridge.seal({k: v for k, v in value.items() if k != field}, field)


@pytest.fixture(scope="module")
def paired_panel(panel):
    source = deepcopy(panel["source"])
    codes, days = source["catalog"], source["calendar"]
    source["evaluation_source_binding"].update(
        sector={"path": "sector.h5", "sha256": "7" * 64},
        index={"path": "index.h5", "sha256": "8" * 64},
        quote_entries={c["sector_code"]: [[days[0], days[-1]]] for c in codes},
    )
    source["identity"]["source_file_hashes"].update(sector_data_h5="7" * 64, index_daily_h5="8" * 64)
    view = ridge.seal(
        {
            "schema_version": "hmm_risk_rotation_l2_official_price_features_v1",
            "start": days[0],
            "end": days[-2],
            "source_pins": {k: source["evaluation_source_binding"][k] for k in ("sector", "index")},
            "sector_returns": [
                {
                    "trade_date": day,
                    "sector_code": c["sector_code"],
                    "quote_available": True,
                    "pct_change": (i - 65) / 100 + j % 3 * 0.01,
                }
                for j, day in enumerate(days[:-1])
                for i, c in enumerate(codes)
            ],
            "benchmark_close": [{"trade_date": d, "close": 1000.0 + j} for j, d in enumerate(days[:-1])],
        },
        "price_sha256",
    )
    source = reseal(source)
    previous = {k: os.environ.get(k) for k in price.THREADS}
    try:
        os.environ.update({k: "1" for k in previous})
        from sklearn.linear_model import Ridge  # noqa: F401

        with threadpool_limits(limits=1):
            environment = subject.numeric_environment()
    finally:
        for k, v in previous.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return ridge.seal(
        {
            "schema_version": subject.INPUT_SCHEMA,
            "contract": subject.CONTRACT,
            "source": source,
            "price_features": view,
            "numeric_environment": environment,
        },
        "input_hash",
    )


@pytest.fixture
def bundle(paired_panel, monkeypatch):
    identity = paired_panel["source"]["identity"]
    monkeypatch.setattr(subject, "APPROVED_INPUT", {k: identity.get(k) for k in subject.APPROVED_INPUT})
    monkeypatch.setattr(subject, "FILE_PINS", {k: identity["source_file_hashes"].get(k) for k in subject.FILE_PINS})
    monkeypatch.setattr(
        price, "APPROVED_NUMERIC", {k: paired_panel["numeric_environment"][k] for k in ("python", "versions")}
    )
    for key in price.THREADS:
        monkeypatch.setenv(key, "1")
    return deepcopy(paired_panel)


def test_interaction_is_exact_and_does_not_mutate_control():
    rows = [
        {"availability": "available", "x": [0.1, -0.3, 0.4, -0.2]},
        {"availability": "available", "x": [0.1, 0.3, -0.4, -0.2]},
        {"availability": "unavailable", "x": None},
    ]
    changed = subject.append_interaction(rows)
    assert changed[0]["x"] == [*rows[0]["x"], -0.12]
    assert changed[1]["x"][-1] == 0.0 and changed[2]["x"] is None
    assert len(rows[0]["x"]) == 4


@pytest.mark.parametrize("bad", [[0, 0, 0], [0, 0, float("nan"), 0], [0, True, 0, 0]])
def test_invalid_interaction_feature_fails(bad):
    with pytest.raises(Exception, match="four finite"):
        subject.append_interaction([{"availability": "available", "x": bad}])


@pytest.mark.parametrize(
    "hac,expected",
    [
        ({"mean": None, "lower": None}, "EVIDENCE_INSUFFICIENT"),
        ({"mean": -0.01, "lower": -0.02}, "NOT_SUPPORTED_BY_CURRENT_EVIDENCE"),
        ({"mean": 0.01, "lower": -0.02}, "POSITIVE_INCREMENT_UNCONFIRMED"),
        ({"mean": 0.02, "lower": 0.01}, "RETROSPECTIVE_INCREMENT_OBSERVED"),
    ],
)
def test_hac_evidence_uses_real_lower_field(hac, expected):
    assert subject.evidence_status(hac) == expected


def test_matched_training_has_identical_y_population_and_weight(bundle):
    control, train, pred = subject.feature_rows(bundle, interaction=False)
    candidate = subject.append_interaction(control)
    matrices = [subject.training_matrix(bundle, rows) for rows in (control, candidate)]
    assert (len(train), len(pred), matrices[0]["usable_dates"]) == (126, 232, 126)
    assert [{k: v for k, v in r.items() if k != "x"} for r in matrices[0]["entries"]] == [
        {k: v for k, v in r.items() if k != "x"} for r in matrices[1]["entries"]
    ]
    assert all(
        a["x"] == b["x"][:4] and len(b["x"]) == 5
        for a, b in zip(matrices[0]["entries"], matrices[1]["entries"], strict=True)
    )
    assert any(abs(r["y"]) < 0.001 for r in matrices[0]["entries"])


def test_self_hashed_old_release_or_quote_drift_rejected(bundle):
    for mutation in ("release", "quote"):
        changed = deepcopy(bundle)
        if mutation == "release":
            changed["source"]["identity"]["generation"] = "old-v15"
        else:
            changed["price_features"]["sector_returns"][0]["pct_change"] = None
            changed["price_features"] = reseal(changed["price_features"], "price_sha256")
        changed["source"] = reseal(changed["source"])
        with pytest.raises(Exception):
            subject.validate_input(reseal(changed))


def test_profile_drift_is_rejected_before_large_file_read(tmp_path, monkeypatch):
    from backend.services.hmm_risk import rotation_l2_input

    profile = tmp_path / "profile.json"
    profile.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        rotation_l2_input,
        "build_rotation_l2_input_bundle",
        lambda **k: pytest.fail("drift reached large source reader"),
    )
    with pytest.raises(Exception, match="profile byte identity changed"):
        subject.prepare_inputs(active_profile_path=profile)


def test_evaluation_outcomes_cannot_enter_fit_input(bundle):
    bundle["source"]["benchmark_close"].append({"trade_date": "2025-04-16", "close": 1000.0})
    bundle["source"] = reseal(bundle["source"])
    with pytest.raises(Exception, match="non-training"):
        subject.validate_input(reseal(bundle))


def test_paired_child_is_two_fits_and_parent_zero_fit_readback(bundle, monkeypatch):
    from sklearn.linear_model import Ridge

    first = subject.run_process(bundle, process_index=1)
    second = reseal(
        {
            **first,
            "process_index": 2,
            "arms": {n: reseal({**r, "process_index": 2}, "report_sha256") for n, r in first["arms"].items()},
        },
        "report_sha256",
    )
    assert first["completed_fits"] == 2
    monkeypatch.setattr(Ridge, "fit", lambda *a, **k: pytest.fail("parent refit"))
    subject.verify_processes(first, second, input_bundle=bundle)
    sector = [
        deepcopy(r)
        for r in bundle["price_features"]["sector_returns"]
        if r["trade_date"] >= ridge.PREDICTION_START.isoformat()
    ]
    sector += [
        {**r, "trade_date": ridge.PREDICTION_END.isoformat()}
        for r in sector
        if r["trade_date"] == bundle["source"]["calendar"][-2]
    ]
    benchmark = [
        {"trade_date": d, "close": 1000.0}
        for d in bundle["source"]["calendar"]
        if d >= ridge.PREDICTION_START.isoformat()
    ]
    facts = ridge.seal(
        {
            "schema_version": subject.VERSION + "_outcomes",
            "input_hash": bundle["input_hash"],
            "sector_returns": sector,
            "benchmark_close": benchmark,
            "tail_accessed": False,
        },
        "outcome_sha256",
    )
    acceptance = subject.close_processes(first, second, input_bundle=bundle, facts=facts)
    subject.validate_acceptance(acceptance)
    assert acceptance["completed_fits"] == 4 and acceptance["advisory_status"] == "NOT_AVAILABLE"
    for key, bad in (("execution_status", "PASS"), ("production_promoted", True), ("completed_fits", 2)):
        forged = reseal({**acceptance, key: bad}, "acceptance_sha256")
        with pytest.raises(Exception, match="acceptance"):
            subject.validate_acceptance(forged)
    changed = deepcopy(first)
    arm = changed["arms"]["interaction"]
    arm["parameters"]["intercept"] += 1
    arm["parameters"] = reseal(arm["parameters"], "parameter_sha256")
    changed["arms"]["interaction"] = reseal(arm, "report_sha256")
    with pytest.raises(Exception, match="objective"):
        subject.verify_processes(reseal(changed, "report_sha256"), second, input_bundle=bundle)


def test_t_minus_one_price_boundary(bundle):
    before, _, _ = subject.feature_rows(bundle)
    changed = deepcopy(bundle)
    day = ridge.PREDICTION_START.isoformat()
    for r in changed["price_features"]["sector_returns"]:
        if r["trade_date"] >= day:
            r["pct_change"] = 10.0
    changed["price_features"] = reseal(changed["price_features"], "price_sha256")
    after, _, _ = subject.feature_rows(reseal(changed))
    assert [r for r in before if r["trade_date"] <= day] == [r for r in after if r["trade_date"] <= day]


def test_legal_held_quote_na_is_not_valued_or_filled():
    from scripts.hmm_risk.rotation_l2_reference_value import cohort_reference_path, summarize

    days = [f"2025-07-{i:02d}" for i in range(1, 13)]
    quotes = {(day, "801001.SI"): (None if i == 2 else 0.01) for i, day in enumerate(days)}
    path = cohort_reference_path(days, days[:2], {d: ["801001.SI"] for d in days[:2]}, quotes, cost_bps=0)
    result = summarize(path)
    assert result["status"] == "INSUFFICIENT_REFERENCE_PATH" and result["cumulative_return"] is None
    assert path[2]["nav"] is None and path[-1]["nav"] is None


def test_fresh_process_import_and_formula_do_not_import_database():
    root = Path(__file__).resolve().parents[3]
    code = "from backend.services.hmm_risk.rotation_l2_interaction import append_interaction; import sys; assert not any(k.startswith(('psycopg','asyncpg','backend.db')) for k in sys.modules); assert append_interaction([{'availability':'available','x':[0.,.2,.4,0.]}])[0]['x'][-1] == .2*.4; print('file_only_import=PASS')"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        env={**os.environ, "PYTHONPATH": str(root)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "file_only_import=PASS" in result.stdout


def test_cli_rejects_child_drift_before_any_outcome_read(tmp_path, monkeypatch):
    from scripts.hmm_risk import run_rotation_l2_interaction as cli
    from backend.services.hmm_risk.formal_state_executor import read_json, write_once

    request = tmp_path / "input.json"
    write_once(
        request,
        {
            "input_hash": "a" * 64,
            "source": {
                "identity": {"source_git_commit": "b" * 40},
                "evaluation_source_binding": {"root": str(tmp_path / "dataset")},
            },
        },
    )
    calls = []
    monkeypatch.setattr(cli.model, "validate_input", lambda bundle: {})
    monkeypatch.setattr(cli, "require_clean_source", lambda expected: calls.append("clean"))
    monkeypatch.setattr(cli, "source_head", lambda: "b" * 40)

    def child(command, **kwargs):
        number = int(command[command.index("--process-index") + 1])
        write_once(Path(command[command.index("--output") + 1]), {"process_index": number})
        return subprocess.CompletedProcess(command, 0)

    def reject(*args, **kwargs):
        calls.append("verify")
        raise subject.fail("synthetic parent drift rejection")

    monkeypatch.setattr(cli.subprocess, "run", child)
    monkeypatch.setattr(cli.model, "verify_processes", reject)
    monkeypatch.setattr(cli.model, "read_evaluation_facts", lambda *a: pytest.fail("outcome read before closure"))
    output = tmp_path / "run"
    assert cli.main(["run", "--input-bundle", str(request), "--output", str(output)]) == 1
    failure = read_json(tmp_path / "run.failure.json")
    assert calls == ["clean", "verify"] and failure["completed_fits_known"] == 4
    assert not (output / "acceptance.json").exists()


def test_cli_unsafe_destination_cannot_write_failure_or_train(tmp_path, monkeypatch):
    from scripts.hmm_risk import run_rotation_l2_interaction as cli

    monkeypatch.setattr(cli.model, "prepare_inputs", lambda **k: pytest.fail("unsafe destination reached input reader"))
    root = Path(__file__).resolve().parents[3]
    output = root / "forbidden_interaction_result.json"
    assert (
        cli.main(
            [
                "preflight",
                "--active-profile",
                str(tmp_path / "profile"),
                "--dataset-root",
                str(tmp_path / "dataset"),
                "--security-identity-manifest",
                str(tmp_path / "security"),
                "--provider-absence-manifest",
                str(tmp_path / "absence"),
                "--security-identity-sha256",
                "a" * 64,
                "--provider-absence-sha256",
                "b" * 64,
                "--approved-manifest-sha256",
                "c" * 64,
                "--output",
                str(output),
            ]
        )
        == 1
    )
    assert not output.exists() and not output.with_name(output.name + ".failure.json").exists()
