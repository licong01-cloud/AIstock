from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as subject
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.rotation_l2_input import bounded_benchmark_close
from backend.services.hmm_risk.rotation_l2_prediction import rows_from_acceptance, _validate_batch


def _calendar() -> list[date]:
    closed = [
        ("2024-09-16", "2024-09-17"),
        ("2024-10-01", "2024-10-07"),
        ("2025-01-01", "2025-01-01"),
        ("2025-01-28", "2025-02-04"),
        ("2025-04-04", "2025-04-04"),
        ("2025-05-01", "2025-05-05"),
        ("2025-06-02", "2025-06-02"),
        ("2025-10-01", "2025-10-08"),
        ("2026-01-01", "2026-01-02"),
        ("2026-02-16", "2026-02-23"),
    ]
    intervals = [(date.fromisoformat(a), date.fromisoformat(b)) for a, b in closed]
    day, end = date(2024, 8, 13), subject.PREDICTION_END
    days = []
    while day <= end:
        if day.weekday() < 5 and not any(a <= day <= b for a, b in intervals):
            days.append(day)
        day += timedelta(days=1)
    return days


@pytest.fixture(scope="module")
def panel() -> dict:
    calendar = _calendar()
    codes = [f"801{i:03d}.SI" for i in range(131)]
    source_commit = "a" * 40
    daily = []
    for offset, day in enumerate(calendar[:-1]):
        for i, code in enumerate(codes):
            daily.append(
                {
                    "trade_date": day.isoformat(),
                    "sector_code": code,
                    "structural_eligible": i != 0,
                    "eligible": i != 0,
                    "reason_code": None if i else "hmm_risk_rotation_l2_no_resolved_members",
                    "expected_contributors": 2,
                    "valid_contributors": 2,
                    "coverage": 1.0,
                    "net_mf_amount_cny": (i + 1) * (offset + 1),
                    "amount_cny": 1000.0,
                    "maximum_member_amount_share": 0.6,
                }
            )
    source = {
        "schema_version": baseline.INPUT_SCHEMA,
        "identity": {
            "source_commit": hashlib.sha256(source_commit.encode()).hexdigest(),
            "source_git_commit": source_commit,
            "profile_sha256": "1" * 64,
            "manifest_sha256": "2" * 64,
            "mapping_hash": "3" * 64,
            "quote_authority_hash": "4" * 64,
            "calendar_hash": "5" * 64,
            "source_file_hashes": {"test": "6" * 64, "sector_data_h5": "7" * 64, "index_daily_h5": "8" * 64},
            "release_id": "qe_hmm_full_v2_20260831",
            "cutoff": "2026-08-31",
            "sector_display_name_authority": "canonical_sw_l2_code_only",
        },
        "catalog": [{"sector_code": code, "sector_name": code} for code in codes],
        "calendar": [day.isoformat() for day in calendar],
        "daily_aggregates": daily,
        "sector_returns": [
            {
                "trade_date": day.isoformat(),
                "sector_code": code,
                "quote_available": True,
                "pct_change": (i - 65) / 10000.0,
            }
            for day in calendar
            if subject.TRAIN_START <= day <= subject.TRAIN_OUTCOME_END
            for i, code in enumerate(codes)
        ],
        "benchmark_close": [
            {"trade_date": day.isoformat(), "close": 1000.0}
            for day in calendar
            if subject.TRAIN_START <= day <= subject.TRAIN_OUTCOME_END
        ],
        "outcome_bounds": {"start": subject.TRAIN_START.isoformat(), "end": subject.TRAIN_OUTCOME_END.isoformat()},
        "evaluation_source_binding": {"root": str(Path(__file__).anchor + "fixture-dataset")},
    }
    source["input_hash"] = canonical_sha256(source)
    return subject.seal(
        {"schema_version": subject.INPUT_SCHEMA, "contract": subject.CONTRACT, "source": source}, "input_hash"
    )


def _rehash(panel: dict) -> dict:
    panel["source"] = subject.seal({k: v for k, v in panel["source"].items() if k != "input_hash"}, "input_hash")
    return subject.seal({k: v for k, v in panel.items() if k != "input_hash"}, "input_hash")


@pytest.fixture(scope="module")
def completed(panel):
    previous = {
        key: os.environ.get(key)
        for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS")
    }
    os.environ.update({key: "1" for key in previous})
    try:
        first = subject.run_process(panel, process_index=1)
        second = subject.seal(
            {**{k: v for k, v in first.items() if k != "report_sha256"}, "process_index": 2}, "report_sha256"
        )
        calendar = _calendar()
        codes = [row["sector_code"] for row in panel["source"]["catalog"]]
        facts = subject.seal(
            {
                "schema_version": subject.VERSION + "_outcomes",
                "input_hash": panel["input_hash"],
                "tail_accessed": False,
                "sector_returns": [
                    {
                        "trade_date": day.isoformat(),
                        "sector_code": code,
                        "quote_available": True,
                        "pct_change": (i - 65) / 10000.0,
                    }
                    for day in calendar
                    if day >= subject.PREDICTION_START
                    for i, code in enumerate(codes)
                ],
                "benchmark_close": [
                    {"trade_date": day.isoformat(), "close": 1000.0}
                    for day in calendar
                    if day >= subject.PREDICTION_START
                ],
            },
            "outcome_sha256",
        )
        final = subject.close_processes(first, second, input_bundle=panel, facts=facts)
        yield first, second, facts, final
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_approved_calendar_and_objective_accounting(panel):
    parsed = subject.validate_input(panel)
    train, pred = subject.schedule(list(parsed["calendar"]))
    assert (len(train), len(pred)) == (126, 232)
    rows, _, _ = subject.feature_rows(panel)
    training = subject.training_matrix(panel, rows)
    x, y, w = subject._arrays(training)
    assert x.shape == (126 * 130, 2)
    assert training["usable_dates"] == 126
    assert np.isclose(w.sum(), 1.0)
    assert np.allclose(x[:, 0], y) and np.allclose(x[:, 1], y)


@pytest.mark.parametrize("drift", ["target", "contract", "calendar"])
def test_input_drift_is_not_accepted_after_rehash(panel, drift):
    changed = deepcopy(panel)
    if drift == "target":
        changed["source"]["benchmark_close"].append({"trade_date": "2025-04-16", "close": 1000.0})
    elif drift == "contract":
        changed["contract"] = {**subject.CONTRACT, "alpha": 0.1}
    else:
        changed["source"]["calendar"].remove("2025-04-15")
    with pytest.raises(baseline.RotationL2Error):
        subject.validate_input(_rehash(changed))


def test_future_features_do_not_change_training_parameters(panel):
    changed = deepcopy(panel)
    for row in changed["source"]["daily_aggregates"]:
        if row["trade_date"] > subject.TRAIN_END.isoformat():
            row["net_mf_amount_cny"] *= -7
    first = subject.training_matrix(panel, subject.feature_rows(panel)[0])
    second = subject.training_matrix(_rehash(changed), subject.feature_rows(_rehash(changed))[0])
    assert first == second


def test_same_population_comparison_keeps_full_grid_and_legal_empty(panel, completed):
    _, _, _, final = completed
    subject.validate_acceptance(final)
    assert len(final["predictions"]) == 30392
    assert final["metrics"]["mature_day_count"] == 222
    assert final["metrics"]["overall"]["decision_day_count"] == 232
    assert final["paired_increment"]["valid_date_count"] == 222
    assert final["paired_increment"]["hac"]["mean"] == pytest.approx(0.0, abs=1e-12)
    assert all(row["rotation_score"] is None for row in final["predictions"] if row["sector_code"] == "801000.SI")
    assert final["tail_accessed"] is False and final["completed_fits"] == 2


def test_parent_rejects_jointly_rehashed_wrong_parameters_without_refit(panel, completed, monkeypatch):
    import sklearn.linear_model

    first, second, facts, _ = completed
    monkeypatch.setattr(sklearn.linear_model.Ridge, "fit", lambda *a, **k: pytest.fail("parent must not refit"))
    tampered = []
    for child in (first, second):
        changed = deepcopy(child)
        changed["parameters"]["coefficients"][0] += 0.1
        changed["parameters"] = subject.seal(
            {k: v for k, v in changed["parameters"].items() if k != "parameter_sha256"}, "parameter_sha256"
        )
        changed["predictions"] = subject.predictions_from_parameters(
            subject.feature_rows(panel)[0], changed["parameters"]
        )
        changed["prediction_sha256"] = canonical_sha256(changed["predictions"])
        tampered.append(subject.seal({k: v for k, v in changed.items() if k != "report_sha256"}, "report_sha256"))
    with pytest.raises(baseline.RotationL2Error, match="weighted Ridge objective"):
        subject.close_processes(*tampered, input_bundle=panel, facts=facts)


def test_product_version_identity_and_rank_projection_are_checked(completed):
    rows = rows_from_acceptance(completed[3])
    assert len(rows) == 30392 and rows[1]["validation_basis"] == subject.BASIS
    assert "daily_ic_difference" not in rows[1]["run_summary"]["paired_increment"]
    assert "daily_rank_ic" not in rows[1]["run_summary"]["baseline_metrics"]
    corrupted = deepcopy(rows[-131:])
    corrupted[10]["rotation_score"] = 0.42
    corrupted[10]["feature_contributions"]["average_rank_score"] = 0.42
    with pytest.raises(RuntimeError, match="rank projection"):
        _validate_batch(corrupted)


@pytest.mark.parametrize("field", ["structural_eligible", "feature_eligible"])
def test_supervised_available_row_requires_both_eligibility_flags(completed, field):
    from backend.services.hmm_risk.rotation_l2_prediction import _validate_row

    row = next(row for row in rows_from_acceptance(completed[3]) if row["availability"] == "available")
    row[field] = False
    row["prediction_id"] = None
    with pytest.raises(RuntimeError, match="supervised product contract"):
        _validate_row(row)


def test_supervised_unavailable_row_may_not_claim_feature_eligibility(completed):
    from backend.services.hmm_risk.rotation_l2_prediction import _validate_row

    row = next(row for row in rows_from_acceptance(completed[3]) if row["availability"] == "unavailable")
    row["feature_eligible"] = True
    row["prediction_id"] = None
    with pytest.raises(RuntimeError, match="supervised product contract"):
        _validate_row(row)


@pytest.mark.parametrize("field,value", [("coefficients", [1.0]), ("intercept", False)])
def test_rehashed_supervised_parameter_shape_is_not_a_valid_product(completed, field, value):
    from backend.services.hmm_risk.rotation_l2_prediction import _validate_row

    row = deepcopy(rows_from_acceptance(completed[3])[0])  # Shape must be checked even for legal unavailable rows.
    parameters = row["run_summary"]["parameters"]
    parameters[field] = value
    parameters = subject.seal({k: v for k, v in parameters.items() if k != "parameter_sha256"}, "parameter_sha256")
    row["run_summary"]["parameters"] = parameters
    row["model_hash"] = canonical_sha256(
        {"contract_hash": subject.MODEL_CONTRACT_HASH, "parameter_sha256": parameters["parameter_sha256"]}
    )
    row["prediction_id"] = None
    with pytest.raises(RuntimeError, match="supervised product contract"):
        _validate_row(row)


@pytest.mark.parametrize("mode", ["child", "run"])
def test_cli_dataset_output_rejection_cannot_write_failure_receipt(panel, tmp_path, monkeypatch, mode):
    from scripts.hmm_risk import run_rotation_l2_moneyflow_supervised as cli

    root = tmp_path / "dataset"
    root.mkdir()
    bundle = deepcopy(panel)
    bundle["source"]["evaluation_source_binding"]["root"] = str(root)
    bundle = _rehash(bundle)
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(bundle), encoding="utf-8")
    monkeypatch.setattr(cli, "source_head", lambda: bundle["source"]["identity"]["source_git_commit"])
    monkeypatch.setattr(subject, "run_process", lambda *a, **k: pytest.fail("rejected output may not fit"))
    output = root / "forbidden"
    args = [mode, "--input-bundle", str(input_path), "--output", str(output)]
    if mode == "child":
        args += ["--input-sha256", bundle["input_hash"], "--executor-commit", cli.source_head(), "--process-index", "1"]
    assert cli.main(args) == 1
    assert list(root.iterdir()) == [], "rejected dataset output may not create even a failure receipt"


def test_fixed_index_reader_never_reads_numeric_values_outside_range(tmp_path, monkeypatch):
    import tables

    path = tmp_path / "index.h5"
    pd.DataFrame(
        {
            "trade_date": ["2025-04-16", "2026-04-01", "2025-04-17"],
            "ts_code": ["000300.SH"] * 3,
            "close": [100.0, 12345.0, 101.0],
        }
    ).to_hdf(path, key="data", format="fixed")
    original = tables.Array.__getitem__
    numeric_reads = []

    def guarded(self, key):
        if self._v_name.endswith("_values"):
            numeric_reads.append(key)
            assert key[0].start != 1 and key[0].stop <= key[0].start + 1
        return original(self, key)

    monkeypatch.setattr(tables.Array, "__getitem__", guarded)
    result = bounded_benchmark_close(path, start=date(2025, 4, 16), end=date(2025, 4, 17))
    assert result == [{"trade_date": "2025-04-16", "close": 100.0}, {"trade_date": "2025-04-17", "close": 101.0}]
    assert len(numeric_reads) == 2


def test_child_fresh_process_reproduction_is_real(panel, completed, tmp_path):
    root = Path(__file__).resolve().parents[3]
    current = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    changed = deepcopy(panel)
    changed["source"]["identity"].update(
        source_git_commit=current, source_commit=hashlib.sha256(current.encode()).hexdigest()
    )
    changed = _rehash(changed)
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(changed), encoding="utf-8")
    reports = []
    env = {
        **os.environ,
        "PYTHONPATH": str(root),
        **{k: "1" for k in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS")},
    }
    for index in (1, 2):
        output = tmp_path / f"{index}.json"
        subprocess.run(
            [
                sys.executable,
                str(root / "scripts/hmm_risk/run_rotation_l2_moneyflow_supervised.py"),
                "child",
                "--input-bundle",
                str(input_path),
                "--input-sha256",
                changed["input_hash"],
                "--executor-commit",
                current,
                "--process-index",
                str(index),
                "--output",
                str(output),
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        reports.append(json.loads(output.read_text(encoding="utf-8")))
    subject.verify_processes(*reports, input_bundle=changed)
    assert reports[0]["parameters"] == completed[0]["parameters"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("started_fits", 3),
        ("completed_fits", True),
        ("forward_confirmation", "PASSED"),
        ("effect_status", "BELOW_BINDING_MBE"),
    ],
)
def test_rehashed_acceptance_does_not_override_budget_or_status(completed, field, value):
    changed = deepcopy(completed[3])
    changed[field] = value
    changed = subject.seal({k: v for k, v in changed.items() if k != "acceptance_sha256"}, "acceptance_sha256")
    with pytest.raises(baseline.RotationL2Error):
        subject.validate_acceptance(changed)


def test_rehashed_child_missing_thread_contract_is_rejected(panel, completed):
    changed = deepcopy(completed[0])
    changed["numeric_environment"]["thread_variables"] = {}
    changed = subject.seal({k: v for k, v in changed.items() if k != "report_sha256"}, "report_sha256")
    with pytest.raises(baseline.RotationL2Error, match="single-thread contract"):
        subject.verify_processes(changed, completed[1], input_bundle=panel)


def test_cli_rejects_source_output_without_writing_or_fitting(monkeypatch):
    from scripts.hmm_risk import run_rotation_l2_moneyflow_supervised as cli

    output = Path(__file__).resolve().parents[3] / "forbidden-supervised-test-output"
    monkeypatch.setattr(subject, "run_process", lambda *a, **k: pytest.fail("unsafe output may not fit"))
    assert (
        cli.main(
            [
                "child",
                "--input-bundle",
                "missing.json",
                "--output",
                str(output),
                "--input-sha256",
                "a" * 64,
                "--executor-commit",
                "b" * 40,
                "--process-index",
                "1",
            ]
        )
        == 1
    )
    assert not output.exists() and not output.with_suffix(".failure.json").exists()


def test_cli_finalization_failure_reports_completed_fits_not_success(panel, completed, tmp_path, monkeypatch):
    from scripts.hmm_risk import run_rotation_l2_moneyflow_supervised as cli

    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(panel), encoding="utf-8")
    output = tmp_path / "run"
    monkeypatch.setattr(cli, "source_head", lambda: panel["source"]["identity"]["source_git_commit"])
    monkeypatch.setattr(cli.subprocess, "check_output", lambda *a, **k: "")

    def child(command, **kwargs):
        index = int(command[command.index("--process-index") + 1])
        child_path = Path(command[command.index("--output") + 1])
        child_path.write_text(json.dumps(completed[index - 1]), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cli.subprocess, "run", child)
    monkeypatch.setattr(subject, "read_evaluation_facts", lambda *a: (_ for _ in ()).throw(OSError("readback failed")))
    assert cli.main(["run", "--input-bundle", str(input_path), "--output", str(output)]) == 1
    failure = json.loads(output.with_name("run.failure.json").read_text(encoding="utf-8"))
    assert failure["completed_fits_known"] == 2 and failure["status"] == "FAILED"
    assert not (output / "acceptance.json").exists()


@pytest.mark.skipif(
    os.environ.get("AISTOCK_HMM_DEV_ROLLBACK_AUTHORIZED") != "1",
    reason="requires explicit existing-aistock_dev rollback validation authorization",
)
def test_existing_dev_migration_writer_and_rollback(monkeypatch):
    """Real DEV SQL/writer contract, synthetic rows, zero fits, mandatory rollback."""
    from contextlib import contextmanager

    from dotenv import dotenv_values
    import psycopg2
    from psycopg2.extras import Json
    from sklearn.linear_model import Ridge

    from backend.services.hmm_risk.rotation_l2_prediction import (
        PREDICTION_COLUMNS,
        RotationL2PredictionRepository,
        _database_parameter,
        _validate_row,
    )
    from backend.tests.hmm_risk.test_rotation_l2_prediction import _acceptance

    monkeypatch.setattr(Ridge, "fit", lambda *a, **k: pytest.fail("DEV contract validation must not fit"))
    root = Path(__file__).resolve().parents[3]
    # The canonical credential location is read without logging credential contents.
    values = dotenv_values("F:/Dev/AIstock/.env")
    fields = {name: values.get("TDX_DB_DEV_" + name) for name in ("HOST", "PORT", "USER", "PASSWORD", "NAME")}
    assert all(fields.values()) and fields["NAME"] == "aistock_dev", "explicit authorized DEV configuration required"
    conn = psycopg2.connect(
        host=fields["HOST"],
        port=int(fields["PORT"]),
        user=fields["USER"],
        password=fields["PASSWORD"],
        dbname=fields["NAME"],
        connect_timeout=5,
        application_name="HMM-supervised-dev-rollback-validation",
    )

    def snapshot():
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database(),to_regclass('hmm_risk.rotation_l2_prediction')::text")
            assert cursor.fetchone() == ("aistock_dev", "hmm_risk.rotation_l2_prediction")
            cursor.execute("SELECT count(*) FROM hmm_risk.rotation_l2_prediction")
            count = cursor.fetchone()[0]
            cursor.execute(
                "SELECT conname,pg_get_constraintdef(oid),obj_description(oid,'pg_constraint'),convalidated "
                "FROM pg_constraint WHERE conrelid='hmm_risk.rotation_l2_prediction'::regclass ORDER BY conname"
            )
            return count, cursor.fetchall()

    @contextmanager
    def borrowed_connection():
        # Do not enter psycopg2's connection context: its __exit__ would commit.
        yield conn

    def direct_insert(row):
        with conn.cursor() as cursor:
            data = [_database_parameter(key, row[key]) for key in PREDICTION_COLUMNS]
            for key in ("run_summary", "feature_contributions"):
                index = PREDICTION_COLUMNS.index(key)
                if data[index] is not None:
                    data[index] = Json(data[index])
            cursor.execute(
                f"INSERT INTO hmm_risk.rotation_l2_prediction ({','.join(PREDICTION_COLUMNS)}) "
                f"VALUES ({','.join(['%s'] * len(data))})",
                tuple(data),
            )

    def database_rejects(row):
        changed = deepcopy(row)
        changed.update(run_id=canonical_sha256(uuid.uuid4().hex), prediction_id=uuid.uuid4())
        with conn.cursor() as cursor:
            cursor.execute("SAVEPOINT reject_invalid_contract")
        try:
            with pytest.raises(psycopg2.errors.CheckViolation):
                direct_insert(changed)
        finally:
            with conn.cursor() as cursor:
                cursor.execute("ROLLBACK TO SAVEPOINT reject_invalid_contract")
                cursor.execute("RELEASE SAVEPOINT reject_invalid_contract")

    prior = None
    baseline_run = canonical_sha256(uuid.uuid4().hex)
    trained_run = canonical_sha256(uuid.uuid4().hex)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout='5s'")
            cursor.execute("SET LOCAL statement_timeout='60s'")
        prior = snapshot()
        acceptance = _acceptance()
        acceptance["run_id"] = baseline_run
        acceptance = subject.seal(
            {k: v for k, v in acceptance.items() if k != "acceptance_sha256"}, "acceptance_sha256"
        )
        old_rows = rows_from_acceptance(acceptance)
        parameters = subject.seal(
            {
                "contract_hash": subject.MODEL_CONTRACT_HASH,
                "training_sha256": "1" * 64,
                "feature_names": list(subject.FEATURE_NAMES),
                "coefficients": [0.0, 1.0],
                "intercept": 0.0,
                "train_rows": 1000,
                "usable_train_dates": 126,
            },
            "parameter_sha256",
        )
        scores, states = baseline._score_and_states({row["sector_code"]: row["rotation_score"] for row in old_rows[1:]})
        new_rows = []
        for index, old_row in enumerate(old_rows):
            row = deepcopy(old_row)
            row.update(
                run_id=trained_run,
                prediction_id=None,
                validation_basis=subject.BASIS,
                model_hash=canonical_sha256(
                    {"contract_hash": subject.MODEL_CONTRACT_HASH, "parameter_sha256": parameters["parameter_sha256"]}
                ),
                evaluation_contract_hash=subject.EVALUATION_HASH,
            )
            row["run_summary"].update(
                contract_version=subject.VERSION,
                planned_fits=2,
                completed_fits=2,
                parameters=parameters,
                model_contract_hash=subject.MODEL_CONTRACT_HASH,
            )
            if index == 0:
                row.update(
                    rotation_score=None,
                    forecast_state=None,
                    feature_contributions=None,
                    availability="unavailable",
                    reason_code="hmm_risk_rotation_l2_no_resolved_members",
                    feature_eligible=False,
                    structural_eligible=False,
                )
            else:
                raw = row["rotation_score"]
                row.update(
                    rotation_score=scores[row["sector_code"]],
                    forecast_state=states[row["sector_code"]],
                    feature_contributions={
                        "raw_prediction": raw,
                        "intercept": 0.0,
                        "moneyflow_level_linear_term": 0.0,
                        "moneyflow_delta_linear_term": raw,
                        "average_rank_score": scores[row["sector_code"]],
                        "daily_rank_group": states[row["sector_code"]],
                        "model_parameter_sha256": parameters["parameter_sha256"],
                    },
                )
            new_rows.append(_validate_row(row))
        database_rejects(new_rows[1])  # RED: existing zero-fit CHECK rejects the new contract.

        migration = root / "backend/db/migrations/extend_hmm_risk_rotation_l2_supervised_20261007.sql"
        text = migration.read_text(encoding="utf-8")
        header, separator, body = text.partition("\nBEGIN;\n")
        assert separator and all(not line or line.startswith("--") for line in header.splitlines())
        assert body.rstrip().endswith("COMMIT;")
        # Execute the exact migration body under our existing rollback-only transaction.
        with conn.cursor() as cursor:
            cursor.execute(body.rstrip()[: -len("COMMIT;")])
        installed = snapshot()[1]
        assert len(installed) == len(prior[1])
        assert {r[0]: r[2:] for r in installed} == {r[0]: r[2:] for r in prior[1]}, (
            "constraint comments/validation drift"
        )
        repository = RotationL2PredictionRepository(conn_factory=borrowed_connection)
        assert repository.write_rows(old_rows)["row_count"] == 131
        first_write = repository.write_rows(new_rows)
        assert first_write["row_count"] == 131
        assert repository.write_rows(new_rows) == first_write
        readback = repository.read_date(date(2026, 3, 31), run_id=trained_run)
        assert len(readback["rows"]) == 131
        assert sum(row["availability"] == "available" for row in readback["rows"]) == 130
        assert all(row["model_version"] == subject.VERSION for row in readback["rows"])
        assert snapshot()[0] == prior[0] + 262
        for field, value in (("planned_fits", 0), ("completed_fits", 3), ("model_contract_hash", "0" * 64)):
            changed = deepcopy(new_rows[1])
            changed["run_summary"][field] = value
            database_rejects(changed)
        changed = deepcopy(new_rows[1])
        changed["feature_contributions"]["model_parameter_sha256"] = "0" * 64
        database_rejects(changed)
        changed = deepcopy(old_rows[1])
        changed["run_summary"]["planned_fits"] = 2
        database_rejects(changed)
    finally:
        conn.rollback()
        try:
            if prior is not None:
                assert snapshot() == prior, "DEV schema/comments/row count did not restore after rollback"
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM hmm_risk.rotation_l2_prediction WHERE run_id IN (%s,%s)",
                        (baseline_run, trained_run),
                    )
                    assert cursor.fetchone()[0] == 0, "transient test runs survived rollback"
        finally:
            conn.rollback()
            conn.close()
    print(
        json.dumps(
            {
                "dev_target": "aistock_dev",
                "fixture_only": True,
                "fits": 0,
                "baseline_rows_readback": 131,
                "supervised_rows_readback": 131,
                "legal_unavailable_rows": 1,
                "invalid_contracts_rejected": 5,
                "initial_rows": prior[0],
                "final_rows": prior[0],
                "constraints_and_comments_restored": True,
                "test_runs_remaining": 0,
                "transaction": "ROLLED_BACK",
                "migration_sha256": hashlib.sha256(migration.read_bytes()).hexdigest(),
            }
        )
    )
