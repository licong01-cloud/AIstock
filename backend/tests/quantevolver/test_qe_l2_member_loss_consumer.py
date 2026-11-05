"""Complete synthetic grid and cash oracles; fixtures are not admitted assets."""

# ruff: noqa: E402 -- isolated workers import from scripts directly.
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import qe_l2_member_loss_consumer as c


def seal(value):
    result = {k: v for k, v in value.items() if k != "receipt_sha256"}
    return {**result, "receipt_sha256": c.canonical_sha(result)}


@pytest.fixture(scope="module")
def sample():
    # Deliberately synthetic closed calendar, not a research dataset release.
    old = pd.bdate_range(c.START, "2026-09-30").delete([3, 21, 22, 23, 57, 100])
    calendar = ["2026-03-31", *[str(d.date()) for d in old],
                *[str(d.date()) for d in pd.bdate_range("2026-10-08", c.SIGNAL_END)]]
    catalog = [f"{i:06}.SI" for i in range(131)]
    release = {"manifest_sha256": c.MANIFEST, "release_id": c.RELEASE, "cutoff": c.SIGNAL_END, "fixture_only": True}
    contract = {"version": "hmm_risk_l2_member_loss_study_v1", "target": c.TARGET, "fixture_only": True}
    models, pins = [], {}
    for key, effective_start, effective_end, train_end in [
        ("2026Q2", "2026-04-01", "2026-06-30", "2026-03-31"),
        ("2026Q3", "2026-07-01", "2026-09-30", "2026-06-30"),
        ("2026Q4", "2026-10-01", "2026-12-31", "2026-09-30"),
    ]:
        vintage = {"id": key, "train_start": "2023-01-01", "train_end": train_end,
                   "effective_start": effective_start, "effective_end": effective_end}
        parameters = {"synthetic_test_coefficient": len(models)}
        model = seal({"schema_version": c.PRODUCER + "_model", "arm": "C", "contract": contract,
                      "catalog": catalog, "vintage": vintage, "parameters": parameters,
                      "model_sha256": c.canonical_sha(parameters)})
        models.append(model)
        pins[key] = {k: model[k] for k in ("vintage", "model_sha256", "receipt_sha256")}
    rows = []
    for i, day in enumerate(calendar[1:], 1):
        key = "2026Q2" if day <= "2026-06-30" else "2026Q3" if day <= "2026-09-30" else "2026Q4"
        future = calendar[i + 1:i + 11]
        for n, sector in enumerate(catalog):
            rows.append({"trade_date": day, "as_of_date": calendar[i - 1], "sector_code": sector,
                         "vintage_id": key, "model_sha256": pins[key]["model_sha256"],
                         c.FIELD: n / 130, "raw_" + c.FIELD: n / 130,
                         "availability": "available", "reason_code": None,
                         "target_first_return_date": future[0] if future else None,
                         "target_last_return_date": future[-1] if len(future) == 10 else None})
    prediction = seal({"schema_version": c.PRODUCER + "_predictions", "arm": "C", "contract": contract,
                       "release_binding": release, "model_bundles": models, "predictions": rows,
                       "research_only": True, "historical_as_known": False, "untouched_holdout": False,
                       "policy_applied": False})
    binding = {"schema_version": c.PREFLIGHT, "release_binding": release, "prediction_sha256": "a" * 64,
               "producer_contract_sha256": c.canonical_sha(contract), "calendar_sha256": "b" * 64,
               "industry_catalog": catalog, "models": pins}
    return prediction, binding, calendar


def test_complete_signal_window_and_economic_terminal_are_separate(sample):
    prediction, binding, calendar = sample
    before = deepcopy(prediction)
    result = c.validate_predictions(prediction, binding, calendar)
    assert len(result.rows) == 18602 and len(result.as_of) == 142
    assert result.as_of[c.ECONOMIC_END] == "2026-10-28"
    assert result.as_of["2026-09-30"] == "2026-09-29"
    assert result.receipt["economic_days"] == 141 and result.receipt["october_economic_days"] == 16
    assert result.receipt["experiment_ready"] is False
    assert result.rows[c.SIGNAL_END, binding["industry_catalog"][0]]["target_last_return_date"] is None
    assert prediction == before


@pytest.mark.parametrize("fault,reason", [
    ("old_manifest", "DATASET_IDENTITY"), ("schema", "PRODUCER_IDENTITY"),
    ("probability_target", "PRODUCER_CONTRACT"), ("no_q4", "Q4_MODELS_REQUIRED"),
    ("missing", "FULL_GRID"), ("duplicate", "ROW_KEY"), ("extra_lag", "EXTRA_LAG_OR_CLOCK"),
    ("na", "RISK_UNAVAILABLE"), ("absent_reason", "RISK_UNAVAILABLE"),
    ("nonfinite", "PUBLISHED_RAW_MISMATCH"), ("boolean", "PUBLISHED_RAW_MISMATCH"),
    ("new_clip", "PUBLISHED_RAW_MISMATCH"), ("wrong_quarter", "ROW_MODEL"),
    ("model_pin", "MODEL_PIN"), ("receipt", "PRODUCER_RECEIPT"),
    ("silent_short_window", "SIGNAL_ECONOMIC_WINDOW"),
])
def test_invalid_assets_never_fall_back_or_reduce_population(sample, fault, reason):
    original, original_binding, original_calendar = sample
    prediction, binding, calendar = deepcopy(original), deepcopy(original_binding), list(original_calendar)
    row = prediction["predictions"][0]
    if fault == "old_manifest":
        binding["release_binding"]["manifest_sha256"] = "5" * 64
    elif fault == "schema":
        prediction["schema_version"] = "old_probability_schema"
    elif fault == "probability_target":
        prediction["contract"]["target"] = {"probability": True}
    elif fault == "no_q4":
        binding["models"].pop("2026Q4")
    elif fault == "missing":
        prediction["predictions"].pop()
    elif fault == "duplicate":
        prediction["predictions"][1] = deepcopy(row)
    elif fault == "extra_lag":
        row["as_of_date"] = "2026-03-30"
    elif fault == "na":
        row.update(availability="unavailable", reason_code="MISSING_INPUT", expected_member_loss=None)
    elif fault == "absent_reason":
        row.pop("reason_code")
    elif fault == "nonfinite":
        row[c.FIELD] = float("inf")
    elif fault == "boolean":
        row[c.FIELD] = False
    elif fault == "new_clip":
        row[c.FIELD], row["raw_" + c.FIELD] = 0.20, 0.10
    elif fault == "wrong_quarter":
        row["vintage_id"] = "2026Q3"
    elif fault == "model_pin":
        binding["models"]["2026Q2"]["model_sha256"] = "0" * 64
    elif fault == "silent_short_window":
        calendar.pop()
    # Nonfinite cannot have a valid canonical JSON receipt; reject at sealing/IO.
    if fault == "nonfinite":
        with pytest.raises(ValueError):
            seal(prediction)
        return
    prediction = seal(prediction)
    if fault == "receipt":
        prediction["receipt_sha256"] = "0" * 64
    with pytest.raises(c.MemberLossError, match=reason):
        c.validate_predictions(prediction, binding, calendar)


def test_member_budget_preserves_groups_and_original_audit_field(sample):
    result = c.validate_predictions(*sample)
    day = "2026-09-30"
    catalog = sample[1]["industry_catalog"]
    risk = result.risk_context(day, {"a": catalog[0], "b": catalog[-1], "c": catalog[100]})
    before = deepcopy(risk)
    base = {"a": 3e6, "b": 1e6, "c": 2e6}
    value, audit = c.adjust_member_entry_budgets(
        day, "2026-09-29", ["a", "held1", "b", "held2", "c", "held3"], list(base), base, 1e8,
        {"max_weight": 0.05, "max_single_order_value": 1e7}, risk)
    assert value == pytest.approx({"a": 3075000, "b": 925000, "c": 2000000})
    assert [g["new_buys"] for g in audit["groups"]] == [["a", "b"], ["c"]]
    assert audit["risk_inputs"] == risk["sector_rows"] and risk == before
    assert all("expected_loss" not in r for r in audit["risk_inputs"].values())
    assert audit["contract"] == c.CONTRACT
    identity, diagnostic = c.adjust_member_entry_budgets(
        day, "2026-09-29", ["a", "b", "c"], list(base), base, 1e8,
        {"max_weight": 0.05, "max_single_order_value": 1e7}, None)
    assert identity == base and diagnostic["risk_inputs"] == {}


def test_publisher_projection_is_checked_without_qe_transform(sample):
    prediction, binding, calendar = deepcopy(sample)
    prediction["predictions"][0].update(expected_member_loss=0.0, raw_expected_member_loss=-0.1)
    prediction = seal(prediction)
    result = c.validate_predictions(prediction, binding, calendar)
    row = result.rows[c.START, binding["industry_catalog"][0]]
    assert row[c.FIELD] == 0 and row["raw_" + c.FIELD] == -0.1


@pytest.mark.parametrize("fault", ["catalog", "mapping", "cap", "missing_risk", "ambiguous_field"])
def test_budget_adapter_keeps_original_failure_boundaries(sample, fault):
    inputs = c.validate_predictions(*sample)
    catalog = sample[1]["industry_catalog"]
    risk = inputs.risk_context("2026-09-30", {"a": catalog[0], "b": catalog[-1]})
    base = {"a": 3e6, "b": 1e6}
    if fault == "catalog":
        risk["sector_rows"].pop(catalog[20])
    elif fault == "mapping":
        risk["stock_sectors"].pop("a")
    elif fault == "cap":
        base["a"] = 5e6
    elif fault == "missing_risk":
        risk["sector_rows"][catalog[20]].update(availability="unavailable", reason_code="MISSING")
    else:
        risk["sector_rows"][catalog[20]]["expected_loss"] = 0.0
    with pytest.raises(ValueError):
        c.adjust_member_entry_budgets(
            "2026-09-30", "2026-09-29", ["a", "b", "held"], list(base), base, 1e8,
            {"max_weight": 0.05, "max_single_order_value": 1e7}, risk)


def test_duplicate_json_keys_and_nonfinite_json_are_rejected(tmp_path):
    path = tmp_path / "asset.json"
    path.write_text('{"manifest": "a", "manifest": "b"}', encoding="utf-8")
    with pytest.raises(c.MemberLossError, match="JSON_DUPLICATE_KEY"):
        c.read_json(path)
    path.write_text('{"value": NaN}', encoding="utf-8")
    with pytest.raises(c.MemberLossError, match="JSON_NONFINITE"):
        c.read_json(path)


def test_cli_hashes_real_bytes_and_does_not_grant_admission(sample, tmp_path):
    prediction, binding, calendar = deepcopy(sample)
    pred, cal, bind = (tmp_path / n for n in ("prediction.json", "day.txt", "binding.json"))
    pred.write_text(json.dumps(prediction, allow_nan=False), encoding="utf-8")
    cal.write_text("\n".join(calendar) + "\n", encoding="utf-8")
    binding.update(prediction_sha256=hashlib.sha256(pred.read_bytes()).hexdigest(),
                   calendar_sha256=hashlib.sha256(cal.read_bytes()).hexdigest())
    bind.write_text(json.dumps(binding), encoding="utf-8")
    command = [sys.executable, "-B", str(ROOT / "scripts/qe_l2_member_loss_consumer.py"),
               "--prediction", str(pred), "--binding", str(bind), "--calendar", str(cal)]
    first = subprocess.run(command, capture_output=True, text=True, check=False)
    assert first.returncode == 0, first.stderr + first.stdout
    receipt = json.loads(first.stdout)
    assert receipt["status"] == "PRE_ADMISSION_VALIDATED" and receipt["experiment_ready"] is False
    pred.write_text(pred.read_text(encoding="utf-8") + " ", encoding="utf-8")
    second = subprocess.run(command, capture_output=True, text=True, check=False)
    assert second.returncode == 2 and "FILE_HASH" in second.stdout


def test_cli_nonobject_binding_is_an_explicit_rejection_before_asset_reads(tmp_path):
    path = tmp_path / "bad_binding.json"
    path.write_text("[]", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-B", str(ROOT / "scripts/qe_l2_member_loss_consumer.py"),
         "--binding", str(path), "--prediction", "must_not_open", "--calendar", "must_not_open"],
        capture_output=True, text=True, check=False)
    assert result.returncode == 2 and json.loads(result.stdout)["status"] == "REJECTED"
    assert "PREFLIGHT_BINDING" in result.stdout and not result.stderr


def test_malformed_vintage_fails_with_a_contract_reason(sample):
    prediction, binding, calendar = deepcopy(sample)
    prediction["model_bundles"][0]["vintage"] = []
    prediction["model_bundles"][0] = seal(prediction["model_bundles"][0])
    with pytest.raises(c.MemberLossError, match="MODEL_VINTAGE"):
        c.validate_predictions(seal(prediction), binding, calendar)
