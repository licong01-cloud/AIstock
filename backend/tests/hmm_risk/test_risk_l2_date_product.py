"""New date carrier uses the original repository/API, without generating labels."""

from copy import deepcopy
from datetime import date
import json

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from backend.routers import hmm_risk
from backend.services.hmm_risk import risk_l2_date_inference as inference
from backend.services.hmm_risk import risk_l2_date_product as importer
from backend.services.hmm_risk import risk_l2_prediction as product
from backend.services.hmm_risk.formal_state_executor import write_once
from backend.tests.hmm_risk.test_risk_l2_date_inference import case, reseal  # noqa: F401
from backend.tests.hmm_risk.test_risk_l2_prediction import MemoryDB
from scripts.hmm_risk import import_risk_l2_predictions as cli


@pytest.fixture
def evidence(case, tmp_path, monkeypatch):  # noqa: F811
    source, bundle = case
    bundle["model_identity"].update(training_pit_bundle_sha256="d" * 64)
    bundle = reseal(bundle)
    prediction = reseal(
        {
            **inference.infer(source, bundle, expected_bundle_sha256=bundle["receipt_sha256"]),
            "numeric_environment": {"fixture": "fixed"},
        }
    )
    acceptance = reseal(
        {
            "schema_version": inference.VERSION + "_acceptance",
            "execution_status": "COMPLETED",
            "request_sha256": source["receipt_sha256"],
            "executor_commit": bundle["source_commit"],
            "features_sha256": bundle["receipt_sha256"],
            "prediction_sha256": prediction["receipt_sha256"],
            "model_identity": bundle["model_identity"],
            "inference_identity": bundle["inference_identity"],
            "dates": prediction["dates"],
            "two_process_bitwise_equal": True,
            "completed_children": 2,
            "fits": 0,
            **dict.fromkeys(
                (
                    "labels_read",
                    "outcomes_read",
                    "database_write",
                    "dataset_write",
                    "active_profile_write",
                    "runtime_action",
                    "policy_applied",
                    "forward_confirmed",
                ),
                False,
            ),
        }
    )
    assets = dict(
        inference_request=source,
        features=bundle,
        acceptance=acceptance,
        prediction_1=prediction,
        prediction_2=deepcopy(prediction),
    )
    request = {"schema_version": importer.REQUEST_SCHEMA}
    for key, asset in assets.items():
        path = tmp_path / f"{key}.json"
        write_once(path, asset)
        request.update({key + "_path": str(path), key + "_hash": asset["receipt_sha256"]})
    monkeypatch.setattr(product, "find_receipt", lambda *_a, **_k: None)
    return request, assets


def test_existing_cli_repository_and_api_consume_same_new_run(evidence, tmp_path, monkeypatch, capsys):
    request, assets = evidence
    monkeypatch.setattr(inference, "prepare", lambda *_a: pytest.fail("no source rebuild"))
    monkeypatch.setattr(inference, "infer", lambda *_a, **_k: pytest.fail("no new inference"))
    monkeypatch.setattr(inference.risk, "evaluate", lambda *_a: pytest.fail("no labels/evaluation"))
    monkeypatch.setattr(cli, "target_connection", lambda *_a: pytest.fail("validate must not access DB"))
    path = tmp_path / "import.json"
    write_once(path, request)
    assert cli.main(["--request", str(path), "--mode", "validate"]) == 0
    assert json.loads(capsys.readouterr().out)["row_count"] == 262
    value = product.load_product(path)
    assert value.run["model_hash"] == assets["features"]["model_identity"]["model_sha256"]
    assert value.run["validation_basis"] == importer.BASIS
    assert value.run["effect_status"] == importer.EFFECT
    assert all(row["outcome_status"] == importer.OUTCOME and row["event"] is None for row in value.rows)
    db = MemoryDB()
    repo = product.RiskL2PredictionRepository(conn_factory=db.connection)
    db.corrupt = True
    with pytest.raises(product.RiskL2PredictionError):
        repo.write_product(value, database_target="aistock_dev")
    assert db.rollbacks == 1 and db.commits == 0 and db.run is None
    db.corrupt = False
    assert repo.write_product(value, database_target="aistock_dev")["inserted_rows"] == 262
    assert repo.write_product(value, database_target="aistock_dev")["inserted_rows"] == 0
    app = FastAPI()
    app.include_router(hmm_risk.router, prefix="/api/v1")
    app.dependency_overrides[hmm_risk.get_risk_l2_repository] = lambda: repo
    with TestClient(app) as client:
        run_id = value.run["run_id"]
        overview = client.get(f"/api/v1/hmm-risk/risk-l2/overview?run_id={run_id}").json()["data"]
        assert overview["dates"] == ["2026-08-28", "2026-08-31"]
        response = client.get(f"/api/v1/hmm-risk/risk-l2?trade_date=2026-08-28&run_id={run_id}")
        assert response.status_code == 200
        detail = response.json()["data"]
        assert len(detail["rows"]) == 131 and detail["effect_status"] == importer.EFFECT
        assert detail["research_surface_status"] == "NOT_AVAILABLE"
        assert detail["day_summary"]["outcome_status_counts"] == {importer.OUTCOME: 131}
        assert client.get(f"/api/v1/hmm-risk/risk-l2?trade_date=2026-08-27&run_id={run_id}").status_code == 404


@pytest.mark.parametrize("change", ["child", "counter", "labels", "basis", "missing", "source", "effect"])
def test_import_drift_is_not_a_new_valid_run(evidence, change):
    request, original = evidence
    assets = deepcopy(original)
    if change == "child":
        assets["prediction_2"]["predictions"][0]["probability"] = 0.99
    elif change == "counter":
        assets["acceptance"]["completed_children"] = 1
    elif change == "labels":
        assets["prediction_1"]["labels"] = []
    elif change == "basis":
        assets["acceptance"]["schema_version"] = inference.risk.VERSION + "_acceptance"
    elif change == "missing":
        assets["prediction_1"]["predictions"].pop()
    elif change == "source":
        assets["acceptance"]["executor_commit"] = "0" * 40
    else:
        assets["acceptance"]["outcomes_read"] = True
    for name in assets:
        assets[name] = reseal(assets[name])
        request = {**request, name + "_hash": assets[name]["receipt_sha256"]}
    with pytest.raises(product.RiskL2PredictionError):
        importer.from_assets(request, assets)


def test_real_byte_mismatch_rejected_even_for_same_json(evidence, tmp_path):
    request, assets = evidence
    # Same parsed payload/canonical hash is weaker than actual child bytes.
    path = tmp_path / "other.json"
    path.write_text(json.dumps(assets["prediction_2"], indent=4), encoding="utf-8")
    request = {**request, "prediction_2_path": str(path)}
    import_path = tmp_path / "import.json"
    write_once(import_path, request)
    with pytest.raises(product.RiskL2PredictionError, match="bytes differ"):
        product.load_product(import_path)


def test_outcome_and_historical_basis_cannot_be_relabelled(evidence):
    value = importer.from_assets(*evidence)
    value.rows[0].update(outcome_status="OUTCOME_NOT_MATURE")
    value.run["compact_summary"]["day_row_hashes"]["2026-08-28"] = product.canonical_sha256(value.rows[:131])
    with pytest.raises(product.RiskL2PredictionError, match="outcome status"):
        product._validate_day(value.run, value.rows[:131], "2026-08-28")
    value.run["validation_basis"] = inference.risk.BASIS
    value.run["compact_summary"]["run_identity_hash"] = product._run_hash(value.run)
    with pytest.raises(product.RiskL2PredictionError, match="historical research"):
        product._validate_run(value.run)


def test_date_reader_does_not_accept_neighbor_run_or_date(evidence):
    value = importer.from_assets(*evidence)
    db = MemoryDB()
    repo = product.RiskL2PredictionRepository(conn_factory=db.connection)
    repo.write_product(value, database_target="aistock_dev")
    with pytest.raises(product.RiskL2PredictionError, match="not found"):
        repo.read_date(date(2026, 8, 31), run_id="0" * 64)


def test_all_natural_na_retains_full_catalog_and_no_research_capability(evidence):
    request, old = evidence
    assets = deepcopy(old)
    bundle = assets["features"]
    for rows in bundle["rows"].values():
        for row in rows.values():
            row.update(features=None, reason_code="hmm_risk_c010_observation_unavailable")
    bundle = assets["features"] = reseal(bundle)
    prediction = reseal(
        {
            **inference.infer(assets["inference_request"], bundle, expected_bundle_sha256=bundle["receipt_sha256"]),
            "numeric_environment": {"fixture": "fixed"},
        }
    )
    assets["prediction_1"] = assets["prediction_2"] = prediction
    assets["acceptance"].update(
        features_sha256=bundle["receipt_sha256"],
        prediction_sha256=prediction["receipt_sha256"],
        dates=prediction["dates"],
    )
    assets["acceptance"] = reseal(assets["acceptance"])
    request = {**request, **{name + "_hash": payload["receipt_sha256"] for name, payload in assets.items()}}
    value = importer.from_assets(request, assets)
    assert len(value.rows) == 262 and value.run["risk_l2_capability_status"] == "NOT_AVAILABLE"
    assert value.run["compact_summary"]["prediction_coverage"] == 0
    assert all(row["warning"] is None and row["probability"] is None for row in value.rows)
