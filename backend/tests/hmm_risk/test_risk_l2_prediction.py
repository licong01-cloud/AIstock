"""Synthetic contract counterexamples; never a real product acceptance receipt."""

from __future__ import annotations

from contextlib import contextmanager
import copy
from datetime import date
from pathlib import Path
import json

import pytest

from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk import risk_l2_prediction as product
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_model import receipt
from backend.tests.hmm_risk.test_risk_l2 import _calendar


@pytest.fixture(scope="module")
def assets():
    calendar = _calendar()
    plan = risk.schedule(calendar)
    codes = [f"801{i:03d}.SI" for i in range(131)]
    identity = {
        "pit_bundle_sha256": "a" * 64,
        "industry_authority_sha256": "b" * 64,
        "release_identity": {"dataset_manifest_sha256": "c" * 64},
    }
    features = receipt(
        {
            "schema_version": risk.VERSION + "_features",
            "contract": risk.CONTRACT,
            "calendar": calendar,
            "catalog": codes,
            "input_identity": identity,
            "feature_names": risk.CONTRACT["feature_names"],
        }
    )
    parameters = {"classes": [0, 1], "fixture": True}
    predictions, labels = [], {}
    for day in plan["dev"]:
        labels[day] = {}
        for i, code in enumerate(codes):
            predictions.append(
                {
                    "trade_date": day,
                    "as_of_date": plan["as_of"][day],
                    "sector_code": code,
                    "probability": 0.6 if i < 30 else 0.1,
                    "warning": i < 30,
                    "availability": "available",
                    "reason_code": None,
                    "structural_eligible": True,
                    "volatility_Nd": float(i) / 1000,
                }
            )
            labels[day][code] = {
                "status": "AVAILABLE",
                "event": int(i < 20),
                "drawdown": -0.1 if i < 20 else -0.01,
                "return": 0.02,
            }
            if day not in plan["mature"]:
                labels[day][code] = {"status": "OUTCOME_NOT_MATURE", "event": None, "drawdown": None, "return": None}
    sealed = receipt(
        {
            "schema_version": risk.VERSION + "_sealed",
            "contract": risk.CONTRACT,
            "status": "PREDICTIONS_SEALED",
            "fits": 1,
            "tail_accessed": False,
            "target_accessed": False,
            "model_sha256": canonical_sha256(parameters),
            "parameters": parameters,
            "input_identity": identity,
            "feature_sha256": features["receipt_sha256"],
            "predictions": predictions,
        }
    )
    result = risk.evaluate(sealed, labels, calendar)
    acceptance = receipt(
        {
            "schema_version": risk.VERSION + "_acceptance",
            "contract": risk.CONTRACT,
            "execution_status": "COMPLETED",
            "fresh_process_bitwise_equal": True,
            "planned_fits": 2,
            "completed_fits": 2,
            "tail_accessed": False,
            "database_write": False,
            "runtime_action": False,
            "validation_basis": risk.BASIS,
            "advisory_status": "NOT_AVAILABLE",
            "research_surface_status": "NOT_AVAILABLE",
            "executor_commit": "d" * 40,
            "effect_status": result["effect_status"],
            "result": result,
            "model": {k: v for k, v in sealed.items() if k not in {"receipt_sha256", "predictions"}},
        }
    )
    request = {
        "schema_version": product.REQUEST_SCHEMA,
        "acceptance_hash": acceptance["receipt_sha256"],
        "sealed_hash": sealed["receipt_sha256"],
        "features_hash": features["receipt_sha256"],
        "model_hash": sealed["model_sha256"],
        "input_hash": canonical_sha256(identity),
        "mapping_hash": canonical_sha256({k: identity[k] for k in ("pit_bundle_sha256", "industry_authority_sha256")}),
        "pit_bundle_sha256": identity["pit_bundle_sha256"],
        "dataset_manifest_sha256": "c" * 64,
        "executor_commit": "d" * 40,
    }
    return request, acceptance, sealed, features


@pytest.fixture(scope="module")
def prepared(assets):
    return product.product_from_assets(*assets)


def test_full_population_order_independent_zero_refit(assets, prepared, monkeypatch):
    monkeypatch.setattr(risk.LogisticRegression, "fit", lambda *_a, **_k: pytest.fail("unexpected fit"))
    request, acceptance, sealed, features = assets
    reordered = {
        **acceptance,
        "result": {**acceptance["result"], "predictions": list(reversed(acceptance["result"]["predictions"]))},
    }
    reordered = receipt({k: v for k, v in reordered.items() if k != "receipt_sha256"})
    changed = product.product_from_assets(
        {**request, "acceptance_hash": reordered["receipt_sha256"]}, reordered, sealed, features
    )
    assert len(changed.rows) == 55544
    assert {k: v for k, v in changed.rows[0].items() if k != "run_id"} == {
        k: v for k, v in prepared.rows[0].items() if k != "run_id"
    }
    assert changed.run["compact_summary"]["overall"] == prepared.run["compact_summary"]["overall"]
    assert prepared.run["risk_l2_capability_status"] == product.CAPABILITY


@pytest.mark.parametrize(
    "change", ["rehash", "unknown_schema", "input_pin", "missing", "duplicate", "as_of", "probability", "summary"]
)
def test_asset_drift_rejected(assets, change):
    request, original, sealed, features = assets
    acceptance = copy.deepcopy(original)
    if change == "input_pin":
        request = {**request, "input_hash": "f" * 64}
    elif change == "unknown_schema":
        acceptance["schema_version"] = "unknown"
    elif change == "missing":
        acceptance["result"]["predictions"].pop()
    elif change == "duplicate":
        acceptance["result"]["predictions"][1] = acceptance["result"]["predictions"][0]
    elif change in {"as_of", "probability"}:
        acceptance["result"]["predictions"][0]["as_of_date" if change == "as_of" else "probability"] = (
            "2024-07-01" if change == "as_of" else 0.9
        )
    else:
        acceptance["result"]["metrics"]["overall"]["TP"] += 1
    if change == "rehash":
        acceptance = receipt({k: v for k, v in acceptance.items() if k != "receipt_sha256"})
    elif change not in {"input_pin"}:
        # Explicit authority refresh bypasses only the outer hash to test deeper contracts.
        acceptance = receipt({k: v for k, v in acceptance.items() if k != "receipt_sha256"})
        request = {**request, "acceptance_hash": acceptance["receipt_sha256"]}
    with pytest.raises(product.RiskL2PredictionError):
        product.product_from_assets(request, acceptance, sealed, features)


@pytest.mark.parametrize(
    "probability,warning", [(0.0, False), (0.199999, False), (0.2, True), (1.0, True), (None, None)]
)
def test_threshold_and_null_outcome_coupling(prepared, probability, warning):
    row = {**prepared.rows[-1], "probability": probability, "warning": warning}
    if probability is None:
        row.update(
            availability="unavailable", reason_code="hmm_risk_c010_observation_unavailable", structural_eligible=False
        )
    assert product.validate_row(row)["warning"] is warning
    row["event"] = 0
    with pytest.raises(product.RiskL2PredictionError, match="negative label"):
        product.validate_row(row)


@pytest.mark.parametrize(
    "field,value", [("probability", float("nan")), ("probability", float("inf")), ("warning", None), ("event", True)]
)
def test_invalid_numeric_or_unknown_not_silenced(prepared, field, value):
    with pytest.raises(product.RiskL2PredictionError):
        product.validate_row({**prepared.rows[0], field: value})


class MemoryDB:
    """Transactional DB double records whether validation happened before commit."""

    def __init__(self):
        from types import SimpleNamespace

        self.info = SimpleNamespace(dbname="aistock_dev")
        self.run = None
        self.rows = []
        self.answer = []
        self.commits = self.rollbacks = 0
        self.corrupt = False
        self.selects = []

    @contextmanager
    def connection(self):
        old = copy.deepcopy((self.run, self.rows))
        try:
            yield self
            self.commits += 1
        except Exception:
            self.run, self.rows = old
            self.rollbacks += 1
            raise

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, args):
        self.selects.append(sql)
        if sql.startswith("SELECT pg_advisory"):
            self.answer = []
        elif sql.startswith("SELECT run_id FROM"):
            self.answer = [(args[0],)] if self.run else []
        elif sql.startswith("INSERT INTO hmm_risk.risk_l2_run"):
            self.run = args
        elif "FROM hmm_risk.risk_l2_run" in sql:
            self.answer = [self.run] if self.run and self.run[0] == args[0] else []
        elif "FROM hmm_risk.risk_l2_prediction" in sql:
            self.answer = [r for r in self.rows if r[0] == args[0] and (len(args) == 1 or r[1] == args[1])]
            if self.corrupt and self.answer:
                self.answer = [tuple("BROKEN" if i == 4 else v for i, v in enumerate(self.answer[0])), *self.answer[1:]]
        else:
            raise AssertionError(sql)

    def executemany(self, sql, rows):
        assert sql.startswith("INSERT INTO hmm_risk.risk_l2_prediction")
        self.rows.extend(rows)

    def fetchone(self):
        return self.answer[0] if self.answer else None

    def fetchall(self):
        return self.answer


def test_atomic_writer_idempotence_and_precommit_rollback(prepared):
    db = MemoryDB()
    repo = product.RiskL2PredictionRepository(conn_factory=db.connection)
    db.corrupt = True
    with pytest.raises(product.RiskL2PredictionError):
        repo.write_product(prepared, database_target="aistock_dev")
    assert (db.commits, db.rollbacks, db.run, len(db.rows)) == (0, 1, None, 0)
    db.corrupt = False
    assert repo.write_product(prepared, database_target="aistock_dev")["inserted_rows"] == 55544
    assert repo.write_product(prepared, database_target="aistock_dev")["inserted_rows"] == 0
    assert len(db.rows) == 55544 and db.commits == 2
    db.rows.pop()
    with pytest.raises(product.RiskL2PredictionError):
        repo.write_product(prepared, database_target="aistock_dev")
    assert db.commits == 2 and db.rollbacks == 2


def test_writer_no_implicit_target_or_wrong_db(prepared):
    db = MemoryDB()
    repo = product.RiskL2PredictionRepository(conn_factory=db.connection)
    with pytest.raises(product.RiskL2PredictionError, match="explicit"):
        repo.write_product(prepared)
    with pytest.raises(product.RiskL2PredictionError, match="differs"):
        repo.write_product(prepared, database_target="aistock")
    assert not db.selects and db.run is None and db.commits == 0


def test_read_date_complete_identity_no_neighbor_or_full_rescan(prepared):
    db = MemoryDB()
    db.run = product._params(prepared.run, product.RUN_COLUMNS)
    db.rows = [product._params(r, product.ROW_COLUMNS) for r in prepared.rows]
    repo = product.RiskL2PredictionRepository(conn_factory=db.connection)
    result = repo.read_date(date(2026, 3, 31), run_id=prepared.run["run_id"])
    assert len(result["rows"]) == 131 and result["day_summary"]["warning_count"] == 30
    assert result["day_summary"]["outcome_status_counts"] == {"OUTCOME_NOT_MATURE": 131}
    assert result["research_surface_status"] == "NOT_AVAILABLE" and result["forward_confirmation"] == "NOT_STARTED"
    assert all("AND trade_date=%s" in q for q in db.selects if "risk_l2_prediction" in q)
    with pytest.raises(product.RiskL2PredictionError, match="not found"):
        repo.read_date(date(2026, 4, 1), run_id=prepared.run["run_id"])
    db.corrupt = True
    with pytest.raises(product.RiskL2PredictionError):
        repo.read_date(date(2026, 3, 31), run_id=prepared.run["run_id"])


def test_existing_invalid_surface_receipt_is_error_not_absent(prepared, tmp_path, monkeypatch):
    path = tmp_path / "surface.json"
    path.write_text("{}", encoding="utf-8")
    # exists() follows links and can hide an existing invalid path entry.
    monkeypatch.setattr(Path, "exists", lambda self: False)
    repo = product.RiskL2PredictionRepository(surface_validation_receipt_path=path)
    with pytest.raises(product.RiskL2PredictionError, match="receipt is invalid"):
        repo._surface(prepared.run)


def test_surface_requires_real_check_flags_dates_not_unrelated_deployment(prepared, tmp_path):
    run = prepared.run
    value = {k: run[k] for k in ("run_id", "model_hash", "acceptance_hash", "input_hash")}
    value.update(
        schema_version=product.SURFACE_SCHEMA,
        row_hash=run["compact_summary"]["row_hash"],
        deployment_commit="e" * 40,
        target="backend-main",
        writer_readback=True,
        api_readback=True,
        browser_no_mock=True,
        day_row_hashes={d: run["compact_summary"]["day_row_hashes"][d] for d in (run["dates"][0], run["dates"][-1])},
    )
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(receipt(value)), encoding="utf-8")
    repo = product.RiskL2PredictionRepository(surface_validation_receipt_path=path, deployment_commit="e" * 40)
    assert repo._surface(run) == "AVAILABLE_EXPERIMENTAL"
    repo.deployment_commit = "f" * 40
    assert repo._surface(run) == "AVAILABLE_EXPERIMENTAL"
    repo.deployment_commit = "e" * 40
    value["browser_no_mock"] = False
    path.write_text(json.dumps(receipt(value)), encoding="utf-8")
    with pytest.raises(product.RiskL2PredictionError):
        repo._surface(run)


def test_windows_reparse_artifact_rejected_without_following_link(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace

    path = tmp_path / "asset.json"
    path.write_text("{}", encoding="utf-8")
    original = Path.lstat

    def changed(p):
        value = original(p)
        return SimpleNamespace(st_mode=value.st_mode, st_file_attributes=0x400) if p == path else value

    monkeypatch.setattr(Path, "lstat", changed)
    with pytest.raises(product.RiskL2PredictionError, match="junction"):
        product._regular(path)


def test_file_only_request_db_poison_and_symlink_boundary(assets, monkeypatch, tmp_path):
    monkeypatch.setattr(product, "get_conn", lambda **_k: pytest.fail("DB forbidden"))
    request, acceptance, sealed, features = assets
    request = dict(request)
    for name, payload in (("acceptance", acceptance), ("sealed", sealed), ("features", features)):
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps(payload), encoding="utf-8")
        request[name + "_path"] = str(path)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    assert len(product.load_product(path).rows) == 55544
    monkeypatch.setattr(
        product,
        "_regular",
        lambda _p: (_ for _ in ()).throw(
            product.RiskL2PredictionError(product.REASON_INPUT, "symlink/junction forbidden")
        ),
    )
    with pytest.raises(product.RiskL2PredictionError, match="symlink"):
        product.load_product(path)
