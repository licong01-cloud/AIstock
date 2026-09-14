from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l1_prediction as subject
from backend.services.hmm_risk import risk_l1_g2b
from backend.services.hmm_risk.state_model_set import canonical_sha256


def _rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(31):
        row: dict[str, object] = {
            "product_bundle_id": None,
            "trade_date": date(2026, 3, 31),
            "as_of_date": date(2026, 3, 30),
            "sector_level": "L1",
            "sector_code": f"801{index:03d}.SI",
            "sector_name": f"板块 {index:02d}",
            "risk_score": index / 30.0,
            "risk_percentile": index / 30.0,
            "risk_level": "high" if index >= 24 else "watch" if index >= 18 else "normal",
            "predicted_warning": index >= 24,
            "feature_contributions": [float(index)] + [0.0] * 9,
            "availability": "available",
            "reason_code": None,
            "risk_l1_research_surface_status": "NOT_AVAILABLE",
            "risk_l1_capability_status": "RESEARCH_RISK_WARNING_AVAILABLE_FORWARD_UNCONFIRMED",
            "forward_power_status": "UNAVAILABLE",
            "forward_confirmation": "NOT_STARTED",
            "advisory_status": "NOT_AVAILABLE",
            "validation_basis": "development_causal_oof",
            "development_precision_lift": 0.12,
            "development_recall": 0.30,
            "model_hash": "a" * 64,
            "input_hash": "b" * 64,
            "mapping_snapshot_hash": "c" * 64,
            "tail_accessed": False,
            "revision": 1,
            "supersedes_prediction_id": None,
        }
        row["prediction_id"] = subject._prediction_id(row)
        rows.append(row)
    return rows


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.result = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, values=None):
        normalized = " ".join(sql.lower().split())
        if normalized.startswith("insert into"):
            row = dict(zip(subject.PREDICTION_COLUMNS, values, strict=True))
            if isinstance(row["feature_contributions"], str):
                row["feature_contributions"] = json.loads(row["feature_contributions"])
            key = (str(row["model_hash"]), row["trade_date"], row["sector_code"], row["revision"])
            self.rows.setdefault(key, row)
            self.result = []
        elif "where model_hash=%s and trade_date=%s and sector_code=%s and revision=%s" in normalized:
            key = (str(values[0]), values[1], values[2], values[3])
            row = self.rows.get(key)
            self.result = [tuple(row[column] for column in subject.PREDICTION_COLUMNS)] if row else []
        elif normalized.startswith("select distinct model_hash"):
            hashes = {key[0] for key in self.rows if "where trade_date=%s" not in normalized or key[1] == values[0]}
            self.result = [(value,) for value in sorted(hashes)]
        elif normalized.startswith("select max(trade_date)"):
            dates = [key[1] for key in self.rows if "where model_hash=%s" not in normalized or key[0] == values[0]]
            self.result = [(max(dates) if dates else None,)]
        elif "from hmm_risk.risk_l1_prediction p" in normalized:
            model, day = values
            latest = {}
            for key, row in self.rows.items():
                if key[0] == model and key[1] == day:
                    prior = latest.get(key[2])
                    if prior is None or row["revision"] > prior["revision"]:
                        latest[key[2]] = row
            self.result = [tuple(latest[code][column] for column in subject.PREDICTION_COLUMNS) for code in sorted(latest)]
        else:
            raise AssertionError(normalized)

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return list(self.result)


class _Connection:
    def __init__(self):
        self.rows = {}

    def cursor(self):
        return _Cursor(self.rows)


def _factory(connection):
    @contextmanager
    def factory():
        yield connection

    return factory


def _receipt(rows):
    body = {
        "schema_version": "hmm_risk_risk_l1_product_validation_v1",
        "model_hash": rows[0]["model_hash"],
        "trade_date": rows[0]["trade_date"].isoformat(),
        "repository_readback_passed": True,
        "api_readback_passed": True,
        "ui_readback_passed": True,
        "mock_used": False,
        "row_count": 31,
        "input_row_sha256": canonical_sha256([subject._identity_payload(row) for row in rows]),
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def test_repository_is_idempotent_model_bound_and_requires_surface_receipt(tmp_path) -> None:
    rows = _rows()
    connection = _Connection()
    repository = subject.RiskL1PredictionRepository(conn_factory=_factory(connection))

    first = repository.write_rows(rows)
    second = repository.write_rows(rows)
    detail = repository.read_date(date(2026, 3, 31), model_hash="a" * 64)
    overview = repository.overview(model_hash="a" * 64)

    assert first == second
    assert len(connection.rows) == 31
    assert len(detail["rows"]) == 31
    assert overview["risk_l1_research_surface_status"] == "NOT_AVAILABLE"

    receipt = tmp_path / "risk-product-validation.json"
    receipt.write_text(json.dumps(_receipt(rows)), encoding="utf-8")
    validated = subject.RiskL1PredictionRepository(
        conn_factory=_factory(connection), surface_validation_receipt_path=receipt
    ).overview(model_hash="a" * 64)
    assert validated["risk_l1_research_surface_status"] == "AVAILABLE_EXPERIMENTAL"
    assert validated["high_warning_count"] == 7


def test_repository_rejects_partial_batch_payload_conflict_and_forged_surface() -> None:
    rows = _rows()
    connection = _Connection()
    repository = subject.RiskL1PredictionRepository(conn_factory=_factory(connection))
    with pytest.raises(subject.RiskL1PredictionError, match="31 sectors"):
        repository.write_rows(rows[:-1])
    repository.write_rows(rows)

    changed = copy.deepcopy(rows)
    changed[0]["risk_score"] = 0.25
    changed[0]["prediction_id"] = subject._prediction_id(changed[0])
    with pytest.raises(subject.RiskL1PredictionError) as conflict:
        repository.write_rows(changed)
    assert conflict.value.reason_code == subject.REASON_CONFLICT

    forged = copy.deepcopy(rows)
    forged[0]["risk_l1_research_surface_status"] = "AVAILABLE_EXPERIMENTAL"
    forged[0]["prediction_id"] = subject._prediction_id(forged[0])
    with pytest.raises(subject.RiskL1PredictionError, match="validated product surface"):
        subject._validate_row(forged[0])

    invalid_revision = copy.deepcopy(rows[0])
    invalid_revision["revision"] = "1"
    with pytest.raises(subject.RiskL1PredictionError, match="revision chain"):
        subject._validate_row(invalid_revision)


def test_repository_fails_closed_on_tampered_surface_receipt(tmp_path) -> None:
    rows = _rows()
    connection = _Connection()
    writer = subject.RiskL1PredictionRepository(conn_factory=_factory(connection))
    writer.write_rows(rows)
    receipt = _receipt(rows)
    receipt["ui_readback_passed"] = False
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    reader = subject.RiskL1PredictionRepository(conn_factory=_factory(connection), surface_validation_receipt_path=path)
    with pytest.raises(subject.RiskL1PredictionError, match="validation receipt differs"):
        reader.overview(model_hash="a" * 64)


class _Booster:
    def __init__(self, *, model_str: str):
        assert model_str == "fixed-risk-model"

    def predict(self, frame, raw_score=False, pred_contrib=False):
        probability = np.linspace(0.1, 0.9, len(frame))
        raw = np.log(probability / (1.0 - probability))
        if pred_contrib:
            values = np.zeros((len(frame), len(risk_l1_g2b.RISK_FEATURES) + 1))
            values[:, -1] = raw
            return values
        return raw if raw_score else probability


def test_single_date_asset_entry_is_target_free_zero_fit_and_risk_specific(monkeypatch) -> None:
    from backend.services.hmm_risk import rotation_l1_input_bundle, rotation_l1_prediction

    trade_day, as_of = date(2026, 4, 1), date(2026, 3, 31)
    codes = tuple(f"801{index:03d}.SI" for index in range(31))
    receipt_body = {
        "schema_version": "hmm_risk_risk_l1_single_date_source_v1",
        "model_contract_version": risk_l1_g2b.CONTRACT_VERSION,
        "trade_date": trade_day.isoformat(),
        "as_of_date": as_of.isoformat(),
        "mapping_snapshot_sha256": "c" * 64,
        "target_columns_read": False,
    }
    receipt = {**receipt_body, "receipt_sha256": canonical_sha256(receipt_body)}
    captured = {}

    def build_source(**kwargs):
        captured.update(kwargs)
        return {
            "schema_version": receipt_body["schema_version"],
            "model_contract_version": risk_l1_g2b.CONTRACT_VERSION,
            "source_receipt": receipt,
            "input_hash": canonical_sha256(receipt_body),
            "mapping_snapshot_hash": "c" * 64,
            "feature_calendar": (),
            "sector_close": {},
            "benchmark_close": {},
            "stock_daily_inputs": [],
            "sector_names": {code: f"Sector {index}" for index, code in enumerate(codes)},
        }

    index = pd.MultiIndex.from_product([[trade_day], codes], names=["trade_date", "sector_code"])
    features = pd.DataFrame(index=index)
    for offset, feature in enumerate(risk_l1_g2b.RISK_FEATURES):
        features[feature] = np.arange(31, dtype=np.float64) + offset

    monkeypatch.setattr(rotation_l1_input_bundle, "build_rotation_l1_single_date_source_from_assets", build_source)
    monkeypatch.setattr(rotation_l1_prediction, "build_single_date_raw_features", lambda **_kwargs: features)
    model_body = {
        "schema_version": risk_l1_g2b.MODEL_SCHEMA_VERSION,
        "contract_version": risk_l1_g2b.CONTRACT_VERSION,
        "model_hash": canonical_sha256("fixed-risk-model"),
        "model_text": "fixed-risk-model",
        "feature_names": list(risk_l1_g2b.RISK_FEATURES),
        "profile": risk_l1_g2b.lightgbm_profile(),
        "input_identity": {
            "source_sha256": "a" * 64,
            "mapping_sha256": "b" * 64,
            "feature_contract_sha256": "c" * 64,
            "target_contract_sha256": "d" * 64,
        },
        "development_metrics": {"precision_lift": 0.12, "recall": 0.30},
        "risk_l1_research_surface_status": "NOT_AVAILABLE",
        "risk_l1_capability_status": "RESEARCH_RISK_WARNING_AVAILABLE_FORWARD_UNCONFIRMED",
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
        "tail_accessed": False,
    }
    artifact = {**model_body, "artifact_sha256": canonical_sha256(model_body)}

    result = subject.predict_single_date_from_assets(
        direct_v2_candidate_root=Path("X:/candidate"),
        security_identity_manifest=Path("X:/security.json"),
        provider_absence_manifest=Path("X:/provider.json"),
        industry_authority={},
        forbidden_roots=(),
        work_parent=Path("X:/work"),
        trade_date=trade_day,
        as_of_date=as_of,
        model_artifact=artifact,
        booster_factory=_Booster,
    )

    assert captured["model_contract_version"] == risk_l1_g2b.CONTRACT_VERSION
    assert len(result["rows"]) == 31
    assert result["model_fit_count"] == 0
    assert result["target_columns_read"] is False
    assert result["tail_accessed"] is False
    assert {row["validation_basis"] for row in result["rows"]} == {"single_date_frozen_model"}
