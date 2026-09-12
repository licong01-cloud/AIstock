from __future__ import annotations

import copy
import json
import uuid
from contextlib import contextmanager
from datetime import date, timedelta
from unittest.mock import patch
import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import rotation_l1_prediction as subject
from backend.services.hmm_risk.rotation_l1_gbdt import CONTINUOUS_FEATURES, FEATURES, canonical_sha256


SECTORS = {f"801{index:03d}.SI": f"板块 {index:02d}" for index in range(31)}


def _process_and_acceptance(*, mean_ic: float = 0.01):
    model_text = "tree\nmodel"
    model_hash = canonical_sha256(model_text)
    oof = []
    day = date(2026, 1, 5)
    for index, code in enumerate(SECTORS):
        oof.append(
            {
                "trade_date": day.isoformat(),
                "as_of_date": date(2026, 1, 2).isoformat(),
                "sector_code": code,
                "availability": "available",
                "reason_code": None,
                "rotation_score": float(index),
                "forecast_state": "fading" if index < 7 else "trending" if index >= 24 else "neutral",
                "feature_contributions": [float(index)] + [0.0] * len(FEATURES),
                "model_hash": model_hash,
            }
        )
    input_identity = {
        "source_sha256": "a" * 64,
        "mapping_sha256": "b" * 64,
        "feature_contract_sha256": "c" * 64,
    }
    payload = {
        "oof_prediction_rows": oof,
        "oof_prediction_rows_sha256": canonical_sha256(oof),
        "final_model": {"model_sha256": model_hash},
        "folds": [{"model_sha256": model_hash}],
        "development_summary": {
            "mean_rank_ic": mean_ic,
            "hac_lower_two_sided_95pct": -0.02,
            "hac_upper_two_sided_95pct": 0.04,
        },
        "input_identity": input_identity,
        "tail_access_gate": {"passed": mean_ic >= 0.02},
        "forward_power_status": "INSUFFICIENT",
        "research_product_gate": {"passed": True, "effect_threshold_applied": False},
    }
    process = {
        "reproducibility_payload": payload,
        "reproducibility_payload_sha256": canonical_sha256(payload),
        "final_model_text": model_text,
    }
    process["report_sha256"] = canonical_sha256(process)
    acceptance = {
        "reproducibility_payload_sha256": process["reproducibility_payload_sha256"],
        "research_surface_status": "NOT_AVAILABLE",
        "research_product_gate_passed": False,
    }
    acceptance["acceptance_sha256"] = canonical_sha256(acceptance)
    return process, acceptance


def _v16_process_and_acceptance():
    scoring_contract = subject._v16_scoring_contract()
    model_text = subject.canonical_json_bytes(scoring_contract).decode("utf-8")
    model_hash = canonical_sha256(scoring_contract)
    oof = []
    day = date(2026, 1, 5)
    contribution_index = subject.V14_FEATURES.index(subject.V16_SCORE_FEATURE)
    for index, code in enumerate(SECTORS):
        contributions = [0.0] * (len(subject.V14_FEATURES) + 1)
        score = (index / 30.0) - 0.5
        contributions[contribution_index] = score
        oof.append(
            {
                "trade_date": day.isoformat(),
                "as_of_date": date(2026, 1, 2).isoformat(),
                "sector_code": code,
                "availability": "available",
                "reason_code": None,
                "rotation_score": score,
                "forecast_state": "fading" if index < 7 else "trending" if index >= 24 else "neutral",
                "feature_contributions": contributions,
                "model_hash": model_hash,
            }
        )
    input_identity = {
        "source_sha256": "a" * 64,
        "mapping_sha256": "b" * 64,
        "feature_contract_sha256": "c" * 64,
    }
    payload = {
        "contract_version": subject.V16_CONTRACT_VERSION,
        "profile": {"model_kind": "deterministic_cross_section_rank", "fit_required": False},
        "scoring_contract_sha256": model_hash,
        "oof_prediction_rows": oof,
        "oof_prediction_rows_sha256": canonical_sha256(oof),
        "final_model": {
            "model_kind": "deterministic_cross_section_rank",
            "model_sha256": model_hash,
            "scoring_contract_sha256": model_hash,
            "training_performed": False,
        },
        "folds": [{"model_sha256": model_hash}],
        "development_summary": {
            "mean_rank_ic": 0.039,
            "hac_lower_two_sided_95pct": 0.001,
            "hac_upper_two_sided_95pct": 0.077,
        },
        "input_identity": input_identity,
        "tail_access_gate": {"passed": True},
        "forward_power_status": "INSUFFICIENT",
        "research_product_gate": {"passed": True, "effect_threshold_applied": False},
    }
    process = {
        "reproducibility_payload": payload,
        "reproducibility_payload_sha256": canonical_sha256(payload),
        "final_model_text": model_text,
    }
    process["report_sha256"] = canonical_sha256(process)
    acceptance = {
        "contract_version": subject.V16_CONTRACT_VERSION,
        "scoring_contract_sha256": model_hash,
        "reproducibility_payload_sha256": process["reproducibility_payload_sha256"],
        "research_surface_status": "NOT_AVAILABLE",
        "research_product_gate_passed": False,
    }
    acceptance["acceptance_sha256"] = canonical_sha256(acceptance)
    return process, acceptance


def _surface_receipt(rows, *, mock_used: bool = False):
    body = {
        "schema_version": "hmm_risk_rotation_l1_product_validation_v1",
        "model_hash": rows[0]["model_hash"],
        "trade_date": rows[0]["trade_date"].isoformat(),
        "repository_readback_passed": True,
        "api_readback_passed": True,
        "ui_readback_passed": True,
        "mock_used": mock_used,
        "row_count": len(rows),
        "input_row_sha256": canonical_sha256([subject._row_identity_payload(row) for row in rows]),
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _build_rows(process, acceptance):
    with patch.object(subject, "close_processes", return_value=acceptance):
        return subject.build_oof_prediction_rows(
            acceptance=acceptance,
            process_reports=(process, process),
            sector_names=SECTORS,
        )


def test_oof_product_rows_keep_snapshot_and_surface_authority_is_separate() -> None:
    process, acceptance = _process_and_acceptance(mean_ic=0.01)
    rows = _build_rows(process, acceptance)

    assert len(rows) == 31
    assert {row["research_surface_status"] for row in rows} == {"NOT_AVAILABLE"}
    assert {row["rotation_l1_capability_status"] for row in rows} == {"NOT_AVAILABLE"}
    assert {row["advisory_status"] for row in rows} == {"NOT_AVAILABLE"}
    assert all(str(uuid.UUID(row["prediction_id"])) == row["prediction_id"] for row in rows)

    status = subject._surface_status_from_receipt(rows, receipt=_surface_receipt(rows))
    assert status == "AVAILABLE_EXPERIMENTAL"
    assert {row["research_surface_status"] for row in rows} == {"NOT_AVAILABLE"}
    assert {row["revision"] for row in rows} == {1}
    assert all(row["supersedes_prediction_id"] is None for row in rows)


def test_v16_oof_product_rows_accept_deterministic_contract_without_fake_model() -> None:
    process, acceptance = _v16_process_and_acceptance()
    rows = _build_rows(process, acceptance)

    assert len(rows) == 31
    assert {row["model_hash"] for row in rows} == {process["reproducibility_payload"]["final_model"]["model_sha256"]}
    assert {row["rotation_l1_capability_status"] for row in rows} == {
        "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
    }
    assert {row["forward_confirmation"] for row in rows} == {"PENDING_INSUFFICIENT_POWER"}


def test_v16_oof_product_closure_passes_parent_authorities_to_revalidation() -> None:
    process, acceptance = _v16_process_and_acceptance()
    v14_reference = {"authority": "v1.4"}
    input_bundle = {"authority": "immutable-input"}
    v14_input_bundle = {"authority": "immutable-v1.4-input"}

    with patch.object(subject, "close_processes", return_value=acceptance) as close:
        rows = subject.build_oof_prediction_rows(
            acceptance=acceptance,
            process_reports=(process, process),
            sector_names=SECTORS,
            v14_reference=v14_reference,
            input_bundle=input_bundle,
            v14_input_bundle=v14_input_bundle,
        )

    assert len(rows) == 31
    assert close.call_args.kwargs == {
        "v14_reference": v14_reference,
        "input_bundle": input_bundle,
        "v14_input_bundle": v14_input_bundle,
    }


def test_oof_product_rows_reject_offline_surface_claim() -> None:
    process, acceptance = _process_and_acceptance()
    acceptance["research_surface_status"] = "AVAILABLE_EXPERIMENTAL"
    acceptance["acceptance_sha256"] = canonical_sha256(
        {key: value for key, value in acceptance.items() if key != "acceptance_sha256"}
    )
    with pytest.raises(subject.RotationL1PredictionError, match="offline/product boundary"):
        _build_rows(process, acceptance)


def test_surface_authority_rejects_mock_or_incomplete_product_validation() -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    receipt = _surface_receipt(rows, mock_used=True)

    with pytest.raises(subject.RotationL1PredictionError, match="validation receipt"):
        subject._surface_status_from_receipt(rows, receipt=receipt)


def test_overview_uses_explicit_product_receipt_model_identity(tmp_path) -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    other_rows = copy.deepcopy(rows)
    for row in other_rows:
        row["model_hash"] = "f" * 64
        row["prediction_id"] = subject._prediction_id(row)
    receipt = _surface_receipt(rows)
    receipt_path = tmp_path / "product-validation.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    connection = _Connection()
    repository = subject.RotationL1PredictionRepository(
        conn_factory=_factory(connection),
        surface_validation_receipt_path=receipt_path,
    )
    repository.write_rows(rows)
    repository.write_rows(other_rows)

    overview = repository.overview()

    assert overview["model_hash"] == rows[0]["model_hash"]
    assert overview["research_surface_status"] == "AVAILABLE_EXPERIMENTAL"


def test_oof_product_rows_reject_incomplete_daily_denominator() -> None:
    process, acceptance = _process_and_acceptance()
    process["reproducibility_payload"]["oof_prediction_rows"].pop()
    process["reproducibility_payload"]["oof_prediction_rows_sha256"] = canonical_sha256(
        process["reproducibility_payload"]["oof_prediction_rows"]
    )
    process["reproducibility_payload_sha256"] = canonical_sha256(process["reproducibility_payload"])
    process["report_sha256"] = canonical_sha256(
        {key: value for key, value in process.items() if key != "report_sha256"}
    )
    acceptance["reproducibility_payload_sha256"] = process["reproducibility_payload_sha256"]
    acceptance["acceptance_sha256"] = canonical_sha256(
        {key: value for key, value in acceptance.items() if key != "acceptance_sha256"}
    )
    with pytest.raises(subject.RotationL1PredictionError, match="denominator"):
        _build_rows(process, acceptance)


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
            self.result = [(item,) for item in sorted(hashes)]
        elif normalized.startswith("select max(trade_date)"):
            dates = [key[1] for key in self.rows if "where model_hash=%s" not in normalized or key[0] == values[0]]
            self.result = [(max(dates) if dates else None,)]
        elif "from hmm_risk.rotation_l1_prediction p" in normalized:
            model, day = values
            latest = {}
            for key, row in self.rows.items():
                if key[0] == model and key[1] == day:
                    prior = latest.get(key[2])
                    if prior is None or row["revision"] > prior["revision"]:
                        latest[key[2]] = row
            self.result = [
                tuple(latest[code][column] for column in subject.PREDICTION_COLUMNS) for code in sorted(latest)
            ]
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


def test_repository_is_idempotent_model_bound_and_reads_exactly_31_rows(tmp_path) -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    connection = _Connection()
    receipt_path = tmp_path / "product-validation.json"
    receipt_path.write_text(json.dumps(_surface_receipt(rows)), encoding="utf-8")
    repository = subject.RotationL1PredictionRepository(
        conn_factory=_factory(connection), surface_validation_receipt_path=receipt_path
    )

    first = repository.write_rows(rows)
    second = repository.write_rows(rows)
    detail = repository.read_date(date(2026, 1, 5), model_hash=rows[0]["model_hash"])
    overview = repository.overview(model_hash=rows[0]["model_hash"])

    assert first == second
    assert first["row_count"] == 31
    assert len(connection.rows) == 31
    assert len(detail["rows"]) == 31
    assert overview["sector_count"] == 31
    assert overview["binding_mbe_rank_ic"] == 0.02
    assert overview["research_surface_status"] == "AVAILABLE_EXPERIMENTAL"


def test_repository_keeps_surface_not_available_without_explicit_validation_receipt() -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    connection = _Connection()
    repository = subject.RotationL1PredictionRepository(conn_factory=_factory(connection))
    repository.write_rows(rows)

    overview = repository.overview(model_hash=rows[0]["model_hash"])

    assert overview["research_surface_status"] == "NOT_AVAILABLE"


def test_repository_rejects_forged_persisted_surface_availability() -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    forged = copy.deepcopy(rows)
    forged[0]["research_surface_status"] = "AVAILABLE_EXPERIMENTAL"
    forged[0]["prediction_id"] = subject._prediction_id(forged[0])
    repository = subject.RotationL1PredictionRepository(conn_factory=_factory(_Connection()))

    with pytest.raises(subject.RotationL1PredictionError, match="must be derived from product readback"):
        repository.write_rows(forged)


def test_repository_fails_closed_on_tampered_surface_receipt(tmp_path) -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    connection = _Connection()
    receipt = _surface_receipt(rows)
    receipt["ui_readback_passed"] = False
    receipt_path = tmp_path / "tampered.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    repository = subject.RotationL1PredictionRepository(
        conn_factory=_factory(connection), surface_validation_receipt_path=receipt_path
    )
    repository.write_rows(rows)

    with pytest.raises(subject.RotationL1PredictionError, match="validation receipt differs"):
        repository.overview(model_hash=rows[0]["model_hash"])


def test_repository_rejects_same_key_with_different_payload() -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    connection = _Connection()
    repository = subject.RotationL1PredictionRepository(conn_factory=_factory(connection))
    repository.write_rows(rows)
    changed = copy.deepcopy(rows)
    changed[0]["rotation_score"] += 1.0
    changed[0]["prediction_id"] = subject._prediction_id(changed[0])

    with pytest.raises(subject.RotationL1PredictionError, match="canonical readback"):
        repository.write_rows(changed)


def test_repository_rejects_partial_or_mixed_lineage_cross_section() -> None:
    process, acceptance = _process_and_acceptance()
    rows = _build_rows(process, acceptance)
    repository = subject.RotationL1PredictionRepository(conn_factory=_factory(_Connection()))

    with pytest.raises(subject.RotationL1PredictionError, match="31-sector"):
        repository.write_rows(rows[:-1])

    mixed = copy.deepcopy(rows)
    mixed[0]["input_hash"] = "0" * 64
    mixed[0]["prediction_id"] = subject._prediction_id(mixed[0])
    with pytest.raises(subject.RotationL1PredictionError, match="31-sector"):
        repository.write_rows(mixed)


class _Booster:
    fit_called = False

    def __init__(self, *, model_str):
        assert model_str == "frozen-model"

    def predict(self, frame, pred_contrib=False):
        score = frame.iloc[:, 0].to_numpy(dtype=np.float64)
        if not pred_contrib:
            return score
        result = np.zeros((len(frame), len(FEATURES) + 1), dtype=np.float64)
        result[:, 0] = score
        return result


def test_single_date_feature_builder_reuses_formal_formula_without_future_target() -> None:
    start = date(2025, 1, 2)
    calendar = tuple(start + timedelta(days=index) for index in range(62))
    trade_day = calendar[-1]
    sector_close = {
        (day, code): 100.0 + day_index * 0.2 + sector_index * 0.01
        for day_index, day in enumerate(calendar[:-1])
        for sector_index, code in enumerate(SECTORS)
    }
    benchmark = {day: 3000.0 + index for index, day in enumerate(calendar[:-1])}
    stock_daily = [
        {
            "source_date": day,
            "sector_code": code,
            "pit_breadth_above_ma20": 0.5,
            "moneyflow_net_amount_cny": 1.0,
            "moneyflow_traded_amount_cny": 10.0,
        }
        for day in calendar[-21:-1]
        for code in SECTORS
    ]

    frame = subject.build_single_date_raw_features(
        calendar=calendar,
        sector_close=sector_close,
        benchmark_close=benchmark,
        stock_daily_inputs=stock_daily,
        trade_date=trade_day,
    )

    assert len(frame) == 31
    assert tuple(frame.columns) == CONTINUOUS_FEATURES
    assert set(frame.index.get_level_values("trade_date")) == {trade_day}
    full_feature_panel = subject.build_label_free_feature_panel(
        calendar=calendar,
        sector_close=sector_close,
        benchmark_close=benchmark,
        stock_daily_inputs=stock_daily,
    )
    assert not any(column.startswith("target_") for column in full_feature_panel.columns)
    with pytest.raises(subject.RotationL1PredictionError, match="source boundary"):
        subject.build_single_date_raw_features(
            calendar=calendar,
            sector_close={**sector_close, (trade_day, next(iter(SECTORS))): 1.0},
            benchmark_close=benchmark,
            stock_daily_inputs=stock_daily,
            trade_date=trade_day,
        )


def test_single_date_inference_is_zero_fit_causal_and_returns_31_rows() -> None:
    start = date(2023, 1, 2)
    calendar = tuple(start + timedelta(days=index) for index in range(600))
    trade_day, as_of = calendar[-1], calendar[-2]
    benchmark = {day: 3000.0 + index * 2.0 + (index % 7) for index, day in enumerate(calendar)}
    index = pd.MultiIndex.from_product([[trade_day], list(SECTORS)], names=["trade_date", "sector_code"])
    raw = pd.DataFrame(index=index)
    for feature_index, feature in enumerate(CONTINUOUS_FEATURES):
        raw[feature] = [float(row + feature_index) for row in range(31)]
    market_values = np.asarray(
        [[benchmark[calendar[i - 1]] / benchmark[calendar[i - 2]] - 1.0, 0.001] for i in range(2, 506)]
    )
    mean = market_values.mean(axis=0)
    std = np.asarray([0.01, 0.01])
    model_text = "frozen-model"
    market_context = {
        "train_start": calendar[50].isoformat(),
        "train_count": 504,
        "train_date_sha256": "d" * 64,
        "mean": mean.tolist(),
        "std": std.tolist(),
        "lower": [-1.0, 0.0],
        "upper": [1.0, 1.0],
        "centers": [[-1.0, 1.0], [1.0, -1.0]],
        "risk_on_state": 1,
        "jump_penalty": 4.0,
    }
    market_context["receipt_sha256"] = canonical_sha256(market_context)
    final_model = {
        "model_sha256": canonical_sha256(model_text),
        "market_context": market_context,
    }

    arguments = dict(
        model_text=model_text,
        final_model=final_model,
        model_profile=subject._lightgbm_profile(),
        raw_features=raw,
        benchmark_close=benchmark,
        calendar=calendar,
        trade_date=trade_day,
        as_of_date=as_of,
        sector_names=SECTORS,
        input_hash="e" * 64,
        mapping_snapshot_hash="f" * 64,
        development_summary={
            "mean_rank_ic": 0.03,
            "hac_lower_two_sided_95pct": -0.01,
            "hac_upper_two_sided_95pct": 0.07,
        },
        capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
        forward_power_status="INSUFFICIENT",
        forward_confirmation="PENDING_INSUFFICIENT_POWER",
        booster_factory=_Booster,
    )
    rows = subject.predict_single_date_rows(**arguments)

    assert len(rows) == 31
    assert sum(row["availability"] == "available" for row in rows) == 31
    assert {row["validation_basis"] for row in rows} == {"single_date_frozen_model"}
    assert {row["research_surface_status"] for row in rows} == {"NOT_AVAILABLE"}
    assert _Booster.fit_called is False

    with pytest.raises(subject.RotationL1PredictionError, match="extra data"):
        subject.predict_single_date_rows(**{**arguments, "raw_features": raw.assign(target_5d=0.0)})


def test_v16_single_date_inference_uses_only_delta_and_preserves_typed_missing() -> None:
    calendar = tuple(date(2026, 1, 1) + timedelta(days=index) for index in range(26))
    trade_day, as_of = calendar[-1], calendar[-2]
    index = pd.MultiIndex.from_product([[trade_day], list(SECTORS)], names=["trade_date", "sector_code"])
    raw = pd.DataFrame(index=index)
    raw[subject.V16_SCORE_FEATURE] = np.arange(31, dtype=np.float64)
    raw[f"reason__{subject.V16_SCORE_FEATURE}"] = None
    missing_code = list(SECTORS)[0]
    raw.at[(trade_day, missing_code), subject.V16_SCORE_FEATURE] = np.nan
    raw.at[(trade_day, missing_code), f"reason__{subject.V16_SCORE_FEATURE}"] = "provider_absent"
    scoring_contract = subject._v16_scoring_contract()
    model_text = subject.canonical_json_bytes(scoring_contract).decode("utf-8")
    model_hash = canonical_sha256(scoring_contract)

    rows = subject.predict_single_date_rows(
        model_text=model_text,
        final_model={
            "model_kind": "deterministic_cross_section_rank",
            "model_sha256": model_hash,
            "scoring_contract_sha256": model_hash,
            "training_performed": False,
        },
        model_profile={"model_kind": "deterministic_cross_section_rank", "fit_required": False},
        raw_features=raw,
        benchmark_close={},
        calendar=calendar,
        trade_date=trade_day,
        as_of_date=as_of,
        sector_names=SECTORS,
        input_hash="e" * 64,
        mapping_snapshot_hash="f" * 64,
        development_summary={
            "mean_rank_ic": 0.039,
            "hac_lower_two_sided_95pct": 0.001,
            "hac_upper_two_sided_95pct": 0.077,
        },
        capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
        forward_power_status="INSUFFICIENT",
        forward_confirmation="PENDING_INSUFFICIENT_POWER",
        booster_factory=lambda **_kwargs: pytest.fail("v1.6 must not construct a booster"),
    )

    assert len(rows) == 31
    assert sum(row["availability"] == "available" for row in rows) == 30
    missing = next(row for row in rows if row["sector_code"] == missing_code)
    assert missing["reason_code"] == "provider_absent"
    assert missing["rotation_score"] is None
    assert all(row["model_hash"] == model_hash for row in rows)
    assert all(
        len(row["feature_contributions"]) == len(subject.V14_FEATURES) + 1
        for row in rows
        if row["availability"] == "available"
    )


def test_v16_asset_entry_requests_target_free_moneyflow_source(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.services.hmm_risk import rotation_l1_input_bundle

    calendar = tuple(date(2026, 1, 1) + timedelta(days=index) for index in range(26))
    trade_day, as_of = calendar[-1], calendar[-2]
    stock = [
        {
            "source_date": day,
            "sector_code": code,
            "moneyflow_net_amount_cny": float(sector_index + day_index),
            "moneyflow_traded_amount_cny": 100.0,
            "moneyflow_reason_code": None,
        }
        for day_index, day in enumerate(calendar[:-1])
        for sector_index, code in enumerate(SECTORS)
    ]
    captured = {}
    source_body = {
        "schema_version": "hmm_risk_rotation_l1_single_date_source_v2",
        "model_contract_version": subject.V16_CONTRACT_VERSION,
        "trade_date": trade_day.isoformat(),
        "as_of_date": as_of.isoformat(),
        "mapping_snapshot_sha256": "f" * 64,
        "target_columns_read": False,
        "market_context_used_for_score": False,
        "sector_close_used_for_score": False,
    }
    source_receipt = {**source_body, "receipt_sha256": canonical_sha256(source_body)}

    def build_source(**kwargs):
        captured.update(kwargs)
        return {
            "schema_version": "hmm_risk_rotation_l1_single_date_source_v2",
            "model_contract_version": subject.V16_CONTRACT_VERSION,
            "feature_calendar": calendar,
            "stock_daily_inputs": stock,
            "sector_names": SECTORS,
            "input_hash": source_receipt["receipt_sha256"],
            "mapping_snapshot_hash": "f" * 64,
            "source_receipt": source_receipt,
        }

    monkeypatch.setattr(rotation_l1_input_bundle, "build_rotation_l1_single_date_source_from_assets", build_source)
    scoring_contract = subject._v16_scoring_contract()
    model_text = subject.canonical_json_bytes(scoring_contract).decode("utf-8")
    model_hash = canonical_sha256(scoring_contract)
    result = subject.predict_single_date_from_assets(
        direct_v2_candidate_root=(tmp_path / "candidate").resolve(),
        security_identity_manifest=tmp_path / "security.json",
        provider_absence_manifest=tmp_path / "provider.json",
        industry_authority={},
        forbidden_roots=(),
        work_parent=tmp_path / "work",
        trade_date=trade_day,
        as_of_date=as_of,
        model_text=model_text,
        final_model={
            "model_kind": "deterministic_cross_section_rank",
            "model_sha256": model_hash,
            "scoring_contract_sha256": model_hash,
            "training_performed": False,
        },
        model_profile={"model_kind": "deterministic_cross_section_rank", "fit_required": False},
        development_summary={
            "mean_rank_ic": 0.039,
            "hac_lower_two_sided_95pct": 0.001,
            "hac_upper_two_sided_95pct": 0.077,
        },
        capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
        forward_power_status="INSUFFICIENT",
        forward_confirmation="PENDING_INSUFFICIENT_POWER",
    )

    assert captured["market_start"] is None
    assert captured["model_contract_version"] == subject.V16_CONTRACT_VERSION
    assert len(result["rows"]) == 31
    assert result["source_receipt"]["target_columns_read"] is False


def test_single_date_asset_entry_binds_explicit_source_and_never_requests_targets(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.services.hmm_risk import rotation_l1_input_bundle

    start = date(2023, 1, 2)
    market_calendar = tuple(start + timedelta(days=index) for index in range(600))
    feature_calendar = market_calendar[-62:]
    trade_day, as_of = feature_calendar[-1], feature_calendar[-2]
    benchmark = {day: 3000.0 + index * 2.0 + (index % 7) for index, day in enumerate(market_calendar[:-1])}
    sector_close = {
        (day, code): 100.0 + day_index * 0.2 + code_index * 0.01
        for day_index, day in enumerate(feature_calendar[:-1])
        for code_index, code in enumerate(SECTORS)
    }
    stock_daily = [
        {
            "source_date": day,
            "sector_code": code,
            "pit_breadth_above_ma20": 0.5,
            "moneyflow_net_amount_cny": 1.0,
            "moneyflow_traded_amount_cny": 10.0,
        }
        for day in feature_calendar[-21:-1]
        for code in SECTORS
    ]
    model_text = "frozen-model"
    market_context = {
        "train_start": market_calendar[50].isoformat(),
        "train_count": 504,
        "train_date_sha256": "d" * 64,
        "mean": [0.0, 0.0],
        "std": [0.01, 0.01],
        "lower": [-1.0, 0.0],
        "upper": [1.0, 1.0],
        "centers": [[-1.0, 1.0], [1.0, -1.0]],
        "risk_on_state": 1,
        "jump_penalty": 4.0,
    }
    market_context["receipt_sha256"] = canonical_sha256(market_context)
    final_model = {"model_sha256": canonical_sha256(model_text), "market_context": market_context}
    candidate_root = (tmp_path / "candidate").resolve()
    captured = {}
    source_body = {
        "schema_version": "hmm_risk_rotation_l1_single_date_source_v1",
        "trade_date": trade_day.isoformat(),
        "as_of_date": as_of.isoformat(),
        "mapping_snapshot_sha256": "f" * 64,
        "target_columns_read": False,
    }
    source_receipt = {**source_body, "receipt_sha256": canonical_sha256(source_body)}

    def build_source(**kwargs):
        captured.update(kwargs)
        return {
            "schema_version": "hmm_risk_rotation_l1_single_date_source_v1",
            "feature_calendar": feature_calendar,
            "market_calendar": market_calendar,
            "sector_close": sector_close,
            "benchmark_close": benchmark,
            "stock_daily_inputs": stock_daily,
            "sector_names": SECTORS,
            "input_hash": source_receipt["receipt_sha256"],
            "mapping_snapshot_hash": "f" * 64,
            "source_receipt": source_receipt,
        }

    monkeypatch.setattr(rotation_l1_input_bundle, "build_rotation_l1_single_date_source_from_assets", build_source)
    result = subject.predict_single_date_from_assets(
        direct_v2_candidate_root=candidate_root,
        security_identity_manifest=tmp_path / "security.json",
        provider_absence_manifest=tmp_path / "provider.json",
        industry_authority={},
        forbidden_roots=(),
        work_parent=tmp_path / "work",
        trade_date=trade_day,
        as_of_date=as_of,
        model_text=model_text,
        final_model=final_model,
        model_profile=subject._lightgbm_profile(),
        development_summary={
            "mean_rank_ic": 0.03,
            "hac_lower_two_sided_95pct": -0.01,
            "hac_upper_two_sided_95pct": 0.07,
        },
        capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
        forward_power_status="INSUFFICIENT",
        forward_confirmation="PENDING_INSUFFICIENT_POWER",
        booster_factory=_Booster,
    )

    assert captured["direct_v2_candidate_root"] == candidate_root
    assert captured["market_start"] == market_calendar[50]
    assert len(result["rows"]) == 31
    assert result["source_receipt"]["target_columns_read"] is False

    monkeypatch.setattr(
        rotation_l1_input_bundle,
        "build_rotation_l1_single_date_source_from_assets",
        lambda **kwargs: {**build_source(**kwargs), "input_hash": "0" * 64},
    )
    with pytest.raises(subject.RotationL1PredictionError, match="source receipt differs"):
        subject.predict_single_date_from_assets(
            direct_v2_candidate_root=candidate_root,
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "provider.json",
            industry_authority={},
            forbidden_roots=(),
            work_parent=tmp_path / "work",
            trade_date=trade_day,
            as_of_date=as_of,
            model_text=model_text,
            final_model=final_model,
            model_profile=subject._lightgbm_profile(),
            development_summary={
                "mean_rank_ic": 0.03,
                "hac_lower_two_sided_95pct": -0.01,
                "hac_upper_two_sided_95pct": 0.07,
            },
            capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
            forward_power_status="INSUFFICIENT",
            forward_confirmation="PENDING_INSUFFICIENT_POWER",
            booster_factory=_Booster,
        )

    captured.clear()
    with pytest.raises(subject.RotationL1PredictionError, match="does not permit"):
        subject.predict_single_date_from_assets(
            direct_v2_candidate_root=candidate_root,
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "provider.json",
            industry_authority={},
            forbidden_roots=(),
            work_parent=tmp_path / "work",
            trade_date=trade_day,
            as_of_date=as_of,
            model_text=model_text,
            final_model=final_model,
            model_profile=subject._lightgbm_profile(),
            development_summary={
                "mean_rank_ic": 0.0,
                "hac_lower_two_sided_95pct": -0.01,
                "hac_upper_two_sided_95pct": 0.01,
            },
            capability_status="NOT_AVAILABLE",
            forward_power_status="UNAVAILABLE",
            forward_confirmation="NOT_STARTED",
            booster_factory=_Booster,
        )
    assert captured == {}

    with pytest.raises(subject.RotationL1PredictionError, match="below binding MBE"):
        subject.predict_single_date_from_assets(
            direct_v2_candidate_root=candidate_root,
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "provider.json",
            industry_authority={},
            forbidden_roots=(),
            work_parent=tmp_path / "work",
            trade_date=trade_day,
            as_of_date=as_of,
            model_text=model_text,
            final_model=final_model,
            model_profile=subject._lightgbm_profile(),
            development_summary={
                "mean_rank_ic": 0.0,
                "hac_lower_two_sided_95pct": -0.01,
                "hac_upper_two_sided_95pct": 0.01,
            },
            capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
            forward_power_status="SUFFICIENT",
            forward_confirmation="PENDING_INCONCLUSIVE",
            booster_factory=_Booster,
        )
    assert captured == {}

    with pytest.raises(subject.RotationL1PredictionError, match="forward state coupling"):
        subject.predict_single_date_from_assets(
            direct_v2_candidate_root=candidate_root,
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "provider.json",
            industry_authority={},
            forbidden_roots=(),
            work_parent=tmp_path / "work",
            trade_date=trade_day,
            as_of_date=as_of,
            model_text=model_text,
            final_model=final_model,
            model_profile=subject._lightgbm_profile(),
            development_summary={
                "mean_rank_ic": 0.03,
                "hac_lower_two_sided_95pct": -0.01,
                "hac_upper_two_sided_95pct": 0.07,
            },
            capability_status="RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
            forward_power_status="INSUFFICIENT",
            forward_confirmation="PENDING_INCONCLUSIVE",
            booster_factory=_Booster,
        )
    assert captured == {}


def test_single_date_rejects_wrong_as_of_and_not_available_capability() -> None:
    with pytest.raises(subject.RotationL1PredictionError, match="calendar identity"):
        subject.predict_single_date_rows(
            model_text="x",
            final_model={},
            model_profile=subject._lightgbm_profile(),
            raw_features=pd.DataFrame(),
            benchmark_close={},
            calendar=(),
            trade_date=date(2026, 1, 2),
            as_of_date=date(2026, 1, 1),
            sector_names=SECTORS,
            input_hash="a" * 64,
            mapping_snapshot_hash="b" * 64,
            development_summary={
                "mean_rank_ic": 0.0,
                "hac_lower_two_sided_95pct": -0.1,
                "hac_upper_two_sided_95pct": 0.1,
            },
            capability_status="NOT_AVAILABLE",
            forward_power_status="UNAVAILABLE",
            forward_confirmation="NOT_STARTED",
            booster_factory=_Booster,
        )
