from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import types

import pandas as pd
import pytest

from backend.services.quantevolver.config_composer import ConfigComposer

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = PROJECT_ROOT / "scripts" / "hmm_qe_assistance_contract.py"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _load_contract():
    spec = importlib.util.spec_from_file_location("hmm_qe_assistance_contract_under_test", CONTRACT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_workspace_strategy(monkeypatch: pytest.MonkeyPatch):
    modules = {
        name: types.ModuleType(name)
        for name in (
            "qlib",
            "qlib.contrib",
            "qlib.contrib.strategy",
            "qlib.contrib.strategy.signal_strategy",
            "qlib.backtest",
            "qlib.backtest.decision",
        )
    }
    modules["qlib.contrib.strategy.signal_strategy"].TopkDropoutStrategy = type(
        "TopkDropoutStrategy",
        (),
        {},
    )
    modules["qlib.backtest.decision"].Order = type("Order", (), {})
    modules["qlib.backtest.decision"].OrderDir = type("OrderDir", (), {"BUY": 1, "SELL": 0})
    modules["qlib.backtest.decision"].TradeDecisionWO = type("TradeDecisionWO", (), {})
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    sys.modules.pop("score_weighted_strategy", None)
    spec = importlib.util.spec_from_file_location(
        "score_weighted_strategy",
        SCRIPTS_DIR / "score_weighted_strategy.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _payload(subject) -> dict:
    sectors = [f"801{index:03d}.SI" for index in range(31)]
    coefficients = {sector: 1.0 for sector in sectors}
    coefficients[sectors[0]] = 1.02
    coefficients[sectors[1]] = 0.98
    row_hash = "7" * 64

    def entry(*, sector_code: str | None, applied: bool) -> dict:
        return {
            "status": subject.APPLIED if applied else subject.NOT_APPLICABLE,
            "sector_code": sector_code,
            "reason_code": None if applied else subject.UNAVAILABLE_REASON,
            "adjustment_applied": applied,
            "classification_receipt_hash": "1" * 64,
            "index_membership_receipt_hash": "2" * 64,
            "classification_row_hashes": [row_hash] if applied else [],
            # A resolved classification may legitimately have no independent
            # index-membership row; the frozen producer contract requires the
            # classification lineage, not fabricated dual lineage.
            "index_membership_row_hashes": [],
        }

    body = {
        "schema_version": subject.SCHEMA_VERSION,
        "adjustment_mode": subject.ADJUSTMENT_MODE,
        "adapter_formula": {
            "text": subject.FORMULA_TEXT,
            "sha256": subject.FORMULA_SHA256,
        },
        "tail_accessed": False,
        "source_model_contract": subject.EXPECTED_MODEL_CONTRACT,
        "model_hash": subject.EXPECTED_MODEL_HASH,
        "source_mapping_sha256": subject.EXPECTED_MAPPING_HASH,
        "source_prediction_file_sha256": subject.EXPECTED_SOURCE_HASH,
        "source_prediction_row_sha256": "3" * 64,
        "authority_identity": {"bundle_hash": subject.EXPECTED_AUTHORITY_HASH},
        "window_start": subject.EXPECTED_WINDOW[0],
        "window_end": subject.EXPECTED_WINDOW[1],
        "canonical_l1_codes": sectors,
        "daily_coefficients": {"2024-07-02": coefficients},
        "stock_sector_applicability_by_date": {
            "2024-07-02": {
                "positive": entry(sector_code=sectors[0], applied=True),
                "negative": entry(sector_code=sectors[1], applied=True),
                "unavailable": entry(sector_code=None, applied=False),
            }
        },
        "date_count": 1,
        "sector_denominator": 31,
        "prediction_row_count": 3,
        "applied_row_count": 2,
        "not_applicable_row_count": 1,
    }
    return {**body, "artifact_sha256": _sha(body)}


def test_qe_assistance_contract_validates_identity_and_applies_sign_safe_formula(monkeypatch) -> None:
    subject = _load_contract()
    monkeypatch.setattr(subject, "EXPECTED_ARTIFACT_CANONICAL_SHA256", _payload(subject)["artifact_sha256"])
    monkeypatch.setattr(subject, "EXPECTED_DATE_COUNT", 1)
    monkeypatch.setattr(subject, "EXPECTED_PREDICTION_ROWS", 3)
    monkeypatch.setattr(subject, "EXPECTED_APPLIED_ROWS", 2)
    monkeypatch.setattr(subject, "EXPECTED_NOT_APPLICABLE_ROWS", 1)

    payload = subject.validate_payload(_payload(subject))
    raw = pd.Series({"positive": 2.0, "negative": -2.0, "unavailable": 1.0})
    adjusted, trace = subject.apply_adjustment(payload, raw, "2024-07-02")

    assert adjusted.to_dict() == {
        "positive": pytest.approx(2.04),
        "negative": pytest.approx(-2.04),
        "unavailable": pytest.approx(1.0),
    }
    assert trace["mapping_mode"] == "qe_assistance_by_trade_date_v1"
    assert {row["reason"] for row in trace["rows"]} == {
        "hmm_qe_assistance_applied",
        subject.NOT_APPLICABLE,
    }


def test_qe_assistance_contract_rejects_identity_drift_and_incomplete_score_denominator(monkeypatch) -> None:
    subject = _load_contract()
    monkeypatch.setattr(subject, "EXPECTED_ARTIFACT_CANONICAL_SHA256", _payload(subject)["artifact_sha256"])
    monkeypatch.setattr(subject, "EXPECTED_DATE_COUNT", 1)
    monkeypatch.setattr(subject, "EXPECTED_PREDICTION_ROWS", 3)
    monkeypatch.setattr(subject, "EXPECTED_APPLIED_ROWS", 2)
    monkeypatch.setattr(subject, "EXPECTED_NOT_APPLICABLE_ROWS", 1)
    payload = _payload(subject)
    payload["source_mapping_sha256"] = "f" * 64
    with pytest.raises(subject.HMMQEAssistanceContractError) as exc:
        subject.validate_payload(payload)
    assert exc.value.reason_code == subject.REASON_AUTHORITY

    valid = subject.validate_payload(_payload(subject))
    with pytest.raises(subject.HMMQEAssistanceContractError) as exc:
        subject.apply_adjustment(valid, pd.Series({"positive": 1.0}), "2024-07-02")
    assert exc.value.reason_code == subject.REASON_MAPPING


def test_strategy_dependency_bundle_contains_qe_assistance_contract() -> None:
    strategy_content, dependencies = ConfigComposer()._build_strategy_py_content(
        {
            "strategy_id": "score_weighted_topk_v2",
            "source_code": (
                "from score_weighted_strategy import ScoreWeightedTopkStrategy\n\n"
                "class ScoreWeightedTopkStrategyV2(ScoreWeightedTopkStrategy):\n"
                "    pass\n"
            ),
        }
    )

    assert "from score_weighted_strategy import ScoreWeightedTopkStrategy" in strategy_content
    assert "score_weighted_strategy.py" in dependencies
    assert "hmm_qe_assistance_contract.py" in dependencies
    assert "stock_sector_applicability_by_date" in dependencies["hmm_qe_assistance_contract.py"]


def test_only_schema_less_legacy_payload_is_accepted_and_duplicate_json_is_rejected() -> None:
    subject = _load_contract()
    with pytest.raises(subject.HMMQEAssistanceContractError, match="duplicate JSON key"):
        json.loads('{"a":1,"a":2}', object_pairs_hook=subject.json_object_without_duplicate_keys)

    legacy = {"daily_coefficients": {}, "stock_sector_map": {}}
    assert subject.validate_payload_schema(legacy) is legacy
    with pytest.raises(subject.HMMQEAssistanceContractError, match="unsupported explicit") as exc:
        subject.validate_payload_schema({"schema_version": "unknown_hmm_coefficients"})
    assert exc.value.reason_code == subject.REASON_INPUT


def test_new_schema_rejects_legacy_latest_signal_date_fallback(monkeypatch) -> None:
    module = _load_workspace_strategy(monkeypatch)
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    scores = pd.Series(
        [1.0],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-07-01"), "000001.SZ")],
            names=["datetime", "instrument"],
        ),
    )

    with pytest.raises(module.HMMQEAssistanceContractError, match="signal date is missing") as exc:
        strategy._normalize_signal_scores(
            scores,
            pd.Timestamp("2024-07-02"),
            allow_latest_date_fallback=False,
        )
    assert exc.value.reason_code == module.HMM_QE_ASSISTANCE_INPUT_INVALID


def test_legacy_schema_retains_existing_latest_signal_date_fallback(monkeypatch) -> None:
    module = _load_workspace_strategy(monkeypatch)
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    scores = pd.Series(
        [1.0],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-07-01"), "000001.SZ")],
            names=["datetime", "instrument"],
        ),
    )

    normalized = strategy._normalize_signal_scores(
        scores,
        pd.Timestamp("2024-07-02"),
        allow_latest_date_fallback=True,
    )
    assert normalized.to_dict() == {"000001.SZ": 1.0}
