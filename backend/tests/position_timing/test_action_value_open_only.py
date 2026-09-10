from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    CORE_INFORMATION_BLOCK,
    ActionValueError,
    PositionState,
    cutoff_on,
)
from backend.services.position_timing.action_value_advice import (
    ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    action_authority_policy_sha256,
    decide_stock_day,
)
from backend.services.position_timing.action_value_model import HEADS
from backend.services.position_timing import action_value_open_only as open_only
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256


class FixedModel:
    metadata = {"model_sha256": "1" * 64}

    def __init__(self, value: float = 20.0) -> None:
        self.value = value
        self.objectives: list[str] = []

    def predict(self, frame, objectives, *, decision_as_of):
        self.objectives.extend(objectives)
        return np.full(len(objectives), self.value)


def _inputs(*, quantity: int, entry_cost: Decimal | None = None) -> dict:
    index = pd.bdate_range("2024-01-01", periods=60)
    bars = pd.DataFrame(
        {
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.0,
            "volume": 1000.0,
            "factor": 1.0,
        },
        index=index,
    )
    return {
        "symbol": "000001.SZ",
        "state": PositionState(
            quantity,
            quantity,
            Decimal("100000") - Decimal(10) * quantity,
            Decimal("100000"),
            entry_cost,
            20 if entry_cost is not None else None,
        ),
        "bars": bars,
        "benchmark": pd.Series(3000.0, index=index),
        "decision_as_of": cutoff_on(index[-1].date()),
    }


def test_open_only_never_uses_entry_head_to_add() -> None:
    model = FixedModel()

    decision = decide_stock_day(
        **_inputs(quantity=1000),
        model=model,
        model_action_authority=OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    )

    assert decision.action == "HOLD"
    assert decision.plan.delta == 0
    assert model.objectives == []
    assert {candidate["action"] for candidate in decision.candidates} == {"HOLD"}
    assert "MODEL_ADD_AUTHORITY_REMOVED" in decision.reason_codes
    assert "MODEL_EXIT_AUTHORITY_REMOVED" in decision.reason_codes


def test_open_only_can_open_from_cash_with_same_entry_head() -> None:
    model = FixedModel()

    decision = decide_stock_day(
        **_inputs(quantity=0),
        model=model,
        model_action_authority=OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    )

    assert decision.action == "OPEN"
    assert decision.plan.delta > 0
    assert model.objectives
    assert set(model.objectives) == {HEADS[0]}
    assert {candidate["action"] for candidate in decision.candidates} == {"WAIT", "OPEN"}


def test_frozen_risk_exit_precedes_open_only_model() -> None:
    decision = decide_stock_day(
        **_inputs(quantity=1000, entry_cost=Decimal("11")),
        model=None,
        model_action_authority=OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    )

    assert decision.action == "EXIT"
    assert decision.plan.risk_exit is True
    assert decision.authority == "FROZEN_RULE_RISK_OVERRIDE"


def test_open_only_policy_identity_is_distinct() -> None:
    entry_hash = action_authority_policy_sha256(
        CORE_INFORMATION_BLOCK, ENTRY_ONLY_MODEL_ACTION_AUTHORITY
    )
    open_hash = action_authority_policy_sha256(
        CORE_INFORMATION_BLOCK, OPEN_ONLY_MODEL_ACTION_AUTHORITY
    )

    assert open_hash != entry_hash
    assert len(open_hash) == 64


def _write_diagnostic_sources(tmp_path: Path, *, supported: bool = True) -> tuple[Path, Path]:
    v4 = tmp_path / "v4"
    heldout = tmp_path / "heldout"
    v4.mkdir()
    heldout.mkdir()
    pd.DataFrame(
        {
            "objective": ["ENTRY_ACTION_VALUE_V2", "ENTRY_ACTION_VALUE_V2", "EXIT_ACTION_VALUE_V2"],
            "holding_exposure": [0.0, 0.0 if supported else 0.25, 0.9],
            "cash_fraction": [1.0, 1.0, 0.1],
            "holding_age_missing": [1.0, 1.0, 0.0],
            "entry_cost_missing": [1.0, 1.0, 0.0],
        }
    ).to_parquet(v4 / "training_rows.parquet", index=False)
    rows = []
    for baseline in ("BUY_AND_HOLD", "FROZEN_L1_V1"):
        rows.extend(
            (
                {
                    "sleeve_id": "s1",
                    "baseline": baseline,
                    "action": "ADD",
                    "pre_quantity": 100,
                    "symbol": "000001.SZ",
                    "decision_trade_date": "2026-01-05",
                },
                {
                    "sleeve_id": "s2",
                    "baseline": baseline,
                    "action": "HOLD",
                    "pre_quantity": 200,
                    "symbol": "000002.SZ",
                    "decision_trade_date": "2026-01-05",
                },
            )
        )
    pd.DataFrame(rows).to_parquet(heldout / "heldout_sleeve_days.parquet", index=False)
    return v4, heldout


def test_label_support_diagnostic_binds_cash_training_and_observed_add(tmp_path: Path) -> None:
    v4, heldout = _write_diagnostic_sources(tmp_path)

    diagnostic = open_only.label_support_diagnostic(v4_bundle=v4, heldout_bundle=heldout)

    assert diagnostic["outcomes_read"] is False
    assert diagnostic["entry_training_row_count"] == 2
    assert diagnostic["entry_training_holding_exposure_values"] == (0.0,)
    assert diagnostic["parent_heldout_add_decision_count"] == 1
    assert diagnostic["parent_heldout_add_symbol_count"] == 1
    assert diagnostic["diagnostic_sha256"] == canonical_sha256(
        {key: value for key, value in diagnostic.items() if key != "diagnostic_sha256"}
    )


def test_label_support_diagnostic_rejects_non_cash_entry_support(tmp_path: Path) -> None:
    v4, heldout = _write_diagnostic_sources(tmp_path, supported=False)

    with pytest.raises(ActionValueError, match="OPEN_ONLY_HYPOTHESIS_EVIDENCE_UNAVAILABLE"):
        open_only.label_support_diagnostic(v4_bundle=v4, heldout_bundle=heldout)


def _request(tmp_path: Path) -> dict:
    evaluation = tuple(f"{index:06d}.SZ" for index in range(100, 164))
    prior_payload = {
        "schema_version": "position_timing_prior_action_value_requests_v1",
        "research_root": (tmp_path / "timing" / "research").as_posix(),
        "request_folders": open_only.PRIOR_REQUEST_FOLDERS,
        "request_count": 1,
        "folder_counts": {"action_value_heldout_v1": 1},
        "requests": [{"request_sha256": "1" * 64}],
        "forbidden_symbols": ("000001.SZ",),
        "forbidden_symbol_count": 1,
        "outcomes_read": False,
    }
    prior = {**prior_payload, "aggregate_sha256": canonical_sha256(prior_payload)}
    evidence_payload = {
        "schema_version": "position_timing_action_value_label_support_diagnostic_v1",
        "outcomes_read": False,
        "parent_heldout_add_decision_count": 1,
    }
    evidence = {**evidence_payload, "diagnostic_sha256": canonical_sha256(evidence_payload)}
    payload = {
        "schema_version": open_only.REQUEST_SCHEMA,
        "pipeline_id": open_only.PIPELINE_ID,
        "created_at": "2026-09-10T12:00:00+08:00",
        "repository_root": tmp_path.as_posix(),
        "repository_commit": "a" * 40,
        "timing_root": (tmp_path / "timing").as_posix(),
        "parent_heldout": {
            "bundle_path": (tmp_path / "heldout").as_posix(),
            "manifest_file": {"path": "x", "sha256": "1" * 64, "size_bytes": 1},
            "manifest_sha256": "2" * 64,
            "request_sha256": "3" * 64,
            "receipt_sha256": "4" * 64,
        },
        "parent_entry_only": {
            "bundle_path": (tmp_path / "entry").as_posix(),
            "manifest_file": {"path": "x", "sha256": "8" * 64, "size_bytes": 1},
            "manifest_sha256": "9" * 64,
            "request_sha256": "a" * 64,
            "receipt_sha256": "b" * 64,
        },
        "parent_v4": {
            "bundle_path": (tmp_path / "v4").as_posix(),
            "manifest_file": {"path": "x", "sha256": "c" * 64, "size_bytes": 1},
            "manifest_sha256": "d" * 64,
            "request_sha256": "e" * 64,
            "receipt_sha256": "f" * 64,
            "training_rows_file": {"path": "x", "sha256": "1" * 64, "size_bytes": 1},
            "oof_predictions_file": {"path": "x", "sha256": "2" * 64, "size_bytes": 1},
        },
        "candidate_root": (tmp_path / "candidate").as_posix(),
        "training_symbols": ("000010.SZ",),
        "evaluation_symbols": evaluation,
        "population_spec": {
            "start": "2018-08-01",
            "end": "2026-08-31",
            "seed": 20260907,
            "symbol_limit": 64,
            "selected_symbols": evaluation,
            "selection": "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
            "forbidden_symbol_count": 1,
            "prior_requests_sha256": prior["aggregate_sha256"],
        },
        "prior_request_identity": prior,
        "hypothesis_evidence": evidence,
        "daily_replay_source_identity": {"aggregate_sha256": "5" * 64},
        "corporate_action_snapshot": {"sha256": "6" * 64},
        "suspension_snapshot": {"sha256": "7" * 64},
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "parent_source_sha256": "8" * 64,
        "parent_feature_spec_sha256": "9" * 64,
        "parent_policy_sha256": "0" * 64,
        "candidate_policy_sha256": action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, OPEN_ONLY_MODEL_ACTION_AUTHORITY
        ),
        "study_contract": open_only.STUDY_CONTRACT,
        "study_contract_sha256": open_only.STUDY_CONTRACT_SHA256,
        "result_class": open_only.RESULT_CLASS,
        "registry_write": False,
        "current_write": False,
        "model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
    }
    payload["request_sha256"] = canonical_sha256(payload)
    return payload


def test_open_only_request_rejects_overlap_and_side_effects(tmp_path: Path) -> None:
    request = _request(tmp_path)
    path = tmp_path / "request.json"
    path.write_bytes(canonical_json_bytes(request))

    assert open_only._load_request(path)["request_sha256"] == request["request_sha256"]

    request["evaluation_symbols"] = (request["training_symbols"][0], *request["evaluation_symbols"][1:])
    request["population_spec"]["selected_symbols"] = request["evaluation_symbols"]
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ActionValueError, match="OPEN_ONLY_REQUEST_IDENTITY_MISMATCH"):
        open_only._load_request(path)

    request = _request(tmp_path)
    request["runtime_write"] = True
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ActionValueError, match="OPEN_ONLY_REQUEST_IDENTITY_MISMATCH"):
        open_only._load_request(path)


def test_open_only_receipt_joint_classification_is_conservative() -> None:
    assert open_only._joint_evidence(
        {
            "BUY_AND_HOLD": {"effect_evidence": "SUPPORTED"},
            "FROZEN_L1_V1": {"effect_evidence": "SUPPORTED"},
        }
    ) == "SUPPORTED"
    assert open_only._joint_evidence(
        {
            "BUY_AND_HOLD": {"effect_evidence": "SUPPORTED"},
            "FROZEN_L1_V1": {"effect_evidence": "INCONCLUSIVE"},
        }
    ) == "INCONCLUSIVE"
    assert open_only._joint_evidence(
        {
            "BUY_AND_HOLD": {"effect_evidence": "NEGATIVE"},
            "FROZEN_L1_V1": {"effect_evidence": "SUPPORTED"},
        }
    ) == "NEGATIVE"
