from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    CORE_INFORMATION_BLOCK,
    POLICY_SHA256,
    ActionValueError,
    PositionState,
    cutoff_on,
)
from backend.services.position_timing.action_value_advice import (
    ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    FULL_MODEL_ACTION_AUTHORITY,
    action_authority_policy_sha256,
    decide_stock_day,
)
from backend.services.position_timing.action_value_model import HEADS
from backend.services.position_timing import action_value_entry_only as entry_only
from backend.services.position_timing.contracts import canonical_sha256


class FixedModel:
    metadata = {"model_sha256": "1" * 64}

    def __init__(self, *, entry: float, exit: float) -> None:
        self.values = {HEADS[0]: entry, HEADS[1]: exit}
        self.objectives: list[str] = []

    def predict(self, frame, objectives, *, decision_as_of):
        self.objectives.extend(objectives)
        return np.asarray([self.values[objective] for objective in objectives])


def _inputs(*, entry_cost: Decimal | None = None) -> dict:
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
            1000,
            1000,
            Decimal("90000"),
            Decimal("100000"),
            entry_cost,
            20 if entry_cost is not None else None,
        ),
        "bars": bars,
        "benchmark": pd.Series(3000.0, index=index),
        "decision_as_of": cutoff_on(index[-1].date()),
    }


def test_entry_only_removes_model_exit_and_holds_existing_position() -> None:
    model = FixedModel(entry=-1.0, exit=100.0)

    decision = decide_stock_day(
        **_inputs(),
        model=model,
        model_action_authority=ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    )

    assert decision.action == "HOLD"
    assert decision.plan.delta == 0
    assert model.objectives
    assert set(model.objectives) == {HEADS[0]}
    assert {candidate["action"] for candidate in decision.candidates} == {"HOLD", "ADD"}
    assert "MODEL_EXIT_AUTHORITY_REMOVED" in decision.reason_codes


def test_entry_only_can_add_when_entry_head_is_positive() -> None:
    decision = decide_stock_day(
        **_inputs(),
        model=FixedModel(entry=20.0, exit=100.0),
        model_action_authority=ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    )

    assert decision.action == "ADD"
    assert decision.plan.delta > 0


def test_frozen_risk_exit_still_precedes_entry_only_model() -> None:
    decision = decide_stock_day(
        **_inputs(entry_cost=Decimal("11")),
        model=None,
        model_action_authority=ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    )

    assert decision.action == "EXIT"
    assert decision.plan.risk_exit is True
    assert decision.authority == "FROZEN_RULE_RISK_OVERRIDE"


def test_default_authority_and_policy_identity_are_unchanged() -> None:
    model = FixedModel(entry=2.0, exit=5.0)
    decision = decide_stock_day(**_inputs(), model=model)

    assert decision.action == "EXIT"
    assert set(model.objectives) == set(HEADS)
    assert (
        action_authority_policy_sha256(CORE_INFORMATION_BLOCK, FULL_MODEL_ACTION_AUTHORITY)
        == POLICY_SHA256
    )
    assert (
        action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, ENTRY_ONLY_MODEL_ACTION_AUTHORITY
        )
        != POLICY_SHA256
    )


def test_unknown_model_authority_fails_closed() -> None:
    with pytest.raises(ActionValueError, match="MODEL_ACTION_AUTHORITY_UNSUPPORTED"):
        decide_stock_day(**_inputs(), model=FixedModel(entry=1.0, exit=1.0), model_action_authority="OTHER")


def _sleeves(*, changed: bool = False) -> pd.DataFrame:
    rows = []
    for baseline in ("BUY_AND_HOLD", "FROZEN_L1_V1"):
        for index, day in enumerate(("2026-01-05", "2026-01-06")):
            rows.append(
                {
                    "sleeve_id": "s1",
                    "symbol": "000001.SZ",
                    "initial_state": "HOLDING_START",
                    "continuous_start": "2026-01-02",
                    "decision_trade_date": "2026-01-02" if index == 0 else "2026-01-05",
                    "valuation_date": day,
                    "target_trade_date": day,
                    "baseline": baseline,
                    "policy_wealth_cny": 100010.0 + index + (3.0 if changed else 0.0),
                    "baseline_wealth_cny": 100000.0 + index,
                    "incremental_net_value_cny": 10.0 + (3.0 if changed else 0.0),
                    "action": "HOLD" if changed else "EXIT",
                    "decision_authority": "LOCAL_MODEL_ESTIMATE",
                    "decision_input_status": "AVAILABLE",
                    "decision_reason": "AVAILABLE",
                    "fill_status": "NO_ACTION" if changed else "FILLED",
                    "fill_reason": "NO_ACTION" if changed else "FROZEN_GUARD_ACCEPTED",
                    "fill_price_raw": None if changed else 10.0,
                    "planned_delta_qty": 0 if changed else -100,
                    "plan_reference_raw": 10.0,
                    "plan_risk_exit": False,
                    "pre_quantity": 100,
                    "pre_sellable_qty": 100,
                    "model_sha256": "1" * 64,
                    "corporate_action_applied": False,
                    "corporate_action_source_rows_sha256": None,
                    "policy_fractional_share_discarded": 0.0,
                    "baseline_fractional_share_discarded": 0.0,
                }
            )
    return pd.DataFrame(rows)


def _request(tmp_path: Path) -> dict:
    payload = {
        "schema_version": entry_only.REQUEST_SCHEMA,
        "pipeline_id": entry_only.PIPELINE_ID,
        "created_at": "2026-09-09T12:00:00+08:00",
        "repository_root": tmp_path.as_posix(),
        "repository_commit": "a" * 40,
        "timing_root": (tmp_path / "timing").as_posix(),
        "parent_v4": {},
        "study_contract": entry_only.STUDY_CONTRACT,
        "study_contract_sha256": entry_only.STUDY_CONTRACT_SHA256,
        "same_history_interpretation": entry_only.RESULT_CLASS,
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


def _receipt(request: dict) -> dict:
    comparison = {"effect_evidence": "INCONCLUSIVE"}
    payload = {
        "schema_version": entry_only.RECEIPT_SCHEMA,
        "pipeline_id": entry_only.PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "result_class": entry_only.RESULT_CLASS,
        "provenance_reason": entry_only.PROVENANCE_REASON,
        "trial_count": 2,
        "selected_trial_count": 0,
        "entry_only_policy": {
            "comparisons": {
                "BUY_AND_HOLD": comparison,
                "FROZEN_L1_V1": comparison,
            }
        },
        "serving_status": "NOT_SERVING_SAME_HISTORY_HYPOTHESIS_GENERATED",
    }
    payload["receipt_sha256"] = canonical_sha256(payload)
    return payload


def test_candidate_and_full_controls_require_exact_path_identity() -> None:
    full = _sleeves()
    candidate = _sleeves(changed=True)
    candidate["decision_input_status"] = "UNAVAILABLE"
    candidate["decision_reason"] = "POLICY_STATE_DEPENDENT_TEST"

    identity = entry_only._assert_same_path_identity(candidate, full)
    daily, diagnostic = entry_only._candidate_minus_full_daily(candidate, full)

    assert identity["exact_match"] is True
    assert diagnostic["interpretation"] == "DIAGNOSTIC_ONLY_NOT_A_TRIAL"
    assert diagnostic["interval"] is None
    assert diagnostic["action_changed_count"] == 2
    assert len(daily) == 2
    broken = candidate.iloc[:-1]
    with pytest.raises(ActionValueError, match="PATH_IDENTITY_MISMATCH"):
        entry_only._assert_same_path_identity(broken, full)


def test_bundle_is_immutable_inspectable_and_exact_retry_is_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(tmp_path)
    receipt = _receipt(request)
    bundle = (
        Path(request["timing_root"])
        / "research"
        / entry_only.ARTIFACT_FOLDER
        / "bundles"
        / request["request_sha256"]
    )
    sleeves = _sleeves()
    daily = pd.DataFrame(
        {"valuation_date": ["2026-01-05"], "incremental_net_value_bps": [1.0]}
    )
    entry_only._publish_bundle(
        bundle,
        request=request,
        receipt=receipt,
        oof=pd.DataFrame({"x": [1]}),
        candidate_sleeves=sleeves,
        full_sleeves=sleeves,
        candidate_daily=daily,
        diagnostic_daily=daily,
    )
    manifest_before = (bundle / "manifest.json").read_bytes()
    assert entry_only.inspect_entry_only_bundle(bundle)["receipt"] == receipt

    monkeypatch.setattr(entry_only, "_clean_repository_commit", lambda _: "a" * 40)
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    result = entry_only.run_entry_only_request(request_path)

    assert result["status"] == "ALREADY_MATERIALIZED"
    assert (bundle / "manifest.json").read_bytes() == manifest_before


def test_bundle_corruption_and_request_write_flag_fail_closed(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request_path = tmp_path / "request.json"
    request["database_write"] = True
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    request_path.write_text(json.dumps(request), encoding="utf-8")

    with pytest.raises(ActionValueError, match="REQUEST_IDENTITY_MISMATCH"):
        entry_only._load_request(request_path)
