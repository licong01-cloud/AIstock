from __future__ import annotations

from datetime import date
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    CORE_INFORMATION_BLOCK,
    FEATURE_ORDER,
    ActionValueError,
    PositionState,
    cutoff_on,
)
from backend.services.position_timing.action_value_add_model import (
    ADD_MODEL_SCHEMA,
    ADD_OBJECTIVE,
    add_training_rows_asof,
    fit_add_model,
)
from backend.services.position_timing.action_value_advice import (
    STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY,
    decide_stock_day,
)
from backend.services.position_timing.action_value_model import HEADS, MODEL_SCHEMA
from backend.services.position_timing.action_value_research import (
    ActionValuePopulationSpec,
    build_state_matched_add_rows,
    replay_continuous_cohorts,
)
from backend.services.position_timing import action_value_state_matched_add as study
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256


class RecordingEntryModel:
    def __init__(self, *, available_at=None, value: float = 10.0) -> None:
        self.metadata = {
            "model_sha256": "1" * 64,
            "available_at": (available_at or cutoff_on(date(2024, 1, 2))).isoformat(),
            "information_block": CORE_INFORMATION_BLOCK,
        }
        self.value = value
        self.objectives: list[str] = []

    def predict(self, frame, objectives, *, decision_as_of):
        self.objectives.extend(objectives)
        return np.full(len(objectives), self.value)


class RecordingAddModel:
    metadata = {
        "model_sha256": "2" * 64,
        "available_at": cutoff_on(date(2024, 1, 2)).isoformat(),
        "information_block": CORE_INFORMATION_BLOCK,
    }

    def __init__(self, value: float = 10.0) -> None:
        self.value = value
        self.calls = 0

    def predict(self, frame, *, decision_as_of):
        self.calls += len(frame)
        return np.full(len(frame), self.value)


def _decision_inputs(*, quantity: int) -> dict:
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
            Decimal(10) if quantity else None,
            20 if quantity else None,
        ),
        "bars": bars,
        "benchmark": pd.Series(3000.0, index=index),
        "decision_as_of": cutoff_on(index[-1].date()),
    }


def test_state_matched_policy_routes_cash_to_entry_and_holding_to_add() -> None:
    entry = RecordingEntryModel()
    add = RecordingAddModel()

    opened = decide_stock_day(
        **_decision_inputs(quantity=0),
        model=entry,
        add_model=add,
        model_action_authority=STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY,
    )
    assert opened.action == "OPEN"
    assert set(entry.objectives) == {HEADS[0]}
    assert add.calls == 0

    entry.objectives.clear()
    added = decide_stock_day(
        **_decision_inputs(quantity=2500),
        model=entry,
        add_model=add,
        model_action_authority=STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY,
    )
    assert added.action == "ADD"
    assert entry.objectives == []
    assert add.calls > 0
    assert {
        item["objective"] for item in added.candidates if item["action"] == "ADD"
    } == {ADD_OBJECTIVE}


def test_state_matched_policy_keeps_frozen_risk_exit_priority() -> None:
    args = _decision_inputs(quantity=2500)
    args["state"] = PositionState(
        2500,
        2500,
        Decimal("75000"),
        Decimal("100000"),
        Decimal("11"),
        20,
    )

    decision = decide_stock_day(
        **args,
        model=None,
        add_model=None,
        model_action_authority=STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY,
    )

    assert decision.action == "EXIT"
    assert decision.authority == "FROZEN_RULE_RISK_OVERRIDE"
    assert decision.plan.risk_exit is True


def test_legacy_two_head_contract_is_not_extended() -> None:
    assert MODEL_SCHEMA == "position_timing_local_model_v2"
    assert HEADS == ("ENTRY_ACTION_VALUE_V2", "EXIT_ACTION_VALUE_V2")
    assert ADD_OBJECTIVE not in HEADS


def _add_training_fixture(rows: int = 500) -> pd.DataFrame:
    rng = np.random.default_rng(23)
    frame = pd.DataFrame(rng.normal(size=(rows, len(FEATURE_ORDER))), columns=FEATURE_ORDER)
    frame["holding_exposure"] = 0.25
    frame["cash_fraction"] = 0.75
    frame["action_fraction"] = 0.25
    frame["holding_age"] = 20.0
    frame["holding_age_missing"] = 0.0
    frame["unrealized_return_bps"] = 50.0
    frame["entry_cost_missing"] = 0.0
    frame["objective"] = ADD_OBJECTIVE
    frame["planned_delta_qty"] = 100
    frame["baseline_action"] = "HOLD"
    frame["net_action_value_bps"] = frame["return_20d_bps"] * 0.5
    frame["decision_as_of"] = cutoff_on(date(2024, 1, 2))
    frame["label_available_at"] = cutoff_on(date(2024, 2, 2))
    return frame


def test_add_training_contract_is_single_head_held_state_and_pit() -> None:
    rows = _add_training_fixture()
    rows.loc[0, "label_available_at"] = cutoff_on(date(2025, 1, 1))

    eligible = add_training_rows_asof(
        rows,
        cutoff_on(date(2024, 3, 29)),
        feature_order=FEATURE_ORDER,
    )

    assert len(eligible) == len(rows) - 1
    broken = rows.copy()
    broken.loc[1, "baseline_action"] = "OPEN"
    with pytest.raises(ActionValueError, match="STATE_SUPPORT_INVALID"):
        add_training_rows_asof(
            broken,
            cutoff_on(date(2024, 3, 29)),
            feature_order=FEATURE_ORDER,
        )


def test_add_model_uses_separate_identity_and_frozen_estimator() -> None:
    rows = _add_training_fixture()
    model = fit_add_model(
        rows,
        cutoff=cutoff_on(date(2024, 3, 29)),
        available_at=cutoff_on(date(2024, 4, 1)),
        source_sha256="1" * 64,
        request_sha256="2" * 64,
        source_commit="a" * 40,
    )

    assert model.metadata["schema_version"] == ADD_MODEL_SCHEMA
    assert model.metadata["objective"] == ADD_OBJECTIVE
    assert model.metadata["package_version"] == "4.6.0"
    values = model.predict(
        rows.loc[:4, FEATURE_ORDER],
        decision_as_of=cutoff_on(date(2024, 4, 2)),
    )
    assert values.shape == (5,)
    assert np.isfinite(values).all()
    assert model.predict(
        rows.loc[0:0, FEATURE_ORDER].iloc[0:0],
        decision_as_of=cutoff_on(date(2024, 4, 2)),
    ).shape == (0,)


class FakeCandidate:
    def __init__(self, sessions: int = 140) -> None:
        self.calendar = pd.bdate_range("2023-01-02", periods=sessions)
        self.symbols = ("000001.SZ",)
        self._frames = {
            "000300.SH": self._frame("000300.SH"),
            "000001.SZ": self._frame("000001.SZ"),
        }

    def _frame(self, symbol: str) -> pd.DataFrame:
        close = np.linspace(10.0, 12.0, len(self.calendar))
        if symbol == "000300.SH":
            close = np.linspace(3000.0, 3200.0, len(self.calendar))
        frame = pd.DataFrame(index=self.calendar)
        frame["open"] = close
        frame["high"] = close * 1.01
        frame["low"] = close * 0.99
        frame["close"] = close
        frame["volume"] = 100000.0
        frame["factor"] = 1.0
        frame["up_limit"] = close * 1.1
        frame["down_limit"] = close * 0.9
        frame["is_suspended"] = False
        frame["pit_active"] = symbol != "000300.SH"
        return frame

    def bars(self, symbol: str) -> pd.DataFrame:
        return self._frames[symbol].copy()


def test_add_labels_come_from_real_open_only_held_states() -> None:
    candidate = FakeCandidate()
    model = RecordingEntryModel(available_at=cutoff_on(candidate.calendar[35].date()))
    spec = ActionValuePopulationSpec(
        start=candidate.calendar[35].date(),
        end=candidate.calendar[-1].date(),
        symbol_limit=1,
        review_stride=10,
    )

    result = build_state_matched_add_rows(
        candidate,
        spec,
        entry_models=(model,),
        symbols=candidate.symbols,
    )

    assert result.coverage["state_selection_outcomes_read"] is False
    assert result.coverage["baseline_action"] == "HOLD"
    assert result.coverage["counts"]["positive_add_candidate_states"] > 0
    assert result.rows["objective"].eq(ADD_OBJECTIVE).all()
    assert result.rows["planned_delta_qty"].gt(0).all()
    assert result.rows["holding_exposure"].gt(0).all()
    assert result.rows["baseline_action"].eq("HOLD").all()
    assert result.rows["state_path_action"].eq("HOLD").all()
    assert result.rows["label_available_at"].gt(result.rows["decision_as_of"]).all()


def test_composite_replay_uses_add_head_only_after_position_exists() -> None:
    candidate = FakeCandidate()
    available = cutoff_on(candidate.calendar[35].date())
    entry = RecordingEntryModel(available_at=available)
    add = RecordingAddModel()
    add.metadata = {**add.metadata, "available_at": available.isoformat()}

    replay = replay_continuous_cohorts(
        candidate,
        models=(entry,),
        add_models=(add,),
        symbols=candidate.symbols,
        bootstrap_samples=20,
        model_action_authority=STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY,
    )

    assert replay.receipt["schema_version"] == (
        "position_timing_state_matched_add_continuous_policy_receipt_v1"
    )
    assert "ADD" in set(replay.sleeve_days["action"])
    assert add.calls > 0
    assert set(entry.objectives) == {HEADS[0]}


def _request(tmp_path) -> dict:
    evaluation = tuple(f"{index:06d}.SZ" for index in range(100, 164))
    training = ("000001.SZ",)
    prior_payload = {
        "schema_version": "position_timing_prior_action_value_requests_v1",
        "research_root": (tmp_path / "timing" / "research").as_posix(),
        "request_folders": study.PRIOR_REQUEST_FOLDERS,
        "request_count": 1,
        "folder_counts": {"action_value_open_only_v1": 1},
        "requests": [{"request_sha256": "1" * 64}],
        "forbidden_symbols": ("000002.SZ",),
        "forbidden_symbol_count": 1,
        "outcomes_read": False,
    }
    prior = {**prior_payload, "aggregate_sha256": canonical_sha256(prior_payload)}
    parent = {
        "bundle_path": (tmp_path / "parent").as_posix(),
        "manifest_file": {"path": "x", "sha256": "1" * 64, "size_bytes": 1},
        "manifest_sha256": "2" * 64,
        "request_sha256": "3" * 64,
        "receipt_sha256": "4" * 64,
    }
    payload = {
        "schema_version": study.REQUEST_SCHEMA,
        "pipeline_id": study.PIPELINE_ID,
        "created_at": "2026-09-11T12:00:00+08:00",
        "repository_root": tmp_path.as_posix(),
        "repository_commit": "a" * 40,
        "timing_root": (tmp_path / "timing").as_posix(),
        "parent_open_only": parent,
        "parent_entry_only": parent,
        "parent_v4": {
            **parent,
            "training_rows_file": {"path": "x", "sha256": "5" * 64, "size_bytes": 1},
            "oof_predictions_file": {"path": "x", "sha256": "6" * 64, "size_bytes": 1},
        },
        "candidate_root": (tmp_path / "candidate").as_posix(),
        "training_symbols": training,
        "evaluation_symbols": evaluation,
        "snapshot_symbols": tuple(sorted((*training, *evaluation))),
        "population_spec": {
            "start": "2018-08-01",
            "end": "2026-08-31",
            "review_stride": 10,
            "seed": 20260907,
            "symbol_limit": 64,
            "selected_symbols": evaluation,
            "selection": "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
            "forbidden_symbol_count": 1,
            "prior_requests_sha256": prior["aggregate_sha256"],
        },
        "prior_request_identity": prior,
        "training_daily_source_identity": {"aggregate_sha256": "7" * 64},
        "evaluation_daily_source_identity": {"aggregate_sha256": "8" * 64},
        "corporate_action_snapshot": {"sha256": "9" * 64},
        "suspension_snapshot": {"sha256": "0" * 64},
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "parent_source_sha256": "a" * 64,
        "parent_feature_spec_sha256": "b" * 64,
        "parent_policy_sha256": "c" * 64,
        "candidate_policy_sha256": study.action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY
        ),
        "add_label_contract": study.ADD_LABEL_CONTRACT,
        "add_label_contract_sha256": study.ADD_LABEL_CONTRACT_SHA256,
        "study_contract": study.STUDY_CONTRACT,
        "study_contract_sha256": study.STUDY_CONTRACT_SHA256,
        "result_class": study.RESULT_CLASS,
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


def test_request_contract_rejects_overlap_and_side_effects(tmp_path) -> None:
    request = _request(tmp_path)
    path = tmp_path / "request.json"
    path.write_bytes(canonical_json_bytes(request))
    assert study._load_request(path)["request_sha256"] == request["request_sha256"]

    request["runtime_write"] = True
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ActionValueError, match="REQUEST_IDENTITY_MISMATCH"):
        study._load_request(path)

    request = _request(tmp_path)
    request["evaluation_symbols"] = (
        request["training_symbols"][0],
        *request["evaluation_symbols"][1:],
    )
    request["population_spec"]["selected_symbols"] = request["evaluation_symbols"]
    request["snapshot_symbols"] = tuple(
        sorted(set(request["training_symbols"]).union(request["evaluation_symbols"]))
    )
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ActionValueError, match="REQUEST_IDENTITY_MISMATCH"):
        study._load_request(path)


def test_three_comparison_interval_and_cli_failure_are_typed(
    tmp_path, monkeypatch, capsys
) -> None:
    comparison = study._comparison(np.ones(80), seed=7)
    assert comparison["simultaneous_interval_level"] == pytest.approx(1 - 0.05 / 3)
    assert comparison["effect_evidence"] == "SUPPORTED"

    monkeypatch.setattr(
        study,
        "run_state_matched_add_request",
        lambda _: (_ for _ in ()).throw(
            ActionValueError("EXPECTED_FAILURE", detail="bounded")
        ),
    )
    assert study.main(["run", "--request", str(tmp_path / "missing.json")]) == 2
    assert capsys.readouterr().out.strip() == (
        '{"status": "FAILED", "error_code": "EXPECTED_FAILURE", '
        '"details": {"detail": "bounded"}}'
    )


def test_prepare_source_snapshot_failure_is_typed(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.services.position_timing.action_value_pipeline._clean_repository_commit",
        lambda _: "a" * 40,
    )
    parent = {
        "request": {
            "candidate_root": (tmp_path / "candidate").as_posix(),
        }
    }
    monkeypatch.setattr(study, "inspect_open_only_bundle", lambda _: parent)
    monkeypatch.setattr(study, "_validate_parent_open_only", lambda _: None)
    monkeypatch.setattr(
        study,
        "prior_request_identity",
        lambda *_args, **_kwargs: {
            "forbidden_symbols": frozenset({"000002.SZ"}),
        },
    )
    monkeypatch.setattr(
        study.DailyCandidate,
        "open",
        lambda _: type("Candidate", (), {"symbols": ("000001.SZ", "000002.SZ")})(),
    )
    monkeypatch.setattr(
        study,
        "select_heldout_symbols",
        lambda *_args, **_kwargs: ("000002.SZ",),
    )
    parent["request"].update(
        {
            "population_spec": {
                "start": "2024-01-01",
                "end": "2024-12-31",
                "seed": 7,
            },
            "training_symbols": ("000001.SZ",),
            "evaluation_symbols": ("000002.SZ",),
        }
    )
    monkeypatch.setattr(
        study,
        "_freeze_source_snapshots",
        lambda **_kwargs: (_ for _ in ()).throw(ConnectionError("not connected")),
    )

    with pytest.raises(
        ActionValueError, match="STATE_MATCHED_ADD_SOURCE_SNAPSHOT_UNAVAILABLE"
    ) as exc_info:
        study.prepare_state_matched_add_request(
            timing_root=tmp_path / "timing",
            repository_root=tmp_path,
            parent_open_only_bundle=tmp_path / "parent",
        )

    assert exc_info.value.details == {"exception_type": "ConnectionError"}
