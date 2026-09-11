from collections import Counter
from decimal import Decimal
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionPlan, ActionValueError, PositionState, cutoff_on
from backend.services.position_timing.action_value_corporate_actions import CorporateAction, CorporateActionBook
from backend.services.position_timing.pattern_research import (
    BUNDLE_SCHEMA,
    CORPORATE_ACTION_APPLICATION_POLICY,
    CORPORATE_ACTION_APPLICATION_POLICY_SHA256,
    FACTOR_ACTION_COVERAGE_POLICY,
    FACTOR_ACTION_COVERAGE_POLICY_SHA256,
    LEGACY_REQUEST_SCHEMA,
    PIPELINE_ID,
    PREREGISTERED_FAMILY_COUNT,
    PROTOTYPE_CONTRACT,
    PROTOTYPE_CONTRACT_SHA256,
    PROTOTYPE_FAMILY_SIZE,
    RECEIPT_SCHEMA,
    REQUEST_SCHEMA,
    RESULT_CLASS,
    TOTAL_FORMAL_COMPARISON_COUNT,
    PrototypeReplayResult,
    apply_pattern_corporate_action_policy,
    audit_pattern_factor_action_coverage,
    evaluate_entry_and_exit_mechanisms,
    _execute_next_session,
    _effect_evidence,
    _manifest,
    _load_request,
    _model_contract,
    _model_contract_sha256,
    _optimizer_contract,
    _optimizer_contract_sha256,
    _publish_bundle,
    _snapshot_scope,
    inspect_pattern_bundle,
    mean_interval,
    prior_timing_request_population,
    replay_prototype,
    replay_full_policy_symbol,
    select_pattern_evaluation_symbols,
    sparse_event_interval,
)
from backend.services.position_timing.pattern_strategy import pattern_feature_frame


def test_root_manifest_binds_nested_model_manifests(tmp_path: Path):
    (tmp_path / "manifest.json").write_text("root", encoding="utf-8")
    nested = tmp_path / "models" / "CORE_ONLY" / ("a" * 64) / "manifest.json"
    nested.parent.mkdir(parents=True)
    nested.write_text("model", encoding="utf-8")

    manifest = _manifest(
        tmp_path,
        {"request_sha256": "b" * 64, "receipt_sha256": "c" * 64},
    )

    assert "manifest.json" not in manifest["files"]
    assert nested.relative_to(tmp_path).as_posix() in manifest["files"]


def _bars(periods: int = 90) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    close = np.full(periods, 10.0)
    return pd.DataFrame(
        {
            "open": close,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": 1_000.0,
            "factor": 1.0,
            "up_limit": close * 1.1,
            "down_limit": close * 0.9,
            "is_suspended": False,
            "pit_active": True,
        },
        index=dates,
    )


def _forced_breakout_features(bars: pd.DataFrame, breakout: int, *, confirm: int | None) -> pd.DataFrame:
    features = pattern_feature_frame(bars, symbol="000001.SZ")
    features.loc[:, "sma5"] = 10.0
    features.loc[:, "sma10"] = 10.0
    features.loc[:, "atr14"] = 1.0
    features.loc[:, "low20_prior"] = 8.0
    features.loc[:, "low20_age"] = 5.0
    features.loc[:, "acceleration_atr"] = 0.0
    features.loc[:, "distance_ma10_atr"] = 0.0
    features.loc[:, "volume_ratio"] = 1.0
    features.loc[:, "close_position"] = 0.5
    features.loc[:, "upper_wick_fraction"] = 0.0
    features.iloc[breakout - 1, features.columns.get_loc("adjusted_close")] = 9.9
    features.iloc[breakout, features.columns.get_loc("adjusted_close")] = 10.2
    if confirm is not None:
        features.iloc[confirm - 1, features.columns.get_loc("sma5")] = 10.0
        features.iloc[confirm - 1, features.columns.get_loc("sma10")] = 10.0
        features.iloc[confirm, features.columns.get_loc("adjusted_low")] = 10.0
        features.iloc[confirm, features.columns.get_loc("adjusted_close")] = 10.4
        features.iloc[confirm, features.columns.get_loc("sma5")] = 10.2
        features.iloc[confirm, features.columns.get_loc("sma10")] = 10.1
    else:
        features.iloc[breakout + 1 : breakout + 6, features.columns.get_loc("adjusted_low")] = 11.0
    return features


def test_population_selection_uses_fixed_pipe_hash_and_exclusion():
    symbols = [f"{index:06d}.SZ" for index in range(100)]
    first = select_pattern_evaluation_symbols(symbols, forbidden_symbols=symbols[:10], limit=8)
    second = select_pattern_evaluation_symbols(tuple(reversed(symbols)), forbidden_symbols=symbols[:10], limit=8)
    assert first == second
    assert not set(first).intersection(symbols[:10])


def _stock_action(
    effective: date,
    *,
    multiplier: str,
    cash: str = "0",
    source_economic_action_count: int = 1,
) -> CorporateAction:
    return CorporateAction(
        symbol="000001.SZ",
        effective_trade_date=effective,
        quantity_multiplier=Decimal(multiplier),
        cashflow_yuan_per_share=Decimal(cash),
        reference_price_cash_yuan_per_share=Decimal(cash),
        cash_pay_date=None,
        share_listing_date=effective,
        source_available_at=cutoff_on(effective - timedelta(days=1)),
        source_row_count=1,
        source_rows_sha256="a" * 64,
        source_economic_action_count=source_economic_action_count,
    )


def test_pattern_corporate_action_policy_ignores_audited_non_pro_rata_action():
    from backend.services.position_timing.contracts import canonical_sha256

    bars = _bars(8)
    effective = bars.index[4].date()
    bars.iloc[3, bars.columns.get_loc("factor")] = np.nan
    action = _stock_action(
        effective,
        multiplier="3.10",
        source_economic_action_count=2,
    )
    source = CorporateActionBook((action,), canonical_sha256({"source": "special"}))

    class Candidate:
        def bars(self, symbol: str) -> pd.DataFrame:
            assert symbol == "000001.SZ"
            return bars

    normalized, audit = apply_pattern_corporate_action_policy(
        Candidate(),
        symbols=("000001.SZ",),
        corporate_actions=source,
        start=bars.index[0].date(),
        end=bars.index[-1].date(),
        candidate_source_sha256="b" * 64,
    )

    assert normalized.actions == ()
    assert normalized.snapshot_sha256 == audit["application_sha256"]
    assert audit["policy_sha256"] == CORPORATE_ACTION_APPLICATION_POLICY_SHA256
    assert audit["ignored_non_pro_rata_action_count"] == 1
    assert audit["normalized_non_pro_rata_actions"][0][
        "source_economic_action_count"
    ] == 2
    assert audit["normalized_non_pro_rata_actions"][0]["classification"] == (
        "NON_PRO_RATA_STOCK_ACTION_IGNORED"
    )
    assert audit["normalized_non_pro_rata_actions"][0]["previous_factor_date"] == (
        bars.index[2].date().isoformat()
    )


def test_pattern_corporate_action_policy_factor_maps_partial_holder_distribution():
    from backend.services.position_timing.contracts import canonical_sha256

    bars = _bars(8)
    effective = bars.index[4].date()
    bars.iloc[4:, bars.columns.get_loc("factor")] = 1.075
    action = _stock_action(effective, multiplier="1.12")
    source = CorporateActionBook((action,), canonical_sha256({"source": "partial-holder"}))

    class Candidate:
        def bars(self, symbol: str) -> pd.DataFrame:
            assert symbol == "000001.SZ"
            return bars

    normalized, audit = apply_pattern_corporate_action_policy(
        Candidate(),
        symbols=("000001.SZ",),
        corporate_actions=source,
        start=bars.index[0].date(),
        end=bars.index[-1].date(),
        candidate_source_sha256="b" * 64,
    )

    assert normalized.actions[0].quantity_multiplier == Decimal("1.075")
    assert audit["factor_mapped_non_pro_rata_action_count"] == 1
    assert audit["ignored_non_pro_rata_action_count"] == 0
    assert audit["normalized_non_pro_rata_actions"][0]["classification"] == (
        "NON_PRO_RATA_STOCK_ACTION_FACTOR_MAPPED"
    )
    assert audit["normalized_non_pro_rata_actions"][0][
        "applied_quantity_multiplier"
    ] == "1.075"


def test_pattern_corporate_action_policy_fails_closed_for_ambiguous_factor_mismatch():
    from backend.services.position_timing.contracts import canonical_sha256

    bars = _bars(8)
    action = _stock_action(bars.index[4].date(), multiplier="1.4", cash="0.5")
    source = CorporateActionBook((action,), canonical_sha256({"source": "ambiguous"}))

    class Candidate:
        def bars(self, symbol: str) -> pd.DataFrame:
            assert symbol == "000001.SZ"
            return bars

    with pytest.raises(ActionValueError, match="PATTERN_CORPORATE_ACTION_FACTOR_MISMATCH"):
        apply_pattern_corporate_action_policy(
            Candidate(),
            symbols=("000001.SZ",),
            corporate_actions=source,
            start=bars.index[0].date(),
            end=bars.index[-1].date(),
            candidate_source_sha256="b" * 64,
        )


def test_factor_action_coverage_audit_binds_material_transition():
    from backend.services.position_timing.contracts import canonical_sha256

    bars = _bars(8)
    effective = bars.index[4].date()
    bars.iloc[4:, bars.columns.get_loc("factor")] = 1.2
    action = _stock_action(effective, multiplier="1.2")
    actions = CorporateActionBook((action,), canonical_sha256({"source": "bound"}))

    class Candidate:
        def bars(self, symbol: str) -> pd.DataFrame:
            assert symbol == "000001.SZ"
            return bars

    audit = audit_pattern_factor_action_coverage(
        Candidate(),
        symbols=("000001.SZ",),
        corporate_actions=actions,
        start=bars.index[0].date(),
        end=bars.index[-1].date(),
        candidate_source_sha256="b" * 64,
    )

    assert audit["policy_sha256"] == FACTOR_ACTION_COVERAGE_POLICY_SHA256
    assert audit["material_factor_change_count"] == 1
    assert audit["bound_material_factor_change_count"] == 1
    assert audit["unbound_material_factor_change_count"] == 0
    assert audit["coverage_complete"] is True
    assert audit["outcomes_read"] is False


def test_factor_action_coverage_audit_exposes_unbound_source_coordinates():
    bars = _bars(8)
    bars.iloc[4:, bars.columns.get_loc("factor")] = 1.2

    class Candidate:
        def bars(self, symbol: str) -> pd.DataFrame:
            assert symbol == "000001.SZ"
            return bars

    audit = audit_pattern_factor_action_coverage(
        Candidate(),
        symbols=("000001.SZ",),
        corporate_actions=CorporateActionBook.empty(),
        start=bars.index[0].date(),
        end=bars.index[-1].date(),
        candidate_source_sha256="b" * 64,
    )

    assert audit["coverage_complete"] is False
    assert audit["unbound_material_factor_change_count"] == 1
    assert audit["unbound_material_factor_changes"] == [
        {
            "symbol": "000001.SZ",
            "previous_factor_date": bars.index[3].date().isoformat(),
            "current_factor_date": bars.index[4].date().isoformat(),
            "previous_factor": "1.0",
            "current_factor": "1.2",
            "absolute_change_bps": "2000.0",
        }
    ]


def test_request_v2_cannot_drop_factor_action_coverage_contract(tmp_path: Path):
    from backend.services.position_timing.contracts import canonical_sha256

    request = {
        "schema_version": LEGACY_REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "prototype_contract": PROTOTYPE_CONTRACT,
        "prototype_contract_sha256": PROTOTYPE_CONTRACT_SHA256,
        "optimizer_contract": _optimizer_contract(),
        "optimizer_contract_sha256": _optimizer_contract_sha256(),
        "model_contract": _model_contract(),
        "model_contract_sha256": _model_contract_sha256(),
        "preregistered_family_count": PREREGISTERED_FAMILY_COUNT,
        "total_formal_comparison_count": TOTAL_FORMAL_COMPARISON_COUNT,
        "result_class": RESULT_CLASS,
        "research_model_outputs_write": True,
        "registry_write": False,
        "current_write": False,
        "serving_model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
        "corporate_action_snapshot_sha256": "a" * 64,
        "suspension_snapshot_sha256": "b" * 64,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    assert _load_request(path)["schema_version"] == LEGACY_REQUEST_SCHEMA

    request["schema_version"] = REQUEST_SCHEMA
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(ActionValueError, match="PATTERN_REQUEST_IDENTITY_MISMATCH"):
        _load_request(path)

    application_audit = {
        "schema_version": "position_timing_pattern_corporate_action_application_audit_v1",
        "source_snapshot_sha256": "a" * 64,
        "candidate_source_sha256": "c" * 64,
        "policy_sha256": CORPORATE_ACTION_APPLICATION_POLICY_SHA256,
        "scope": {
            "symbols_sha256": canonical_sha256(("000001.SZ",)),
            "symbol_count": 1,
            "start": "2024-01-02",
            "end": "2024-01-31",
        },
        "checked_stock_action_count": 0,
        "checked_factor_interval_count": 0,
        "normalized_non_pro_rata_action_count": 0,
        "normalized_non_pro_rata_actions": [],
        "ignored_non_pro_rata_action_count": 0,
        "factor_mapped_non_pro_rata_action_count": 0,
        "retained_action_count": 0,
        "retained_actions_sha256": canonical_sha256([]),
    }
    application_audit["application_sha256"] = canonical_sha256(application_audit)
    audit = {
        "schema_version": "position_timing_pattern_factor_action_coverage_audit_v1",
        "candidate_source_sha256": "c" * 64,
        "corporate_action_application_sha256": application_audit[
            "application_sha256"
        ],
        "policy_sha256": FACTOR_ACTION_COVERAGE_POLICY_SHA256,
        "scope": {
            "symbols_sha256": canonical_sha256(("000001.SZ",)),
            "symbol_count": 1,
            "start": "2024-01-02",
            "end": "2024-01-31",
        },
        "material_factor_change_count": 0,
        "bound_material_factor_change_count": 0,
        "unbound_material_factor_change_count": 0,
        "unbound_material_factor_changes": [],
        "insufficient_factor_symbol_count": 0,
        "insufficient_factor_symbols": [],
        "coverage_complete": True,
        "outcomes_read": False,
    }
    audit["audit_sha256"] = canonical_sha256(audit)
    request.update(
        {
            "candidate_source_identity": {"source_sha256": "c" * 64},
            "snapshot_symbols": ["000001.SZ"],
            "population_spec": {"start": "2024-01-02", "end": "2024-01-31"},
            "corporate_action_application_policy": CORPORATE_ACTION_APPLICATION_POLICY,
            "corporate_action_application_policy_sha256": CORPORATE_ACTION_APPLICATION_POLICY_SHA256,
            "corporate_action_application_audit": application_audit,
            "corporate_action_application_sha256": application_audit[
                "application_sha256"
            ],
            "factor_action_coverage_policy": FACTOR_ACTION_COVERAGE_POLICY,
            "factor_action_coverage_policy_sha256": FACTOR_ACTION_COVERAGE_POLICY_SHA256,
            "factor_action_coverage_audit": audit,
            "factor_action_coverage_audit_sha256": audit["audit_sha256"],
        }
    )
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_text(json.dumps(request), encoding="utf-8")
    assert _load_request(path)["schema_version"] == REQUEST_SCHEMA


def test_prior_population_includes_old_pattern_requests_but_can_exclude_current(tmp_path: Path):
    from backend.services.position_timing.contracts import canonical_sha256

    digests = []
    for folder, symbol in (("older", "000001.SZ"), ("pattern_strategy_v1", "000002.SZ")):
        request = {"schema_version": "test_request_v1", "selected_symbols": [symbol]}
        request["request_sha256"] = canonical_sha256(request)
        digests.append(request["request_sha256"])
        path = tmp_path / folder / "requests" / f"{request['request_sha256']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(request), encoding="utf-8")

    complete = prior_timing_request_population(tmp_path)
    excluding_current = prior_timing_request_population(
        tmp_path,
        exclude_request_sha256=digests[1],
    )

    assert complete["forbidden_symbols"] == ("000001.SZ", "000002.SZ")
    assert excluding_current["forbidden_symbols"] == ("000001.SZ",)


def test_snapshot_scope_fails_closed_for_new_evaluation_symbol(tmp_path):
    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps(
            {
                "scope": {
                    "symbols": ["000001.SZ"],
                    "start": "2018-08-01",
                    "end": "2026-08-31",
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ActionValueError, match="SCOPE_MISMATCH"):
        _snapshot_scope(
            path,
            expected_symbols=("000001.SZ", "000002.SZ"),
            start=date(2018, 8, 1),
            end=date(2026, 8, 31),
        )


def test_sparse_bootstrap_uses_active_dates_not_zero_filled_mean():
    result = sparse_event_interval(
        np.array([0.0, 10.0, 0.0, -2.0, 0.0]),
        np.array([0.0, 1.0, 0.0, 1.0, 0.0]),
        block_sessions=2,
        samples=200,
        seed=7,
        confidence_level=0.95,
    )
    assert result["point_bps"] == 4.0
    assert result["valid_resample_fraction"] <= 1.0
    with pytest.raises(ActionValueError, match="SPARSE"):
        sparse_event_interval(np.zeros(4), np.zeros(4), samples=10)
    assert mean_interval(np.array([1.0, 3.0]), samples=50)["point_bps"] == 2.0
    assert _effect_evidence({"lower_bps": 0.0, "upper_bps": 0.0}, coverage_complete=True) == "INCONCLUSIVE"


def test_typed_unknown_event_makes_prototype_evidence_coverage_incomplete(monkeypatch):
    from backend.services.position_timing import pattern_research as module

    bars = _bars(800)

    class Candidate:
        def __init__(self, source: pd.DataFrame) -> None:
            self.calendar = source.index
            self.source = source

        def bars(self, symbol: str) -> pd.DataFrame:
            assert symbol == "000001.SZ"
            return self.source

    monkeypatch.setattr(
        module,
        "evaluate_entry_and_exit_mechanisms",
        lambda **kwargs: ([], [], Counter({"UNKNOWN_PATH_VALUATION_UNKNOWN": 1})),
    )
    monkeypatch.setattr(module, "replay_full_policy_symbol", lambda **kwargs: ([], [], Counter()))
    result = replay_prototype(
        Candidate(bars),
        symbols=("000001.SZ",),
        corporate_actions=CorporateActionBook.empty(),
        start=bars.index[0].date(),
        end=bars.index[-1].date(),
        bootstrap_samples=20,
    )
    assert result.coverage["coverage_complete"] is False
    assert result.coverage["mechanism_unknown_event_count"] == 1
    assert all(item["effect_evidence"] == "INCONCLUSIVE" for item in result.receipt["comparisons"].values())


def test_unconfirmed_breakout_remains_in_entry_comparison():
    bars = _bars()
    features = _forced_breakout_features(bars, 25, confirm=None)
    # Immediate entry benefits while the waiting path remains cash.
    bars.iloc[26:46, bars.columns.get_loc("close")] = np.linspace(10.0, 12.0, 20)
    bars.iloc[26:46, bars.columns.get_loc("open")] = bars.iloc[26:46]["close"]
    bars.iloc[26:46, bars.columns.get_loc("high")] = bars.iloc[26:46]["close"] + 0.2
    bars.iloc[26:46, bars.columns.get_loc("low")] = bars.iloc[26:46]["close"] - 0.2
    bars.iloc[26:46, bars.columns.get_loc("up_limit")] = bars.iloc[26:46]["close"] * 1.1
    bars.iloc[26:46, bars.columns.get_loc("down_limit")] = bars.iloc[26:46]["close"] * 0.9
    events, fills, counts = evaluate_entry_and_exit_mechanisms(
        symbol="000001.SZ",
        bars=bars,
        features=features,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        start_ordinal=20,
        end_ordinal=80,
    )
    entry = [row for row in events if row["comparison"] == "E1_MINUS_E0"]
    assert len(entry) == 1
    assert entry[0]["event_outcome"] == "PULLBACK_WINDOW_EXPIRED"
    assert entry[0]["candidate_fill_status"] == "NO_ACTION"
    assert entry[0]["net_incremental_bps"] < 0
    assert counts["PULLBACK_WINDOW_EXPIRED"] == 1
    assert any(row["path_role"] == "E0_IMMEDIATE" for row in fills)


def test_confirmed_entry_executes_only_on_following_session():
    bars = _bars()
    features = _forced_breakout_features(bars, 25, confirm=27)
    events, fills, _ = evaluate_entry_and_exit_mechanisms(
        symbol="000001.SZ",
        bars=bars,
        features=features,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        start_ordinal=20,
        end_ordinal=80,
    )
    entry = next(row for row in events if row["comparison"] == "E1_MINUS_E0")
    candidate_fill = next(row for row in fills if row["path_role"] == "E1_PULLBACK")
    assert entry["confirmation_date"] == bars.index[27].date()
    assert entry["feature_available_at"] == entry["decision_as_of"] == cutoff_on(bars.index[25].date())
    assert candidate_fill["target_date"] == bars.index[28].date()
    assert candidate_fill["decision_as_of"] == cutoff_on(bars.index[27].date())
    assert candidate_fill["feature_available_at"] == candidate_fill["decision_as_of"]


def test_delayed_entry_budget_shortfall_is_no_fill_not_unknown_path():
    bars = _bars(3)
    state, fill, *_ = _execute_next_session(
        state=PositionState(0, 0, Decimal("1000"), Decimal("1000")),
        plan=ActionPlan("000001.SZ", 100, Decimal("10")),
        symbol="000001.SZ",
        bars=bars,
        decision_ordinal=0,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        parent_count=1,
        additional_friction_bps=Decimal(0),
        path_role="TEST",
    )
    assert fill.status == "NO_FILL"
    assert fill.reason == "NO_FILL_BUDGET"
    assert state.quantity == 0
    assert state.cash == Decimal("1000")

    funded_state, funded_fill, *_ = _execute_next_session(
        state=PositionState(0, 0, Decimal("2000"), Decimal("2000")),
        plan=ActionPlan("000001.SZ", 100, Decimal("10")),
        symbol="000001.SZ",
        bars=bars,
        decision_ordinal=0,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        parent_count=1,
        additional_friction_bps=Decimal(10),
        path_role="TEST_FRICTION",
    )
    assert funded_fill.status == "FILLED"
    assert funded_fill.reason == "ADDITIONAL_FRICTION_SCENARIO"
    assert funded_fill.fee > Decimal("5")
    assert funded_state.quantity == 100


def test_full_policy_preserves_state_across_suspension_and_records_fixed_l1_limit():
    bars = _bars(70)
    features = _forced_breakout_features(bars, 25, confirm=27)
    bars.iloc[40, bars.columns.get_loc("is_suspended")] = True
    rows, fills, counts = replay_full_policy_symbol(
        symbol="000001.SZ",
        bars=bars,
        features=features,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        start_ordinal=20,
        terminal_ordinal=60,
    )
    assert len(rows) == 2 * (60 - 20)
    assert counts["DECISION_PRICE_UNAVAILABLE"] == 1
    assert counts["L1_BASELINE_INFORMATION_LIMITATION_CASH_WITHOUT_HISTORICAL_INTENT"] == 1
    assert all(row["feature_available_at"] == row["decision_as_of"] for row in rows)
    candidate = [row for row in fills if row["path_role"] == "FULL_POLICY"]
    assert candidate
    assert candidate[0]["target_date"] == bars.index[28].date()
    same_price_entry = next(
        row
        for row in rows
        if row["comparison"] == "P_MINUS_FROZEN_L1" and row["valuation_date"] == bars.index[28].date()
    )
    assert same_price_entry["incremental_net_value_bps"] < 0
    assert same_price_entry["incremental_gross_value_bps"] == pytest.approx(0.0, abs=1e-9)


def test_buy_and_hold_does_not_enter_before_pit_membership():
    bars = _bars(45)
    bars.loc[bars.index[:10], "pit_active"] = False
    features = pattern_feature_frame(bars, symbol="000001.SZ")
    _, fills, _ = replay_full_policy_symbol(
        symbol="000001.SZ",
        bars=bars,
        features=features,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        start_ordinal=0,
        terminal_ordinal=30,
    )
    buy = next(row for row in fills if row["path_role"] == "FULL_BUY_AND_HOLD")
    assert buy["decision_date"] == bars.index[10].date()
    assert buy["target_date"] == bars.index[11].date()


def test_existing_inventory_can_be_priced_and_sold_after_pit_buy_eligibility_ends():
    bars = _bars(45)
    bars.loc[bars.index[20:], "pit_active"] = False
    state, fill, *_ = _execute_next_session(
        state=PositionState(100, 100, Decimal("99000"), Decimal("100000"), Decimal("10"), 5),
        plan=ActionPlan("000001.SZ", -100, Decimal("10"), risk_exit=True),
        symbol="000001.SZ",
        bars=bars,
        decision_ordinal=20,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        parent_count=1,
        additional_friction_bps=Decimal(0),
        path_role="TEST_POST_PIT_EXIT",
    )
    assert fill.status == "FILLED"
    assert state.quantity == 0

    features = pattern_feature_frame(bars, symbol="000001.SZ")
    rows, _, coverage = replay_full_policy_symbol(
        symbol="000001.SZ",
        bars=bars,
        features=features,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=CorporateActionBook.empty(),
        start_ordinal=0,
        terminal_ordinal=35,
    )
    assert len(rows) == 70
    assert coverage["HELD_INVENTORY_OUTSIDE_PIT_BUY_ELIGIBILITY"] == 15


def test_suspended_ex_date_maps_last_price_without_inventing_wealth_jump():
    from backend.services.position_timing.contracts import canonical_sha256

    bars = _bars(70)
    features = _forced_breakout_features(bars, 25, confirm=27)
    ex_ordinal = 30
    bars.iloc[ex_ordinal:, bars.columns.get_loc("open")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("high")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("low")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("close")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("up_limit")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("down_limit")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("factor")] = 2.0
    bars.iloc[ex_ordinal, bars.columns.get_loc("is_suspended")] = True
    action = CorporateAction(
        symbol="000001.SZ",
        effective_trade_date=bars.index[ex_ordinal].date(),
        quantity_multiplier=Decimal("2"),
        cashflow_yuan_per_share=Decimal("0"),
        reference_price_cash_yuan_per_share=Decimal("0"),
        cash_pay_date=None,
        share_listing_date=bars.index[ex_ordinal + 1].date(),
        source_available_at=cutoff_on(bars.index[ex_ordinal].date() - timedelta(days=5)),
        source_row_count=1,
        source_rows_sha256="a" * 64,
    )
    actions = CorporateActionBook((action,), canonical_sha256({"test": "split"}))
    features = pattern_feature_frame(bars, symbol="000001.SZ", corporate_actions=actions)
    rows, _, _ = replay_full_policy_symbol(
        symbol="000001.SZ",
        bars=bars,
        features=features,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=actions,
        start_ordinal=20,
        terminal_ordinal=40,
    )
    comparison = pd.DataFrame(rows).query("comparison == 'P_MINUS_FROZEN_L1'").set_index("valuation_date")
    before = comparison.loc[bars.index[ex_ordinal - 1].date(), "policy_wealth_cny"]
    ex_date = comparison.loc[bars.index[ex_ordinal].date(), "policy_wealth_cny"]
    assert ex_date == pytest.approx(before, abs=0.01)


def test_available_ex_date_price_does_not_require_missing_prior_day_factor():
    from backend.services.position_timing.contracts import canonical_sha256

    bars = _bars(70)
    decision_ordinal = 30
    ex_ordinal = decision_ordinal + 1
    bars.iloc[ex_ordinal:, bars.columns.get_loc("open")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("high")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("low")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("close")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("up_limit")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("down_limit")] /= 2
    bars.iloc[ex_ordinal:, bars.columns.get_loc("factor")] = 2.0
    bars.iloc[
        decision_ordinal,
        [bars.columns.get_loc(name) for name in ("open", "high", "low", "close", "factor")],
    ] = np.nan
    action = _stock_action(bars.index[ex_ordinal].date(), multiplier="2")
    actions = CorporateActionBook((action,), canonical_sha256({"test": "missing-prior-factor"}))
    features = pattern_feature_frame(bars, symbol="000001.SZ", corporate_actions=actions)

    rows, _, counts = replay_full_policy_symbol(
        symbol="000001.SZ",
        bars=bars,
        features=features,
        calendar_dates=tuple(bars.index.date),
        corporate_actions=actions,
        start_ordinal=20,
        terminal_ordinal=40,
    )

    assert len(rows) == 40
    assert counts["DECISION_PRICE_UNAVAILABLE"] == 1
    comparison = pd.DataFrame(rows).query(
        "comparison == 'P_MINUS_FROZEN_L1'"
    ).set_index("valuation_date")
    before = comparison.loc[bars.index[decision_ordinal].date(), "policy_wealth_cny"]
    ex_date = comparison.loc[bars.index[ex_ordinal].date(), "policy_wealth_cny"]
    assert ex_date == pytest.approx(before, abs=0.01)


def test_pattern_bundle_is_recursive_immutable_and_exact_retry_is_noop(tmp_path: Path):
    request = {
        "schema_version": LEGACY_REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "prototype_contract": PROTOTYPE_CONTRACT,
        "prototype_contract_sha256": PROTOTYPE_CONTRACT_SHA256,
        "optimizer_contract": _optimizer_contract(),
        "optimizer_contract_sha256": _optimizer_contract_sha256(),
        "model_contract": _model_contract(),
        "model_contract_sha256": _model_contract_sha256(),
        "preregistered_family_count": PREREGISTERED_FAMILY_COUNT,
        "total_formal_comparison_count": TOTAL_FORMAL_COMPARISON_COUNT,
        "result_class": RESULT_CLASS,
        "research_model_outputs_write": True,
        "registry_write": False,
        "current_write": False,
        "serving_model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
        "corporate_action_snapshot_sha256": "a" * 64,
        "suspension_snapshot_sha256": "b" * 64,
    }
    from backend.services.position_timing.contracts import canonical_sha256

    request["request_sha256"] = canonical_sha256(request)
    evolution = {
        "familywise_hypothesis_count": 5,
        "formal_comparison_count": 5,
        "internal_template_candidate_count": 8,
        "model_feature_set_count": 2,
        "model_head_count_per_feature_set": 2,
        "comparisons": {
            name: {"effect_evidence": "INCONCLUSIVE"}
            for name in (
                "Q_MINUS_P",
                "Q_MINUS_BUY_AND_HOLD",
                "ENHANCED_MINUS_CORE",
                "ENHANCED_MINUS_P",
                "ENHANCED_MINUS_BUY_AND_HOLD",
            )
        },
    }
    evolution["receipt_sha256"] = canonical_sha256(evolution)
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "result_class": RESULT_CLASS,
        "familywise_hypothesis_count": PROTOTYPE_FAMILY_SIZE,
        "preregistered_family_count": PREREGISTERED_FAMILY_COUNT,
        "total_formal_comparison_count": TOTAL_FORMAL_COMPARISON_COUNT,
        "selected_trial_count": 0,
        "research_model_outputs_written": True,
        "registry_written": False,
        "current_written": False,
        "serving_model_artifact_written": False,
        "card_written": False,
        "alert_written": False,
        "order_written": False,
        "database_written": False,
        "runtime_written": False,
        "evolution": evolution,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    result = PrototypeReplayResult(
        pd.DataFrame({"event": ["x"]}),
        pd.DataFrame({"day": ["2026-09-01"]}),
        pd.DataFrame({"fill": ["NO_ACTION"]}),
        {"coverage_complete": True},
        receipt,
    )
    bundle = tmp_path / "timing" / "research" / "pattern_strategy_v1" / "bundles" / request["request_sha256"]
    kwargs = {
        "request": request,
        "result": result,
        "receipt": receipt,
        "additional_frames": {"empty.parquet": pd.DataFrame()},
        "additional_json": {"nested_identity.json": {"schema_version": BUNDLE_SCHEMA}},
    }

    _publish_bundle(bundle, **kwargs)
    first = inspect_pattern_bundle(bundle)
    manifest_before = (bundle / "manifest.json").read_bytes()
    _publish_bundle(bundle, **kwargs)
    second = inspect_pattern_bundle(bundle)

    assert first["manifest"] == second["manifest"]
    assert (bundle / "manifest.json").read_bytes() == manifest_before
    assert "empty.parquet" in first["manifest"]["files"]
    with (bundle / "events.parquet").open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(ActionValueError, match="BUNDLE_FILE_IDENTITY_MISMATCH"):
        inspect_pattern_bundle(bundle)


def test_pattern_bundle_rejects_unmanifested_file(tmp_path: Path):
    request = {
        "schema_version": LEGACY_REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "prototype_contract": PROTOTYPE_CONTRACT,
        "prototype_contract_sha256": PROTOTYPE_CONTRACT_SHA256,
        "optimizer_contract": _optimizer_contract(),
        "optimizer_contract_sha256": _optimizer_contract_sha256(),
        "model_contract": _model_contract(),
        "model_contract_sha256": _model_contract_sha256(),
        "preregistered_family_count": PREREGISTERED_FAMILY_COUNT,
        "total_formal_comparison_count": TOTAL_FORMAL_COMPARISON_COUNT,
        "result_class": RESULT_CLASS,
        "research_model_outputs_write": True,
        "registry_write": False,
        "current_write": False,
        "serving_model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
        "corporate_action_snapshot_sha256": "a" * 64,
        "suspension_snapshot_sha256": "b" * 64,
    }
    from backend.services.position_timing.contracts import canonical_sha256

    request["request_sha256"] = canonical_sha256(request)
    evolution = {
        "familywise_hypothesis_count": 5,
        "formal_comparison_count": 5,
        "internal_template_candidate_count": 8,
        "model_feature_set_count": 2,
        "model_head_count_per_feature_set": 2,
        "comparisons": {
            name: {"effect_evidence": "INCONCLUSIVE"}
            for name in (
                "Q_MINUS_P",
                "Q_MINUS_BUY_AND_HOLD",
                "ENHANCED_MINUS_CORE",
                "ENHANCED_MINUS_P",
                "ENHANCED_MINUS_BUY_AND_HOLD",
            )
        },
    }
    evolution["receipt_sha256"] = canonical_sha256(evolution)
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "result_class": RESULT_CLASS,
        "familywise_hypothesis_count": PROTOTYPE_FAMILY_SIZE,
        "preregistered_family_count": PREREGISTERED_FAMILY_COUNT,
        "total_formal_comparison_count": TOTAL_FORMAL_COMPARISON_COUNT,
        "selected_trial_count": 0,
        "research_model_outputs_written": True,
        "registry_written": False,
        "current_written": False,
        "serving_model_artifact_written": False,
        "card_written": False,
        "alert_written": False,
        "order_written": False,
        "database_written": False,
        "runtime_written": False,
        "evolution": evolution,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    result = PrototypeReplayResult(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {"coverage_complete": True}, receipt
    )
    bundle = tmp_path / "timing" / "research" / "pattern_strategy_v1" / "bundles" / request["request_sha256"]
    _publish_bundle(bundle, request=request, result=result, receipt=receipt)
    (bundle / "unexpected.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(ActionValueError, match="BUNDLE_FILE_SET_MISMATCH"):
        inspect_pattern_bundle(bundle)
