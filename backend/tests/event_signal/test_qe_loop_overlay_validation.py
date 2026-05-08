import datetime as dt

import pandas as pd

from backend.services.event_signal.qe_loop_overlay_validation import (
    CandidateScore,
    run_cash_counterfactual,
    run_next_candidate_counterfactual,
)


def test_qe_overlay_cash_counterfactual_blocks_new_buy_contribution():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    report = pd.DataFrame(
        {
            "account": [100.0, 110.0, 121.0],
            "return": [0.0, 0.10, 0.10],
            "cash": [100.0, 0.0, 0.0],
        },
        index=dates,
    )
    positions = {
        dt.date(2024, 1, 2): {},
        dt.date(2024, 1, 3): {"000001.SZ": {"price": 10.0, "weight": 1.0, "amount": 1.0}},
        dt.date(2024, 1, 4): {"000001.SZ": {"price": 11.0, "weight": 1.0, "amount": 1.0}},
    }
    overlay = pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "000001.SZ",
                "can_buy": False,
                "force_exit": False,
                "policy_risk_level": "P0_BLOCK",
                "primary_action": "block_buy",
            }
        ]
    )

    account, hits = run_cash_counterfactual(
        positions=positions,
        report=report,
        overlay=overlay,
        date_from=dt.date(2024, 1, 2),
        date_to=dt.date(2024, 1, 4),
    )

    assert hits["blocked_buy_events"] == 1
    assert hits["unique_buy_hit_symbols"] == 1
    assert account.iloc[-1] == 110.0


def test_qe_overlay_cash_counterfactual_force_exit_existing_position():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    report = pd.DataFrame(
        {"account": [100.0, 90.0], "return": [0.0, -0.10], "cash": [0.0, 0.0]},
        index=dates,
    )
    positions = {
        dt.date(2024, 1, 2): {"000001.SZ": {"price": 10.0, "weight": 1.0, "amount": 1.0}},
        dt.date(2024, 1, 3): {"000001.SZ": {"price": 9.0, "weight": 1.0, "amount": 1.0}},
    }
    overlay = pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "000001.SZ",
                "can_buy": False,
                "force_exit": True,
                "policy_risk_level": "P0_FORCE_EXIT",
                "primary_action": "force_exit",
            }
        ]
    )

    account, hits = run_cash_counterfactual(
        positions=positions,
        report=report,
        overlay=overlay,
        date_from=dt.date(2024, 1, 2),
        date_to=dt.date(2024, 1, 3),
    )

    assert hits["force_exit_events"] == 1
    assert account.iloc[-1] == 100.0


def test_qe_overlay_next_candidate_replaces_blocked_buy_contribution():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    report = pd.DataFrame(
        {
            "account": [100.0, 110.0, 121.0],
            "return": [0.0, 0.10, 0.10],
            "cash": [100.0, 0.0, 0.0],
        },
        index=dates,
    )
    positions = {
        dt.date(2024, 1, 2): {},
        dt.date(2024, 1, 3): {"000001.SZ": {"price": 10.0, "weight": 1.0, "amount": 1.0}},
        dt.date(2024, 1, 4): {"000001.SZ": {"price": 11.0, "weight": 1.0, "amount": 1.0}},
    }
    overlay = pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "000001.SZ",
                "can_buy": False,
                "force_exit": False,
                "policy_risk_level": "P0_BLOCK",
                "primary_action": "block_buy",
            }
        ]
    )
    candidate_scores = {
        dt.date(2024, 1, 3): [CandidateScore(ts_code="000002.SZ", score=0.9)],
    }
    price_returns = {
        dt.date(2024, 1, 4): {"000002.SZ": 0.05},
    }

    account, hits = run_next_candidate_counterfactual(
        positions=positions,
        report=report,
        overlay=overlay,
        candidate_scores=candidate_scores,
        price_returns=price_returns,
        date_from=dt.date(2024, 1, 2),
        date_to=dt.date(2024, 1, 4),
    )

    assert hits["blocked_buy_events"] == 1
    assert hits["replacement_open_events"] == 1
    assert hits["replacement_no_candidate_events"] == 0
    assert account.iloc[-1] == 115.5


def test_qe_overlay_next_candidate_force_exit_replaces_same_day_return():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    report = pd.DataFrame(
        {"account": [100.0, 90.0], "return": [0.0, -0.10], "cash": [0.0, 0.0]},
        index=dates,
    )
    positions = {
        dt.date(2024, 1, 2): {"000001.SZ": {"price": 10.0, "weight": 1.0, "amount": 1.0}},
        dt.date(2024, 1, 3): {"000001.SZ": {"price": 9.0, "weight": 1.0, "amount": 1.0}},
    }
    overlay = pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "000001.SZ",
                "can_buy": False,
                "force_exit": True,
                "policy_risk_level": "P0_FORCE_EXIT",
                "primary_action": "force_exit",
            }
        ]
    )
    candidate_scores = {
        dt.date(2024, 1, 3): [CandidateScore(ts_code="000002.SZ", score=0.9)],
    }
    price_returns = {
        dt.date(2024, 1, 3): {"000002.SZ": 0.02},
    }

    account, hits = run_next_candidate_counterfactual(
        positions=positions,
        report=report,
        overlay=overlay,
        candidate_scores=candidate_scores,
        price_returns=price_returns,
        date_from=dt.date(2024, 1, 2),
        date_to=dt.date(2024, 1, 3),
    )

    assert hits["force_exit_events"] == 1
    assert hits["replacement_open_events"] == 1
    assert account.iloc[-1] == 102.0


def test_qe_overlay_next_candidate_skips_blocked_candidate():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    report = pd.DataFrame(
        {"account": [100.0, 90.0], "return": [0.0, -0.10], "cash": [0.0, 0.0]},
        index=dates,
    )
    positions = {
        dt.date(2024, 1, 2): {"000001.SZ": {"price": 10.0, "weight": 1.0, "amount": 1.0}},
        dt.date(2024, 1, 3): {"000001.SZ": {"price": 9.0, "weight": 1.0, "amount": 1.0}},
    }
    overlay = pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "000001.SZ",
                "can_buy": False,
                "force_exit": True,
                "policy_risk_level": "P0_FORCE_EXIT",
                "primary_action": "force_exit",
            },
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "000002.SZ",
                "can_buy": False,
                "force_exit": False,
                "policy_risk_level": "P0_BLOCK",
                "primary_action": "block_buy",
            },
        ]
    )
    candidate_scores = {
        dt.date(2024, 1, 3): [
            CandidateScore(ts_code="000002.SZ", score=0.9),
            CandidateScore(ts_code="000003.SZ", score=0.8),
        ],
    }
    price_returns = {
        dt.date(2024, 1, 3): {"000002.SZ": 0.50, "000003.SZ": 0.03},
    }

    account, hits = run_next_candidate_counterfactual(
        positions=positions,
        report=report,
        overlay=overlay,
        candidate_scores=candidate_scores,
        price_returns=price_returns,
        date_from=dt.date(2024, 1, 2),
        date_to=dt.date(2024, 1, 3),
    )

    assert hits["replacement_open_events"] == 1
    assert hits["sample_replacement_events"][0]["replacement_symbol"] == "000003.SZ"
    assert account.iloc[-1] == 103.0
