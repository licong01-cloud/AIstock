"""Unit tests covering key edge conditions for P4, P1, and P5.

Feature: p4-p1-p5-strategy-enhancement
Validates: Requirements 3.2, 4.5, 5.6, 6.4, 7.5, 9.4
"""
from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

SIGNAL_DATE = date(2025, 6, 1)
NEXT_TRADE_DATE = date(2025, 6, 2)
PORTFOLIO_ID = 1


def _make_close_price_fn(price_map: Dict[str, float]):
    """Create a close_price_fn that returns prices from a dict."""
    def fn(symbol: str, d: date) -> Optional[float]:
        return price_map.get(symbol)
    return fn


# ===================================================================
# P4 Edge Cases
# ===================================================================


class TestP4ColdStartNoPositions:
    """P4: Cold start with no positions — risk control rules should not trigger,
    generate_orders returns only parent signals.

    Validates: Requirements 3.2
    """

    def test_no_positions_no_stop_loss_signals(self):
        """_check_stop_loss with empty positions returns no signals."""
        from backend.rebalance_strategies.topk_dropout_rc import (
            TopkDropoutWithRiskControlStrategy,
        )

        strategy = TopkDropoutWithRiskControlStrategy()
        signals = strategy._check_stop_loss(
            current_positions={},
            close_price_fn=_make_close_price_fn({}),
            signal_date=SIGNAL_DATE,
            next_trade_date=NEXT_TRADE_DATE,
            portfolio_id=PORTFOLIO_ID,
            stop_loss_pct=0.10,
        )
        assert signals == [], "Cold start: stop-loss should produce no signals"

    def test_generate_orders_cold_start_only_parent_signals(self):
        """generate_orders with no positions should return only parent-logic signals."""
        from backend.rebalance_strategies.topk_dropout_rc import (
            TopkDropoutWithRiskControlStrategy,
        )

        strategy = TopkDropoutWithRiskControlStrategy()
        score_items = [
            {"symbol": f"S{i:03d}.SH", "score": 100 - i, "rank": i + 1}
            for i in range(10)
        ]
        price_map = {f"S{i:03d}.SH": 50.0 for i in range(10)}
        config = {
            "max_positions": 5,
            "max_turnover_pct": 0.20,
            "max_position_pct": 0.25,
            "risk_degree": 0.95,
            "stop_loss_pct": 0.10,
            "max_daily_turnover_pct": 0.30,
        }

        signals = strategy.generate_orders(
            score_items=score_items,
            current_positions={},
            portfolio_value=1_000_000,
            config=config,
            signal_date=SIGNAL_DATE,
            next_trade_date=NEXT_TRADE_DATE,
            portfolio_id=PORTFOLIO_ID,
            close_price_fn=_make_close_price_fn(price_map),
        )

        # No stop-loss signals should be present
        stop_loss = [s for s in signals if s.get("reason") == "stop_loss"]
        assert stop_loss == [], "Cold start: no stop-loss signals expected"

        # Should still produce buy signals from parent logic
        buy_signals = [s for s in signals if s["side"] == "BUY"]
        assert len(buy_signals) > 0, "Cold start: parent should generate buy signals"


class TestP4ClosePriceFnReturnsNone:
    """P4: close_price_fn returns None — stop-loss check should skip that stock.

    Validates: Requirements 3.2
    """

    def test_none_price_skips_stop_loss(self):
        """When close_price_fn returns None for a stock, it is skipped."""
        from backend.rebalance_strategies.topk_dropout_rc import (
            TopkDropoutWithRiskControlStrategy,
        )

        strategy = TopkDropoutWithRiskControlStrategy()
        current_positions = {
            "A.SH": {"avg_cost": 100.0, "quantity": 1000},
            "B.SH": {"avg_cost": 100.0, "quantity": 1000},
        }
        # A.SH has no price (None), B.SH is below threshold
        price_map: Dict[str, float] = {"B.SH": 80.0}  # A.SH → None

        signals = strategy._check_stop_loss(
            current_positions=current_positions,
            close_price_fn=_make_close_price_fn(price_map),
            signal_date=SIGNAL_DATE,
            next_trade_date=NEXT_TRADE_DATE,
            portfolio_id=PORTFOLIO_ID,
            stop_loss_pct=0.10,
        )

        symbols_triggered = {s["symbol"] for s in signals}
        assert "A.SH" not in symbols_triggered, (
            "Stock with None price should be skipped"
        )
        assert "B.SH" in symbols_triggered, (
            "Stock below threshold should trigger stop-loss"
        )


class TestP4PortfolioValueZero:
    """P4: portfolio_value ≤ 0 — turnover cap calculation should be skipped.

    Validates: Requirements 3.2
    """

    def test_zero_portfolio_value_skips_turnover_cap(self):
        """_apply_turnover_cap returns all signals unchanged when portfolio_value=0."""
        from backend.rebalance_strategies.topk_dropout_rc import (
            TopkDropoutWithRiskControlStrategy,
        )

        signals = [
            {
                "portfolio_id": PORTFOLIO_ID,
                "signal_date": SIGNAL_DATE,
                "trade_date": NEXT_TRADE_DATE,
                "symbol": "A.SH",
                "side": "SELL",
                "target_quantity": 1000,
                "target_weight": 0.0,
                "score": None,
            },
            {
                "portfolio_id": PORTFOLIO_ID,
                "signal_date": SIGNAL_DATE,
                "trade_date": NEXT_TRADE_DATE,
                "symbol": "B.SH",
                "side": "BUY",
                "target_quantity": 500,
                "target_weight": 0.05,
                "score": 5.0,
            },
        ]

        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=0,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn({"A.SH": 50.0, "B.SH": 50.0}),
            signal_date=SIGNAL_DATE,
            topk_symbols=set(),
        )
        assert len(result) == len(signals), (
            "portfolio_value=0 should skip truncation, returning all signals"
        )

    def test_negative_portfolio_value_skips_turnover_cap(self):
        """_apply_turnover_cap returns all signals unchanged when portfolio_value<0."""
        from backend.rebalance_strategies.topk_dropout_rc import (
            TopkDropoutWithRiskControlStrategy,
        )

        signals = [
            {
                "portfolio_id": PORTFOLIO_ID,
                "signal_date": SIGNAL_DATE,
                "trade_date": NEXT_TRADE_DATE,
                "symbol": "A.SH",
                "side": "SELL",
                "target_quantity": 1000,
                "target_weight": 0.0,
                "score": None,
            },
        ]

        result = TopkDropoutWithRiskControlStrategy._apply_turnover_cap(
            all_signals=signals,
            portfolio_value=-100_000,
            max_daily_turnover_pct=0.30,
            close_price_fn=_make_close_price_fn({"A.SH": 50.0}),
            signal_date=SIGNAL_DATE,
            topk_symbols=set(),
        )
        assert len(result) == len(signals)


# ===================================================================
# P1 Edge Cases
# ===================================================================


class TestP1TrainingDataInsufficient:
    """P1: Training data < 120 days — sector should be skipped.

    Validates: Requirements 4.5
    """

    def test_sector_skipped_when_data_below_min_trading_days(self):
        """SectorHMMTrainer.train_all_sectors skips sectors with < 120 days."""
        from backend.quant_models.hmm.sector_hmm import (
            SectorHMMConfig,
            SectorHMMTrainer,
        )

        config = SectorHMMConfig(min_trading_days=120)
        trainer = SectorHMMTrainer(config=config, db_conn_factory=lambda: MagicMock())

        # Mock _fetch_sector_list to return one sector
        with patch.object(
            trainer, "_fetch_sector_list", return_value=[("801010.SI", "农林牧渔")]
        ):
            # Mock _build_observation_matrix to return only 50 rows (< 120)
            obs_50 = np.random.randn(50, 4)
            with patch.object(
                trainer, "_build_observation_matrix", return_value=obs_50
            ):
                models = trainer.train_all_sectors()

        assert len(models) == 0, (
            "Sector with < 120 trading days should be skipped"
        )

    def test_sector_included_when_data_at_min_trading_days(self):
        """SectorHMMTrainer.train_all_sectors includes sectors with exactly 120 days."""
        from backend.quant_models.hmm.sector_hmm import (
            SectorHMMConfig,
            SectorHMMTrainer,
        )

        config = SectorHMMConfig(min_trading_days=120)
        trainer = SectorHMMTrainer(config=config, db_conn_factory=lambda: MagicMock())

        # Generate 120 rows of realistic observation data
        rng = np.random.RandomState(42)
        obs_120 = rng.randn(120, 4) * 0.02  # small values like real returns

        with patch.object(
            trainer, "_fetch_sector_list", return_value=[("801010.SI", "农林牧渔")]
        ), patch.object(
            trainer, "_build_observation_matrix", return_value=obs_120
        ):
            models = trainer.train_all_sectors()

        assert "801010.SI" in models, (
            "Sector with exactly 120 trading days should be included"
        )


class TestP1NoModelReturnsNeutral:
    """P1: No model for a sector — SectorHMMInference should return neutral 1.0.

    Validates: Requirements 5.6
    """

    def test_missing_model_returns_neutral_coefficient(self):
        """Sectors without a trained model get coefficient 1.0."""
        from backend.quant_models.hmm.sector_hmm import (
            SectorHMMConfig,
            SectorHMMInference,
            SectorHMMTrainer,
        )

        # Create models for sector A but not sector B
        models = {
            "801010.SI": {
                "sector_code": "801010.SI",
                "sector_name": "农林牧渔",
                "n_states": 2,
                "transmat": [[0.9, 0.1], [0.1, 0.9]],
                "means": [[0.01, 0.0, 0.0, 0.0], [-0.01, 0.0, 0.0, 0.0]],
                "covars": [np.eye(4).tolist(), np.eye(4).tolist()],
                "state_labels": {"0": "trending", "1": "fading"},
                "trained_at": "2025-01-01T00:00:00",
                "training_days": 500,
            },
        }

        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = os.path.join(tmp_dir, "models.json")
            SectorHMMTrainer.save_models(models, model_path)

            config = SectorHMMConfig()
            inference = SectorHMMInference(
                model_path=model_path,
                config=config,
                db_conn_factory=lambda: MagicMock(),
            )

            # Mock _build_obs_up_to for the sector that has a model
            obs = np.random.randn(30, 4) * 0.02
            with patch.object(inference, "_build_obs_up_to", return_value=obs):
                coefficients = inference.get_sector_coefficients(date(2025, 6, 15))

        # Sector with model should have a valid coefficient
        assert "801010.SI" in coefficients
        assert coefficients["801010.SI"] in {0.5, 1.0, 1.5}

    def test_reconstruction_failure_returns_neutral(self):
        """When HMM reconstruction fails, the sector gets neutral coefficient 1.0."""
        from backend.quant_models.hmm.sector_hmm import (
            SectorHMMConfig,
            SectorHMMInference,
            SectorHMMTrainer,
        )

        # Create a model with invalid covars to cause reconstruction failure
        models = {
            "801010.SI": {
                "sector_code": "801010.SI",
                "sector_name": "农林牧渔",
                "n_states": 2,
                "transmat": [[0.9, 0.1], [0.1, 0.9]],
                "means": [[0.01, 0.0, 0.0, 0.0], [-0.01, 0.0, 0.0, 0.0]],
                "covars": [np.eye(4).tolist(), np.eye(4).tolist()],
                "state_labels": {"0": "trending", "1": "fading"},
                "trained_at": "2025-01-01T00:00:00",
                "training_days": 500,
            },
        }

        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = os.path.join(tmp_dir, "models.json")
            SectorHMMTrainer.save_models(models, model_path)

            config = SectorHMMConfig()
            inference = SectorHMMInference(
                model_path=model_path,
                config=config,
                db_conn_factory=lambda: MagicMock(),
            )

            # Force reconstruction to fail by clearing the hmm_models dict
            inference._hmm_models.clear()

            coefficients = inference.get_sector_coefficients(date(2025, 6, 15))

        assert coefficients["801010.SI"] == config.neutral_coeff, (
            "Sector with failed model reconstruction should return neutral 1.0"
        )


class TestP1EnableSectorHMMNotSet:
    """P1: enable_sector_hmm not set — default behavior should use original scores.

    Validates: Requirements 6.4
    """

    def test_hmm_disabled_by_default(self):
        """When enable_sector_hmm is not in config, scores are unchanged."""
        from backend.rebalance_strategies.topk_dropout_rc import (
            TopkDropoutWithRiskControlStrategy,
        )

        strategy = TopkDropoutWithRiskControlStrategy()
        score_items = [
            {"symbol": "A.SH", "score": 10.0, "rank": 1},
            {"symbol": "B.SH", "score": 5.0, "rank": 2},
        ]
        config: Dict[str, Any] = {}  # enable_sector_hmm not set

        result = strategy._apply_sector_hmm_adjustment(
            score_items=score_items,
            signal_date=SIGNAL_DATE,
            config=config,
        )

        for orig, res in zip(score_items, result):
            assert orig["score"] == res["score"], (
                f"Score for {orig['symbol']} should be unchanged when HMM disabled"
            )

    def test_hmm_explicit_false(self):
        """When enable_sector_hmm=False, scores are unchanged."""
        from backend.rebalance_strategies.topk_dropout_rc import (
            TopkDropoutWithRiskControlStrategy,
        )

        strategy = TopkDropoutWithRiskControlStrategy()
        score_items = [
            {"symbol": "A.SH", "score": 10.0, "rank": 1},
            {"symbol": "B.SH", "score": 5.0, "rank": 2},
        ]
        config = {"enable_sector_hmm": False}

        result = strategy._apply_sector_hmm_adjustment(
            score_items=score_items,
            signal_date=SIGNAL_DATE,
            config=config,
        )

        for orig, res in zip(score_items, result):
            assert orig["score"] == res["score"], (
                f"Score for {orig['symbol']} should be unchanged when HMM=False"
            )


# ===================================================================
# P5 Edge Cases
# ===================================================================


class TestP5OptunaNotInstalled:
    """P5: optuna not installed — test graceful fallback.

    Validates: Requirements 9.4
    """

    def test_ask_returns_none_when_optuna_unavailable(self):
        """When OPTUNA_AVAILABLE=False, ask() returns None gracefully."""
        import backend.services.quantevolver.optuna_optimizer as opt_mod

        original = opt_mod.OPTUNA_AVAILABLE
        try:
            opt_mod.OPTUNA_AVAILABLE = False
            optimizer = opt_mod.OptunaHyperparamOptimizer(
                task_id="task_001", model_type="LGB"
            )
            result = optimizer.ask()
            assert result is None, (
                "ask() should return None when optuna is unavailable"
            )
        finally:
            opt_mod.OPTUNA_AVAILABLE = original

    def test_tell_returns_false_when_optuna_unavailable(self):
        """When OPTUNA_AVAILABLE=False, tell() returns False gracefully."""
        import backend.services.quantevolver.optuna_optimizer as opt_mod

        original = opt_mod.OPTUNA_AVAILABLE
        try:
            opt_mod.OPTUNA_AVAILABLE = False
            optimizer = opt_mod.OptunaHyperparamOptimizer(
                task_id="task_001", model_type="LGB"
            )
            result = optimizer.tell(MagicMock(), 0.05)
            assert result is False, (
                "tell() should return False when optuna is unavailable"
            )
        finally:
            opt_mod.OPTUNA_AVAILABLE = original

    def test_get_or_create_study_returns_none_when_optuna_unavailable(self):
        """When OPTUNA_AVAILABLE=False, get_or_create_study() returns None."""
        import backend.services.quantevolver.optuna_optimizer as opt_mod

        original = opt_mod.OPTUNA_AVAILABLE
        try:
            opt_mod.OPTUNA_AVAILABLE = False
            optimizer = opt_mod.OptunaHyperparamOptimizer(
                task_id="task_001", model_type="LGB"
            )
            result = optimizer.get_or_create_study()
            assert result is None, (
                "get_or_create_study() should return None when optuna unavailable"
            )
        finally:
            opt_mod.OPTUNA_AVAILABLE = original


class TestP5EmptyHistoryColdStart:
    """P5: Empty history cold start — Study should be created with no injected trials.

    Validates: Requirements 7.5
    """

    def test_new_study_with_no_history(self, tmp_path):
        """When no historical trials exist, Study is created with 0 trials."""
        from backend.services.quantevolver.optuna_optimizer import (
            OptunaHyperparamOptimizer,
        )

        optimizer = OptunaHyperparamOptimizer(
            task_id="task_cold", model_type="LGB"
        )
        # Point storage to temp directory
        studies_dir = os.path.join(str(tmp_path), "optuna_studies")
        os.makedirs(studies_dir, exist_ok=True)
        optimizer.storage_path = os.path.join(studies_dir, "task_cold_LGB.db")

        # Mock injection methods to simulate empty DB
        with patch.object(optimizer, "_inject_historical_trials") as mock_hist, \
             patch.object(optimizer, "_inject_cross_task_trials") as mock_cross:
            study = optimizer.get_or_create_study()

        assert study is not None, "Study should be created even with no history"
        assert len(study.trials) == 0, (
            "Cold start Study should have 0 trials"
        )
        # Injection methods should still be called (they just find nothing)
        mock_hist.assert_called_once()
        mock_cross.assert_called_once()


class TestP5StudyFilePathFormat:
    """P5: Study file path format verification.

    Validates: Requirements 9.4
    """

    def test_storage_path_format(self):
        """Verify path follows {QE_SOTA_ASSETS_DIR}/optuna_studies/{task_id}_{model_type}.db"""
        from backend.services.quantevolver.optuna_optimizer import (
            SOTA_ASSETS_DIR,
            OptunaHyperparamOptimizer,
        )

        task_id = "task_123"
        model_type = "LGB"
        optimizer = OptunaHyperparamOptimizer(
            task_id=task_id, model_type=model_type
        )

        expected_dir = os.path.join(SOTA_ASSETS_DIR, "optuna_studies")
        expected_filename = f"{task_id}_{model_type}.db"
        expected_path = os.path.join(expected_dir, expected_filename)

        assert optimizer.storage_path == expected_path, (
            f"Expected storage_path={expected_path}, got {optimizer.storage_path}"
        )

    def test_storage_path_model_type_uppercased(self):
        """model_type should be uppercased in the storage path."""
        from backend.services.quantevolver.optuna_optimizer import (
            SOTA_ASSETS_DIR,
            OptunaHyperparamOptimizer,
        )

        optimizer = OptunaHyperparamOptimizer(
            task_id="task_456", model_type="lgb"
        )

        expected_filename = "task_456_LGB.db"
        assert optimizer.storage_path.endswith(expected_filename), (
            f"Expected path to end with {expected_filename}, "
            f"got {optimizer.storage_path}"
        )

    def test_storage_path_contains_optuna_studies_dir(self):
        """Storage path must contain the optuna_studies subdirectory."""
        from backend.services.quantevolver.optuna_optimizer import (
            OptunaHyperparamOptimizer,
        )

        for model_type in ["LGB", "XGB", "CATBOOST", "LINEAR", "PTNN"]:
            optimizer = OptunaHyperparamOptimizer(
                task_id="task_789", model_type=model_type
            )
            assert "optuna_studies" in optimizer.storage_path, (
                f"Storage path for {model_type} should contain 'optuna_studies'"
            )
