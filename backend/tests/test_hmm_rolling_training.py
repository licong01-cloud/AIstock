from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.services.hmm_training_service import HMMTrainingService


def _weekdays(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def test_build_rolling_training_plan_defaults_to_three_month_validation() -> None:
    trading_days = _weekdays(date(2023, 1, 1), date(2026, 4, 24))

    plan = HMMTrainingService.build_rolling_training_plan(
        trading_days=trading_days,
        latest_completed_trade_date=date(2026, 4, 24),
        train_window_years=3.0,
        validation_window_months=3,
    )

    assert plan["best_validation_policy"] == "latest_3_calendar_months"
    assert plan["recommended_validation_window_months"] == 3
    assert plan["latest_completed_trade_date"] == "2026-04-24"
    assert plan["validation_start"] == "2026-01-26"
    assert plan["validation_end"] == "2026-04-24"
    assert plan["train_end"] == "2026-01-23"
    assert plan["train_start"] == "2023-01-23"
    assert plan["coefficient_start"] == plan["validation_start"]
    assert plan["coefficient_end"] == plan["validation_end"]
    assert plan["validation_trading_days"] >= 60
    assert plan["train_trading_days"] >= 500
    assert plan["warnings"] == []


def test_build_rolling_training_plan_warns_for_one_month_validation() -> None:
    trading_days = _weekdays(date(2023, 1, 1), date(2026, 4, 24))

    plan = HMMTrainingService.build_rolling_training_plan(
        trading_days=trading_days,
        latest_completed_trade_date=date(2026, 4, 24),
        train_window_years=3.0,
        validation_window_months=1,
    )

    assert plan["validation_start"] == "2026-03-25"
    assert plan["validation_window_months"] == 1
    assert any("3 calendar months" in warning for warning in plan["warnings"])
    assert any("trading days" in warning for warning in plan["warnings"])


def test_build_rolling_training_plan_rejects_invalid_validation_months() -> None:
    trading_days = _weekdays(date(2025, 1, 1), date(2026, 4, 24))

    with pytest.raises(ValueError, match="1, 2, or 3"):
        HMMTrainingService.build_rolling_training_plan(
            trading_days=trading_days,
            latest_completed_trade_date=date(2026, 4, 24),
            validation_window_months=4,
        )


def test_build_rolling_training_plan_adjusts_to_previous_trading_day() -> None:
    trading_days = _weekdays(date(2023, 1, 1), date(2026, 4, 24))

    plan = HMMTrainingService.build_rolling_training_plan(
        trading_days=trading_days,
        latest_completed_trade_date=date(2026, 4, 25),
        train_window_years=3.0,
        validation_window_months=3,
    )

    assert plan["latest_completed_trade_date"] == "2026-04-24"
    assert any("adjusted to previous trading day" in warning for warning in plan["warnings"])


def test_rolling_config_from_plan_sets_train_validation_and_precompute_fields() -> None:
    plan = {
        "latest_completed_trade_date": "2026-04-24",
        "train_window_years": 3.0,
        "validation_window_months": 3,
        "best_validation_policy": "latest_3_calendar_months",
        "train_start": "2023-01-23",
        "train_end": "2026-01-23",
        "validation_start": "2026-01-26",
        "validation_end": "2026-04-24",
        "coefficient_start": "2026-01-26",
        "coefficient_end": "2026-04-24",
        "train_trading_days": 784,
        "validation_trading_days": 65,
        "warnings": [],
    }

    config = HMMTrainingService._rolling_config_from_plan(
        {"n_states": 3, "signal_presets": {"preset_A": {"trending": 1.05}}},
        plan,
    )

    assert config["train_start"] == "2023-01-23"
    assert config["train_end"] == "2026-01-23"
    assert config["val_start"] == "2026-01-26"
    assert config["val_end"] == "2026-04-24"
    assert config["coefficient_start"] == "2026-01-26"
    assert config["coefficient_end"] == "2026-04-24"
    assert config["rolling_training"]["enabled"] is True
    assert config["rolling_training"]["executor"] == "wsl_rdagent_gpu"
    assert config["rolling_training"]["precompute_coefficients_required"] is True
    assert config["signal_presets"]["preset_A"]["trending"] == pytest.approx(1.05)
