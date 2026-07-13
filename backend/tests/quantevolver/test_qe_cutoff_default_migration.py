from backend.routers import quantevolver_evolution
from backend.services.quantevolver import (
    factor_metrics_scheduler,
    official_factor_batch_compute_service,
)
from backend.services.quantevolver.config_composer import (
    QE_DEFAULT_BACKTEST_END,
    QE_DEFAULT_SIGNAL_END,
)
from scripts.migrate_qe_cutoff_defaults import plan_updates, upgrade_data_split


OLD_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
    "test_end": "2026-04-28",
    "backtest_end": "2026-04-27",
}


def test_all_backend_qe_cutoff_defaults_match_latest_dataset() -> None:
    assert QE_DEFAULT_SIGNAL_END == "2026-06-30"
    assert QE_DEFAULT_BACKTEST_END == "2026-06-29"
    assert official_factor_batch_compute_service.OFFICIAL_FACTOR_WINDOW_END == "2026-06-30"
    assert factor_metrics_scheduler.OFFICIAL_FACTOR_WINDOW_END == "2026-06-30"
    assert quantevolver_evolution.OFFICIAL_FACTOR_WINDOW_END == "2026-06-30"


def test_exact_legacy_default_is_upgraded_without_mutating_input() -> None:
    upgraded, changed = upgrade_data_split(OLD_SPLIT)

    assert changed == 1
    assert upgraded["test_end"] == "2026-06-30"
    assert upgraded["backtest_end"] == "2026-06-29"
    assert OLD_SPLIT["test_end"] == "2026-04-28"


def test_intentional_custom_window_is_preserved() -> None:
    custom = {**OLD_SPLIT, "train_end": "2023-12-31"}

    upgraded, changed = upgrade_data_split(custom)

    assert changed == 0
    assert upgraded is custom


def test_plan_updates_only_explicit_legacy_splits() -> None:
    template = {"loops": [{"data_split": OLD_SPLIT}, {"data_split": {**OLD_SPLIT, "test_end": "2026-05-29"}}]}

    experiments, templates = plan_updates(
        [
            ("created-old", "created", OLD_SPLIT),
            ("created-custom", "created", {**OLD_SPLIT, "test_end": "2026-05-29"}),
        ],
        [("template-old", "approved", template)],
    )

    assert [item.record_id for item in experiments] == ["created-old"]
    assert [item.record_id for item in templates] == ["template-old"]
    assert templates[0].new_value["loops"][0]["data_split"]["backtest_end"] == "2026-06-29"
    assert templates[0].new_value["loops"][1]["data_split"]["test_end"] == "2026-05-29"
