"""
Sample configuration dicts for unit tests.

All dicts mirror the exact shape of data that arrives from the DB
in each of the four call paths.
"""
from typing import Any

# ── Path 1: exp_record from qe_experiments ────────────────────────────────────

EXP_RECORD_MINIMAL: dict[str, Any] = {
    "factor_names": ["Alpha001", "Alpha002"],
    "model_id": "model_lgbm_v1",
    "strategy_id": "TopkDropoutStrategy",
    "data_split": {"train_start": "2020-01-01", "test_end": "2023-01-01"},
    "custom_params": {},
    "strategy_params": {},
    "experiment_name": "qe_exp_test_001",
}

EXP_RECORD_WITH_HMM: dict[str, Any] = {
    "factor_names": ["Alpha001", "Alpha002"],
    "model_id": "model_lgbm_v1",
    "strategy_id": "TopkDropoutStrategy",
    "data_split": {"train_start": "2020-01-01", "test_end": "2023-01-01"},
    "custom_params": {
        "enable_sector_hmm": True,
        "sector_hmm_model_path": "/mnt/f/models/hmm_v2/model.pkl",
        "hmm_model_version_id": "hmm_snap_001",
        "hmm_signal_preset": "conservative",
        "execution_algo": "twap",
        "execution_algo_params": {"interval": 5},
    },
    "strategy_params": {"topk": 50, "n_drop": 5},
    "experiment_name": "qe_exp_test_002",
}

# ── Path 2: evolution loop config + task ──────────────────────────────────────

EVOLUTION_CONFIG_MINIMAL: dict[str, Any] = {
    "factor_list": ["Alpha001", "Alpha003"],
    "model_id": "model_lgbm_v1",
    "strategy_id": "TopkDropoutStrategy",
    "model_params": {"topk": 50},
    "data_split": {"train_start": "2020-01-01", "test_end": "2023-01-01"},
}

EVOLUTION_TASK_MINIMAL: dict[str, Any] = {
    "task_id": "task_001",
    "stock_pool": "csi300",
    "label_type": "Ref($close, -2)/Ref($close, -1) - 1",
    "strategy_id": None,
    "strategy_params": None,
    "execution_algo": None,
    "execution_algo_params": None,
    "unfilled_handler": None,
    "unfilled_handler_params": None,
    "node_id": None,
}

EVOLUTION_TASK_WITH_UNFILLED: dict[str, Any] = {
    **EVOLUTION_TASK_MINIMAL,
    "unfilled_handler": "cancel_and_resubmit",
    "unfilled_handler_params": {"trigger_minute": 145, "backup_depth": 3},
}

EVOLUTION_TASK_WITH_STRATEGY_PARAMS: dict[str, Any] = {
    **EVOLUTION_TASK_MINIMAL,
    "strategy_params": {"topk": 30, "n_drop": 3, "hold_thresh": 0.5},
    "execution_algo": "twap",
    "execution_algo_params": {"interval": 5},
}

EVOLUTION_TASK_WITH_HMM: dict[str, Any] = {
    **EVOLUTION_TASK_MINIMAL,
    "strategy_params": {
        "enable_sector_hmm": True,
        "hmm_model_version_id": "hmm_snap_001",
        "sector_hmm_model_path": "/mnt/f/Dev/AIstock/data/hmm_models/cfg_test/20260401/models.json",
        "hmm_signal_preset": "preset_B",
    },
}

# ── Path 3: strategy evo loop ─────────────────────────────────────────────────

STRATEGY_EVO_BASE_CONFIG: dict[str, Any] = {
    "factor_list": ["Alpha001", "Alpha002"],
    "model_id": "model_lgbm_v1",
    "strategy_id": "TopkDropoutStrategy",
    "model_params": {"topk": 50, "n_drop": 5},
    "data_split": {"train_start": "2020-01-01", "test_end": "2023-01-01"},
}

STRATEGY_EVO_LOOP_NO_HMM: dict[str, Any] = {
    "loop_index": 1,
    "strategy_id": "TopkDropoutStrategy",
    "strategy_params": {"topk": 30},
    "execution_algo": None,
    "execution_algo_params": {},
    "enable_sector_hmm": False,
    "sector_blacklist": ["SW_Coal", "SW_Steel"],
    "unfilled_handler": None,
    "unfilled_handler_params": None,
}

STRATEGY_EVO_LOOP_WITH_HMM: dict[str, Any] = {
    **STRATEGY_EVO_LOOP_NO_HMM,
    "enable_sector_hmm": True,
    "hmm_model_version_id": "hmm_snap_001",
    "hmm_signal_preset": "aggressive",
}

STRATEGY_EVO_TASK: dict[str, Any] = {
    "task_id": "task_strat_001",
    "stock_pool": None,
    "label_type": None,
    "unfilled_handler": None,
    "unfilled_handler_params": None,
    "node_id": None,
}

# ── Path 4: custom evo loop ───────────────────────────────────────────────────

CUSTOM_EVO_LOOP_MINIMAL: dict[str, Any] = {
    "factor_keys": ["Alpha001||v1", "Alpha002||v1"],
    "model_id": "model_lgbm_v1",
    "strategy_id": "TopkDropoutStrategy",
    "strategy_params": {"topk": 50, "n_drop": 5},
    "execution_algo": None,
    "execution_algo_params": {},
    "data_split": {"train_start": "2020-01-01", "test_end": "2023-01-01"},
    "label_type": None,
    "enable_sector_hmm": False,
    "sector_blacklist": None,
    "stock_pool": None,
    "unfilled_handler": None,
    "unfilled_handler_params": None,
}

CUSTOM_EVO_LOOP_FULL: dict[str, Any] = {
    "factor_keys": ["Alpha001||v1", "mf_rsi_14d||v2"],
    "model_id": "model_lgbm_v1",
    "strategy_id": "TopkDropoutStrategy",
    "strategy_params": {"topk": 30, "n_drop": 3, "hold_thresh": 0.5},
    "execution_algo": "twap",
    "execution_algo_params": {"interval": 5},
    "data_split": {"train_start": "2020-01-01", "test_end": "2023-01-01"},
    "label_type": "Ref($close, -2)/Ref($close, -1) - 1",
    "enable_sector_hmm": True,
    "hmm_model_version_id": "hmm_snap_001",
    "hmm_signal_preset": "conservative",
    "sector_blacklist": ["SW_Coal", "SW_Steel"],
    "stock_pool": "/mnt/f/data/stock_pools/csi300.txt",
    "unfilled_handler": "cancel_and_resubmit",
    "unfilled_handler_params": {"trigger_minute": 145, "backup_depth": 3},
}

CUSTOM_EVO_TASK: dict[str, Any] = {
    "task_id": "task_custom_001",
    "node_id": None,
}

# ── HMM snapshot (returned by HMMTrainingService.get_snapshot) ────────────────

HMM_SNAPSHOT: dict[str, Any] = {
    "snapshot_id": "hmm_snap_001",
    "config_id": "cfg_test",
    "model_path": "/mnt/f/Dev/AIstock/data/hmm_models/cfg_test/20260401/models.json",
    "status": "completed",
    "sector_count": 31,
}
