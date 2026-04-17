"""Test Multi-Alpha Engine end-to-end (without actual Qlib execution)."""
import os, sys
os.environ.setdefault("TDX_DB_PASSWORD", "lc78080808")
sys.path.insert(0, '/mnt/f/Dev/AIstock')

from backend.services.quantevolver.experiment_config import (
    ExperimentConfig, AlphaGroup, MetaModelConfig, MultiAlphaConfig
)
from backend.services.quantevolver.multi_alpha_engine import MultiAlphaEngine
from backend.services.quantevolver.multi_alpha_resource_planner import plan_assignments

# 1. Test resource planner
print("=== Resource Planner ===")
groups = [
    AlphaGroup(group_name="pv", factor_names=["f1","f2"], model_id="ALSTM", dataset_type="TSDatasetH", compute_resource="gpu"),
    AlphaGroup(group_name="mf", factor_names=["f3","f4"], model_id="LGB", compute_resource="cpu"),
    AlphaGroup(group_name="fund", factor_names=["f5"], model_id="Ridge", compute_resource="cpu"),
    AlphaGroup(group_name="sector", factor_names=["f6","f7"], model_id="ALSTM_s", dataset_type="TSDatasetH", compute_resource="gpu"),
]

for mode in ("serial", "local_parallel", "distributed"):
    assignments = plan_assignments(groups, mode, available_nodes=[
        {"node_id": "wsl2-5080", "gpu_vram_mb": 16384, "status": "online"},
        {"node_id": "rdagent-node1", "gpu_vram_mb": 6144, "status": "online"},
    ])
    summary = [(a.group.group_name, a.node_id, a.order) for a in assignments]
    print(f"  {mode}: {summary}")

# 2. Test engine (no composer — placeholder mode)
print("\n=== Multi-Alpha Engine ===")
ma_cfg = MultiAlphaConfig(
    alpha_groups=[
        AlphaGroup(group_name="pv_medium", factor_names=["mom_20","vol_5","atr_14"],
                   model_id="__seed_ALSTM_default_v1__", dataset_type="TSDatasetH", compute_resource="gpu"),
        AlphaGroup(group_name="mf", factor_names=["mf_flow","mf_ratio"],
                   model_id="__seed_LGBModel_conservative_v1__"),
    ],
    meta_model=MetaModelConfig(method="ic_weighted"),
    execution_mode="distributed",
)

cfg = ExperimentConfig(
    factor_names=["mom_20","vol_5","atr_14","mf_flow","mf_ratio"],
    model_id="__seed_ALSTM_default_v1__",
    alpha_mode="multi",
    multi_alpha_config=ma_cfg,
    data_split={"train_start":"2018-08-01","train_end":"2022-12-31",
                "valid_start":"2023-01-01","valid_end":"2024-06-30",
                "test_start":"2024-07-01","test_end":"2026-03-10"},
    experiment_name="test_malpha_01",
)

engine = MultiAlphaEngine(
    cfg,
    available_nodes=[
        {"node_id": "wsl2-5080", "gpu_vram_mb": 16384, "status": "online"},
        {"node_id": "rdagent-node1", "gpu_vram_mb": 6144, "status": "online"},
    ],
)

result = engine.run()
print(f"  ok: {result['ok']}")
print(f"  groups: {result['total_groups']}")
print(f"  meta_method: {result['meta_method']}")
print(f"  execution_mode: {result['execution_mode']}")
print(f"  experiment_files: {list(result['experiment_files'].keys())}")

# Check DB records
from backend.db.pg_pool import get_conn
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT group_name, model_id, dataset_type, assigned_node_id, status
            FROM qe_multi_alpha_groups WHERE parent_experiment_id = %s
        """, (result["parent_experiment_id"],))
        print(f"\n  DB records:")
        for row in cur.fetchall():
            print(f"    {row}")

# 3. Test MetaModel combiner with synthetic data
print("\n=== Meta-Model Combiner ===")
import pandas as pd
import numpy as np
np.random.seed(42)

dates = pd.date_range("2024-01-01", periods=60, freq="B")
stocks = [f"S{i:04d}" for i in range(50)]
idx = pd.MultiIndex.from_product([dates, stocks], names=["datetime", "instrument"])

preds = {
    "pv_medium": pd.DataFrame({"score": np.random.randn(len(idx)) * 0.01}, index=idx),
    "mf": pd.DataFrame({"score": np.random.randn(len(idx)) * 0.005}, index=idx),
}
returns = pd.Series(np.random.randn(len(idx)) * 0.02, index=idx, name="return")

from backend.services.quantevolver.meta_model import MetaModelCombiner

for method in ("ic_weighted", "ols", "stacking"):
    combiner = MetaModelCombiner(method=method, lookback_days=30)
    combined, weights = combiner.fit_and_combine(preds, returns)
    print(f"  {method}: weights={weights}, combined_shape={combined.shape}")

print("\nAll tests passed!")
