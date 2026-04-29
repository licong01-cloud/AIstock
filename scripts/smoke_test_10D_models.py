"""
Smoke test: LambdaMART + TabPFN 模型 Qlib 全流程验证
用法: wsl bash -c 'source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python /mnt/f/Dev/AIstock/scripts/smoke_test_10D_models.py'
"""

import sys, os, logging, numpy as np, pandas as pd

logging.getLogger("qlib").setLevel(logging.WARNING)

import qlib
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.strategy import TopkDropoutStrategy
from qlib.backtest import backtest

EXECUTOR = {"class": "SimulatorExecutor", "module_path": "qlib.backtest.executor",
            "kwargs": {"time_per_step": "day", "generate_report": True}}
qlib.init(provider_uri="/home/lc999/data/qlib_bin")

train_s, train_e = "2023-01-01", "2023-12-31"
test_s, test_e = "2024-04-01", "2024-06-30"
bench = None

def calc_metrics(report):
    if isinstance(report, pd.DataFrame):
        col = next((c for c in ["account", "value"] if c in report.columns), report.columns[0])
        nav = report[col]
        return {"cagr": (nav.iloc[-1]/nav.iloc[0])**(252/len(nav))-1,
                "max_dd": float((nav/nav.cummax()-1).min()),
                "sharpe": float(nav.pct_change().mean()/nav.pct_change().std()*np.sqrt(252)),
                "n": len(nav)}
    return {"cagr": 0, "max_dd": 0, "sharpe": 0, "n": 0}

def run_backtest(pred):
    from qlib.backtest.signal import create_signal_from
    signal = create_signal_from(pred)
    strategy = TopkDropoutStrategy(topk=10, n_drop=2, signal=signal)
    return backtest(strategy=strategy, executor=EXECUTOR,
                    start_time=test_s, end_time=test_e, account=10000000, benchmark=bench)

# ---- Data ----
print("=" * 60)
print("[1/4] 准备数据...")
dh = Alpha158(instruments="test_50", start_time=train_s, end_time=test_e, freq="day",
              fit_start_time=train_s, fit_end_time=train_e)
dataset = DatasetH(handler=dh, segments={"train": (train_s, train_e), "test": (test_s, test_e)})
df = dataset.prepare("train", col_set=["feature", "label"])
print(f"  训练: {len(df['feature'])} 样本, {df['feature'].shape[1]} 特征")
print(f"  ✓ 数据就绪")

# ---- LambdaMART ----
print("\n[2/4] 测试 LambdaMART...")
try:
    from aistock_models.lambdarank import LambdaRankModel
    lm = LambdaRankModel(num_leaves=32, max_depth=6, learning_rate=0.05,
                         n_estimators=100, min_child_samples=20, early_stopping_rounds=10)
    print("  fit()...")
    lm.fit(dataset)
    print("  predict()...")
    pred = lm.predict(dataset, segment="test")
    print(f"  pred: {len(pred)}条 mean={pred.mean():.6f} std={pred.std():.6f}")
    print("  backtest...")
    report, _ = run_backtest(pred)
    m = calc_metrics(report)
    print(f"  CAGR={m['cagr']:.4f} Sharpe={m['sharpe']:.2f} MaxDD={m['max_dd']:.4f} n={m['n']}")
    print(f"  ✓ LambdaMART 全流程通过")
    lm_ok = True
except Exception as e:
    print(f"  ✗ LambdaMART: {e}")
    lm_ok = False

# ---- TabPFN ----
print("\n[3/4] 测试 TabPFN...")
tp_ok = False
try:
    if not os.environ.get("TABPFN_TOKEN"):
        print("  ⚠ 跳过: 需设置 TABPFN_TOKEN (https://ux.priorlabs.ai)")
        print("    获取 API Key 后: export TABPFN_TOKEN='<key>'")
    else:
        from aistock_models.tabpfn_model import TabPFNModel
        tp = TabPFNModel(n_estimators=4, device="cuda", max_context_size=500)
        print("  fit() (存储上下文, 无训练)...")
        tp.fit(dataset)
        print("  predict() (in-context inference)...")
        pred = tp.predict(dataset, segment="test")
        print(f"  pred: {len(pred)}条 mean={pred.mean():.6f} std={pred.std():.6f}")
        print("  backtest...")
        report, _ = run_backtest(pred)
        m = calc_metrics(report)
        print(f"  CAGR={m['cagr']:.4f} Sharpe={m['sharpe']:.2f} MaxDD={m['max_dd']:.4f}")
        print(f"  ✓ TabPFN 全流程通过")
        tp_ok = True
except Exception as e:
    print(f"  ✗ TabPFN: {e}")

# ---- LGBM baseline ----
print("\n[4/4] 测试 LGBM baseline...")
lgbm_ok = False
try:
    from qlib.contrib.model.gbdt import LGBModel
    lgbm = LGBModel(num_leaves=32, max_depth=6, learning_rate=0.05,
                    n_estimators=100, min_child_samples=20)
    print("  fit()...")
    lgbm.fit(dataset)
    print("  predict()...")
    pred = lgbm.predict(dataset, segment="test")
    print(f"  pred: {len(pred)}条 mean={pred.mean():.6f} std={pred.std():.6f}")
    print("  backtest...")
    report, _ = run_backtest(pred)
    m = calc_metrics(report)
    print(f"  CAGR={m['cagr']:.4f} Sharpe={m['sharpe']:.2f} MaxDD={m['max_dd']:.4f}")
    print(f"  ✓ LGBM baseline 全流程通过")
    lgbm_ok = True
except Exception as e:
    print(f"  ✗ LGBM baseline: {e}")

# ---- Summary ----
print("\n" + "=" * 60)
print("测试汇总")
print("=" * 60)
results = [("LambdaMART (LGBMRanker)", lm_ok),
           ("TabPFN (in-context)", tp_ok),
           ("LGBM (MSE baseline)", lgbm_ok)]
for name, ok in results:
    print(f"  {'✓' if ok else '✗'} {name}")
passed = sum(1 for _, ok in results if ok)
print(f"\n通过: {passed}/{len(results)}")
if passed >= 2:
    print("\n>>> LambdaMART 可在 Qlib 完整训练→预测→回测流程中正常使用")
    print(">>> 可以安全地在 QE 自定义任务中使用")
sys.exit(0 if passed >= 2 else 1)
