#!/usr/bin/env python3
"""
QE 单次实验深度分析工具 — 直接从 workspace 文件分析回测结果
不依赖 API，可离线运行（需在 WSL rdagent-gpu 环境中执行）

功能：
1. 模型与训练诊断 — 从 run.log / mlruns 解析训练过程
2. 信号质量分析 — 从 pred.pkl / sig_analysis 解析 IC/ICIR
3. 回测执行分析 — 重新执行回测并提取完整指标
4. 交易记录分析 — 持仓快照、换手率、现金比率、交易成本
5. 持仓集中度分析 — Top N 持仓占比、行业分布
6. 回撤深度分析 — 回撤期间持仓暴露与市场环境

用法：
  python qe_single_experiment_analyzer.py <workspace_path> [--no-rerun] [--json]
"""

import argparse
import json
import os
import sys
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import yaml

warnings.filterwarnings('ignore')

# 抑制 Qlib 日志输出到 stderr
import logging
logging.getLogger('qlib').setLevel(logging.CRITICAL)

# ─── 工具函数 ─────────────────────────────────────────────

def load_yaml_conf(workspace: Path) -> dict:
    """加载 conf.yaml（处理 Jinja2 模板变量）"""
    conf_path = workspace / "conf.yaml"
    if not conf_path.exists():
        return {}
    text = conf_path.read_text()
    # 替换 Jinja2 模板变量为默认值
    text = text.replace("{{ num_features }}", "41")
    text = text.replace("{% if num_timesteps %}, \"num_timesteps\": {{ num_timesteps }}{% endif %}", "")
    text = text.replace("{{ dataset_cls | default(\"DatasetH\") }}", "DatasetH")
    text = text.replace("{% if step_len %}step_len: {{ step_len }}{% endif %}", "")
    try:
        return yaml.safe_load(text)
    except Exception as e:
        print(f"  [WARN] conf.yaml 解析失败: {e}")
        return {}


def find_pred_pkl(workspace: Path) -> Path | None:
    """查找 pred.pkl"""
    mlruns = workspace / "mlruns"
    if not mlruns.exists():
        return None
    for pkl in mlruns.rglob("pred.pkl"):
        return pkl
    return None


def find_ic_pkl(workspace: Path) -> Path | None:
    """查找 ic.pkl"""
    mlruns = workspace / "mlruns"
    if not mlruns.exists():
        return None
    for pkl in mlruns.rglob("ic.pkl"):
        return pkl
    return None


def find_label_pkl(workspace: Path) -> Path | None:
    """查找 label.pkl"""
    mlruns = workspace / "mlruns"
    if not mlruns.exists():
        return None
    for pkl in mlruns.rglob("label.pkl"):
        return pkl
    return None


def parse_run_log(workspace: Path) -> dict:
    """解析 Loop1/run.log 或根目录的训练日志"""
    result = {"error": None, "best_epoch": None, "train_loss": None, "valid_loss": None, "test_samples": None}

    # 尝试多个路径
    log_paths = [
        workspace / "Loop1" / "run.log",
        workspace / "run.log",
    ]

    log_path = None
    for p in log_paths:
        if p.exists():
            log_path = p
            break

    if log_path is None:
        return result

    text = log_path.read_text(errors='replace')

    # 检查错误
    for line in text.split('\n'):
        if 'ERROR' in line and 'exception' in line.lower():
            result['error'] = line.strip()
        if 'ValueError' in line or 'RuntimeError' in line:
            result['error'] = line.strip()

    # 解析训练信息
    import re
    # Final train eval
    m = re.search(r'Final train eval \(best model @ epoch (\d+)\): train ([\d.]+), valid ([\d.]+)', text)
    if m:
        result['best_epoch'] = int(m.group(1))
        result['train_loss'] = float(m.group(2))
        result['valid_loss'] = float(m.group(3))

    # Test samples
    m = re.search(r'Test samples: (\d+)', text)
    if m:
        result['test_samples'] = int(m.group(1))

    # 检查 backtest 错误
    if 'backtest failed' in text.lower():
        m = re.search(r'\[ERROR\].*error=(.*)', text)
        if m:
            result['backtest_error'] = m.group(1).strip()

    return result


# ─── 分析模块 ─────────────────────────────────────────────

def analyze_signal(workspace: Path) -> dict:
    """分析信号质量"""
    result = {"ic_mean": None, "icir": None, "rank_ic": None, "rank_icir": None,
              "ic_positive_ratio": None, "pred_shape": None}

    # 从已有的 qlib_results_llm.json 读取
    llm_json = workspace / "qlib_results_llm.json"
    if llm_json.exists():
        data = json.loads(llm_json.read_text())
        summary = data.get("summary", {})
        result["ic_mean"] = summary.get("IC")
        result["icir"] = summary.get("ICIR")
        result["rank_ic"] = summary.get("Rank IC")
        result["rank_icir"] = summary.get("Rank ICIR")

        diag = data.get("ic_diagnostics_summary", {})
        result["ic_positive_ratio"] = diag.get("ic_positive_ratio")
        result["ic_stability"] = diag.get("ic_stability_score")
        result["ic_trend"] = diag.get("ic_trend")

        pred_diag = data.get("prediction_diagnostics_summary", {})
        result["pred_rank_turnover"] = pred_diag.get("pred_rank_turnover")
        result["top30_stability"] = pred_diag.get("top30_stability")
        return result

    # 从 qlib_res.csv 读取
    csv_path = workspace / "qlib_res.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path, index_col=0)
        result["ic_mean"] = df.loc["IC", "0"] if "IC" in df.index else None
        result["icir"] = df.loc["ICIR", "0"] if "ICIR" in df.index else None
        result["rank_ic"] = df.loc["Rank IC", "0"] if "Rank IC" in df.index else None
        result["rank_icir"] = df.loc["Rank ICIR", "0"] if "Rank ICIR" in df.index else None

    # 从 ic.pkl 读取
    ic_pkl = find_ic_pkl(workspace)
    if ic_pkl:
        try:
            ic = pd.read_pickle(ic_pkl)
            if result["ic_mean"] is None:
                result["ic_mean"] = ic.mean()
            result["ic_positive_ratio"] = (ic > 0).mean()
        except Exception:
            pass

    # 从 pred.pkl 获取 shape
    pred_pkl = find_pred_pkl(workspace)
    if pred_pkl:
        try:
            pred = pd.read_pickle(pred_pkl)
            result["pred_shape"] = list(pred.shape)
        except Exception:
            pass

    return result


def run_backtest_and_analyze(workspace: Path, conf: dict) -> dict:
    """执行回测并提取完整的交易和收益指标"""
    # 确保 workspace 在 sys.path 中，以便 Qlib 能导入 custom_strategy 等模块
    ws_str = str(workspace)
    if ws_str not in sys.path:
        sys.path.insert(0, ws_str)
    orig_cwd = os.getcwd()
    os.chdir(workspace)

    import qlib
    from qlib.backtest import backtest

    result = {
        "success": False, "error": None,
        "total_return": None, "ann_return": None, "max_drawdown": None,
        "sharpe": None, "calmar": None, "volatility": None,
        "avg_turnover": None, "total_cost": None, "ann_cost_rate": None,
        "avg_cash_ratio": None, "avg_position_count": None,
        "positive_days_ratio": None, "max_daily_gain": None, "max_daily_loss": None,
        "monthly_returns": {}, "drawdown_periods": [],
        "position_snapshots": [], "benchmark_return": None,
    }

    # 加载 pred.pkl
    pred_pkl = find_pred_pkl(workspace)
    if not pred_pkl:
        result["error"] = "pred.pkl 不存在"
        return result

    try:
        pred = pd.read_pickle(pred_pkl)
    except Exception as e:
        result["error"] = f"pred.pkl 加载失败: {e}"
        return result

    # 初始化 Qlib
    qlib_init = conf.get("qlib_init", {})
    if not qlib_init:
        qlib_init = {"provider_uri": "/home/lc999/data/qlib_bin", "region": "cn"}
    qlib.init(**qlib_init)

    # 提取策略和回测配置
    port_config = conf.get("port_analysis_config", {})
    strategy_config = port_config.get("strategy", {}).copy()
    bt_config = port_config.get("backtest", {})

    if not strategy_config or not bt_config:
        result["error"] = "conf.yaml 缺少 port_analysis_config"
        return result

    strategy_config["kwargs"]["signal"] = pred
    account = bt_config.get("account", 100000000)

    try:
        portfolio_metric, indicator = backtest(
            start_time=bt_config["start_time"],
            end_time=bt_config["end_time"],
            strategy=strategy_config,
            executor={
                "class": "SimulatorExecutor",
                "module_path": "qlib.backtest.executor",
                "kwargs": {
                    "time_per_step": "day",
                    "generate_portfolio_metrics": True,
                    **bt_config.get("exchange_kwargs", {})
                }
            },
            benchmark=bt_config.get("benchmark", "000300.SH"),
            account=account,
        )
    except Exception as e:
        result["error"] = f"回测执行失败: {e}"
        return result

    result["success"] = True
    pm, ind = portfolio_metric['1day']

    # ── 基本指标 ──
    acct = pm['account']
    daily_ret = acct.pct_change().dropna()
    total_ret = acct.iloc[-1] / acct.iloc[0] - 1
    n_days = len(pm)
    ann_ret = (1 + total_ret) ** (252 / n_days) - 1

    cum_max = acct.cummax()
    drawdown = acct / cum_max - 1
    max_dd = drawdown.min()
    max_dd_date = drawdown.idxmin()

    result["total_return"] = round(float(total_ret), 6)
    result["ann_return"] = round(float(ann_ret), 6)
    result["max_drawdown"] = round(float(max_dd), 6)
    result["max_drawdown_date"] = str(max_dd_date.date()) if hasattr(max_dd_date, 'date') else str(max_dd_date)
    result["sharpe"] = round(float(daily_ret.mean() / daily_ret.std() * np.sqrt(252)), 4) if daily_ret.std() > 0 else 0
    result["calmar"] = round(float(ann_ret / abs(max_dd)), 4) if max_dd != 0 else 0
    result["volatility"] = round(float(daily_ret.std() * np.sqrt(252)), 4)
    result["backtest_days"] = n_days
    result["initial_account"] = float(acct.iloc[0])
    result["final_account"] = round(float(acct.iloc[-1]), 2)

    # ── 交易指标 ──
    result["avg_turnover"] = round(float(pm['turnover'].mean()), 4)
    result["max_turnover"] = round(float(pm['turnover'].max()), 4)
    result["total_cost"] = round(float(pm['cost'].sum()), 2)
    result["ann_cost_rate"] = round(float(pm['cost'].sum() / n_days * 252 / account), 6)

    # ── 现金比率 ──
    cash_ratio = pm['cash'] / pm['account']
    result["avg_cash_ratio"] = round(float(cash_ratio.mean()), 4)
    result["max_cash_ratio"] = round(float(cash_ratio.max()), 4)
    result["min_cash_ratio"] = round(float(cash_ratio.min()), 4)

    # ── 日收益分布 ──
    result["positive_days_ratio"] = round(float((daily_ret > 0).mean()), 4)
    result["max_daily_gain"] = round(float(daily_ret.max()), 4)
    result["max_daily_loss"] = round(float(daily_ret.min()), 4)
    result["daily_return_mean"] = round(float(daily_ret.mean()), 6)
    result["daily_return_median"] = round(float(daily_ret.median()), 6)
    result["daily_return_skew"] = round(float(daily_ret.skew()), 4)
    result["daily_return_kurtosis"] = round(float(daily_ret.kurtosis()), 4)

    # ── 月度收益 ──
    pm_m = pm.copy()
    pm_m['month'] = pm_m.index.to_period('M')
    monthly_agg = pm_m.groupby('month')['account'].agg(['first', 'last'])
    monthly_agg['ret'] = monthly_agg['last'] / monthly_agg['first'] - 1
    result["monthly_returns"] = {str(m): round(float(r), 6) for m, r in monthly_agg['ret'].items()}
    result["positive_months"] = int((monthly_agg['ret'] > 0).sum())
    result["negative_months"] = int((monthly_agg['ret'] <= 0).sum())

    # ── 回撤期 ──
    dd_periods = []
    in_dd = False
    start = None
    for date, dd_val in drawdown.items():
        if dd_val < -0.05 and not in_dd:
            in_dd = True
            start = date
        elif dd_val >= -0.01 and in_dd:
            in_dd = False
            dd_periods.append((start, date, float(drawdown.loc[start:date].min())))
    if in_dd:
        dd_periods.append((start, drawdown.index[-1], float(drawdown.loc[start:].min())))

    dd_periods.sort(key=lambda x: x[2])
    result["drawdown_periods"] = [
        {"start": s.strftime('%Y-%m-%d'), "end": e.strftime('%Y-%m-%d'),
         "days": (e - s).days, "max_dd": round(d, 4)}
        for s, e, d in dd_periods[:5]
    ]

    # ── 持仓快照 ──
    sample_indices = [0, 5, n_days // 4, n_days // 2, 3 * n_days // 4, n_days - 1]
    for idx in sample_indices:
        if idx >= n_days:
            continue
        date = pm.index[idx]
        snapshot = {
            "date": date.strftime('%Y-%m-%d'),
            "account": round(float(acct.iloc[idx]), 0),
            "cash": round(float(pm['cash'].iloc[idx]), 0),
            "cash_ratio": round(float(cash_ratio.iloc[idx]), 4),
            "stocks": [],
        }

        if date in ind:
            pos = ind[date]
            if hasattr(pos, 'position'):
                holdings = {k: v for k, v in pos.position.items()
                           if k != 'cash' and isinstance(v, dict)}
                snapshot["position_count"] = len(holdings)

                sorted_h = sorted(
                    holdings.items(),
                    key=lambda x: x[1].get('amount', 0) * x[1].get('price', 0) if isinstance(x[1], dict) else 0,
                    reverse=True
                )
                for stock, h in sorted_h[:10]:
                    if isinstance(h, dict):
                        mv = h.get('amount', 0) * h.get('price', 0)
                        snapshot["stocks"].append({
                            "code": stock,
                            "amount": int(h.get('amount', 0)),
                            "price": round(float(h.get('price', 0)), 2),
                            "market_value": round(mv, 0),
                        })

                # 持仓集中度
                all_mv = [h.get('amount', 0) * h.get('price', 0) for h in holdings.values() if isinstance(h, dict)]
                total_mv = sum(all_mv)
                if total_mv > 0 and all_mv:
                    all_mv.sort(reverse=True)
                    snapshot["top5_ratio"] = round(sum(all_mv[:5]) / total_mv, 4)
                    snapshot["top10_ratio"] = round(sum(all_mv[:10]) / total_mv, 4)
                    snapshot["total_position_value"] = round(total_mv, 0)

        result["position_snapshots"].append(snapshot)

    # ── 策略参数 ──
    strat_kwargs = strategy_config.get("kwargs", {})
    result["strategy_params"] = {
        "topk": strat_kwargs.get("topk"),
        "n_drop": strat_kwargs.get("n_drop"),
        "min_score": strat_kwargs.get("min_score"),
        "stop_loss": strat_kwargs.get("stop_loss"),
        "max_market_cap": strat_kwargs.get("max_market_cap"),
        "max_position_ratio": strat_kwargs.get("max_position_ratio"),
    }

    return result


def analyze_config(workspace: Path) -> dict:
    """分析实验配置"""
    result = {"factors": [], "model_class": None, "model_params": {}, "data_split": {}}

    # Loop1 config.json
    config_json = workspace / "Loop1" / "config.json"
    if config_json.exists():
        data = json.loads(config_json.read_text())
        result["factors"] = data.get("factor_list", [])
        result["model_id"] = data.get("model_id")
        result["strategy_id"] = data.get("strategy_id")
        result["data_split"] = data.get("data_split", {})
        result["model_params"] = data.get("model_params", {})

    # model.py
    model_py = workspace / "model.py"
    if model_py.exists():
        text = model_py.read_text()
        import re
        m = re.search(r'class\s+(\w+)\s*\(nn\.Module\)', text)
        if m:
            result["model_class"] = m.group(1)

        m = re.search(r'hidden_size\s*=\s*(\d+)', text)
        if not m:
            m = re.search(r'nn\.GRU\([^)]*hidden_size\s*=\s*(\d+)', text)
        if m:
            result["model_hidden_size"] = int(m.group(1))

        m = re.search(r'num_layers\s*=\s*(\d+)', text)
        if m:
            result["model_num_layers"] = int(m.group(1))

    # conf.yaml 训练参数
    conf = load_yaml_conf(workspace)
    task = conf.get("task", {})
    model = task.get("model", {})
    kwargs = model.get("kwargs", {})
    for k in ["n_epochs", "lr", "early_stop", "batch_size", "weight_decay"]:
        if k in kwargs:
            result["model_params"][k] = kwargs[k]

    return result


# ─── 报告生成 ─────────────────────────────────────────────

def generate_report(workspace: Path, signal: dict, config: dict,
                    training: dict, backtest_result: dict) -> str:
    """生成 Markdown 报告"""
    lines = []
    exp_name = workspace.name

    lines.append(f"# QE 单次实验深度分析: {exp_name}")
    lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # ── §1 实验概览 ──
    lines.append("## 1. 实验概览\n")
    lines.append(f"| 项目 | 值 |")
    lines.append(f"|------|-----|")
    lines.append(f"| 模型 | {config.get('model_class', 'N/A')} |")
    if config.get("model_hidden_size"):
        lines.append(f"| 隐藏层 | hidden={config['model_hidden_size']}, layers={config.get('model_num_layers', 'N/A')} |")
    lines.append(f"| 因子数 | {len(config.get('factors', []))} + 20 Alpha158 |")
    lines.append(f"| 数据划分 | Train {config.get('data_split', {}).get('train_start', '')} ~ {config.get('data_split', {}).get('train_end', '')} |")
    lines.append(f"| | Valid {config.get('data_split', {}).get('valid_start', '')} ~ {config.get('data_split', {}).get('valid_end', '')} |")
    lines.append(f"| | Test {config.get('data_split', {}).get('test_start', '')} ~ {config.get('data_split', {}).get('test_end', '')} |")
    for k in ["n_epochs", "lr", "batch_size", "weight_decay", "early_stop"]:
        v = config.get("model_params", {}).get(k)
        if v is not None:
            lines.append(f"| {k} | {v} |")
    lines.append("")

    # ── §2 训练诊断 ──
    lines.append("## 2. 训练诊断\n")
    if training.get("best_epoch") is not None:
        lines.append(f"- **Best Epoch**: {training['best_epoch']}")
        lines.append(f"- **Train Loss**: {training['train_loss']:.6f}")
        lines.append(f"- **Valid Loss**: {training['valid_loss']:.6f}")
        overfit = training['train_loss'] / training['valid_loss'] if training['valid_loss'] > 0 else 0
        lines.append(f"- **过拟合比率**: {overfit:.4f} (train/valid, 1.0=完美)")
        if training['best_epoch'] == 0:
            lines.append(f"- **[WARNING]** best_epoch=0，模型从未改善！")
        if overfit < 0.95:
            lines.append(f"- **[WARNING]** 过拟合比率 <0.95，可能存在过拟合")
    if training.get("error"):
        lines.append(f"- **[ERROR]** {training['error']}")
    if training.get("backtest_error"):
        lines.append(f"- **[BACKTEST ERROR]** {training['backtest_error']}")
    if training.get("test_samples"):
        lines.append(f"- **Test Samples**: {training['test_samples']:,}")
    lines.append("")

    # ── §3 信号质量 ──
    lines.append("## 3. 信号质量\n")
    lines.append("| 指标 | 值 | 评价 |")
    lines.append("|------|-----|------|")

    def eval_metric(val, good, warn):
        if val is None: return "N/A"
        if val >= good: return "优秀"
        if val >= warn: return "正常"
        return "**警告**"

    if signal.get("ic_mean") is not None:
        ic = signal["ic_mean"]
        lines.append(f"| IC | {ic:.4f} | {eval_metric(ic, 0.06, 0.03)} |")
    if signal.get("icir") is not None:
        icir = signal["icir"]
        lines.append(f"| ICIR | {icir:.4f} | {eval_metric(icir, 0.6, 0.3)} |")
    if signal.get("rank_ic") is not None:
        lines.append(f"| Rank IC | {signal['rank_ic']:.4f} | {eval_metric(signal['rank_ic'], 0.06, 0.03)} |")
    if signal.get("ic_positive_ratio") is not None:
        lines.append(f"| IC 正值比率 | {signal['ic_positive_ratio']:.1%} | {eval_metric(signal['ic_positive_ratio'], 0.7, 0.55)} |")
    if signal.get("pred_rank_turnover") is not None:
        lines.append(f"| 预测排名换手 | {signal['pred_rank_turnover']:.4f} | {'正常' if signal['pred_rank_turnover'] < 0.4 else '**不稳定**'} |")
    if signal.get("top30_stability") is not None:
        lines.append(f"| Top30 稳定性 | {signal['top30_stability']:.4f} | {'正常' if signal['top30_stability'] > 0.3 else '**不稳定**'} |")
    lines.append("")

    # ── §4 回测结果 ──
    bt = backtest_result
    if bt.get("success"):
        lines.append("## 4. 回测结果\n")
        lines.append("| 指标 | 值 | 评价 |")
        lines.append("|------|-----|------|")
        lines.append(f"| 初始资金 | {bt['initial_account']:,.0f} | |")
        lines.append(f"| 最终资金 | {bt['final_account']:,.0f} | |")
        lines.append(f"| **总收益率** | **{bt['total_return']:.2%}** | |")
        lines.append(f"| **年化收益率** | **{bt['ann_return']:.2%}** | {eval_metric(bt['ann_return'], 0.5, 0.1)} |")
        lines.append(f"| **最大回撤** | **{bt['max_drawdown']:.2%}** | {'优秀' if bt['max_drawdown'] > -0.15 else '正常' if bt['max_drawdown'] > -0.30 else '**严重**'} |")
        lines.append(f"| **夏普比率** | **{bt['sharpe']:.4f}** | {eval_metric(bt['sharpe'], 2.0, 0.5)} |")
        lines.append(f"| Calmar比率 | {bt['calmar']:.4f} | |")
        lines.append(f"| 年化波动率 | {bt['volatility']:.2%} | |")
        lines.append(f"| 回测天数 | {bt['backtest_days']} | |")
        lines.append("")

        # ── §5 交易效率 ──
        lines.append("## 5. 交易效率\n")
        lines.append("| 指标 | 值 | 评价 |")
        lines.append("|------|-----|------|")
        lines.append(f"| 平均换手率 | {bt['avg_turnover']:.4f} | {'正常' if bt['avg_turnover'] < 0.5 else '**偏高**'} |")
        lines.append(f"| 最大换手率 | {bt['max_turnover']:.4f} | |")
        lines.append(f"| 年化成本率 | {bt['ann_cost_rate']:.4%} | {'正常' if bt['ann_cost_rate'] < 0.03 else '**过高**'} |")
        lines.append(f"| **平均现金比率** | **{bt['avg_cash_ratio']:.2%}** | {'正常' if bt['avg_cash_ratio'] < 0.2 else '**资金利用率低**'} |")
        lines.append(f"| 最大现金比率 | {bt['max_cash_ratio']:.2%} | |")
        lines.append(f"| 最小现金比率 | {bt['min_cash_ratio']:.2%} | |")
        lines.append("")

        # ── §6 日收益分布 ──
        lines.append("## 6. 日收益分布\n")
        lines.append(f"- 正收益天数: {bt['positive_days_ratio']:.1%}")
        lines.append(f"- 日均收益: {bt['daily_return_mean']:.4%}")
        lines.append(f"- 日收益中位数: {bt['daily_return_median']:.4%}")
        lines.append(f"- 偏度: {bt['daily_return_skew']:.4f}")
        lines.append(f"- 峰度: {bt['daily_return_kurtosis']:.4f}")
        lines.append(f"- 最大单日涨幅: {bt['max_daily_gain']:.2%}")
        lines.append(f"- 最大单日跌幅: {bt['max_daily_loss']:.2%}")
        lines.append("")

        # ── §7 月度收益 ──
        lines.append("## 7. 月度收益\n")
        lines.append("| 月份 | 收益率 |")
        lines.append("|------|--------|")
        for m, r in bt.get("monthly_returns", {}).items():
            marker = " **" if abs(r) > 0.10 else ""
            lines.append(f"| {m} | {r:+.2%}{marker} |")
        lines.append(f"\n正收益月 {bt.get('positive_months', 0)}, 负收益月 {bt.get('negative_months', 0)}")
        lines.append("")

        # ── §8 回撤分析 ──
        lines.append("## 8. 回撤分析\n")
        if bt.get("drawdown_periods"):
            lines.append("| 排名 | 开始 | 结束 | 持续天数 | 最大回撤 |")
            lines.append("|------|------|------|---------|---------|")
            for i, dd in enumerate(bt["drawdown_periods"]):
                lines.append(f"| {i+1} | {dd['start']} | {dd['end']} | {dd['days']}天 | {dd['max_dd']:.2%} |")
        else:
            lines.append("无显著回撤期 (>5%)")
        lines.append("")

        # ── §9 持仓快照 ──
        lines.append("## 9. 持仓快照\n")
        for snap in bt.get("position_snapshots", []):
            n = snap.get("position_count", 0)
            lines.append(f"### {snap['date']} — {n}只股票, 现金比率 {snap['cash_ratio']:.1%}")
            lines.append(f"账户: {snap['account']:,.0f}, 现金: {snap['cash']:,.0f}")
            if snap.get("total_position_value"):
                lines.append(f"持仓市值: {snap['total_position_value']:,.0f}")
            if snap.get("top5_ratio"):
                lines.append(f"Top5集中度: {snap['top5_ratio']:.1%}, Top10集中度: {snap['top10_ratio']:.1%}")

            if snap.get("stocks"):
                lines.append("\n| 代码 | 数量 | 价格 | 市值 |")
                lines.append("|------|------|------|------|")
                for s in snap["stocks"][:10]:
                    lines.append(f"| {s['code']} | {s['amount']:,} | {s['price']:.2f} | {s['market_value']:,.0f} |")
            lines.append("")

        # ── §10 策略参数 ──
        lines.append("## 10. 策略参数\n")
        sp = bt.get("strategy_params", {})
        for k, v in sp.items():
            if v is not None:
                lines.append(f"- **{k}**: {v}")
        lines.append("")

    elif bt.get("error"):
        lines.append("## 4. 回测结果\n")
        lines.append(f"**回测失败**: {bt['error']}\n")

    # ── §11 诊断结论 ──
    lines.append("## 11. 诊断结论\n")
    issues = []
    recommendations = []

    # 训练问题
    if training.get("best_epoch") == 0:
        issues.append("[CRITICAL] 模型从未改善 (best_epoch=0)")
    if training.get("backtest_error"):
        issues.append(f"[CRITICAL] 回测崩溃: {training['backtest_error']}")

    # 信号问题
    if signal.get("ic_mean") is not None and signal["ic_mean"] < 0.03:
        issues.append(f"[WARNING] IC={signal['ic_mean']:.4f} 低于阈值 0.03")
    if signal.get("top30_stability") is not None and signal["top30_stability"] < 0.2:
        issues.append("[WARNING] Top30 预测排名不稳定")

    # 回测问题
    if bt.get("success"):
        if bt.get("avg_cash_ratio", 0) > 0.3:
            issues.append(f"[WARNING] 平均现金比率 {bt['avg_cash_ratio']:.1%}，资金利用率低")
            recommendations.append("降低 min_score 或提高 max_market_cap 以增加可投资标的")
        if bt.get("avg_turnover", 0) > 0.5:
            issues.append(f"[WARNING] 平均换手率 {bt['avg_turnover']:.4f}，交易过于频繁")
            recommendations.append("提高 hold_thresh 或降低 n_drop 以减少交易频率")
        if bt.get("max_drawdown", 0) < -0.30:
            issues.append(f"[WARNING] 最大回撤 {bt['max_drawdown']:.2%}，风险较高")
        if bt.get("ann_cost_rate", 0) > 0.05:
            issues.append(f"[WARNING] 年化成本率 {bt['ann_cost_rate']:.2%}，成本侵蚀严重")

    if issues:
        lines.append("### 检测到的问题\n")
        for issue in issues:
            lines.append(f"- {issue}")
        lines.append("")
    else:
        lines.append("无明显问题。\n")

    if recommendations:
        lines.append("### 优化建议\n")
        for i, rec in enumerate(recommendations, 1):
            lines.append(f"{i}. {rec}")
        lines.append("")

    return "\n".join(lines)


# ─── 主函数 ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="QE 单次实验深度分析")
    parser.add_argument("workspace", help="实验 workspace 路径")
    parser.add_argument("--no-rerun", action="store_true", help="不重新执行回测")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式")
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    if not workspace.exists():
        print(f"ERROR: workspace 不存在: {workspace}")
        sys.exit(1)

    print(f"分析实验: {workspace.name}")
    print(f"Workspace: {workspace}\n")

    # 加载配置
    conf = load_yaml_conf(workspace)

    # 各模块分析
    print("[1/4] 分析实验配置...")
    config = analyze_config(workspace)

    print("[2/4] 解析训练日志...")
    training = parse_run_log(workspace)

    print("[3/4] 分析信号质量...")
    signal = analyze_signal(workspace)

    if args.no_rerun:
        print("[4/4] 跳过回测 (--no-rerun)")
        backtest_result = {"success": False, "error": "跳过 (--no-rerun)"}
    else:
        print("[4/4] 执行回测并分析交易记录...")
        backtest_result = run_backtest_and_analyze(workspace, conf)

    if args.json:
        output = {
            "experiment": workspace.name,
            "generated_at": datetime.now().isoformat(),
            "config": config,
            "training": training,
            "signal": signal,
            "backtest": backtest_result,
        }
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
    else:
        report = generate_report(workspace, signal, config, training, backtest_result)
        print("\n" + report)


if __name__ == "__main__":
    main()
