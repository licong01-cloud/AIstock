"""
Multi-Alpha Execution Engine (Phase 3).

Orchestrates N independent Qlib experiments (one per AlphaGroup),
collects predictions, and combines them via MetaModelCombiner.

Core principle: does NOT modify Qlib internals — reuses existing
compose_experiment_in_memory() for each group as a sub-experiment.

Execution modes:
  serial         — groups run one-by-one
  local_parallel — CPU groups parallel via asyncio
  distributed    — groups dispatched to multiple compute nodes
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from ...db.pg_pool import get_conn
from .experiment_config import ExperimentConfig, MultiAlphaConfig, AlphaGroup
from .meta_model import MetaModelCombiner
from .multi_alpha_resource_planner import plan_assignments, GroupAssignment

logger = logging.getLogger("aistock.quantevolver.multi_alpha_engine")


class MultiAlphaEngine:
    """Multi-Alpha experiment orchestrator.

    Usage:
        engine = MultiAlphaEngine(config, shared_params, composer)
        result = engine.run()
        # result contains combined prediction + per-group metrics
    """

    def __init__(
        self,
        experiment_config: ExperimentConfig,
        composer: Any = None,
        available_nodes: list[dict[str, Any]] | None = None,
    ):
        if experiment_config.alpha_mode != "multi":
            raise ValueError("MultiAlphaEngine requires alpha_mode='multi'")
        if not experiment_config.multi_alpha_config:
            raise ValueError("multi_alpha_config is required")

        self.config = experiment_config
        self.ma_config = experiment_config.multi_alpha_config
        self.composer = composer
        self.available_nodes = available_nodes

    def run(self) -> dict[str, Any]:
        """Run the full Multi-Alpha pipeline.

        Returns:
            {
                "ok": True/False,
                "group_results": [{group_name, ic, icir, sharpe, meta_weight, ...}],
                "combined_ic": float,
                "combined_icir": float,
                "combined_sharpe": float,
                "meta_weights": {group_name: weight},
                "meta_method": str,
                "experiment_files": dict,  # all sub-experiment files
            }
        """
        start_time = time.time()

        # ── Step A: Resource planning ──────────────────────────────
        assignments = plan_assignments(
            groups=self.ma_config.alpha_groups,
            execution_mode=self.ma_config.execution_mode,
            available_nodes=self.available_nodes,
            default_node_id=self.config.node_id or "wsl2-5080",
        )
        logger.info(
            f"Multi-Alpha resource plan: {len(assignments)} groups, "
            f"mode={self.ma_config.execution_mode}"
        )

        # ── Step B: Generate sub-experiment files ──────────────────
        all_experiment_files: dict[str, str] = {}
        group_configs: list[dict] = []
        reuse_summary: list[str] = []

        for assignment in assignments:
            group = assignment.group
            reuse_mode = group.reuse_mode or "retrain"

            if reuse_mode == "reuse_prediction":
                if not group.model_source_experiment_id:
                    raise ValueError(
                        f"Group {group.group_name}: reuse_prediction requires model_source_experiment_id"
                    )
                source_info = self._load_source_group(
                    group.model_source_experiment_id,
                    group.model_source_group_name or group.group_name,
                )
                if not source_info or not source_info.get("prediction_path"):
                    raise ValueError(
                        f"Group {group.group_name}: reuse_prediction requested but "
                        f"source {group.model_source_experiment_id}/{group.model_source_group_name or group.group_name} not found"
                    )
                reuse_summary.append(
                    f"  {group.group_name}: reuse_prediction from "
                    f"{group.model_source_experiment_id}/{source_info['group_name']}"
                )
                group_configs.append({
                    "group_name": group.group_name,
                    "node_id": assignment.node_id,
                    "order": assignment.order,
                    "factor_count": len(group.factor_names),
                    "model_id": group.model_id,
                    "wsl_command": f"# REUSED prediction from {group.model_source_experiment_id}",
                    "reuse_mode": "reuse_prediction",
                    "source_prediction_path": source_info["prediction_path"],
                    "prediction_path": source_info["prediction_path"],
                })
                all_experiment_files[f"group_{group.group_name}/REUSE_PREDICTION.txt"] = (
                    f"source_experiment_id: {group.model_source_experiment_id}\n"
                    f"source_group_name: {source_info['group_name']}\n"
                    f"prediction_path: {source_info['prediction_path']}\n"
                )
                continue

            if reuse_mode == "reuse_model":
                raise ValueError(
                    f"Group {group.group_name}: reuse_model is not supported because model-only prediction reuse is not implemented"
                )

            if reuse_mode != "retrain":
                raise ValueError(f"Group {group.group_name}: unsupported reuse_mode={reuse_mode}")

            # 正常训练（retrain）
            sub_files = self._compose_group_experiment(group, assignment.node_id)
            # Prefix all files with group name
            for fname, content in sub_files.get("experiment_files", {}).items():
                prefixed = f"group_{group.group_name}/{fname}"
                all_experiment_files[prefixed] = content

            group_configs.append({
                "group_name": group.group_name,
                "node_id": assignment.node_id,
                "order": assignment.order,
                "factor_count": len(group.factor_names),
                "model_id": group.model_id,
                "wsl_command": sub_files.get("wsl_command", ""),
                "wsl_command_core": sub_files.get("wsl_command_core", ""),
                "wsl_workdir": sub_files.get("wsl_workdir", ""),
                "reuse_mode": reuse_mode,
            })

        if reuse_summary:
            logger.info("Multi-Alpha model reuse:\n" + "\n".join(reuse_summary))

        # ── Step C: Generate meta-model runner script ─────────────
        meta_runner = self._generate_meta_runner_script(group_configs)
        all_experiment_files["meta_model_runner.py"] = meta_runner

        # Config dump for debugging
        all_experiment_files["multi_alpha_config.json"] = json.dumps(
            self.ma_config.model_dump(), indent=2, ensure_ascii=False, default=str
        )

        # ── Step C2: 根目录回测依赖文件（统一回测用） ─────────────
        # meta_model_runner.py 末尾调用 qrun_limit_minute.py --pred-backtest
        # 需要在根目录有: qrun_limit_minute.py, read_exp_res.py, conf.yaml, 策略依赖
        self._add_root_backtest_files(all_experiment_files, group_configs)

        # ── Step D: Store group assignments to DB ──────────────────
        parent_id = self.config.experiment_name or str(uuid.uuid4())[:12]
        self._store_group_records(parent_id, assignments)

        elapsed = time.time() - start_time
        logger.info(f"Multi-Alpha setup complete in {elapsed:.1f}s: {len(assignments)} groups")

        return {
            "ok": True,
            "parent_experiment_id": parent_id,
            "group_configs": group_configs,
            "experiment_files": all_experiment_files,
            "meta_method": self.ma_config.meta_model.method,
            "execution_mode": self.ma_config.execution_mode,
            "total_groups": len(assignments),
        }

    def _compose_group_experiment(
        self,
        group: AlphaGroup,
        node_id: str,
    ) -> dict[str, Any]:
        """Generate Qlib experiment files for a single AlphaGroup.

        Reuses the existing compose_experiment_in_memory() for each group.

        多Alpha统一回测架构：
        - 所有组（无论主节点/从节点）都使用 train_only=True
        - 只训练模型 + 生成 pred.pkl，跳过回测
        - 主节点收集所有 pred.pkl 后执行 meta_model_runner.py 合并
        - 最后由主节点用 combined_prediction 做一次统一回测
        """
        if not self.composer:
            raise RuntimeError(
                f"MultiAlphaEngine requires composer to build group {group.group_name}"
            )

        # Build a single-alpha ExperimentConfig for this group
        custom_params = self.config.build_custom_params()
        if group.model_params:
            custom_params.update(group.model_params)

        # 多Alpha默认禁用Alpha158（除非实验配置中显式启用）
        if "disable_alpha158" not in custom_params:
            custom_params["disable_alpha158"] = True

        # 统一回测模式：所有组 train_only=True，不传 execution_algo（从节点不需要v24）
        result = self.composer.compose_experiment_in_memory(
            factor_names=group.factor_names,
            model_id=group.model_id,
            strategy_id=self.config.strategy_id,
            data_split=self.config.data_split,
            custom_params=custom_params,
            experiment_name=f"{self.config.experiment_name or 'malpha'}_{group.group_name}",
            skip_db_save=True,
            execution_algo=None,  # train-only 不需要执行策略
            execution_algo_params=None,
            strategy_params=self.config.build_strategy_params(),
            node_id=node_id,
            train_only=True,
        )
        return result

    def _add_root_backtest_files(
        self,
        all_experiment_files: dict[str, str],
        group_configs: list[dict],
    ) -> None:
        """Add unified-backtest files to the loop root.

        Group experiments are train-only and may not include portfolio strategy
        dependencies.  Build one authoritative full backtest bundle for the
        root instead of copying dependencies from a train-only group.
        """
        unified_files = self._generate_unified_backtest_files(group_configs)
        conf_yaml = unified_files.get("conf.yaml")
        if not conf_yaml:
            raise RuntimeError(
                "Unified backtest bundle missing conf.yaml; cannot run pred-backtest"
            )

        required_root_files = ["qrun_limit_minute.py", "read_exp_res.py"]
        for fname in required_root_files:
            if fname not in unified_files:
                raise RuntimeError(
                    f"Unified backtest dependency missing: {fname}. "
                    "compose_experiment_in_memory did not generate it."
                )

        # Copy the whole generated root bundle. This preserves any factors,
        # loaders, strategy modules, and .b64 binary payloads referenced by
        # conf.yaml without fabricating missing dependencies.
        for fname, content in unified_files.items():
            all_experiment_files[fname] = content

        referenced_modules = {
            "custom_strategy.py": ["custom_strategy"],
            "tail_twap_strategy.py": ["tail_twap_strategy"],
            "tail_twap_v24_strategy.py": ["tail_twap_v24_strategy"],
            "qe_custom_loaders.py": ["qe_custom_loaders"],
        }
        for fname, needles in referenced_modules.items():
            if any(needle in conf_yaml for needle in needles) and fname not in unified_files:
                raise RuntimeError(
                    f"Unified backtest conf.yaml references {fname}, but the file was not generated"
                )

        all_experiment_files["conf.yaml"] = conf_yaml

    def _generate_unified_backtest_files(self, group_configs: list[dict]) -> dict[str, str]:
        """Generate the authoritative root file bundle for pred-backtest."""
        if not self.composer:
            raise RuntimeError("MultiAlphaEngine requires composer to build unified backtest files")
        if not group_configs:
            raise RuntimeError("Unified backtest requires at least one group config")

        first_group = group_configs[0]["group_name"]
        first_group_obj = next(
            (g for g in self.ma_config.alpha_groups if g.group_name == first_group), None
        )
        if first_group_obj is None:
            raise RuntimeError(
                f"Unified backtest cannot find first group in multi_alpha_config: {first_group}"
            )

        custom_params = self.config.build_custom_params()
        if "disable_alpha158" not in custom_params:
            custom_params["disable_alpha158"] = True

        result = self.composer.compose_experiment_in_memory(
            factor_names=first_group_obj.factor_names,
            model_id=first_group_obj.model_id,
            strategy_id=self.config.strategy_id,
            data_split=self.config.data_split,
            custom_params=custom_params,
            experiment_name=f"{self.config.experiment_name or 'malpha'}_unified_backtest",
            skip_db_save=True,
            execution_algo=self.config.execution_algo,
            execution_algo_params=self.config.execution_algo_params,
            strategy_params=self.config.build_strategy_params(),
            node_id=None,
            train_only=False,
        )
        files = result.get("experiment_files") or {}
        if not isinstance(files, dict) or not files:
            raise RuntimeError(
                "Unified backtest file generation failed: compose_experiment_in_memory returned no files"
            )
        return files

    def _generate_meta_runner_script(self, group_configs: list[dict]) -> str:
        """Generate the meta_model_runner.py script that combines group predictions."""
        method = self.ma_config.meta_model.method
        lookback = self.ma_config.meta_model.lookback_days

        group_names = [g["group_name"] for g in group_configs]
        group_prediction_paths = {
            g["group_name"]: g.get("prediction_path") for g in group_configs if g.get("prediction_path")
        }

        script = f'''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Multi-Alpha Meta-Model Runner
Auto-generated — combines predictions from {len(group_names)} alpha groups.
Method: {method} (lookback={lookback} days)

产出文件:
  - combined_prediction.pkl  : 合并后的预测信号
  - meta_weights.json        : 各组权重
  - multi_alpha_results.json : 完整结果（组级IC/ICIR/Sharpe + 相关性 + 权重）
                               供 MultiAlphaResultCollector 读取写入DB
"""
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

GROUP_NAMES = {group_names!r}
GROUP_PREDICTION_PATHS = {group_prediction_paths!r}
META_METHOD = "{method}"
LOOKBACK = {lookback}
IC_QUALITY = {{}}


def _resolve_prediction_path(path_value):
    if not path_value:
        raise RuntimeError("Missing prediction_path for reuse group")
    p = Path(path_value)
    if p.exists():
        return p
    normalized = str(path_value).replace("\\\\", "/")
    marker = "/qe_workspace/"
    marker_idx = normalized.find(marker)
    if marker_idx >= 0:
        relative = normalized[marker_idx + len(marker):].lstrip("/")
        candidate = Path(relative)
        if candidate.exists():
            return candidate
    raise RuntimeError(f"Prediction path does not exist: {{path_value}}")


def load_predictions():
    """Load prediction pkl from each group's experiment output.

    Search order per group:
    1. group_xxx/output/pred.pkl (full mode)
    2. GROUP_PREDICTION_PATHS[g_name] (reuse mode)
    3. group_xxx/mlruns/**/artifacts/pred.pkl (train-only mode)
    """
    import glob
    preds = {{}}
    missing_groups = []
    for g_name in GROUP_NAMES:
        pred_path = Path(f"group_{{g_name}}/output/pred.pkl")
        if pred_path.exists():
            with open(pred_path, "rb") as f:
                preds[g_name] = pickle.load(f)
            print(f"Loaded {{g_name}} from {{pred_path}}: {{len(preds[g_name])}} rows")
            continue

        # reuse 组：从 prediction_path 加载
        reuse_path = GROUP_PREDICTION_PATHS.get(g_name)
        if reuse_path:
            pred_path = _resolve_prediction_path(reuse_path)
            with open(pred_path, "rb") as f:
                preds[g_name] = pickle.load(f)
            print(f"Loaded {{g_name}} from {{pred_path}} (reuse): {{len(preds[g_name])}} rows")
            continue

        # train-only 模式：从 mlruns artifacts 查找
        pattern = f"group_{{g_name}}/mlruns/**/artifacts/pred.pkl"
        matches = glob.glob(pattern, recursive=True)
        if matches:
            pred_path = Path(matches[-1])  # 取最新的
            with open(pred_path, "rb") as f:
                preds[g_name] = pickle.load(f)
            print(f"Loaded {{g_name}} from {{pred_path}} (mlruns): {{len(preds[g_name])}} rows")
            continue

        missing_groups.append(g_name)

    if missing_groups:
        raise RuntimeError(
            f"Missing prediction files for groups: {{missing_groups}}. "
            f"Checked: output/pred.pkl, GROUP_PREDICTION_PATHS, mlruns/**/artifacts/pred.pkl"
        )
    return preds


def load_label():
    """Load actual returns (label) for IC computation.

    Search order:
    1. group_xxx/output/label.pkl (full mode)
    2. group_xxx/mlruns/**/artifacts/label.pkl (train-only mode)
    """
    import glob
    for g_name in GROUP_NAMES:
        # 优先检查 output 目录
        label_path = Path(f"group_{{g_name}}/output/label.pkl")
        if label_path.exists():
            with open(label_path, "rb") as f:
                label = pickle.load(f)
            print(f"Loaded label from {{label_path}} ({{len(label)}} rows)")
            if isinstance(label, pd.DataFrame):
                return label.iloc[:, 0]
            return label
        # fallback: 从 mlruns artifacts 查找（train-only 模式）
        pattern = f"group_{{g_name}}/mlruns/**/artifacts/label.pkl"
        matches = glob.glob(pattern, recursive=True)
        if matches:
            label_path = Path(matches[-1])  # 取最新的
            with open(label_path, "rb") as f:
                label = pickle.load(f)
            print(f"Loaded label from {{label_path}} ({{len(label)}} rows)")
            if isinstance(label, pd.DataFrame):
                return label.iloc[:, 0]
            return label
    raise RuntimeError(
        "No label.pkl found for any group. "
        "Checked: group_xxx/output/label.pkl and group_xxx/mlruns/**/artifacts/label.pkl"
    )


def compute_daily_ic_series(pred_s, label_s, dates, context, min_daily_samples=10):
    """Compute daily IC after dropping invalid rows; persist skipped-day diagnostics."""
    daily_ics = []
    skipped = []
    for dt in dates:
        if isinstance(pred_s.index, pd.MultiIndex):
            p_day = pred_s.xs(dt, level=0)
            r_day = label_s.xs(dt, level=0)
        else:
            p_day = pred_s
            r_day = label_s

        aligned = pd.concat(
            [p_day.rename("prediction"), r_day.rename("label")],
            axis=1,
        ).dropna()
        raw_samples = int(len(p_day))
        valid_samples = int(len(aligned))

        reason = None
        if valid_samples < min_daily_samples:
            reason = "insufficient_valid_samples_after_dropna"
        elif aligned["prediction"].nunique(dropna=True) < 2:
            reason = "constant_prediction"
        elif aligned["label"].nunique(dropna=True) < 2:
            reason = "constant_or_all_nan_label"

        if reason:
            skipped.append({{
                "date": str(dt),
                "reason": reason,
                "raw_samples": raw_samples,
                "valid_samples": valid_samples,
            }})
            continue

        ic = aligned["prediction"].corr(aligned["label"], method="spearman")
        if np.isnan(ic):
            skipped.append({{
                "date": str(dt),
                "reason": "nan_spearman_ic",
                "raw_samples": raw_samples,
                "valid_samples": valid_samples,
            }})
            continue
        daily_ics.append(float(ic))

    total_days = len(dates)
    required_valid_days = min(5, max(1, int(total_days * 0.5)))
    IC_QUALITY[context] = {{
        "total_days": int(total_days),
        "valid_days": int(len(daily_ics)),
        "skipped_days": int(len(skipped)),
        "skipped_samples": skipped[:20],
    }}
    if skipped:
        print(
            f"[WARN] {{context}} skipped {{len(skipped)}} invalid daily IC days "
            f"after dropping NaN/constant rows; first={{skipped[:5]}}"
        )
    if len(daily_ics) < required_valid_days:
        raise RuntimeError(
            f"{{context}} produced only {{len(daily_ics)}} valid daily IC days "
            f"out of {{total_days}}; required={{required_valid_days}}; "
            f"skipped={{skipped[:10]}}"
        )
    return daily_ics


def combine(preds, actual_returns=None):
    """Combine predictions using IC-weighted/equal method (standalone, no external imports)."""
    group_names = list(preds.keys())
    if len(group_names) < 2:
        raise RuntimeError(f"Need at least 2 prediction groups, got {{len(group_names)}}")

    if META_METHOD == "ic_weighted":
        if actual_returns is None:
            raise RuntimeError("IC-weighted combination requires label data")
        weights = {{}}
        for g_name, g_pred in preds.items():
            pred_s = g_pred["score"] if isinstance(g_pred, pd.DataFrame) and "score" in g_pred.columns else (g_pred.iloc[:, 0] if isinstance(g_pred, pd.DataFrame) else g_pred)
            common = pred_s.index.intersection(actual_returns.index)
            if len(common) < 30:
                raise RuntimeError(f"Group {{g_name}} has insufficient overlap with label: {{len(common)}}")
            dates = sorted(set(idx[0] if isinstance(idx, tuple) else idx for idx in common))
            if len(dates) > LOOKBACK:
                cutoff = dates[-LOOKBACK]
                common = [idx for idx in common if (idx[0] if isinstance(idx, tuple) else idx) >= cutoff]
            daily_ics = compute_daily_ic_series(
                pred_s,
                actual_returns,
                sorted(set(idx[0] if isinstance(idx, tuple) else idx for idx in common)),
                f"weight:{{g_name}}",
            )
            mean_ic = float(np.mean(daily_ics))
            if mean_ic <= 0:
                raise RuntimeError(f"Group {{g_name}} produced non-positive IC mean: {{mean_ic}}")
            weights[g_name] = mean_ic
        total = sum(weights.values())
        if total <= 0:
            raise RuntimeError(f"Invalid meta weights total: {{total}}")
        weights = {{k: v / total for k, v in weights.items()}}
    elif META_METHOD == "equal":
        weights = {{g: 1.0 / len(group_names) for g in group_names}}
    else:
        raise RuntimeError(f"Unsupported meta method: {{META_METHOD}}")

    combined = None
    for g_name, g_pred in preds.items():
        pred_s = g_pred["score"] if isinstance(g_pred, pd.DataFrame) and "score" in g_pred.columns else (g_pred.iloc[:, 0] if isinstance(g_pred, pd.DataFrame) else g_pred)
        w = weights[g_name]
        weighted = pred_s * w
        if combined is None:
            combined = weighted
        else:
            combined = combined.add(weighted, fill_value=0.0)

    if combined is None:
        raise RuntimeError("Combined prediction is empty")

    if not isinstance(combined, pd.DataFrame):
        combined = combined.to_frame("score")

    print(f"Meta weights: {{weights}}")
    return combined, weights


def compute_group_metrics(preds, label):
    """Compute per-group IC, ICIR, and estimated Sharpe."""
    results = {{}}
    for g_name, pred_df in preds.items():
        if isinstance(pred_df, pd.DataFrame):
            pred_s = pred_df["score"] if "score" in pred_df.columns else pred_df.iloc[:, 0]
        else:
            pred_s = pred_df

        common_idx = pred_s.index.intersection(label.index)
        if len(common_idx) < 50:
            raise RuntimeError(f"Group {{g_name}} has insufficient samples for metrics: {{len(common_idx)}}")
        p = pred_s.loc[common_idx]
        r = label.loc[common_idx]

        dates = sorted(set(
            idx[0] if isinstance(idx, tuple) else idx for idx in common_idx
        ))
        daily_ics = compute_daily_ic_series(p, r, dates, f"group:{{g_name}}")
        avg_ic = float(np.mean(daily_ics))
        std_ic = float(np.std(daily_ics))
        gm = {{
            "ic": round(avg_ic, 6),
            "icir": round(avg_ic / std_ic, 4) if std_ic > 1e-8 else None,
            "sharpe": round((avg_ic / std_ic) * np.sqrt(252) / np.sqrt(len(daily_ics)), 4) if std_ic > 1e-8 else None,
        }}
        if gm["icir"] is None or gm["sharpe"] is None:
            raise RuntimeError(f"Group {{g_name}} produced degenerate ICIR/Sharpe")

        results[g_name] = gm
        print(f"  {{g_name}}: IC={{gm['ic']}}, ICIR={{gm['icir']}}, Sharpe={{gm['sharpe']}}")

    return results


def compute_correlations(preds):
    """Compute pairwise Spearman correlation between group predictions."""
    corrs = {{}}
    names = list(preds.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            g_a, g_b = names[i], names[j]
            pred_a = preds[g_a]
            pred_b = preds[g_b]

            if isinstance(pred_a, pd.DataFrame):
                pred_a = pred_a["score"] if "score" in pred_a.columns else pred_a.iloc[:, 0]
            if isinstance(pred_b, pd.DataFrame):
                pred_b = pred_b["score"] if "score" in pred_b.columns else pred_b.iloc[:, 0]

            common = pred_a.index.intersection(pred_b.index)
            if len(common) < 30:
                raise RuntimeError(f"Insufficient overlap for correlation: {{g_a}} vs {{g_b}} => {{len(common)}}")
            corr = pred_a.loc[common].corr(pred_b.loc[common], method="spearman")
            if np.isnan(corr):
                raise RuntimeError(f"NaN correlation for groups {{g_a}} and {{g_b}}")
            corrs[f"{{g_a}}|{{g_b}}"] = round(float(corr), 4)

    return corrs


def prepare_unified_backtest_env():
    """Prepare root features required by the unified pred-backtest dataset."""
    import os
    import subprocess

    prepare_script = Path("prepare_factors.py")
    if not prepare_script.exists():
        raise RuntimeError("Unified backtest requires prepare_factors.py in the loop root")

    print("\\n=== Preparing unified backtest dataset ===")
    prep_result = subprocess.run([sys.executable, str(prepare_script)])
    if prep_result.returncode != 0:
        raise RuntimeError("prepare_factors.py FAILED (exit code %s). Cannot run unified backtest." % prep_result.returncode)

    if not Path("combined_factors_df.parquet").exists():
        raise RuntimeError("prepare_factors.py completed but combined_factors_df.parquet is missing")

    env = os.environ.copy()
    factor_env = Path(".factor_env")
    if factor_env.exists():
        for raw_line in factor_env.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("'").strip('"')
    print("[OK] Unified backtest dataset prepared")
    return env


def main():
    preds = load_predictions()
    if len(preds) < 2:
        raise RuntimeError(f"Need at least 2 groups with predictions, got {{len(preds)}}")

    label = load_label()

    combined, weights = combine(preds, label)

    out_path = Path("combined_prediction.pkl")
    with open(out_path, "wb") as f:
        pickle.dump(combined, f)
    print(f"Combined prediction saved to {{out_path}} ({{len(combined)}} rows)")

    with open("meta_weights.json", "w") as f:
        json.dump(weights, f, indent=2)

    print("\\nComputing per-group metrics...")
    group_metrics = compute_group_metrics(preds, label)

    print("\\nComputing inter-group correlations...")
    correlations = compute_correlations(preds)
    print(f"Correlations: {{correlations}}")

    if isinstance(combined, pd.DataFrame):
        combined_s = combined["score"] if "score" in combined.columns else combined.iloc[:, 0]
    else:
        combined_s = combined
    common = combined_s.index.intersection(label.index)
    if len(common) < 50:
        raise RuntimeError(f"Combined prediction has insufficient label overlap: {{len(common)}}")
    dates = sorted(set(
        idx[0] if isinstance(idx, tuple) else idx for idx in common
    ))
    daily_ics = compute_daily_ic_series(combined_s, label, dates, "combined")
    combined_ic = round(float(np.mean(daily_ics)), 6)
    print(f"\\nCombined IC: {{combined_ic}}")

    ma_results = {{
        "group_metrics": group_metrics,
        "meta_weights": weights,
        "correlations": correlations,
        "combined_ic": combined_ic,
        "meta_method": META_METHOD,
        "total_groups": len(preds),
        "group_names": list(preds.keys()),
        "ic_quality": IC_QUALITY,
    }}
    with open("multi_alpha_results.json", "w") as f:
        json.dump(ma_results, f, indent=2, default=str)
    print(f"\\nResults saved to multi_alpha_results.json")

    # ── 统一回测：用 combined prediction 执行完整选股+分钟线回测 ──
    print("\\n=== Running unified backtest on combined prediction ===")
    import subprocess
    bt_cmd = [sys.executable, "qrun_limit_minute.py", "conf.yaml",
              "--pred-backtest", "combined_prediction.pkl"]
    print(f"Command: {{' '.join(bt_cmd)}}")
    bt_env = prepare_unified_backtest_env()
    bt_result = subprocess.run(bt_cmd, env=bt_env)
    if bt_result.returncode != 0:
        raise RuntimeError(
            f"Unified backtest FAILED (exit code {{bt_result.returncode}}). "
            f"Cannot proceed without backtest results."
        )
    print("[OK] Unified backtest completed")

    # 提取增强指标（read_exp_res.py 从 mlruns 提取完整回测指标）
    print("\\n=== Extracting enhanced metrics ===")
    res_cmd = [sys.executable, "read_exp_res.py"]
    res_env = os.environ.copy()
    res_env["QE_REQUIRE_RECORDER_ID"] = "1"
    res_result = subprocess.run(res_cmd, env=res_env)
    if res_result.returncode != 0:
        raise RuntimeError(
            f"read_exp_res.py FAILED (exit code {{res_result.returncode}}). "
            f"Enhanced metrics extraction failed."
        )
    print("[DONE] Multi-alpha unified backtest pipeline completed")


if __name__ == "__main__":
    main()
'''
        return script

    def _load_source_group(
        self,
        source_experiment_id: str,
        source_group_name: str,
    ) -> dict[str, Any] | None:
        """Load a completed group record from qe_multi_alpha_groups for reuse.

        Returns None if source not found (正常场景).
        Raises on DB error (不静默吞噬).
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT group_name, prediction_path, group_ic, group_icir,
                              group_sharpe, model_id, status
                       FROM qe_multi_alpha_groups
                       WHERE parent_experiment_id = %s
                         AND group_name = %s
                         AND status = 'completed'
                    """,
                    (source_experiment_id, source_group_name),
                )
                row = cur.fetchone()
                if row:
                    return {
                        "group_name": row[0],
                        "prediction_path": row[1],
                        "group_ic": row[2],
                        "group_icir": row[3],
                        "group_sharpe": row[4],
                        "model_id": row[5],
                        "status": row[6],
                    }
        return None

    def _store_group_records(
        self,
        parent_experiment_id: str,
        assignments: list[GroupAssignment],
    ) -> None:
        """Store group records to qe_multi_alpha_groups table.

        失败时抛出异常，不静默吞噬。组记录丢失会导致 ResultCollector 无法工作。
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                for a in assignments:
                    g = a.group
                    cur.execute("""
                        INSERT INTO qe_multi_alpha_groups
                            (parent_experiment_id, group_name, factor_names,
                             model_id, dataset_type, model_params,
                             compute_resource, assigned_node_id, qe_loop_id, status,
                             model_source_experiment_id, model_source_group_name,
                             reuse_mode)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending',
                                %s, %s, %s)
                        ON CONFLICT (parent_experiment_id, group_name)
                        DO UPDATE SET
                            factor_names = EXCLUDED.factor_names,
                            model_id = EXCLUDED.model_id,
                            assigned_node_id = EXCLUDED.assigned_node_id,
                            qe_loop_id = EXCLUDED.qe_loop_id,
                            model_source_experiment_id = EXCLUDED.model_source_experiment_id,
                            model_source_group_name = EXCLUDED.model_source_group_name,
                            reuse_mode = EXCLUDED.reuse_mode,
                            status = 'pending'
                    """, (
                        parent_experiment_id,
                        g.group_name,
                        json.dumps(g.factor_names),
                        g.model_id,
                        g.dataset_type,
                        json.dumps(g.model_params) if g.model_params else None,
                        g.compute_resource,
                        a.node_id,
                        None,
                        g.model_source_experiment_id,
                        g.model_source_group_name,
                        g.reuse_mode or "retrain",
                    ))
            conn.commit()
