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

            if reuse_mode == "reuse_prediction" and group.model_source_experiment_id:
                # 复用已有预测：跳过训练和预测，直接引用源实验的 prediction_path
                source_info = self._load_source_group(
                    group.model_source_experiment_id,
                    group.model_source_group_name or group.group_name,
                )
                if source_info and source_info.get("prediction_path"):
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
                    })
                    # Write a marker file instead of full experiment
                    all_experiment_files[f"group_{group.group_name}/REUSE_PREDICTION.txt"] = (
                        f"source_experiment_id: {group.model_source_experiment_id}\n"
                        f"source_group_name: {source_info['group_name']}\n"
                        f"prediction_path: {source_info['prediction_path']}\n"
                    )
                    continue
                else:
                    logger.warning(
                        f"Group {group.group_name}: reuse_prediction requested but "
                        f"source not found, falling back to retrain"
                    )

            elif reuse_mode == "reuse_model" and group.model_source_experiment_id:
                # 复用模型：跳过训练，只重新预测
                # 注入 backtest_only 标记，让 composer 跳过训练
                source_info = self._load_source_group(
                    group.model_source_experiment_id,
                    group.model_source_group_name or group.group_name,
                )
                if source_info and source_info.get("prediction_path"):
                    reuse_summary.append(
                        f"  {group.group_name}: reuse_model from "
                        f"{group.model_source_experiment_id}/{source_info['group_name']}"
                    )
                    # NOTE: reuse_model 目前降级为 reuse_prediction，
                    # 因为 Qlib composer 尚不支持 "加载模型 pkl → 只预测" 模式。
                    # 完整 reuse_model 需要 Qlib workflow 层支持 model_path 注入。
                    group_configs.append({
                        "group_name": group.group_name,
                        "node_id": assignment.node_id,
                        "order": assignment.order,
                        "factor_count": len(group.factor_names),
                        "model_id": group.model_id,
                        "wsl_command": f"# REUSED model from {group.model_source_experiment_id}",
                        "reuse_mode": "reuse_model",
                        "source_prediction_path": source_info["prediction_path"],
                    })
                    all_experiment_files[f"group_{group.group_name}/REUSE_MODEL.txt"] = (
                        f"source_experiment_id: {group.model_source_experiment_id}\n"
                        f"source_group_name: {source_info['group_name']}\n"
                        f"prediction_path: {source_info['prediction_path']}\n"
                        f"note: reuse_model currently uses reuse_prediction (Qlib limitation)\n"
                    )
                    continue
                else:
                    logger.warning(
                        f"Group {group.group_name}: reuse_model requested but "
                        f"source not found, falling back to retrain"
                    )

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
        """
        if not self.composer:
            # Without a composer, return placeholder
            return {
                "experiment_files": {
                    "conf.yaml": f"# Placeholder for group {group.group_name}\n"
                                 f"# model: {group.model_id}\n"
                                 f"# factors: {group.factor_names}\n"
                                 f"# dataset: {group.dataset_type}\n",
                },
                "wsl_command": f"# Run group {group.group_name}",
            }

        # Build a single-alpha ExperimentConfig for this group
        custom_params = self.config.build_custom_params()
        if group.model_params:
            custom_params.update(group.model_params)

        try:
            result = self.composer.compose_experiment_in_memory(
                factor_names=group.factor_names,
                model_id=group.model_id,
                strategy_id=self.config.strategy_id,
                data_split=self.config.data_split,
                custom_params=custom_params,
                experiment_name=f"{self.config.experiment_name or 'malpha'}_{group.group_name}",
                skip_db_save=True,
                execution_algo=self.config.execution_algo,
                execution_algo_params=self.config.execution_algo_params,
                strategy_params=self.config.build_strategy_params(),
                node_id=node_id,
            )
            return result
        except Exception as e:
            logger.error(f"Failed to compose group {group.group_name}: {e}")
            return {
                "experiment_files": {"error.txt": str(e)},
                "wsl_command": f"# ERROR: {e}",
            }

    def _generate_meta_runner_script(self, group_configs: list[dict]) -> str:
        """Generate the meta_model_runner.py script that combines group predictions."""
        method = self.ma_config.meta_model.method
        lookback = self.ma_config.meta_model.lookback_days

        group_names = [g["group_name"] for g in group_configs]

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
META_METHOD = "{method}"
LOOKBACK = {lookback}


def load_predictions():
    """Load prediction pkl from each group's experiment output."""
    preds = {{}}
    for g_name in GROUP_NAMES:
        pred_path = Path(f"group_{{g_name}}/output/pred.pkl")
        if not pred_path.exists():
            print(f"WARNING: {{pred_path}} not found, skipping group {{g_name}}")
            continue
        with open(pred_path, "rb") as f:
            preds[g_name] = pickle.load(f)
        print(f"Loaded {{g_name}}: {{len(preds[g_name])}} rows")
    return preds


def load_label():
    """Load actual returns (label) for IC computation.

    Tries the first group's label.pkl, then falls back to any group.
    """
    for g_name in GROUP_NAMES:
        label_path = Path(f"group_{{g_name}}/output/label.pkl")
        if label_path.exists():
            with open(label_path, "rb") as f:
                label = pickle.load(f)
            print(f"Loaded label from {{label_path}} ({{len(label)}} rows)")
            if isinstance(label, pd.DataFrame):
                return label.iloc[:, 0]
            return label
    print("WARNING: No label.pkl found, IC computation will be skipped")
    return None


def combine(preds, actual_returns=None):
    """Combine predictions using IC-weighted/equal method (standalone, no external imports)."""
    group_names = list(preds.keys())

    if META_METHOD == "ic_weighted" and actual_returns is not None:
        # IC-weighted: compute rolling Rank IC for each group
        weights = {{}}
        for g_name, g_pred in preds.items():
            pred_s = g_pred["score"] if isinstance(g_pred, pd.DataFrame) and "score" in g_pred.columns else (g_pred.iloc[:, 0] if isinstance(g_pred, pd.DataFrame) else g_pred)
            common = pred_s.index.intersection(actual_returns.index)
            if len(common) < 30:
                weights[g_name] = 0.0
                continue
            dates = sorted(set(idx[0] if isinstance(idx, tuple) else idx for idx in common))
            if len(dates) > LOOKBACK:
                cutoff = dates[-LOOKBACK]
                common = [idx for idx in common if (idx[0] if isinstance(idx, tuple) else idx) >= cutoff]
            daily_ics = []
            for dt in sorted(set(idx[0] if isinstance(idx, tuple) else idx for idx in common)):
                try:
                    p_day = pred_s.xs(dt, level=0) if isinstance(pred_s.index, pd.MultiIndex) else pred_s
                    r_day = actual_returns.xs(dt, level=0) if isinstance(actual_returns.index, pd.MultiIndex) else actual_returns
                    if len(p_day) >= 10:
                        ic = p_day.corr(r_day, method="spearman")
                        if not np.isnan(ic):
                            daily_ics.append(ic)
                except Exception:
                    continue
            weights[g_name] = max(float(np.mean(daily_ics)), 0.0) if daily_ics else 0.0
        total = sum(weights.values())
        if total > 0:
            weights = {{k: v / total for k, v in weights.items()}}
        else:
            weights = {{g: 1.0 / len(group_names) for g in group_names}}
    else:
        # Equal weight (default / fallback)
        weights = {{g: 1.0 / len(group_names) for g in group_names}}

    # Weighted sum
    combined = None
    for g_name, g_pred in preds.items():
        pred_s = g_pred["score"] if isinstance(g_pred, pd.DataFrame) and "score" in g_pred.columns else (g_pred.iloc[:, 0] if isinstance(g_pred, pd.DataFrame) else g_pred)
        w = weights.get(g_name, 0.0)
        if w == 0.0:
            continue
        weighted = pred_s * w
        if combined is None:
            combined = weighted
        else:
            combined = combined.add(weighted, fill_value=0.0)

    if combined is None:
        first = next(iter(preds.values()))
        combined = first["score"] if isinstance(first, pd.DataFrame) and "score" in first.columns else (first.iloc[:, 0] if isinstance(first, pd.DataFrame) else first)

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

        gm = {{"ic": None, "icir": None, "sharpe": None}}

        if label is not None:
            common_idx = pred_s.index.intersection(label.index)
            if len(common_idx) >= 50:
                p = pred_s.loc[common_idx]
                r = label.loc[common_idx]

                # Daily Rank IC
                daily_ics = []
                dates = sorted(set(
                    idx[0] if isinstance(idx, tuple) else idx for idx in common_idx
                ))
                for dt in dates:
                    try:
                        if isinstance(p.index, pd.MultiIndex):
                            p_day = p.xs(dt, level=0)
                            r_day = r.xs(dt, level=0)
                        else:
                            p_day = p
                            r_day = r
                        if len(p_day) >= 10:
                            ic = p_day.corr(r_day, method="spearman")
                            if not np.isnan(ic):
                                daily_ics.append(ic)
                    except Exception:
                        continue

                if daily_ics:
                    avg_ic = float(np.mean(daily_ics))
                    std_ic = float(np.std(daily_ics))
                    gm["ic"] = round(avg_ic, 6)
                    gm["icir"] = round(avg_ic / std_ic, 4) if std_ic > 1e-8 else None
                    # Sharpe 估算: ICIR * sqrt(252)
                    if gm["icir"] is not None:
                        gm["sharpe"] = round(gm["icir"] * np.sqrt(252) / np.sqrt(len(dates)), 4)

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
            if len(common) >= 30:
                corr = pred_a.loc[common].corr(pred_b.loc[common], method="spearman")
                if not np.isnan(corr):
                    corrs[f"{{g_a}}|{{g_b}}"] = round(float(corr), 4)

    return corrs


def main():
    preds = load_predictions()
    if len(preds) < 2:
        print("ERROR: Need at least 2 groups with predictions")
        sys.exit(1)

    # Load label first for IC-weighted combination
    label = load_label()

    # Combine
    combined, weights = combine(preds, label)

    # Save combined prediction
    out_path = Path("combined_prediction.pkl")
    with open(out_path, "wb") as f:
        pickle.dump(combined, f)
    print(f"Combined prediction saved to {{out_path}} ({{len(combined)}} rows)")

    # Save weights
    with open("meta_weights.json", "w") as f:
        json.dump(weights, f, indent=2)

    # Compute per-group metrics
    print("\\nComputing per-group metrics...")
    group_metrics = compute_group_metrics(preds, label)

    # Compute inter-group correlations
    print("\\nComputing inter-group correlations...")
    correlations = compute_correlations(preds)
    print(f"Correlations: {{correlations}}")

    # Compute combined IC
    combined_ic = None
    if label is not None:
        if isinstance(combined, pd.DataFrame):
            combined_s = combined["score"] if "score" in combined.columns else combined.iloc[:, 0]
        else:
            combined_s = combined
        common = combined_s.index.intersection(label.index)
        if len(common) >= 50:
            daily_ics = []
            dates = sorted(set(
                idx[0] if isinstance(idx, tuple) else idx for idx in common
            ))
            for dt in dates:
                try:
                    if isinstance(combined_s.index, pd.MultiIndex):
                        c_day = combined_s.xs(dt, level=0)
                        r_day = label.xs(dt, level=0)
                    else:
                        c_day = combined_s
                        r_day = label
                    if len(c_day) >= 10:
                        ic = c_day.corr(r_day, method="spearman")
                        if not np.isnan(ic):
                            daily_ics.append(ic)
                except Exception:
                    continue
            if daily_ics:
                combined_ic = round(float(np.mean(daily_ics)), 6)
    print(f"\\nCombined IC: {{combined_ic}}")

    # Save comprehensive results for MultiAlphaResultCollector
    ma_results = {{
        "group_metrics": group_metrics,
        "meta_weights": weights,
        "correlations": correlations,
        "combined_ic": combined_ic,
        "meta_method": META_METHOD,
        "total_groups": len(preds),
        "group_names": list(preds.keys()),
    }}
    with open("multi_alpha_results.json", "w") as f:
        json.dump(ma_results, f, indent=2, default=str)
    print(f"\\nResults saved to multi_alpha_results.json")


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
                             compute_resource, assigned_node_id, status,
                             model_source_experiment_id, model_source_group_name,
                             reuse_mode)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending',
                                %s, %s, %s)
                        ON CONFLICT (parent_experiment_id, group_name)
                        DO UPDATE SET
                            factor_names = EXCLUDED.factor_names,
                            model_id = EXCLUDED.model_id,
                            assigned_node_id = EXCLUDED.assigned_node_id,
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
                        g.model_source_experiment_id,
                        g.model_source_group_name,
                        g.reuse_mode or "retrain",
                    ))
            conn.commit()
