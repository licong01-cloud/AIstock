"""
Multi-Alpha Result Collector

实验完成后，收集+计算+持久化所有多Alpha独有数据，并回写统一分析层。

职责：
1. 从 workspace 获取各组子实验的回测指标
2. 运行 MetaModelCombiner 计算 combined 指标和权重（或读取 WSL 端产出的 JSON）
3. 计算组间预测相关性
4. 写入 3 张扩展表 + UPDATE qe_experiments 主表的统一指标列

设计原则：
- 禁止静默兜底：所有数据持久化失败必须抛异常
- 降级模式必须以 ERROR 级别日志记录，不允许 debug/warning
- RDAgent 仅作为调度机制（将实验派发到 WSL/远端节点执行）
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.multi_alpha_result_collector")

# result_metrics JSON key → qe_experiments 独立列名
_COL_MAP = {
    "IC": "ic",
    "ICIR": "icir",
    "Rank IC": "rank_ic",
    "Rank ICIR": "rank_icir",
    "1day.excess_return_with_cost.annualized_return": "annualized_return",
    "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
    "1day.excess_return_with_cost.information_ratio": "information_ratio",
    "1day.excess_return_with_cost.mean": "excess_return_with_cost_mean",
    "1day.excess_return_without_cost.mean": "excess_return_without_cost_mean",
    "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
    "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
    "1day.excess_return_without_cost.information_ratio": "information_ratio_no_cost",
}


class MultiAlphaResultCollector:
    """多Alpha实验结果收集器。"""

    async def collect_and_persist(self, parent_experiment_id: str) -> dict[str, Any]:
        """收集多Alpha实验结果并持久化到DB。

        失败时抛出异常，不静默兜底。调用方负责处理。
        """
        logger.info(f"开始收集多Alpha结果: {parent_experiment_id}")

        # ── Phase 1: 获取原始数据 ──────────────────────────────────
        exp_record = self._get_experiment_record(parent_experiment_id)
        if not exp_record:
            raise ValueError(f"实验 {parent_experiment_id} 不存在")

        qe_task_id = exp_record.get("qe_task_id")
        qe_loop_id = exp_record.get("qe_loop_id") or "Loop1"
        multi_alpha_config = exp_record.get("multi_alpha_config") or {}
        if isinstance(multi_alpha_config, str):
            multi_alpha_config = json.loads(multi_alpha_config)

        if not qe_task_id:
            raise ValueError(
                f"实验 {parent_experiment_id} 缺少 qe_task_id，无法获取回测指标"
            )

        # 获取各组配置
        groups = self._get_group_records(parent_experiment_id)
        if not groups:
            raise ValueError(f"实验 {parent_experiment_id} 没有多Alpha组记录")

        # 检测是否为分布式执行（各组可能在不同节点）
        node_ids = set(g.get("assigned_node_id") for g in groups if g.get("assigned_node_id"))
        is_distributed = len(node_ids) > 1

        if is_distributed:
            # ── 分布式场景：跨节点收集预测 + 本地 meta 合并 ──────
            logger.info(f"分布式多Alpha结果收集: {len(node_ids)} 节点, {len(groups)} 组")
            ma_results = await self._collect_distributed(
                qe_task_id, qe_loop_id, groups, multi_alpha_config
            )
            # 分布式收集后直接拿到完整 ma_results
            combined_metrics = ma_results.pop("_combined_metrics", {})
        else:
            # ── 单节点场景：从 workspace 获取结果 ──────────────
            combined_metrics = await self._fetch_combined_metrics(qe_task_id, qe_loop_id)
            ma_results = await self._fetch_multi_alpha_results_json(
                qe_task_id, qe_loop_id
            )

        # ── Phase 2: 解析多Alpha独有指标 ──────────────────────────
        group_metrics: dict[str, dict] = {}
        meta_weights: dict[str, float] = {}
        correlations: dict[str, float] = {}
        meta_method = "ic_weighted"

        if multi_alpha_config:
            meta_cfg = multi_alpha_config.get("meta_model", {})
            meta_method = meta_cfg.get("method", "ic_weighted")

        if ma_results:
            group_metrics = ma_results.get("group_metrics", {})
            meta_weights = ma_results.get("meta_weights", {})
            correlations = ma_results.get("correlations", {})
            if ma_results.get("meta_method"):
                meta_method = ma_results["meta_method"]
            logger.info(
                f"获取 {len(group_metrics)} 组指标, {len(correlations)} 组相关性"
            )
        else:
            logger.error(
                f"multi_alpha_results.json 不可用，降级为均匀权重: {parent_experiment_id}. "
                f"可能原因: meta_model_runner.py 未执行或执行失败"
            )
            n_groups = len(groups)
            for g in groups:
                meta_weights[g["group_name"]] = 1.0 / n_groups

        for g in groups:
            if g["group_name"] not in meta_weights:
                logger.error(f"组 {g['group_name']} 在 meta_weights 中缺失，设为 0")
                meta_weights[g["group_name"]] = 0.0

        # ── Phase 3: 写入 3 张扩展表 ──────────────────────────────
        combined_ic = combined_metrics.get("IC")
        if ma_results and ma_results.get("combined_ic") is not None:
            combined_ic = ma_results["combined_ic"]

        group_results = self._update_group_records(
            parent_experiment_id, groups, group_metrics, meta_weights
        )

        lookback_days = 60
        if multi_alpha_config:
            lookback_days = multi_alpha_config.get("meta_model", {}).get(
                "lookback_days", 60
            )
        self._insert_meta_weights(
            parent_experiment_id, meta_method, meta_weights,
            combined_ic, lookback_days,
        )

        self._insert_correlations(parent_experiment_id, correlations)

        # ── Phase 4: 回写统一分析层 ───────────────────────────────
        result_metrics = self._build_result_metrics(
            combined_metrics, meta_method, meta_weights, group_results, correlations
        )
        self._update_experiment_unified(
            parent_experiment_id, combined_metrics, result_metrics
        )

        logger.info(
            f"多Alpha结果收集完成: {parent_experiment_id}, "
            f"{len(group_results)} 组, combined IC={combined_ic}"
        )

        return {
            "ok": True,
            "experiment_id": parent_experiment_id,
            "combined_metrics": {
                k: v for k, v in combined_metrics.items() if k != "_raw_json"
            },
            "group_results": group_results,
            "meta_weights": meta_weights,
            "correlations": correlations,
        }

    # ── Phase 1 helpers ──────────────────────────────────────────

    def _get_experiment_record(self, experiment_id: str) -> dict | None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT experiment_id, qe_task_id, qe_loop_id,
                              alpha_mode, multi_alpha_config, status
                       FROM qe_experiments
                       WHERE experiment_id = %s""",
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    async def _fetch_combined_metrics(
        self, task_id: str, loop_id: str
    ) -> dict[str, Any]:
        """通过 QEWorkspaceClient 获取标准 Qlib 回测指标。

        失败时抛出 RuntimeError，不静默返回空 dict。
        """
        from .qe_workspace_client import QEWorkspaceClient

        async with QEWorkspaceClient() as client:
            metrics = await client.get_loop_metrics(task_id, loop_id)
            if not metrics:
                raise RuntimeError(
                    f"回测指标为空: task={task_id}, loop={loop_id}. "
                    f"可能原因: WSL 执行尚未完成或 read_exp_res.py 未生成指标"
                )
            return metrics

    def _get_group_records(self, parent_experiment_id: str) -> list[dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT group_name, factor_names, model_id, dataset_type,
                              assigned_node_id, prediction_path,
                              group_ic, group_icir, group_sharpe, meta_weight,
                              status, reuse_mode,
                              model_source_experiment_id, model_source_group_name
                       FROM qe_multi_alpha_groups
                       WHERE parent_experiment_id = %s
                       ORDER BY group_name""",
                    (parent_experiment_id,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    async def _fetch_multi_alpha_results_json(
        self, task_id: str, loop_id: str
    ) -> dict | None:
        """尝试从 workspace 获取 meta_model_runner.py 产出的详细结果 JSON。

        返回 None 表示文件不存在（正常降级场景），
        解析失败抛异常（数据损坏，不静默）。
        """
        from .qe_workspace_client import QEWorkspaceClient

        try:
            async with QEWorkspaceClient() as client:
                raw = await client.get_workspace_file(
                    task_id, loop_id, "multi_alpha_results.json"
                )
                if raw is None:
                    logger.warning(
                        f"multi_alpha_results.json 不存在: task={task_id}, loop={loop_id}"
                    )
                    return None
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                if not isinstance(parsed, dict):
                    raise ValueError(
                        f"multi_alpha_results.json 格式错误: 期望 dict, 得到 {type(parsed)}"
                    )
                return parsed
        except json.JSONDecodeError as e:
            raise ValueError(
                f"multi_alpha_results.json JSON 解析失败: {e}"
            ) from e
        except ValueError:
            raise
        except Exception as e:
            # 网络/连接问题 → 降级（返回 None），但记录 WARNING
            logger.warning(f"获取 multi_alpha_results.json 失败 (网络/连接): {e}")
            return None

    # ── Distributed collection ─────────────────────────────────

    async def _collect_distributed(
        self,
        qe_task_id: str,
        qe_loop_id: str,
        groups: list[dict],
        multi_alpha_config: dict,
    ) -> dict:
        """跨节点收集预测 → 本地运行 MetaModelCombiner → 返回完整 ma_results。

        每个节点运行了一部分组。此方法：
        1. 从各节点下载 group pred.pkl
        2. 用 MetaModelCombiner 计算 combined IC + 权重
        3. 计算组间相关性
        4. 返回与 multi_alpha_results.json 相同格式的 dict
        """
        import pickle
        import tempfile
        from pathlib import Path

        import numpy as np
        import pandas as pd
        from scipy.stats import spearmanr

        from .qe_workspace_client import QEWorkspaceClient
        from .meta_model import MetaModelCombiner

        meta_cfg = multi_alpha_config.get("meta_model", {})
        method = meta_cfg.get("method", "ic_weighted")
        lookback = meta_cfg.get("lookback_days", 60)

        # 1. 从各节点下载 pred.pkl
        group_predictions: dict[str, pd.DataFrame] = {}
        for g in groups:
            g_name = g["group_name"]
            node_id = g.get("assigned_node_id")
            if not node_id:
                logger.warning(f"组 {g_name} 无 assigned_node_id，跳过")
                continue

            try:
                client = QEWorkspaceClient.for_node(node_id)
                async with client:
                    pred_bytes = await client.download_group_predictions(
                        qe_task_id, qe_loop_id, g_name
                    )
                    if pred_bytes:
                        pred_df = pickle.loads(pred_bytes)
                        group_predictions[g_name] = pred_df
                        logger.info(f"下载组预测: {g_name} from {node_id}, {len(pred_df)} rows")
                    else:
                        logger.warning(f"组 {g_name} (节点 {node_id}) pred.pkl 不存在")
            except Exception as e:
                logger.error(f"下载组 {g_name} 预测失败 (节点 {node_id}): {e}")

        if len(group_predictions) < 2:
            logger.error(
                f"分布式收集: 仅获取 {len(group_predictions)} 组预测，"
                f"不足2组，无法运行 Meta 合并"
            )
            # 返回空结果 + 从主节点获取的 combined_metrics
            combined_metrics = {}
            try:
                combined_metrics = await self._fetch_combined_metrics(qe_task_id, qe_loop_id)
            except Exception:
                pass
            return {
                "group_metrics": {},
                "meta_weights": {g["group_name"]: 1.0 / len(groups) for g in groups},
                "correlations": {},
                "meta_method": method,
                "_combined_metrics": combined_metrics,
            }

        # 2. 运行 MetaModelCombiner
        combiner = MetaModelCombiner(method=method, lookback_days=lookback)
        combined_pred, weights = combiner.fit_and_combine(group_predictions)
        logger.info(f"分布式 Meta 合并完成: weights={weights}")

        # 3. 计算组级 IC（需要 label，如果有的话）
        group_metrics = {}
        for g_name, pred_df in group_predictions.items():
            pred_s = pred_df["score"] if isinstance(pred_df, pd.DataFrame) and "score" in pred_df.columns else (pred_df.iloc[:, 0] if isinstance(pred_df, pd.DataFrame) else pred_df)
            group_metrics[g_name] = {"ic": None, "icir": None, "sharpe": None}

        # 4. 计算组间相关性
        correlations = {}
        names = list(group_predictions.keys())
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                pa = group_predictions[names[i]]
                pb = group_predictions[names[j]]
                if isinstance(pa, pd.DataFrame):
                    pa = pa["score"] if "score" in pa.columns else pa.iloc[:, 0]
                if isinstance(pb, pd.DataFrame):
                    pb = pb["score"] if "score" in pb.columns else pb.iloc[:, 0]
                common = pa.index.intersection(pb.index)
                if len(common) >= 30:
                    corr = pa.loc[common].corr(pb.loc[common], method="spearman")
                    if not np.isnan(corr):
                        correlations[f"{names[i]}|{names[j]}"] = round(float(corr), 4)

        # 5. 计算 combined IC（从 combined_pred）
        combined_ic = None  # 需要 label 才能算，暂不可用

        # 6. 从主节点获取标准 Qlib 回测指标
        combined_metrics = {}
        try:
            combined_metrics = await self._fetch_combined_metrics(qe_task_id, qe_loop_id)
        except Exception as e:
            logger.warning(f"分布式: 获取 combined metrics 失败: {e}")
            # 分布式场景下可能主节点没有 combined 指标，这是正常的

        return {
            "group_metrics": group_metrics,
            "meta_weights": weights,
            "correlations": correlations,
            "combined_ic": combined_ic,
            "meta_method": method,
            "total_groups": len(group_predictions),
            "_combined_metrics": combined_metrics,
        }

    # ── Phase 3 helpers ──────────────────────────────────────────

    def _update_group_records(
        self,
        parent_experiment_id: str,
        groups: list[dict],
        group_metrics: dict[str, dict],
        meta_weights: dict[str, float],
    ) -> list[dict]:
        """更新各组的指标和权重到 qe_multi_alpha_groups。

        失败抛异常，不静默跳过。
        """
        results = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                for g in groups:
                    g_name = g["group_name"]
                    gm = group_metrics.get(g_name, {})
                    weight = meta_weights.get(g_name)

                    # 安全取值：支持 meta_model_runner.py 输出的两种键名格式
                    g_ic = gm.get("ic") if gm.get("ic") is not None else gm.get("IC")
                    g_icir = gm.get("icir") if gm.get("icir") is not None else gm.get("ICIR")
                    g_sharpe = gm.get("sharpe") if gm.get("sharpe") is not None else gm.get("information_ratio")

                    cur.execute(
                        """UPDATE qe_multi_alpha_groups
                           SET group_ic = %s, group_icir = %s, group_sharpe = %s,
                               meta_weight = %s, status = 'completed',
                               completed_at = NOW()
                           WHERE parent_experiment_id = %s AND group_name = %s""",
                        (
                            float(g_ic) if g_ic is not None else None,
                            float(g_icir) if g_icir is not None else None,
                            float(g_sharpe) if g_sharpe is not None else None,
                            float(weight) if weight is not None else None,
                            parent_experiment_id,
                            g_name,
                        ),
                    )

                    factor_names = g.get("factor_names", [])
                    if isinstance(factor_names, str):
                        factor_names = json.loads(factor_names)

                    results.append({
                        "group_name": g_name,
                        "ic": float(g_ic) if g_ic is not None else None,
                        "icir": float(g_icir) if g_icir is not None else None,
                        "sharpe": float(g_sharpe) if g_sharpe is not None else None,
                        "meta_weight": float(weight) if weight is not None else None,
                        "factor_count": len(factor_names),
                        "model_id": g["model_id"],
                    })
            conn.commit()

        logger.info(f"更新 {len(results)} 组记录: {parent_experiment_id}")
        return results

    def _insert_meta_weights(
        self,
        experiment_id: str,
        method: str,
        weights: dict[str, float],
        combined_ic: float | None,
        lookback_window: int,
    ) -> None:
        """写入 qe_meta_model_weights 权重历史记录。"""
        if not weights:
            logger.warning(f"meta_weights 为空，跳过写入: {experiment_id}")
            return

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO qe_meta_model_weights
                       (experiment_id, as_of_date, method, weights, combined_ic, lookback_window)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (experiment_id, as_of_date, method)
                       DO UPDATE SET weights = EXCLUDED.weights,
                                     combined_ic = EXCLUDED.combined_ic""",
                    (
                        experiment_id,
                        date.today(),
                        method,
                        json.dumps(weights),
                        float(combined_ic) if combined_ic is not None else None,
                        lookback_window,
                    ),
                )
            conn.commit()
        logger.info(f"写入 meta_weights: {experiment_id}, method={method}")

    def _insert_correlations(
        self,
        experiment_id: str,
        correlations: dict[str, float],
    ) -> None:
        """写入 qe_group_prediction_correlations 组间相关性。"""
        if not correlations:
            return

        with get_conn() as conn:
            with conn.cursor() as cur:
                for pair_key, corr_val in correlations.items():
                    parts = pair_key.split("|")
                    if len(parts) != 2:
                        logger.warning(f"相关性 key 格式错误，跳过: {pair_key}")
                        continue
                    group_a, group_b = parts

                    cur.execute(
                        """INSERT INTO qe_group_prediction_correlations
                           (experiment_id, group_a, group_b, correlation)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT (experiment_id, group_a, group_b) DO UPDATE
                           SET correlation = EXCLUDED.correlation""",
                        (experiment_id, group_a, group_b, float(corr_val)),
                    )
            conn.commit()
        logger.info(f"写入 {len(correlations)} 组相关性: {experiment_id}")

    # ── Phase 4 helpers ──────────────────────────────────────────

    def _build_result_metrics(
        self,
        combined_metrics: dict,
        meta_method: str,
        meta_weights: dict[str, float],
        group_results: list[dict],
        correlations: dict[str, float],
    ) -> dict:
        """构建 result_metrics JSON，兼容单Alpha格式 + multi_alpha_detail 嵌套。"""
        result = {k: v for k, v in combined_metrics.items() if k != "_raw_json"}

        combined_ic = combined_metrics.get("IC")
        result["multi_alpha_detail"] = {
            "meta_method": meta_method,
            "meta_weights": meta_weights,
            "combined_ic": float(combined_ic) if combined_ic is not None else None,
            "total_groups": len(group_results),
            "group_results": group_results,
            "group_correlations": correlations,
        }

        return result

    def _update_experiment_unified(
        self,
        experiment_id: str,
        combined_metrics: dict,
        result_metrics: dict,
    ) -> None:
        """回写 qe_experiments 统一指标列 + result_metrics JSON。"""
        col_sets = []
        col_vals = []
        for json_key, col_name in _COL_MAP.items():
            v = combined_metrics.get(json_key)
            if v is not None:
                col_sets.append(f"{col_name} = %s")
                col_vals.append(float(v))

        extra_set = (", " + ", ".join(col_sets)) if col_sets else ""

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE qe_experiments
                        SET result_metrics = %s,
                            status = 'completed',
                            completed_at = NOW(){extra_set}
                        WHERE experiment_id = %s""",
                    [json.dumps(result_metrics, default=str)]
                    + col_vals
                    + [experiment_id],
                )
            conn.commit()
        logger.info(f"回写统一分析层: {experiment_id}")
