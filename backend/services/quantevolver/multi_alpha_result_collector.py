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


class MultiAlphaArtifactError(RuntimeError):
    """Raised when completed multi-alpha execution is missing required artifacts."""


class MultiAlphaResultCollector:
    """多Alpha实验结果收集器。"""

    async def collect_and_persist(self, parent_experiment_id: str) -> dict[str, Any]:
        """收集多Alpha实验结果并持久化到DB。

        统一回测架构：
        - 所有组（主节点+从节点）都只做 train-only（训练+生成pred.pkl）
        - 本方法收集所有组的 pred.pkl → meta 合并 → 统一回测 → 持久化

        失败时抛出异常，不静默兜底。调用方负责处理。
        """
        logger.info(f"开始收集多Alpha结果: {parent_experiment_id}")

        # ── Phase 0: 检查所有组是否完成 ──────────────────────────────
        groups = self._get_group_records(parent_experiment_id)
        if not groups:
            raise ValueError(f"实验 {parent_experiment_id} 没有多Alpha组记录")

        # 分布式模式下，需要等待所有组完成训练
        pending_groups = [
            g["group_name"] for g in groups
            if g.get("status") not in ("completed", "failed")
            and g.get("reuse_mode") not in ("reuse_prediction", "reuse_model")
        ]
        if pending_groups:
            logger.info(
                f"多Alpha实验 {parent_experiment_id} 尚有 {len(pending_groups)} 组未完成训练: "
                f"{pending_groups}，跳过结果收集（等待所有组完成）"
            )
            return {"ok": False, "reason": "pending_groups", "pending": pending_groups}

        # ── Phase 1: 获取原始数据 ──────────────────────────────────
        exp_record = self._get_experiment_record(parent_experiment_id)
        if not exp_record:
            raise ValueError(f"实验 {parent_experiment_id} 不存在")

        qe_task_id = exp_record.get("qe_task_id")
        qe_loop_id = exp_record.get("qe_loop_id")
        multi_alpha_config = exp_record.get("multi_alpha_config") or {}
        if isinstance(multi_alpha_config, str):
            multi_alpha_config = json.loads(multi_alpha_config)

        if not qe_task_id:
            raise ValueError(
                f"实验 {parent_experiment_id} 缺少 qe_task_id，无法获取回测指标"
            )

        # 检测是否为分布式执行（各组可能在不同节点）
        node_ids = set(g.get("assigned_node_id") for g in groups if g.get("assigned_node_id"))
        is_distributed = len(node_ids) > 1

        if not is_distributed and not qe_loop_id:
            raise ValueError(
                f"实验 {parent_experiment_id} 缺少 qe_loop_id，无法获取回测指标"
            )

        group_enhanced_metrics: dict[str, dict] = {}

        if is_distributed:
            # ── 分布式场景：跨节点收集预测 + 本地 meta 合并 ──────
            logger.info(f"分布式多Alpha结果收集: {len(node_ids)} 节点, {len(groups)} 组")
            ma_results = await self._collect_distributed(
                qe_task_id, groups, multi_alpha_config
            )
            # 分布式收集后直接拿到完整 ma_results
            combined_metrics = ma_results.pop("_combined_metrics", {})
            group_enhanced_metrics = ma_results.get("group_enhanced_metrics", {}) or {}
        else:
            # ── 单节点场景：从 workspace 获取结果 ──────────────
            # get_loop_metrics 返回完整的 qlib_results_enhanced.json
            # 它本身就是 enhanced_metrics，需要标记以便 _build_result_metrics 正确处理
            await self._validate_single_node_artifacts(qe_task_id, qe_loop_id, groups)
            raw_metrics = await self._fetch_combined_metrics(qe_task_id, qe_loop_id)
            combined_metrics = {}
            if raw_metrics:
                # 提取 summary 中的标量指标到顶层
                summary = raw_metrics.get("summary", {})
                combined_metrics.update(summary)
                # 保存完整的 enhanced_metrics（供统一分析层使用）
                combined_metrics["_enhanced_metrics"] = raw_metrics
            ma_results = await self._fetch_multi_alpha_results_json(
                qe_task_id, qe_loop_id
            )
            group_enhanced_metrics = await self._fetch_single_node_group_enhanced(
                qe_task_id, qe_loop_id, groups
            )

        # ── Phase 2: 解析多Alpha独有指标 ──────────────────────────
        group_metrics: dict[str, dict] = {}
        meta_weights: dict[str, float] = {}
        correlations: dict[str, float] = {}
        meta_method = "ic_weighted"

        if multi_alpha_config:
            meta_cfg = multi_alpha_config.get("meta_model", {})
            meta_method = meta_cfg.get("method", "ic_weighted")

        if not ma_results:
            raise RuntimeError(
                f"multi_alpha_results.json 不可用: {parent_experiment_id}. "
                f"meta_model_runner.py 未执行、执行失败或结果读取失败"
            )

        group_metrics = ma_results.get("group_metrics", {})
        meta_weights = ma_results.get("meta_weights", {})
        correlations = ma_results.get("correlations", {})
        ic_quality = ma_results.get("ic_quality", {})
        if ma_results.get("meta_method"):
            meta_method = ma_results["meta_method"]
        logger.info(
            f"获取 {len(group_metrics)} 组指标, {len(correlations)} 组相关性"
        )

        missing_weights = [g["group_name"] for g in groups if g["group_name"] not in meta_weights]
        if missing_weights:
            raise RuntimeError(f"meta_weights 缺失组: {missing_weights}")

        if is_distributed:
            # 用 group_metrics（来自 enhanced.json）回写 per-group 指标到 DB
            self._update_distributed_group_records(
                parent_experiment_id, groups, group_metrics, meta_weights
            )
            group_results = []
            for g in groups:
                g_name = g["group_name"]
                factor_names = g.get("factor_names", [])
                if isinstance(factor_names, str):
                    factor_names = json.loads(factor_names)
                gm = group_metrics.get(g_name, {})
                group_results.append({
                    "group_name": g_name,
                    "ic": gm.get("IC") or gm.get("Rank IC") or g.get("group_ic"),
                    "icir": gm.get("ICIR") or gm.get("Rank ICIR") or g.get("group_icir"),
                    "sharpe": g.get("group_sharpe"),
                    "meta_weight": float(meta_weights.get(g_name, 0)),
                    "factor_count": len(factor_names),
                    "model_id": g["model_id"],
                })
        else:
            group_results = self._update_group_records(
                parent_experiment_id, groups, group_metrics, meta_weights
            )

        # ── Phase 3: 写入 3 张扩展表 ──────────────────────────────
        combined_ic = combined_metrics.get("IC")

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
            combined_metrics,
            meta_method,
            meta_weights,
            group_results,
            correlations,
            group_enhanced_metrics,
            ic_quality,
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
            "result_metrics": result_metrics,
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
        """Fetch the authoritative combined enhanced metrics artifact.

        The workspace `/metrics` endpoint can expose only a flattened summary.
        Multi-alpha parent experiments need the full `qlib_results_enhanced.json`
        so unified analysis matches single-alpha QE output. Missing or malformed
        artifacts fail fast.
        """
        from .qe_workspace_client import QEWorkspaceClient

        async with QEWorkspaceClient() as client:
            raw = await client.get_workspace_file(
                task_id, loop_id, "qlib_results_enhanced.json"
            )
            metrics = self._parse_required_json_artifact(
                "qlib_results_enhanced.json", raw
            )
            summary = metrics.get("summary")
            if not isinstance(summary, dict) or not summary:
                raise RuntimeError(
                    f"qlib_results_enhanced.json missing non-empty summary: "
                    f"task={task_id}, loop={loop_id}"
                )
            return metrics

    def _get_group_records(self, parent_experiment_id: str) -> list[dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT group_name, factor_names, model_id, dataset_type,
                              assigned_node_id, qe_loop_id, prediction_path,
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

    def _parse_required_json_artifact(self, artifact_name: str, raw: Any) -> dict[str, Any]:
        """Parse a required JSON artifact and reject empty or malformed content."""
        if isinstance(raw, dict):
            parsed = raw
        elif isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError as e:
                raise MultiAlphaArtifactError(
                    f"{artifact_name} is not valid JSON: {e}"
                ) from e
        else:
            raise MultiAlphaArtifactError(
                f"{artifact_name} has invalid type: {type(raw).__name__}"
            )

        if not parsed:
            raise MultiAlphaArtifactError(f"{artifact_name} is empty")
        return parsed

    async def _validate_single_node_artifacts(
        self,
        task_id: str,
        loop_id: str,
        groups: list[dict],
    ) -> None:
        """Validate required artifacts before single-node collection succeeds.

        A completed loop without these artifacts is not a successful multi-alpha
        run and must not be converted into an empty UI success state.
        """
        from .qe_workspace_client import QEWorkspaceClient

        errors: list[str] = []
        node_ids = {g.get("assigned_node_id") for g in groups if g.get("assigned_node_id")}
        if len(node_ids) > 1:
            raise MultiAlphaArtifactError(
                f"single-node artifact validation received multiple nodes: {sorted(node_ids)}"
            )
        client_cm = QEWorkspaceClient.for_node(next(iter(node_ids))) if node_ids else QEWorkspaceClient()
        async with client_cm as client:
            try:
                combined = await client.download_workspace_file_bytes(
                    task_id, loop_id, "combined_prediction.pkl"
                )
                if not combined:
                    errors.append("combined_prediction.pkl is empty")
            except Exception as e:
                errors.append(f"combined_prediction.pkl missing or unreadable: {e}")

            for artifact_name in ("multi_alpha_results.json", "qlib_results_enhanced.json"):
                try:
                    raw = await client.get_workspace_file(task_id, loop_id, artifact_name)
                    parsed = self._parse_required_json_artifact(artifact_name, raw)
                    if artifact_name == "qlib_results_enhanced.json" and "summary" not in parsed:
                        errors.append(f"{artifact_name} missing required summary section")
                except Exception as e:
                    errors.append(f"{artifact_name} missing or invalid: {e}")

            for group in groups:
                group_name = group.get("group_name")
                if not group_name:
                    errors.append(f"group record missing group_name: {group}")
                    continue
                if group.get("reuse_mode") in ("reuse_prediction", "reuse_model"):
                    prediction_path = group.get("prediction_path")
                    if not prediction_path:
                        errors.append(f"group {group_name} reuse mode missing prediction_path")
                    continue
                try:
                    pred_bytes = await client.download_group_predictions(
                        task_id, loop_id, group_name
                    )
                    if not pred_bytes:
                        errors.append(f"group {group_name} pred.pkl is empty")
                except Exception as e:
                    errors.append(f"group {group_name} pred.pkl missing or unreadable: {e}")
                try:
                    artifact_name = f"group_{group_name}/qlib_results_enhanced.json"
                    raw = await client.get_workspace_file(task_id, loop_id, artifact_name)
                    parsed = self._parse_required_json_artifact(artifact_name, raw)
                    if "summary" not in parsed:
                        errors.append(f"{artifact_name} missing required summary section")
                except Exception as e:
                    errors.append(
                        f"group {group_name} qlib_results_enhanced.json missing or invalid: {e}"
                    )

        if errors:
            raise MultiAlphaArtifactError(
                "Multi-alpha required artifacts are not ready or invalid: "
                + "; ".join(errors)
            )

    async def _fetch_single_node_group_enhanced(
        self,
        task_id: str,
        loop_id: str,
        groups: list[dict],
    ) -> dict[str, dict]:
        """Fetch group-level enhanced metrics from the single-node workspace."""
        from .qe_workspace_client import QEWorkspaceClient

        group_enhanced: dict[str, dict] = {}
        node_ids = {g.get("assigned_node_id") for g in groups if g.get("assigned_node_id")}
        if len(node_ids) > 1:
            raise MultiAlphaArtifactError(
                f"single-node group enhanced fetch received multiple nodes: {sorted(node_ids)}"
            )
        client_cm = QEWorkspaceClient.for_node(next(iter(node_ids))) if node_ids else QEWorkspaceClient()
        async with client_cm as client:
            for group in groups:
                group_name = group.get("group_name")
                if not group_name:
                    raise MultiAlphaArtifactError(f"group record missing group_name: {group}")
                if group.get("reuse_mode") in ("reuse_prediction", "reuse_model"):
                    continue
                artifact_name = f"group_{group_name}/qlib_results_enhanced.json"
                raw = await client.get_workspace_file(task_id, loop_id, artifact_name)
                parsed = self._parse_required_json_artifact(artifact_name, raw)
                if "summary" not in parsed:
                    raise MultiAlphaArtifactError(
                        f"{artifact_name} missing required summary section"
                    )
                group_enhanced[group_name] = parsed
        return group_enhanced

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
            raise RuntimeError(
                f"获取 multi_alpha_results.json 失败: task={task_id}, loop={loop_id}: {e}"
            ) from e

    async def _load_group_prediction(
        self,
        client: Any,
        task_id: str,
        loop_id: str,
        group_name: str,
        prediction_path: str | None = None,
    ) -> Any | None:
        """优先读取 workspace 内 group 输出；缺失时 fallback 到已记录的 prediction_path。"""
        import pickle

        pred_bytes = await client.download_group_predictions(task_id, loop_id, group_name)
        if pred_bytes:
            return pickle.loads(pred_bytes)

        normalized = prediction_path.replace("\\", "/") if prediction_path else ""
        marker = "/qe_workspace/"
        marker_idx = normalized.find(marker)
        if marker_idx < 0:
            raise RuntimeError(
                f"组 {group_name} 缺少可映射的 prediction_path: {prediction_path}"
            )

        workspace_rel = normalized[marker_idx + len(marker):].lstrip("/")
        file_bytes = await client.download_workspace_file_bytes(
            task_id, loop_id, workspace_rel
        )
        if not file_bytes:
            raise RuntimeError(
                f"组 {group_name} prediction_path 读取为空: {workspace_rel}"
            )
        return pickle.loads(file_bytes)

    # ── Distributed collection ─────────────────────────────────

    async def _collect_distributed(
        self,
        qe_task_id: str,
        groups: list[dict],
        multi_alpha_config: dict,
    ) -> dict:
        """跨节点收集预测 + 增强指标 → MetaModelCombiner → 返回完整 ma_results。

        统一回测架构：
        - 所有节点只做 train-only（训练+生成pred.pkl）
        - 本方法从各节点下载 pred.pkl + qlib_results_enhanced.json
        - 用 enhanced.json 的 per-group IC/ICIR 作为权威指标
        - 用 MetaModelCombiner 计算 combined prediction + 权重
        - 计算组间预测相关性
        - 用 per-group IC series 加权计算 combined IC/ICIR
        """
        import numpy as np
        import pandas as pd

        from .qe_workspace_client import QEWorkspaceClient
        from .meta_model import MetaModelCombiner

        meta_cfg = multi_alpha_config.get("meta_model", {})
        method = meta_cfg.get("method", "ic_weighted")
        lookback = meta_cfg.get("lookback_days", 60)

        # 1. 从各节点下载 pred.pkl + qlib_results_enhanced.json
        group_predictions: dict[str, pd.DataFrame] = {}
        group_enhanced: dict[str, dict] = {}
        label_df = None

        for g in groups:
            g_name = g["group_name"]

            # reuse 组直接从已有路径加载
            if g.get("reuse_mode") in ("reuse_prediction", "reuse_model"):
                pred_path = g.get("prediction_path")
                if not pred_path:
                    raise RuntimeError(f"组 {g_name} reuse 模式但缺少 prediction_path")
                import pickle
                from pathlib import Path
                p = Path(pred_path)
                if not p.exists():
                    raise RuntimeError(f"组 {g_name} prediction_path 不存在: {pred_path}")
                with open(p, "rb") as f:
                    group_predictions[g_name] = pickle.load(f)
                logger.info(f"复用组预测: {g_name} from {pred_path}")
                continue

            node_id = g.get("assigned_node_id")
            node_loop_id = g.get("qe_loop_id")
            if not node_id:
                raise RuntimeError(f"组 {g_name} 缺少 assigned_node_id")
            if not node_loop_id:
                raise RuntimeError(f"组 {g_name} 缺少 qe_loop_id")

            client = QEWorkspaceClient.for_node(node_id)
            async with client:
                pred_df = await self._load_group_prediction(
                    client, qe_task_id, node_loop_id, g_name, g.get("prediction_path"),
                )
                group_predictions[g_name] = pred_df
                logger.info(f"下载组预测: {g_name} from {node_id}/{node_loop_id}, {len(pred_df)} rows")

                # 下载 qlib_results_enhanced.json（per-group IC/ICIR 权威来源）
                enhanced = await self._try_load_group_enhanced(
                    client, qe_task_id, node_loop_id, g_name
                )
                if enhanced:
                    group_enhanced[g_name] = enhanced
                    logger.info(f"下载增强指标: {g_name}, IC={enhanced.get('summary', {}).get('IC')}")
                else:
                    logger.warning(f"组 {g_name} 增强指标不可用，将影响 ic_weighted 权重计算")

                # 尝试下载 label（只需要一次，从任意节点获取）
                if label_df is None:
                    label_df = await self._try_load_group_label(
                        client, qe_task_id, node_loop_id, g_name
                    )

        if len(group_predictions) < 2:
            raise RuntimeError(
                f"分布式收集失败: 仅获取 {len(group_predictions)} 组预测，不足2组"
            )

        # 2. 运行 MetaModelCombiner
        if method == "ic_weighted" and label_df is not None:
            combiner = MetaModelCombiner(method=method, lookback_days=lookback)
            combined_pred, weights = combiner.fit_and_combine(
                group_predictions, actual_returns=label_df
            )
        elif method == "ic_weighted" and group_enhanced:
            # label 不可用但有 enhanced.json → 用 per-group IC 作为权重
            # 验证所有非 reuse 组都有 enhanced 数据
            missing_enhanced = [
                g["group_name"] for g in groups
                if g.get("reuse_mode") not in ("reuse_prediction", "reuse_model")
                and g["group_name"] not in group_enhanced
            ]
            if missing_enhanced:
                raise RuntimeError(
                    f"分布式收集失败: ic_weighted 模式但以下组缺少 enhanced.json: "
                    f"{missing_enhanced}。无法计算完整权重。"
                )
            logger.info("分布式收集: label 不可用，用 enhanced.json IC 计算权重")
            ic_weights = {}
            # 非 reuse 组：从 enhanced.json 获取 IC
            for g_name, enh in group_enhanced.items():
                g_ic = abs(enh.get("summary", {}).get("Rank IC", 0) or 0)
                ic_weights[g_name] = g_ic
            # reuse 组：从 DB 记录获取 IC
            for g in groups:
                g_name = g["group_name"]
                if g.get("reuse_mode") in ("reuse_prediction", "reuse_model"):
                    reuse_ic = abs(g.get("group_ic") or 0)
                    if reuse_ic <= 1e-8:
                        raise RuntimeError(
                            f"分布式收集失败: reuse 组 {g_name} 缺少有效的 group_ic。"
                            f"ic_weighted 模式需要所有组都有 IC 值。"
                        )
                    ic_weights[g_name] = reuse_ic
            total_ic = sum(ic_weights.values())
            if total_ic <= 1e-8:
                raise RuntimeError(
                    f"分布式收集失败: ic_weighted 模式但所有组的 Rank IC 为 0。"
                    f"各组 IC: {ic_weights}。无法计算有效权重。"
                )
            weights = {k: round(v / total_ic, 4) for k, v in ic_weights.items()}
            # 手动加权合并预测
            combined_pred = self._weighted_combine_predictions(group_predictions, weights)
        elif method == "equal":
            combiner = MetaModelCombiner(method="equal", lookback_days=lookback)
            combined_pred, weights = combiner.fit_and_combine(group_predictions)
        else:
            raise RuntimeError(
                f"分布式收集失败: method={method} 但无法获取 label 且 enhanced.json 为空。"
                f"ic_weighted 模式需要 label 或 enhanced.json 来计算权重。"
                f"请检查各节点的训练是否正常完成并生成了 qlib_results_enhanced.json。"
            )

        if combined_pred is None or len(combined_pred) == 0:
            raise RuntimeError("分布式 Meta 合并失败: combined prediction 为空")
        if not weights:
            raise RuntimeError("分布式 Meta 合并失败: meta_weights 为空")
        logger.info(f"分布式 Meta 合并完成: weights={weights}")

        # 3. 从 enhanced.json 提取 per-group IC/ICIR（权威来源）
        group_metrics = {}
        for g_name, enh in group_enhanced.items():
            summary = enh.get("summary", {})
            group_metrics[g_name] = {
                "IC": summary.get("IC"),
                "ICIR": summary.get("ICIR"),
                "Rank IC": summary.get("Rank IC"),
                "Rank ICIR": summary.get("Rank ICIR"),
            }
        # fallback: 如果 enhanced 不可用但有 label，本地计算
        if not group_metrics and label_df is not None:
            group_metrics = self._compute_group_metrics_local(group_predictions, label_df)

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
                if len(common) < 30:
                    raise RuntimeError(
                        f"分布式相关性计算失败: {names[i]} vs {names[j]} overlap={len(common)}"
                    )
                corr = pa.loc[common].corr(pb.loc[common], method="spearman")
                if np.isnan(corr):
                    raise RuntimeError(
                        f"分布式相关性计算失败: {names[i]} vs {names[j]} produced NaN"
                    )
                correlations[f"{names[i]}|{names[j]}"] = round(float(corr), 4)

        if not correlations and len(names) >= 2:
            raise RuntimeError("分布式相关性计算失败: correlations 为空")

        # 5. 触发主节点执行统一回测（combined prediction → 选股+分钟线回测）
        backtest_metrics = await self._trigger_unified_backtest(
            qe_task_id, combined_pred, groups, multi_alpha_config
        )
        # backtest_metrics 包含完整的 enhanced_metrics（IC曲线、收益曲线、持仓等）
        combined_metrics = backtest_metrics

        return {
            "group_metrics": group_metrics,
            "meta_weights": weights,
            "correlations": correlations,
            "combined_ic": combined_metrics.get("IC") or combined_metrics.get("Rank IC"),
            "meta_method": method,
            "total_groups": len(group_predictions),
            "group_enhanced_metrics": group_enhanced,
            "_combined_metrics": combined_metrics,
        }

    async def _try_load_group_enhanced(
        self,
        client: Any,
        task_id: str,
        loop_id: str,
        group_name: str,
    ) -> dict | None:
        """从节点 workspace 下载 qlib_results_enhanced.json。返回 None 表示不可用。"""
        try:
            raw = await client.get_workspace_file(
                task_id, loop_id,
                f"group_{group_name}/qlib_results_enhanced.json",
            )
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict) and "summary" in parsed:
                return parsed
            logger.warning(f"enhanced.json 格式异常 ({group_name}): 缺少 summary")
        except Exception as e:
            logger.warning(f"下载 enhanced.json 失败 ({group_name}): {e}")
        return None

    def _weighted_combine_predictions(
        self,
        group_predictions: dict,
        weights: dict[str, float],
    ):
        """用给定权重手动加权合并多组预测。"""
        import pandas as pd

        aligned = {}
        common_idx = None
        for g_name, pred in group_predictions.items():
            s = pred["score"] if isinstance(pred, pd.DataFrame) and "score" in pred.columns else (pred.iloc[:, 0] if isinstance(pred, pd.DataFrame) else pred)
            aligned[g_name] = s
            common_idx = s.index if common_idx is None else common_idx.intersection(s.index)

        if common_idx is None or len(common_idx) == 0:
            raise RuntimeError("加权合并失败: 无公共索引")

        combined = sum(aligned[g].loc[common_idx] * weights.get(g, 0) for g in aligned)
        return pd.DataFrame({"score": combined}, index=common_idx)

    def _compute_combined_ic_from_enhanced(
        self,
        group_enhanced: dict[str, dict],
        weights: dict[str, float],
        label_df,
        combined_pred,
    ) -> dict:
        """从 enhanced.json 的 ic_series 加权计算 combined IC/ICIR。

        优先用 label_df + combined_pred 直接计算（最准确）。
        fallback: 用 per-group IC series 加权近似。
        """
        import numpy as np

        # 方案 A: 有 label → 直接计算 combined prediction 的 IC
        if label_df is not None and combined_pred is not None:
            return self._compute_combined_ic_from_label(label_df, combined_pred)

        # 方案 B: 用 enhanced.json 的 per-group ic_series 加权
        if not group_enhanced:
            logger.error("无法计算 combined IC: 无 label 且无 enhanced.json")
            return {}

        # 收集各组的 daily IC series
        group_ic_series: dict[str, dict[str, float]] = {}
        for g_name, enh in group_enhanced.items():
            ic_diag = enh.get("ic_diagnostics", {})
            ic_dates = ic_diag.get("ic_dates", [])
            ic_values = ic_diag.get("ic_series", [])
            if ic_dates and ic_values and len(ic_dates) == len(ic_values):
                group_ic_series[g_name] = dict(zip(ic_dates, ic_values))

        if not group_ic_series:
            # fallback: 用 summary IC 的加权平均
            logger.warning("enhanced.json 无 ic_series，用 summary IC 加权")
            weighted_ic = 0.0
            weighted_icir = 0.0
            for g_name, enh in group_enhanced.items():
                w = weights.get(g_name, 0)
                summary = enh.get("summary", {})
                weighted_ic += (summary.get("Rank IC") or 0) * w
                weighted_icir += (summary.get("Rank ICIR") or 0) * w
            return {
                "Rank IC": round(weighted_ic, 6),
                "Rank ICIR": round(weighted_icir, 4),
                "IC": round(weighted_ic, 6),
                "ICIR": round(weighted_icir, 4),
                "source": "enhanced_summary_weighted",
            }

        # 加权合并 daily IC series
        all_dates = sorted(set(d for s in group_ic_series.values() for d in s))
        daily_combined_ics = []
        for dt in all_dates:
            weighted_ic = 0.0
            total_w = 0.0
            for g_name, series in group_ic_series.items():
                if dt in series:
                    w = weights.get(g_name, 0)
                    weighted_ic += series[dt] * w
                    total_w += w
            if total_w > 1e-8:
                daily_combined_ics.append(weighted_ic / total_w)

        if not daily_combined_ics:
            return {}

        ic_mean = float(np.mean(daily_combined_ics))
        ic_std = float(np.std(daily_combined_ics))
        result = {
            "Rank IC": round(ic_mean, 6),
            "ic_days": len(daily_combined_ics),
            "source": "enhanced_ic_series_weighted",
        }
        if ic_std > 1e-8:
            result["Rank ICIR"] = round(ic_mean / ic_std, 4)
        # 映射到标准 key
        result["IC"] = result["Rank IC"]
        result["ICIR"] = result.get("Rank ICIR", 0)
        return result

    def _compute_combined_ic_from_label(self, label_df, combined_pred) -> dict:
        """用 label + combined prediction 直接计算 IC/ICIR。"""
        import numpy as np
        import pandas as pd

        combined_s = combined_pred["score"] if isinstance(combined_pred, pd.DataFrame) and "score" in combined_pred.columns else (combined_pred.iloc[:, 0] if isinstance(combined_pred, pd.DataFrame) else combined_pred)
        common = combined_s.index.intersection(label_df.index)
        if len(common) < 50:
            return {}

        daily_ics = []
        dates = sorted(set(idx[0] if isinstance(idx, tuple) else idx for idx in common))
        for dt in dates:
            if isinstance(combined_s.index, pd.MultiIndex):
                c_day = combined_s.xs(dt, level=0)
                r_day = label_df.xs(dt, level=0)
            else:
                c_day = combined_s
                r_day = label_df
            if len(c_day) >= 10:
                ic = c_day.corr(r_day, method="spearman")
                if not np.isnan(ic):
                    daily_ics.append(ic)
        if not daily_ics:
            return {}

        ic_mean = float(np.mean(daily_ics))
        ic_std = float(np.std(daily_ics))
        result = {"IC": round(ic_mean, 6), "Rank IC": round(ic_mean, 6), "ic_days": len(daily_ics)}
        if ic_std > 1e-8:
            result["ICIR"] = round(ic_mean / ic_std, 4)
            result["Rank ICIR"] = result["ICIR"]
        return result

    async def _try_load_group_label(
        self,
        client: Any,
        task_id: str,
        loop_id: str,
        group_name: str,
    ) -> Any | None:
        """尝试从节点 workspace 下载 label.pkl。返回 None 表示不可用。"""
        import pickle
        try:
            label_bytes = await client.download_workspace_file_bytes(
                task_id, loop_id, f"group_{group_name}/output/label.pkl"
            )
            if label_bytes:
                import pandas as pd
                label = pickle.loads(label_bytes)
                if isinstance(label, pd.DataFrame):
                    return label.iloc[:, 0]
                return label
        except Exception as e:
            logger.warning(f"下载 label.pkl 失败 ({group_name}): {e}")
        return None

    def _compute_group_metrics_local(
        self,
        group_predictions: dict,
        label: Any,
    ) -> dict[str, dict]:
        """本地计算 per-group IC/ICIR（不依赖节点回测结果）。"""
        import numpy as np
        import pandas as pd

        results = {}
        for g_name, pred_df in group_predictions.items():
            if isinstance(pred_df, pd.DataFrame):
                pred_s = pred_df["score"] if "score" in pred_df.columns else pred_df.iloc[:, 0]
            else:
                pred_s = pred_df

            common_idx = pred_s.index.intersection(label.index)
            if len(common_idx) < 50:
                logger.warning(f"组 {g_name} 样本不足: {len(common_idx)}")
                continue

            p = pred_s.loc[common_idx]
            r = label.loc[common_idx]

            daily_ics = []
            dates = sorted(set(idx[0] if isinstance(idx, tuple) else idx for idx in common_idx))
            for dt in dates:
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

            if not daily_ics:
                continue

            avg_ic = float(np.mean(daily_ics))
            std_ic = float(np.std(daily_ics))
            results[g_name] = {
                "ic": round(avg_ic, 6),
                "icir": round(avg_ic / std_ic, 4) if std_ic > 1e-8 else None,
                "sharpe": round((avg_ic / std_ic) * np.sqrt(252) / np.sqrt(len(dates)), 4) if std_ic > 1e-8 else None,
            }

        return results

    async def _trigger_unified_backtest(
        self,
        qe_task_id: str,
        combined_pred,
        groups: list[dict],
        multi_alpha_config: dict,
    ) -> dict:
        """在主节点触发统一回测 Loop，等待完成后返回完整回测指标。

        流程：
        1. 选择主节点（优先本地 wsl2-5080）
        2. 序列化 combined_prediction.pkl
        3. 准备回测依赖文件（conf.yaml + qrun_limit_minute.py + read_exp_res.py + 策略）
        4. 通过 RDAgent API create_and_run_loop 触发 Loop2
        5. 轮询等待完成
        6. 下载 qlib_results_enhanced.json 返回完整指标

        失败直接抛异常，不降级。
        """
        import asyncio
        import base64
        import pickle

        from .qe_workspace_client import QEWorkspaceClient

        # 1. 选择主节点（第一个节点或 wsl2-5080）
        primary_node_id = None
        for g in groups:
            nid = g.get("assigned_node_id")
            if nid:
                if primary_node_id is None:
                    primary_node_id = nid
                # 优先选择本地节点
                if "wsl2" in nid.lower() or "5080" in nid.lower():
                    primary_node_id = nid
                    break
        if not primary_node_id:
            raise RuntimeError("分布式统一回测失败: 无法确定主节点")

        logger.info(f"分布式统一回测: 主节点={primary_node_id}, task={qe_task_id}")

        # 2. 序列化 combined prediction
        pred_bytes = pickle.dumps(combined_pred)
        pred_b64 = base64.b64encode(pred_bytes).decode("ascii")

        # 3. 准备回测依赖文件
        # 从第一个 group 的 workspace 下载回测脚本和配置
        first_group = groups[0]
        first_node_id = first_group.get("assigned_node_id")
        first_loop_id = first_group.get("qe_loop_id")
        first_group_name = first_group["group_name"]

        backtest_files = {
            "combined_prediction.pkl": pred_b64,  # base64 encoded binary
        }

        # 下载回测依赖文件
        deps_to_download = [
            "qrun_limit_minute.py",
            "read_exp_res.py",
            "custom_strategy.py",
            "tail_twap_strategy.py",
            "qe_custom_loaders.py",
        ]

        client = QEWorkspaceClient.for_node(first_node_id)
        async with client:
            for dep_file in deps_to_download:
                content = await client.get_workspace_file(
                    qe_task_id, first_loop_id,
                    f"group_{first_group_name}/{dep_file}",
                )
                if not content:
                    raise RuntimeError(
                        f"分布式统一回测失败: 回测依赖文件 {dep_file} 下载为空。"
                        f"节点={first_node_id}, loop={first_loop_id}, group={first_group_name}"
                    )
                backtest_files[dep_file] = content

            # 下载 conf.yaml（需要完整的 port_analysis_config）
            conf_content = await client.get_workspace_file(
                qe_task_id, first_loop_id,
                f"group_{first_group_name}/conf.yaml",
            )
            if not conf_content:
                raise RuntimeError(
                    f"分布式统一回测失败: conf.yaml 下载为空。"
                    f"节点={first_node_id}, loop={first_loop_id}, group={first_group_name}"
                )
            backtest_files["conf.yaml"] = conf_content

            # 下载 benchmark 文件（可选 — load_benchmark_series 有 qlib 数据源 fallback）
            try:
                benchmark_content = await client.get_workspace_file(
                    qe_task_id, first_loop_id,
                    f"group_{first_group_name}/benchmark_sh000300.parquet",
                )
                if benchmark_content:
                    backtest_files["benchmark_sh000300.parquet"] = benchmark_content
            except Exception:
                logger.info("benchmark_sh000300.parquet 不可用，回测将从 qlib 数据源计算 benchmark")

        # 4. 触发主节点执行统一回测 Loop
        wsl_command = (
            "export MALLOC_ARENA_MAX=4 && "
            "export PYTHONUNBUFFERED=1 && "
            "python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl && "
            "QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py"
        )

        backtest_config = {
            "mode": "pred_backtest",
            "source_task_id": qe_task_id,
            "combined_groups": [g["group_name"] for g in groups],
        }

        primary_client = QEWorkspaceClient.for_node(primary_node_id)
        async with primary_client:
            loop_id = await primary_client.create_and_run_loop(
                task_id=qe_task_id,
                loop_index=2,  # Loop2 = 统一回测
                config=backtest_config,
                experiment_files=backtest_files,
                wsl_command=wsl_command,
            )
            logger.info(f"分布式统一回测 Loop 已触发: loop_id={loop_id}")

            # 5. 轮询等待完成（最长 30 分钟）
            max_wait = 1800  # 30 minutes
            poll_interval = 10  # 10 seconds
            elapsed = 0

            while elapsed < max_wait:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

                status_data = await primary_client.get_loop_status(qe_task_id, loop_id)
                status = status_data.get("status", "")

                if status == "completed":
                    logger.info(f"分布式统一回测完成: {elapsed}s")
                    break
                elif status == "failed":
                    error_msg = status_data.get("error", "unknown")
                    raise RuntimeError(
                        f"分布式统一回测失败: loop={loop_id}, error={error_msg}"
                    )
                elif status not in ("running", "pending"):
                    raise RuntimeError(
                        f"分布式统一回测异常状态: loop={loop_id}, status={status}"
                    )
            else:
                raise RuntimeError(
                    f"分布式统一回测超时: loop={loop_id}, elapsed={elapsed}s"
                )

            # 6. 获取回测指标
            metrics = await primary_client.get_loop_metrics(qe_task_id, loop_id)

        # 验证回测指标完整性
        if not metrics:
            raise RuntimeError(
                "分布式统一回测: get_loop_metrics 返回空数据"
            )

        # 提取标准指标
        result = {}
        summary = metrics.get("summary", metrics)
        for key in ("IC", "ICIR", "Rank IC", "Rank ICIR",
                    "annualized_return", "max_drawdown", "information_ratio",
                    "sharpe", "calmar"):
            if key in summary:
                result[key] = summary[key]

        # 验证回测核心指标存在（IC 来自 SigAnaRecord，annualized_return 来自 PortAnaRecord）
        if "Rank IC" not in result and "IC" not in result:
            raise RuntimeError(
                f"分布式统一回测: 回测完成但缺少 IC 指标。"
                f"get_loop_metrics 返回的 summary keys: {list(summary.keys())}"
            )

        # 保存完整的 enhanced_metrics（供统一分析层使用）
        result["_enhanced_metrics"] = metrics

        logger.info(
            f"分布式统一回测指标: IC={result.get('IC')}, "
            f"annualized_return={result.get('annualized_return')}, "
            f"max_drawdown={result.get('max_drawdown')}"
        )
        return result

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
        validated_groups = []
        for g in groups:
            g_name = g["group_name"]
            gm = group_metrics.get(g_name)
            if gm is None:
                raise RuntimeError(f"group_metrics missing for group: {g_name}")
            weight = meta_weights.get(g_name)
            if weight is None:
                raise RuntimeError(f"meta_weight missing for group: {g_name}")

            # Validate metrics before opening a DB connection; fail-fast tests can
            # exercise the protocol without requiring a live database.
            g_ic = gm.get("ic") if gm.get("ic") is not None else gm.get("IC")
            g_icir = gm.get("icir") if gm.get("icir") is not None else gm.get("ICIR")
            g_sharpe = gm.get("sharpe") if gm.get("sharpe") is not None else gm.get("information_ratio")
            if g_ic is None or g_icir is None or g_sharpe is None:
                raise RuntimeError(f"group_metrics missing key metrics for group: {g_name}")
            validated_groups.append((g, g_name, g_ic, g_icir, g_sharpe, weight))

        results = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                for g, g_name, g_ic, g_icir, g_sharpe, weight in validated_groups:
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

    def _update_distributed_group_records(
        self,
        parent_experiment_id: str,
        groups: list[dict],
        group_metrics: dict[str, dict],
        meta_weights: dict[str, float],
    ) -> None:
        """分布式模式下回写 per-group IC/ICIR/weight 到 qe_multi_alpha_groups。

        与 _update_group_records 不同：不要求 sharpe 必须存在（train-only 无回测）。
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                for g in groups:
                    g_name = g["group_name"]
                    gm = group_metrics.get(g_name, {})
                    weight = meta_weights.get(g_name)
                    g_ic = gm.get("IC") or gm.get("Rank IC")
                    g_icir = gm.get("ICIR") or gm.get("Rank ICIR")
                    cur.execute(
                        """UPDATE qe_multi_alpha_groups
                           SET group_ic = %s, group_icir = %s,
                               meta_weight = %s
                           WHERE parent_experiment_id = %s AND group_name = %s""",
                        (
                            float(g_ic) if g_ic is not None else None,
                            float(g_icir) if g_icir is not None else None,
                            float(weight) if weight is not None else None,
                            parent_experiment_id,
                            g_name,
                        ),
                    )
            conn.commit()
        logger.info(f"分布式回写 {len(groups)} 组指标: {parent_experiment_id}")

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
            raise RuntimeError(f"meta_weights 为空，无法写入: {experiment_id}")

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
            raise RuntimeError(f"correlations 为空，无法写入: {experiment_id}")

        with get_conn() as conn:
            with conn.cursor() as cur:
                for pair_key, corr_val in correlations.items():
                    parts = pair_key.split("|")
                    if len(parts) != 2:
                        raise ValueError(f"相关性 key 格式错误: {pair_key}")
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
        group_enhanced_metrics: dict[str, dict] | None = None,
        ic_quality: dict[str, Any] | None = None,
    ) -> dict:
        """构建 result_metrics JSON，兼容单Alpha格式 + multi_alpha_detail 嵌套。

        如果 combined_metrics 包含 _enhanced_metrics（来自统一回测的完整
        qlib_results_enhanced.json），将其作为 enhanced_metrics 嵌套写入，
        使前端统一分析层 /enhanced-metrics 端点能正常返回所有诊断数据。
        """
        result = {}

        # 提取统一回测的完整增强指标（如果有）
        enhanced = combined_metrics.pop("_enhanced_metrics", None)
        if enhanced and isinstance(enhanced, dict):
            result["enhanced_metrics"] = enhanced

        # 顶层放 combined_metrics 的标量指标
        for k, v in combined_metrics.items():
            if k.startswith("_"):
                continue
            result[k] = v

        combined_ic = combined_metrics.get("IC") or combined_metrics.get("Rank IC")
        multi_alpha_detail = {
            "meta_method": meta_method,
            "meta_weights": meta_weights,
            "combined_ic": float(combined_ic) if combined_ic is not None else None,
            "total_groups": len(group_results),
            "group_results": group_results,
            "group_correlations": correlations,
            "ic_quality": ic_quality or {},
        }
        result["multi_alpha_detail"] = multi_alpha_detail

        multi_alpha_analysis = self._build_multi_alpha_analysis(
            combined_metrics=combined_metrics,
            combined_enhanced=enhanced if isinstance(enhanced, dict) else {},
            multi_alpha_detail=multi_alpha_detail,
            group_enhanced_metrics=group_enhanced_metrics or {},
            ic_quality=ic_quality or {},
        )
        result["multi_alpha_analysis"] = multi_alpha_analysis

        if isinstance(result.get("enhanced_metrics"), dict):
            result["enhanced_metrics"]["multi_alpha_detail"] = multi_alpha_detail
            result["enhanced_metrics"]["multi_alpha_analysis"] = multi_alpha_analysis

        return result

    def _build_multi_alpha_analysis(
        self,
        combined_metrics: dict,
        combined_enhanced: dict,
        multi_alpha_detail: dict,
        group_enhanced_metrics: dict[str, dict],
        ic_quality: dict[str, Any] | None = None,
    ) -> dict:
        """Build compact multi-alpha diagnostics without storing full group artifacts."""
        summary = combined_enhanced.get("summary") if isinstance(combined_enhanced, dict) else {}
        if not isinstance(summary, dict):
            summary = {}
        summary_source = summary or combined_metrics

        group_results = multi_alpha_detail.get("group_results") or []
        meta_weights = multi_alpha_detail.get("meta_weights") or {}
        correlations = multi_alpha_detail.get("group_correlations") or {}
        combined_ic = self._safe_float(multi_alpha_detail.get("combined_ic"))
        ic_quality = ic_quality or multi_alpha_detail.get("ic_quality") or {}
        if not isinstance(ic_quality, dict):
            ic_quality = {}

        group_diagnostics: list[dict[str, Any]] = []
        for group in group_results:
            if not isinstance(group, dict):
                continue
            group_name = group.get("group_name")
            if not group_name:
                continue
            enhanced = group_enhanced_metrics.get(group_name) or {}
            group_summary = enhanced.get("summary") if isinstance(enhanced, dict) else {}
            if not isinstance(group_summary, dict):
                group_summary = {}
            ic_diag = enhanced.get("ic_diagnostics") if isinstance(enhanced, dict) else {}
            if not isinstance(ic_diag, dict):
                ic_diag = {}
            train_diag = enhanced.get("training_diagnostics") if isinstance(enhanced, dict) else {}
            if not isinstance(train_diag, dict):
                train_diag = {}
            pred_diag = enhanced.get("prediction_diagnostics") if isinstance(enhanced, dict) else {}
            if not isinstance(pred_diag, dict):
                pred_diag = {}

            train_curve = train_diag.get("train_loss_curve")
            val_curve = train_diag.get("val_loss_curve")
            final_train = self._first_present_number(
                self._safe_float(train_diag.get("final_train_loss")),
                self._last_number(train_curve),
                self._first_number(group_summary, ("l2.train", "train_loss", "loss.train")),
            )
            final_val = self._first_present_number(
                self._safe_float(train_diag.get("final_val_loss")),
                self._last_number(val_curve),
                self._first_number(group_summary, ("l2.valid", "valid_loss", "loss.valid")),
            )
            overfit_ratio = self._safe_float(train_diag.get("overfit_ratio"))
            if overfit_ratio is None and final_train not in (None, 0) and final_val is not None:
                overfit_ratio = final_val / final_train
            generalization_gap = None
            if final_train is not None and final_val is not None:
                generalization_gap = final_val - final_train

            group_ic = self._safe_float(group.get("ic"))
            group_weight = self._safe_float(group.get("meta_weight"))
            contribution = None
            if group_ic is not None and group_weight is not None:
                contribution = group_ic * group_weight

            group_diagnostics.append({
                "group_name": group_name,
                "model_id": group.get("model_id"),
                "factor_count": group.get("factor_count"),
                "meta_weight": group_weight,
                "group_ic": group_ic,
                "group_icir": self._safe_float(group.get("icir")),
                "group_sharpe": self._safe_float(group.get("sharpe")),
                "contribution_to_combined_ic": contribution,
                "data_available": {
                    "enhanced_metrics": bool(enhanced),
                    "ic_diagnostics": bool(ic_diag),
                    "training_diagnostics": bool(train_diag) or final_train is not None or final_val is not None,
                    "prediction_diagnostics": bool(pred_diag),
                    "feature_importance": bool(self._top_feature_importance(enhanced, limit=1)),
                },
                "ic_diagnostics": {
                    "ic_mean": self._first_present_number(
                        self._safe_float(ic_diag.get("ic_mean")),
                        self._safe_float(group_summary.get("IC")),
                    ),
                    "rank_ic_mean": self._first_present_number(
                        self._safe_float(ic_diag.get("rank_ic_mean")),
                        self._safe_float(group_summary.get("Rank IC")),
                    ),
                    "ic_positive_ratio": self._safe_float(ic_diag.get("ic_positive_ratio")),
                    "rank_ic_positive_ratio": self._safe_float(ic_diag.get("rank_ic_positive_ratio")),
                    "ic_days": len(ic_diag.get("ic_series") or []),
                },
                "training_diagnostics": {
                    "train_loss_points": len(train_curve) if isinstance(train_curve, list) else 0,
                    "val_loss_points": len(val_curve) if isinstance(val_curve, list) else 0,
                    "final_train_loss": final_train,
                    "final_val_loss": final_val,
                    "generalization_gap": generalization_gap,
                    "overfit_ratio": overfit_ratio,
                    "best_epoch": train_diag.get("best_epoch"),
                    "convergence_ratio": self._safe_float(train_diag.get("convergence_ratio")),
                },
                "prediction_diagnostics": {
                    "pred_std": self._safe_float(pred_diag.get("pred_std")),
                    "pred_autocorr_1d": self._safe_float(pred_diag.get("pred_autocorr_1d")),
                    "pred_rank_turnover": self._safe_float(pred_diag.get("pred_rank_turnover")),
                    "top30_stability": self._safe_float(pred_diag.get("top30_stability")),
                },
                "feature_importance_top": self._top_feature_importance(enhanced, limit=10),
            })

        weights = [self._safe_float(v) for v in meta_weights.values()]
        weights = [w for w in weights if w is not None]
        weight_sum = sum(weights)
        hhi = sum(w * w for w in weights) if weights else None
        effective_groups = (1.0 / hhi) if hhi and hhi > 0 else None
        dominant_group = None
        if meta_weights:
            dominant_group = max(meta_weights, key=lambda k: self._safe_float(meta_weights.get(k)) or 0)

        corr_values = [abs(self._safe_float(v) or 0.0) for v in correlations.values()]
        high_corr_pairs = [
            {"pair": k, "correlation": self._safe_float(v)}
            for k, v in correlations.items()
            if abs(self._safe_float(v) or 0.0) >= 0.7
        ]

        portfolio_diagnostics = self._build_portfolio_diagnostics(summary_source, combined_enhanced)
        data_availability = {
            "combined_enhanced_metrics": bool(combined_enhanced),
            "combined_ic_diagnostics": bool(combined_enhanced.get("ic_diagnostics")) if isinstance(combined_enhanced, dict) else False,
            "combined_return_curves": bool(combined_enhanced.get("return_curves")) if isinstance(combined_enhanced, dict) else False,
            "combined_trade_diagnostics": bool(combined_enhanced.get("trade_diagnostics")) if isinstance(combined_enhanced, dict) else False,
            "combined_prediction_diagnostics": bool(combined_enhanced.get("prediction_diagnostics")) if isinstance(combined_enhanced, dict) else False,
            "combined_training_diagnostics": bool(combined_enhanced.get("training_diagnostics")) if isinstance(combined_enhanced, dict) else False,
            "groups_with_enhanced_metrics": len(group_enhanced_metrics),
            "groups_total": len(group_results),
            "ic_quality": bool(ic_quality),
            "missing_group_enhanced_metrics": [
                g.get("group_name")
                for g in group_results
                if isinstance(g, dict) and g.get("group_name") not in group_enhanced_metrics
            ],
        }

        analysis = {
            "schema_version": 1,
            "combined_vs_groups": {
                "combined_ic": combined_ic,
                "best_group_ic": max(
                    (d.get("group_ic") for d in group_diagnostics if d.get("group_ic") is not None),
                    default=None,
                ),
                "weighted_group_ic": sum(
                    d.get("contribution_to_combined_ic") or 0.0 for d in group_diagnostics
                ) if group_diagnostics else None,
            },
            "portfolio_diagnostics": portfolio_diagnostics,
            "diversification": {
                "weight_sum": weight_sum,
                "weight_hhi": hhi,
                "effective_group_count": effective_groups,
                "dominant_group": dominant_group,
                "dominant_weight": self._safe_float(meta_weights.get(dominant_group)) if dominant_group else None,
                "avg_abs_correlation": (sum(corr_values) / len(corr_values)) if corr_values else None,
                "max_abs_correlation": max(corr_values) if corr_values else None,
                "high_correlation_pairs": high_corr_pairs,
            },
            "group_diagnostics": group_diagnostics,
            "data_availability": data_availability,
            "ic_quality": ic_quality,
        }
        analysis["optimization_guidance"] = self._generate_multi_alpha_guidance(analysis)
        return analysis

    def _build_portfolio_diagnostics(self, summary: dict, combined_enhanced: dict) -> dict:
        trade_diag = combined_enhanced.get("trade_diagnostics") if isinstance(combined_enhanced, dict) else {}
        if not isinstance(trade_diag, dict):
            trade_diag = {}
        pred_diag = combined_enhanced.get("prediction_diagnostics") if isinstance(combined_enhanced, dict) else {}
        if not isinstance(pred_diag, dict):
            pred_diag = {}

        ann_no_cost = self._first_number(summary, (
            "1day.excess_return_without_cost.annualized_return",
            "annualized_return_no_cost",
            "ann_return_no_cost",
        ))
        ann_with_cost = self._first_number(summary, (
            "1day.excess_return_with_cost.annualized_return",
            "annualized_return",
            "ann_return_with_cost",
        ))
        cost_drag = None
        if ann_no_cost is not None and ann_with_cost is not None:
            cost_drag = ann_with_cost - ann_no_cost

        return {
            "annualized_return_no_cost": ann_no_cost,
            "annualized_return_with_cost": ann_with_cost,
            "cost_drag_annualized": cost_drag,
            "max_drawdown_no_cost": self._first_number(summary, (
                "1day.excess_return_without_cost.max_drawdown",
                "max_drawdown_no_cost",
            )),
            "max_drawdown_with_cost": self._first_number(summary, (
                "1day.excess_return_with_cost.max_drawdown",
                "max_drawdown",
            )),
            "information_ratio_with_cost": self._first_number(summary, (
                "1day.excess_return_with_cost.information_ratio",
                "information_ratio",
            )),
            "avg_turnover": self._safe_float(trade_diag.get("avg_turnover")),
            "annualized_turnover": self._safe_float(trade_diag.get("annualized_turnover")),
            "pred_rank_turnover": self._safe_float(pred_diag.get("pred_rank_turnover")),
            "top30_stability": self._safe_float(pred_diag.get("top30_stability")),
        }

    def _generate_multi_alpha_guidance(self, analysis: dict) -> list[dict[str, Any]]:
        guidance: list[dict[str, Any]] = []
        combined = analysis.get("combined_vs_groups") or {}
        combined_ic = self._safe_float(combined.get("combined_ic"))
        best_group_ic = self._safe_float(combined.get("best_group_ic"))
        if combined_ic is not None and best_group_ic is not None and best_group_ic - combined_ic > 0.01:
            guidance.append({
                "rule_id": "combined_underperforms_best_group",
                "severity": "medium",
                "message": "组合 IC 明显低于最佳单组，Meta 权重或组间冲突需要复核。",
                "recommendation": "优先尝试调整 Meta 方法、lookback_days，或降低弱组权重后重跑统一回测。",
                "action_type": "tune_meta",
                "affected_groups": [],
                "evidence": {"combined_ic": combined_ic, "best_group_ic": best_group_ic},
                "source_fields": ["multi_alpha_analysis.combined_vs_groups"],
            })

        diversification = analysis.get("diversification") or {}
        dominant_weight = self._safe_float(diversification.get("dominant_weight"))
        if dominant_weight is not None and dominant_weight > 0.6:
            guidance.append({
                "rule_id": "weight_concentration",
                "severity": "medium",
                "message": "Meta 权重集中在单一 Alpha 组，多 Alpha 分散收益有限。",
                "recommendation": "补强低权重组的因子质量，或新增低相关数据源组后再训练。",
                "action_type": "add_factors",
                "affected_groups": [diversification.get("dominant_group")] if diversification.get("dominant_group") else [],
                "evidence": {"dominant_weight": dominant_weight, "effective_group_count": diversification.get("effective_group_count")},
                "source_fields": ["multi_alpha_analysis.diversification"],
            })

        for pair in diversification.get("high_correlation_pairs") or []:
            pair_key = pair.get("pair") or ""
            groups = pair_key.split("|") if "|" in pair_key else []
            guidance.append({
                "rule_id": "high_group_correlation",
                "severity": "medium",
                "message": "存在高相关 Alpha 组，组合信号可能重复。",
                "recommendation": "合并高相关组，或移除其中一组的重叠因子后重新验证。",
                "action_type": "merge_groups",
                "affected_groups": groups,
                "evidence": pair,
                "source_fields": ["multi_alpha_analysis.diversification.high_correlation_pairs"],
            })

        for group in analysis.get("group_diagnostics") or []:
            group_name = group.get("group_name")
            group_ic = self._safe_float(group.get("group_ic"))
            weight = self._safe_float(group.get("meta_weight"))
            if group_name and group_ic is not None and weight is not None and group_ic < -0.005 and weight > 0.05:
                guidance.append({
                    "rule_id": "negative_weighted_group",
                    "severity": "high",
                    "message": f"{group_name} 组 IC 为负且仍有有效权重，会拖累组合。",
                    "recommendation": "优先移除该组、替换模型，或重新筛选该组因子后再纳入 Meta。",
                    "action_type": "switch_model",
                    "affected_groups": [group_name],
                    "evidence": {"group_ic": group_ic, "meta_weight": weight},
                    "source_fields": ["multi_alpha_analysis.group_diagnostics"],
                })

            training = group.get("training_diagnostics") or {}
            overfit_ratio = self._safe_float(training.get("overfit_ratio"))
            if group_name and overfit_ratio is not None and overfit_ratio > 1.2:
                guidance.append({
                    "rule_id": "group_overfit_risk",
                    "severity": "medium",
                    "message": f"{group_name} 组训练/验证损失差异偏大，存在过拟合风险。",
                    "recommendation": "降低模型复杂度、增加正则化，或缩短/重筛该组高噪声因子。",
                    "action_type": "switch_model",
                    "affected_groups": [group_name],
                    "evidence": training,
                    "source_fields": ["multi_alpha_analysis.group_diagnostics.training_diagnostics"],
                })

            prediction = group.get("prediction_diagnostics") or {}
            rank_turnover = self._safe_float(prediction.get("pred_rank_turnover"))
            top30_stability = self._safe_float(prediction.get("top30_stability"))
            if group_name and (
                (rank_turnover is not None and rank_turnover > 0.75)
                or (top30_stability is not None and top30_stability < 0.25)
            ):
                guidance.append({
                    "rule_id": "group_prediction_instability",
                    "severity": "low",
                    "message": f"{group_name} 组预测排名稳定性偏弱。",
                    "recommendation": "检查该组高频噪声因子，必要时调低换手或增加预测平滑约束。",
                    "action_type": "add_factors",
                    "affected_groups": [group_name],
                    "evidence": prediction,
                    "source_fields": ["multi_alpha_analysis.group_diagnostics.prediction_diagnostics"],
                })

        portfolio = analysis.get("portfolio_diagnostics") or {}
        annualized_turnover = self._safe_float(portfolio.get("annualized_turnover"))
        cost_drag = self._safe_float(portfolio.get("cost_drag_annualized"))
        if (annualized_turnover is not None and annualized_turnover > 30) or (
            cost_drag is not None and cost_drag < -0.02
        ):
            guidance.append({
                "rule_id": "combined_turnover_cost_risk",
                "severity": "low",
                "message": "组合回测换手或交易成本拖累偏高。",
                "recommendation": "调高持仓稳定性约束、降低 topk/n_drop 换仓强度，或重新评估交易成本参数。",
                "action_type": "tune_meta",
                "affected_groups": [],
                "evidence": {"annualized_turnover": annualized_turnover, "cost_drag_annualized": cost_drag},
                "source_fields": ["multi_alpha_analysis.portfolio_diagnostics"],
            })

        severity_order = {"high": 0, "medium": 1, "low": 2}
        return sorted(guidance, key=lambda item: severity_order.get(item.get("severity"), 3))

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _first_number(self, data: dict, keys: tuple[str, ...]) -> float | None:
        for key in keys:
            value = self._safe_float(data.get(key))
            if value is not None:
                return value
        return None

    def _last_number(self, value: Any) -> float | None:
        if isinstance(value, list) and value:
            return self._safe_float(value[-1])
        return None

    @staticmethod
    def _first_present_number(*values: float | None) -> float | None:
        for value in values:
            if value is not None:
                return value
        return None

    def _top_feature_importance(self, enhanced: dict, limit: int = 10) -> list[dict[str, Any]]:
        if not isinstance(enhanced, dict):
            return []
        factor_analysis = enhanced.get("factor_analysis") or {}
        if not isinstance(factor_analysis, dict):
            return []
        raw = factor_analysis.get("feature_importance") or enhanced.get("feature_importance")
        if isinstance(raw, list):
            return [x for x in raw[:limit] if isinstance(x, dict)]
        if isinstance(raw, dict):
            rows = [{"name": k, "importance": v} for k, v in raw.items()]
            return rows[:limit]
        return []

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
