import json
import logging
import os
import asyncio
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import aiofiles
from psycopg2.extras import RealDictCursor
from ...db.pg_pool import get_conn

from .qe_workspace_client import QEWorkspaceClient
from .qe_evolution_agents import EvolutionAgents, EvolutionFactorAgent, EvolutionModelAgent, AnalystResult

logger = logging.getLogger(__name__)

SOTA_ASSETS_DIR = os.environ.get("QE_SOTA_ASSETS_DIR", "f:/Dev/AIstock/rdagent_assets/qe_sota_assets")

class AutoEvolutionScheduler:
    """
    自动演进任务的核心调度器与状态机
    负责在 AIstock 侧控制演进流程的流转 (LOOP)，并调用 Agent 和 RDAgent API。
    """
    def __init__(self):
        self.workspace_client = QEWorkspaceClient()
        self.agents = EvolutionAgents()
        self.factor_agent = EvolutionFactorAgent(agents=self.agents)
        self.model_agent = EvolutionModelAgent(agents=self.agents)
        self._node_clients: Dict[str, QEWorkspaceClient] = {}

    def _get_workspace_client_for_task(self, task_id: str) -> QEWorkspaceClient:
        """根据 task 的 node_id 返回对应节点的 workspace 客户端。无 node_id 时返回默认客户端。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT node_id FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
        if row and row.get("node_id"):
            node_id = row["node_id"]
            if node_id not in self._node_clients:
                self._node_clients[node_id] = QEWorkspaceClient.for_node(node_id)
            return self._node_clients[node_id]
        return self.workspace_client

    def _get_callback_url_for_task(self, task_id: str) -> Optional[str]:
        """查询任务关联节点的 callback_url，用于 Loop 完成后主动回调。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT cn.callback_url
                    FROM qe_evolution_tasks t
                    LEFT JOIN infra.compute_nodes cn ON t.node_id = cn.node_id
                    WHERE t.task_id = %s
                """, (task_id,))
                row = cur.fetchone()
        return row.get("callback_url") if row else None
        
    async def create_task(self, task_name: str, target_desc: str, max_loops: int, base_experiment_id: str, allow_created: bool = False, start_from_loop_zero: bool = False, node_id: Optional[str] = None, stock_pool: Optional[str] = None) -> str:
        """
        创建演进任务并写入数据库。
        支持三种场景：
        1. 从单次实验开始演进（base_experiment_id 是主实验）
        2. 从之前演进的某个子 Loop 继续演进（base_experiment_id 是子实验如 qe_xxx_L3）
        3. 恢复已有演进任务（使用 resume_task 方法）

        task_id 复用根实验ID，current_loop 从已有最大 loop_index 开始。
        allow_created: 为 True 时允许 status='created' 的实验（rdagent_task_sota 场景，Loop 1 即为初始回测）。
        start_from_loop_zero: 为 True 时从 Loop0 开始（rdagent_task_sota 场景，Loop1 为初始回测）。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT experiment_id, status, parent_experiment_id, is_evolution_loop, loop_index FROM qe_experiments WHERE experiment_id = %s", (base_experiment_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"base_experiment_id not found: {base_experiment_id}")
                allowed_statuses = {'completed', 'created'} if allow_created else {'completed'}
                if row['status'] not in allowed_statuses:
                    raise ValueError(f"基础实验尚未完成（当前状态: {row['status']}），无法开始演进")

        # 确定根实验ID（如果 base 是子 Loop，追溯到根）
        root_experiment_id = base_experiment_id
        # rdagent_task_sota: 从 0 开始，Loop1 为初始回测
        # qe_experiment: 从 1 开始，基础实验已完成相当于 Loop1
        start_loop_index = 0 if start_from_loop_zero else 1
        if row.get('is_evolution_loop') and row.get('parent_experiment_id'):
            root_experiment_id = row['parent_experiment_id']
            start_loop_index = row.get('loop_index', 1)

        # 检查是否已有该根实验的演进任务
        task_id = root_experiment_id
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT task_id, status, current_loop FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                existing_task = cur.fetchone()

        if existing_task:
            if existing_task['status'] == 'running':
                raise ValueError(f"该实验已有正在运行的演进任务: {task_id}")
            # 已有任务但已完成/暂停/失败 → 更新为新一轮
            if start_from_loop_zero:
                actual_start = 0  # rdagent_task_sota: 从头开始，Loop1 为初始回测
            else:
                actual_start = max(existing_task['current_loop'], start_loop_index)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_tasks
                        SET task_name = %s, target_desc = %s, max_loops = %s,
                            current_loop = %s, status = 'pending',
                            base_experiment_id = %s, node_id = COALESCE(%s, node_id), updated_at = NOW()
                        WHERE task_id = %s
                    """, (task_name, target_desc, actual_start + max_loops, actual_start, base_experiment_id, node_id, task_id))
                conn.commit()
            logger.info(f"Updated existing evolution task {task_id}: start_loop={actual_start}, max_loops={actual_start + max_loops}")
        else:
            # 查找已有的最大 loop_index（可能之前有手动演进的子 Loop）
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(MAX(loop_index), 0) FROM qe_experiments WHERE parent_experiment_id = %s",
                        (root_experiment_id,),
                    )
                    max_existing_loop = cur.fetchone()[0]

            actual_start = max(max_existing_loop, start_loop_index)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO qe_evolution_tasks
                        (task_id, task_name, target_desc, max_loops, current_loop, status, base_experiment_id, node_id, stock_pool)
                        VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s)
                    """, (task_id, task_name, target_desc, actual_start + max_loops, actual_start, base_experiment_id, node_id, stock_pool))
                conn.commit()
            logger.info(f"Created evolution task {task_id}: start_loop={actual_start}, max_loops={actual_start + max_loops}")
            
        return task_id

    def _parse_json_field(self, value: Any, field_name: str) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        raise ValueError(f"Invalid JSON field for {field_name}: {value}")

    def _load_base_config_from_experiment(self, base_experiment_id: str) -> Dict[str, Any]:
        """
        从基础实验加载配置。支持主实验和子 Loop 实验。
        如果 base_experiment_id 是子 Loop（有 parent_experiment_id），
        则从该子 Loop 的配置加载（继承其演进结果）。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT factor_names, model_id, strategy_id, data_split, custom_params,
                           parent_experiment_id, is_evolution_loop
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (base_experiment_id,),
                )
                row = cur.fetchone()

        if not row:
            raise ValueError(f"Base experiment not found: {base_experiment_id}")

        factor_names = row.get("factor_names")
        if isinstance(factor_names, str):
            factor_names = json.loads(factor_names)
        if not isinstance(factor_names, list) or len(factor_names) == 0:
            raise ValueError(f"Base experiment {base_experiment_id} has invalid factor_names")

        data_split = self._parse_json_field(row.get("data_split"), "data_split")
        custom_params = self._parse_json_field(row.get("custom_params"), "custom_params")

        # 如果基础实验是子 Loop（演进产物），action_type 应为 param_tune（继续演进）
        # 如果是主实验（首次演进），action_type 为 initial
        action_type = "param_tune" if row.get("is_evolution_loop") else "initial"

        return {
            "action_type": action_type,
            "factor_list": factor_names,
            "model_id": row.get("model_id"),
            "strategy_id": row.get("strategy_id"),
            "data_split": data_split,
            "model_params": custom_params,
            "base_experiment_id": base_experiment_id,
        }

    # ================================================================
    # 因子可用性验证 + 因子黑名单
    # ================================================================

    def _query_factor_correlation_pairs(self, factor_names: List[str], threshold: float = 0.0) -> List[Dict]:
        """
        查询因子列表中所有已计算的相关性对。
        threshold: |correlation| 过滤阈值，默认 0 返回全部。
        返回 [{factor_a, factor_b, correlation}]，按 |correlation| DESC 排序。
        """
        if len(factor_names) < 2:
            return []

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # factor_name → catalog_id 映射
                ph = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT DISTINCT ON (factor_name) factor_name, id
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({ph})
                    ORDER BY factor_name, id
                """, factor_names)
                id_map = {row["factor_name"]: row["id"] for row in cur.fetchall()}
                id_to_name = {v: k for k, v in id_map.items()}
                ids = list(id_map.values())

                if len(ids) < 2:
                    return []

                cur.execute("""
                    SELECT factor_a_id, factor_b_id, correlation
                    FROM qe_factor_correlations
                    WHERE factor_a_id = ANY(%s)
                      AND factor_b_id = ANY(%s)
                      AND ABS(correlation) >= %s
                    ORDER BY ABS(correlation) DESC
                """, (ids, ids, threshold))

                pairs = []
                for row in cur.fetchall():
                    pairs.append({
                        "factor_a": id_to_name.get(row["factor_a_id"], f"id={row['factor_a_id']}"),
                        "factor_b": id_to_name.get(row["factor_b_id"], f"id={row['factor_b_id']}"),
                        "correlation": float(row["correlation"]),
                    })
                return pairs

    def validate_factor_availability(self, factor_list: List[str]) -> Dict[str, Any]:
        """
        检查因子列表中的可用性问题:
        1. 已删除因子（不在 catalog 中）
        2. 不可用因子（is_available=FALSE）
        3. 高相关性因子对（|corr| > 0.7，仅警告）
        """
        if not factor_list:
            return {"has_issues": False, "deleted_factors": [], "unavailable_factors": [],
                    "valid_factors": [], "high_corr_pairs": [], "warnings": []}

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 查询所有存在且可用的因子
                placeholders = ",".join(["%s"] * len(factor_list))
                cur.execute(f"""
                    SELECT factor_name, is_available
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({placeholders})
                """, factor_list)
                catalog_map = {row["factor_name"]: row["is_available"] for row in cur.fetchall()}

                deleted_factors = [f for f in factor_list if f not in catalog_map]
                unavailable_factors = [f for f in factor_list if catalog_map.get(f) is False]
                valid_factors = [f for f in factor_list
                                 if f in catalog_map and catalog_map[f] is not False]

        # 复用 _query_factor_correlation_pairs，只取 |corr| > 0.7
        high_corr_pairs = self._query_factor_correlation_pairs(valid_factors, threshold=0.7)

        warnings = []
        if high_corr_pairs:
            warnings.append(
                f"存在 {len(high_corr_pairs)} 对高相关因子 (|corr| > 0.7)"
            )

        has_issues = len(deleted_factors) > 0 or len(unavailable_factors) > 0
        if deleted_factors:
            warnings.insert(0, f"{len(deleted_factors)} 个因子已被删除: {deleted_factors}")
        if unavailable_factors:
            warnings.insert(0, f"{len(unavailable_factors)} 个因子已标记不可用: {unavailable_factors}")

        return {
            "has_issues": has_issues,
            "deleted_factors": deleted_factors,
            "unavailable_factors": unavailable_factors,
            "valid_factors": valid_factors,
            "high_corr_pairs": high_corr_pairs,
            "warnings": warnings,
        }

    def _add_to_factor_blacklist(self, task_id: str, factor_names: List[str]):
        """将因子加入任务黑名单，后续演进不得引入"""
        if not factor_names:
            return
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_evolution_tasks
                    SET factor_blacklist = (
                        SELECT jsonb_agg(DISTINCT elem)
                        FROM (
                            SELECT jsonb_array_elements(COALESCE(factor_blacklist, '[]'::jsonb)) AS elem
                            UNION ALL
                            SELECT jsonb_array_elements(%s::jsonb)
                        ) sub
                    ),
                    updated_at = NOW()
                    WHERE task_id = %s
                """, (json.dumps(factor_names), task_id))
            conn.commit()
        logger.info(f"因子黑名单更新: task={task_id}, added={factor_names}")

    def _get_factor_blacklist(self, task_id: str) -> set:
        """获取任务的因子黑名单"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT factor_blacklist FROM qe_evolution_tasks WHERE task_id = %s",
                    (task_id,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    bl = row[0] if isinstance(row[0], list) else json.loads(row[0])
                    return set(bl)
        return set()

    def _compute_factor_context(self, factor_list: List[str]) -> Dict[str, Any]:
        """计算因子组合特征，供 ModelAgent 参考因子-模型兼容性。"""
        if not factor_list:
            return {"factor_count": 0, "category_distribution": {}, "ts_ratio": 0.0}
        cats: Dict[str, int] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(factor_list))
                cur.execute(f"""
                    SELECT factor_name, category
                    FROM qe_factor_classification
                    WHERE factor_name IN ({ph})
                """, factor_list)
                for name, cat in cur.fetchall():
                    cat_key = cat or "unknown"
                    cats[cat_key] = cats.get(cat_key, 0) + 1
        total = len(factor_list)
        ts_count = cats.get("ts_momentum", 0) + cats.get("ts_mean_reversion", 0) + cats.get("ts_volatility", 0)
        return {
            "factor_count": total,
            "category_distribution": cats,
            "ts_ratio": round(ts_count / max(total, 1), 2),
        }

    def _build_full_evolution_history(self, task_id: str) -> Dict[str, Any]:
        """
        构建完整演进历史（查询所有已完成 loop，不再 LIMIT 3）。
        供所有 Agent 做全局最优决策。
        如果 task 是 fork 且 inherit_history=True，先加载源 task 截止到 fork point 的历史。
        """
        inherited_rows = []
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 检查是否为 fork task 且需要继承历史
                cur.execute(
                    "SELECT fork_from_task_id, fork_from_loop_index, inherit_history "
                    "FROM qe_evolution_tasks WHERE task_id = %s",
                    (task_id,),
                )
                task_meta = cur.fetchone()
                if (
                    task_meta
                    and task_meta.get("inherit_history")
                    and task_meta.get("fork_from_task_id")
                    and task_meta.get("fork_from_loop_index") is not None
                ):
                    cur.execute("""
                        SELECT loop_index, action_type, config_json, metrics_json,
                               agent_analysis, is_sota
                        FROM qe_evolution_loops
                        WHERE task_id = %s AND status = 'completed'
                              AND loop_index <= %s
                        ORDER BY loop_index ASC
                    """, (task_meta["fork_from_task_id"], task_meta["fork_from_loop_index"]))
                    inherited_rows = cur.fetchall()

                cur.execute("""
                    SELECT loop_index, action_type, config_json, metrics_json,
                           agent_analysis, is_sota
                    FROM qe_evolution_loops
                    WHERE task_id = %s AND status = 'completed'
                    ORDER BY loop_index ASC
                """, (task_id,))
                rows = cur.fetchall()

        loops = []
        sota_loop_index = None
        sota_metrics = None
        ic_trend = []
        action_type_deltas: Dict[str, list] = {}
        failed_approaches = []
        prev_ic = None

        # 先处理继承的历史 rows（标记 inherited）
        all_rows = [(r, True) for r in inherited_rows] + [(r, False) for r in rows]

        for row, is_inherited in all_rows:
            config = row.get("config_json") or {}
            if isinstance(config, str):
                config = json.loads(config)
            metrics = row.get("metrics_json") or {}
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            analysis = row.get("agent_analysis") or {}
            if isinstance(analysis, str):
                analysis = json.loads(analysis)

            cur_ic = metrics.get("IC")

            delta_vs_previous = None
            if prev_ic is not None and cur_ic is not None:
                delta_vs_previous = {
                    "IC": round(cur_ic - prev_ic, 6),
                }

            action = row.get("action_type") or "initial"
            # 剥离 enhanced_metrics（含 top_stocks/stock_trades/return_curves 等大量数据）
            # 避免多 loop 累积后 context 超限；reviewer 只需核心指标
            metrics_summary = {k: v for k, v in metrics.items() if k != "enhanced_metrics"}
            loop_entry = {
                "loop_index": row["loop_index"],
                "action_type": action,
                "config_summary": {
                    "factors": config.get("factor_list", []),
                    "model_id": config.get("model_id"),
                    "model_params": config.get("model_params", {}),
                },
                "metrics": metrics_summary,
                "is_sota": bool(row.get("is_sota")),
                "analyst_report": (
                    analysis.get("analyst", {}).get("report_text", "")
                    if isinstance(analysis.get("analyst"), dict)
                    else analysis.get("analyst", "")
                ),
                "delta_vs_previous": delta_vs_previous,
            }
            if is_inherited:
                loop_entry["inherited"] = True
                if task_meta:
                    loop_entry["source_task_id"] = task_meta.get("fork_from_task_id", "")
            loops.append(loop_entry)

            # ── 以下统计指标只计算当前 task 的 loops（不含 inherited） ──
            # inherited rows 仅供 Agent 参考历史上下文，不影响决策统计
            if not is_inherited:
                ic_trend.append(cur_ic)

                if row.get("is_sota"):
                    sota_loop_index = row["loop_index"]
                    sota_metrics = metrics

                if delta_vs_previous and cur_ic is not None:
                    action_type_deltas.setdefault(action, []).append(
                        delta_vs_previous["IC"]
                    )

                if delta_vs_previous and delta_vs_previous["IC"] < 0:
                    failed_approaches.append({
                        "loop_index": row["loop_index"],
                        "action_type": action,
                        "ic_delta": delta_vs_previous["IC"],
                    })

            prev_ic = cur_ic

        # 汇总 action_type 统计 (S2: 新增 win_rate)
        action_type_stats = {}
        for at, deltas in action_type_deltas.items():
            wins = sum(1 for d in deltas if d > 0)
            action_type_stats[at] = {
                "count": len(deltas),
                "avg_ic_delta": round(sum(deltas) / len(deltas), 6) if deltas else 0,
                "win_rate": round(wins / len(deltas), 4) if deltas else 0,
            }

        # S2: 计算 consecutive_same_action（只统计当前 task 的 loops，不含 inherited）
        consecutive_same_action = {"action_type": None, "count": 0, "recent_ic_deltas": []}
        current_loops = [l for l in loops if not l.get("inherited")]
        if current_loops:
            last_action = current_loops[-1]["action_type"]
            consecutive = 0
            recent_deltas = []
            for loop_entry in reversed(current_loops):
                if loop_entry["action_type"] == last_action:
                    consecutive += 1
                    delta = loop_entry.get("delta_vs_previous")
                    if delta and delta.get("IC") is not None:
                        recent_deltas.append(delta["IC"])
                else:
                    break
            consecutive_same_action = {
                "action_type": last_action,
                "count": consecutive,
                "recent_ic_deltas": list(reversed(recent_deltas)),
            }

        # S2: 计算 unexplored_directions
        all_possible = {"factor_adjust", "param_tune", "model_switch"}
        explored = set(action_type_stats.keys())
        unexplored = list(all_possible - explored)

        # S2: 提取最近一轮的训练诊断
        latest_training = {}
        if loops:
            latest_metrics = loops[-1].get("metrics", {})
            em = latest_metrics.get("enhanced_metrics", {})
            td = em.get("training_diagnostics", {})
            if td:
                latest_training = {
                    "best_epoch": td.get("best_epoch"),
                    "convergence_ratio": td.get("convergence_ratio"),
                    "overfit_ratio": td.get("overfit_ratio"),
                    "training_failed": td.get("training_failed", False),
                }

        valid_ics = [x for x in ic_trend if x is not None]
        return {
            "total_loops": len(loops),
            "sota_loop_index": sota_loop_index,
            "sota_metrics": sota_metrics,
            "loops": loops,
            "trend_summary": {
                "ic_trend": ic_trend,
                "best_ic": max(valid_ics) if valid_ics else None,
                "action_type_stats": action_type_stats,
                "consecutive_same_action": consecutive_same_action,
                "unexplored_directions": unexplored,
            },
            "failed_approaches": failed_approaches,
            "latest_training_diagnostics": latest_training,
        }

    def _build_analysis_context(self, task_id: str, factor_names: List[str]) -> Dict[str, Any]:
        """
        S7: 构建 Agent 分析上下文（供 process_completed_loop 使用）。
        包含: factor_profiles, target_desc, evolution_guidance
        """
        analysis_context = {}

        # 因子画像
        factor_profiles = self._get_factor_profile(factor_names)
        if factor_profiles:
            analysis_context["factor_profiles"] = factor_profiles

        # 演进目标和指引
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT target_desc, evolution_guidance FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task_row = cur.fetchone()
        target_desc = (task_row or {}).get("target_desc", "")
        evolution_guidance = (task_row or {}).get("evolution_guidance", "")
        if target_desc:
            analysis_context["target_desc"] = target_desc
        if evolution_guidance:
            analysis_context["evolution_guidance"] = evolution_guidance

        return analysis_context

    def _get_evolution_mode(self, task_id: str) -> str:
        """获取任务的 evolution_mode 设置。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT evolution_mode FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
                return (row[0] if row and row[0] else "auto")

    def _get_sota_loop_config(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取 SOTA Loop 的完整配置（用于退化回滚）。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT l.config_json
                    FROM qe_sota_registry r
                    JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                    WHERE l.task_id = %s
                    ORDER BY r.created_at DESC LIMIT 1
                """, (task_id,))
                row = cur.fetchone()
                if row and row.get('config_json'):
                    cfg = row['config_json']
                    if isinstance(cfg, str):
                        cfg = json.loads(cfg)
                    return cfg
        return None

    def _check_sota_surpassed(self, evolution_history: Dict[str, Any]) -> bool:
        """
        检查 SOTA 是否已被超越。
        Returns: True if any post-SOTA loop has IC > SOTA IC + 0.002
        """
        sota_idx = evolution_history.get("sota_loop_index")
        sota_metrics = evolution_history.get("sota_metrics")
        if sota_idx is None or not sota_metrics:
            return False  # 无 SOTA 记录
        sota_ic = sota_metrics.get("IC", 0) or 0

        for lp in evolution_history.get("loops", []):
            if lp["loop_index"] <= sota_idx:
                continue
            lp_ic = (lp.get("metrics") or {}).get("IC")
            if lp_ic is not None and lp_ic > sota_ic + 0.002:
                return True
        return False

    def _decide_evolution_direction(
        self, analyst_result: 'AnalystResult', evolution_mode: str, metrics: Dict[str, Any],
        is_sota: bool = False, config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Phase 7: 方向控制器。
        Level 1: evolution_mode（用户设置） → 如非 auto 直接返回
        Level 1.5: auto 模式下因子数 < 15 → 强制 factor_expand
        Level 2: Analyst Step 2 LLM 决策结果 → auto 模式时使用

        注意: factor_adjust 方向始终允许通过，SOTA 因子保护由 Factor Agent
        的 add-only 机制实现（sota_protected_factors 参数），而非在此层阻止。
        """
        # Level 1: 用户指定模式
        if evolution_mode == "factor_only":
            return "factor_adjust"
        if evolution_mode == "model_only":
            diag = metrics.get("enhanced_metrics", {}).get("training_diagnostics", {})
            return "model_switch" if diag.get("training_failed") else "param_tune"
        if evolution_mode == "joint":
            return "factor_model_joint"

        # Level 1.5: auto 模式 — 因子数 < 15 时强制 factor_expand
        current_factor_count = len((config or {}).get("factor_list", []))
        if evolution_mode == "auto" and current_factor_count < 15:
            logger.info(f"因子数={current_factor_count} < 15，强制 factor_expand 扩充因子组合")
            return "factor_expand"

        # Level 2: auto — 使用 Analyst Step 2 的方向决策
        direction = analyst_result.direction
        recommended = direction.get("recommended_direction", "")
        valid_directions = {"factor_adjust", "param_tune", "model_switch", "factor_model_joint", "factor_expand"}
        if recommended in valid_directions:
            if is_sota and recommended in ("model_switch", "factor_model_joint"):
                logger.info(f"SOTA 轮选择了激进方向 {recommended}，回滚机制保障安全")
            return recommended

        logger.error(
            f"Analyst 返回无效的 recommended_direction: '{recommended}'，"
            f"有效值: {valid_directions}"
        )
        raise ValueError(
            f"无效的演进方向 '{recommended}'，Analyst 决策异常，"
            f"请检查 LLM 返回内容"
        )

    @staticmethod
    def _compute_training_diagnostics(training_curves: Dict[str, Any]) -> Dict[str, Any]:
        """从训练曲线数据派生收敛诊断指标。"""
        train_loss = training_curves.get("train_loss", [])
        val_loss = training_curves.get("val_loss", [])

        if not train_loss:
            return {"training_failed": True, "best_epoch": 0}

        best_epoch = 0
        best_val = float("inf")
        for i, v in enumerate(val_loss or train_loss):
            if v is not None and v < best_val:
                best_val = v
                best_epoch = i + 1

        total_epochs = len(train_loss)
        convergence_ratio = best_epoch / total_epochs if total_epochs > 0 else 0
        overfit_ratio = 0.0
        if val_loss and train_loss and len(val_loss) == len(train_loss):
            final_train = train_loss[-1] or 0
            final_val = val_loss[-1] or 0
            if final_train > 0:
                overfit_ratio = max(0, (final_val - final_train) / final_train)

        training_failed = best_epoch == 0 or (best_epoch == 1 and total_epochs > 3)
        loss_plateau = False
        if len(val_loss or train_loss) >= 5:
            recent = (val_loss or train_loss)[-5:]
            recent_valid = [x for x in recent if x is not None]
            if recent_valid and max(recent_valid) - min(recent_valid) < 0.001:
                loss_plateau = True

        return {
            "best_epoch": best_epoch,
            "total_epochs": total_epochs,
            "convergence_ratio": round(convergence_ratio, 4),
            "overfit_ratio": round(overfit_ratio, 4),
            "training_failed": training_failed,
            "loss_plateau": loss_plateau,
            "train_loss_final": train_loss[-1] if train_loss else None,
            "val_loss_final": val_loss[-1] if val_loss else None,
        }

    def _write_loop_factor_records(
        self, cur, task_id: str, loop_id: str, loop_index: int,
        config: Dict, metrics: Dict, action_type: str, is_sota: bool,
    ):
        """写入 qe_loop_factor_records，含 action_role 计算（S4）。"""
        curr_factors = set(config.get("factor_list", []))
        if not curr_factors:
            return

        # 获取上一轮因子列表以计算 action_role
        prev_factors: set = set()
        if loop_index > 1:
            prev_loop_id = f"{task_id}_Loop{loop_index - 1}"
            cur.execute(
                "SELECT config_json FROM qe_evolution_loops WHERE loop_id = %s",
                (prev_loop_id,),
            )
            prev_row = cur.fetchone()
            if prev_row:
                prev_config = prev_row[0] if isinstance(prev_row[0], dict) else json.loads(prev_row[0] or "{}")
                prev_factors = set(prev_config.get("factor_list", []))

        combo_ic = metrics.get("IC")
        combo_icir = metrics.get("ICIR")
        combo_sharpe = metrics.get("sharpe")
        combo_ann_return = metrics.get("annualized_return")
        combo_max_drawdown = metrics.get("max_drawdown")
        model_id = config.get("model_id")
        all_factors = list(curr_factors)

        # 解析 factor_catalog_id 和 model_catalog_id
        all_factor_names = curr_factors | (prev_factors - curr_factors)
        factor_catalog_map: Dict[str, tuple] = {}
        if all_factor_names:
            ph = ",".join(["%s"] * len(all_factor_names))
            cur.execute(f"""
                SELECT DISTINCT ON (factor_name) factor_name, id, source
                FROM aistock_factor_catalog
                WHERE factor_name IN ({ph})
                ORDER BY factor_name,
                         CASE WHEN source = 'rdagent_task_sync' THEN 0 ELSE 1 END,
                         id
            """, list(all_factor_names))
            for row in cur.fetchall():
                factor_catalog_map[row[0]] = (row[1], row[2])

        model_catalog_id = None
        cur.execute("""
            SELECT mc.id FROM qe_evolution_tasks et
            JOIN aistock_model_catalog mc ON mc.task_run_id = et.source_task_id
            WHERE et.task_id = %s LIMIT 1
        """, (task_id,))
        mc_row = cur.fetchone()
        if mc_row:
            model_catalog_id = mc_row[0]

        # 当前因子：kept / added
        for factor in curr_factors:
            fc_id, fc_source = factor_catalog_map.get(factor, (None, None))
            if fc_id is None:
                logger.warning(f"因子 {factor} 未在 catalog 中找到，跳过记录写入")
                continue
            role = "added" if factor not in prev_factors else "kept"
            other = [f for f in all_factors if f != factor]
            cur.execute("""
                INSERT INTO qe_loop_factor_records
                (task_id, loop_id, loop_index, factor_name, action_role,
                 combo_ic, combo_icir, combo_sharpe, combo_ann_return, combo_max_drawdown,
                 model_id, action_type, is_sota, other_factors,
                 factor_catalog_id, factor_source, model_catalog_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (loop_id, factor_name) DO NOTHING
            """, (
                task_id, loop_id, loop_index, factor, role,
                combo_ic, combo_icir, combo_sharpe, combo_ann_return, combo_max_drawdown,
                model_id, action_type, is_sota, json.dumps(other),
                fc_id, fc_source, model_catalog_id,
            ))

        # 被移除的因子
        for factor in prev_factors - curr_factors:
            fc_id, fc_source = factor_catalog_map.get(factor, (None, None))
            if fc_id is None:
                continue
            cur.execute("""
                INSERT INTO qe_loop_factor_records
                (task_id, loop_id, loop_index, factor_name, action_role,
                 model_id, action_type, is_sota,
                 factor_catalog_id, factor_source, model_catalog_id)
                VALUES (%s, %s, %s, %s, 'removed', %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (loop_id, factor_name) DO NOTHING
            """, (task_id, loop_id, loop_index, factor, model_id, action_type, is_sota,
                  fc_id, fc_source, model_catalog_id))

    def _write_loop_model_records(
        self, cur, task_id: str, loop_id: str, loop_index: int,
        config: Dict, metrics: Dict, action_type: str, is_sota: bool,
    ):
        """写入 qe_loop_model_records，含训练诊断数据。"""
        model_id = config.get("model_id")
        if not model_id:
            return

        # 解析 model_catalog_id
        model_catalog_id = None
        cur.execute("""
            SELECT mc.id FROM qe_evolution_tasks et
            JOIN aistock_model_catalog mc ON mc.task_run_id = et.source_task_id
            WHERE et.task_id = %s LIMIT 1
        """, (task_id,))
        mc_row = cur.fetchone()
        if mc_row:
            model_catalog_id = mc_row[0]

        if model_catalog_id is None:
            logger.warning(f"模型 catalog 未找到 (task={task_id})，跳过 model_records 写入")
            return

        em = metrics.get("enhanced_metrics", {})
        td = em.get("training_diagnostics", {})
        tc = em.get("training_curves", {})

        factor_list = config.get("factor_list", [])
        cur.execute("""
            INSERT INTO qe_loop_model_records
            (task_id, loop_id, loop_index, model_id, model_type,
             combo_ic, combo_icir, combo_sharpe, combo_ann_return, combo_max_drawdown,
             model_params, best_epoch, total_epochs, convergence_ratio, overfit_ratio,
             training_failed, train_loss_final, val_loss_final,
             train_loss_curve, val_loss_curve,
             action_type, is_sota, factor_count, factor_list,
             model_catalog_id)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s)
            ON CONFLICT (loop_id, model_id) DO NOTHING
        """, (
            task_id, loop_id, loop_index, model_id, config.get("model_type"),
            metrics.get("IC"), metrics.get("ICIR"), metrics.get("sharpe"),
            metrics.get("annualized_return"), metrics.get("max_drawdown"),
            json.dumps(config.get("model_params", {})),
            td.get("best_epoch"), td.get("total_epochs"),
            td.get("convergence_ratio"), td.get("overfit_ratio"),
            td.get("training_failed", False),
            td.get("train_loss_final"), td.get("val_loss_final"),
            json.dumps(tc.get("train_loss")) if tc.get("train_loss") else None,
            json.dumps(tc.get("val_loss")) if tc.get("val_loss") else None,
            action_type, is_sota, len(factor_list), json.dumps(factor_list),
            model_catalog_id,
        ))

    def _get_factor_profile(self, factor_names: List[str]) -> Dict[str, Any]:
        """获取因子画像数据（factor_profile）供演进Agent使用。"""
        if not factor_names:
            return {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT factor_name, factor_profile
                    FROM qe_factor_classification
                    WHERE factor_name IN ({ph}) AND factor_profile IS NOT NULL
                """, factor_names)
                return {row[0]: row[1] for row in cur.fetchall()}

    def _get_relevant_correlations(self, factor_names: List[str]) -> Dict[str, Any]:
        """查询当前因子组合的相关性矩阵（如果存在）。"""
        if not factor_names or len(factor_names) < 2:
            return {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 先获取 factor_name -> catalog_id 映射
                    ph = ",".join(["%s"] * len(factor_names))
                    cur.execute(f"""
                        SELECT DISTINCT ON (factor_name) factor_name, id
                        FROM aistock_factor_catalog
                        WHERE factor_name IN ({ph})
                        ORDER BY factor_name, id
                    """, factor_names)
                    id_map = {row[0]: row[1] for row in cur.fetchall()}
                    id_to_name = {v: k for k, v in id_map.items()}
                    factor_ids = list(id_map.values())

                    if len(factor_ids) < 2:
                        return {}

                    cur.execute("""
                        SELECT factor_a_id, factor_b_id, correlation
                        FROM qe_factor_correlations
                        WHERE factor_a_id = ANY(%s) AND factor_b_id = ANY(%s)
                    """, (factor_ids, factor_ids))
                    correlations = {}
                    for row in cur.fetchall():
                        name_a = id_to_name.get(row[0], "")
                        name_b = id_to_name.get(row[1], "")
                        key = f"{name_a}_{name_b}"
                        correlations[key] = row[2]
                    return correlations
        except Exception as e:
            logger.error(f"_get_relevant_correlations 查询失败: {e}", exc_info=True)
            raise RuntimeError(f"查询因子相关性失败: {e}") from e

    def _get_factor_library_summary(self) -> Dict[str, Any]:
        """获取因子库摘要（轻量版，每因子~30 tokens），供 researcher 做方向决策。"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT category, grade,
                               COUNT(*) as cnt,
                               AVG(ic_value) as avg_ic,
                               AVG(sharpe_value) as avg_sharpe
                        FROM qe_factor_classification
                        WHERE grade IS NOT NULL
                        GROUP BY category, grade
                        ORDER BY category, grade
                    """)
                    rows = cur.fetchall()

            summary: Dict[str, Any] = {}
            for row in rows:
                cat = row[0] or "UNKNOWN"
                grade = row[1] or "D"
                cnt = row[2]
                avg_ic = round(float(row[3]), 4) if row[3] is not None else None
                avg_sharpe = round(float(row[4]), 4) if row[4] is not None else None
                if cat not in summary:
                    summary[cat] = {"total": 0, "grades": {}}
                summary[cat]["total"] += cnt
                summary[cat]["grades"][grade] = {
                    "count": cnt,
                    "avg_ic": avg_ic,
                    "avg_sharpe": avg_sharpe,
                }
            return summary
        except Exception as e:
            raise RuntimeError(f"Failed to get factor library summary: {e}") from e

    async def submit_next_loop(self, task_id: str) -> Optional[str]:
        """
        提交下一个 Loop 到 RDAgent，立即返回（不阻塞等待执行完成）。
        Returns: evolution_loop_db_id if submitted, None if task is complete/stopped.
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return None

        if task['status'] not in ('pending', 'running'):
            logger.info(f"Task {task_id} is in state {task['status']}, cannot submit next loop.")
            return None

        current_loop = task['current_loop']
        max_loops = task['max_loops']

        if current_loop >= max_loops:
            logger.info(f"Task {task_id} has reached max_loops ({max_loops}), marking completed.")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()
            return None

        # Mark as running (CAS: only if task is in pending or running state)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s AND status IN ('pending', 'running')", (task_id,))
                if cur.rowcount == 0:
                    logger.warning(f"Task {task_id} CAS update failed: status not in (pending, running)")
                    return None
            conn.commit()

        loop_index = current_loop + 1
        loop_id = f"Loop{loop_index}"
        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        logger.info(f"Submitting Loop {loop_index} for task {task_id}")

        # ── Multi-Alpha 分流 (Phase 3) ──────────────────────────────
        # 从任务记录或基础实验检查 alpha_mode
        _alpha_mode = self._detect_alpha_mode(task)
        if _alpha_mode == "multi":
            return await self._submit_multi_alpha_loop(
                task, task_id, loop_index, evolution_loop_db_id
            )

        # 创建 LOOP 记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_evolution_loops
                    (loop_id, task_id, loop_index, status)
                    VALUES (%s, %s, %s, 'running')
                    ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()
                """, (evolution_loop_db_id, task_id, loop_index))
            conn.commit()

        # 加载配置：查看是否有上一轮 reviewer 输出的 next_config
        # F3: 统一异常捕获覆盖 config 加载 + ConfigComposer + RDAgent 提交
        try:
            config = None
            if loop_index > 1:
                prev_loop_db_id = f"{task_id}_Loop{loop_index - 1}"
                with get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT agent_analysis FROM qe_evolution_loops
                            WHERE loop_id = %s AND status = 'completed'
                        """, (prev_loop_db_id,))
                        prev_row = cur.fetchone()
                        if prev_row and prev_row.get('agent_analysis'):
                            analysis = prev_row['agent_analysis']
                            if isinstance(analysis, str):
                                analysis = json.loads(analysis)
                            reviewer_config = (analysis.get("reviewer") or {}).get("validated_config")
                            if isinstance(reviewer_config, dict):
                                config = reviewer_config
                                logger.info(f"Loop {loop_index}: 配置来源 = reviewer_config (prev loop {prev_loop_db_id})")
                            else:
                                logger.warning(f"Loop {loop_index}: 上轮 loop {prev_loop_db_id} 缺少 reviewer.validated_config")

            if config is None:
                config = self._load_base_config_from_experiment(task["base_experiment_id"])
                logger.info(f"Loop {loop_index}: 配置来源 = base_experiment {task['base_experiment_id']}")

            action_type = config.get("action_type", "initial" if current_loop == 0 else "param_tune")

            # ── 每轮 Loop 因子可用性检查 ──
            factor_list = config.get("factor_list", [])
            validation = self.validate_factor_availability(factor_list)
            if validation["has_issues"]:
                removed = validation["deleted_factors"] + validation["unavailable_factors"]
                # 严格模式：因子不可用时暂停任务，不静默移除
                logger.error(
                    f"Loop {loop_index} 因子可用性检查失败。"
                    f"已删除: {validation['deleted_factors']}, "
                    f"不可用: {validation['unavailable_factors']}。"
                    f"任务暂停，请修复因子后恢复。"
                )
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE qe_evolution_tasks SET status = 'paused', updated_at = NOW()
                            WHERE task_id = %s
                        """, (task_id,))
                    conn.commit()
                raise ValueError(
                    f"Loop {loop_index}: 因子可用性检查失败。"
                    f"已删除: {validation['deleted_factors']}, "
                    f"不可用: {validation['unavailable_factors']}。"
                    f"任务已暂停，请通过 /evolution/tasks/{task_id}/resolve-factors 修复。"
                )

            # 使用 ConfigComposer 在内存中生成实验文件
            from .config_composer import ConfigComposer
            composer = ConfigComposer()
            # 合并 stock_pool：task 级别的设置优先覆盖 model_params 中的值
            loop_custom_params = dict(config.get("model_params") or {})
            task_stock_pool = task.get("stock_pool")
            if task_stock_pool:
                loop_custom_params["stock_pool"] = task_stock_pool
            task_label_type = task.get("label_type")
            if task_label_type:
                loop_custom_params["label_type"] = task_label_type
            # task 级别的策略/执行算法覆盖（优先于 config 继承值）
            effective_strategy_id = task.get("strategy_id") or config.get("strategy_id")
            effective_strategy_params = task.get("strategy_params") or {}
            if isinstance(effective_strategy_params, str):
                effective_strategy_params = json.loads(effective_strategy_params)
            effective_execution_algo = task.get("execution_algo")
            effective_execution_algo_params = task.get("execution_algo_params") or {}
            if isinstance(effective_execution_algo_params, str):
                effective_execution_algo_params = json.loads(effective_execution_algo_params)
            # 将策略参数覆盖合并到 custom_params（不影响模型超参）
            if effective_strategy_params:
                loop_custom_params.update(effective_strategy_params)
            # initial_cash 从 strategy_params 单独提取，不混入 custom_params（避免被当作策略参数）
            _sp = effective_strategy_params.copy() if effective_strategy_params else {}

            # 注入尾盘未成交处理配置 → custom_params（config_composer 从 custom_params 提取）
            _uf = task.get("unfilled_handler")
            if _uf:
                loop_custom_params["unfilled_handler"] = _uf
                _uf_params = task.get("unfilled_handler_params") or {}
                if isinstance(_uf_params, str):
                    import json as _json
                    _uf_params = _json.loads(_uf_params)
                if _uf_params.get("trigger_minute"):
                    loop_custom_params["unfilled_trigger_minute"] = _uf_params["trigger_minute"]
                if _uf_params.get("backup_depth"):
                    loop_custom_params["unfilled_backup_depth"] = _uf_params["backup_depth"]

            loop_custom_params.pop("initial_cash", None)
            compose_res = composer.compose_experiment_in_memory(
                factor_names=config.get("factor_list", []),
                model_id=config.get("model_id"),
                strategy_id=effective_strategy_id,
                data_split=config.get("data_split"),
                custom_params=loop_custom_params,
                experiment_name=f"{task_id}/{loop_id}",
                skip_db_save=True,
                execution_algo=effective_execution_algo,
                execution_algo_params=effective_execution_algo_params,
                strategy_params=_sp,
                node_id=task.get("node_id"),
            )
            experiment_files = compose_res["experiment_files"]
            wsl_command = compose_res.get("wsl_command", "")

            # 保存本轮配置到 loop 记录
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops SET config_json = %s, action_type = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(config), action_type, evolution_loop_db_id))
                conn.commit()

            # 调用节点执行（异步，不等待完成）
            client = self._get_workspace_client_for_task(task_id)
            callback_url = self._get_callback_url_for_task(task_id)
            await client.create_and_run_loop(
                task_id, loop_index, config, experiment_files, wsl_command,
                callback_url=callback_url,
            )
        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"Failed to submit loop {loop_index} for task {task_id}: {e}\n{tb_str}")
            # 幂等保护：独立 try/except 确保 DB 状态不泄漏
            try:
                with get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                        # custom_evo/strategy_evo：单个 loop 失败不标记整个 task，只有所有 loop 都失败才标记
                        cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                        task_row = cur.fetchone()
                        if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                            cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                        else:
                            cur.execute("SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status != 'failed'", (task_id,))
                            non_failed = cur.fetchone()
                            if non_failed and non_failed[0] == 0:
                                cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"FATAL: Failed to mark loop/task as failed for {task_id}: {db_err}")
            return None

        return evolution_loop_db_id

    async def process_completed_loop(self, task_id: str, loop_id_str: str) -> bool:
        """
        Loop 完成后处理：获取 metrics → Agent 分析 → 更新 DB → 判断是否继续。
        CAS 幂等保护：只有 status='running' 的 loop 会被处理。
        Returns: True if processing succeeded, False if skipped/failed.
        """
        # 检查任务类型：策略演进走简化流程
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT task_type, base_experiment_id FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task_row = cur.fetchone()

        if task_row and task_row.get("task_type") in ("strategy_evo", "custom_evo"):
            return await self.process_strategy_evo_completed_loop(task_id, loop_id_str)

        # 检查是否为多Alpha实验：先收集组级结果再走后续流程
        if task_row and task_row.get("base_experiment_id"):
            _alpha_mode = self._detect_alpha_mode({"base_experiment_id": task_row["base_experiment_id"]})
            if _alpha_mode == "multi":
                from .multi_alpha_result_collector import MultiAlphaResultCollector
                # experiment_id 格式: "{task_id}_L{loop_index}"
                _parts = loop_id_str.rsplit("_Loop", 1)
                if len(_parts) == 2:
                    _exp_id = f"{_parts[0]}_L{_parts[1]}"
                else:
                    # 无法从 loop_id_str 推断，查询 DB
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT experiment_id FROM qe_evolution_loops WHERE loop_id = %s",
                                (loop_id_str,),
                            )
                            _eid_row = cur.fetchone()
                    _exp_id = _eid_row[0] if _eid_row and _eid_row[0] else loop_id_str

                try:
                    collector = MultiAlphaResultCollector()
                    await collector.collect_and_persist(_exp_id)
                    logger.info(f"多Alpha结果收集完成: {_exp_id}")
                except Exception as e:
                    # 不静默：记录 ERROR 级别，但不阻断演进流程（演进可能需要继续下一轮）
                    logger.error(f"多Alpha结果收集失败: {_exp_id}: {e}", exc_info=True)

        # 提取 loop_index from loop_id_str (format: "{task_id}_Loop{N}")
        loop_suffix = loop_id_str.rsplit("_Loop", 1)
        if len(loop_suffix) != 2:
            logger.error(f"Invalid loop_id format: {loop_id_str}")
            return False
        try:
            loop_index = int(loop_suffix[1])
        except ValueError:
            logger.error(f"Invalid loop_index in loop_id: {loop_id_str}")
            return False

        evolution_loop_db_id = loop_id_str
        loop_id = f"Loop{loop_index}"

        # 每轮开始前清空 LLM trace
        self.agents.reset_trace()

        # CAS 幂等保护: 只有 status='running' 的 loop 才能被处理
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    UPDATE qe_evolution_loops SET status = 'processing', updated_at = NOW()
                    WHERE loop_id = %s AND status = 'running'
                    RETURNING loop_id
                """, (evolution_loop_db_id,))
                cas_row = cur.fetchone()
            conn.commit()

        if not cas_row:
            logger.info(f"Loop {evolution_loop_db_id} is not in 'running' state, skipping (idempotent).")
            return False

        try:
            # 读取配置
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT config_json FROM qe_evolution_loops WHERE loop_id = %s", (evolution_loop_db_id,))
                    loop_row = cur.fetchone()
            if not loop_row or not loop_row.get('config_json'):
                raise ValueError(f"Loop {evolution_loop_db_id} config_json 为空，数据完整性异常")
            config = loop_row['config_json']
            if isinstance(config, str):
                config = json.loads(config)
            action_type = config.get("action_type", "initial")

            # 获取回测结果
            client = self._get_workspace_client_for_task(task_id)
            metrics = await client.get_loop_metrics(task_id, loop_id)

            # Normalize QLib metric keys → frontend-expected short keys
            _METRIC_ALIASES = {
                "Rank IC": "Rank_IC",
                "1day.excess_return_with_cost.information_ratio": "sharpe",
                "1day.excess_return_with_cost.annualized_return": "annualized_return",
                "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
                "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
                "1day.excess_return_without_cost.information_ratio": "sharpe_no_cost",
                "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
                "1day.excess_return_with_cost.mean": "daily_return",
                "1day.excess_return_without_cost.mean": "daily_return_no_cost",
            }
            for src, dst in _METRIC_ALIASES.items():
                if src in metrics and dst not in metrics:
                    metrics[dst] = metrics[src]

            # 拉取增强诊断指标（训练曲线、收敛诊断等）
            enhanced_data = await client.get_enhanced_metrics(task_id, loop_id)
            # 计算训练诊断派生指标
            td = enhanced_data.get("training_diagnostics", {})
            if not td and "training_curves" in enhanced_data:
                td = self._compute_training_diagnostics(enhanced_data.get("training_curves", {}))
                enhanced_data["training_diagnostics"] = td
            metrics["enhanced_metrics"] = enhanced_data

            # S3: enhanced_metrics 先写入 DB，确保后续 _build_full_evolution_history 可读取
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET metrics_json = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(metrics), evolution_loop_db_id))
                conn.commit()

            # Agent 分析（传递完整演进历史）
            logger.info(f"Running Agent analysis for loop {loop_index} of task {task_id}")
            evolution_history = self._build_full_evolution_history(task_id)

            # 使用共享方法构建分析上下文
            factor_names = config.get("factor_list", [])
            analysis_context = self._build_analysis_context(task_id, factor_names)
            # 注入 evolution_mode 供 Analyst Step 2 使用
            evolution_mode = self._get_evolution_mode(task_id)
            analysis_context["evolution_mode"] = evolution_mode

            # Analyst 两步 LLM — 返回 AnalystResult
            analyst_result = await self.agents.run_analyst(
                loop_index, config, metrics,
                analysis_context=analysis_context,
                evolution_history=evolution_history,
            )

            historical_sota_metrics = None
            sota_loop_id_ref = None
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT r.loop_id, l.metrics_json
                        FROM qe_sota_registry r
                        JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                        WHERE l.task_id = %s
                        ORDER BY r.created_at DESC LIMIT 1
                    """, (task_id,))
                    sota_row = cur.fetchone()
                    if sota_row and sota_row['metrics_json']:
                        historical_sota_metrics = sota_row['metrics_json']
                        sota_loop_id_ref = sota_row['loop_id']

            eval_result = await self.agents.run_evaluator(
                metrics, historical_sota_metrics,
                evolution_history=evolution_history,
            )
            is_sota = eval_result["is_sota"]
            eval_reason = eval_result.get("reason", "")
            eval_method = eval_result.get("method", "unknown")

            # ── SOTA 回滚 ──
            # 策略: 非 SOTA 轮回滚到 SOTA 配置为基础继续演进。
            # 若尚无 SOTA（首轮未达基线），sota_config=None，不回滚，Agent 自由探索。
            actual_config = config.copy()
            # 因子保护已由 importance-based 机制替代，不再传递 SOTA 因子列表
            sota_protected_factors = None
            sota_context = None
            if evolution_history and not is_sota:
                sota_config = self._get_sota_loop_config(task_id)
                if sota_config:
                    logger.info(
                        f"SOTA 回滚: 当前轮非 SOTA，回滚到 SOTA 配置为基础继续演进。"
                        f"因子保护由 importance-based 机制管理。"
                    )
                    config = sota_config
                    # 更新 factor_names 以匹配回滚后的配置
                    factor_names = config.get("factor_list", [])
                    # 构建双组上下文，让 Agent 同时看到本轮和 SOTA 两组数据
                    sota_context = {
                        "current_run": {
                            "config": actual_config,
                            "metrics": metrics,
                        },
                        "sota_baseline": {
                            "config": sota_config,
                            "metrics": historical_sota_metrics or {},
                        },
                        "rollback_applied": True,
                    }

            # Phase 7: 方向控制器 — 决定 action_type
            decided_action_type = self._decide_evolution_direction(
                analyst_result, evolution_mode, metrics, is_sota=is_sota, config=config,
            )
            logger.info(f"Direction decision: {decided_action_type} (mode={evolution_mode}, sota_protected={sota_protected_factors is not None})")

            # Phase 7: 多 Agent 分发
            correlations = self._get_relevant_correlations(factor_names)
            factor_library_summary = self._get_factor_library_summary()
            researcher_context = {}
            if correlations:
                researcher_context["correlations"] = correlations
            if factor_library_summary:
                researcher_context["factor_library_summary"] = factor_library_summary
            # 传递因子黑名单给 Agent
            factor_blacklist = self._get_factor_blacklist(task_id)
            if factor_blacklist:
                researcher_context["factor_blacklist"] = list(factor_blacklist)
            # 传递 loop_index 用于 SOTA 渐进淘汰
            researcher_context["loop_index"] = loop_index
            researcher_context["max_retire_per_loop"] = 3

            if decided_action_type in ("factor_adjust", "factor_expand"):
                next_config_draft = await self.factor_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    researcher_context=researcher_context if researcher_context else None,
                    sota_protected_factors=sota_protected_factors,
                    sota_context=sota_context,
                )
                if not isinstance(next_config_draft, dict):
                    raise ValueError(f"Factor Agent 返回无效输出: type={type(next_config_draft)}")
                if "factor_list" not in next_config_draft:
                    raise ValueError("Factor Agent 输出缺少 'factor_list' 字段")
            elif decided_action_type in ("param_tune", "model_switch"):
                next_config_draft = await self.model_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    sota_context=sota_context,
                    task_id=task_id,
                )
                if not isinstance(next_config_draft, dict):
                    raise ValueError(f"Model Agent 返回无效输出: type={type(next_config_draft)}")
                # 模型轮保留因子
                next_config_draft["factor_list"] = config.get("factor_list", [])
            elif decided_action_type == "factor_model_joint":
                # Phase 1: Factor Agent 先决定因子组合
                factor_draft = await self.factor_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    researcher_context=researcher_context if researcher_context else None,
                    sota_protected_factors=sota_protected_factors,
                    sota_context=sota_context,
                )
                # Phase 2: 计算因子组合特征，传递给 Model Agent
                factor_list = factor_draft.get("factor_list", config.get("factor_list", []))
                factor_context = self._compute_factor_context(factor_list)
                merged_config = {**config, "factor_list": factor_list}
                model_draft = await self.model_agent.run(
                    analyst_result, is_sota, merged_config,
                    evolution_history=evolution_history,
                    factor_context=factor_context,
                    sota_context=sota_context,
                    task_id=task_id,
                )
                next_config_draft = {
                    "factor_list": factor_list,
                    "model_id": model_draft.get("model_id", config.get("model_id")),
                    "model_params": model_draft.get("model_params", config.get("model_params", {})),
                    "rationale": f"Factor: {factor_draft.get('rationale', '')} | Model: {model_draft.get('rationale', '')}",
                }
            else:
                next_config_draft = await self.factor_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    researcher_context=researcher_context if researcher_context else None,
                    sota_protected_factors=sota_protected_factors,
                    sota_context=sota_context,
                )

            # 注入 action_type（由分发层统一设置）
            next_config_draft["action_type"] = decided_action_type

            # 将当前 Loop 的信息补充到 evolution_history 中，
            # 使 Reviewer 能看到最新的 config 状态（当前 Loop 还处于 processing，
            # _build_full_evolution_history 查询不到它）
            # 注意：config_summary 使用 config（rollback 后的基准 config）而非 actual_config，
            # 因为 draft 的 factor_list 也来自 config。如果用 actual_config，
            # rollback 场景下 Reviewer 会误判 factor_list 发生了变化而拒绝。
            reviewer_history = dict(evolution_history) if evolution_history else {}
            rollback_applied = not is_sota and evolution_history is not None and config != actual_config
            current_loop_entry = {
                "loop_index": loop_index,
                "action_type": decided_action_type,
                "config_summary": {
                    "factors": config.get("factor_list", []),
                    "model_id": config.get("model_id"),
                    "model_params": config.get("model_params", {}),
                },
                "metrics": {k: v for k, v in metrics.items() if k != "enhanced_metrics"},
                "is_sota": is_sota,
                "rollback_applied": rollback_applied,
            }
            reviewer_history_loops = list(reviewer_history.get("loops", []))
            reviewer_history_loops.append(current_loop_entry)
            reviewer_history["loops"] = reviewer_history_loops
            reviewer_history["total_loops"] = len(reviewer_history_loops)

            next_config = await self.agents.run_reviewer(
                next_config_draft,
                evolution_history=reviewer_history,
            )

            # 确保关键基础字段不会因为 Agent 漏输出而丢失
            for key in ["model_id", "strategy_id", "data_split", "base_experiment_id"]:
                if key not in next_config and key in config:
                    next_config[key] = config[key]

            agent_analysis = {
                "analyst": analyst_result.to_dict(),
                "evaluator": {
                    "is_sota": is_sota,
                    "reason": eval_reason,
                    "method": eval_method,
                    "path": eval_result.get("path", ""),
                    "sota_loop_id": sota_loop_id_ref,
                },
                "direction": {
                    "decided_action_type": decided_action_type,
                    "analyst_recommendation": analyst_result.direction,
                    "evolution_mode": evolution_mode,
                },
                "researcher": {
                    "draft": {k: v for k, v in next_config_draft.items() if k != "_optuna_trial"},
                    "action_type": decided_action_type,
                },
                "reviewer": {
                    "approved": True,
                    "validated_config": next_config,
                },
                "llm_trace": self.agents.get_trace(),
            }

            # 更新 action_type 为实际决策的方向
            action_type = decided_action_type

            # 更新 LOOP 记录
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 先写入 qe_experiments（外键目标），再更新 loop 的 experiment_id
                    experiment_id = f"{task_id}_L{loop_index}"
                    cur.execute("""
                        INSERT INTO qe_experiments
                        (experiment_id, experiment_name, qe_task_id, qe_loop_id,
                         loop_index, parent_experiment_id,
                         is_evolution_loop, factor_names, model_id, strategy_id,
                         data_split, custom_params,
                         result_metrics, status, is_sota)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s, 'completed', %s)
                        ON CONFLICT (experiment_id) DO UPDATE SET
                            result_metrics = EXCLUDED.result_metrics,
                            status = EXCLUDED.status,
                            is_sota = EXCLUDED.is_sota,
                            qe_task_id = EXCLUDED.qe_task_id,
                            qe_loop_id = EXCLUDED.qe_loop_id
                    """, (
                        experiment_id,
                        f"{task_id} Loop{loop_index}",
                        task_id,
                        loop_id,
                        loop_index,
                        task_id,
                        json.dumps(actual_config.get("factor_list", [])),
                        actual_config.get("model_id"),
                        actual_config.get("strategy_id"),
                        json.dumps(actual_config.get("data_split", {})),
                        json.dumps(actual_config.get("model_params", {})),
                        json.dumps(metrics),
                        is_sota,
                    ))

                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET action_type = %s, config_json = %s, metrics_json = %s,
                            agent_analysis = %s, is_sota = %s, status = 'completed',
                            experiment_id = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (
                        action_type,
                        json.dumps(actual_config),
                        json.dumps(metrics),
                        json.dumps(agent_analysis),
                        is_sota,
                        experiment_id,
                        evolution_loop_db_id
                    ))

                    if is_sota:
                        cur.execute("""
                            INSERT INTO qe_sota_registry (loop_id, evaluation_reason)
                            VALUES (%s, %s)
                        """, (evolution_loop_db_id, "Evaluator Agent marked as SOTA based on metrics."))

                    # 写入 qe_loop_factor_records（action_role 计算）
                    self._write_loop_factor_records(
                        cur, task_id, evolution_loop_db_id, loop_index,
                        actual_config, metrics, action_type, is_sota,
                    )

                    # 写入 qe_loop_model_records
                    self._write_loop_model_records(
                        cur, task_id, evolution_loop_db_id, loop_index,
                        actual_config, metrics, action_type, is_sota,
                    )

                conn.commit()

            # Optuna 反馈：param_tune 方向时将 IC 反馈给 Optuna
            if decided_action_type == "param_tune":
                _optuna_trial = next_config_draft.get("_optuna_trial") if next_config_draft else None
                if _optuna_trial is not None:
                    try:
                        from .optuna_optimizer import OptunaHyperparamOptimizer
                        ic_value = metrics.get("IC")
                        if ic_value is not None:
                            model_type = next_config_draft.get("model_type", "")
                            optimizer = OptunaHyperparamOptimizer(task_id, model_type)
                            optimizer.get_or_create_study()
                            optimizer.tell(_optuna_trial, float(ic_value))
                            logger.info(f"Optuna tell() 成功: task={task_id}, IC={ic_value}")
                    except Exception as e:
                        logger.error(f"Optuna tell() 失败: {e}, 不影响演进流程")

            # 更新总进度
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET current_loop = %s, updated_at = NOW() WHERE task_id = %s", (loop_index, task_id))
                conn.commit()

            # 判断是否还有剩余 Loop → 提交下一轮
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT current_loop, max_loops, status FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task = cur.fetchone()

            if task and task['status'] == 'running' and task['current_loop'] < task['max_loops']:
                _next_task = asyncio.create_task(self.submit_next_loop(task_id))
                _next_task.add_done_callback(
                    lambda t: logger.error(f"submit_next_loop failed: {t.exception()}") if t.exception() else None
                )
            elif task and task['current_loop'] >= task['max_loops']:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
                logger.info(f"Task {task_id} completed successfully.")

            return True

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"Error processing completed loop {evolution_loop_db_id} for task {task_id}: {e}\n{tb_str}")
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # 将完整错误信息写入 agent_analysis 以便调试（保留已收集的 LLM trace）
                    error_detail = json.dumps({"_error": str(e), "_traceback": tb_str, "llm_trace": self.agents.get_trace()}, ensure_ascii=False)
                    cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                    # custom_evo/strategy_evo：单个 loop 失败不标记整个 task，只有所有 loop 都失败才标记
                    cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task_row = cur.fetchone()
                    if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    else:
                        cur.execute("SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status != 'failed'", (task_id,))
                        non_failed = cur.fetchone()
                        if non_failed and non_failed[0] == 0:
                            cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()
            return False

    async def scan_running_loops(self):
        """
        定时扫描兜底：
        1. 查询 status='running' 的 loops，检查 RDAgent 侧状态
        2. F4: 检测卡在 processing 超过 30 分钟的 loop
        3. F5: 检测 running 的 task 但没有任何活跃 loop（僵尸 task）
        """
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # 原有：扫描 running 状态的 loop
                    cur.execute("""
                        SELECT l.loop_id, l.task_id, l.loop_index
                        FROM qe_evolution_loops l
                        JOIN qe_evolution_tasks t ON l.task_id = t.task_id
                        WHERE l.status = 'running' AND t.status = 'running'
                    """)
                    running_loops = cur.fetchall()

                    # F4: 检测卡在 processing 超过 30 分钟的 loop
                    cur.execute("""
                        SELECT loop_id, task_id FROM qe_evolution_loops
                        WHERE status = 'processing'
                          AND updated_at < NOW() - INTERVAL '30 minutes'
                    """)
                    stuck_processing = cur.fetchall()

                    # F5: 检测僵尸 task（running 但无活跃 loop）
                    # 排除最近10分钟内有 failed loop 的 task，防止无限重试循环
                    cur.execute("""
                        SELECT t.task_id FROM qe_evolution_tasks t
                        WHERE t.status = 'running'
                          AND NOT EXISTS (
                              SELECT 1 FROM qe_evolution_loops l
                              WHERE l.task_id = t.task_id AND l.status IN ('running', 'processing')
                          )
                          AND t.updated_at < NOW() - INTERVAL '5 minutes'
                          AND NOT EXISTS (
                              SELECT 1 FROM qe_evolution_loops l2
                              WHERE l2.task_id = t.task_id
                                AND l2.status = 'failed'
                                AND l2.updated_at > NOW() - INTERVAL '10 minutes'
                          )
                    """)
                    zombie_tasks = cur.fetchall()

            # F4: 处理 processing 超时
            for row in stuck_processing:
                logger.error(f"Loop {row['loop_id']} stuck in processing for >30min, marking as failed")
                try:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s AND status = 'processing'", (row['loop_id'],))
                        conn.commit()
                except Exception as e:
                    raise RuntimeError(f"Failed to mark stuck loop {row['loop_id']} as failed: {e}") from e

            # F5: 处理僵尸 task — 尝试提交下一轮
            for row in zombie_tasks:
                logger.warning(f"Zombie task detected: {row['task_id']} is running but has no active loops, attempting recovery")
                asyncio.create_task(self._safe_submit_or_fail(row['task_id']))

            # 原有逻辑：检查 running 的 loop 在 RDAgent 侧的状态
            for loop_row in running_loops:
                task_id = loop_row['task_id']
                loop_index = loop_row['loop_index']
                loop_id = f"Loop{loop_index}"
                evolution_loop_db_id = loop_row['loop_id']

                try:
                    client = self._get_workspace_client_for_task(task_id)
                    status_resp = await client.get_loop_status(task_id, loop_id)
                    rd_status = status_resp.get("status")

                    if rd_status == "completed":
                        logger.info(f"Timer scan: Loop {evolution_loop_db_id} completed on RDAgent side, processing.")
                        asyncio.create_task(self._safe_process_completed_loop(task_id, evolution_loop_db_id))
                    elif rd_status in ("failed", "error"):
                        logger.warning(f"Timer scan: Loop {evolution_loop_db_id} failed on RDAgent side.")
                        with get_conn() as conn:
                            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                                cur.execute("UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s", (evolution_loop_db_id,))
                                # 策略演进任务：单个 loop 失败不标记整个 task 为 failed（其他 loop 可能还在跑）
                                cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                                task_row = cur.fetchone()
                                if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                                    cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                            conn.commit()
                except Exception as e:
                    # 单个 loop 检查失败（超时/网络等）不应中断整个扫描
                    logger.warning(f"Timer scan: Failed to check loop {evolution_loop_db_id}: {e}")

        except Exception as e:
            logger.error(f"Timer scan error: {e}", exc_info=True)
            raise  # 让调用方（定时器框架）感知调度异常，避免静默失效

    async def _safe_submit_or_fail(self, task_id: str):
        """F5: 尝试为僵尸 task 提交下一轮，失败则标记 task 为 failed。"""
        try:
            result = await self.submit_next_loop(task_id)
            if result:
                logger.info(f"Zombie task {task_id} recovered: submitted {result}")
            else:
                logger.info(f"Zombie task {task_id}: submit_next_loop returned None (completed or stopped)")
        except Exception as e:
            logger.error(f"Zombie task {task_id} recovery failed, marking as failed: {e}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"FATAL: Failed to mark zombie task {task_id} as failed: {db_err}")
                raise RuntimeError(f"Failed to mark zombie task {task_id} as failed: {db_err}") from db_err

    _LLM_TRANSIENT_KEYWORDS = ("disconnected", "timeout", "rate_limit", "503", "502", "429", "connection", "reset by peer")

    async def _safe_process_completed_loop(self, task_id: str, loop_id: str):
        """process_completed_loop with retry for transient LLM errors (for use with asyncio.create_task)."""
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                await self.process_completed_loop(task_id, loop_id)
                return  # 成功
            except Exception as e:
                err_lower = str(e).lower()
                is_transient = any(kw in err_lower for kw in self._LLM_TRANSIENT_KEYWORDS)
                if is_transient and attempt < max_retries:
                    wait_sec = 30 * attempt
                    logger.warning(f"Transient LLM error in process_completed_loop({task_id}, {loop_id}), "
                                   f"attempt {attempt}/{max_retries}, retrying in {wait_sec}s: {e}")
                    await asyncio.sleep(wait_sec)
                    continue
                # 非瞬态错误 或 重试耗尽 → 标记失败
                logger.error(f"Error in process_completed_loop({task_id}, {loop_id}) "
                             f"(attempt {attempt}/{max_retries}, transient={is_transient}): {e}", exc_info=True)
                try:
                    with get_conn() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s", (loop_id,))
                            # custom_evo/strategy_evo：单个 loop 失败不标记整个 task，只有所有 loop 都失败才标记
                            cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                            task_row = cur.fetchone()
                            if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                                cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                            else:
                                cur.execute("SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status != 'failed'", (task_id,))
                                non_failed = cur.fetchone()
                                if non_failed and non_failed[0] == 0:
                                    cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                        conn.commit()
                    logger.error(f"Marked loop {loop_id} as failed due to process_completed_loop error")
                except Exception as db_err:
                    logger.critical(f"FATAL: Failed to mark loop/task as failed after process_completed_loop error: {db_err}")
                    raise RuntimeError(f"Failed to mark loop/task as failed: {db_err}") from db_err
                return

    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks ORDER BY created_at DESC")
                return [dict(row) for row in cur.fetchall()]

    async def resume_task(self, task_id: str, additional_loops: int = 0, force_full_train: bool = False) -> dict:
        """
        恢复已暂停/已完成/已失败的演进任务，继续从上次的 current_loop 开始。
        如果 additional_loops > 0，则增加 max_loops。
        force_full_train=True 时，忽略各 loop 的 backtest_only，强制完整训练。
        Returns: {"task_id": str, "task_type": str, "force_full_train": bool}
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            raise ValueError(f"演进任务不存在: {task_id}")

        if task['status'] == 'running':
            raise ValueError(f"演进任务正在运行中: {task_id}")

        task_type = task.get('task_type', 'evolution')

        new_max = task['max_loops']
        if additional_loops > 0:
            new_max = task['current_loop'] + additional_loops

        # 策略演进/自定义演进恢复：将失败的 loop 状态重置为 pending
        if task_type in ('strategy_evo', 'custom_evo'):
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops SET status = 'pending', updated_at = NOW() WHERE task_id = %s AND status IN ('failed', 'cancelled')",
                        (task_id,),
                    )
                    reset_count = cur.rowcount
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'pending', max_loops = %s, updated_at = NOW() WHERE task_id = %s",
                        (new_max, task_id),
                    )
                conn.commit()
            logger.info(f"Resumed {task_type} task {task_id}, reset {reset_count} failed loops, max_loops={new_max}")
        else:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'pending', max_loops = %s, updated_at = NOW() WHERE task_id = %s",
                        (new_max, task_id),
                    )
                conn.commit()
            logger.info(f"Resumed evolution task {task_id}, max_loops={new_max}")

        return {"task_id": task_id, "task_type": task_type, "force_full_train": force_full_train}

    async def fork_task(
        self,
        source_task_id: str,
        from_loop_index: int,
        task_name: Optional[str] = None,
        max_loops: int = 10,
        evolution_guidance: Optional[str] = None,
        evolution_mode: str = "auto",
        inherit_history: bool = False,
        strategy_id: Optional[str] = None,
        strategy_params: Optional[Dict[str, Any]] = None,
        execution_algo: Optional[str] = None,
        execution_algo_params: Optional[Dict[str, Any]] = None,
        unfilled_handler: Optional[str] = None,
        unfilled_handler_params: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None,
    ) -> str:
        """
        从指定 task 的某个已完成 loop 分叉出新的演进任务。
        以该 loop 的因子+模型配置为基础，创建新 task 做全新演进。
        """
        # 1. 验证源 task 和 loop
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (source_task_id,))
                source_task = cur.fetchone()
                if not source_task:
                    raise ValueError(f"源任务不存在: {source_task_id}")

                cur.execute(
                    "SELECT * FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s AND status = 'completed'",
                    (source_task_id, from_loop_index),
                )
                source_loop = cur.fetchone()
                if not source_loop:
                    raise ValueError(
                        f"源任务 {source_task_id} 中不存在已完成的 Loop {from_loop_index}"
                    )

        # 2. 读取源 loop 的配置
        config = source_loop.get("config_json") or {}
        if isinstance(config, str):
            config = json.loads(config)

        metrics = source_loop.get("metrics_json") or {}
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        factor_list = config.get("factor_list", [])
        model_id = config.get("model_id")
        # 继承源任务的 strategy/execution_algo，入参非 None 时覆盖
        effective_strategy_id = strategy_id if strategy_id is not None else source_task.get("strategy_id")
        effective_strategy_params = strategy_params if strategy_params is not None else (source_task.get("strategy_params") or {})
        effective_execution_algo = execution_algo if execution_algo is not None else source_task.get("execution_algo")
        effective_execution_algo_params = execution_algo_params if execution_algo_params is not None else (source_task.get("execution_algo_params") or {})
        effective_unfilled_handler = unfilled_handler if unfilled_handler is not None else source_task.get("unfilled_handler")
        effective_unfilled_handler_params = unfilled_handler_params if unfilled_handler_params is not None else (source_task.get("unfilled_handler_params") or {})
        if isinstance(effective_strategy_params, str):
            effective_strategy_params = json.loads(effective_strategy_params)
        if isinstance(effective_execution_algo_params, str):
            effective_execution_algo_params = json.loads(effective_execution_algo_params)
        strategy_id = effective_strategy_id
        data_split = config.get("data_split", {})
        model_params = config.get("model_params", {})

        if not factor_list:
            raise ValueError(f"源 Loop {from_loop_index} 的因子列表为空，无法分叉")
        # model_id / strategy_id 允许为空 — ConfigComposer 会用默认值补全

        # 3. 生成新 task_id 和名称（微秒精度 + 4位随机后缀防并发冲突）
        import uuid
        from datetime import datetime
        suffix = uuid.uuid4().hex[:4]
        new_task_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{suffix}"
        if not task_name:
            source_name = source_task.get("task_name", source_task_id)
            task_name = f"{source_name}_from_L{from_loop_index}"
        target_desc = source_task.get("target_desc", "")

        # 4+5. 在同一事务中创建 base experiment + evolution task（防止孤儿记录）
        base_exp_id = f"{new_task_id}_base"
        # node_id: 入参优先，否则继承源任务
        effective_node_id = node_id if node_id is not None else source_task.get("node_id")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                    (experiment_id, experiment_name, parent_experiment_id,
                     factor_names, model_id, strategy_id, data_split, custom_params,
                     result_metrics, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'created')
                """, (
                    base_exp_id,
                    f"Fork base from {source_task_id} L{from_loop_index}",
                    None,
                    json.dumps(factor_list),
                    model_id,
                    strategy_id,
                    json.dumps(data_split) if isinstance(data_split, dict) else data_split,
                    json.dumps(model_params) if isinstance(model_params, dict) else model_params,
                    json.dumps(metrics),
                ))
                cur.execute("""
                    INSERT INTO qe_evolution_tasks
                    (task_id, task_name, target_desc, max_loops, current_loop, status,
                     base_experiment_id, node_id, source_type,
                     evolution_guidance, evolution_mode,
                     fork_from_task_id, fork_from_loop_index, inherit_history,
                     strategy_id, strategy_params, execution_algo, execution_algo_params,
                     unfilled_handler, unfilled_handler_params)
                    VALUES (%s, %s, %s, %s, 0, 'pending', %s, %s, 'fork', %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s)
                """, (
                    new_task_id, task_name, target_desc, max_loops,
                    base_exp_id, effective_node_id,
                    evolution_guidance, evolution_mode,
                    source_task_id, from_loop_index, inherit_history,
                    effective_strategy_id,
                    json.dumps(effective_strategy_params) if effective_strategy_params else None,
                    effective_execution_algo,
                    json.dumps(effective_execution_algo_params) if effective_execution_algo_params else None,
                    effective_unfilled_handler,
                    json.dumps(effective_unfilled_handler_params) if effective_unfilled_handler_params else None,
                ))
            conn.commit()

        logger.info(
            f"Forked new task {new_task_id} from {source_task_id} Loop {from_loop_index}, "
            f"max_loops={max_loops}, inherit_history={inherit_history}"
        )
        return new_task_id

    async def get_task_detail(self, task_id: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
                if not task:
                    return None

                cur.execute("SELECT * FROM qe_evolution_loops WHERE task_id = %s ORDER BY loop_index ASC", (task_id,))
                loops = cur.fetchall()

                result = dict(task)
                result['loops'] = [dict(loop_row) for loop_row in loops]

        # Live status 检查：对 running 状态的 loop 查询 RDAgent 侧真实状态
        # 对于 custom_evo/strategy_evo 并行调度任务，不能直接修改 loop status（会破坏
        # submit_custom_evo_all_loops 的 run_with_sem 调度循环），改为触发完整的
        # process_completed_loop 流程。
        task_type = result.get('task_type')
        any_synced = False
        for loop_data in result['loops']:
            if loop_data.get('status') not in ('running', 'processing'):
                continue
            loop_id = loop_data['loop_id']
            loop_index = loop_data['loop_index']
            try:
                client = self._get_workspace_client_for_task(task_id)
                live = await client.get_loop_status(task_id, f"Loop{loop_index}")
                rd_status = live.get("status")
            except Exception as e:
                logger.warning(f"[get_task_detail] live status check failed for {loop_id}: {e}")
                rd_status = "failed"  # RDAgent 不可达，视为失败

            if rd_status in ("completed", "failed", "not_found"):
                if task_type in ("custom_evo", "strategy_evo"):
                    # 并行调度任务：触发完整处理流程（metrics采集+DB更新+后续loop调度），
                    # 不直接改 status，避免抢跑 run_with_sem 的调度循环
                    try:
                        logger.info(f"[get_task_detail] 触发完整处理: {loop_id} (rd_status={rd_status})")
                        await self._safe_process_completed_loop(task_id, loop_id)
                        # 重新读取 loop 状态以反映处理结果
                        with get_conn() as conn:
                            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                                cur.execute("SELECT * FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                                updated = cur.fetchone()
                                if updated:
                                    for i, lp in enumerate(result['loops']):
                                        if lp.get('loop_id') == loop_id:
                                            result['loops'][i] = dict(updated)
                                            break
                        any_synced = True
                    except Exception as e:
                        logger.error(f"[get_task_detail] 完整处理失败 for {loop_id}: {e}")
                else:
                    # 标准演进任务：保留原有快速同步逻辑
                    new_status = "failed" if rd_status in ("failed", "not_found") else "completed"
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE qe_evolution_loops SET status = %s, updated_at = NOW() WHERE loop_id = %s AND status IN ('running', 'processing')",
                                (new_status, loop_id),
                            )
                        conn.commit()
                    loop_data['status'] = new_status
                    any_synced = True
                    logger.info(f"[get_task_detail] auto-synced loop {loop_id}: {rd_status} -> {new_status}")

        # 仅对标准演进任务（非 custom_evo/strategy_evo）自动更新 task 状态。
        # custom_evo/strategy_evo 的 task 状态由 process_strategy_evo_completed_loop 或
        # submit_custom_evo_all_loops 的 final status check 管理。
        if any_synced and task_type not in ("custom_evo", "strategy_evo"):
            all_terminal = all(
                lp.get('status') in ('completed', 'failed', 'cancelled')
                for lp in result['loops']
            )
            if all_terminal and result.get('status') == 'running':
                has_failed = any(lp.get('status') == 'failed' for lp in result['loops'])
                new_task_status = 'failed' if has_failed else 'completed'
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE qe_evolution_tasks SET status = %s, updated_at = NOW() WHERE task_id = %s AND status = 'running'",
                            (new_task_status, task_id),
                        )
                    conn.commit()
                result['status'] = new_task_status
                logger.info(f"[get_task_detail] auto-synced task {task_id} -> {new_task_status}")

        return result
        
    async def stop_task(self, task_id: str) -> dict:
        """
        暂停演进任务：标记 task 为 paused → 终止 RDAgent 侧进程 → 标记 loop 为 cancelled。
        返回详细结果（不静默吞错）。
        """
        result = {"task_id": task_id, "paused": False, "loop_killed": None}

        # 1. 找到当前 running 的 loop
        running_loop = None
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT loop_id, loop_index FROM qe_evolution_loops "
                    "WHERE task_id = %s AND status IN ('running', 'processing') "
                    "ORDER BY loop_index DESC LIMIT 1",
                    (task_id,),
                )
                running_loop = cur.fetchone()

        # 2. 标记 task 为 paused（先改状态，阻止新 loop 提交）
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_evolution_tasks SET status = 'paused', updated_at = NOW() WHERE task_id = %s", (task_id,))
            conn.commit()
        result["paused"] = True
        logger.info(f"Task {task_id} manually stopped/paused.")

        # 3. 终止 RDAgent 侧正在运行的 loop 进程
        if running_loop:
            loop_index = running_loop["loop_index"]
            loop_id = f"Loop{loop_index}"
            loop_db_id = running_loop["loop_id"]
            kill_success = False
            kill_error = None

            try:
                client = self._get_workspace_client_for_task(task_id)
                kill_result = await client.kill_loop(task_id, loop_id)
                kill_success = kill_result.get("killed", False)
                kill_error = kill_result.get("error")
                if kill_error:
                    logger.warning(f"Kill loop {loop_db_id} returned error: {kill_error}")
                else:
                    logger.info(f"Kill loop {loop_db_id} success: {kill_result}")
            except Exception as e:
                kill_error = str(e)
                logger.error(f"Failed to kill loop process for {loop_db_id}: {e}")

            # 无论 kill 是否成功，都标记 loop 为 cancelled（防止被 scan 重新处理）
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops SET status = 'cancelled', updated_at = NOW() "
                        "WHERE loop_id = %s AND status IN ('running', 'processing')",
                        (loop_db_id,),
                    )
                    rows_affected = cur.rowcount
                conn.commit()

            result["loop_killed"] = {
                "loop_id": loop_db_id,
                "process_killed": kill_success,
                "db_cancelled": rows_affected > 0,
                "error": kill_error,
            }
            if kill_error:
                logger.warning(
                    f"Task {task_id} paused but loop {loop_db_id} kill had issues: {kill_error}. "
                    f"Loop marked cancelled in DB (db_cancelled={rows_affected > 0}). "
                    f"Remote process may still be running."
                )

        return result

    async def retry_loop(self, task_id: str, loop_index: int) -> Dict[str, Any]:
        """重试失败的 Loop：自动判断训练是否已完成，决定从训练或回测恢复.

        判断逻辑：
        - workspace 中 mlruns 有 params.pkl → 训练完成，使用 --backtest-only
        - 无 params.pkl → 训练未完成，全量重跑

        Returns: {"loop_id": str, "mode": "backtest_only"|"full"}
        """
        from .config_composer import QE_WORKSPACE_WIN, ConfigComposer

        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_id = f"Loop{loop_index}"

        # 1. 验证 loop 存在且状态为 failed
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT loop_id, status, config_json FROM qe_evolution_loops WHERE loop_id = %s",
                    (evolution_loop_db_id,),
                )
                loop_row = cur.fetchone()

        if not loop_row:
            raise ValueError(f"Loop {evolution_loop_db_id} 不存在")
        if loop_row["status"] not in ("failed", "cancelled"):
            raise ValueError(
                f"Loop {evolution_loop_db_id} 状态为 '{loop_row['status']}'，"
                f"只有 failed 或 cancelled 状态的 loop 可以重试"
            )

        # 2. 获取 task 信息（retry 需要 task 元数据，但不再因 task running 而拒绝）
        # parallel custom_evo 任务中，一个 loop 失败时其他 loop 可能仍在运行，
        # 此时 task 状态为 running 是正常的，不应阻塞失败 loop 的重试。
        # loop 级别的状态校验已在步骤 1 完成（只允许 failed/cancelled）。
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
        if not task:
            raise ValueError(f"任务不存在: {task_id}")

        # 3. 检查 workspace 中训练是否已完成
        workspace_dir = QE_WORKSPACE_WIN / task_id / loop_id
        mlruns_dir = workspace_dir / "mlruns"

        if not workspace_dir.exists() or not mlruns_dir.exists():
            raise ValueError(
                f"Loop workspace 不存在: {workspace_dir}，无法重试"
            )

        import glob
        params_files = glob.glob(str(mlruns_dir / "**" / "params.pkl"), recursive=True)
        if not params_files:
            raise ValueError(
                f"模型文件 params.pkl 不存在于 {mlruns_dir}，"
                f"训练未完成，无法跳过训练直接回测。"
                f"请使用恢复任务功能重新执行完整训练。"
            )

        logger.info(
            f"Retry loop {evolution_loop_db_id}: params.pkl found at {params_files[0]}, using backtest-only mode"
        )

        # 4. 更新 loop 状态为 running
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE qe_evolution_loops SET status = 'running', agent_analysis = NULL, updated_at = NOW() WHERE loop_id = %s",
                    (evolution_loop_db_id,),
                )
                cur.execute(
                    "UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s",
                    (task_id,),
                )
            conn.commit()

        # 5. 使用统一引擎重新 compose 并提交
        try:
            config = loop_row["config_json"]
            if isinstance(config, str):
                config = json.loads(config)

            from .experiment_config_builders import build_config_from_retry_loop
            from .executors.backtest import BacktestExecutor, BacktestMode
            from .executors.base import ExecutionContext

            cfg = build_config_from_retry_loop(config, task, experiment_name=f"{task_id}/{loop_id}")

            composer = ConfigComposer()
            client = self._get_workspace_client_for_task(task_id)
            executor = BacktestExecutor(composer, client)
            ctx = ExecutionContext(
                task_id=task_id,
                loop_index=loop_index,
                experiment_name=f"{task_id}/{loop_id}",
                node_id=task.get("node_id"),
                callback_url=self._get_callback_url_for_task(task_id),
            )
            result = await executor.submit(cfg, ctx, mode=BacktestMode.BACKTEST_ONLY)
            logger.info(f"Retry in backtest-only mode via unified engine: {result.wsl_command}")

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"Retry loop failed for {evolution_loop_db_id}: {e}\n{tb_str}")
            error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s",
                        (error_detail, evolution_loop_db_id),
                    )
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s",
                        (task_id,),
                    )
                conn.commit()
            raise

        return {"loop_id": evolution_loop_db_id, "mode": "backtest_only"}

    async def delete_task(self, task_id: str) -> Dict[str, Any]:
        """
        删除演进任务及其所有关联数据。
        包括: 演进Loops(CASCADE)、SOTA注册(CASCADE)、Loop因子/模型记录(CASCADE)、
              子实验(qe_experiments)、因子实验指标(qe_factor_experiment_metrics)。
        运行中的任务不允许删除。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. 验证任务存在且非运行中
                cur.execute("SELECT task_id, task_name, status FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
                if not task:
                    raise ValueError(f"任务不存在: {task_id}")
                if task["status"] == "running":
                    raise ValueError("运行中的任务不允许删除，请先停止任务")

                # 1b. 检查是否有 fork task 依赖此 task 的演进历史
                cur.execute(
                    "SELECT task_id, task_name FROM qe_evolution_tasks "
                    "WHERE fork_from_task_id = %s AND inherit_history = TRUE",
                    (task_id,),
                )
                dependent_forks = cur.fetchall()
                if dependent_forks:
                    # 清除依赖（降级为不继承历史），而非阻止删除
                    fork_ids = [f["task_id"] for f in dependent_forks]
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET inherit_history = FALSE "
                        "WHERE fork_from_task_id = %s AND inherit_history = TRUE",
                        (task_id,),
                    )
                    logger.warning(
                        f"源任务 {task_id} 被删除，{len(fork_ids)} 个 fork task 的 inherit_history 已降级为 FALSE: {fork_ids}"
                    )

                # 2. 收集该任务关联的所有子实验 ID (qe_experiments 中 qe_task_id = task_id)
                cur.execute(
                    "SELECT experiment_id FROM qe_experiments WHERE qe_task_id = %s",
                    (task_id,),
                )
                sub_experiment_ids = [r["experiment_id"] for r in cur.fetchall()]

                deleted_counts = {}

                # 3. 删除 qe_factor_experiment_metrics (子实验的因子指标)
                if sub_experiment_ids:
                    cur.execute(
                        "DELETE FROM qe_factor_experiment_metrics WHERE experiment_id = ANY(%s)",
                        (sub_experiment_ids,),
                    )
                    deleted_counts["qe_factor_experiment_metrics"] = cur.rowcount

                # 4. 删除 qe_evolution_tasks (CASCADE 自动清理 loops, sota_registry, loop_factor/model_records)
                cur.execute("DELETE FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                deleted_counts["qe_evolution_tasks"] = cur.rowcount

                # 5. 删除子实验 (qe_experiments 中 qe_task_id = task_id)
                if sub_experiment_ids:
                    cur.execute(
                        "DELETE FROM qe_experiments WHERE experiment_id = ANY(%s)",
                        (sub_experiment_ids,),
                    )
                    deleted_counts["qe_experiments"] = cur.rowcount

            conn.commit()

        # 5b. 清理远端 WSL workspace（如果任务在远端节点执行）
        try:
            client = self._get_workspace_client_for_task(task_id)
            await client.cleanup_task_workspace(task_id)
            logger.info(f"已清理远端 workspace: {task_id}")
        except Exception as e:
            logger.warning(f"远端 workspace 清理失败（非致命）: {task_id}: {e}")

        # 6. 清理文件系统上的实验目录
        import shutil
        from .config_composer import QE_WORKSPACE_WIN, QE_EXPERIMENTS_ROOT

        cleaned_dirs = []
        for dir_path in [
            QE_WORKSPACE_WIN / task_id,       # WSL 回测 workspace
            QE_EXPERIMENTS_ROOT / task_id,     # AIstock 侧实验副本
            Path(SOTA_ASSETS_DIR) / task_id,   # SOTA 资产 + 日志
        ]:
            if dir_path.exists() and dir_path.is_dir():
                shutil.rmtree(dir_path, ignore_errors=False)
                cleaned_dirs.append(str(dir_path))
                logger.info(f"已清理实验目录: {dir_path}")

        # 6b. 清理 Optuna study 文件
        try:
            optuna_dir = Path(SOTA_ASSETS_DIR) / "optuna_studies"
            if optuna_dir.exists():
                for f in optuna_dir.glob(f"{task_id}_*.db"):
                    f.unlink(missing_ok=True)
                    logger.info(f"已清理 Optuna study: {f}")
        except Exception as e:
            logger.warning(f"Optuna study cleanup failed for {task_id}: {e}")

        deleted_counts["cleaned_dirs"] = len(cleaned_dirs)

        logger.info(f"Task {task_id} ({task['task_name']}) deleted. Counts: {deleted_counts}")
        return {
            "task_id": task_id,
            "task_name": task["task_name"],
            "deleted_counts": deleted_counts,
            "cleaned_dirs": cleaned_dirs,
        }
        
    async def stream_task_logs(self, task_id: str):
        """
        转发 RDAgent SSE 日志流，同时写入 log 文件（SOTA_ASSETS_DIR/{task_id}/logs/evolution.log）
        """
        import aiofiles
        log_dir = os.path.join(SOTA_ASSETS_DIR, task_id, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "evolution.log")
        client = self._get_workspace_client_for_task(task_id)
        async with aiofiles.open(log_path, "a", encoding="utf-8") as log_file:
            session_header = f"\n{'='*60}\n[Session Start] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}\n"
            await log_file.write(session_header)
            async for line in client.stream_task_logs(task_id):
                # 提取日志文本写入文件
                text = line[len("data:"):].strip() if line.startswith("data:") else line
                if text:
                    await log_file.write(text + "\n")
                    await log_file.flush()
                # 若上游已是 SSE data 行则直接透传，否则包装为 SSE
                if line.startswith("data:"):
                    yield f"{line}\n\n"
                else:
                    yield f"data: {line}\n\n"

    async def get_sota_registry(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT r.*, l.config_json, l.metrics_json, t.task_name 
                    FROM qe_sota_registry r
                    JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                    JOIN qe_evolution_tasks t ON l.task_id = t.task_id
                    ORDER BY r.created_at DESC
                """)
                return [dict(row) for row in cur.fetchall()]
        
    async def sync_loop_assets(self, task_id: str, loop_id: str) -> str:
        """
        同步 RDAgent 侧的物理资产到本地（双参数：task_id + loop_id）
        注意：loop_id 来自路由参数（如 "Loop2"），DB 中存储的是 "{task_id}_Loop{N}" 格式
        """
        dest_dir = os.path.join(SOTA_ASSETS_DIR, task_id, loop_id)
        client = self._get_workspace_client_for_task(task_id)
        synced_path = await client.download_loop_assets(task_id, loop_id, dest_dir)
        
        # 更新 DB SOTA registry：loop_id 在 DB 中是 "{task_id}_{loop_id}" 格式
        evolution_loop_db_id = f"{task_id}_{loop_id}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_sota_registry 
                    SET model_assets_synced = TRUE, local_asset_path = %s 
                    WHERE loop_id = %s
                """, (synced_path, evolution_loop_db_id))
            conn.commit()
            
        return synced_path

    async def get_task_sota_assets(self, task_id: str, include_alpha_baseline: bool = False) -> Dict[str, Any]:
        """
        查询指定 RDAgent task 的 SOTA 因子和模型资产。
        用于 Phase 4 多入口演进：从 RDAgent task 创建演进任务。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 获取 SOTA 因子（排除已删除/不可用）+ LEFT JOIN 独立指标
                cur.execute("""
                    SELECT c.factor_name, c.source,
                           c.ic AS task_ic, c.annualized_return AS task_ann_ret,
                           c.max_drawdown AS task_drawdown,
                           m.ic_mean, m.rank_ic_mean, m.icir,
                           m.top_excess_sharpe, m.top_excess_annual_return,
                           m.top_max_drawdown,
                           c.is_sota_factor, c.catalog_source
                    FROM aistock_factor_catalog c
                    LEFT JOIN LATERAL (
                        SELECT ic_mean, rank_ic_mean, icir,
                               top_excess_sharpe, top_excess_annual_return,
                               top_max_drawdown
                        FROM aistock_factor_metrics
                        WHERE factor_name = c.factor_name AND eval_window = 'full'
                        ORDER BY calculated_at DESC
                        LIMIT 1
                    ) m ON TRUE
                    WHERE c.source_task_id = %s
                      AND c.is_sota_factor = TRUE
                      AND c.is_available = TRUE
                    ORDER BY m.ic_mean DESC NULLS LAST, c.ic DESC NULLS LAST
                """, (task_id,))
                sota_factors = [dict(r) for r in cur.fetchall()]

                # 获取 SOTA 模型
                cur.execute("""
                    SELECT model_id, model_name, model_type, ic, annualized_return,
                           max_drawdown, is_sota, task_run_id
                    FROM aistock_model_catalog
                    WHERE task_run_id = %s AND is_sota = TRUE
                    ORDER BY ic DESC NULLS LAST
                """, (task_id,))
                sota_models = [dict(r) for r in cur.fetchall()]

                # 可选：获取 Alpha 基准因子
                alpha_factors = []
                if include_alpha_baseline:
                    cur.execute("""
                        SELECT factor_name, source, ic
                        FROM aistock_factor_catalog
                        WHERE source IN ('alpha158', 'alpha360')
                        ORDER BY ic DESC NULLS LAST
                        LIMIT 50
                    """)
                    alpha_factors = [dict(r) for r in cur.fetchall()]

                # 统计该 task 的全部因子数（含非 SOTA），用于判断是否有演进因子
                cur.execute("""
                    SELECT COUNT(*) AS cnt
                    FROM aistock_factor_catalog
                    WHERE source_task_id = %s
                """, (task_id,))
                total_task_factors = cur.fetchone()["cnt"]

        return {
            "task_id": task_id,
            "sota_factors": sota_factors,
            "sota_models": sota_models,
            "alpha_factors": alpha_factors,
            "total_sota_factors": len(sota_factors),
            "total_sota_models": len(sota_models),
            "total_task_factors": total_task_factors,
        }

    async def create_experiment_from_task_sota(
        self,
        task_id: str,
        experiment_name: str,
        include_alpha_baseline: bool = False,
        model_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        factor_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        基于 RDAgent task 的 SOTA 资产创建真实 QE 实验。
        用于入口 D (rdagent_task_sota)。

        - factor_keys: 用户手动选择的因子 key 列表（格式 'name||source'）
                       若提供则使用用户选择的因子，否则使用 SOTA 因子
        - model_id: 用户手动选择的模型ID
                    Factor Task 必须由前端传入；Model Task 使用 SOTA 第一个模型
        """
        assets = await self.get_task_sota_assets(task_id, include_alpha_baseline)

        # 确定因子列表
        if factor_keys and len(factor_keys) > 0:
            # 用户手动选择了因子（Model Task 场景）
            factor_names = [k.split("||")[0] for k in factor_keys]
        else:
            # 使用 SOTA 因子（Factor Task 场景）
            factor_names = [f["factor_name"] for f in assets["sota_factors"]]
            if include_alpha_baseline:
                alpha_names = [f["factor_name"] for f in assets["alpha_factors"]]
                factor_names = list(dict.fromkeys(factor_names + alpha_names))

        if not factor_names:
            raise ValueError(f"RDAgent task {task_id} 没有可用因子（SOTA 因子为空且未手动选择因子）")

        # 确定模型
        # model_id 由调用方传入（Factor Task 由前端传入，Model Task 自动使用 SOTA 第一个模型）
        if not model_id and assets["sota_models"]:
            model_id = assets["sota_models"][0]["model_id"]

        # 使用 ConfigComposer 创建实验
        from .config_composer import ConfigComposer
        composer = ConfigComposer()
        result = composer.compose_experiment(
            factor_names=factor_names,
            model_id=model_id,
            strategy_id=strategy_id,
            experiment_name=experiment_name,
        )

        return {
            "ok": True,
            "experiment_id": result.get("experiment_id"),
            "factor_count": len(factor_names),
            "model_id": model_id,
            "source_task_id": task_id,
        }

    async def get_available_source_tasks(self) -> List[Dict[str, Any]]:
        """获取所有已同步的 RDAgent task 列表（从 model/factor catalog 聚合）。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT task_id,
                           COALESCE(SUM(sota_factor_count), 0)::int AS sota_factor_count,
                           COALESCE(SUM(sota_model_count), 0)::int  AS sota_model_count,
                           MAX(best_ic) AS best_ic,
                           MAX(best_sharpe) AS best_sharpe,
                           MAX(best_annualized_return) AS best_annualized_return,
                           MIN(worst_max_drawdown) AS worst_max_drawdown,
                           COALESCE(SUM(total_loops), 0)::int AS total_loops
                    FROM (
                        SELECT source_task_id AS task_id,
                               COUNT(*) FILTER (WHERE is_sota_factor = TRUE) AS sota_factor_count,
                               0 AS sota_model_count,
                               MAX(ic) AS best_ic,
                               MAX(sharpe) AS best_sharpe,
                               MAX(annualized_return) AS best_annualized_return,
                               MIN(max_drawdown) AS worst_max_drawdown,
                               0 AS total_loops
                        FROM aistock_factor_catalog
                        WHERE source_task_id IS NOT NULL AND source_task_id != ''
                        GROUP BY source_task_id
                        UNION ALL
                        SELECT task_run_id AS task_id,
                               0 AS sota_factor_count,
                               COUNT(*) FILTER (WHERE is_sota = TRUE) AS sota_model_count,
                               MAX(ic) AS best_ic,
                               MAX(sharpe) AS best_sharpe,
                               MAX(annualized_return) AS best_annualized_return,
                               MIN(max_drawdown) AS worst_max_drawdown,
                               COUNT(DISTINCT loop_id) AS total_loops
                        FROM aistock_model_catalog
                        WHERE task_run_id IS NOT NULL
                        GROUP BY task_run_id
                    ) sub
                    GROUP BY task_id
                    HAVING COALESCE(SUM(sota_factor_count), 0) > 0
                        OR COALESCE(SUM(sota_model_count), 0) > 0
                    ORDER BY best_ic DESC NULLS LAST
                """)
                tasks = [dict(r) for r in cur.fetchall()]
                for t in tasks:
                    t["has_sota"] = t["sota_model_count"] > 0 or t["sota_factor_count"] > 0

        return tasks

    async def get_completed_experiments(self) -> List[Dict[str, Any]]:
        """获取所有已完成的 QE 实验列表，用于作为演进起点选择。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT e.experiment_id, e.experiment_name, e.status,
                           e.factor_names, e.model_id, e.strategy_id,
                           e.ic, e.icir, e.rank_ic, e.annualized_return,
                           e.max_drawdown, e.information_ratio,
                           e.is_sota, e.is_evolution_loop,
                           e.created_at, e.completed_at,
                           COALESCE(jsonb_array_length(e.factor_names), 0) AS factor_count
                    FROM qe_experiments e
                    WHERE e.status = 'completed'
                    ORDER BY e.completed_at DESC NULLS LAST
                    LIMIT 200
                """)
                experiments = [dict(r) for r in cur.fetchall()]
        return experiments

    # ================================================================
    # 策略演进（Strategy Evolution）- 跳过训练的批量策略回测
    # ================================================================

    async def strategy_fork_task(
        self,
        source_task_id: str,
        from_loop_index: int,
        task_name: Optional[str] = None,
        loops_config: List[Dict[str, Any]] = None,
        execution_mode: str = "serial",
        inherit_history: bool = False,
        node_id: Optional[str] = None,
    ) -> str:
        """
        从指定 task 的某个已完成 loop 创建策略演进任务。
        复用源 loop 的模型，仅修改策略参数进行批量回测。
        """
        if not loops_config or len(loops_config) == 0:
            raise ValueError("loops_config 不能为空，至少需要配置一个 Loop")

        # 1. 验证源 task 和 loop
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (source_task_id,))
                source_task = cur.fetchone()
                if not source_task:
                    raise ValueError(f"源任务不存在: {source_task_id}")

                cur.execute(
                    "SELECT * FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s AND status = 'completed'",
                    (source_task_id, from_loop_index),
                )
                source_loop = cur.fetchone()
                if not source_loop:
                    raise ValueError(
                        f"源任务 {source_task_id} 中不存在已完成的 Loop {from_loop_index}"
                    )

        # 2. 验证模型文件存在
        client = self._get_workspace_client_for_task(source_task_id)
        source_loop_id = f"Loop{from_loop_index}"
        workspace_status = await client.get_loop_status(source_task_id, source_loop_id)
        if workspace_status.get("status") not in ("completed",):
            logger.warning(
                f"源 Loop {source_task_id} L{from_loop_index} 在 workspace 侧状态为 {workspace_status.get('status')}，"
                "模型文件可能不完整"
            )

        # 3. 读取源 loop 的基础配置（因子、模型、data_split 等）
        config = source_loop.get("config_json") or {}
        if isinstance(config, str):
            config = json.loads(config)

        base_factor_list = config.get("factor_list", [])
        base_model_id = config.get("model_id")
        base_strategy_id = config.get("strategy_id")
        base_data_split = config.get("data_split", {})
        base_model_params = config.get("model_params", {})

        if not base_factor_list:
            raise ValueError(f"源 Loop {from_loop_index} 的因子列表为空，无法创建策略演进")
        if not base_model_id:
            raise ValueError(f"源 Loop {from_loop_index} 的 model_id 为空，无法复用模型")

        # 4. 生成新 task_id
        suffix = uuid.uuid4().hex[:4]
        new_task_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{suffix}"
        if not task_name:
            source_name = source_task.get("task_name", source_task_id)
            task_name = f"{source_name}_策略演进_L{from_loop_index}"

        target_desc = source_task.get("target_desc", "")

        # 5. 创建 base_experiment 记录
        base_exp_id = f"{new_task_id}_base"
        metrics = source_loop.get("metrics_json") or {}
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        # node_id: 入参优先，否则继承源任务
        effective_node_id = node_id if node_id is not None else source_task.get("node_id")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                    (experiment_id, experiment_name, qe_task_id, qe_loop_id,
                     loop_index, parent_experiment_id,
                     is_evolution_loop, factor_names, model_id, strategy_id,
                     data_split, custom_params,
                     result_metrics, status, is_sota)
                    VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s, %s, %s, %s, %s, %s, 'created', FALSE)
                """, (
                    base_exp_id,
                    f"策略演进基准 from {source_task_id} L{from_loop_index}",
                    new_task_id,
                    source_loop_id,
                    0,
                    None,
                    json.dumps(base_factor_list),
                    base_model_id,
                    base_strategy_id,
                    json.dumps(base_data_split) if isinstance(base_data_split, dict) else base_data_split,
                    json.dumps(base_model_params) if isinstance(base_model_params, dict) else base_model_params,
                    json.dumps(metrics),
                ))

                # 6. 创建策略演进任务
                cur.execute("""
                    INSERT INTO qe_evolution_tasks
                    (task_id, task_name, target_desc, max_loops, current_loop, status,
                     base_experiment_id, node_id, source_type,
                     task_type, strategy_evo_config, strategy_evo_execution_mode,
                     model_source_task_id, model_source_loop_index,
                     fork_from_task_id, fork_from_loop_index, inherit_history)
                    VALUES (%s, %s, %s, %s, 0, 'pending', %s, %s, 'strategy_fork',
                            'strategy_evo', %s, %s, %s, %s, %s, %s, %s)
                """, (
                    new_task_id, task_name, target_desc, len(loops_config),
                    base_exp_id, effective_node_id,
                    json.dumps({"loops": loops_config}),
                    execution_mode,
                    source_task_id, from_loop_index,
                    source_task_id, from_loop_index, inherit_history,
                ))
            conn.commit()

        logger.info(
            f"创建策略演进任务 {new_task_id} 从 {source_task_id} L{from_loop_index}, "
            f"共 {len(loops_config)} 个 Loop, 执行方式={execution_mode}"
        )

        # 7. 异步启动批量调度
        bg_task = asyncio.create_task(self.submit_strategy_evo_all_loops(new_task_id))
        bg_task.add_done_callback(
            lambda t: logger.error(f"submit_strategy_evo_all_loops failed: {t.exception()}") if t.exception() else None
        )

        return new_task_id

    async def submit_strategy_evo_loop(self, task_id: str, loop_index: int) -> Optional[str]:
        """
        提交单个策略回测 Loop（跳过训练）。
        使用 --backtest-only 模式，复用源 loop 的模型。
        """
        # 读取 task 的 strategy_evo_config
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return None

        strategy_evo_config = task.get("strategy_evo_config")
        if not strategy_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空，数据完整性异常")
            return None
        if isinstance(strategy_evo_config, str):
            strategy_evo_config = json.loads(strategy_evo_config)

        loops_config = strategy_evo_config.get("loops")
        if not loops_config:
            logger.error(f"Task {task_id} strategy_evo_config.loops 为空")
            return None

        loop_config = None
        for cfg in loops_config:
            if cfg.get("loop_index") == loop_index:
                loop_config = cfg
                break

        if not loop_config:
            logger.error(f"Loop {loop_index} 的配置未找到")
            raise ValueError(f"Loop configuration not found for loop_index={loop_index} in task {task_id}")

        # 加载源 Loop 的基础配置
        source_task_id = task.get("model_source_task_id")
        source_loop_idx = task.get("model_source_loop_index")

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT config_json FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s",
                    (source_task_id, source_loop_idx),
                )
                source_loop_row = cur.fetchone()

        if not source_loop_row:
            logger.error(f"源 Loop {source_task_id} L{source_loop_idx} 未找到")
            return None

        source_config = source_loop_row.get("config_json") or {}
        if isinstance(source_config, str):
            source_config = json.loads(source_config)

        # 合并策略参数
        base_config = {
            "action_type": "strategy_backtest",
            "factor_list": source_config.get("factor_list", []),
            "model_id": source_config.get("model_id"),
            "model_params": source_config.get("model_params", {}),
            "data_split": source_config.get("data_split", {}),
            "strategy_id": loop_config.get("strategy_id") or source_config.get("strategy_id"),
        }

        # 策略参数覆盖
        strategy_params = loop_config.get("strategy_params", {})
        if strategy_params:
            base_config["model_params"] = dict(source_config.get("model_params", {}))
            base_config["model_params"].update(strategy_params)

        execution_algo = loop_config.get("execution_algo")
        execution_algo_params = loop_config.get("execution_algo_params", {})

        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_id = f"Loop{loop_index}"

        # 创建 LOOP 记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_evolution_loops
                    (loop_id, task_id, loop_index, status, action_type)
                    VALUES (%s, %s, %s, 'running', 'strategy_backtest')
                    ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()
                """, (evolution_loop_db_id, task_id, loop_index))
            conn.commit()

        # 使用 ConfigComposer 组装实验
        try:
            from .config_composer import ConfigComposer
            composer = ConfigComposer()

            loop_custom_params = dict(base_config.get("model_params", {}))
            task_stock_pool = task.get("stock_pool")

            # 处理 HMM 和行业黑名单
            if loop_config.get("enable_sector_hmm"):
                loop_custom_params["enable_sector_hmm"] = True
                hmm_model_version = loop_config.get("hmm_model_version_id")
                hmm_preset = loop_config.get("hmm_signal_preset")
                if not hmm_model_version:
                    raise ValueError(
                        "enable_sector_hmm=True 但 hmm_model_version_id 未配置。"
                        "请在任务配置中指定 HMM 模型版本。"
                    )
                loop_custom_params["hmm_model_version_id"] = hmm_model_version
                # 解析 snapshot_id → sector_hmm_model_path
                from ..hmm_training_service import HMMTrainingService
                _hmm_svc = HMMTrainingService()
                _snapshot = _hmm_svc.get_snapshot(hmm_model_version)
                if _snapshot is None:
                    raise ValueError(f"HMM 快照 {hmm_model_version} 不存在")
                loop_custom_params["sector_hmm_model_path"] = _snapshot["model_path"]
                if hmm_preset:
                    loop_custom_params["hmm_signal_preset"] = hmm_preset

            sector_blacklist = loop_config.get("sector_blacklist", [])
            if sector_blacklist:
                # TODO: 生成过滤黑名单行业的 stock_pool 文件
                loop_custom_params["sector_blacklist"] = sector_blacklist

            if task_stock_pool:
                loop_custom_params["stock_pool"] = task_stock_pool
            task_label_type = task.get("label_type")
            if task_label_type:
                loop_custom_params["label_type"] = task_label_type

            _sp = strategy_params.copy() if strategy_params else {}
            # 注入尾盘未成交处理配置 → custom_params（优先 loop 级别，fallback 到 task 级别）
            _uf = loop_config.get("unfilled_handler") or task.get("unfilled_handler")
            if _uf:
                loop_custom_params["unfilled_handler"] = _uf
                _uf_params = loop_config.get("unfilled_handler_params") or task.get("unfilled_handler_params") or {}
                if isinstance(_uf_params, str):
                    import json as _json
                    _uf_params = _json.loads(_uf_params)
                if _uf_params.get("trigger_minute"):
                    loop_custom_params["unfilled_trigger_minute"] = _uf_params["trigger_minute"]
                if _uf_params.get("backup_depth"):
                    loop_custom_params["unfilled_backup_depth"] = _uf_params["backup_depth"]
            loop_custom_params.pop("initial_cash", None)

            compose_res = composer.compose_experiment_in_memory(
                factor_names=base_config.get("factor_list", []),
                model_id=base_config.get("model_id"),
                strategy_id=base_config.get("strategy_id"),
                data_split=base_config.get("data_split"),
                custom_params=loop_custom_params,
                experiment_name=f"{task_id}/{loop_id}",
                skip_db_save=True,
                execution_algo=execution_algo,
                execution_algo_params=execution_algo_params,
                strategy_params=_sp,
                node_id=task.get("node_id"),
            )
            experiment_files = compose_res["experiment_files"]
            wsl_command = compose_res.get("wsl_command", "")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops SET config_json = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(base_config), evolution_loop_db_id))
                conn.commit()

            # 注入 --backtest-only 和 model_source
            import re as _re
            wsl_command = _re.sub(
                r"(python\s+qrun_limit_minute\.py\s+\S+\.yaml)",
                r"\1 --backtest-only",
                wsl_command,
            )

            # 调用节点执行
            model_source = {
                "source_task_id": source_task_id,
                "source_loop": f"Loop{source_loop_idx}",
            }

            # 跨节点检测：源 task 和目标 task 在不同节点时，同步 mlruns 中的 params.pkl
            target_node_id = task.get("node_id")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT node_id FROM qe_evolution_tasks WHERE task_id = %s", (source_task_id,))
                    src_row = cur.fetchone()
            source_node_id = src_row[0] if src_row else None

            if (target_node_id or None) != (source_node_id or None):
                # 跨节点：从源节点下载 params.pkl，通过 experiment_files 传给目标节点
                logger.info(f"跨节点策略演进: 源={source_node_id or 'local'} → 目标={target_node_id or 'local'}，同步 mlruns")
                try:
                    source_client = self.workspace_client if not source_node_id else self._node_clients.get(source_node_id) or QEWorkspaceClient.for_node(source_node_id)
                    mlruns_tar = await source_client.download_mlruns_params(source_task_id, f"Loop{source_loop_idx}")
                    if mlruns_tar:
                        import base64
                        experiment_files["mlruns_params.tar.gz.b64"] = base64.b64encode(mlruns_tar).decode("ascii")
                        model_source["cross_node"] = True
                        logger.info(f"跨节点 mlruns 同步完成: {len(mlruns_tar)} bytes")
                    else:
                        logger.warning("跨节点 mlruns 下载返回空，回测可能失败")
                except Exception as e:
                    logger.error(f"跨节点 mlruns 同步失败: {e}，回测可能失败")

            client = self._get_workspace_client_for_task(task_id)
            callback_url = self._get_callback_url_for_task(task_id)
            await client.create_and_run_loop(
                task_id, loop_index, base_config, experiment_files, wsl_command,
                model_source=model_source,
                callback_url=callback_url,
            )

            logger.info(f"策略演进 Loop {loop_index} 已提交 (backtest-only)")
            return evolution_loop_db_id

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"策略演进 Loop {loop_index} 提交失败: {e}\n{tb_str}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"策略演进 Loop {loop_index} 失败标记失败: {db_err}")
            return None

    async def submit_strategy_evo_all_loops(self, task_id: str):
        """
        批量调度策略演进 Loops，支持串行/并行模式。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return

        strategy_evo_config = task.get("strategy_evo_config")
        if not strategy_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空，数据完整性异常")
            return
        if isinstance(strategy_evo_config, str):
            strategy_evo_config = json.loads(strategy_evo_config)

        loops_config = strategy_evo_config.get("loops", [])
        if not loops_config:
            logger.error(f"策略演进任务 {task_id} 没有 loops 配置")
            return

        execution_mode_raw = task.get("strategy_evo_execution_mode", "serial")
        if execution_mode_raw.startswith("parallel"):
            mode = "parallel"
            parts = execution_mode_raw.split("_")
            parallelism = int(parts[1]) if len(parts) > 1 else 2
        else:
            mode = "serial"
            parallelism = 1

        # 标记任务为 running
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s", (task_id,))
            conn.commit()

        # 恢复场景：过滤掉已完成的 loop，只提交 pending/failed 的
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT loop_index, status FROM qe_evolution_loops WHERE task_id = %s",
                    (task_id,),
                )
                existing_loops = {row["loop_index"]: row["status"] for row in cur.fetchall()}

        loops_to_run = []
        for loop_config in loops_config:
            loop_index = loop_config.get("loop_index", len(loops_config))
            status = existing_loops.get(loop_index)
            if status == "completed":
                logger.info(f"策略演进 Loop {loop_index} 已完成，跳过")
                continue
            loops_to_run.append(loop_config)

        if not loops_to_run:
            logger.info(f"策略演进任务 {task_id} 所有 Loop 已完成")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()
            return

        if mode == "serial":
            # 串行模式：逐个提交并等待完成
            for loop_config in loops_to_run:
                loop_index = loop_config.get("loop_index", len(loops_config))
                loop_id = await self.submit_strategy_evo_loop(task_id, loop_index)
                if not loop_id:
                    logger.error(f"策略演进 Loop {loop_index} 提交失败，停止后续 Loops")
                    break

                # 等待 Loop 完成（超时 2 小时）
                max_wait = 7200  # 2 小时
                waited = 0
                interval = 10
                while waited < max_wait:
                    await asyncio.sleep(interval)
                    waited += interval

                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT status FROM qe_evolution_loops WHERE loop_id = %s",
                                (loop_id,),
                            )
                            row = cur.fetchone()
                    if not row:
                        break

                    status = row[0]
                    if status in ("completed", "failed", "cancelled"):
                        break

                # 超时检查
                if waited >= max_wait:
                    logger.error(f"策略演进 Loop {loop_index} 等待超时（{max_wait}s），标记为 failed")
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s AND status = 'running'",
                                (loop_id,),
                            )
                        conn.commit()
                    continue

                # 处理完成的 Loop
                if loop_id:
                    await self._safe_process_completed_loop(task_id, loop_id)

        else:
            # 并行模式：使用 semaphore 控制并行度（持有到 loop 完成）
            sem = asyncio.Semaphore(parallelism)
            logger.info(f"策略演进 {task_id} 并行模式启动，并行度={parallelism}，共 {len(loops_to_run)} 个 Loop（跳过 {len(loops_config) - len(loops_to_run)} 个已完成）")

            async def run_with_sem(loop_config):
                loop_index = loop_config.get("loop_index", len(loops_config))
                async with sem:
                    loop_id = await self.submit_strategy_evo_loop(task_id, loop_index)
                    if not loop_id:
                        return None
                    # 等待 loop 完成后才释放 semaphore
                    max_wait = 7200
                    waited = 0
                    interval = 10
                    while waited < max_wait:
                        await asyncio.sleep(interval)
                        waited += interval
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                                row = cur.fetchone()
                        if not row or row[0] in ("completed", "failed", "cancelled"):
                            break
                    if waited >= max_wait:
                        logger.error(f"并行模式 Loop {loop_index} 等待超时（{max_wait}s）")
                    if loop_id:
                        await self._safe_process_completed_loop(task_id, loop_id)
                    return loop_id

            tasks = [run_with_sem(lc) for lc in loops_to_run]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"并行模式 Loop {loops_to_run[i].get('loop_index', i+1)} 提交失败: {result}")

    async def process_strategy_evo_completed_loop(self, task_id: str, loop_id_str: str) -> bool:
        """
        处理策略演进 Loop 的完成事件（简化版 process_completed_loop）。
        跳过 Agent 分析、SOTA 判定，只收集 metrics。
        """
        # 提取 loop_index
        loop_suffix = loop_id_str.rsplit("_Loop", 1)
        if len(loop_suffix) != 2:
            logger.error(f"Invalid loop_id format: {loop_id_str}")
            return False
        try:
            loop_index = int(loop_suffix[1])
        except ValueError:
            logger.error(f"Invalid loop_index in loop_id: {loop_id_str}")
            return False

        evolution_loop_db_id = loop_id_str

        # CAS 幂等保护
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    UPDATE qe_evolution_loops SET status = 'processing', updated_at = NOW()
                    WHERE loop_id = %s AND status = 'running'
                    RETURNING loop_id
                """, (evolution_loop_db_id,))
                cas_row = cur.fetchone()
            conn.commit()

        if not cas_row:
            logger.info(f"Loop {evolution_loop_db_id} is not in 'running' state, skipping.")
            return False

        try:
            # 获取回测结果
            client = self._get_workspace_client_for_task(task_id)
            loop_id = f"Loop{loop_index}"
            metrics = await client.get_loop_metrics(task_id, loop_id)

            # Normalize metric keys
            _METRIC_ALIASES = {
                "Rank IC": "Rank_IC",
                "1day.excess_return_with_cost.information_ratio": "sharpe",
                "1day.excess_return_with_cost.annualized_return": "annualized_return",
                "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
                "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
                "1day.excess_return_without_cost.information_ratio": "sharpe_no_cost",
                "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
                "1day.excess_return_with_cost.mean": "daily_return",
                "1day.excess_return_without_cost.mean": "daily_return_no_cost",
            }
            for src, dst in _METRIC_ALIASES.items():
                if src in metrics and dst not in metrics:
                    metrics[dst] = metrics[src]

            # 拉取增强诊断指标（与 process_completed_loop:1227-1234 对齐）
            # 失败时 raise — 前端 on-demand API 依赖 metrics_json 中的 enhanced_metrics 字段
            enhanced_data = await client.get_enhanced_metrics(task_id, loop_id)
            td = enhanced_data.get("training_diagnostics", {})
            if not td and "training_curves" in enhanced_data:
                td = self._compute_training_diagnostics(enhanced_data.get("training_curves", {}))
                enhanced_data["training_diagnostics"] = td
            metrics["enhanced_metrics"] = enhanced_data

            # 读取配置
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT config_json FROM qe_evolution_loops WHERE loop_id = %s", (evolution_loop_db_id,))
                    loop_row = cur.fetchone()
            if not loop_row or not loop_row.get('config_json'):
                raise ValueError(f"Loop {evolution_loop_db_id} config_json 为空，数据完整性异常")
            config = loop_row['config_json']
            if isinstance(config, str):
                config = json.loads(config)

            # 更新 LOOP 记录
            with get_conn() as conn:
                with conn.cursor() as cur:
                    experiment_id = f"{task_id}_L{loop_index}"
                    cur.execute("""
                        INSERT INTO qe_experiments
                        (experiment_id, experiment_name, qe_task_id, qe_loop_id,
                         loop_index, parent_experiment_id,
                         is_evolution_loop, factor_names, model_id, strategy_id,
                         data_split, custom_params,
                         result_metrics, status, is_sota)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s, 'completed', FALSE)
                        ON CONFLICT (experiment_id) DO UPDATE SET
                            result_metrics = EXCLUDED.result_metrics,
                            status = EXCLUDED.status
                    """, (
                        experiment_id,
                        f"{task_id} 策略回测{loop_index}",
                        task_id, loop_id, loop_index, task_id,
                        json.dumps(config.get("factor_list", [])),
                        config.get("model_id"), config.get("strategy_id"),
                        json.dumps(config.get("data_split", {})),
                        json.dumps(config.get("model_params", {})),
                        json.dumps(metrics),
                    ))

                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET metrics_json = %s, status = 'completed',
                            experiment_id = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(metrics), experiment_id, evolution_loop_db_id))
                conn.commit()

            # 更新任务进度
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT current_loop, max_loops FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task_row = cur.fetchone()

            current_loop = task_row.get("current_loop", 0) if task_row else 0
            max_loops = task_row.get("max_loops", 0) if task_row else 0

            if loop_index > current_loop:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET current_loop = %s, updated_at = NOW() WHERE task_id = %s", (loop_index, task_id))
                    conn.commit()

            # 检查是否所有 Loops 完成
            if loop_index >= max_loops:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
                logger.info(f"策略演进任务 {task_id} 完成")
            else:
                # 串行模式：提交下一个 Loop
                execution_mode = task_row.get("strategy_evo_execution_mode", "serial") if task_row else "serial"
                if execution_mode == "serial":
                    next_loop_index = loop_index + 1
                    next_task = asyncio.create_task(self.submit_strategy_evo_loop(task_id, next_loop_index))
                    next_task.add_done_callback(
                        lambda t: logger.error(f"submit_strategy_evo_loop failed: {t.exception()}") if t.exception() else None
                    )

            return True

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"策略演进 Loop {evolution_loop_db_id} 处理失败: {e}\n{tb_str}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"FATAL: 策略演进 Loop {evolution_loop_db_id} 失败标记失败: {db_err}")
            return False

    # ═══════════════════════════════════════════════════════════════════
    # 自定义演进 (Custom Evolution) — 每个 Loop 完全自定义因子+模型+策略
    # ═══════════════════════════════════════════════════════════════════

    async def create_custom_evo_task(
        self,
        task_name: str,
        target_desc: str = "",
        loops_config: List[Dict[str, Any]] = None,
        execution_mode: str = "serial",
        node_id: Optional[str] = None,
        engine_mode: str = "legacy",
    ) -> str:
        """
        创建自定义演进任务。每个 Loop 都可以完全自定义因子、模型、策略配置，
        执行完整的训练+回测流程。
        """
        if not loops_config or len(loops_config) == 0:
            raise ValueError("loops_config 不能为空，至少需要配置一个 Loop")

        # 生成 task_id
        suffix = uuid.uuid4().hex[:4]
        new_task_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{suffix}"

        # 用第一个 Loop 的配置创建 base experiment 记录
        first_loop = loops_config[0]
        factor_names = [k.split("||")[0] for k in first_loop.get("factor_keys", [])]
        base_exp_id = f"{new_task_id}_base"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                    (experiment_id, experiment_name, qe_task_id,
                     is_evolution_loop, factor_names, model_id, strategy_id,
                     data_split, custom_params, status, is_sota)
                    VALUES (%s, %s, %s, FALSE, %s, %s, %s, %s, %s, 'created', FALSE)
                """, (
                    base_exp_id,
                    f"自定义演进基准 {task_name}",
                    new_task_id,
                    json.dumps(factor_names),
                    first_loop.get("model_id"),
                    first_loop.get("strategy_id"),
                    json.dumps(first_loop.get("data_split") or {}),
                    json.dumps(first_loop.get("strategy_params") or {}),
                ))

                # 创建自定义演进任务
                cur.execute("""
                    INSERT INTO qe_evolution_tasks
                    (task_id, task_name, target_desc, max_loops, current_loop, status,
                     base_experiment_id, node_id, source_type,
                     task_type, strategy_evo_config, strategy_evo_execution_mode)
                    VALUES (%s, %s, %s, %s, 0, 'pending', %s, %s, 'custom',
                            'custom_evo', %s, %s)
                """, (
                    new_task_id, task_name, target_desc, len(loops_config),
                    base_exp_id, node_id,
                    json.dumps({"loops": loops_config, "engine_mode": engine_mode}),
                    execution_mode,
                ))
            conn.commit()

        logger.info(
            f"创建自定义演进任务 {new_task_id}, "
            f"共 {len(loops_config)} 个 Loop, 执行方式={execution_mode}"
        )

        # 异步启动批量调度
        bg_task = asyncio.create_task(self.submit_custom_evo_all_loops(new_task_id))
        bg_task.add_done_callback(
            lambda t: logger.error(f"submit_custom_evo_all_loops failed: {t.exception()}") if t.exception() else None
        )

        return new_task_id

    async def submit_custom_evo_loop(self, task_id: str, loop_index: int, force_full_train: bool = False) -> Optional[str]:
        """
        提交单个自定义演进 Loop（完整训练+回测）。
        从 strategy_evo_config.loops 中读取该 Loop 的完整因子+模型+策略配置。
        force_full_train=True 时，忽略 backtest_only，强制完整训练。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return None

        custom_evo_config = task.get("strategy_evo_config")
        if not custom_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空")
            return None
        if isinstance(custom_evo_config, str):
            custom_evo_config = json.loads(custom_evo_config)

        loops_config = custom_evo_config.get("loops", [])
        loop_config = None
        for cfg in loops_config:
            if cfg.get("loop_index") == loop_index:
                loop_config = cfg
                break

        if not loop_config:
            logger.error(f"Loop {loop_index} 的配置未找到")
            raise ValueError(f"Loop configuration not found for loop_index={loop_index} in task {task_id}")

        # engine_mode 分发：从 strategy_evo_config 读取
        _engine_mode = custom_evo_config.get("engine_mode", "legacy")
        if _engine_mode == "unified":
            return await self._submit_custom_evo_loop_unified(task_id, loop_index, loop_config, task, force_full_train=force_full_train)
        elif _engine_mode != "legacy":
            logger.error(
                f"Task {task_id} Loop {loop_index}: 未知 engine_mode={_engine_mode!r}，"
                f"回退到 legacy 路径。请检查任务配置。"
            )

        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_id = f"Loop{loop_index}"

        # 创建 LOOP 记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_evolution_loops
                    (loop_id, task_id, loop_index, status, action_type)
                    VALUES (%s, %s, %s, 'running', 'custom_config')
                    ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()
                """, (evolution_loop_db_id, task_id, loop_index))
            conn.commit()

        try:
            from .config_composer import ConfigComposer
            composer = ConfigComposer()

            # 从 loop_config 提取完整配置
            factor_keys = loop_config.get("factor_keys", [])
            factor_names = [k.split("||")[0] for k in factor_keys]
            model_id = loop_config.get("model_id")
            strategy_id = loop_config.get("strategy_id")
            strategy_params = loop_config.get("strategy_params") or {}
            execution_algo = loop_config.get("execution_algo")
            execution_algo_params = loop_config.get("execution_algo_params") or {}
            data_split = loop_config.get("data_split")
            label_type = loop_config.get("label_type")

            # 构建 custom_params
            loop_custom_params = dict(strategy_params)

            if loop_config.get("enable_sector_hmm"):
                loop_custom_params["enable_sector_hmm"] = True
                hmm_ver = loop_config.get("hmm_model_version_id")
                if not hmm_ver:
                    raise ValueError(f"Loop {loop_index}: enable_sector_hmm=True 但 hmm_model_version_id 未配置")
                loop_custom_params["hmm_model_version_id"] = hmm_ver
                # 解析 snapshot_id → sector_hmm_model_path
                from ..hmm_training_service import HMMTrainingService
                _hmm_svc = HMMTrainingService()
                _snapshot = _hmm_svc.get_snapshot(hmm_ver)
                if _snapshot is None:
                    raise ValueError(f"Loop {loop_index}: HMM 快照 {hmm_ver} 不存在")
                loop_custom_params["sector_hmm_model_path"] = _snapshot["model_path"]
                hmm_preset = loop_config.get("hmm_signal_preset")
                if hmm_preset:
                    loop_custom_params["hmm_signal_preset"] = hmm_preset

            if loop_config.get("sector_blacklist"):
                loop_custom_params["sector_blacklist"] = loop_config["sector_blacklist"]

            if loop_config.get("stock_pool"):
                loop_custom_params["stock_pool"] = loop_config["stock_pool"]

            if label_type:
                loop_custom_params["label_type"] = label_type

            # 尾盘处理
            _uf = loop_config.get("unfilled_handler")
            if _uf:
                loop_custom_params["unfilled_handler"] = _uf
                _uf_params = loop_config.get("unfilled_handler_params") or {}
                if isinstance(_uf_params, str):
                    _uf_params = json.loads(_uf_params)
                if _uf_params.get("trigger_minute"):
                    loop_custom_params["unfilled_trigger_minute"] = _uf_params["trigger_minute"]
                if _uf_params.get("backup_depth"):
                    loop_custom_params["unfilled_backup_depth"] = _uf_params["backup_depth"]

            loop_custom_params.pop("initial_cash", None)
            _sp = strategy_params.copy() if strategy_params else {}

            # 构建 config 记录（用于 DB 存储）
            config = {
                "action_type": "custom_config",
                "label": loop_config.get("label"),
                "factor_list": factor_names,
                "model_id": model_id,
                "strategy_id": strategy_id,
                "model_params": strategy_params,
                "data_split": data_split or {},
            }

            compose_res = composer.compose_experiment_in_memory(
                factor_names=factor_names,
                model_id=model_id,
                strategy_id=strategy_id,
                data_split=data_split,
                custom_params=loop_custom_params,
                experiment_name=f"{task_id}/{loop_id}",
                skip_db_save=True,
                execution_algo=execution_algo,
                execution_algo_params=execution_algo_params,
                strategy_params=_sp,
                node_id=task.get("node_id"),
            )
            experiment_files = compose_res["experiment_files"]
            wsl_command = compose_res.get("wsl_command", "")

            # 保存配置到 loop 记录
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops SET config_json = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(config), evolution_loop_db_id))
                conn.commit()

            # 提交到节点执行（完整训练+回测，不加 --backtest-only）
            client = self._get_workspace_client_for_task(task_id)
            callback_url = self._get_callback_url_for_task(task_id)
            await client.create_and_run_loop(
                task_id, loop_index, config, experiment_files, wsl_command,
                callback_url=callback_url,
            )

            logger.info(f"自定义演进 Loop {loop_index} 已提交 (完整训练+回测)")
            return evolution_loop_db_id

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"自定义演进 Loop {loop_index} 提交失败: {e}\n{tb_str}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                        # 不标记整个 task 为 failed — 其他 loop 可能还在跑（并行模式）
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"自定义演进 Loop {loop_index} 失败标记失败: {db_err}")
            return None

    async def _submit_custom_evo_loop_unified(
        self,
        task_id: str,
        loop_index: int,
        loop_config: Dict[str, Any],
        task: Dict[str, Any],
        force_full_train: bool = False,
    ) -> Optional[str]:
        """
        统一引擎路径：使用 ExperimentConfig + BacktestExecutor 提交自定义演进 Loop。
        替代 submit_custom_evo_loop 中的手动参数组装代码。
        """
        from .experiment_config_builders import build_config_from_custom_evo_loop
        from .executors.backtest import BacktestExecutor, BacktestMode
        from .executors.base import ExecutionContext
        from .config_composer import ConfigComposer
        from .qe_workspace_client import QEWorkspaceClient

        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_id = f"Loop{loop_index}"

        # 创建 LOOP 记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_evolution_loops
                    (loop_id, task_id, loop_index, status, action_type)
                    VALUES (%s, %s, %s, 'running', 'custom_config')
                    ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()
                """, (evolution_loop_db_id, task_id, loop_index))
            conn.commit()

        try:
            # 1. 构建 ExperimentConfig（配置层）
            experiment_name = f"{task_id}/{loop_id}"
            cfg = build_config_from_custom_evo_loop(
                loop_config=loop_config,
                task=task,
                experiment_name=experiment_name,
            )

            # 2. 保存 config 记录到 loop
            config_record = {
                "action_type": "custom_config",
                "label": loop_config.get("label"),
                "factor_list": cfg.factor_names,
                "model_id": cfg.model_id,
                "strategy_id": cfg.strategy_id,
                "model_params": cfg.build_custom_params(),
                "data_split": cfg.data_split or {},
            }
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops SET config_json = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(config_record), evolution_loop_db_id))
                conn.commit()

            # 3. 执行层提交
            composer = ConfigComposer()
            client = self._get_workspace_client_for_task(task_id)
            executor = BacktestExecutor(composer, client)
            ctx = ExecutionContext(
                task_id=task_id,
                loop_index=loop_index,
                experiment_name=experiment_name,
                node_id=task.get("node_id"),
                callback_url=self._get_callback_url_for_task(task_id),
            )
            # backtest-only 模式：注入 model_source 并切换执行模式
            # force_full_train 可覆盖 backtest_only 配置，用于恢复时源模型不可用的场景
            if cfg.backtest_only and not force_full_train:
                if not cfg.model_source_task_id or cfg.model_source_loop_index is None:
                    raise ValueError(
                        f"Loop {loop_index}: backtest_only=True 但未指定 model_source"
                    )
                ctx = ExecutionContext(
                    task_id=task_id,
                    loop_index=loop_index,
                    experiment_name=experiment_name,
                    node_id=task.get("node_id"),
                    callback_url=self._get_callback_url_for_task(task_id),
                    model_source={
                        "source_task_id": cfg.model_source_task_id,
                        "source_loop": f"Loop{cfg.model_source_loop_index}",
                    },
                )
                mode = BacktestMode.BACKTEST_ONLY
                logger.info(
                    f"[unified] 自定义演进 Loop {loop_index} 使用 backtest-only 模式，"
                    f"model_source={cfg.model_source_task_id}/Loop{cfg.model_source_loop_index}"
                )
            else:
                if force_full_train and cfg.backtest_only:
                    logger.info(
                        f"[unified] 自定义演进 Loop {loop_index} force_full_train=True，"
                        f"忽略 backtest_only 配置，执行完整训练"
                    )
                mode = BacktestMode.FULL_TRAIN
            await executor.submit(cfg, ctx, mode=mode)

            logger.info(f"[unified] 自定义演进 Loop {loop_index} 已提交")
            return evolution_loop_db_id

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"[unified] 自定义演进 Loop {loop_index} 提交失败: {e}\n{tb_str}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute(
                            "UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s",
                            (error_detail, evolution_loop_db_id),
                        )
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"[unified] Loop {loop_index} 失败标记失败: {db_err}")
            return None

    async def submit_custom_evo_all_loops(self, task_id: str, force_full_train: bool = False):
        """
        批量调度自定义演进 Loops，支持串行/并行模式。
        与 submit_strategy_evo_all_loops 逻辑相同，但调用 submit_custom_evo_loop。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return

        custom_evo_config = task.get("strategy_evo_config")
        if not custom_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空")
            return
        if isinstance(custom_evo_config, str):
            custom_evo_config = json.loads(custom_evo_config)

        loops_config = custom_evo_config.get("loops", [])
        if not loops_config:
            logger.error(f"自定义演进任务 {task_id} 没有 loops 配置")
            return

        execution_mode_raw = task.get("strategy_evo_execution_mode", "serial")
        if execution_mode_raw.startswith("parallel"):
            mode = "parallel"
            parts = execution_mode_raw.split("_")
            parallelism = int(parts[1]) if len(parts) > 1 else 2
        else:
            mode = "serial"
            parallelism = 1

        # 标记任务为 running
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s", (task_id,))
            conn.commit()

        # 过滤已完成的 loop
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT loop_index, status FROM qe_evolution_loops WHERE task_id = %s",
                    (task_id,),
                )
                existing_loops = {row["loop_index"]: row["status"] for row in cur.fetchall()}

        loops_to_run = []
        for loop_config in loops_config:
            loop_index = loop_config.get("loop_index")
            status = existing_loops.get(loop_index)
            if status == "completed":
                logger.info(f"自定义演进 Loop {loop_index} 已完成，跳过")
                continue
            loops_to_run.append(loop_config)

        if not loops_to_run:
            logger.info(f"自定义演进任务 {task_id} 所有 Loop 已完成")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()
            return

        if mode == "serial":
            for loop_config in loops_to_run:
                loop_index = loop_config.get("loop_index")
                loop_id = await self.submit_custom_evo_loop(task_id, loop_index, force_full_train=force_full_train)
                if not loop_id:
                    logger.error(f"自定义演进 Loop {loop_index} 提交失败，停止后续 Loops")
                    break

                # 等待完成（超时 4 小时，因为包含训练）
                max_wait = 14400
                waited = 0
                interval = 15
                while waited < max_wait:
                    await asyncio.sleep(interval)
                    waited += interval
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                            row = cur.fetchone()
                    if not row or row[0] in ("completed", "failed", "cancelled"):
                        break

                if waited >= max_wait:
                    logger.error(f"自定义演进 Loop {loop_index} 等待超时（{max_wait}s），标记为 failed")
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s AND status = 'running'",
                                (loop_id,),
                            )
                        conn.commit()
                    continue

                if loop_id:
                    await self._safe_process_completed_loop(task_id, loop_id)
        else:
            sem = asyncio.Semaphore(parallelism)
            logger.info(f"自定义演进 {task_id} 并行模式启动，并行度={parallelism}，共 {len(loops_to_run)} 个 Loop")

            async def run_with_sem(loop_config):
                loop_index = loop_config.get("loop_index")
                async with sem:
                    loop_id = await self.submit_custom_evo_loop(task_id, loop_index, force_full_train=force_full_train)
                    if not loop_id:
                        return None
                    max_wait = 14400
                    waited = 0
                    interval = 15
                    while waited < max_wait:
                        await asyncio.sleep(interval)
                        waited += interval
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                                row = cur.fetchone()
                        if not row or row[0] in ("completed", "failed", "cancelled"):
                            break
                    if waited >= max_wait:
                        logger.error(f"并行模式 Loop {loop_index} 等待超时（{max_wait}s）")
                    if loop_id:
                        await self._safe_process_completed_loop(task_id, loop_id)
                    return loop_id

            tasks = [run_with_sem(lc) for lc in loops_to_run]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"并行模式 Loop {loops_to_run[i].get('loop_index', i+1)} 提交失败: {result}")

        # ── 所有 loop 调度结束，确保 task 有最终状态 ──
        # process_strategy_evo_completed_loop 内部会在 loop_index >= max_loops 时标记 completed，
        # 但串行 break 或并行 loop 全部失败时可能漏掉，这里做兜底。
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT status FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                final_task = cur.fetchone()
        if final_task and final_task["status"] == "running":
            # 检查是否还有 running 的 loop
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status = 'running'",
                        (task_id,),
                    )
                    running_count = cur.fetchone()[0]
            if running_count == 0:
                # 没有正在跑的 loop 了，检查结果
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status = 'completed'",
                            (task_id,),
                        )
                        completed_count = cur.fetchone()[0]
                        cur.execute(
                            "SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status = 'failed'",
                            (task_id,),
                        )
                        failed_count = cur.fetchone()[0]
                final_status = "completed" if completed_count > 0 else "failed"
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE qe_evolution_tasks SET status = %s, updated_at = NOW() WHERE task_id = %s AND status = 'running'",
                            (final_status, task_id),
                        )
                    conn.commit()
                logger.info(f"自定义演进任务 {task_id} 最终状态: {final_status} (completed={completed_count}, failed={failed_count})")


    # ── Multi-Alpha Phase 3: helper methods ──────────────────────────────

    def _detect_alpha_mode(self, task: dict) -> str:
        """Detect alpha_mode from task or base experiment records.

        DB 错误不静默吞噬 — 记录日志并返回 single（安全降级，因为
        单 Alpha 路径不会破坏数据，而错误检测为 multi 反而会导致
        MultiAlphaEngine 因缺少 config 而崩溃）。
        """
        base_exp_id = task.get("base_experiment_id")
        if base_exp_id:
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT alpha_mode FROM qe_experiments WHERE experiment_id = %s",
                            (base_exp_id,),
                        )
                        row = cur.fetchone()
                        if row and row[0] and row[0] != "single":
                            return row[0]
            except Exception as e:
                logger.error(
                    "检测 alpha_mode 失败 (base_exp_id=%s): %s — 降级为 single",
                    base_exp_id, e,
                )
        return "single"

    async def _submit_multi_alpha_loop(
        self,
        task: dict,
        task_id: str,
        loop_index: int,
        evolution_loop_db_id: str,
    ):
        """Submit a Multi-Alpha evolution loop.

        Phase 3: generates sub-experiments for each alpha group, stores results.
        Actual Qlib execution dispatch is handled by the existing node infrastructure.
        """
        logger.info(f"Multi-Alpha loop {loop_index} for task {task_id}")

        # Create LOOP record
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO qe_evolution_loops "
                    "(loop_id, task_id, loop_index, status) "
                    "VALUES (%s, %s, %s, 'running') "
                    "ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()",
                    (evolution_loop_db_id, task_id, loop_index),
                )
            conn.commit()

        try:
            base_exp_id = task.get("base_experiment_id")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT multi_alpha_config, factor_names, model_id, strategy_id, "
                        "data_split, custom_params, strategy_params "
                        "FROM qe_experiments WHERE experiment_id = %s",
                        (base_exp_id,),
                    )
                    exp_row = cur.fetchone()

            if not exp_row or not exp_row[0]:
                raise ValueError(f"Base experiment {base_exp_id} has no multi_alpha_config")

            from .experiment_config_builders import build_config_from_multi_alpha

            multi_alpha_raw = exp_row[0]
            if isinstance(multi_alpha_raw, str):
                multi_alpha_raw = json.loads(multi_alpha_raw)

            data_split = exp_row[4]
            if isinstance(data_split, str):
                data_split = json.loads(data_split)
            strat_params = exp_row[6]
            if isinstance(strat_params, str):
                strat_params = json.loads(strat_params)

            cfg = build_config_from_multi_alpha(
                multi_alpha_config=multi_alpha_raw,
                data_split=data_split,
                strategy_id=exp_row[3],
                strategy_params=strat_params,
                node_id=task.get("node_id"),
                experiment_name=f"{task_id}_Loop{loop_index}",
            )

            from .multi_alpha_engine import MultiAlphaEngine

            engine = MultiAlphaEngine(cfg)
            result = engine.run()

            config_json = {
                "alpha_mode": "multi",
                "multi_alpha_config": (
                    cfg.multi_alpha_config.model_dump() if cfg.multi_alpha_config else None
                ),
                "group_configs": result.get("group_configs"),
                "meta_method": result.get("meta_method"),
            }
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops "
                        "SET status = 'completed', config_json = %s, updated_at = NOW() "
                        "WHERE loop_id = %s",
                        (json.dumps(config_json, default=str), evolution_loop_db_id),
                    )
                    cur.execute(
                        "UPDATE qe_evolution_tasks "
                        "SET current_loop = %s, updated_at = NOW() "
                        "WHERE task_id = %s",
                        (loop_index, task_id),
                    )
                conn.commit()

            logger.info(
                f"Multi-Alpha loop {loop_index} completed: "
                f"{result['total_groups']} groups"
            )
            return evolution_loop_db_id

        except Exception as e:
            logger.error(
                f"Multi-Alpha loop {loop_index} failed: {e}", exc_info=True
            )
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops "
                        "SET status = 'failed', error_message = %s, updated_at = NOW() "
                        "WHERE loop_id = %s",
                        (str(e)[:2000], evolution_loop_db_id),
                    )
                conn.commit()
            return None
