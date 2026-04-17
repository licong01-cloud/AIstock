"""
QE Unified Engine — MetricsStore

统一的指标存储接口，替代 process_completed_loop 和
process_strategy_evo_completed_loop 中 3 套不同的 INSERT/UPDATE 逻辑。
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("aistock.quantevolver.analysis.metrics_store")


class MetricsStore:
    """统一的指标存储接口。

    所有方法使用同一个 DB 连接上下文，调用方负责事务管理。
    设计为无状态，方便测试时注入 mock cursor。
    """

    def save_loop_metrics(
        self,
        loop_id: str,
        metrics: dict[str, Any],
        experiment_id: str | None = None,
        status: str = "completed",
        agent_analysis: dict[str, Any] | None = None,
        is_sota: bool = False,
        action_type: str | None = None,
        config_json: dict[str, Any] | None = None,
    ) -> None:
        """更新 qe_evolution_loops 记录。

        对齐 process_completed_loop:1499-1513 和
        process_strategy_evo_completed_loop:3168-3173 的 UPDATE 逻辑。
        """
        from ....db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                if agent_analysis is not None:
                    # 完整更新（自动演进路径）
                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET action_type = %s, config_json = %s, metrics_json = %s,
                            agent_analysis = %s, is_sota = %s, status = %s,
                            experiment_id = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (
                        action_type,
                        json.dumps(config_json) if config_json is not None else None,
                        json.dumps(metrics),
                        json.dumps(agent_analysis),
                        is_sota,
                        status,
                        experiment_id,
                        loop_id,
                    ))
                else:
                    # 简化更新（策略演进/自定义演进路径）
                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET metrics_json = %s, status = %s,
                            experiment_id = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(metrics), status, experiment_id, loop_id))
            conn.commit()

    def save_experiment_record(
        self,
        task_id: str,
        loop_id: str,
        loop_index: int,
        config: "ExperimentConfig",  # noqa: F821
        metrics: dict[str, Any],
        is_sota: bool = False,
        experiment_name_suffix: str | None = None,
    ) -> str:
        """插入或更新 qe_experiments 记录，返回 experiment_id。

        对齐 process_completed_loop:1468-1497 和
        process_strategy_evo_completed_loop:3145-3166 的 INSERT 逻辑。
        """
        from ....db.pg_pool import get_conn
        experiment_id = f"{task_id}_L{loop_index}"
        rdagent_loop_id = f"Loop{loop_index}"
        name_suffix = experiment_name_suffix or f"Loop{loop_index}"
        experiment_name = f"{task_id} {name_suffix}"

        # 序列化 multi_alpha_config（如果存在）
        multi_alpha_json = None
        if config.multi_alpha_config is not None:
            try:
                multi_alpha_json = json.dumps(
                    config.multi_alpha_config.model_dump()
                    if hasattr(config.multi_alpha_config, "model_dump")
                    else config.multi_alpha_config
                )
            except Exception as e:
                logger.warning("序列化 multi_alpha_config 失败: %s", e)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                    (experiment_id, experiment_name, qe_task_id, qe_loop_id,
                     loop_index, parent_experiment_id,
                     is_evolution_loop, factor_names, model_id, strategy_id,
                     data_split, custom_params,
                     result_metrics, status, is_sota,
                     alpha_mode, multi_alpha_config)
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s, 'completed', %s,
                            %s, %s::jsonb)
                    ON CONFLICT (experiment_id) DO UPDATE SET
                        result_metrics = EXCLUDED.result_metrics,
                        status = EXCLUDED.status,
                        is_sota = EXCLUDED.is_sota,
                        qe_task_id = EXCLUDED.qe_task_id,
                        qe_loop_id = EXCLUDED.qe_loop_id,
                        alpha_mode = EXCLUDED.alpha_mode,
                        multi_alpha_config = EXCLUDED.multi_alpha_config
                """, (
                    experiment_id,
                    experiment_name,
                    task_id,
                    rdagent_loop_id,
                    loop_index,
                    task_id,
                    json.dumps(config.factor_names),
                    config.model_id,
                    config.strategy_id,
                    json.dumps(config.data_split or {}),
                    json.dumps(config.build_custom_params()),
                    json.dumps(metrics),
                    is_sota,
                    config.alpha_mode or "single",
                    multi_alpha_json,
                ))
            conn.commit()

        return experiment_id

    def save_sota_registry(self, loop_id: str, reason: str = "") -> None:
        """写入 qe_sota_registry。对齐 process_completed_loop:1515-1519。"""
        from ....db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_sota_registry (loop_id, evaluation_reason)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (loop_id, reason))
            conn.commit()

    def update_task_progress(
        self,
        task_id: str,
        loop_index: int,
        max_loops: int,
    ) -> None:
        """更新 qe_evolution_tasks.current_loop，必要时标记 completed。

        对齐 process_strategy_evo_completed_loop:3176-3196。
        """
        from ....db.pg_pool import get_conn
        import psycopg2.extras
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT current_loop FROM qe_evolution_tasks WHERE task_id = %s",
                    (task_id,),
                )
                row = cur.fetchone()
            current_loop = row.get("current_loop", 0) if row else 0

        if loop_index > current_loop:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET current_loop = %s, updated_at = NOW() WHERE task_id = %s",
                        (loop_index, task_id),
                    )
                conn.commit()

        if loop_index >= max_loops:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s",
                        (task_id,),
                    )
                conn.commit()
