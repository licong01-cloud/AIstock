import json
import logging
import asyncio
from uuid import uuid4
from typing import Dict, Any, List, Optional
from pathlib import Path
from psycopg2.extras import RealDictCursor
from ...db.pg_pool import get_conn

from .qe_rdagent_api_client import RdagentApiClient
from .qe_evolution_agents import EvolutionAgents

logger = logging.getLogger(__name__)

class AutoEvolutionScheduler:
    """
    自动演进任务的核心调度器与状态机
    负责在 AIstock 侧控制演进流程的流转 (LOOP)，并调用 Agent 和 RDAgent API。
    """
    def __init__(self):
        self.rdagent_client = RdagentApiClient()
        self.agents = EvolutionAgents()
        
    async def create_task(self, task_name: str, target_desc: str, max_loops: int, base_experiment_id: str) -> str:
        """
        创建演进任务并写入数据库
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT experiment_id FROM qe_experiments WHERE experiment_id = %s", (base_experiment_id,))
                if not cur.fetchone():
                    raise ValueError(f"base_experiment_id not found: {base_experiment_id}")

        task_id = f"Evo_{uuid4().hex[:8]}"
        
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_evolution_tasks 
                    (task_id, task_name, target_desc, max_loops, current_loop, status, base_experiment_id)
                    VALUES (%s, %s, %s, %s, 0, 'pending', %s)
                """, (task_id, task_name, target_desc, max_loops, base_experiment_id))
            conn.commit()
            
        logger.info(f"Created evolution task {task_id}: {task_name}")
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
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT factor_names, model_id, strategy_id, data_split, custom_params
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

        return {
            "action_type": "initial",
            "factor_list": factor_names,
            "model_id": row.get("model_id"),
            "strategy_id": row.get("strategy_id"),
            "data_split": data_split,
            "model_params": custom_params,
            "base_experiment_id": base_experiment_id,
        }
        
    async def start_task_loop(self, task_id: str):
        """
        异步后台执行状态机，驱动 LOOP 流转
        """
        logger.info(f"Starting evolution loop for task {task_id}")
        
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task = cur.fetchone()
                    
            if not task:
                logger.error(f"Task {task_id} not found")
                return
                
            if task['status'] not in ('pending', 'running'):
                logger.info(f"Task {task_id} is already in state {task['status']}, aborting start.")
                return

            # Mark as running
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()

            current_loop = task['current_loop']
            max_loops = task['max_loops']
            config: Dict[str, Any] = {}
            
            while current_loop < max_loops:
                # 检查任务是否被中止
                with get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT status FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                        curr_status = cur.fetchone()['status']
                        if curr_status != 'running':
                            logger.info(f"Task {task_id} stopped or paused. Exiting loop.")
                            break

                loop_id = f"{task_id}_L{current_loop}"
                logger.info(f"Executing Loop {current_loop} for task {task_id}")
                
                # 创建 LOOP 记录
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO qe_evolution_loops 
                            (loop_id, task_id, loop_index, status)
                            VALUES (%s, %s, %s, 'running')
                            ON CONFLICT (loop_id) DO NOTHING
                        """, (loop_id, task_id, current_loop))
                    conn.commit()

                # 1. 组装本轮配置 (如果是 Loop 0，基于 base_experiment；否则由 Researcher Agent 决定)
                # config is maintained across loops or initialized at loop 0
                if current_loop == 0:
                    config = self._load_base_config_from_experiment(task["base_experiment_id"])
                
                action_type = config.get("action_type", "initial" if current_loop == 0 else "param_tune")

                # 使用 ConfigComposer 生成文件
                from .config_composer import ConfigComposer
                composer = ConfigComposer()
                compose_res = composer.compose_experiment(
                    factor_names=config.get("factor_list", []),
                    model_id=config.get("model_id"),
                    strategy_id=config.get("strategy_id"),
                    data_split=config.get("data_split"),
                    custom_params=config.get("model_params"),
                    experiment_name=loop_id
                )
                
                # 收集生成的实验文件，准备发送给 RDAgent
                experiment_files = {}
                exp_dir = Path(compose_res["experiment_dir"])
                allowed_suffixes = {".yaml", ".yml", ".py", ".txt", ".json"}
                for f in exp_dir.rglob("*"):
                    if f.is_file() and f.suffix in allowed_suffixes:
                        rel_path = f.relative_to(exp_dir).as_posix()
                        experiment_files[rel_path] = f.read_text(encoding="utf-8")
                        
                wsl_command = compose_res.get("wsl_command", "")

                # 2. 调用 WSL 执行
                try:
                    rd_loop_id = await self.rdagent_client.create_and_run_loop(
                        task_id, current_loop, config, experiment_files, wsl_command
                    )
                    
                    # 轮询等待执行完成（真实状态，不使用本地模拟）
                    while True:
                        status_resp = await self.rdagent_client.get_loop_status(rd_loop_id)
                        rd_status = status_resp.get("status")
                        if rd_status == "completed":
                            break
                        if rd_status in ("failed", "error"):
                            raise RuntimeError(f"RDAgent loop failed: {rd_loop_id}, status={rd_status}")
                        await asyncio.sleep(2)
                    
                    # 3. 获取回测结果
                    metrics = await self.rdagent_client.get_loop_metrics(rd_loop_id)
                    
                    # 4. Agent 分析
                    logger.info(f"Running Agent analysis for loop {current_loop}")
                    
                    # 构建历史分析上下文 (提取前3轮的 SOTA 状态和 Analyst 报告)
                    analysis_context = {}
                    with get_conn() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT loop_index, is_sota, agent_analysis 
                                FROM qe_evolution_loops 
                                WHERE task_id = %s AND status = 'completed'
                                ORDER BY loop_index DESC LIMIT 3
                            """, (task_id,))
                            history_rows = cur.fetchall()
                            if history_rows:
                                analysis_context["recent_history"] = []
                                for row in history_rows:
                                    analysis = row.get("agent_analysis")
                                    if isinstance(analysis, str):
                                        try:
                                            analysis = json.loads(analysis)
                                        except Exception:
                                            analysis = {}
                                    analysis_context["recent_history"].append({
                                        "loop": row["loop_index"],
                                        "is_sota": row["is_sota"],
                                        "analyst_report": analysis.get("analyst", "") if isinstance(analysis, dict) else ""
                                    })
                    
                    analyst_report = await self.agents.run_analyst(current_loop, config, metrics, analysis_context)
                    
                    historical_sota_metrics = None
                    with get_conn() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT l.metrics_json 
                                FROM qe_sota_registry r
                                JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                                WHERE l.task_id = %s
                                ORDER BY r.created_at DESC LIMIT 1
                            """, (task_id,))
                            sota_row = cur.fetchone()
                            if sota_row and sota_row['metrics_json']:
                                historical_sota_metrics = sota_row['metrics_json']
                    
                    is_sota = await self.agents.run_evaluator(metrics, historical_sota_metrics)
                    
                    next_config_draft = await self.agents.run_researcher(analyst_report, is_sota, config)
                    next_config = await self.agents.run_reviewer(next_config_draft)
                    
                    agent_analysis = {
                        "analyst": analyst_report,
                        "evaluator": f"SOTA Status: {is_sota}",
                        "researcher": "Draft generated",
                        "reviewer": "Config validated"
                    }
                    
                    # 更新 LOOP 记录
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE qe_evolution_loops 
                                SET action_type = %s, config_json = %s, metrics_json = %s, 
                                    agent_analysis = %s, is_sota = %s, status = 'completed', updated_at = NOW()
                                WHERE loop_id = %s
                            """, (
                                action_type, 
                                json.dumps(config), 
                                json.dumps(metrics), 
                                json.dumps(agent_analysis), 
                                is_sota, 
                                loop_id
                            ))
                            
                            if is_sota:
                                cur.execute("""
                                    INSERT INTO qe_sota_registry (loop_id, evaluation_reason)
                                    VALUES (%s, %s)
                                """, (loop_id, "Evaluator Agent marked as SOTA based on metrics."))
                                
                        conn.commit()

                    # 准备下一轮的 config
                    config = next_config
                    current_loop += 1
                    
                    # 更新总进度
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE qe_evolution_tasks SET current_loop = %s, updated_at = NOW() WHERE task_id = %s", (current_loop, task_id))
                        conn.commit()

                except Exception as e:
                    logger.error(f"Error executing loop {current_loop} for task {task_id}: {str(e)}")
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s", (loop_id,))
                            cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                        conn.commit()
                    break
            
            # 如果正常跑完 max_loops，标记完成
            if current_loop >= max_loops:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
                logger.info(f"Task {task_id} completed successfully.")

        except Exception as e:
            logger.error(f"Fatal error in task loop {task_id}: {str(e)}", exc_info=True)

    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks ORDER BY created_at DESC")
                return [dict(row) for row in cur.fetchall()]
        
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
                return result
        
    async def stop_task(self, task_id: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_evolution_tasks SET status = 'paused', updated_at = NOW() WHERE task_id = %s", (task_id,))
            conn.commit()
        logger.info(f"Task {task_id} manually stopped/paused.")
        
    async def stream_task_logs(self, task_id: str):
        """
        转发 RDAgent SSE 日志流
        """
        async for line in self.rdagent_client.stream_task_logs(task_id):
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
        
    async def sync_loop_assets(self, loop_id: str) -> str:
        """
        同步 RDAgent 侧的物理资产到本地
        """
        dest_dir = f"f:/Dev/AIstock/rdagent_assets/qe_sota_assets/{loop_id}"
        synced_path = await self.rdagent_client.download_loop_assets(loop_id, dest_dir)
        
        # 更新 DB SOTA registry 如果有记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_sota_registry 
                    SET model_assets_synced = TRUE, local_asset_path = %s 
                    WHERE loop_id = %s
                """, (synced_path, loop_id))
            conn.commit()
            
        return synced_path
