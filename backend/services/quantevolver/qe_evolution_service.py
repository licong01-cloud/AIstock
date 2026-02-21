import logging
import asyncio
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from uuid import uuid4

from psycopg2.extras import RealDictCursor
from ...db.pg_pool import get_conn

from .qe_rdagent_api_client import RdagentApiClient

logger = logging.getLogger(__name__)

class AutoEvolutionScheduler:
    """
    自动演进任务的核心调度器与状态机
    负责在 AIstock 侧控制演进流程的流转 (LOOP)，并调用 Agent 和 RDAgent API。
    """
    def __init__(self):
        self.rdagent_client = RdagentApiClient()
        
    async def create_task(self, task_name: str, target_desc: str, max_loops: int, base_experiment_id: str) -> str:
        """
        创建演进任务并写入数据库
        """
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
                config = {} # Mock config
                action_type = "initial" if current_loop == 0 else "factor_adjust"

                # 2. 调用 WSL 执行
                try:
                    rd_loop_id = await self.rdagent_client.create_and_run_loop(task_id, current_loop, config)
                    
                    # 模拟等待执行完成
                    await asyncio.sleep(2)
                    
                    # 3. 获取回测结果
                    metrics = await self.rdagent_client.get_loop_metrics(rd_loop_id)
                    
                    # 4. Agent 分析 (Mock)
                    agent_analysis = {
                        "analyst": "各项指标正常",
                        "evaluator": "未超越历史 SOTA",
                        "researcher": "建议下轮增加动量因子"
                    }
                    is_sota = False
                    
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
                        conn.commit()

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
                result['loops'] = [dict(l) for l in loops]
                return result
        
    async def stop_task(self, task_id: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_evolution_tasks SET status = 'paused', updated_at = NOW() WHERE task_id = %s", (task_id,))
            conn.commit()
        logger.info(f"Task {task_id} manually stopped/paused.")
        
    async def stream_task_logs(self, task_id: str):
        """
        模拟生成日志流
        """
        for i in range(10):
            yield f"data: Log line {i} from RDAgent for task {task_id}\n\n"
            await asyncio.sleep(1)

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
