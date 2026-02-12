"""
RD-Agent 备选TASK和LOOP管理服务

功能：
1. 扫描RD-Agent log目录，发现新的TASK
2. 缓存TASK和LOOP信息到数据库
3. 检查目录存在性
4. 提供快速的数据查询接口
"""
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

from .rdagent_results_api_client import RDAgentResultsApiClient

# 加载环境变量
load_dotenv()

logger = logging.getLogger(__name__)


class RDAgentCandidateService:
    """RD-Agent备选TASK和LOOP管理服务"""
    
    def __init__(self):
        """初始化服务"""
        self.db_config = {
            'host': os.getenv('TDX_DB_HOST', '127.0.0.1'),
            'port': os.getenv('TDX_DB_PORT', '5432'),
            'database': os.getenv('TDX_DB_NAME', 'aistock'),
            'user': os.getenv('TDX_DB_USER', 'postgres'),
            'password': os.getenv('TDX_DB_PASSWORD', '')
        }
        
        # RD-Agent API客户端
        self.rdagent_client = RDAgentResultsApiClient()
        
        # RD-Agent log目录
        rdagent_root = os.getenv('QLIB_RDAGENT_ROOT_WIN', 'F:\\Dev\\RD-Agent-main')
        self.log_root = Path(rdagent_root) / 'log'
    
    def _get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_config)
    
    def scan_and_sync_tasks(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        扫描RD-Agent log目录，发现新TASK并同步到数据库
        
        Returns:
            {
                "total_dirs": int,  # 扫描到的目录总数
                "new_tasks": int,  # 新发现的TASK数量
                "updated_tasks": int,  # 更新的TASK数量
                "deleted_tasks": int,  # 标记为删除的TASK数量
            }
        """
        logger.info(f"开始扫描RD-Agent log目录: {self.log_root}")
        
        if not self.log_root.exists():
            logger.error(f"Log目录不存在: {self.log_root}")
            return {"error": "log_dir_not_found", "total_dirs": 0, "new_tasks": 0, "updated_tasks": 0, "deleted_tasks": 0}
        
        # 1. 扫描所有TASK目录
        task_dirs = []
        for item in self.log_root.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # 排除特殊目录
                if item.name in ['__pycache__', 'scheduler_tasks']:
                    continue
                task_dirs.append({
                    'task_id': item.name,
                    'log_dir': str(item.resolve())
                })
        
        logger.info(f"扫描到 {len(task_dirs)} 个TASK目录")
        
        # 2. 获取数据库中已有的TASK
        conn = self._get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT task_id, dir_exists FROM rdagent.rdagent_candidate_tasks")
        existing_tasks = {row['task_id']: row for row in cur.fetchall()}
        
        # 3. 处理新TASK和更新已有TASK
        new_tasks = []
        updated_tasks = []
        current_task_ids = set()
        
        for task_dir in task_dirs[:limit] if limit else task_dirs:
            task_id = task_dir['task_id']
            current_task_ids.add(task_id)
            
            if task_id not in existing_tasks:
                # 新TASK：获取详细信息并插入
                task_info = self._fetch_task_info(task_id, task_dir['log_dir'])
                if task_info:
                    new_tasks.append(task_info)
            else:
                # 已有TASK：检查目录是否存在
                if not existing_tasks[task_id]['dir_exists']:
                    # 目录重新出现，更新状态
                    cur.execute("""
                        UPDATE rdagent.rdagent_candidate_tasks
                        SET dir_exists = TRUE,
                            dir_checked_at = %s,
                            updated_at = %s
                        WHERE task_id = %s
                    """, (datetime.now(timezone.utc), datetime.now(timezone.utc), task_id))
                    updated_tasks.append(task_id)
        
        # 4. 批量插入新TASK
        if new_tasks:
            self._batch_insert_tasks(cur, new_tasks)
        
        # 5. 标记已删除的TASK
        deleted_task_ids = set(existing_tasks.keys()) - current_task_ids
        deleted_count = 0
        if deleted_task_ids:
            cur.execute("""
                UPDATE rdagent.rdagent_candidate_tasks
                SET dir_exists = FALSE,
                    dir_checked_at = %s,
                    updated_at = %s
                WHERE task_id = ANY(%s)
            """, (datetime.now(timezone.utc), datetime.now(timezone.utc), list(deleted_task_ids)))
            deleted_count = cur.rowcount
        
        conn.commit()
        cur.close()
        conn.close()
        
        result = {
            "total_dirs": len(task_dirs),
            "new_tasks": len(new_tasks),
            "updated_tasks": len(updated_tasks),
            "deleted_tasks": deleted_count
        }
        
        logger.info(f"扫描完成: {result}")
        return result
    
    def _fetch_task_info(self, task_id: str, log_dir: str) -> Optional[Dict[str, Any]]:
        """
        从RD-Agent V2对齐预览API获取TASK详细信息
        
        使用 /tasks/{task_id}/v2_alignment_preview API 获取完整对齐信息
        包括SOTA因子数、Alpha基线因子数、模型特征数、对齐状态
        
        Returns:
            {
                "task_id": str,
                "log_dir": str,
                "has_sota": bool,
                "sota_factors_count": int,
                "alpha_factors_count": int,
                "model_feature_count": int | None,
                "is_aligned": bool | None,
                "sota_factors_list": list | None,
                "alpha_factors_list": list | None,
                "hist_len": int,
                "task_status": str | None,
            }
        """
        try:
            import requests
            import json as _json
            base_url = self.rdagent_client.base_url.rstrip("/")
            
            has_sota = False
            sota_factors_count = 0
            alpha_factors_count = 0
            model_feature_count = None
            is_aligned = None
            sota_factors_list = None
            alpha_factors_list = None
            hist_len = 0
            task_status = None
            
            # 优先使用V2对齐预览API
            try:
                url = f"{base_url}/tasks/{task_id}/v2_alignment_preview"
                resp = requests.get(url, timeout=300.0)
                resp.raise_for_status()
                v2_data = resp.json()
                
                if v2_data and v2_data.get('ok'):
                    sota_factors_count = v2_data.get('sota_factors_count', 0)
                    has_sota = sota_factors_count > 0
                    alpha_factors_count = v2_data.get('alpha_factors_count', 0)
                    model_feature_count = v2_data.get('model_feature_count')
                    is_aligned = v2_data.get('is_aligned', False)
                    hist_len = v2_data.get('hist_len', 0)
                    sota_factors_list = v2_data.get('sota_factors')
                    alpha_factors_list = v2_data.get('alpha_factors')
                    logger.info(f"TASK {task_id} V2预览: SOTA={sota_factors_count}, "
                                f"Alpha={alpha_factors_count}, "
                                f"模型特征={model_feature_count}, "
                                f"对齐={is_aligned}")
                elif v2_data and v2_data.get('error'):
                    err_msg = v2_data.get('error', '')
                    if 'no_sota' in err_msg or 'no_accepted' in err_msg:
                        has_sota = False
                        sota_factors_count = 0
                        logger.info(f"TASK {task_id} V2确认无SOTA因子: {err_msg}")
                    elif 'session_not_found' in err_msg:
                        logger.debug(f"TASK {task_id} 无session: {err_msg}")
                    else:
                        logger.warning(f"TASK {task_id} V2预览返回错误: {err_msg}")
            except requests.exceptions.Timeout:
                logger.warning(f"TASK {task_id} V2对齐预览API超时(300s)")
            except Exception as e:
                logger.warning(f"TASK {task_id} V2对齐预览API失败: {e}")
            
            return {
                "task_id": task_id,
                "log_dir": log_dir,
                "has_sota": has_sota,
                "sota_factors_count": sota_factors_count,
                "alpha_factors_count": alpha_factors_count,
                "model_feature_count": model_feature_count,
                "is_aligned": is_aligned,
                "sota_factors_list": sota_factors_list,
                "alpha_factors_list": alpha_factors_list,
                "sota_checked_at": datetime.now(timezone.utc),
                "v2_checked_at": datetime.now(timezone.utc),
                "hist_len": hist_len,
                "task_status": task_status,
            }
            
        except Exception as e:
            logger.error(f"获取TASK {task_id} 信息失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _ensure_task_exists(self, task_id: str):
        """
        确保TASK存在于数据库中，如果不存在则创建
        
        这是关键修复：在获取LOOP前必须先确保TASK已入库，避免外键约束错误
        """
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        # 检查TASK是否已存在
        cur.execute("SELECT 1 FROM rdagent.rdagent_candidate_tasks WHERE task_id = %s", (task_id,))
        if cur.fetchone():
            cur.close()
            conn.close()
            return  # 已存在，直接返回
        
        # TASK不存在，需要创建
        logger.info(f"TASK {task_id} 不存在于数据库，正在创建...")
        
        # 构造log目录路径
        log_root = os.getenv('QLIB_RDAGENT_ROOT_WIN', 'F:/Dev/RD-Agent-main')
        log_dir = f"{log_root}/log/{task_id}"
        
        # 检查目录是否存在
        dir_exists = os.path.isdir(log_dir)
        
        # 尝试获取TASK信息
        task_info = self._fetch_task_info(task_id, log_dir)
        
        if task_info:
            # 插入完整信息
            cur.execute("""
                INSERT INTO rdagent.rdagent_candidate_tasks 
                (task_id, log_dir, has_sota, sota_factors_count, sota_checked_at, 
                 hist_len, task_status, dir_exists)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO NOTHING
            """, (
                task_info['task_id'],
                task_info['log_dir'],
                task_info['has_sota'],
                task_info['sota_factors_count'],
                task_info['sota_checked_at'],
                task_info['hist_len'],
                task_info['task_status'],
                dir_exists
            ))
            logger.info(f"TASK {task_id} 已创建（has_sota={task_info['has_sota']}）")
        else:
            # 插入基本信息
            cur.execute("""
                INSERT INTO rdagent.rdagent_candidate_tasks 
                (task_id, log_dir, dir_exists)
                VALUES (%s, %s, %s)
                ON CONFLICT (task_id) DO NOTHING
            """, (task_id, log_dir, dir_exists))
            logger.warning(f"TASK {task_id} 已创建（仅基本信息）")
        
        conn.commit()
        cur.close()
        conn.close()
    
    def _batch_insert_tasks(self, cur, tasks: List[Dict[str, Any]]):
        """批量插入TASK到数据库（含V2对齐信息）"""
        if not tasks:
            return
        
        import json as _json
        
        insert_sql = """
            INSERT INTO rdagent.rdagent_candidate_tasks 
            (task_id, log_dir, has_sota, sota_factors_count, sota_checked_at, 
             hist_len, task_status, dir_exists, dir_checked_at,
             alpha_factors_count, model_feature_count, is_aligned, v2_checked_at,
             sota_factors_list, alpha_factors_list)
            VALUES %s
            ON CONFLICT (task_id) DO UPDATE SET
                has_sota = EXCLUDED.has_sota,
                sota_factors_count = EXCLUDED.sota_factors_count,
                sota_checked_at = EXCLUDED.sota_checked_at,
                hist_len = EXCLUDED.hist_len,
                task_status = EXCLUDED.task_status,
                dir_exists = EXCLUDED.dir_exists,
                dir_checked_at = EXCLUDED.dir_checked_at,
                alpha_factors_count = EXCLUDED.alpha_factors_count,
                model_feature_count = EXCLUDED.model_feature_count,
                is_aligned = EXCLUDED.is_aligned,
                v2_checked_at = EXCLUDED.v2_checked_at,
                sota_factors_list = EXCLUDED.sota_factors_list,
                alpha_factors_list = EXCLUDED.alpha_factors_list,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = [
            (
                task['task_id'],
                task['log_dir'],
                task.get('has_sota'),
                task.get('sota_factors_count', 0),
                task.get('sota_checked_at'),
                task.get('hist_len', 0),
                task.get('task_status'),
                True,  # dir_exists
                datetime.now(timezone.utc),  # dir_checked_at
                task.get('alpha_factors_count', 0),
                task.get('model_feature_count'),
                task.get('is_aligned'),
                task.get('v2_checked_at'),
                _json.dumps(task.get('sota_factors_list')) if task.get('sota_factors_list') else None,
                _json.dumps(task.get('alpha_factors_list')) if task.get('alpha_factors_list') else None,
            )
            for task in tasks
        ]
        
        execute_values(cur, insert_sql, values)
        logger.info(f"批量插入 {len(tasks)} 个TASK")
    
    def get_candidate_tasks(self, limit: Optional[int] = None, include_deleted: bool = False) -> List[Dict[str, Any]]:
        """
        获取备选TASK列表
        
        Args:
            limit: 限制返回数量
            include_deleted: 是否包含已删除的TASK
            
        Returns:
            TASK列表
        """
        conn = self._get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        sql = """
            SELECT 
                task_id, log_dir, has_sota, sota_factors_count, sota_checked_at,
                hist_len, task_status, is_synced, sync_status, synced_at,
                dir_exists, dir_checked_at, discovered_at, updated_at,
                alpha_factors_count, model_feature_count, is_aligned, v2_checked_at,
                sota_factors_list, alpha_factors_list
            FROM rdagent.rdagent_candidate_tasks
        """
        
        if not include_deleted:
            sql += " WHERE dir_exists = TRUE"
        
        sql += " ORDER BY discovered_at DESC"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        cur.execute(sql)
        tasks = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # 转换为字典列表
        return [dict(task) for task in tasks]
    
    def cache_task_loops(self, task_id: str, loops_data: List[Dict[str, Any]]) -> int:
        """
        缓存TASK的LOOP详情到数据库
        
        Args:
            task_id: 任务ID
            loops_data: LOOP数据列表
            
        Returns:
            插入的LOOP数量
        """
        if not loops_data:
            return 0
        
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        insert_sql = """
            INSERT INTO rdagent.rdagent_candidate_loops 
            (task_id, loop_id, exp_type, hypothesis, reason, valid_score, test_score, 
             annualized_return, max_drawdown, information_ratio, is_sota, feedback,
             tested_count, total_count)
            VALUES %s
            ON CONFLICT (task_id, loop_id) DO UPDATE SET
                exp_type = EXCLUDED.exp_type,
                hypothesis = EXCLUDED.hypothesis,
                reason = EXCLUDED.reason,
                valid_score = EXCLUDED.valid_score,
                test_score = EXCLUDED.test_score,
                annualized_return = EXCLUDED.annualized_return,
                max_drawdown = EXCLUDED.max_drawdown,
                information_ratio = EXCLUDED.information_ratio,
                is_sota = EXCLUDED.is_sota,
                feedback = EXCLUDED.feedback,
                tested_count = EXCLUDED.tested_count,
                total_count = EXCLUDED.total_count,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = [
            (
                task_id,
                loop['loop_id'],
                loop.get('exp_type'),
                loop.get('hypothesis'),
                loop.get('reason'),
                loop.get('valid_score'),
                loop.get('test_score'),
                loop.get('annualized_return'),
                loop.get('max_drawdown'),
                loop.get('information_ratio'),
                loop.get('is_sota', False),
                loop.get('feedback'),
                loop.get('tested_count'),
                loop.get('total_count')
            )
            for loop in loops_data
        ]
        
        logger.info(f"cache_task_loops: 插入 {len(values)} 条LOOP, 每条 {len(values[0]) if values else 0} 个字段, tested_count样本={values[0][-2] if values else 'N/A'}")
        execute_values(cur, insert_sql, values)
        inserted_count = cur.rowcount
        
        # 统计SOTA因子和模型数量
        # 注意：统计的是每个SOTA LOOP中包含的因子/模型数量总和，而不是LOOP数量
        sota_factor_loops = [l for l in loops_data if l.get('is_sota') and 'Factor' in l.get('exp_type', '')]
        sota_model_loops = [l for l in loops_data if l.get('is_sota') and 'Model' in l.get('exp_type', '')]
        
        # 计算SOTA因子总数：累加每个SOTA factor LOOP中的tested_factors数量
        total_sota_factors = sum(len(l.get('tested_factors', [])) for l in sota_factor_loops)
        # 计算SOTA模型总数：累加每个SOTA model LOOP中的tested_factors数量
        total_sota_models = sum(len(l.get('tested_factors', [])) for l in sota_model_loops)
        
        # 更新task表的SOTA统计
        # 如果已有V2对齐数据，不覆盖sota_factors_count和has_sota（V2数据更准确）
        # 只更新sota_models_count（V2不提供此字段）
        cur.execute("""
            UPDATE rdagent.rdagent_candidate_tasks
            SET sota_models_count = %s,
                sota_factors_count = CASE WHEN v2_checked_at IS NULL THEN %s ELSE sota_factors_count END,
                has_sota = CASE WHEN v2_checked_at IS NULL THEN %s ELSE has_sota END,
                sota_checked_at = CASE WHEN v2_checked_at IS NULL THEN %s ELSE sota_checked_at END,
                updated_at = CURRENT_TIMESTAMP
            WHERE task_id = %s
        """, (
            total_sota_models,
            total_sota_factors,
            total_sota_factors > 0 or total_sota_models > 0,
            datetime.now(timezone.utc),
            task_id
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"缓存TASK {task_id} 的 {inserted_count} 个LOOP (SOTA: {len(sota_factor_loops)} factors, {len(sota_model_loops)} models)")
        return inserted_count
    
    def get_task_loops(self, task_id: str, force_refresh: bool = False) -> Tuple[List[Dict[str, Any]], bool]:
        """
        获取TASK的LOOP详情（优先从缓存读取）
        
        Args:
            task_id: 任务ID
            force_refresh: 是否强制刷新（重新从API获取）
            
        Returns:
            (LOOP列表, 是否来自缓存)
        """
        # 0. 先确保TASK已入库（关键修复！）
        self._ensure_task_exists(task_id)
        
        # 1. 尝试从数据库读取
        if not force_refresh:
            conn = self._get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT 
                    loop_id, exp_type, hypothesis, reason, valid_score, test_score,
                    annualized_return, max_drawdown, information_ratio, is_sota, feedback,
                    tested_count, total_count,
                    created_at, updated_at
                FROM rdagent.rdagent_candidate_loops
                WHERE task_id = %s
                ORDER BY loop_id
            """, (task_id,))
            
            loops = cur.fetchall()
            cur.close()
            conn.close()
            
            if loops:
                logger.info(f"从缓存读取TASK {task_id} 的 {len(loops)} 个LOOP")
                result = []
                for loop in loops:
                    d = dict(loop)
                    # feedback字段在数据库中为text类型，需要转换为布尔值
                    fb = d.get('feedback')
                    if isinstance(fb, str):
                        d['feedback'] = fb.lower() == 'true'
                    result.append(d)
                return result, True
        
        # 2. 从RD-Agent API获取
        try:
            import requests
            base_url = self.rdagent_client.base_url.rstrip("/")
            url = f"{base_url}/tasks/{task_id}/loops"
            
            resp = requests.get(url, timeout=300.0)
            resp.raise_for_status()
            api_resp = resp.json()
            
            if not api_resp.get('ok'):
                logger.error(f"获取TASK {task_id} LOOP失败: {api_resp.get('error')}")
                return [], False
            
            loops_data = api_resp.get('loops', [])
            
            # 3. 缓存到数据库
            if loops_data:
                try:
                    self.cache_task_loops(task_id, loops_data)
                except Exception as cache_err:
                    logger.error(f"缓存TASK {task_id} LOOP数据失败: {cache_err}", exc_info=True)
            
            logger.info(f"从API获取TASK {task_id} 的 {len(loops_data)} 个LOOP")
            return loops_data, False
            
        except Exception as e:
            logger.error(f"获取TASK {task_id} LOOP失败: {e}")
            return [], False


# 全局服务实例
_candidate_service: Optional[RDAgentCandidateService] = None


def get_candidate_service() -> RDAgentCandidateService:
    """获取全局服务实例"""
    global _candidate_service
    if _candidate_service is None:
        _candidate_service = RDAgentCandidateService()
    return _candidate_service
