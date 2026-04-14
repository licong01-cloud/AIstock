"""
RD-Agent 备选TASK和LOOP管理服务

功能：
1. 通过各计算节点 API 发现新的TASK（多节点架构）
2. 缓存TASK和LOOP信息到数据库
3. 提供快速的数据查询接口
"""
import os
import logging
import threading
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

from .rdagent_results_api_client import RDAgentResultsApiClient

# 加载环境变量
load_dotenv()

logger = logging.getLogger(__name__)

# 防止多线程并发对同一 task 发起 V2 预览请求
_task_fetch_lock = threading.Lock()
_task_fetching: set[str] = set()


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

        # RD-Agent API客户端（默认节点，用于 get_task_loops 等）
        self.rdagent_client = RDAgentResultsApiClient()
    
    def _get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_config)
    
    def _get_all_node_clients(self) -> list[tuple[str, RDAgentResultsApiClient]]:
        """从 infra.compute_nodes 获取所有节点的 API 客户端。"""
        clients: list[tuple[str, RDAgentResultsApiClient]] = []
        conn = None
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT node_id, api_base_url FROM infra.compute_nodes")
            for row in cur.fetchall():
                try:
                    clients.append((row[0], RDAgentResultsApiClient(base_url=row[1])))
                except Exception:
                    logger.warning(f"创建节点 {row[0]} 客户端失败，跳过")
            cur.close()
        except Exception as e:
            logger.error(f"查询 compute_nodes 失败: {e}")
        finally:
            if conn is not None:
                conn.close()
        # fallback: 至少使用默认客户端
        if not clients:
            clients = [("default", self.rdagent_client)]
        return clients

    def _get_client_for_task(self, task_id: str, *, strict: bool = False) -> RDAgentResultsApiClient:
        """根据 task_id 从 DB 的 log_dir 字段解析 node_id，返回对应节点的 API 客户端。

        log_dir 格式: "node_id:task_id"（scan_and_sync_tasks 写入）。

        strict=False（默认）: DB 无记录/log_dir 为空时返回默认客户端（兼容旧数据、读操作）。
        strict=True: DB 无记录/log_dir 为空时直接 raise（用于删除等破坏性操作，防止误删）。

        log_dir 有值但无 ":" 前缀 → 单节点时代数据，返回默认客户端（两种模式均如此）。
        DB 连接失败或 for_node() 节点不存在直接抛异常，不静默降级。
        """
        conn = self._get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT log_dir FROM rdagent.rdagent_candidate_tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            cur.close()
        finally:
            conn.close()

        if not row or not row[0]:
            if strict:
                raise ValueError(f"Task {task_id} 在 DB 中无记录或 log_dir 为空，无法确定所属节点，拒绝执行破坏性操作")
            return self.rdagent_client

        log_dir = row[0]
        if ":" not in log_dir:
            # 旧格式，无节点前缀 → 单节点时代的 task，默认客户端即为正确节点
            return self.rdagent_client

        node_id = log_dir.split(":", 1)[0]
        # Windows 盘符 (如 "F:\...") → 单节点时代旧数据，回退到默认客户端
        if len(node_id) == 1 and node_id.isalpha():
            return self.rdagent_client
        return RDAgentResultsApiClient.for_node(node_id)

    def get_task_workspaces(self, task_id: str, *, quick: bool = True) -> dict:
        """通过自动路由获取 task 所属节点的 workspace 信息。"""
        client = self._get_client_for_task(task_id)
        return client.get_task_workspaces(task_id, quick=quick)

    def delete_task_on_node(self, task_id: str) -> dict:
        """通过自动路由删除 task 所属节点上的 log + workspace + scheduler log。

        使用 strict=True 确保必须能从 DB 明确解析节点归属，
        DB 无记录时直接 raise，防止 fallback 到默认节点误删。
        """
        client = self._get_client_for_task(task_id, strict=True)
        return client.delete_task_remote(task_id)

    def scan_and_sync_tasks(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        通过各计算节点 API 发现新TASK并同步到数据库（多节点架构）

        Returns:
            {
                "total_discovered": int,  # API发现的任务总数
                "new_tasks": int,         # 新发现的TASK数量
                "updated_tasks": int,     # 更新的TASK数量
                "deleted_tasks": int,     # 标记为不存在的TASK数量
                "nodes_queried": int,     # 查询的节点数
                "nodes_failed": int,      # 查询失败的节点数
            }
        """
        logger.info("开始通过 API 扫描各计算节点的TASK")

        # 1. 遍历所有计算节点，通过 API 获取任务列表
        node_clients = self._get_all_node_clients()
        # {task_id: {task_id, node_id, client}} — 保留 client 引用供后续 V2 调用
        discovered_tasks: dict[str, dict] = {}
        nodes_failed = 0
        failed_node_ids: set[str] = set()

        PAGE_SIZE = 200  # 每次请求的最大数量
        for node_id, client in node_clients:
            try:
                # 分页循环：持续拉取直到获取全部 task
                node_offset = 0
                node_total = None
                while True:
                    api_resp = client.get_tasks_latest(limit=PAGE_SIZE, offset=node_offset)
                    raw_items: list = []
                    if isinstance(api_resp, list):
                        raw_items = api_resp
                    elif isinstance(api_resp, dict):
                        # 首次请求时记录 total 总数
                        if node_total is None:
                            node_total = api_resp.get("total")
                        for k in ("items", "tasks", "data"):
                            if isinstance(api_resp.get(k), list):
                                raw_items = api_resp[k]
                                break

                    for t in raw_items:
                        tid = t.get("task_id")
                        if tid and tid not in discovered_tasks:
                            discovered_tasks[tid] = {
                                "task_id": tid,
                                "node_id": node_id,
                                "client": client,
                            }

                    # 本页无数据或已拉取全部 → 退出
                    if not raw_items:
                        break
                    node_offset += len(raw_items)
                    if node_total is not None and node_offset >= node_total:
                        break

                logger.info(f"节点 {node_id}: 发现 {len([t for t in discovered_tasks.values() if t['node_id'] == node_id])} 个TASK (分页拉取完成)")
            except Exception as e:
                nodes_failed += 1
                failed_node_ids.add(node_id)
                logger.warning(f"节点 {node_id} 查询失败（继续处理其他节点）: {e}")

        logger.info(f"API扫描完成: 查询 {len(node_clients)} 个节点, "
                     f"发现 {len(discovered_tasks)} 个TASK（去重后）, 失败 {nodes_failed} 个")

        # 2. 获取数据库中已有的TASK
        conn = self._get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute("SELECT task_id, dir_exists, log_dir FROM rdagent.rdagent_candidate_tasks")
            existing_tasks = {row['task_id']: row for row in cur.fetchall()}

            # 3. 处理新TASK和更新已有TASK
            new_tasks = []
            updated_tasks = []
            current_task_ids = set(discovered_tasks.keys())

            for tid, item in discovered_tasks.items():
                if tid not in existing_tasks:
                    # 快速入库：不阻塞等 V2 预览，先写入基本信息
                    new_tasks.append({
                        "task_id": tid,
                        "log_dir": f"{item['node_id']}:{tid}",
                        "node_id": item['node_id'],
                        "has_sota": None,
                        "sota_factors_count": None,
                        "task_status": "discovered",
                        "discovered_at": datetime.now(timezone.utc),
                    })
                else:
                    # 已有TASK：如果之前标记为不存在，恢复
                    if not existing_tasks[tid]['dir_exists']:
                        cur.execute("""
                            UPDATE rdagent.rdagent_candidate_tasks
                            SET dir_exists = TRUE,
                                dir_checked_at = %s,
                                updated_at = %s
                            WHERE task_id = %s
                        """, (datetime.now(timezone.utc), datetime.now(timezone.utc), tid))
                        updated_tasks.append(tid)

            # 4. 批量插入新TASK
            if new_tasks:
                self._batch_insert_tasks(cur, new_tasks)

            # 5. 标记不再出现的TASK为不存在
            # 关键：仅当所有节点都成功查询时才标记删除，
            # 否则不可达节点上的任务会被误删
            deleted_count = 0
            if nodes_failed == 0:
                deleted_task_ids = set(existing_tasks.keys()) - current_task_ids
                if deleted_task_ids:
                    cur.execute("""
                        UPDATE rdagent.rdagent_candidate_tasks
                        SET dir_exists = FALSE,
                            dir_checked_at = %s,
                            updated_at = %s
                        WHERE task_id = ANY(%s)
                    """, (datetime.now(timezone.utc), datetime.now(timezone.utc), list(deleted_task_ids)))
                    deleted_count = cur.rowcount
            elif failed_node_ids:
                logger.info(f"有 {nodes_failed} 个节点不可达 ({failed_node_ids})，跳过删除标记")

            conn.commit()
            cur.close()
        finally:
            conn.close()

        result = {
            "total_discovered": len(discovered_tasks),
            "new_tasks": len(new_tasks),
            "updated_tasks": len(updated_tasks),
            "deleted_tasks": deleted_count,
            "nodes_queried": len(node_clients),
            "nodes_failed": nodes_failed,
        }

        logger.info(f"扫描完成: {result}")

        # 后台异步补全新 task 的 V2 详情（不阻塞返回）
        if new_tasks:
            _uncached_tids = [t["task_id"] for t in new_tasks]
            import threading
            def _bg_backfill_v2(tids, node_map):
                """后台线程：逐个补全 V2 对齐信息"""
                try:
                    svc = get_candidate_service()
                    conn_bg = svc._get_db_connection()
                    try:
                        cur_bg = conn_bg.cursor()
                        for _tid in tids:
                            try:
                                _item = node_map.get(_tid)
                                _nid = _item["node_id"] if _item else "default"
                                _cli = _item["client"] if _item else None
                                _info = svc._fetch_task_info(_tid, node_id=_nid, client=_cli)
                                if _info:
                                    svc._batch_insert_tasks(cur_bg, [_info])
                                    conn_bg.commit()
                                    logger.info(f"V2补全: {_tid} done")
                            except Exception as e:
                                logger.warning(f"V2补全: {_tid} 失败: {e}")
                        cur_bg.close()
                    finally:
                        conn_bg.close()
                    logger.info(f"V2后台补全完成: {len(tids)} 个task")
                except Exception as e:
                    logger.error(f"V2后台补全异常: {e}")

            t = threading.Thread(target=_bg_backfill_v2,
                                 args=(_uncached_tids, discovered_tasks), daemon=True)
            t.start()
            logger.info(f"已启动后台V2补全线程: {len(_uncached_tids)} 个task")

        return result
    
    def _fetch_task_info(self, task_id: str, *, node_id: str = "default",
                         client: Optional[RDAgentResultsApiClient] = None) -> Optional[Dict[str, Any]]:
        """
        从RD-Agent V2对齐预览API获取TASK详细信息

        使用 /tasks/{task_id}/v2_alignment_preview API 获取完整对齐信息
        包括SOTA因子数、Alpha基线因子数、模型特征数、对齐状态

        Args:
            task_id: 任务ID
            node_id: 所属计算节点ID（存入 log_dir 字段以标识来源节点）
            client: 用于请求的 API 客户端（None 时使用默认客户端）
        """
        try:
            import requests
            _client = client or self.rdagent_client
            base_url = _client.base_url.rstrip("/")

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
                    elif 'no_3_feedback' in err_msg or 'no_feedback' in err_msg:
                        task_status = 'running'
                        logger.debug(f"TASK {task_id} 尚未产生足够反馈: {err_msg}")
                    else:
                        logger.warning(f"TASK {task_id} V2预览返回错误: {err_msg}")
            except requests.exceptions.HTTPError as e:
                # 404 = task 不存在于该节点，立即返回 None（不继续填充假数据）
                if resp.status_code == 404:
                    logger.debug(f"TASK {task_id} 在节点 {node_id} 不存在(404)")
                    return None
                logger.warning(f"TASK {task_id} V2预览API失败: {e}")
            except requests.exceptions.Timeout:
                logger.warning(f"TASK {task_id} V2对齐预览API超时(300s)")
            except Exception as e:
                logger.warning(f"TASK {task_id} V2对齐预览API失败: {e}")

            return {
                "task_id": task_id,
                "log_dir": f"{node_id}:{task_id}",
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
        确保TASK存在于数据库中，如果不存在则通过 API 创建。

        在获取LOOP前必须先确保TASK已入库，避免外键约束错误。
        使用模块级锁防止多线程并发对同一 task 发起重复 V2 预览请求。
        """
        conn = self._get_db_connection()
        try:
            cur = conn.cursor()

            # 检查TASK是否已存在
            cur.execute("SELECT 1 FROM rdagent.rdagent_candidate_tasks WHERE task_id = %s", (task_id,))
            if cur.fetchone():
                cur.close()
                return  # 已存在，直接返回

            # 并发去重：如果另一个线程正在 fetch 同一 task，跳过
            with _task_fetch_lock:
                if task_id in _task_fetching:
                    logger.debug(f"TASK {task_id} 正在被另一线程处理，跳过")
                    cur.close()
                    return
                _task_fetching.add(task_id)

            try:
                # TASK不存在，需要创建（纯 API，不访问本地文件系统）
                # 遍历所有节点尝试找到该 task（因为尚未入库，无法从 log_dir 推断节点）
                logger.info(f"TASK {task_id} 不存在于数据库，正在通过 API 创建...")

                task_info = None
                for _nid, _client in self._get_all_node_clients():
                    _info = self._fetch_task_info(task_id, node_id=_nid, client=_client)
                    if _info is not None:
                        # 节点确认 task 存在（非 None = V2 API 返回了数据）
                        task_info = _info
                        logger.info(f"TASK {task_id} 在节点 {_nid} 上找到")
                        break  # 找到即停，不继续遍历其他节点
                if task_info is None:
                    # 所有节点都返回 None（task 不存在于任何节点），用默认客户端兜底
                    task_info = self._fetch_task_info(task_id)

                if task_info:
                    import json as _json
                    cur.execute("""
                        INSERT INTO rdagent.rdagent_candidate_tasks
                        (task_id, log_dir, has_sota, sota_factors_count, sota_checked_at,
                         hist_len, task_status, dir_exists,
                         alpha_factors_count, model_feature_count, is_aligned, v2_checked_at,
                         sota_factors_list, alpha_factors_list)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (task_id) DO UPDATE SET
                            log_dir = EXCLUDED.log_dir,
                            has_sota = EXCLUDED.has_sota,
                            sota_factors_count = EXCLUDED.sota_factors_count,
                            sota_checked_at = EXCLUDED.sota_checked_at,
                            hist_len = EXCLUDED.hist_len,
                            task_status = EXCLUDED.task_status,
                            dir_exists = EXCLUDED.dir_exists,
                            alpha_factors_count = EXCLUDED.alpha_factors_count,
                            model_feature_count = EXCLUDED.model_feature_count,
                            is_aligned = EXCLUDED.is_aligned,
                            v2_checked_at = EXCLUDED.v2_checked_at,
                            sota_factors_list = EXCLUDED.sota_factors_list,
                            alpha_factors_list = EXCLUDED.alpha_factors_list,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        task_info['task_id'],
                        task_info['log_dir'],
                        task_info['has_sota'],
                        task_info['sota_factors_count'],
                        task_info['sota_checked_at'],
                        task_info['hist_len'],
                        task_info['task_status'],
                        True,  # API 能返回说明任务存在
                        task_info.get('alpha_factors_count', 0),
                        task_info.get('model_feature_count'),
                        task_info.get('is_aligned'),
                        task_info.get('v2_checked_at'),
                        _json.dumps(task_info.get('sota_factors_list')) if task_info.get('sota_factors_list') else None,
                        _json.dumps(task_info.get('alpha_factors_list')) if task_info.get('alpha_factors_list') else None,
                    ))
                    logger.info(f"TASK {task_id} 已创建（has_sota={task_info['has_sota']}, "
                                f"alpha={task_info.get('alpha_factors_count')}, "
                                f"aligned={task_info.get('is_aligned')}）")
                else:
                    cur.execute("""
                        INSERT INTO rdagent.rdagent_candidate_tasks
                        (task_id, log_dir, dir_exists)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (task_id) DO NOTHING
                    """, (task_id, task_id, True))
                    logger.warning(f"TASK {task_id} 已创建（仅基本信息）")

                conn.commit()
                cur.close()
            finally:
                with _task_fetch_lock:
                    _task_fetching.discard(task_id)
        finally:
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
                log_dir = EXCLUDED.log_dir,
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
        
        # 2. 从RD-Agent API获取（路由到正确的计算节点）
        try:
            import requests
            _client = self._get_client_for_task(task_id)
            base_url = _client.base_url.rstrip("/")
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
