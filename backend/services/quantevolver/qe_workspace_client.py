import logging
import os
import aiofiles
import zipfile
from typing import Dict, Any, Optional
import httpx

logger = logging.getLogger(__name__)

class QEWorkspaceClient:
    """
    专门负责与被物理隔离的 RDAgent 端进行网络交互的客户端
    封装了诸如触发任务、获取回测指标、获取日志流、下载模型资产等操作。
    """
    def __init__(self, base_url: str = "http://localhost:9000/api/v1/qe_workspace"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0))

    @staticmethod
    def _to_rdagent_loop_id(task_id: str, loop_id: str) -> str:
        """DB 中 loop_id 格式为 '{task_id}_{LoopN}'，RDAgent 文件系统期望 'LoopN'"""
        if loop_id.startswith(task_id + "_"):
            return loop_id[len(task_id) + 1:]
        return loop_id

    @classmethod
    def for_node(cls, node_id: str) -> "QEWorkspaceClient":
        """根据 node_id 从 compute_nodes 表获取 api_base_url 创建客户端。"""
        from ...db.pg_pool import get_conn
        from psycopg2.extras import RealDictCursor
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT api_base_url FROM infra.compute_nodes WHERE node_id = %s", (node_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"节点不存在: {node_id}")
                base = row["api_base_url"].rstrip("/")
                return cls(base_url=f"{base}/api/v1/qe_workspace")

    async def close(self):
        """显式关闭内部 httpx 客户端，释放连接池资源。"""
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def create_and_run_loop(
        self, task_id: str, loop_index: int, config: Dict[str, Any], experiment_files: Dict[str, str] = None, wsl_command: str = "",
        model_source: Dict[str, Any] = None,
        callback_url: str = None,
    ) -> str:
        """
        通知 RDAgent 根据配置生成代码并启动执行 QLib 回测
        返回 RDAgent 端生成的 loop_id

        model_source: 策略演进时传入模型来源信息，用于创建 mlruns 符号链接
            {
                "source_task_id": "qe_xxx",
                "source_loop": "Loop3",
            }
        callback_url: Loop 完成后回调 AIstock 的 URL（远端节点主动通知）
        """
        url = f"{self.base_url}/tasks/{task_id}/loops"
        payload = {
            "loop_index": loop_index,
            "config": config,
            "experiment_files": experiment_files or {},
            "wsl_command": wsl_command,
        }
        if model_source:
            payload["model_source"] = model_source
        if callback_url:
            payload["callback_url"] = callback_url
        
        try:
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            loop_id = data.get("loop_id")
            if not isinstance(loop_id, str) or not loop_id:
                raise ValueError(f"Invalid loop_id in response: {data}")
            return loop_id
        except httpx.HTTPError as e:
            logger.error(f"Failed to create loop {loop_index} for task {task_id}: {str(e)}")
            raise
        
    async def get_loop_status(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """
        查询 WSL 侧 QLib 任务执行的状态（双参数：task_id + loop_id）
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/status"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get status for task {task_id} loop {loop_id}: {str(e)}")
            raise
        
    async def get_loop_metrics(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """
        获取某个 LOOP 跑完后的各项指标（双参数：task_id + loop_id）。
        404 时重试一次（等待 5s，可能 read_exp_res.py 还未完成），最终仍失败则抛异常。
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/metrics"
        import asyncio
        for attempt in range(2):
            try:
                response = await self.client.get(url)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not payload:
                    raise RuntimeError(
                        f"回测指标响应为空或格式错误: task={task_id} loop={loop_id} payload={payload}"
                    )
                return payload
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and attempt == 0:
                    logger.warning(f"Metrics not ready yet for {task_id}/{loop_id}, retrying in 5s...")
                    await asyncio.sleep(5)
                    continue
                raise RuntimeError(f"Failed to get metrics for task {task_id} loop {loop_id}: {e}") from e
            except httpx.HTTPError as e:
                raise RuntimeError(f"Failed to get metrics for task {task_id} loop {loop_id}: {e}") from e

    async def kill_loop(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """终止 RDAgent 侧正在运行的 Loop 进程。"""
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/kill"
        try:
            response = await self.client.post(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            raise RuntimeError(f"Failed to kill loop {task_id}/{loop_id}: {e}") from e

    async def get_enhanced_metrics(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """
        获取增强诊断指标（训练曲线、IC 时间序列、收益曲线等）。
        Loop 已完成时调用，数据必须存在。404 时重试一次（read_exp_res.py 可能尚未写完）。
        """
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/enhanced-metrics"
        import asyncio
        for attempt in range(2):
            try:
                response = await self.client.get(url)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not payload:
                    raise RuntimeError(
                        f"增强指标响应为空或格式错误: task={task_id} loop={loop_id} payload={payload}"
                    )
                return payload
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and attempt == 0:
                    logger.warning(f"Enhanced metrics not ready yet for {task_id}/{loop_id}, retrying in 5s...")
                    await asyncio.sleep(5)
                    continue
                raise RuntimeError(f"Failed to get enhanced metrics for task {task_id} loop {loop_id}: {e}") from e
            except httpx.HTTPError as e:
                raise RuntimeError(f"Failed to get enhanced metrics for task {task_id} loop {loop_id}: {e}") from e

    async def stream_task_logs(self, task_id: str):
        """
        从 RDAgent 侧实时拉取任务日志流。
        SSE 长连接不设 read timeout — 实验可能运行数小时，训练期间可能长时间无输出。
        """
        url = f"{self.base_url}/tasks/{task_id}/logs"
        stream_timeout = httpx.Timeout(connect=30.0, read=None, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=stream_timeout) as stream_client:
            async with stream_client.stream("GET", url) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    yield line

    async def download_mlruns_params(self, task_id: str, loop_id: str) -> Optional[bytes]:
        """从节点下载指定 loop 的 mlruns params.pkl（tar.gz 打包，保留目录结构）。

        Returns: tar.gz bytes.
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/mlruns-params"
        try:
            response = await self.client.get(url, timeout=60.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as e:
            raise RuntimeError(f"download_mlruns_params failed for {task_id}/{loop_id}: {e}") from e

    async def download_loop_assets(self, task_id: str, loop_id: str, dest_dir: str) -> str:
        """
        调用 API 将 models/*.pkl 和 features_order.txt 打包下载，并解压到 AIstock 本地的 dest_dir
        （双参数：task_id + loop_id）
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/assets/download"
        zip_path = os.path.join(dest_dir, f"{loop_id}_assets.zip")
        
        try:
            os.makedirs(dest_dir, exist_ok=True)
            async with self.client.stream("GET", url) as response:
                response.raise_for_status()
                async with aiofiles.open(zip_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        await f.write(chunk)

            with zipfile.ZipFile(zip_path, "r") as zf:
                real_dest = os.path.realpath(dest_dir) + os.sep
                for info in zf.infolist():
                    target = os.path.realpath(os.path.join(dest_dir, info.filename))
                    if not target.startswith(real_dest) and target != real_dest.rstrip(os.sep):
                        raise ValueError(f"ZIP 路径遍历攻击: {info.filename}")
                zf.extractall(dest_dir)

            logger.info(f"Successfully downloaded assets for {loop_id} to {zip_path}")
            return dest_dir
        except httpx.HTTPError as e:
            logger.error(f"Failed to download assets for {loop_id}: {str(e)}")
            raise
        
    async def get_workspace_config(self) -> Dict[str, Any]:
        """
        获取 RDAgent 侧的工作区配置（路径等），用于动态生成 WSL 命令。
        """
        url = f"{self.base_url}/config"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get workspace config: {e}")
            raise

    async def download_group_predictions(
        self, task_id: str, loop_id: str, group_name: str
    ) -> bytes:
        """从节点下载指定组的 pred.pkl（用于多Alpha跨节点预测收集）。"""
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/groups/{group_name}/predictions"
        try:
            response = await self.client.get(url, timeout=60.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as e:
            raise RuntimeError(f"下载组预测失败: task={task_id} loop={loop_id} group={group_name}: {e}") from e

    async def download_workspace_file_bytes(
        self, task_id: str, loop_id: str, file_path: str
    ) -> bytes:
        """下载 workspace 中的任意文件原始字节。"""
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/files/{file_path}"
        try:
            response = await self.client.get(url, timeout=60.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as e:
            raise RuntimeError(f"下载 workspace 文件失败: task={task_id} loop={loop_id} file={file_path}: {e}") from e

    async def get_workspace_file(self, task_id: str, loop_id: str, file_path: str) -> Dict[str, Any] | str:
        """读取 workspace 中的指定文件内容。"""
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/files/{file_path}"
        try:
            response = await self.client.get(url, timeout=30.0)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if "json" in content_type:
                payload = response.json()
                if payload is None:
                    raise RuntimeError(
                        f"workspace 文件 JSON 为空: task={task_id} loop={loop_id} file={file_path}"
                    )
                return payload
            if not response.text:
                raise RuntimeError(
                    f"workspace 文件内容为空: task={task_id} loop={loop_id} file={file_path}"
                )
            return response.text
        except httpx.HTTPError as e:
            raise RuntimeError(f"读取 workspace 文件失败: task={task_id} loop={loop_id} file={file_path}: {e}") from e

    async def cleanup_task_workspace(self, task_id: str) -> bool:
        """
        要求 RDAgent 彻底删除任务工作区
        """
        url = f"{self.base_url}/tasks/{task_id}"
        try:
            response = await self.client.delete(url)
            response.raise_for_status()
            return True
        except httpx.HTTPError as e:
            logger.error(f"Failed to cleanup workspace for task {task_id}: {str(e)}")
            raise
