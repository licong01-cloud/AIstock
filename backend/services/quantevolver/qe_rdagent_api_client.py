import logging
import os
import aiofiles
import zipfile
from typing import Dict, Any
import httpx

logger = logging.getLogger(__name__)

class RdagentApiClient:
    """
    专门负责与被物理隔离的 RDAgent 端进行网络交互的客户端
    封装了诸如触发任务、获取回测指标、获取日志流、下载模型资产等操作。
    """
    def __init__(self, base_url: str = "http://localhost:9000/api/v1/qe_workspace"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=60.0)
        
    async def create_and_run_loop(
        self, task_id: str, loop_index: int, config: Dict[str, Any], experiment_files: Dict[str, str] = None, wsl_command: str = ""
    ) -> str:
        """
        通知 RDAgent 根据配置生成代码并启动执行 QLib 回测
        返回 RDAgent 端生成的 loop_id
        """
        url = f"{self.base_url}/tasks/{task_id}/loops"
        payload = {
            "loop_index": loop_index,
            "config": config,
            "experiment_files": experiment_files or {},
            "wsl_command": wsl_command
        }
        
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
        
    async def get_loop_status(self, loop_id: str) -> Dict[str, Any]:
        """
        查询 WSL 侧 QLib 任务执行的状态
        """
        url = f"{self.base_url}/loops/{loop_id}/status"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get status for loop {loop_id}: {str(e)}")
            raise
        
    async def get_loop_metrics(self, loop_id: str) -> Dict[str, Any]:
        """
        获取某个 LOOP 跑完后的各项指标 (读取 WSL 中的 qlib_res.csv 和图表分析)
        """
        url = f"{self.base_url}/loops/{loop_id}/metrics"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get metrics for loop {loop_id}: {str(e)}")
            raise

    async def stream_task_logs(self, task_id: str):
        """
        从 RDAgent 侧实时拉取任务日志流。
        注意：需要 RDAgent 提供 /tasks/{task_id}/logs SSE 接口。
        """
        url = f"{self.base_url}/tasks/{task_id}/logs"
        async with self.client.stream("GET", url) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line:
                    continue
                yield line
        
    async def download_loop_assets(self, loop_id: str, dest_dir: str) -> str:
        """
        调用 API 将 models/*.pkl 和 features_order.txt 打包下载，并解压到 AIstock 本地的 dest_dir
        """
        url = f"{self.base_url}/loops/{loop_id}/assets/download"
        zip_path = os.path.join(dest_dir, f"{loop_id}_assets.zip")
        
        try:
            os.makedirs(dest_dir, exist_ok=True)
            async with self.client.stream("GET", url) as response:
                response.raise_for_status()
                async with aiofiles.open(zip_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        await f.write(chunk)

            with zipfile.ZipFile(zip_path, "r") as zf:
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
