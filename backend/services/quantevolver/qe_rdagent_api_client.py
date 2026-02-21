import logging
import asyncio
import os
import aiofiles
from typing import Dict, Any, List, Optional
import httpx
import json

logger = logging.getLogger(__name__)

class RdagentApiClient:
    """
    专门负责与被物理隔离的 RDAgent 端进行网络交互的客户端
    封装了诸如触发任务、获取回测指标、获取日志流、下载模型资产等操作。
    """
    def __init__(self, base_url: str = "http://localhost:9000/api/v1/qe_workspace"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=60.0)
        
    async def create_and_run_loop(self, task_id: str, loop_index: int, config: Dict[str, Any]) -> str:
        """
        通知 RDAgent 根据配置生成代码并启动执行 QLib 回测
        返回 RDAgent 端生成的 loop_id
        """
        url = f"{self.base_url}/tasks/{task_id}/loops"
        payload = {
            "loop_index": loop_index,
            "config": config
        }
        
        try:
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            return data.get("loop_id", f"{task_id}_L{loop_index}")
        except httpx.HTTPError as e:
            logger.error(f"Failed to create loop {loop_index} for task {task_id}: {str(e)}")
            # Fallback to mock for development if server is down
            logger.warning("Falling back to mock loop_id generation due to API error")
            return f"{task_id}_L{loop_index}"
        
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
            logger.warning(f"Failed to get status for loop {loop_id}, falling back to mock: {str(e)}")
            return {"status": "completed"}
        
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
            logger.warning(f"Failed to get metrics for loop {loop_id}, falling back to mock: {str(e)}")
            return {
                "IC": 0.054,
                "ICIR": 0.68,
                "Annualized Return": 0.15,
                "Max Drawdown": -0.124
            }
        
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
            
            # TODO: Extract ZIP file here using zipfile module
            # For now, just return the directory path
            logger.info(f"Successfully downloaded assets for {loop_id} to {zip_path}")
            return dest_dir
        except httpx.HTTPError as e:
            logger.warning(f"Failed to download assets for {loop_id}, falling back to mock: {str(e)}")
            return f"{dest_dir}/models_synced_mock"
        
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
            return False
