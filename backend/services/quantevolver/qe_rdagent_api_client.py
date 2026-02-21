import logging
import asyncio
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
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def create_and_run_loop(self, task_id: str, loop_index: int, config: Dict[str, Any]) -> str:
        """
        通知 RDAgent 根据配置生成代码并启动执行 QLib 回测
        返回 RDAgent 端生成的 loop_id
        """
        # TODO: 实际的 API 路径和 payload 需要和 RDAgent 侧对齐
        url = f"{self.base_url}/tasks/{task_id}/loops"
        payload = {
            "loop_index": loop_index,
            "config": config
        }
        # mock return
        return f"{task_id}_loop_{loop_index}"
        
    async def get_loop_status(self, loop_id: str) -> Dict[str, Any]:
        """
        查询 WSL 侧 QLib 任务执行的状态
        """
        # mock status
        return {"status": "completed"}
        
    async def get_loop_metrics(self, loop_id: str) -> Dict[str, Any]:
        """
        获取某个 LOOP 跑完后的各项指标 (读取 WSL 中的 qlib_res.csv 和图表分析)
        """
        # mock metrics
        return {
            "IC": 0.05,
            "ICIR": 0.5,
            "Annualized Return": 0.15,
            "Max Drawdown": -0.1
        }
        
    async def download_loop_assets(self, loop_id: str, dest_dir: str) -> str:
        """
        调用 API 将 models/*.pkl 和 features_order.txt 打包下载，并解压到 AIstock 本地的 dest_dir
        """
        # mock download path
        return f"{dest_dir}/models_synced"
        
    async def cleanup_task_workspace(self, task_id: str) -> bool:
        """
        要求 RDAgent 彻底删除任务工作区
        """
        return True
