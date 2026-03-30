"""
QE文件同步客户端

通过RDAgent Results API将QE实验文件同步到RDAgent侧的qe_workspace目录。
当AIstock和RDAgent在同一台机器上时，直接写文件；
当两者独立部署时，通过HTTP API同步。
"""
from __future__ import annotations

import logging
import os
from typing import Dict, Optional

import requests

logger = logging.getLogger("aistock.quantevolver.qe_file_sync_client")


def _get_qe_api_base() -> str:
    """获取RDAgent API基础URL"""
    base = (os.getenv("RDAGENT_RESULTS_API_BASE_URL") or "").strip().rstrip("/")
    if not base:
        base = "http://127.0.0.1:9000"
    return base


class QEFileSyncClient:
    """QE文件同步客户端。
    
    支持两种模式：
    1. 直接写文件（同机部署，通过共享文件系统）
    2. 通过HTTP API同步（独立部署）
    """

    def __init__(
        self,
        api_base_url: Optional[str] = None,
        timeout_s: float = 30.0,
    ):
        self.api_base_url = (api_base_url or _get_qe_api_base()).rstrip("/")
        self.timeout_s = timeout_s

    def sync_experiment_files(
        self,
        exp_id: str,
        files: Dict[str, str],
    ) -> Dict:
        """批量同步实验文件到RDAgent侧。
        
        Args:
            exp_id: 实验ID/名称
            files: {filename: content} 字典
            
        Returns:
            同步结果
        """
        url = f"{self.api_base_url}/api/qe/experiments/{exp_id}/files/batch"
        try:
            resp = requests.post(
                url,
                json={"files": files},
                timeout=self.timeout_s,
            )
            resp.raise_for_status()
            result = resp.json()
            logger.info(
                f"[QESync] 实验 {exp_id} 文件同步完成: "
                f"{result.get('success_count', 0)}/{result.get('total', 0)} 成功"
            )
            return result
        except requests.exceptions.ConnectionError:
            logger.warning(
                f"[QESync] RDAgent API ({self.api_base_url}) 无法连接，"
                f"文件同步失败。请确认RDAgent API服务已启动。"
            )
            return {
                "success": False,
                "exp_id": exp_id,
                "error": f"RDAgent API ({self.api_base_url}) 无法连接",
                "uploaded": [],
                "failed": list(files.keys()),
                "total": len(files),
                "success_count": 0,
            }
        except requests.exceptions.Timeout:
            logger.warning(f"[QESync] 文件同步超时: {exp_id}")
            return {
                "success": False,
                "exp_id": exp_id,
                "error": "请求超时",
                "uploaded": [],
                "failed": list(files.keys()),
                "total": len(files),
                "success_count": 0,
            }
        except Exception as e:
            logger.error(f"[QESync] 文件同步异常: {exp_id}, {e}")
            return {
                "success": False,
                "exp_id": exp_id,
                "error": str(e),
                "uploaded": [],
                "failed": list(files.keys()),
                "total": len(files),
                "success_count": 0,
            }

    def list_experiments(self) -> Dict:
        """列出RDAgent侧所有QE实验目录。"""
        url = f"{self.api_base_url}/api/qe/experiments"
        try:
            resp = requests.get(url, timeout=self.timeout_s)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[QESync] 列出实验失败: {e}")
            return {"experiments": [], "total": 0, "error": str(e)}

    def list_experiment_files(self, exp_id: str) -> Dict:
        """列出实验目录中的文件。"""
        url = f"{self.api_base_url}/api/qe/experiments/{exp_id}/files"
        try:
            resp = requests.get(url, timeout=self.timeout_s)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[QESync] 列出实验文件失败: {exp_id}, {e}")
            return {"exp_id": exp_id, "files": [], "total": 0, "error": str(e)}

    def check_api_available(self) -> bool:
        """检查RDAgent QE文件同步API是否可用。"""
        url = f"{self.api_base_url}/api/qe/experiments"
        try:
            resp = requests.get(url, timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    def setup_experiment_workspace(
        self,
        exp_id: str,
        link_data_files: bool = True,
        overwrite: bool = False,
    ) -> Dict:
        """通过API在RDAgent侧创建实验工作区并链接数据文件。
        
        Args:
            exp_id: 实验ID/名称
            link_data_files: 是否链接数据文件（daily_pv.h5等）
            overwrite: 是否覆盖已存在的实验目录
            
        Returns:
            创建结果，包含workspace_path等信息
        """
        url = f"{self.api_base_url}/api/qe/experiments/{exp_id}/setup"
        try:
            resp = requests.post(
                url,
                json={
                    "link_data_files": link_data_files,
                    "overwrite": overwrite,
                },
                timeout=self.timeout_s,
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("success"):
                logger.info(
                    f"[QESetup] 实验工作区创建成功: {exp_id}, "
                    f"workspace={result.get('workspace_path')}, "
                    f"linked_files={result.get('linked_files_count', 0)}"
                )
            else:
                logger.warning(
                    f"[QESetup] 实验工作区创建失败: {exp_id}, "
                    f"error={result.get('error', 'unknown')}"
                )
            return result
        except requests.exceptions.ConnectionError:
            logger.warning(
                f"[QESetup] RDAgent API ({self.api_base_url}) 无法连接，"
                f"实验工作区创建失败。请确认RDAgent API服务已启动。"
            )
            return {
                "success": False,
                "exp_id": exp_id,
                "error": f"RDAgent API ({self.api_base_url}) 无法连接",
            }
        except requests.exceptions.Timeout:
            logger.warning(f"[QESetup] 实验工作区创建超时: {exp_id}")
            return {
                "success": False,
                "exp_id": exp_id,
                "error": "请求超时",
            }
        except Exception as e:
            logger.error(f"[QESetup] 实验工作区创建异常: {exp_id}, {e}")
            return {
                "success": False,
                "exp_id": exp_id,
                "error": str(e),
            }

    def get_factor_source_code(
        self,
        task_id: str,
        factor_name: str,
    ) -> Dict:
        """从RDAgent获取因子的原始源码。
        
        Args:
            task_id: RDAgent任务ID
            factor_name: 因子名称
            
        Returns:
            包含source_code的结果字典
        """
        url = f"{self.api_base_url}/api/qe/tasks/{task_id}/factors/{factor_name}/source"
        try:
            resp = requests.get(url, timeout=self.timeout_s)
            resp.raise_for_status()
            result = resp.json()
            if result.get("success"):
                logger.info(
                    f"[QEFactor] 获取因子源码成功: task={task_id}, "
                    f"factor={factor_name}, source={result.get('source')}"
                )
            else:
                logger.warning(
                    f"[QEFactor] 获取因子源码失败: task={task_id}, "
                    f"factor={factor_name}"
                )
            return result
        except requests.exceptions.ConnectionError:
            logger.warning(
                f"[QEFactor] RDAgent API ({self.api_base_url}) 无法连接"
            )
            return {
                "success": False,
                "task_id": task_id,
                "factor_name": factor_name,
                "error": f"RDAgent API ({self.api_base_url}) 无法连接",
            }
        except requests.exceptions.Timeout:
            logger.warning(f"[QEFactor] 获取因子源码超时: {factor_name}")
            return {
                "success": False,
                "task_id": task_id,
                "factor_name": factor_name,
                "error": "请求超时",
            }
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"[QEFactor] 因子源码不存在: task={task_id}, factor={factor_name}"
                )
                return {
                    "success": False,
                    "task_id": task_id,
                    "factor_name": factor_name,
                    "error": "因子源码不存在",
                }
            logger.error(f"[QEFactor] 获取因子源码HTTP错误: {e}")
            return {
                "success": False,
                "task_id": task_id,
                "factor_name": factor_name,
                "error": str(e),
            }
        except Exception as e:
            logger.error(f"[QEFactor] 获取因子源码异常: {factor_name}, {e}")
            return {
                "success": False,
                "task_id": task_id,
                "factor_name": factor_name,
                "error": str(e),
            }

    def list_task_factors(self, task_id: str) -> Dict:
        """列出RDAgent任务中的所有因子名称。
        
        Args:
            task_id: RDAgent任务ID
            
        Returns:
            包含factors列表的结果字典
        """
        url = f"{self.api_base_url}/api/qe/tasks/{task_id}/factors"
        try:
            resp = requests.get(url, timeout=self.timeout_s)
            resp.raise_for_status()
            result = resp.json()
            if result.get("success"):
                logger.info(
                    f"[QEFactor] 列出任务因子成功: task={task_id}, "
                    f"count={result.get('count', 0)}"
                )
            return result
        except Exception as e:
            logger.error(f"[QEFactor] 列出任务因子失败: task={task_id}, {e}")
            return {
                "success": False,
                "task_id": task_id,
                "factors": [],
                "error": str(e),
            }

    def get_linked_files_status(self, exp_id: str) -> Dict:
        """获取实验工作区中数据文件的链接状态。
        
        Args:
            exp_id: 实验ID
            
        Returns:
            包含files状态列表的结果字典
        """
        url = f"{self.api_base_url}/api/qe/experiments/{exp_id}/linked-files"
        try:
            resp = requests.get(url, timeout=self.timeout_s)
            resp.raise_for_status()
            result = resp.json()
            return result
        except Exception as e:
            logger.error(f"[QESetup] 获取链接文件状态失败: {exp_id}, {e}")
            return {
                "exp_id": exp_id,
                "files": [],
                "all_ready": False,
                "error": str(e),
            }
