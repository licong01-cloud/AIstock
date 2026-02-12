from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from fastapi import HTTPException


JsonDict = Dict[str, Any]

try:
    from dotenv import load_dotenv
    _service_file = Path(__file__).resolve()
    # f:/Dev/AIstock/backend/services/rdagent_results_api_client.py -> f:/Dev/AIstock/.env
    _root_dir = _service_file.parents[2]
    _env_path = _root_dir / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
        # logger.info(f"Loaded .env from {_env_path}") # logger not yet initialized
    else:
        # 尝试上一级 (如果是从 scripts 运行)
        _env_path = _service_file.parents[3] / ".env"
        if _env_path.exists():
            load_dotenv(_env_path, override=True)
except Exception:
    pass

logger = logging.getLogger("aistock.rdagent_client")


@dataclass(frozen=True)
class ResultsApiConfig:
    base_url: str
    timeout_seconds: float = 60.0


def _resolve_base_url() -> str:
    base = (os.getenv("RDAGENT_RESULTS_API_BASE_URL") or "").strip().rstrip("/")
    if not base:
        # default suggested in design doc
        base = "http://127.0.0.1:9000"
    return base


def get_results_api_config(*, timeout_seconds: Optional[float] = None) -> ResultsApiConfig:
    t = timeout_seconds
    if t is None:
        raw = (os.getenv("RDAGENT_RESULTS_API_TIMEOUT_SECONDS") or "").strip()
        if raw:
            try:
                t = float(raw)
            except Exception:
                t = None
    return ResultsApiConfig(base_url=_resolve_base_url(), timeout_seconds=float(t or 300.0))


class RDAgentResultsApiClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout_s: Optional[float] = None,
    ) -> None:
        cfg = get_results_api_config(timeout_seconds=timeout_s)
        base = (base_url or cfg.base_url).rstrip("/")
        self.base_url = base
        self.timeout_s = float(timeout_s if timeout_s is not None else cfg.timeout_seconds)
        self.cfg = ResultsApiConfig(base_url=self.base_url, timeout_seconds=float(self.timeout_s))
        # 记录成功探测到的前缀，避免后续子请求继续盲目探测
        self._detected_prefix: Optional[str] = None

    def _task_api_prefix_candidates(self) -> list[str]:
        # 如果已经探测到成功的前缀，优先使用
        if self._detected_prefix is not None:
            return [self._detected_prefix]

        raw = (os.getenv("RDAGENT_RESULTS_TASK_API_PREFIX") or "").strip().rstrip("/")
        cand: list[str] = []
        if raw:
            cand.append(raw)
        # 常见部署方式候选：
        # - RD-Agent 侧标准的接口前缀：/api/v1/rdagent
        # - 直接挂根路径: /tasks/...
        # - 命名空间: /rdagent
        # - 其他可能的前缀
        cand.extend(["/api/v1/rdagent", "", "/rdagent", "/api/v1", "/api/v1/rdagent/tasks"])
        out: list[str] = []
        seen = set()
        for p in cand:
            p = (p or "").strip().rstrip("/")
            if p in seen:
                continue
            seen.add(p)
            out.append(p)
        return out

    def _task_get_json(self, path: str, params: dict | None = None) -> dict:
        rp = "/" + str(path or "").lstrip("/")
        last_exc: Optional[Exception] = None
        
        # 1. 如果已有探测成功的前缀，直接使用，不再遍历
        if self._detected_prefix is not None:
            url = f"{self.base_url}{self._detected_prefix}{rp}"
            try:
                resp = requests.get(url, params=params, timeout=self.timeout_s)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                # 如果锁定前缀失效（例如服务重启且路径变了），重置前缀并进入全面探测
                logger.warning(f"Sticky prefix '{self._detected_prefix}' failed for {rp}: {e}. Resetting...")
                self._detected_prefix = None

        # 2. 全面探测所有可能的候选路径
        candidates = self._task_api_prefix_candidates()
        for prefix in candidates:
            url = f"{self.base_url}{prefix}{rp}"
            try:
                # 对于耗时的API（如sota_factor_anchor），使用完整超时时间
                # 其他API使用较短超时避免阻塞
                if "sota_factor_anchor" in rp or "session_anchor" in rp or "v2_alignment_preview" in rp:
                    probe_timeout = self.timeout_s  # 使用完整超时（默认60秒）
                else:
                    probe_timeout = min(10.0, self.timeout_s)  # 其他API最多10秒
                resp = requests.get(url, params=params, timeout=probe_timeout)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                
                # 探测成功，锁定此前缀
                logger.info(f"Successfully detected and locked RD-Agent task API prefix: '{prefix}'")
                self._detected_prefix = prefix
                return resp.json()
            except Exception as e:
                last_exc = e
                continue
        if last_exc is not None:
            raise last_exc
        raise HTTPException(status_code=404, detail=f"RD-Agent task API not found: {rp}")

    def _task_get_bytes(self, path: str, params: dict | None = None) -> bytes:
        rp = "/" + str(path or "").lstrip("/")
        last_exc: Optional[Exception] = None
        
        if self._detected_prefix is not None:
            url = f"{self.base_url}{self._detected_prefix}{rp}"
            try:
                resp = requests.get(url, params=params, timeout=self.timeout_s)
                if resp.status_code == 404:
                    # 404表示资源不存在，但前缀本身是正确的，不重置
                    raise HTTPException(status_code=404, detail=f"asset not found: {rp}")
                resp.raise_for_status()
                return resp.content
            except HTTPException:
                raise
            except Exception as e:
                # 连接错误等，重置前缀
                logger.warning(f"Sticky prefix '{self._detected_prefix}' failed for bytes {rp}: {e}. Resetting...")
                self._detected_prefix = None

        candidates = self._task_api_prefix_candidates()
        for prefix in candidates:
            url = f"{self.base_url}{prefix}{rp}"
            try:
                probe_timeout = min(30.0, self.timeout_s)
                resp = requests.get(url, params=params, timeout=probe_timeout)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                self._detected_prefix = prefix
                return resp.content
            except Exception as e:
                last_exc = e
                continue
        if last_exc is not None:
            raise last_exc
        raise HTTPException(status_code=404, detail=f"RD-Agent task API not found: {rp}")

    def list_tasks(self) -> list[dict]:
        data = self._task_get_json("/tasks")
        for k in ("tasks", "items", "data"):
            v = data.get(k)
            if isinstance(v, list):
                return v
        return []

    def get_task_session_anchor(self, task_id: str) -> dict:
        return self._task_get_json(f"/tasks/{task_id}/session_anchor")

    def get_task_sota_factor_anchor(self, task_id: str) -> dict:
        return self._task_get_json(f"/tasks/{task_id}/sota_factor_anchor")

    def get_v2_alignment_preview(self, task_id: str) -> dict:
        """调用 RD-Agent V2 对齐预览 API，获取 SOTA 因子数、Alpha 基线因子数、模型特征数及对齐验证结果。"""
        return self._task_get_json(f"/tasks/{task_id}/v2_alignment_preview")

    def get_aligned_sota_factors(self, task_id: str) -> dict:
        """调用 RD-Agent V2 aligned API，获取每个因子的 formulation、description、variables 等完整详情。"""
        return self._get_json(f"/api/extractors/sota_factors/v2/{task_id}/aligned")

    def download_task_asset_bytes(self, task_id: str, key: str) -> bytes:
        return self._task_get_bytes(f"/tasks/{task_id}/asset_bytes", params={"key": key})

    def _get_json(self, path: str, *, params: Optional[Dict[str, Any]] = None) -> JsonDict:
        url = f"{self.base_url}{path}"
        try:
            resp = requests.get(url, params=params, timeout=self.timeout_s)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.ConnectionError:
            logger.error(f"RD-Agent 结果服务器连接失败 (ConnectionError): {url}")
            raise HTTPException(
                status_code=503,
                detail=f"RD-Agent 结果服务器 ({self.base_url}) 无法连接。请检查端口是否正确（RD-Agent 侧建议 8081 或 9000），并确保该服务已启动。"
            )
        except requests.exceptions.Timeout:
            logger.error(f"RD-Agent 结果服务器请求超时: {url}")
            raise HTTPException(status_code=504, detail="RD-Agent 结果服务器请求超时")
        except requests.exceptions.HTTPError as e:
            logger.error(f"RD-Agent 结果服务器返回错误 ({resp.status_code}): {e}")
            raise HTTPException(status_code=resp.status_code, detail=f"RD-Agent 服务器错误: {str(e)}")
        except Exception as e:
            logger.error(f"RD-Agent 客户端未知错误: {e}")
            raise HTTPException(status_code=500, detail=f"RD-Agent 客户端错误: {str(e)}")

    def _download_file(self, path: str, *, params: Optional[Dict[str, Any]] = None, target_path: str) -> bool:
        url = f"{self.cfg.base_url}{path}"
        try:
            resp = requests.get(url, params=params, stream=True, timeout=self.cfg.timeout_seconds)
            resp.raise_for_status()
            p = Path(target_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except requests.exceptions.ConnectionError:
            logger.error(f"RD-Agent 结果服务器连接失败 (ConnectionError): {url}")
            raise HTTPException(
                status_code=503,
                detail=f"RD-Agent 结果服务器 ({self.cfg.base_url}) 无法连接。请检查端口是否正确（RD-Agent 侧建议 8081 或 9000），并确保该服务已启动。",
            )
        except requests.exceptions.Timeout:
            logger.error(f"RD-Agent 结果服务器请求超时: {url}")
            raise HTTPException(status_code=504, detail="RD-Agent 结果服务器请求超时")
        except requests.exceptions.HTTPError as e:
            logger.error(f"RD-Agent 结果服务器返回错误 ({resp.status_code}): {e}")
            raise HTTPException(status_code=resp.status_code, detail=f"RD-Agent 服务器错误: {str(e)}")
        except Exception as e:
            logger.error(f"RD-Agent 客户端未知错误: {e}")
            raise HTTPException(status_code=500, detail=f"RD-Agent 客户端错误: {str(e)}")

    def get_catalog_incremental(self, last_sync_time: Optional[str] = None, limit: int = 100) -> JsonDict:
        """获取增量/全量 Catalog 记录 (Phase 3 Section 16.2).

        当 last_sync_time 为 None 时，按 RD-Agent 约定拉取全量固化资产清单。
        """
        params: Dict[str, Any] = {"limit": limit}
        if last_sync_time:
            params["last_sync_time"] = last_sync_time
        return self._get_json("/catalog/incremental", params=params)

    def download_asset_bundle(self, asset_bundle_id: str, target_path: str) -> bool:
        """下载资产包 (Asset Bundle) (Phase 3 Section 16.2)."""
        url = f"{self.cfg.base_url}/artifacts/bundle/{asset_bundle_id}"
        logger.info(f"正在下载资产包: {asset_bundle_id} -> {target_path}")
        try:
            resp = requests.get(url, stream=True, timeout=60.0)
            resp.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            logger.error(f"下载资产包失败: {e}")
            return False

    def post_catalog_sync_status(self, asset_bundle_id: str, status: str = "synced") -> bool:
        """回传同步状态至 RD-Agent (Phase 3 Section 15.3, 16.2).

        Args:
            asset_bundle_id: 资产包唯一标识
            status: 同步状态，默认为 "synced"
        """
        url = f"{self.cfg.base_url}/ops/sync-confirm"
        payload = {
            "asset_bundle_id": asset_bundle_id,
            "status": status
        }
        try:
            resp = requests.post(url, json=payload, timeout=5.0)
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"报告同步状态失败: {e}")
            return False

    def get_catalog_factors(self, *, params: Optional[Dict[str, Any]] = None) -> JsonDict:
        return self._get_json("/catalog/factors", params=params)

    def get_catalog_strategies(self, *, params: Optional[Dict[str, Any]] = None) -> JsonDict:
        return self._get_json("/catalog/strategies", params=params)

    def get_catalog_loops(self, *, params: Optional[Dict[str, Any]] = None) -> JsonDict:
        return self._get_json("/catalog/loops", params=params)

    def get_catalog_models(self, *, params: Optional[Dict[str, Any]] = None) -> JsonDict:
        return self._get_json("/catalog/models", params=params)

    def get_alpha158_meta(self) -> JsonDict:
        return self._get_json("/alpha158/meta")

    def get_alpha360_meta(self) -> JsonDict:
        """获取 Alpha360 因子元数据 (Phase 3 补充)."""
        return self._get_json("/alpha360/meta")

    def get_loop_artifacts(self, *, task_run_id: str, loop_id: int) -> JsonDict:
        return self._get_json(f"/loops/{task_run_id}/{loop_id}/artifacts")

    def post_ops_materialize_and_refresh(self) -> JsonDict:
        """调用 RD-Agent Ops API 触发离线物化与 Catalog 刷新."""
        url = f"{self.cfg.base_url}/ops/materialize-and-refresh"
        logger.info(f"正在触发 RD-Agent 物化操作 (URL: {url})... 请耐心等待，这可能需要较长时间。")
        try:
            # Ops API 可能是长耗时操作，大幅增加超时时间
            resp = requests.post(url, timeout=1800.0)
            resp.raise_for_status()
            logger.info("RD-Agent 物化操作完成。")
            return resp.json()
        except requests.exceptions.ConnectionError:
            raise HTTPException(
                status_code=503,
                detail="RD-Agent 结果服务器无法连接，无法执行物化操作。"
            )
        except Exception as e:
            logger.error(f"物化操作失败: {e}")
            raise HTTPException(status_code=500, detail=f"物化操作失败: {str(e)}")

    # --- Task 级增量同步 API ---

    def get_tasks_latest(self, *, limit: int = 20) -> JsonDict:
        return self._task_get_json("/tasks/latest", params={"limit": int(limit)})

    def get_task_summary(self, *, task_id: str) -> JsonDict:
        tid = str(task_id).strip()
        return self._task_get_json(f"/tasks/{tid}/summary")

    def get_task_manifest(self, *, task_id: str) -> JsonDict:
        tid = str(task_id).strip()
        return self._task_get_json(f"/tasks/{tid}")

    def download_task_asset(self, *, task_id: str, relpath: str, target_path: str) -> bool:
        tid = str(task_id).strip()
        rp = str(relpath).strip().lstrip("/\\")
        self._task_download_file(
            f"/tasks/{tid}/assets",
            target_path,
            params={"relpath": rp},
        )
        return True
