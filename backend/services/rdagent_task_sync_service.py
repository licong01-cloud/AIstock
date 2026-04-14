from __future__ import annotations

import hashlib  # noqa
import json
import logging
import os
import pickle
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path, WindowsPath, PosixPath
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from pydantic import BaseModel

from ..data_service import xtquant_adapter
from ..db.pg_pool import get_conn
from .rdagent_results_api_client import RDAgentResultsApiClient


# 强制从项目根目录加载 .env
try:
    from dotenv import load_dotenv
    _service_file = Path(__file__).resolve()
    # 路径: f:/Dev/AIstock/backend/services/rdagent_task_sync_service.py
    # 根目录应为 f:/Dev/AIstock/
    _root_dir = _service_file.parents[2]
    _env_path = _root_dir / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
    else:
        # 尝试上一级
        _env_path = _service_file.parents[3] / ".env"
        if _env_path.exists():
            load_dotenv(_env_path, override=True)
except Exception:
    pass

logger = logging.getLogger("aistock.rdagent_task_sync_service")

# 初始化 API 客户端（默认节点）
_rdagent_client = RDAgentResultsApiClient()


def _get_client_for_node(node_id: Optional[str] = None) -> RDAgentResultsApiClient:
    """获取指定节点的 API 客户端。node_id=None 时返回默认客户端。"""
    if node_id is None:
        return _rdagent_client
    return RDAgentResultsApiClient.for_node(node_id)

JsonDict = Dict[str, Any]

def _utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

def _parse_factor_names_from_factor_py(factor_py_content: str) -> List[str]:
    """从factor.py内容中解析实际计算的因子名称
    
    查找所有以calculate_开头的函数定义，提取因子名称。
    这确保了factor_order.json中的dynamic_factors与factor.py实际计算的因子一致。
    
    Args:
        factor_py_content: factor.py文件的内容
    
    Returns:
        因子名称列表（不包含calculate_前缀）
    """
    import re
    pattern = r'def\s+(calculate_[a-zA-Z0-9_]+)\s*\('
    matches = re.findall(pattern, factor_py_content)
    
    factor_names = []
    for func_name in matches:
        if func_name.startswith('calculate_'):
            factor_name = func_name[len('calculate_'):]
            factor_names.append(factor_name)
    
    return factor_names

def _get_task_row(task_id: str) -> Optional[JsonDict]:
    sql = """
        SELECT task_id, task_dir, manifest_path, manifest_sha1, manifest_schema_version, 
               log_dir, task_run_id, sync_status, sync_error, sync_diagnostics, 
               last_sota_factor_workspace_id, updated_at_utc, is_enabled_for_selection
        FROM aistock_task_catalog
        WHERE task_id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (task_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "task_id": row[0],
                "task_dir": row[1],
                "manifest_path": row[2],
                "manifest_sha1": row[3],
                "manifest_schema_version": row[4],
                "log_dir": row[5],
                "task_run_id": row[6],
                "sync_status": row[7],
                "sync_error": row[8],
                "sync_diagnostics": row[9],
                "last_sota_factor_workspace_id": row[10],
                "updated_at_utc": row[11].isoformat() if row[11] else None,
                "is_enabled_for_selection": row[12],
            }

def _upsert_task_catalog(task_id: str, data: JsonDict) -> None:
    fields = list(data.keys())
    # 将字典类型的值转换为JSON字符串
    values = []
    for f in fields:
        val = data[f]
        if isinstance(val, dict):
            values.append(json.dumps(val))
        else:
            values.append(val)
    
    update_parts = [f"{f} = EXCLUDED.{f}" for f in fields if f != "task_id"]
    
    sql = f"""
        INSERT INTO aistock_task_catalog (task_id, {", ".join(fields)})
        VALUES (%s, {", ".join(["%s"] * len(fields))})
        ON CONFLICT (task_id) DO UPDATE SET
            {", ".join(update_parts)}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [task_id] + values)

def _ensure_task_catalog_table() -> None:
    sql = """
        CREATE TABLE IF NOT EXISTS aistock_task_catalog (
            task_id TEXT PRIMARY KEY,
            task_dir TEXT,
            manifest_path TEXT,
            manifest_sha1 TEXT,
            manifest_schema_version INTEGER,
            log_dir TEXT,
            task_run_id TEXT,
            sync_status TEXT, -- syncing, success, failed
            sync_error TEXT,
            sync_diagnostics JSONB,
            last_sota_factor_workspace_id TEXT,
            updated_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_enabled_for_selection BOOLEAN DEFAULT FALSE,
            enabled_for_selection_at_utc TIMESTAMP WITH TIME ZONE,
            enabled_for_selection_by TEXT,
            sota_factors_count INTEGER,
            has_model_weight BOOLEAN DEFAULT FALSE,
            has_factor_order BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_aistock_task_catalog_updated ON aistock_task_catalog(updated_at_utc DESC);
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

def _normalize_workspace_path(ws_path: Optional[str]) -> Optional[str]:
    if not ws_path:
        return None
    p = str(ws_path).replace("\\", "/")
    if p.startswith("/mnt/f/"):
        p = "F:/" + p[7:]
    return p

class TaskSyncResult(BaseModel):
    ok: bool
    task_id: str
    sync_status: str
    task_dir: str
    manifest_path: str
    error: Optional[str] = None
    diagnostics: Optional[JsonDict] = None

class RDAgentTaskSyncService:
    def __init__(self):
        aistock_root = Path(os.environ.get("AISTOCK_ROOT") or "f:/Dev/AIstock").resolve()
        self.assets_root = aistock_root / "rdagent_assets" / "rdagent_tasks"

    def sync_task(self, *, task_id: str, operator: str = "script", node_id: Optional[str] = None) -> TaskSyncResult:
        """API 同步（严格模式）：强制调用 sync_task_from_log (v2 逻辑)。"""
        tid = str(task_id).strip()
        if not tid:
            return TaskSyncResult(ok=False, task_id=tid, sync_status="failed", task_dir="", manifest_path="", error="task_id 为空")
        return self.sync_task_from_log(task_id=tid, operator=operator, node_id=node_id)

    def sync_task_from_log(self, *, task_id: str, operator: str = "ui", node_id: Optional[str] = None) -> TaskSyncResult:
        """从RD-Agent同步任务资产。基于《模型权重文件定位方案_v2.md》实现。

        核心流程：
        1. 调用sota_factor_anchor API获取SOTA因子信息
        2. 下载模型权重（从最后一个SOTA因子实验）
        3. 调用v2_alignment_preview API获取因子顺序（alpha基线+动态因子）
        4. 下载主因子和所有based factors
        5. 生成factor_order.json
        6. 填充catalog表（factor、alpha_baseline、factor_order）

        node_id: 指定从哪个节点同步。None 使用默认节点。
        """
        tid = str(task_id).strip()
        task_dir = self.assets_root / tid
        manifest_path = task_dir / "manifest.json"

        # 使用指定节点的客户端
        _client = _get_client_for_node(node_id)

        catalog_init: JsonDict = {
            "task_dir": str(task_dir),
            "manifest_path": str(manifest_path),
            "sync_status": "syncing",
            "sync_error": None,
            "sync_diagnostics": None,
            "updated_at_utc": datetime.utcnow(),
        }
        if node_id:
            catalog_init["node_id"] = node_id
        _upsert_task_catalog(tid, catalog_init)

        diagnostics: JsonDict = {"mode": "v2_sota_factor_anchor", "errors": [], "warnings": []}

        try:
            task_dir.mkdir(parents=True, exist_ok=True)

            # ============================================================
            # 步骤1: 调用sota_factor_anchor API获取完整SOTA信息
            # ============================================================
            # 注意：混合task（有SOTA模型但无SOTA因子）会返回no_sota_exp_in_log，
            # 此时不应报错退出，而是标记为无SOTA因子继续后续步骤
            logger.info(f"[{tid}] 调用sota_factor_anchor API...")
            has_sota_factors = False
            try:
                anchor_resp = _client.get_task_sota_factor_anchor(task_id=tid)
                if anchor_resp and anchor_resp.get("ok"):
                    has_sota_factors = True
                    diagnostics["sota_anchor"] = {
                        "last_sota_factor_index": anchor_resp.get("last_sota_factor_index"),
                        "model_exp_index": anchor_resp.get("model_exp_index"),
                        "resolved_model_weight_key": anchor_resp.get("resolved_model_weight_key"),
                        "resolved_model_weight_source": anchor_resp.get("resolved_model_weight_source"),
                        "resolved_factor_entry_key": anchor_resp.get("resolved_factor_entry_key"),
                        "based_factors_count": len(anchor_resp.get("based_factor_entries", [])),
                    }
                else:
                    error_msg = anchor_resp.get("error") if anchor_resp else "API返回空"
                    logger.warning(f"[{tid}] sota_factor_anchor无SOTA因子: {error_msg}")
                    diagnostics["sota_anchor"] = {"ok": False, "error": error_msg}
                    anchor_resp = anchor_resp or {}
            except Exception as e:
                logger.warning(f"[{tid}] sota_factor_anchor调用异常: {e}")
                diagnostics["sota_anchor"] = {"ok": False, "error": str(e)}
                anchor_resp = {}

            # ============================================================
            # 步骤2: 下载模型权重（双重定位：file_dict → mlruns）
            # ============================================================
            # 模型权重可能存在于两个位置：
            # A) session file_dict 中（anchor.resolved_model_weight_key 有值）
            # B) workspace/mlruns 目录中（anchor.resolved_model_weight_key 为null）
            # 当A失败时，通过 complete_assets API 获取（该API内部实现了双重定位）
            resolved_weight_key = anchor_resp.get("resolved_model_weight_key")
            has_model_weight = False
            weight_bytes = None

            # 方式A: 从anchor的file_dict key下载
            if resolved_weight_key:
                logger.info(f"[{tid}] 下载模型权重(file_dict): {resolved_weight_key}")
                try:
                    weight_bytes = _client.download_task_asset_bytes(tid, resolved_weight_key)
                    if weight_bytes:
                        has_model_weight = True
                        diagnostics["model_weight"] = {
                            "key": resolved_weight_key,
                            "path": "model.pkl",
                            "size": len(weight_bytes),
                            "source": anchor_resp.get("resolved_model_weight_source", "file_dict"),
                        }
                        logger.info(f"[{tid}] 模型权重下载成功(file_dict): {len(weight_bytes)} bytes")
                    else:
                        diagnostics["warnings"].append(f"模型权重file_dict下载返回空: {resolved_weight_key}")
                except Exception as e:
                    raise RuntimeError(f"[{tid}] 模型权重file_dict下载失败: {e}") from e

            # 方式B: file_dict无权重时，通过complete_assets API获取（支持mlruns定位）
            if not has_model_weight:
                logger.info(f"[{tid}] file_dict无模型权重，尝试complete_assets API(mlruns定位)...")
                try:
                    import requests as _req_mw
                    base_url = _client.base_url.rstrip("/")
                    ca_url = f"{base_url}/tasks/{tid}/complete_assets"
                    ca_resp = _req_mw.get(ca_url, timeout=300.0)
                    ca_resp.raise_for_status()
                    ca_data = ca_resp.json()

                    mw_info = ca_data.get("model_weight", {}) if ca_data else {}
                    if not mw_info.get("found"):
                        raise RuntimeError(f"[{tid}] complete_assets未找到模型权重")

                    mw_source = mw_info.get("source", "unknown")
                    mw_file_path = mw_info.get("file_path")
                    mw_key = mw_info.get("key") or mw_info.get("file_name")
                    download_key = mw_file_path or mw_key
                    if not download_key:
                        raise RuntimeError(f"[{tid}] complete_assets找到权重但无可用下载key")

                    logger.info(f"[{tid}] 通过asset_bytes下载模型权重(source={mw_source}): {download_key}")
                    weight_bytes = _client.download_task_asset_bytes(tid, download_key)
                    if not weight_bytes:
                        raise RuntimeError(f"[{tid}] asset_bytes下载模型权重返回空 (key={download_key})")

                    has_model_weight = True
                    diagnostics["model_weight"] = {
                        "key": download_key,
                        "path": "model.pkl",
                        "size": len(weight_bytes),
                        "source": mw_source,
                    }
                    logger.info(f"[{tid}] 模型权重下载成功(complete_assets/{mw_source}): {len(weight_bytes)} bytes")
                except RuntimeError:
                    raise
                except Exception as ca_err:
                    raise RuntimeError(f"[{tid}] complete_assets API调用失败: {ca_err}") from ca_err

            # 保存模型权重文件
            if has_model_weight and weight_bytes:
                weight_path = task_dir / "model.pkl"
                weight_path.write_bytes(weight_bytes)
                logger.info(f"[{tid}] 模型权重已保存: {len(weight_bytes)} bytes")
            elif not has_model_weight:
                raise RuntimeError(f"[{tid}] 最终未获取到模型权重")

            # ============================================================
            # 步骤3: 调用v2_alignment_preview获取对齐数据
            # ============================================================
            # 设计原则（金融数据严谨性要求）：
            # - factor_order 必须且只能来自 v2_alignment_preview（parquet schema）
            #   因为 parquet schema 是模型实际训练使用的特征列，是唯一可信来源
            # - 不再下载 model_meta.json（该文件由历史backfill脚本生成，非RDAgent/QLib原生产物）
            # - 所有因子数据通过API获取，禁止直接操作RDAgent侧文件
            alpha_baseline_factors = []
            alpha_source = None
            v2_preview_data = None  # V2 preview返回的完整数据（含sota_factors、factor_order等）
            
            # 调用v2_alignment_preview获取对齐数据（唯一可信的factor_order来源）
            # 注意：混合task（有SOTA模型但无SOTA因子）的v2_alignment_preview也会返回失败，
            # 此时跳过因子对齐，继续模型同步
            try:
                import requests as _req
                base_url = _client.base_url.rstrip("/")
                v2_url = f"{base_url}/tasks/{tid}/v2_alignment_preview"
                logger.info(f"[{tid}] 调用V2 alignment preview: {v2_url}")
                v2_resp = _req.get(v2_url, timeout=600.0)
                v2_resp.raise_for_status()
                v2_data = v2_resp.json()
                if v2_data and v2_data.get('ok'):
                    v2_alpha = v2_data.get('alpha_factors', [])
                    v2_sota = v2_data.get('sota_factors', [])
                    v2_model_features = v2_data.get('model_feature_count')
                    v2_is_aligned = v2_data.get('is_aligned', False)
                    
                    if not v2_alpha:
                        raise RuntimeError("V2 preview未返回alpha因子列表")
                    
                    # SOTA因子可以为空（混合task可能只有模型没有因子）
                    if not v2_sota:
                        logger.warning(f"[{tid}] V2 preview未返回SOTA因子列表，此task可能只有模型")
                        diagnostics["warnings"].append("V2 preview未返回SOTA因子列表")
                    
                    # 对齐验证（仅在有SOTA因子时执行）
                    if v2_sota:
                        expected_total = len(v2_alpha) + len(v2_sota)
                        if v2_model_features is not None and expected_total != v2_model_features:
                            raise RuntimeError(
                                f"V2 preview对齐验证失败: alpha({len(v2_alpha)}) + sota({len(v2_sota)}) = {expected_total} "
                                f"!= model_feature_count({v2_model_features})"
                            )
                    
                    alpha_baseline_factors = v2_alpha
                    alpha_source = f"v2_alignment_preview/{v2_data.get('alpha_source', 'unknown')}"
                    v2_preview_data = v2_data
                    expected_total = len(v2_alpha) + len(v2_sota)
                    logger.info(
                        f"[{tid}] V2 preview获取成功: {len(v2_alpha)}个alpha + {len(v2_sota)}个SOTA = {expected_total}个因子, "
                        f"model_feature_count={v2_model_features}, is_aligned={v2_is_aligned}, "
                        f"source={v2_data.get('sota_source', '?')}"
                    )
                else:
                    v2_error = v2_data.get('error') if v2_data else 'empty'
                    logger.warning(f"[{tid}] V2 preview返回失败: {v2_error}，此task可能无SOTA因子实验")
                    diagnostics["warnings"].append(f"V2 preview失败: {v2_error}")
                    # 不报错，继续后续步骤（模型同步等）
            except RuntimeError:
                raise
            except Exception as v2_err:
                raise RuntimeError(f"[{tid}] V2 alignment preview调用异常: {v2_err}") from v2_err
            
            diagnostics["alpha_baseline_factors"] = {
                "count": len(alpha_baseline_factors),
                "source": alpha_source,
                "factors": alpha_baseline_factors,
            }

            # ============================================================
            # 步骤4: 通过complete_assets API获取所有SOTA因子代码
            # ============================================================
            # 设计原则：
            # - 使用complete_assets API获取所有SOTA因子代码（anchor只返回最后一轮的主因子）
            # - 每个因子保存为独立的原始文件（factor_{name}.py），保持RDAgent原始代码不修改
            # - 合并所有因子代码到factor.py，推理引擎逐个执行calculate_函数
            all_factors = []
            
            try:
                import requests as _req
                base_url = _client.base_url.rstrip("/")
                ca_url = f"{base_url}/tasks/{tid}/complete_assets"
                logger.info(f"[{tid}] 调用complete_assets API获取所有因子代码...")
                ca_resp = _req.get(ca_url, timeout=600.0)
                ca_resp.raise_for_status()
                ca_data = ca_resp.json()
                
                if not ca_data or not ca_data.get("ok"):
                    raise RuntimeError(f"complete_assets API失败: {ca_data.get('error') if ca_data else 'empty'}")
                
                factor_codes = ca_data.get("factor_codes", [])
                logger.info(f"[{tid}] complete_assets返回 {len(factor_codes)} 个因子代码")
                
                if factor_codes:
                    # 保存每个因子的原始代码为独立文件
                    factors_dir = task_dir / "factors"
                    factors_dir.mkdir(exist_ok=True)
                    
                    # 合并所有因子代码到factor.py（保持原始代码，仅拼接）
                    merged_lines = [
                        "# 合并的SOTA因子文件（由task同步自动生成）",
                        "# 每个calculate_函数对应一个独立的SOTA因子",
                        "# 原始代码未做任何修改，直接兼容QLib环境",
                        "",
                    ]
                    
                    # 收集所有import语句（去重）
                    seen_imports = set()
                    all_func_bodies = []
                    
                    for i, code_info in enumerate(factor_codes):
                        factor_name = code_info.get("factor_name", f"factor_{i}")
                        code_content = code_info.get("code", "")
                        
                        if not code_content:
                            diagnostics["warnings"].append(f"因子 {factor_name} 代码为空")
                            continue
                        
                        # 保存独立的原始因子文件（不做任何修改）
                        ind_path = factors_dir / f"{factor_name}.py"
                        ind_path.write_text(code_content, encoding="utf-8")
                        
                        all_factors.append({
                            "type": "sota",
                            "index": i,
                            "factor_name": factor_name,
                            "path": f"factors/{factor_name}.py",
                            "size": len(code_content),
                        })
                        
                        # 分离import语句和函数体
                        import_lines = []
                        body_lines = []
                        for line in code_content.split('\n'):
                            stripped = line.strip()
                            if stripped.startswith('import ') or stripped.startswith('from '):
                                if stripped not in seen_imports:
                                    seen_imports.add(stripped)
                                    # 使用stripped确保import在顶层无缩进（原始代码中import可能在函数体内有缩进）
                                    import_lines.append(stripped)
                            else:
                                body_lines.append(line)
                        
                        all_func_bodies.append((factor_name, '\n'.join(body_lines)))
                        if import_lines:
                            merged_lines.extend(import_lines)
                        
                        logger.info(f"[{tid}] 因子 {factor_name} 代码已保存 ({len(code_content)} chars)")
                    
                    # 拼接所有函数体
                    merged_lines.append("")
                    for factor_name, body in all_func_bodies:
                        merged_lines.append(f"# --- SOTA因子: {factor_name} ---")
                        merged_lines.append(body)
                        merged_lines.append("")
                    
                    # 写入合并的factor.py
                    factor_py_path = task_dir / "factor.py"
                    merged_code = '\n'.join(merged_lines)
                    factor_py_path.write_text(merged_code, encoding="utf-8")
                    
                    logger.info(
                        f"[{tid}] 合并factor.py生成成功: {len(factor_codes)}个因子, "
                        f"{len(merged_code)} chars"
                    )
                else:
                    raise RuntimeError(f"[{tid}] complete_assets未返回因子代码")

            except RuntimeError:
                raise
            except Exception as e:
                raise RuntimeError(f"[{tid}] complete_assets因子代码获取失败: {e}") from e

            diagnostics["all_factors"] = {
                "total": len(all_factors),
                "sota_from_complete_assets": sum(1 for f in all_factors if f.get("type") == "sota"),
            }

            # ============================================================
            # 步骤5: 生成factor_order.json（严格使用v2_alignment_preview数据）
            # ============================================================
            # 设计原则：factor_order 只能来自 v2_alignment_preview（parquet schema），
            # 禁止使用 V1 extractor API（会包含幽灵因子导致数据不对齐）。
            # v2_preview_data 在步骤3中已经获取，可能为None（混合task无SOTA因子实验）。
            if v2_preview_data is not None:
                v2_sota = v2_preview_data.get('sota_factors', [])
                v2_alpha = v2_preview_data.get('alpha_factors', [])
                v2_factor_order = v2_preview_data.get('factor_order', [])
                
                if not v2_factor_order:
                    # factor_order字段为空时，手动拼接：alpha在前，sota在后
                    v2_factor_order = list(v2_alpha) + list(v2_sota)
                
                factor_order_data = {
                    "task_id": tid,
                    "total_factors": len(v2_factor_order),
                    "alpha158_count": len(v2_alpha),
                    "dynamic_count": len(v2_sota),
                    "factor_order": v2_factor_order,
                    "alpha158_factors": v2_alpha,
                    "dynamic_factors": v2_sota,
                    "source": "v2_alignment_preview",
                    "is_aligned": v2_preview_data.get('is_aligned', False),
                    "model_feature_count": v2_preview_data.get('model_feature_count'),
                    "sota_source": v2_preview_data.get('sota_source', 'unknown'),
                }
                logger.info(f"[{tid}] 从V2 preview生成factor_order.json: {len(v2_alpha)}个alpha + {len(v2_sota)}个SOTA = {len(v2_factor_order)}个因子")
                
                factor_order_path = task_dir / "factor_order.json"
                factor_order_path.write_text(
                    json.dumps(factor_order_data, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )
                diagnostics["factor_order"] = {
                    "path": "factor_order.json",
                    "source": v2_preview_data.get('sota_source', 'v2_preview'),
                    "total_factors": factor_order_data["total_factors"],
                    "alpha158_count": factor_order_data["alpha158_count"],
                    "dynamic_count": factor_order_data["dynamic_count"],
                }
                logger.info(f"[{tid}] factor_order.json生成成功")
            else:
                logger.warning(f"[{tid}] v2_preview_data为None，跳过factor_order.json生成（此task可能无SOTA因子实验）")
                diagnostics["factor_order"] = {"skipped": True, "reason": "v2_preview_data为None"}

            # ============================================================
            # 步骤6: 生成本地manifest.json
            # ============================================================
            local_manifest = {
                "schema_version": 3,
                "task_id": tid,
                "generated_at_utc": _utc_now_iso(),
                "primary_assets": {
                    "model_weight_relpath": "model.pkl" if has_model_weight else None,
                    "factor_entry_relpath": "factor.py" if all_factors else None,
                },
                "assets": {
                    "model_weight": "model.pkl" if has_model_weight else None,
                    "factor_entry": "factor.py" if all_factors else None,
                    "factor_order": "factor_order.json",
                    "factors_count": len(all_factors),
                },
                "diagnostics": diagnostics
            }
            manifest_txt = json.dumps(local_manifest, ensure_ascii=False, indent=2)
            manifest_path.write_text(manifest_txt, encoding="utf-8")
            manifest_sha1 = _sha1_text(manifest_txt)

            # ============================================================
            # 步骤7: 更新task catalog表
            # ============================================================
            _upsert_task_catalog(
                tid,
                {
                    "task_dir": str(task_dir),
                    "manifest_path": str(manifest_path),
                    "manifest_sha1": manifest_sha1,
                    "manifest_schema_version": 3,
                    "sync_status": "success",
                    "sync_error": None,
                    "sync_diagnostics": json.dumps(diagnostics, ensure_ascii=False),
                    "updated_at_utc": datetime.utcnow(),
                    "enabled_for_selection_by": operator,
                    "sota_factors_count": len(all_factors),
                    "has_model_weight": has_model_weight,
                    "has_factor_order": True,
                },
            )
            
            logger.info(f"[{tid}] Task同步成功")

            # ============================================================
            # 步骤8: SOTA 因子入库到 aistock_factor_catalog
            # ============================================================
            if v2_preview_data and v2_preview_data.get("sota_factors"):
                try:
                    from .rdagent_factor_catalog_sync import sync_factors_from_task
                    factor_sync_result = sync_factors_from_task(
                        task_id=tid,
                        v2_preview_data=v2_preview_data,
                        anchor_resp=anchor_resp,
                        task_dir=str(task_dir),
                    )
                    diagnostics["factor_catalog_sync"] = {
                        "ok": factor_sync_result.ok,
                        "total": factor_sync_result.total_sota_factors,
                        "inserted": factor_sync_result.inserted,
                        "updated": factor_sync_result.updated,
                        "dedup_skipped": factor_sync_result.dedup_skipped,
                        "errors": factor_sync_result.errors,
                    }
                    logger.info(
                        f"[{tid}] 因子入库完成: {factor_sync_result.inserted}/{factor_sync_result.total_sota_factors} 入库"
                    )
                except Exception as e:
                    diagnostics["factor_catalog_sync"] = {"ok": False, "error": str(e)}
                    logger.error(f"[{tid}] 因子入库失败（不影响Task同步状态）: {e}")

            # ============================================================
            # 步骤9: SOTA Model Loop数据同步到 aistock_model_catalog
            # 只同步进入SOTA的模型（model task和quant task均适用）
            # ============================================================
            try:
                from .rdagent_model_catalog_sync import sync_models_from_task
                model_sync_result = sync_models_from_task(
                    task_id=tid,
                    task_dir=str(task_dir),
                )
                diagnostics["model_catalog_sync"] = {
                    "ok": model_sync_result.ok,
                    "total": model_sync_result.total_models,
                    "inserted": model_sync_result.inserted,
                    "errors": model_sync_result.errors,
                }
                if model_sync_result.total_models > 0:
                    logger.info(
                        f"[{tid}] 模型入库完成: {model_sync_result.inserted}/{model_sync_result.total_models} 入库"
                    )
            except Exception as e:
                diagnostics["model_catalog_sync"] = {"ok": False, "error": str(e)}
                logger.error(f"[{tid}] 模型入库失败（不影响Task同步状态）: {e}")

            return TaskSyncResult(
                ok=True,
                task_id=tid,
                sync_status="success",
                task_dir=str(task_dir),
                manifest_path=str(manifest_path),
                diagnostics=diagnostics
            )

        except Exception as e:
            err_msg = str(e)
            diagnostics["errors"].append(err_msg)
            logger.exception(f"[{tid}] Task同步失败")
            _upsert_task_catalog(
                tid,
                {
                    "sync_status": "failed",
                    "sync_error": err_msg,
                    "sync_diagnostics": json.dumps(diagnostics, ensure_ascii=False),
                    "updated_at_utc": datetime.utcnow(),
                },
            )
            return TaskSyncResult(
                ok=False,
                task_id=tid,
                sync_status="failed",
                task_dir=str(task_dir),
                manifest_path=str(manifest_path),
                error=err_msg,
                diagnostics=diagnostics
            )

    def list_local_tasks(self, *, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        limit_i = max(int(limit or 200), 1)
        offset_i = max(int(offset or 0), 0)
        sql = """
            SELECT task_id, updated_at_utc, sync_status, sync_error, manifest_path, task_dir, is_enabled_for_selection
            FROM aistock_task_catalog
            ORDER BY updated_at_utc DESC NULLS LAST
            LIMIT %s OFFSET %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit_i, offset_i))
                rows = cur.fetchall() or []
        items = []
        for r in rows:
            items.append({
                "task_id": r[0],
                "updated_at_utc": r[1].isoformat() if r[1] else None,
                "sync_status": r[2],
                "sync_error": r[3],
                "manifest_path": r[4],
                "task_dir": r[5],
                "is_enabled_for_selection": bool(r[6]) if r[6] is not None else False,
            })
        return {"ok": True, "count": len(items), "items": items}

    def get_local_task(self, *, task_id: str) -> Dict[str, Any]:
        tid = str(task_id).strip()
        if not tid:
            return {"ok": False, "error": "task_id 为空"}
        row = _get_task_row(tid)
        if not row:
            return {"ok": False, "error": "task_not_found"}
        try:
            di = row.get("sync_diagnostics")
            if isinstance(di, str):
                row["sync_diagnostics"] = json.loads(di)
        except Exception:
            pass
        return {"ok": True, "task": row}

    def get_local_manifest_text(self, *, task_id: str) -> Dict[str, Any]:
        tid = str(task_id).strip()
        if not tid:
            return {"ok": False, "error": "task_id 为空"}
        row = _get_task_row(tid)
        if not row:
            return {"ok": False, "error": "task_not_found"}
        mp = row.get("manifest_path")
        if not mp:
            return {"ok": False, "error": "manifest_path_missing"}
        p = Path(str(mp))
        if not p.exists() or not p.is_file():
            return {"ok": False, "error": f"manifest_not_found: {p}"}
        return {"ok": True, "content": p.read_text(encoding="utf-8", errors="ignore"), "manifest_path": str(p)}

    def audit_local_task_assets(self, *, limit: int = 5000) -> Dict[str, Any]:
        task_root = self.assets_root
        if not task_root.exists() or not task_root.is_dir():
            return {"ok": False, "error": f"task_root_not_found: {task_root}"}
        dirs = [p for p in task_root.iterdir() if p.is_dir() and not p.name.startswith(".")]
        dirs.sort(key=lambda p: p.name, reverse=True)
        dirs = dirs[: max(int(limit or 5000), 1)]
        out: List[Dict[str, Any]] = []
        ok_cnt = 0
        for d in dirs:
            rec: Dict[str, Any] = {"task_id": d.name, "task_dir": str(d)}
            mp = (d / "manifest.json").resolve()
            rec["manifest"] = {"path": str(mp), "exists": bool(mp.exists() and mp.is_file())}
            manifest_obj: Dict[str, Any] = {}
            if mp.exists() and mp.is_file():
                try:
                    manifest_obj = json.loads(mp.read_text(encoding="utf-8", errors="ignore"))
                except Exception:
                    manifest_obj = {}
            primary_assets = manifest_obj.get("primary_assets") if isinstance(manifest_obj, dict) else None
            primary_assets = primary_assets if isinstance(primary_assets, dict) else {}
            factor_rel = primary_assets.get("factor_entry_relpath")
            model_rel = primary_assets.get("model_weight_relpath")
            factor_abs = (d / str(factor_rel)).resolve() if factor_rel else None
            model_abs = (d / str(model_rel)).resolve() if model_rel else None
            factor_ok = bool(factor_abs and factor_abs.exists() and factor_abs.is_file())
            model_ok = bool(model_abs and model_abs.exists() and model_abs.is_file())
            rec["primary_assets"] = {"factor_entry_relpath": factor_rel, "model_weight_relpath": model_rel}
            rec["factor_entry"] = {"path": str(factor_abs) if factor_abs else None, "exists": factor_ok}
            rec["model_weight"] = {"path": str(model_abs) if model_abs else None, "exists": model_ok}
            rec["ok"] = bool(factor_ok and model_ok)
            if rec["ok"]:
                ok_cnt += 1
            out.append(rec)
        return {"ok": True, "count": len(out), "ok_count": ok_cnt, "items": out}

    def enable_for_selection(self, *, task_id: str, operator: str = "ui") -> JsonDict:
        tid = str(task_id).strip()
        if not tid:
            return {"ok": False, "error": "task_id 为空"}
        sql = """
            UPDATE aistock_task_catalog
            SET
                is_enabled_for_selection = TRUE,
                enabled_for_selection_at_utc = NOW(),
                enabled_for_selection_by = %s,
                updated_at_utc = NOW()
            WHERE task_id = %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (operator, tid))
        return {"ok": True, "task_id": tid, "is_enabled_for_selection": True}

    def disable_for_selection(self, *, task_id: str, operator: str = "ui") -> JsonDict:
        tid = str(task_id).strip()
        if not tid:
            return {"ok": False, "error": "task_id 为空"}
        sql = """
            UPDATE aistock_task_catalog
            SET
                is_enabled_for_selection = FALSE,
                updated_at_utc = NOW()
            WHERE task_id = %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tid,))
        return {"ok": True, "task_id": tid, "is_enabled_for_selection": False}

    def list_sync_candidates(self, *, limit: int = 50, offset: int = 0, node_id: str | None = None) -> JsonDict:
        """分页获取task同步候选。

        全部从DB分页查询（快速响应）。API 扫描由 scan_and_sync_tasks 单独触发。
        """
        return self._query_candidates_from_db(limit=limit, offset=offset, node_id=node_id)

    def _query_candidates_from_db(self, *, limit: int, offset: int, node_id: str | None = None) -> JsonDict:
        """从DB缓存分页查询task候选（用于offset>0的快速翻页）。"""
        import time as _time
        _t_start = _time.monotonic()

        # node_id → display_name 映射
        node_display_names: dict[str, str] = {}
        try:
            with get_conn() as _conn_dn:
                with _conn_dn.cursor() as _cur_dn:
                    _cur_dn.execute("SELECT node_id, display_name FROM infra.compute_nodes")
                    for _row_dn in _cur_dn.fetchall():
                        node_display_names[_row_dn[0]] = _row_dn[1]
        except Exception:
            pass

        # SOTA Loop 摘要
        sota_loop_summary: dict[str, dict] = {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT task_id, COUNT(*) as cnt
                        FROM rdagent.rdagent_candidate_loops
                        WHERE is_sota = TRUE
                        GROUP BY task_id
                    """)
                    for row in cur.fetchall():
                        sota_loop_summary[row[0]] = {"sota_loops_count": row[1]}
        except Exception:
            pass

        # 已同步 task ID 集合（来自 aistock_task_catalog）
        synced_ids: set[str] = set()
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT task_id FROM aistock_task_catalog")
                    synced_ids = {r[0] for r in cur.fetchall()}
        except Exception:
            pass

        # 构建查询条件
        where_parts = ["dir_exists IS NOT FALSE"]
        params: list = []
        if node_id:
            where_parts.append("log_dir LIKE %s")
            params.append(f"{node_id}:%")

        where_sql = "WHERE " + " AND ".join(where_parts)

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 总数
                    cur.execute(f"SELECT COUNT(*) FROM rdagent.rdagent_candidate_tasks {where_sql}", params)
                    total_count = cur.fetchone()[0]

                    # 分页数据
                    cur.execute(f"""
                        SELECT task_id, log_dir, has_sota, sota_factors_count, sota_checked_at,
                               alpha_factors_count, model_feature_count, is_aligned, v2_checked_at,
                               sota_factors_list, alpha_factors_list, hist_len,
                               is_synced, task_status, updated_at, discovered_at
                        FROM rdagent.rdagent_candidate_tasks
                        {where_sql}
                        ORDER BY task_id DESC
                        LIMIT %s OFFSET %s
                    """, params + [limit, offset])

                    items = []
                    for row in cur.fetchall():
                        (tid, log_dir, has_sota, sota_count, checked_at,
                         alpha_count, model_feat, aligned, v2_at,
                         sota_list, alpha_list, hist_len_val,
                         is_synced, task_status, updated_at, discovered_at) = row

                        # 从log_dir提取node_id
                        _nid = ""
                        if log_dir and ":" in log_dir:
                            _part = log_dir.split(":", 1)[0]
                            if not (len(_part) == 1 and _part.isalpha()):
                                _nid = _part

                        # 构建discovery
                        discovery = None
                        if v2_at:
                            discovery = {
                                "has_sota": has_sota,
                                "sota_factors_count": sota_count or 0,
                                "sota_checked_at_utc": checked_at.isoformat() if checked_at else None,
                                "v2_alignment": {
                                    "sota_factors_count": sota_count or 0,
                                    "alpha_factors_count": alpha_count or 0,
                                    "model_feature_count": model_feat,
                                    "is_aligned": aligned,
                                    "v2_checked_at": v2_at.isoformat() if v2_at else None,
                                    "hist_len": hist_len_val or 0,
                                },
                            }
                            sl = sota_loop_summary.get(tid)
                            if sl:
                                discovery["sota_loop_summary"] = sl

                        items.append({
                            "task_id": tid,
                            "node_id": _nid,
                            "node_display_name": node_display_names.get(_nid, _nid) if _nid else "",
                            "last_modified": updated_at.isoformat() if updated_at else (discovered_at.isoformat() if discovered_at else None),
                            "is_synced": tid in synced_ids,
                            "discovery": discovery,
                            "latest": {
                                "task_status": task_status,
                                "updated_at_utc": updated_at.isoformat() if updated_at else None,
                            } if updated_at else None,
                        })

            logger.info(f"[TIMING] _query_candidates_from_db 耗时: {_time.monotonic() - _t_start:.2f}s, "
                        f"total={total_count}, page={len(items)}, offset={offset}")
            return {"ok": True, "items": items, "total_count": total_count, "source": "db"}
        except Exception as e:
            logger.error(f"DB分页查询失败: {e}")
            return {"ok": False, "error": str(e), "items": [], "total_count": 0}

    def get_task_summary(self, *, task_id: str) -> JsonDict:
        """获取 task 的基本概要，包含 V2 对齐验证信息。

        优先使用 RD-Agent V2 对齐预览 API 获取准确的 SOTA 因子数、Alpha 基线因子数、
        模型特征数及对齐验证结果。回退到旧 summary API 或本地扫描。
        """
        tid = str(task_id).strip()

        # 1. 优先调用 V2 对齐预览 API（包含完整的因子/模型/对齐信息）
        v2_preview = None
        try:
            v2_resp = _rdagent_client.get_v2_alignment_preview(tid)
            if v2_resp and v2_resp.get("ok"):
                v2_preview = v2_resp
                logger.info(f"[{tid}] V2对齐预览: SOTA={v2_resp.get('sota_factors_count')}, "
                            f"Alpha={v2_resp.get('alpha_factors_count')}, "
                            f"模型特征={v2_resp.get('model_feature_count')}, "
                            f"对齐={v2_resp.get('is_aligned')}")
        except Exception as e:
            logger.warning(f"[{tid}] V2对齐预览API调用失败: {e}")

        # 2. 尝试旧 summary API 获取基础信息
        base_summary = None
        try:
            api_resp = _rdagent_client.get_task_summary(task_id=tid)
            if api_resp and api_resp.get("ok"):
                base_summary = api_resp
        except Exception as e:
            logger.warning(f"[{tid}] 旧summary API调用失败: {e}")

        # 3. 如果 V2 预览成功，合并到返回结果
        if v2_preview is not None:
            summary = {
                "task_id": tid,
                "has_session": True,
                "has_sota": v2_preview.get("sota_factors_count", 0) > 0,
                "hist_len": v2_preview.get("hist_len"),
                "v2_alignment": {
                    "sota_factors": v2_preview.get("sota_factors", []),
                    "sota_factors_count": v2_preview.get("sota_factors_count", 0),
                    "sota_source": v2_preview.get("sota_source"),
                    "alpha_factors": v2_preview.get("alpha_factors", []),
                    "alpha_factors_count": v2_preview.get("alpha_factors_count", 0),
                    "alpha_source": v2_preview.get("alpha_source"),
                    "model_feature_count": v2_preview.get("model_feature_count"),
                    "model_source": v2_preview.get("model_source"),
                    "expected_total_features": v2_preview.get("expected_total_features"),
                    "is_aligned": v2_preview.get("is_aligned", False),
                    "factor_order": v2_preview.get("factor_order", []),
                    "has_model_weight": v2_preview.get("has_model_weight", False),
                    "has_model_meta": v2_preview.get("has_model_meta", False),
                },
            }
            # 合并旧 summary 的额外字段（如 log_dir 等）
            if base_summary and isinstance(base_summary.get("summary"), dict):
                for k, v in base_summary["summary"].items():
                    if k not in summary:
                        summary[k] = v
            return {"ok": True, "summary": summary}

        # 4. 如果 V2 不可用但旧 API 可用，直接返回旧结果
        if base_summary is not None:
            return base_summary

        # 5. V2 和旧 API 均不可用，返回错误（不再回退到本地文件扫描）
        return {"ok": False, "error": f"V2对齐预览API和旧summary API均不可用，无法获取task {tid} 的概要信息"}

    def get_task_loops(self, *, task_id: str) -> JsonDict:
        """获取 task 的所有 LOOP 详细信息，包括SOTA标记和目录状态。"""
        tid = str(task_id).strip()
        if not tid:
            return {"ok": False, "error": "task_id 为空", "loops": []}
        
        try:
            # LOOP数据提取需要较长时间（约30秒），直接使用requests调用，避免探测超时
            import requests
            
            # 构造完整URL，使用已知的RD-Agent API地址
            base_url = _rdagent_client.base_url.rstrip("/")
            url = f"{base_url}/tasks/{tid}/loops"
            
            # 使用较长的超时时间（300秒/5分钟）
            resp = requests.get(url, timeout=300.0)
            resp.raise_for_status()
            api_resp = resp.json()
            
            if api_resp and isinstance(api_resp, dict):
                # 直接使用 API 返回的每个 loop 的 is_sota 字段
                # 不再从 sota_factor_anchor 推测，避免错误标记非 SOTA loop
                loops = api_resp.get('loops', [])
                for loop in loops:
                    # 确保 is_sota 字段存在，若 API 未返回则默认 False
                    if 'is_sota' not in loop:
                        loop['is_sota'] = False

                return api_resp
            return {"ok": False, "error": "API 返回格式错误", "loops": []}
        except Exception as e:
            logger.error(f"获取 task {tid} LOOP 详情失败: {e}")
            return {"ok": False, "error": str(e), "loops": []}

    def sync_task_complete_assets(self, *, task_id: str, operator: str = "ui") -> TaskSyncResult:
        """使用新的complete_assets API端点同步任务资产
        
        这是简化版的同步方法，直接调用/tasks/{task_id}/complete_assets API，
        一次性获取所有TASK资产（SOTA因子、因子代码、模型权重、特征序列）。
        
        相比sync_task_from_log，这个方法：
        1. 使用统一的API端点，减少API调用次数
        2. 数据一致性由TaskAssetsExtractor保证
        3. 代码更简洁，易于维护
        """
        tid = str(task_id).strip()
        task_dir = self.assets_root / tid
        manifest_path = task_dir / "manifest.json"

        _upsert_task_catalog(
            tid,
            {
                "task_dir": str(task_dir),
                "manifest_path": str(manifest_path),
                "sync_status": "syncing",
                "sync_error": None,
                "sync_diagnostics": None,
                "updated_at_utc": datetime.utcnow(),
            },
        )

        diagnostics: JsonDict = {"mode": "complete_assets_api", "errors": [], "warnings": []}

        try:
            task_dir.mkdir(parents=True, exist_ok=True)

            # 调用新的complete_assets API
            logger.info(f"[{tid}] 调用complete_assets API...")
            try:
                import requests
                base_url = _rdagent_client.base_url.rstrip("/")
                url = f"{base_url}/tasks/{tid}/complete_assets"
                
                resp = requests.get(url, timeout=300.0)
                resp.raise_for_status()
                assets_data = resp.json()
                
                if not assets_data or not assets_data.get("ok"):
                    error_msg = assets_data.get("error") if assets_data else "API返回空"
                    raise RuntimeError(f"complete_assets API失败: {error_msg}")
                
                diagnostics["api_response"] = {
                    "task_id": assets_data.get("task_id"),
                    "sota_factors_count": assets_data.get("sota_factors", {}).get("count", 0),
                    "factor_codes_count": len(assets_data.get("factor_codes", [])),
                    "model_weight_found": assets_data.get("model_weight", {}).get("found", False),
                    "feature_sequence_total": assets_data.get("feature_sequence", {}).get("total_count", 0),
                    "validation_ok": assets_data.get("validation", {}).get("all_ok", False),
                }
                
            except Exception as e:
                raise RuntimeError(f"调用complete_assets API失败: {e}")

            # 保存模型权重
            model_weight = assets_data.get("model_weight", {})
            if model_weight.get("found"):
                logger.info(f"[{tid}] 下载模型权重...")
                try:
                    # 使用API下载模型权重，而不是直接读取文件
                    weight_key = model_weight.get("key") or model_weight.get("file_path")
                    if weight_key:
                        weight_bytes = _rdagent_client.download_task_asset_bytes(tid, weight_key)
                        if weight_bytes:
                            weight_path_dst = task_dir / "model.pkl"
                            weight_path_dst.write_bytes(weight_bytes)
                            diagnostics["model_weight"] = {
                                "key": weight_key,
                                "path": "model.pkl",
                                "size": len(weight_bytes),
                                "source": model_weight.get("source"),
                            }
                            logger.info(f"[{tid}] 模型权重下载成功: {len(weight_bytes)} bytes")
                        else:
                            diagnostics["warnings"].append(f"模型权重API返回空: {weight_key}")
                    else:
                        diagnostics["warnings"].append("未找到模型权重key")
                except Exception as e:
                    diagnostics["warnings"].append(f"模型权重下载失败: {e}")
                    logger.warning(f"[{tid}] 模型权重下载失败: {e}")
            else:
                diagnostics["warnings"].append("未找到模型权重")
            
            # 注意：不复制static_factors.parquet文件
            # 推理引擎会从实盘数据库实时获取静态因子数据（fetch_fundamental_data_ts）
            # 这确保了使用的是最新的实盘数据，而不是历史回测数据

            # 提取Alpha基线因子
            feature_sequence = assets_data.get("feature_sequence", {})
            alpha_baseline_factors = feature_sequence.get("alpha_baseline", [])
            alpha_source = "complete_assets_api/feature_sequence/alpha_baseline"
            
            diagnostics["alpha_baseline_factors"] = {
                "count": len(alpha_baseline_factors),
                "source": alpha_source,
                "factors": alpha_baseline_factors,
            }

            # 保存因子代码
            factor_codes = assets_data.get("factor_codes", [])
            logger.info(f"[{tid}] 保存{len(factor_codes)}个因子代码...")
            
            # 生成合并的factor.py文件（推理引擎需要）
            factor_py_lines = []
            factor_py_lines.append("# 自动生成的因子文件")
            factor_py_lines.append("# 包含所有SOTA因子的计算函数")
            factor_py_lines.append("")
            factor_py_lines.append("import pandas as pd")
            factor_py_lines.append("import numpy as np")
            factor_py_lines.append("")
            
            factor_names = []
            for i, code_info in enumerate(factor_codes):
                factor_name = code_info.get("factor_name", f"factor_{i}")
                code_content = code_info.get("code", "")
                
                if code_content:
                    factor_names.append(factor_name)
                    # 保存单独的因子文件
                    code_file = task_dir / f"factor_{factor_name}.py"
                    code_file.write_text(code_content, encoding="utf-8")
                    
                    # 添加到合并文件（移除if __name__ == "__main__"部分）
                    lines = code_content.split('\n')
                    filtered_lines = []
                    skip_main = False
                    for line in lines:
                        if 'if __name__ ==' in line:
                            skip_main = True
                        if not skip_main:
                            filtered_lines.append(line)
                    
                    factor_py_lines.append(f"# {factor_name}")
                    factor_py_lines.append('\n'.join(filtered_lines))
                    factor_py_lines.append("")
            
            # 添加统一的compute函数（推理引擎需要）
            factor_py_lines.append("# 统一的compute函数，供推理引擎调用")
            factor_py_lines.append("def compute(df: pd.DataFrame) -> pd.DataFrame:")
            factor_py_lines.append('    """')
            factor_py_lines.append("    统一的因子计算入口函数")
            factor_py_lines.append("    参数: df - 包含价量数据的DataFrame，索引为MultiIndex(datetime, instrument)")
            factor_py_lines.append("    返回: 包含所有因子值的DataFrame")
            factor_py_lines.append('    """')
            factor_py_lines.append("    result = pd.DataFrame(index=df.index)")
            factor_py_lines.append("    ")
            factor_py_lines.append("    # 逐个计算每个因子")
            for fname in factor_names:
                factor_py_lines.append(f"    # 计算 {fname}")
                factor_py_lines.append(f"    try:")
                factor_py_lines.append(f"        # 临时保存数据供因子函数读取")
                factor_py_lines.append(f"        df.to_hdf('daily_pv.h5', key='data', mode='w')")
                factor_py_lines.append(f"        calculate_{fname}()")
                factor_py_lines.append(f"        factor_df = pd.read_hdf('result.h5', key='data')")
                factor_py_lines.append(f"        result['{fname}'] = factor_df['{fname}']")
                factor_py_lines.append(f"    except Exception as e:")
                factor_py_lines.append(f"        print(f'计算因子 {fname} 失败: {{e}}')")
                factor_py_lines.append(f"        result['{fname}'] = np.nan")
                factor_py_lines.append("    ")
            factor_py_lines.append("    return result")
            factor_py_lines.append("")
            
            # 保存合并的factor.py文件
            factor_py_path = task_dir / "factor.py"
            factor_py_path.write_text("\n".join(factor_py_lines), encoding="utf-8")
            
            diagnostics["factor_codes"] = {
                "count": len(factor_codes),
                "saved": len([c for c in factor_codes if c.get("code")]),
                "factor_py": "factor.py",
            }

            # 生成factor_order.json
            sota_factors = assets_data.get("sota_factors", {}).get("factors", [])
            dynamic_factors = feature_sequence.get("dynamic_factors", [])
            
            # 处理动态因子的tuple格式
            processed_dynamic_factors = []
            for df in dynamic_factors:
                if isinstance(df, str) and df.startswith("('feature',"):
                    # 从"('feature', 'factor_name')"中提取因子名
                    import ast
                    try:
                        parsed = ast.literal_eval(df)
                        if isinstance(parsed, tuple) and len(parsed) >= 2:
                            processed_dynamic_factors.append(parsed[1])
                        else:
                            processed_dynamic_factors.append(df)
                    except:
                        processed_dynamic_factors.append(df)
                else:
                    processed_dynamic_factors.append(df)
            
            # 生成完整的因子顺序列表（推理引擎需要）
            complete_factor_order = alpha_baseline_factors + processed_dynamic_factors
            
            factor_order = {
                "factor_order": complete_factor_order,  # 推理引擎需要这个字段
                "alpha158_factors": alpha_baseline_factors,  # 推理引擎期望这个字段名
                "dynamic_factors": processed_dynamic_factors,
                "total_feature_count": len(complete_factor_order),
                "generated_at": _utc_now_iso(),
                "source": "complete_assets_api",
            }
            
            factor_order_path = task_dir / "factor_order.json"
            factor_order_path.write_text(json.dumps(factor_order, indent=2, ensure_ascii=False), encoding="utf-8")
            
            diagnostics["factor_order"] = {
                "path": "factor_order.json",
                "alpha_count": len(alpha_baseline_factors),
                "dynamic_count": len(processed_dynamic_factors),
                "total_count": factor_order["total_feature_count"],
            }

            # 生成manifest.json
            # 添加选股功能所需的task_run_id和loop_id字段
            session_info = assets_data.get("session_info", {})
            last_sota_loop_id = session_info.get("last_sota_factor_loop_id")
            
            manifest = {
                "task_id": tid,
                "task_run_id": tid,  # 使用task_id作为task_run_id
                "loop_id": last_sota_loop_id if last_sota_loop_id is not None else 0,
                "sync_mode": "complete_assets_api",
                "synced_at": _utc_now_iso(),
                "operator": operator,
                "primary_assets": {
                    "factor_entry_relpath": "factor.py",
                    "model_weight_relpath": "model.pkl",
                    "factor_order_relpath": "factor_order.json",
                },
                "assets": {
                    "model_weight": model_weight.get("found", False),
                    "factor_codes": len(factor_codes),
                    "factor_order": "factor_order.json",  # 必须是文件路径字符串，不是布尔值
                    "alpha_baseline_factors": len(alpha_baseline_factors),
                    "dynamic_factors": len(processed_dynamic_factors),
                },
                "validation": assets_data.get("validation", {}),
                "session_info": {
                    "last_sota_factor_loop_id": last_sota_loop_id,
                    "session_loop_id": session_info.get("session_loop_id"),
                },
            }
            
            manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

            # 更新catalog
            _upsert_task_catalog(
                tid,
                {
                    "sync_status": "success",
                    "sync_error": None,
                    "sync_diagnostics": diagnostics,
                    "updated_at_utc": datetime.utcnow(),
                    "is_enabled_for_selection": True,
                },
            )

            logger.info(f"[{tid}] 同步成功（complete_assets模式）")
            return TaskSyncResult(
                ok=True,
                task_id=tid,
                sync_status="success",
                task_dir=str(task_dir),
                manifest_path=str(manifest_path),
                diagnostics=diagnostics,
            )

        except Exception as e:
            error_msg = str(e)
            diagnostics["errors"].append(error_msg)
            logger.error(f"[{tid}] 同步失败: {error_msg}")

            _upsert_task_catalog(
                tid,
                {
                    "sync_status": "failed",
                    "sync_error": error_msg,
                    "sync_diagnostics": diagnostics,
                    "updated_at_utc": datetime.utcnow(),
                },
            )

            return TaskSyncResult(
                ok=False,
                task_id=tid,
                sync_status="failed",
                task_dir=str(task_dir),
                manifest_path=str(manifest_path),
                error=error_msg,
                diagnostics=diagnostics,
            )

    def sync_all_tasks_from_api(
        self,
        operator: str = "system",
        limit: Optional[int] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        通过 RD-Agent API 获取所有task列表并批量同步（API-only，不直接访问本地目录）
        
        Args:
            operator: 操作者标识
            limit: 限制同步的task数量，None表示不限制
            force: 是否强制重新同步已存在的task
            
        Returns:
            同步结果统计
        """
        # 通过 API 获取 task 列表
        try:
            api_resp = _rdagent_client.get_tasks_latest(limit=limit or 200)
        except Exception as e:
            return {
                "ok": False,
                "error": f"RD-Agent API 获取task列表失败: {e}",
                "total": 0,
                "success": 0,
                "results": [],
            }

        candidate_list = []
        if isinstance(api_resp, list):
            candidate_list = api_resp
        elif isinstance(api_resp, dict):
            for k in ("items", "tasks", "data"):
                if isinstance(api_resp.get(k), list):
                    candidate_list = api_resp[k]
                    break

        task_ids = [t.get("task_id") for t in candidate_list if t.get("task_id")]
        if limit:
            task_ids = task_ids[:limit]

        logger.info(f"sync_all_tasks_from_api: 从API获取到{len(task_ids)}个task，准备同步")

        # 逐个同步
        results = []
        ok_cnt = 0

        for tid in task_ids:
            r = self.sync_task_from_log(task_id=tid, operator=operator)
            rr = {
                "task_id": r.task_id,
                "ok": bool(r.ok),
                "sync_status": r.sync_status,
                "task_dir": r.task_dir,
                "manifest_path": r.manifest_path,
                "error": r.error,
                "diagnostics": r.diagnostics,
            }
            if rr["ok"]:
                ok_cnt += 1
            results.append(rr)

        return {"ok": True, "total": len(task_ids), "success": ok_cnt, "results": results}

rdagent_task_sync_service = RDAgentTaskSyncService()
