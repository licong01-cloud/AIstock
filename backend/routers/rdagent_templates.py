from __future__ import annotations

import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..infra.deepseek_client import DeepSeekClient
from ..db.pg_pool import get_conn

# 配置日志
logger = logging.getLogger("aistock.rdagent_templates")

router = APIRouter(prefix="/rdagent/templates", tags=["rdagent-templates"])

ACTIVE_TEMPLATE_FILE = "rdagent_active_template.json"


# ---------------------------------------------------------------------------
# RDAgent Template API Client
# ---------------------------------------------------------------------------

def _rdagent_scheduler_base_url() -> str:
    """RDAgent scheduler API base URL (mounted at /scheduler on results_api_server)."""
    base = (os.getenv("RDAGENT_RESULTS_API_BASE_URL") or "").strip().rstrip("/")
    if not base:
        base = "http://127.0.0.1:9000"
    return f"{base}/scheduler"


def _rdagent_tpl_api(method: str, path: str, *,
                     params: Dict[str, Any] | None = None,
                     json_body: Dict[str, Any] | None = None,
                     timeout: float = 30.0) -> Dict[str, Any]:
    """Call RDAgent scheduler template API and return JSON response."""
    url = f"{_rdagent_scheduler_base_url()}{path}"
    try:
        if method.upper() == "GET":
            resp = requests.get(url, params=params, timeout=timeout)
        elif method.upper() == "POST":
            resp = requests.post(url, json=json_body or {}, params=params, timeout=timeout)
        elif method.upper() == "DELETE":
            resp = requests.delete(url, params=params, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        logger.error(f"RDAgent scheduler 连接失败: {url}")
        raise HTTPException(status_code=503, detail=f"RDAgent scheduler ({url}) 无法连接")
    except requests.exceptions.Timeout:
        logger.error(f"RDAgent scheduler 请求超时: {url}")
        raise HTTPException(status_code=504, detail="RDAgent scheduler 请求超时")
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 500
        detail = ""
        try:
            detail = e.response.json().get("detail", str(e)) if e.response is not None else str(e)
        except Exception:
            detail = str(e)
        raise HTTPException(status_code=status, detail=detail)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"RDAgent scheduler 未知错误: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TemplateFileSaveRequest(BaseModel):
    content: str


class TemplateFileValidateRequest(BaseModel):
    path: str
    content: str


class TemplatePromptAuditRequest(BaseModel):
    path: str
    content: str


class TemplatePublishFile(BaseModel):
    path: str
    content: str


class TemplatePublishRequest(BaseModel):
    scenario: str
    version: str
    task_id: Optional[str] = None
    description: Optional[str] = None
    base_version: Optional[str] = None
    changed_files: Optional[List[str]] = None
    files: List[TemplatePublishFile]


# ---------------------------------------------------------------------------
# AIstock local helpers (active state + DB registry)
# ---------------------------------------------------------------------------

def _get_backend_data_root() -> Path:
    return Path(__file__).resolve().parents[1] / "data"


def _get_active_state_path() -> Path:
    return _get_backend_data_root() / ACTIVE_TEMPLATE_FILE


def _load_active_state() -> Optional[Dict[str, str]]:
    path = _get_active_state_path()
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    scenario = str(data.get("scenario") or "").strip()
    version = str(data.get("version") or "").strip()
    if not scenario or not version:
        return None
    return {"scenario": scenario, "version": version}


def _save_active_state(scenario: str, version: str) -> None:
    data_dir = _get_backend_data_root()
    data_dir.mkdir(parents=True, exist_ok=True)
    path = _get_active_state_path()
    payload = {"scenario": scenario, "version": version, "updated_at": datetime.now().isoformat()}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _ensure_default_active(items: List[Dict[str, Any]]) -> Optional[Dict[str, str]]:
    if not items:
        return None
    preferred = next((item for item in items if item.get("version") == "v0" and item.get("scenario") == "all"), None)
    if not preferred:
        preferred = next((item for item in items if item.get("version") == "v0"), None)
    if not preferred:
        return None
    scenario = str(preferred.get("scenario") or "").strip()
    version = str(preferred.get("version") or "").strip()
    if not scenario or not version:
        return None
    _save_active_state(scenario, version)
    return {"scenario": scenario, "version": version}


def _ensure_template_table() -> None:
    sql = """
        CREATE TABLE IF NOT EXISTS aistock_template_registry (
            id BIGSERIAL PRIMARY KEY,
            scenario TEXT NOT NULL,
            version TEXT NOT NULL,
            created_at TEXT,
            description TEXT,
            base_version TEXT,
            changed_files JSONB,
            files_count INTEGER,
            manifest_path TEXT,
            manifest_hash TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (scenario, version)
        );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def _upsert_template_record(item: Dict[str, Any]) -> None:
    _ensure_template_table()
    sql = """
        INSERT INTO aistock_template_registry (
            scenario,
            version,
            created_at,
            description,
            base_version,
            changed_files,
            files_count,
            manifest_path,
            manifest_hash,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (scenario, version) DO UPDATE SET
            created_at = EXCLUDED.created_at,
            description = EXCLUDED.description,
            base_version = EXCLUDED.base_version,
            changed_files = EXCLUDED.changed_files,
            files_count = EXCLUDED.files_count,
            manifest_path = EXCLUDED.manifest_path,
            manifest_hash = EXCLUDED.manifest_hash,
            updated_at = NOW()
    """
    changed_files = item.get("changed_files")
    payload = (
        item.get("scenario"),
        item.get("version"),
        item.get("created_at"),
        item.get("description"),
        item.get("base_version"),
        json.dumps(changed_files, ensure_ascii=False) if changed_files is not None else None,
        item.get("files_count"),
        item.get("manifest_path"),
        item.get("manifest_hash"),
    )
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)


def _delete_template_record(scenario: str, version: str) -> None:
    _ensure_template_table()
    sql = "DELETE FROM aistock_template_registry WHERE scenario = %s AND version = %s"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (scenario, version))


# ---------------------------------------------------------------------------
# Pure-logic helpers (kept locally – no file I/O on RDAgent side)
# ---------------------------------------------------------------------------

def _validate_content(rel_path: str, content: str) -> None:
    suffix = Path(rel_path).suffix
    if "{{" in content or "{%" in content:
        return
    try:
        if suffix in {".yaml", ".yml"}:
            yaml.safe_load(content)
        elif suffix == ".json":
            json.loads(content)
    except Exception as exc:
        raise ValueError(f"Invalid {suffix} content") from exc


def _build_prompt_audit_messages(path: str, content: str) -> List[Dict[str, str]]:
    lower_path = path.lower()
    scenario_hint = "通用"
    if "kaggle" in lower_path:
        scenario_hint = "kaggle"
    elif "qlib" in lower_path:
        scenario_hint = "qlib"
    elif "general_model" in lower_path:
        scenario_hint = "general_model"

    analysis_focus = "保持逻辑一致性、角色清晰、流程连贯"
    if "experiment" in lower_path and "qlib" in lower_path:
        analysis_focus = "确保实验步骤、数据处理与评估指标一致"
    elif "factor" in lower_path:
        analysis_focus = "保证因子生成步骤、评测指标与输出格式一致"
    elif "kaggle" in lower_path:
        analysis_focus = "保证比赛目标、指标与输出建议一致"

    system_prompt = (
        "你是一名RD-Agent/QLib提示词审阅专家，需要检查提示词是否存在前后矛盾、"
        "缺失关键约束、输出格式不一致或与业务逻辑冲突。"
    )
    user_prompt = f"""
请审阅以下提示词文件，给出一致性与逻辑性检查结论与修正建议：

【文件路径】{path}
【场景】{scenario_hint}
【重点检查】{analysis_focus}

检查要求：
1. 是否有前后矛盾的指令或目标。
2. 角色、输入输出是否定义清晰，是否缺少关键约束。
3. 是否存在与业务逻辑或流程步骤冲突的内容。
4. 输出格式是否与上文定义一致。
5. 给出具体修改建议，按"问题->建议"格式输出。

提示词内容：
"""
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt + content},
    ]


# ============================================================================
# API Routes – delegate to RDAgent scheduler template APIs
# ============================================================================

@router.get("", summary="模板列表")
def list_templates(scenario: Optional[str] = Query(None)) -> Dict[str, Any]:
    """通过 RDAgent API 获取模板列表，并同步到本地 DB 和 active state。"""
    params: Dict[str, Any] = {}
    if scenario:
        params["scenario"] = scenario
    result = _rdagent_tpl_api("GET", "/templates", params=params)
    items = result.get("items", [])

    # 同步到本地 DB
    for item in items:
        _upsert_template_record(item)

    # active state 管理（AIstock 本地）
    active_state = _load_active_state()
    if not active_state:
        active_state = _ensure_default_active(items)
    elif not any(
        item.get("scenario") == active_state.get("scenario")
        and item.get("version") == active_state.get("version")
        for item in items
    ):
        active_state = _ensure_default_active(items)

    for item in items:
        item["is_active"] = bool(
            active_state
            and item.get("scenario") == active_state.get("scenario")
            and item.get("version") == active_state.get("version")
        )

    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return {"items": items}


@router.post("/publish", summary="发布模板")
def publish_template(req: TemplatePublishRequest) -> Dict[str, Any]:
    """通过 RDAgent API 发布模板。"""
    scenario = str(req.scenario or "").strip()
    version = str(req.version or "").strip()
    if not scenario or not version:
        raise HTTPException(status_code=422, detail="scenario/version 不能为空")
    if version == "v0":
        raise HTTPException(status_code=403, detail="v0 template is read-only")
    if not req.files:
        raise HTTPException(status_code=422, detail="files 不能为空")

    # 本地语法校验
    for item in req.files:
        _validate_content(item.path, item.content)

    # 调用 RDAgent API 发布
    payload = {
        "scenario": scenario,
        "version": version,
        "task_id": req.task_id,
        "description": req.description,
        "base_version": req.base_version,
        "changed_files": req.changed_files or [],
        "files": [{"path": f.path, "content": f.content} for f in req.files],
    }
    result = _rdagent_tpl_api("POST", "/templates/publish", json_body=payload)

    # 同步到本地 DB
    record = {
        "scenario": scenario,
        "version": version,
        "created_at": datetime.now().isoformat(),
        "description": req.description,
        "base_version": req.base_version,
        "changed_files": req.changed_files or [],
        "files_count": len(req.files),
        "manifest_path": result.get("manifest_path", ""),
        "manifest_hash": result.get("manifest_hash", ""),
    }
    _upsert_template_record(record)

    return {"ok": True, "scenario": scenario, "version": version, "manifest_hash": result.get("manifest_hash", "")}


@router.post("/{scenario}/{version}/activate", summary="激活模板")
def activate_template(scenario: str, version: str) -> Dict[str, Any]:
    """激活模板（仅更新 AIstock 本地 active state，通过 API 验证模板存在）。"""
    # 先验证模板存在（通过 RDAgent API 获取文件列表）
    _rdagent_tpl_api("GET", f"/templates/{scenario}/{version}/files")
    _save_active_state(scenario, version)
    return {"ok": True, "scenario": scenario, "version": version}


@router.get("/{scenario}/{version}/files", summary="模板文件列表")
def list_template_files(scenario: str, version: str) -> Dict[str, Any]:
    """通过 RDAgent API 获取模板文件列表。"""
    return _rdagent_tpl_api("GET", f"/templates/{scenario}/{version}/files")


@router.get("/{scenario}/{version}/file", summary="读取模板文件")
def get_template_file(scenario: str, version: str, path: str = Query("")) -> Dict[str, Any]:
    """通过 RDAgent API 读取模板文件内容。"""
    return _rdagent_tpl_api("GET", f"/templates/{scenario}/{version}/file", params={"path": path})


@router.post("/{scenario}/{version}/file", summary="保存模板文件")
def save_template_file(scenario: str, version: str, req: TemplateFileSaveRequest, path: str = Query("")) -> Dict[str, Any]:
    """通过 RDAgent API 保存模板文件。"""
    if version == "v0":
        raise HTTPException(status_code=403, detail="v0 template is read-only")

    # 本地语法校验
    _validate_content(path, req.content)

    return _rdagent_tpl_api("POST", f"/templates/{scenario}/{version}/file",
                            json_body={"path": path, "content": req.content})


@router.delete("/{scenario}/{version}", summary="删除模板")
def delete_template(scenario: str, version: str) -> Dict[str, Any]:
    """通过 RDAgent API 删除模板，并同步本地 DB 和 active state。"""
    sc = str(scenario or "").strip()
    ver = str(version or "").strip()
    if not sc or not ver:
        raise HTTPException(status_code=422, detail="scenario/version 不能为空")
    if ver == "v0":
        raise HTTPException(status_code=403, detail="v0 template is read-only")

    active_state = _load_active_state()

    # 调用 RDAgent API 删除
    _rdagent_tpl_api("DELETE", f"/templates/{sc}/{ver}")

    # 同步本地 DB
    _delete_template_record(sc, ver)

    # 如果删除的是当前激活模板，重新选择默认
    if active_state and active_state.get("scenario") == sc and active_state.get("version") == ver:
        result = _rdagent_tpl_api("GET", "/templates")
        items = result.get("items", [])
        _ensure_default_active(items)

    return {"ok": True, "scenario": sc, "version": ver}


@router.post("/validate", summary="模板文件语法校验")
def validate_template_file(req: TemplateFileValidateRequest) -> Dict[str, Any]:
    """纯本地逻辑，不需要调用 RDAgent API。"""
    _validate_content(req.path, req.content)
    return {"ok": True, "path": req.path}


@router.post("/prompt-audit", summary="提示词一致性检查")
def prompt_audit(req: TemplatePromptAuditRequest) -> Dict[str, Any]:
    """纯本地逻辑（调用 DeepSeek），不需要调用 RDAgent API。"""
    if not os.getenv("DEEPSEEK_API_KEY"):
        raise HTTPException(status_code=400, detail="DEEPSEEK_API_KEY 未配置，无法执行提示词检查")
    client = DeepSeekClient()
    messages = _build_prompt_audit_messages(req.path, req.content)
    result = client.call_api(messages, temperature=0.2, max_tokens=2000)
    return {"ok": True, "path": req.path, "result": result}


# ============================================================================
# 模板应用功能 – 全部委托给 RDAgent API
# ============================================================================

@router.post("/{scenario}/{version}/apply", summary="应用模板")
def apply_template(
    scenario: str,
    version: str,
    force: bool = False,
    backup: bool = True
) -> Dict[str, Any]:
    """通过 RDAgent API 应用模板到 RD-Agent 项目。"""
    result = _rdagent_tpl_api(
        "POST", f"/templates/{scenario}/{version}/apply",
        json_body={"force": force, "backup": backup},
        timeout=120.0,
    )

    # 应用成功后更新本地 active state
    if result.get("ok"):
        _save_active_state(scenario, version)

    return result


@router.post("/rollback", summary="回滚模板")
def rollback_template(backup_id: Optional[str] = None) -> Dict[str, Any]:
    """通过 RDAgent API 回滚到指定备份。"""
    if not backup_id:
        # 获取最近的备份
        backups_result = _rdagent_tpl_api("GET", "/templates/backups")
        backups = backups_result.get("items", [])
        if not backups:
            raise HTTPException(status_code=404, detail="没有可用的备份")
        backup_id = backups[0].get("backup_id")
        logger.info(f"使用最近的备份: {backup_id}")

    result = _rdagent_tpl_api(
        "POST", "/templates/backups/rollback",
        json_body={"backup_id": backup_id},
        timeout=120.0,
    )

    # 回滚成功后更新本地 active state
    if result.get("scenario") and result.get("version"):
        _save_active_state(result["scenario"], result["version"])

    return {"ok": True, **result}


@router.get("/sync-status", summary="同步状态检查")
def get_sync_status() -> Dict[str, Any]:
    """通过 RDAgent API 检查同步状态，并附加本地 active state 信息。"""
    active_state = _load_active_state()

    # 从 RDAgent 获取同步状态
    rdagent_status = _rdagent_tpl_api("GET", "/templates/sync-status")

    # 附加本地 active state 信息
    rdagent_status["local_active_template"] = active_state

    return rdagent_status


@router.post("/{scenario}/{version}/refresh-sha256", summary="更新SHA256验证值")
def refresh_template_sha256(scenario: str, version: str) -> Dict[str, Any]:
    """通过 RDAgent API 重新计算并更新模板的 SHA256 验证值。"""
    return _rdagent_tpl_api("POST", f"/templates/{scenario}/{version}/refresh-sha256")


@router.get("/backups", summary="备份列表")
def list_backups_api() -> Dict[str, Any]:
    """通过 RDAgent API 获取所有备份列表。"""
    return _rdagent_tpl_api("GET", "/templates/backups")


# ============================================================================
# P3: 通过 APP_TPL 环境变量切换模板（无需文件拷贝）
# ============================================================================

@router.post("/{scenario}/{version}/activate-env", summary="通过ENV激活模板")
def activate_template_env(scenario: str, version: str) -> Dict[str, Any]:
    """通过修改 .env 中的 APP_TPL 参数激活模板（仅影响 yaml/txt 模板）。

    同时更新 AIstock 本地 active state。
    """
    result = _rdagent_tpl_api("POST", f"/templates/{scenario}/{version}/activate-env")
    if result.get("ok"):
        _save_active_state(scenario, version)
    return result


@router.get("/active-env", summary="当前ENV激活的模板")
def get_active_env_template() -> Dict[str, Any]:
    """获取当前 .env 中 APP_TPL 指向的模板版本。"""
    rdagent_result = _rdagent_tpl_api("GET", "/templates/active-env")
    rdagent_result["local_active_template"] = _load_active_state()
    return rdagent_result
