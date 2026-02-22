"""
RD-Agent LLM Configuration Management API Router for AIstock.

This module provides API endpoints for managing RD-Agent LLM configurations.
It acts as a proxy to RD-Agent's LLM config API and manages local database records.
"""

import os
from typing import Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
from ..db.pg_pool import get_conn

# Import RD-Agent LLM Config API client
from ..services.rdagent_llm_config_client import get_llm_config_client

router = APIRouter(prefix="/rdagent/llm-config", tags=["rdagent-llm-config"])


def _find_provider_api_config(
    cursor: Any,
    provider_id: int,
    model_type: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """按优先级查找服务商可用API配置。

    优先级：
    1) 与 model_type 同名用途（chat/reasoner/embedding）
    2) chat
    3) default
    4) 任意激活配置（按优先级、更新时间）
    """
    purposes: list[str] = []
    if model_type in {"chat", "reasoner", "embedding"}:
        purposes.append(model_type)
    for fallback in ("chat", "default"):
        if fallback not in purposes:
            purposes.append(fallback)

    for purpose in purposes:
        cursor.execute(
            """
            SELECT id, api_base, api_key, env_api_base_name, env_api_key_name
            FROM aistock_llm_api_configs
            WHERE provider_id = %s
              AND config_purpose = %s
              AND is_active = true
            ORDER BY priority DESC, updated_at DESC, id DESC
            LIMIT 1
            """,
            (provider_id, purpose),
        )
        row = cursor.fetchone()
        if row:
            api_base = row[1]
            # 如果 api_base 为空，回退使用 provider 表中的 api_base_url
            if not api_base:
                cursor.execute("SELECT api_base_url FROM aistock_llm_providers WHERE id = %s", (provider_id,))
                p_row = cursor.fetchone()
                if p_row and p_row[0]:
                    api_base = p_row[0]
                    
            return {
                "id": row[0],
                "api_base": api_base,
                "api_key": row[2],
                "env_api_base_name": row[3],
                "env_api_key_name": row[4],
            }

    cursor.execute(
        """
        SELECT id, api_base, api_key, env_api_base_name, env_api_key_name
        FROM aistock_llm_api_configs
        WHERE provider_id = %s
          AND is_active = true
        ORDER BY priority DESC, updated_at DESC, id DESC
        LIMIT 1
        """,
        (provider_id,),
    )
    row = cursor.fetchone()
    if row:
        api_base = row[1]
        if not api_base:
            cursor.execute("SELECT api_base_url FROM aistock_llm_providers WHERE id = %s", (provider_id,))
            p_row = cursor.fetchone()
            if p_row and p_row[0]:
                api_base = p_row[0]
                
        return {
            "id": row[0],
            "api_base": api_base,
            "api_key": row[2],
            "env_api_base_name": row[3],
            "env_api_key_name": row[4],
        }
        
    # 如果数据库中没有，尝试从环境变量自动导入并保存
    cursor.execute("SELECT provider_name, default_env_prefix, api_base_url FROM aistock_llm_providers WHERE id = %s", (provider_id,))
    p_row = cursor.fetchone()
    if p_row:
        p_name, env_prefix, p_api_base_url = p_row
        prefix = (env_prefix or p_name).upper()
        import os
        env_key = os.environ.get(f"{prefix}_API_KEY")
        if env_key:
            # 优先用环境变量的API_BASE，其次用服务商自带的API_BASE
            env_base = os.environ.get(f"{prefix}_API_BASE", "")
            final_api_base = env_base or p_api_base_url or ""
            cursor.execute("""
                INSERT INTO aistock_llm_api_configs 
                (provider_id, api_base, api_key, env_api_base_name, env_api_key_name, config_purpose, description)
                VALUES (%s, %s, %s, %s, %s, 'default', '自动从环境变量导入')
                RETURNING id
            """, (provider_id, env_base, env_key, f"{prefix}_API_BASE", f"{prefix}_API_KEY"))
            new_id = cursor.fetchone()[0]
            return {
                "id": new_id,
                "api_base": final_api_base,
                "api_key": env_key,
                "env_api_base_name": f"{prefix}_API_BASE",
                "env_api_key_name": f"{prefix}_API_KEY",
            }
            
    return None


def _infer_model_litellm_prefix(
    provider_name: str,
    provider_prefix: Optional[str],
    model_id: str,
) -> str:
    """推断模型级 LiteLLM 前缀。"""
    model_lower = (model_id or "").lower()
    provider_lower = (provider_name or "").lower()
    default_prefix = (provider_prefix or provider_name or "").strip()

    if provider_lower in {"siliconflow", "silicon"}:
        if "deepseek" in model_lower:
            return "deepseek"
        if "qwen" in model_lower:
            return "dashscope"
        if "glm" in model_lower or "zai-org" in model_lower:
            return "openai"
    if provider_lower in {"openai", "deepseek", "dashscope", "qwen", "tongyi", "bailian"}:
        return provider_lower if provider_lower != "tongyi" else "dashscope"
    return default_prefix


async def verify_model_api(full_model_id: str, api_key: str, api_base: Optional[str] = None) -> dict:
    """验证模型API可用性"""
    try:
        import litellm
        
        # 测试调用，直接传递 api_key 和 api_base，避免依赖环境变量前缀猜测
        kwargs = {
            "model": full_model_id,
            "messages": [{"role": "user", "content": "Test"}],
            "max_tokens": 5,
            "timeout": 30,
            "api_key": api_key
        }
        if api_base:
            kwargs["api_base"] = api_base
            
        response = await litellm.acompletion(**kwargs)
        
        return {
            "success": True,
            "message": "验证通过",
            "response_model": response.model if hasattr(response, 'model') else full_model_id
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"验证失败: {str(e)}"
        }


# Pydantic模型
class ProviderCreate(BaseModel):
    provider_name: str
    display_name: str
    api_base_url: Optional[str] = None
    litellm_prefix: Optional[str] = None
    supports_chat: bool = True
    supports_embedding: bool = False
    supports_reasoner: bool = False


class ProviderUpdate(BaseModel):
    display_name: Optional[str] = None
    api_base_url: Optional[str] = None
    litellm_prefix: Optional[str] = None
    supports_chat: Optional[bool] = None
    supports_embedding: Optional[bool] = None
    supports_reasoner: Optional[bool] = None
    is_active: Optional[bool] = None


class ModelCreate(BaseModel):
    provider_id: int
    model_name: str
    display_name: str
    full_model_id: str
    model_type: str  # chat, reasoner, embedding
    model_category: Optional[str] = "general"  # 对话/Coding, 推理模型, 嵌入式模型
    description: Optional[str] = None  # ≤100字
    api_key: Optional[str] = None  # 用于验证
    api_base: Optional[str] = None  # 用于验证
    verify_on_add: bool = True  # 是否在添加时验证


class StageMappingUpdate(BaseModel):
    stage_name: str
    model_id: Optional[int] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None


class ConfigUpdate(BaseModel):
    stage_mappings: list[StageMappingUpdate]
    embedding_model_id: Optional[int] = None  # 全局单一embedding模型ID
    change_reason: str  # 必填


class ModelAPIConfigUpdate(BaseModel):
    """模型API配置更新"""
    model_id: int
    api_key: str
    api_base: Optional[str] = None
    verify_before_save: bool = True  # 保存前验证


class ModelVerifyRequest(BaseModel):
    """模型验证请求"""
    model_id: int
    api_key: Optional[str] = None  # 如果提供则用于验证，否则使用数据库中的配置
    api_base: Optional[str] = None
    run_health_check: bool = True  # 是否运行RDAgent健康检查
    run_litellm_test: bool = True  # 是否运行litellm测试

# RD-Agent API配置
RDAGENT_API_BASE = "http://127.0.0.1:9000"


async def comprehensive_model_verification(
    provider_name: str,
    model_name: str,
    full_model_id: str,
    api_key: str,
    api_base: Optional[str] = None,
    run_health_check: bool = True,
    run_litellm_test: bool = True
) -> dict:
    """综合模型验证：litellm + RDAgent健康检查"""
    results = {
        "overall_success": False,
        "litellm_test": None,
        "rdagent_health_check": None,
        "errors": []
    }
    
    # 1. LiteLLM验证
    if run_litellm_test:
        try:
            import litellm
            
            # 临时设置环境变量
            os.environ[f"{provider_name.upper()}_API_KEY"] = api_key
            if api_base:
                os.environ[f"{provider_name.upper()}_API_BASE"] = api_base
            
            # 测试调用
            response = await litellm.acompletion(
                model=full_model_id,
                messages=[{"role": "user", "content": "Test"}],
                max_tokens=5,
                timeout=30
            )
            
            results["litellm_test"] = {
                "success": True,
                "message": "LiteLLM验证通过",
                "response_model": response.model if hasattr(response, 'model') else full_model_id
            }
            
        except Exception as e:
            results["litellm_test"] = {
                "success": False,
                "message": f"LiteLLM验证失败: {str(e)}"
            }
            results["errors"].append(f"LiteLLM: {str(e)}")
        finally:
            # 清理环境变量
            os.environ.pop(f"{provider_name.upper()}_API_KEY", None)
            if api_base:
                os.environ.pop(f"{provider_name.upper()}_API_BASE", None)
    
    # 2. RDAgent健康检查
    if run_health_check:
        try:
            import httpx
            
            # 调用RDAgent健康检查API
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(f"{RDAGENT_API_BASE}/health")
                
                if response.status_code == 200:
                    health_data = response.json()
                    results["rdagent_health_check"] = {
                        "success": True,
                        "message": "RDAgent健康检查通过",
                        "data": health_data
                    }
                else:
                    results["rdagent_health_check"] = {
                        "success": False,
                        "message": f"RDAgent健康检查失败: HTTP {response.status_code}"
                    }
                    results["errors"].append(f"RDAgent Health: HTTP {response.status_code}")
                    
        except Exception as e:
            results["rdagent_health_check"] = {
                "success": False,
                "message": f"RDAgent健康检查失败: {str(e)}"
            }
            results["errors"].append(f"RDAgent Health: {str(e)}")
    
    # 判断总体成功
    litellm_ok = results["litellm_test"] is None or results["litellm_test"]["success"]
    health_ok = results["rdagent_health_check"] is None or results["rdagent_health_check"]["success"]
    results["overall_success"] = litellm_ok and health_ok
    
    return results


@router.get("/providers")
async def get_providers() -> dict[str, Any]:
    """获取所有LLM服务商"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, provider_name, display_name, api_base_url, litellm_prefix,
                       supports_chat, supports_embedding, supports_reasoner, is_active
                FROM aistock_llm_providers
                WHERE is_active = true
                ORDER BY id
            """)
            
            providers = []
            for row in cursor.fetchall():
                providers.append({
                    "id": row[0],
                    "provider_name": row[1],
                    "display_name": row[2],
                    "api_base_url": row[3],
                    "litellm_prefix": row[4],
                    "supports_chat": row[5],
                    "supports_embedding": row[6],
                    "supports_reasoner": row[7],
                    "is_active": row[8],
                })
            
            cursor.close()
        
        return {"providers": providers}
    except Exception as e:
        return {"providers": [], "error": str(e)}


@router.get("/models")
async def get_models(
    provider_id: Optional[int] = None,
    model_type: Optional[str] = None,
) -> dict[str, Any]:
    """获取所有模型"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            params = []
            
            query = """
                SELECT m.id, m.provider_id, m.model_name, m.display_name, m.full_model_id,
                       m.model_type, m.model_category, m.description, m.is_verified,
                       m.last_verified_at, p.provider_name, p.display_name as provider_display_name
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                WHERE m.is_active = true
            """
            
            if provider_id:
                query += " AND m.provider_id = %s"
                params.append(provider_id)
            
            if model_type:
                query += " AND m.model_type = %s"
                params.append(model_type)
            
            query += " ORDER BY p.id, m.id"
            
            cursor.execute(query, params)
            
            models = []
            for row in cursor.fetchall():
                models.append({
                    "id": row[0],
                    "provider_id": row[1],
                    "model_name": row[2],
                    "display_name": row[3],
                    "full_model_id": row[4],
                    "model_type": row[5],
                    "model_category": row[6],
                    "description": row[7],
                    "is_verified": row[8],
                    "last_verified_at": row[9].isoformat() if row[9] else None,
                    "provider_name": row[10],
                    "provider_display_name": row[11],
                })
            
            cursor.close()
        
        return {"models": models}
    except Exception as e:
        return {"models": [], "error": str(e)}


@router.get("/stage-mappings")
async def get_stage_mappings() -> dict[str, Any]:
    """获取所有阶段映射配置"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT sm.id, sm.stage_name, sm.model_id, sm.temperature, sm.max_tokens,
                       sm.is_active, m.model_name, m.display_name as model_display_name,
                       p.provider_name, p.display_name as provider_display_name
                FROM aistock_llm_stage_mappings sm
                LEFT JOIN aistock_llm_models m ON sm.model_id = m.id
                LEFT JOIN aistock_llm_providers p ON m.provider_id = p.id
                WHERE sm.is_active = true
                ORDER BY sm.stage_name
            """)
            
            mappings = []
            for row in cursor.fetchall():
                mappings.append({
                    "id": row[0],
                    "stage_name": row[1],
                    "model_id": row[2],
                    "temperature": float(row[3]) if row[3] else None,
                    "max_tokens": row[4],
                    "is_active": row[5],
                    "model_name": row[6],
                    "model_display_name": row[7],
                    "provider_name": row[8],
                    "provider_display_name": row[9],
                })
            
            cursor.close()
        
        return {"stage_mappings": mappings}
    except Exception as e:
        return {"stage_mappings": [], "error": str(e)}


@router.get("/providers/{provider_id}/available-models")
async def get_provider_available_models(
    provider_id: int,
    model_type: Optional[str] = None,
) -> dict[str, Any]:
    """从服务商API获取可用模型列表"""
    import httpx
    
    try:
        # 获取服务商信息
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT provider_name, display_name, api_base_url, litellm_prefix
                FROM aistock_llm_providers
                WHERE id = %s AND is_active = true
            """, (provider_id,))
            row = cursor.fetchone()
            cursor.close()
        
        if not row:
            return {"models": [], "error": "服务商不存在"}
        
        provider_name, display_name, api_base_url, litellm_prefix = row
        
        # 根据服务商类型获取模型列表
        models = []
        
        # 硅基流动 (siliconflow) - 使用OpenAI兼容API
        if provider_name.lower() in ["siliconflow", "silicon"]:
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    # 需要API key才能获取模型列表，从环境变量获取
                    import os
                    api_key = os.environ.get("SILICONFLOW_API_KEY", "")
                    if not api_key:
                        return {"models": [], "error": "未配置SILICONFLOW_API_KEY"}
                    
                    # 硅基流动API endpoint
                    base_url = api_base_url or "https://api.siliconflow.cn/v1"
                    resp = await client.get(
                        f"{base_url}/models",
                        headers={"Authorization": f"Bearer {api_key}"}
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        for m in data.get("data", []):
                            model_id = m.get("id", "")
                            # 根据模型ID推断类型
                            m_type = "chat"
                            model_lower = model_id.lower()
                            if "embed" in model_lower or "embedding" in model_lower:
                                m_type = "embedding"
                            elif any(k in model_lower for k in ["reason", "think", "deepseek-reasoner", "o1", "o3"]):
                                m_type = "reasoner"
                            
                            if model_type and m_type != model_type:
                                continue
                            
                            model_prefix = _infer_model_litellm_prefix(
                                provider_name=provider_name,
                                provider_prefix=litellm_prefix,
                                model_id=model_id,
                            )
                            full_model_id = f"{model_prefix}/{model_id}" if model_prefix else model_id
                            models.append({
                                "model_id": model_id,
                                "display_name": model_id,
                                "model_type": m_type,
                                "litellm_prefix": model_prefix,
                                "full_model_id": full_model_id,
                                "provider_name": provider_name,
                                "provider_display_name": display_name,
                                "source": "api"
                            })
                    else:
                        return {"models": [], "error": f"API返回错误: {resp.status_code}"}
                except Exception as e:
                    return {"models": [], "error": f"获取模型列表失败: {str(e)}"}
        
        # DeepSeek
        elif provider_name.lower() == "deepseek":
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    import os
                    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
                    if not api_key:
                        return {"models": [], "error": "未配置DEEPSEEK_API_KEY"}
                    
                    resp = await client.get(
                        f"{api_base_url or 'https://api.deepseek.com'}/models",
                        headers={"Authorization": f"Bearer {api_key}"}
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        for m in data.get("data", []):
                            model_id = m.get("id", "")
                            m_type = "chat"
                            if "reasoner" in model_id.lower():
                                m_type = "reasoner"
                            
                            if model_type and m_type != model_type:
                                continue
                            model_prefix = _infer_model_litellm_prefix(
                                provider_name=provider_name,
                                provider_prefix=litellm_prefix,
                                model_id=model_id,
                            )
                            full_model_id = f"{model_prefix}/{model_id}" if model_prefix else model_id
                            models.append({
                                "model_id": model_id,
                                "display_name": model_id,
                                "model_type": m_type,
                                "litellm_prefix": model_prefix,
                                "full_model_id": full_model_id,
                                "provider_name": provider_name,
                                "provider_display_name": display_name,
                                "source": "api"
                            })
                except Exception as e:
                    return {"models": [], "error": f"获取模型列表失败: {str(e)}"}
        
        # 阿里云百炼/通义千问 (dashscope/qwen)
        elif provider_name.lower() in ["dashscope", "qwen", "tongyi", "bailian", "aliyun"]:
            # 阿里云百炼模型列表（静态列表，包含主流模型）
            qwen_models = [
                # 对话模型
                ("qwen-turbo", "通义千问-Turbo", "chat"),
                ("qwen-plus", "通义千问-Plus", "chat"),
                ("qwen-max", "通义千问-Max", "chat"),
                ("qwen-max-longcontext", "通义千问-Max长上下文", "chat"),
                ("qwen-long", "通义千问-Long", "chat"),
                ("qwen-vl-plus", "通义千问-VL-Plus", "chat"),
                ("qwen-vl-max", "通义千问-VL-Max", "chat"),
                ("qwen-audio-turbo", "通义千问-Audio", "chat"),
                ("qwen2.5-72b-instruct", "Qwen2.5-72B-Instruct", "chat"),
                ("qwen2.5-32b-instruct", "Qwen2.5-32B-Instruct", "chat"),
                ("qwen2.5-14b-instruct", "Qwen2.5-14B-Instruct", "chat"),
                ("qwen2.5-7b-instruct", "Qwen2.5-7B-Instruct", "chat"),
                ("qwen2.5-3b-instruct", "Qwen2.5-3B-Instruct", "chat"),
                ("qwen2-72b-instruct", "Qwen2-72B-Instruct", "chat"),
                ("qwen2-57b-a14b-instruct", "Qwen2-57B-A14B-Instruct", "chat"),
                ("qwen2-7b-instruct", "Qwen2-7B-Instruct", "chat"),
                ("qwen2-1.5b-instruct", "Qwen2-1.5B-Instruct", "chat"),
                ("qwen2-0.5b-instruct", "Qwen2-0.5B-Instruct", "chat"),
                ("qwen1.5-110b-chat", "Qwen1.5-110B-Chat", "chat"),
                ("qwen1.5-72b-chat", "Qwen1.5-72B-Chat", "chat"),
                ("qwen1.5-32b-chat", "Qwen1.5-32B-Chat", "chat"),
                ("qwen1.5-14b-chat", "Qwen1.5-14B-Chat", "chat"),
                ("qwen1.5-7b-chat", "Qwen1.5-7B-Chat", "chat"),
                ("llama3.1-8b-instruct", "Llama3.1-8B-Instruct", "chat"),
                ("llama3.1-70b-instruct", "Llama3.1-70B-Instruct", "chat"),
                ("llama3.2-1b-instruct", "Llama3.2-1B-Instruct", "chat"),
                ("llama3.2-3b-instruct", "Llama3.2-3B-Instruct", "chat"),
                ("llama3-8b-instruct", "Llama3-8B-Instruct", "chat"),
                ("llama3-70b-instruct", "Llama3-70B-Instruct", "chat"),
                ("deepseek-v3", "DeepSeek-V3", "chat"),
                ("deepseek-r1", "DeepSeek-R1", "reasoner"),
                ("deepseek-r1-distill-qwen-1.5b", "DeepSeek-R1-Distill-Qwen-1.5B", "reasoner"),
                ("deepseek-r1-distill-qwen-7b", "DeepSeek-R1-Distill-Qwen-7B", "reasoner"),
                ("deepseek-r1-distill-qwen-14b", "DeepSeek-R1-Distill-Qwen-14B", "reasoner"),
                ("deepseek-r1-distill-qwen-32b", "DeepSeek-R1-Distill-Qwen-32B", "reasoner"),
                # 嵌入模型
                ("text-embedding-v1", "文本嵌入V1", "embedding"),
                ("text-embedding-v2", "文本嵌入V2", "embedding"),
                ("text-embedding-v3", "文本嵌入V3", "embedding"),
                ("text-embedding-v4", "文本嵌入V4", "embedding"),
            ]
            for model_id, display, m_type in qwen_models:
                if model_type and m_type != model_type:
                    continue
                model_prefix = _infer_model_litellm_prefix(
                    provider_name=provider_name,
                    provider_prefix=litellm_prefix,
                    model_id=model_id,
                )
                full_model_id = f"{model_prefix}/{model_id}" if model_prefix else model_id
                models.append({
                    "model_id": model_id,
                    "display_name": display,
                    "model_type": m_type,
                    "litellm_prefix": model_prefix,
                    "full_model_id": full_model_id,
                    "provider_name": provider_name,
                    "provider_display_name": display_name,
                    "source": "static"
                })
        
        # OpenAI
        elif provider_name.lower() == "openai":
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    import os
                    api_key = os.environ.get("OPENAI_API_KEY", "")
                    if not api_key:
                        return {"models": [], "error": "未配置OPENAI_API_KEY"}
                    
                    resp = await client.get(
                        f"{api_base_url or 'https://api.openai.com/v1'}/models",
                        headers={"Authorization": f"Bearer {api_key}"}
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        for m in data.get("data", []):
                            model_id = m.get("id", "")
                            m_type = "chat"
                            if "embed" in model_id.lower() or "text-embedding" in model_id.lower():
                                m_type = "embedding"
                            elif "o1" in model_id.lower() or "o3" in model_id.lower():
                                m_type = "reasoner"
                            
                            if model_type and m_type != model_type:
                                continue
                            model_prefix = _infer_model_litellm_prefix(
                                provider_name=provider_name,
                                provider_prefix=litellm_prefix,
                                model_id=model_id,
                            )
                            full_model_id = f"{model_prefix}/{model_id}" if model_prefix else model_id
                            
                            models.append({
                                "model_id": model_id,
                                "display_name": model_id,
                                "model_type": m_type,
                                "litellm_prefix": model_prefix,
                                "full_model_id": full_model_id,
                                "provider_name": provider_name,
                                "provider_display_name": display_name,
                                "source": "api"
                            })
                except Exception as e:
                    return {"models": [], "error": f"获取模型列表失败: {str(e)}"}
        
        else:
            # 其他服务商返回数据库中已有的模型
            with get_conn() as conn:
                cursor = conn.cursor()
                query = "SELECT model_name, display_name, model_type FROM aistock_llm_models WHERE provider_id = %s AND is_active = true"
                params = [provider_id]
                if model_type:
                    query += " AND model_type = %s"
                    params.append(model_type)
                cursor.execute(query, params)
                for row in cursor.fetchall():
                    model_id = row[0]
                    model_prefix = _infer_model_litellm_prefix(
                        provider_name=provider_name,
                        provider_prefix=litellm_prefix,
                        model_id=model_id,
                    )
                    full_model_id = f"{model_prefix}/{model_id}" if model_prefix else model_id
                    models.append({
                        "model_id": row[0],
                        "display_name": row[1],
                        "model_type": row[2],
                        "litellm_prefix": model_prefix,
                        "full_model_id": full_model_id,
                        "provider_name": provider_name,
                        "provider_display_name": display_name,
                        "source": "database"
                    })
                cursor.close()
        
        return {"models": models, "provider_id": provider_id, "provider_name": provider_name}
    
    except Exception as e:
        return {"models": [], "error": str(e)}


@router.get("/current-config")
async def get_current_config() -> dict[str, Any]:
    """获取当前RD-Agent的LLM配置（优先通过API调用RD-Agent，失败时从本地数据库降级读取）"""
    rdagent_error = None

    # 优先尝试从RDAgent侧API获取
    try:
        client = get_llm_config_client()
        config = await client.get_current_config()

        # Transform API response to match expected format
        stage_mappings = {}
        for stage, stage_config in config.get("stage_mappings", {}).items():
            stage_mappings[stage] = {
                "model": stage_config.get("model", ""),
                "temperature": stage_config.get("temperature"),
                "max_tokens": stage_config.get("max_tokens"),
            }

        # Extract embedding config
        embedding_config = config.get("embedding_config", {})
        embedding_stage_mappings = {}
        if embedding_config.get("litellm_embedding_model"):
            embedding_stage_mappings["default"] = {
                "model": embedding_config["litellm_embedding_model"]
            }

        return {
            "base_config": {
                "chat_model": config.get("chat_model", ""),
                "backend": config.get("backend", ""),
            },
            "stage_mappings": stage_mappings,
            "embedding_stage_mappings": embedding_stage_mappings,
            "last_updated": config.get("last_updated"),
            "source": "rdagent_api",
        }
    except Exception as e:
        rdagent_error = str(e)

    # RDAgent侧API不可用，从本地数据库降级读取
    try:
        with get_conn() as conn:
            cursor = conn.cursor()

            # 读取阶段映射
            cursor.execute("""
                SELECT sm.stage_name, m.full_model_id, sm.temperature, sm.max_tokens
                FROM aistock_llm_stage_mappings sm
                LEFT JOIN aistock_llm_models m ON sm.model_id = m.id
                WHERE sm.is_active = true
                ORDER BY sm.stage_name
            """)
            stage_mappings = {}
            for row in cursor.fetchall():
                stage_mappings[row[0]] = {
                    "model": row[1] or "",
                    "temperature": float(row[2]) if row[2] else None,
                    "max_tokens": row[3],
                }

            # 读取embedding配置
            cursor.execute("""
                SELECT m.full_model_id
                FROM aistock_llm_stage_mappings sm
                JOIN aistock_llm_models m ON sm.model_id = m.id
                WHERE sm.stage_name = 'embedding' AND sm.is_active = true
                LIMIT 1
            """)
            embedding_row = cursor.fetchone()
            embedding_stage_mappings = {}
            if embedding_row and embedding_row[0]:
                embedding_stage_mappings["default"] = {"model": embedding_row[0]}

            cursor.close()

        return {
            "base_config": {
                "chat_model": "",
                "backend": "",
            },
            "stage_mappings": stage_mappings,
            "embedding_stage_mappings": embedding_stage_mappings,
            "last_updated": None,
            "source": "local_database",
            "rdagent_api_error": rdagent_error,
        }
    except Exception as db_e:
        # 两种方式都失败，返回空配置而不是500
        return {
            "base_config": {"chat_model": "", "backend": ""},
            "stage_mappings": {},
            "embedding_stage_mappings": {},
            "last_updated": None,
            "source": "fallback_empty",
            "rdagent_api_error": rdagent_error,
            "database_error": str(db_e),
        }


@router.get("/change-logs")
async def get_change_logs(
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """获取配置变更记录"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 获取总数
            cursor.execute("SELECT COUNT(*) FROM aistock_llm_config_change_log")
            total = cursor.fetchone()[0]
            
            # 获取记录，包含模型名称
            cursor.execute("""
                SELECT 
                    log.id, 
                    log.stage_name, 
                    log.old_model_id, 
                    log.new_model_id, 
                    log.change_reason,
                    log.changed_at, 
                    log.changed_by,
                    old_m.full_model_id as old_model_name,
                    new_m.full_model_id as new_model_name
                FROM aistock_llm_config_change_log log
                LEFT JOIN aistock_llm_models old_m ON log.old_model_id = old_m.id
                LEFT JOIN aistock_llm_models new_m ON log.new_model_id = new_m.id
                ORDER BY log.changed_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            
            logs = []
            for row in cursor.fetchall():
                logs.append({
                    "id": row[0],
                    "stage_name": row[1],
                    "old_model_id": row[2],
                    "new_model_id": row[3],
                    "change_reason": row[4],
                    "changed_at": row[5].isoformat() if row[5] else None,
                    "changed_by": row[6],
                    "old_model_name": row[7] or "未配置",
                    "new_model_name": row[8] or "未配置",
                })
            
            cursor.close()
        
        return {
            "logs": logs,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        return {"logs": [], "total": 0, "error": str(e)}


@router.post("/providers")
async def create_provider(provider: ProviderCreate) -> dict[str, Any]:
    """添加新的LLM服务商"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 检查是否已存在
            cursor.execute(
                "SELECT id FROM aistock_llm_providers WHERE provider_name = %s",
                (provider.provider_name,)
            )
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="服务商已存在")
            
            # 插入新服务商
            cursor.execute("""
                INSERT INTO aistock_llm_providers
                (provider_name, display_name, api_base_url, litellm_prefix,
                 supports_chat, supports_embedding, supports_reasoner)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                provider.provider_name,
                provider.display_name,
                provider.api_base_url,
                provider.litellm_prefix or provider.provider_name,
                provider.supports_chat,
                provider.supports_embedding,
                provider.supports_reasoner
            ))
            
            provider_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            
            return {"success": True, "provider_id": provider_id, "message": "服务商添加成功"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/providers/{provider_id}")
async def update_provider(provider_id: int, provider: ProviderUpdate) -> dict[str, Any]:
    """更新LLM服务商配置"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 检查服务商是否存在
            cursor.execute(
                "SELECT id FROM aistock_llm_providers WHERE id = %s",
                (provider_id,)
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="服务商不存在")
            
            # 构建更新语句
            updates = []
            params = []
            
            if provider.display_name is not None:
                updates.append("display_name = %s")
                params.append(provider.display_name)
            if provider.api_base_url is not None:
                updates.append("api_base_url = %s")
                params.append(provider.api_base_url)
            if provider.litellm_prefix is not None:
                updates.append("litellm_prefix = %s")
                params.append(provider.litellm_prefix)
            if provider.supports_chat is not None:
                updates.append("supports_chat = %s")
                params.append(provider.supports_chat)
            if provider.supports_embedding is not None:
                updates.append("supports_embedding = %s")
                params.append(provider.supports_embedding)
            if provider.supports_reasoner is not None:
                updates.append("supports_reasoner = %s")
                params.append(provider.supports_reasoner)
            if provider.is_active is not None:
                updates.append("is_active = %s")
                params.append(provider.is_active)
            
            if not updates:
                raise HTTPException(status_code=400, detail="没有要更新的字段")
            
            params.append(provider_id)
            
            cursor.execute(
                f"UPDATE aistock_llm_providers SET {', '.join(updates)} WHERE id = %s",
                params
            )
            
            conn.commit()
            cursor.close()
            
            return {"success": True, "provider_id": provider_id, "message": "服务商更新成功"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/providers/{provider_id}")
async def delete_provider(provider_id: int) -> dict[str, Any]:
    """删除LLM服务商（软删除）"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 检查服务商是否存在
            cursor.execute(
                "SELECT id FROM aistock_llm_providers WHERE id = %s",
                (provider_id,)
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="服务商不存在")
            
            # 软删除
            cursor.execute(
                "UPDATE aistock_llm_providers SET is_active = false WHERE id = %s",
                (provider_id,)
            )
            
            conn.commit()
            cursor.close()
            
            return {"success": True, "provider_id": provider_id, "message": "服务商已删除"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models")
async def create_model(model: ModelCreate) -> dict[str, Any]:
    """添加新的LLM模型，支持API验证"""
    try:
        # 验证描述长度
        if model.description and len(model.description) > 100:
            raise HTTPException(status_code=400, detail="模型说明不能超过100字")
        
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 检查服务商是否存在并获取provider_name
            cursor.execute(
                "SELECT provider_name FROM aistock_llm_providers WHERE id = %s",
                (model.provider_id,)
            )
            provider_row = cursor.fetchone()
            if not provider_row:
                raise HTTPException(status_code=404, detail="服务商不存在")
            
            provider_name = provider_row[0]
            
            # 检查模型是否已存在
            cursor.execute(
                "SELECT id FROM aistock_llm_models WHERE provider_id = %s AND model_name = %s",
                (model.provider_id, model.model_name)
            )
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="模型已存在")
            
            # 如果需要验证，先验证API
            is_verified = False
            verification_message = None
            
            provider_api_config = _find_provider_api_config(
                cursor=cursor,
                provider_id=model.provider_id,
                model_type=model.model_type,
            )

            verify_api_key = model.api_key
            verify_api_base = model.api_base
            if not verify_api_key and provider_api_config:
                verify_api_key = provider_api_config.get("api_key")
                if not verify_api_base:
                    verify_api_base = provider_api_config.get("api_base")

            if model.verify_on_add:
                if verify_api_key:
                    verification_result = await verify_model_api(
                        model.full_model_id,
                        verify_api_key,
                        verify_api_base,
                    )
                    is_verified = verification_result["success"]
                    verification_message = verification_result["message"]
                    if not is_verified:
                        raise HTTPException(status_code=400, detail=f"模型验证失败: {verification_message}")
                else:
                    verification_message = "未提供模型专属API Key，且服务商默认API配置不存在，已跳过验证"
            
            # 插入新模型
            cursor.execute("""
                INSERT INTO aistock_llm_models
                (provider_id, model_name, display_name, full_model_id, model_type,
                 model_category, description, is_verified, last_verified_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                model.provider_id,
                model.model_name,
                model.display_name,
                model.full_model_id,
                model.model_type,
                model.model_category,
                model.description,
                is_verified,
                datetime.now() if is_verified else None
            ))
            
            model_id = cursor.fetchone()[0]

            # 处理API配置绑定
            if model.api_key:
                # 用户提供了专属API Key，创建独立配置
                cursor.execute("SELECT default_env_prefix FROM aistock_llm_providers WHERE id = %s", (model.provider_id,))
                env_prefix = cursor.fetchone()[0]
                prefix_str = (env_prefix or provider_name).upper()
                
                cursor.execute("""
                    INSERT INTO aistock_llm_api_configs
                    (provider_id, api_base, api_key, env_api_base_name, env_api_key_name,
                     config_purpose, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    model.provider_id,
                    model.api_base or '',
                    model.api_key,
                    f"{prefix_str}_API_BASE",
                    f"{prefix_str}_API_KEY",
                    f"model_{model_id}",
                    f"{provider_name} 模型 {model_id} 专属配置"
                ))
                api_config_id = cursor.fetchone()[0]
                cursor.execute(
                    """
                    UPDATE aistock_llm_models
                    SET api_config_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (api_config_id, model_id),
                )
            # 如果没有提供模型级api_key，保持 api_config_id 为 NULL，这样在使用时会自动回退到服务商配置
            
            conn.commit()
            cursor.close()
            
            return {
                "success": True,
                "model_id": model_id,
                "message": "模型添加成功",
                "is_verified": is_verified,
                "verification_message": verification_message
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ModelUpdate(BaseModel):
    """模型更新请求"""
    model_name: str
    display_name: str
    full_model_id: str
    model_type: str
    model_category: Optional[str] = None
    description: Optional[str] = None


@router.put("/models/{model_id}")
async def update_model(model_id: int, model_update: ModelUpdate) -> dict[str, Any]:
    """更新模型基本信息"""
    try:
        # 验证描述长度
        if model_update.description and len(model_update.description) > 100:
            raise HTTPException(status_code=400, detail="模型说明不能超过100字")
        
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 检查模型是否存在
            cursor.execute(
                "SELECT id FROM aistock_llm_models WHERE id = %s",
                (model_id,)
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="模型不存在")
            
            # 更新模型信息
            cursor.execute("""
                UPDATE aistock_llm_models
                SET model_name = %s,
                    display_name = %s,
                    full_model_id = %s,
                    model_type = %s,
                    model_category = %s,
                    description = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                model_update.model_name,
                model_update.display_name,
                model_update.full_model_id,
                model_update.model_type,
                model_update.model_category,
                model_update.description,
                model_id
            ))
            
            conn.commit()
            cursor.close()
            
            return {
                "success": True,
                "message": "模型更新成功",
                "model_id": model_id
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{model_id}/config")
async def get_model_config(model_id: int) -> dict[str, Any]:
    """获取模型API配置（不包含敏感信息如API Key）"""
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 获取模型和API配置信息
            cursor.execute("""
                SELECT m.id, m.full_model_id, m.model_name, p.provider_name,
                       ac.api_base, ac.env_api_base_name, ac.env_api_key_name,
                       ac.config_purpose, ac.is_active
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
                WHERE m.id = %s
            """, (model_id,))
            
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="模型不存在")
            
            cursor.close()
            
            return {
                "success": True,
                "model_id": row[0],
                "full_model_id": row[1],
                "model_name": row[2],
                "provider_name": row[3],
                "config": {
                    "api_base": row[4],
                    "env_api_base_name": row[5],
                    "env_api_key_name": row[6],
                    "config_purpose": row[7],
                    "is_active": row[8]
                } if row[4] else None
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update-config")
async def update_config(config: ConfigUpdate) -> dict[str, Any]:
    """更新阶段映射配置，通过API调用RD-Agent"""
    client = None
    
    try:
        # 1. 获取API客户端
        client = get_llm_config_client()
        
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 2. 验证所有模型ID是否存在，并检查API配置
            for mapping in config.stage_mappings:
                if mapping.model_id:
                    cursor.execute(
                        "SELECT id, full_model_id, api_config_id, provider_id, model_type FROM aistock_llm_models WHERE id = %s",
                        (mapping.model_id,),
                    )
                    row = cursor.fetchone()
                    if not row:
                        raise HTTPException(status_code=400, detail=f"模型ID {mapping.model_id} 不存在")

                    api_config_id = row[2]
                    if not api_config_id:
                        provider_api = _find_provider_api_config(
                            cursor=cursor,
                            provider_id=row[3],
                            model_type=row[4],
                        )
                        api_config_id = provider_api["id"] if provider_api else None

                    # 检查模型是否有可用API配置（模型级或服务商级）
                    if not api_config_id:
                        raise HTTPException(
                            status_code=400,
                            detail=f"模型 '{row[1]}' 缺少API配置。请先为该模型配置API Key，或选择其他已配置的模型。"
                        )
            
            # 3. 更新数据库中的阶段映射
            for mapping in config.stage_mappings:
                # 检查阶段映射是否存在
                cursor.execute(
                    "SELECT id, model_id FROM aistock_llm_stage_mappings WHERE stage_name = %s",
                    (mapping.stage_name,)
                )
                row = cursor.fetchone()
                
                old_model_id = row[1] if row else None
                
                if row:
                    # 更新现有映射
                    cursor.execute("""
                        UPDATE aistock_llm_stage_mappings
                        SET model_id = %s, temperature = %s, max_tokens = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE stage_name = %s
                    """, (
                        mapping.model_id,
                        mapping.temperature,
                        mapping.max_tokens,
                        mapping.stage_name
                    ))
                else:
                    # 创建新映射
                    cursor.execute("""
                        INSERT INTO aistock_llm_stage_mappings
                        (stage_name, model_id, temperature, max_tokens)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        mapping.stage_name,
                        mapping.model_id,
                        mapping.temperature,
                        mapping.max_tokens
                    ))
                
                # 记录变更历史
                if old_model_id != mapping.model_id:
                    cursor.execute("""
                        INSERT INTO aistock_llm_config_change_log
                        (stage_name, old_model_id, new_model_id, change_reason)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        mapping.stage_name,
                        old_model_id,
                        mapping.model_id,
                        config.change_reason
                    ))
            
            # 4. 获取所有模型信息，用于构建API更新
            cursor.execute("""
                SELECT sm.stage_name, m.full_model_id, sm.temperature, sm.max_tokens,
                       m.provider_id, m.model_type, m.api_config_id
                FROM aistock_llm_stage_mappings sm
                LEFT JOIN aistock_llm_models m ON sm.model_id = m.id
                WHERE sm.is_active = true
            """)
            
            stage_map = {}
            api_credentials = {}
            
            for row in cursor.fetchall():
                stage_name, full_model_id, temperature, max_tokens = row[0], row[1], row[2], row[3]
                provider_id, model_type, model_api_config_id = row[4], row[5], row[6]
                
                if full_model_id:
                    stage_map[stage_name] = {
                        "model": full_model_id,
                        "temperature": str(temperature) if temperature is not None else None,
                        "max_tokens": str(max_tokens) if max_tokens is not None else None
                    }
                
                # 收集API凭证（优先模型级，回退服务商级）
                selected_api_config = None
                if model_api_config_id:
                    cursor.execute(
                        """
                        SELECT id, api_base, api_key, env_api_base_name, env_api_key_name
                        FROM aistock_llm_api_configs
                        WHERE id = %s AND is_active = true
                        LIMIT 1
                        """,
                        (model_api_config_id,),
                    )
                    cfg = cursor.fetchone()
                    if cfg:
                        selected_api_config = {
                            "id": cfg[0],
                            "api_base": cfg[1],
                            "api_key": cfg[2],
                            "env_api_base_name": cfg[3],
                            "env_api_key_name": cfg[4],
                        }
                if not selected_api_config and provider_id:
                    selected_api_config = _find_provider_api_config(
                        cursor=cursor,
                        provider_id=provider_id,
                        model_type=model_type,
                    )

                if selected_api_config:
                    if selected_api_config.get("api_base") and selected_api_config.get("env_api_base_name"):
                        api_credentials[selected_api_config["env_api_base_name"]] = selected_api_config["api_base"]
                    if selected_api_config.get("api_key") and selected_api_config.get("env_api_key_name"):
                        api_credentials[selected_api_config["env_api_key_name"]] = selected_api_config["api_key"]
            
            # 5. 处理embedding模型配置
            if config.embedding_model_id:
                cursor.execute("""
                    SELECT m.full_model_id, m.provider_id, m.model_type, m.api_config_id
                    FROM aistock_llm_models m
                    WHERE m.id = %s
                """, (config.embedding_model_id,))
                
                row = cursor.fetchone()
                if row:
                    full_model_id, provider_id, model_type, model_api_config_id = row
                    if full_model_id:
                        stage_map["embedding"] = {
                            "model": full_model_id,
                        }
                    selected_api_config = None
                    if model_api_config_id:
                        cursor.execute(
                            """
                            SELECT id, api_base, api_key, env_api_base_name, env_api_key_name
                            FROM aistock_llm_api_configs
                            WHERE id = %s AND is_active = true
                            LIMIT 1
                            """,
                            (model_api_config_id,),
                        )
                        cfg = cursor.fetchone()
                        if cfg:
                            selected_api_config = {
                                "id": cfg[0],
                                "api_base": cfg[1],
                                "api_key": cfg[2],
                                "env_api_base_name": cfg[3],
                                "env_api_key_name": cfg[4],
                            }
                    if not selected_api_config and provider_id:
                        selected_api_config = _find_provider_api_config(
                            cursor=cursor,
                            provider_id=provider_id,
                            model_type=model_type,
                        )
                    if selected_api_config:
                        if selected_api_config.get("api_base") and selected_api_config.get("env_api_base_name"):
                            api_credentials[selected_api_config["env_api_base_name"]] = selected_api_config["api_base"]
                        if selected_api_config.get("api_key") and selected_api_config.get("env_api_key_name"):
                            api_credentials[selected_api_config["env_api_key_name"]] = selected_api_config["api_key"]
            
            # 6. 构建API请求
            api_stage_mappings = []
            for stage_name, stage_config in stage_map.items():
                mapping = {
                    "stage_name": stage_name,
                    "model_id": stage_config["model"],
                }
                if stage_config.get("temperature"):
                    mapping["temperature"] = float(stage_config["temperature"])
                if stage_config.get("max_tokens"):
                    mapping["max_tokens"] = int(stage_config["max_tokens"])
                api_stage_mappings.append(mapping)
            
            # 7. 调用RD-Agent API更新配置
            result = await client.update_config(
                stage_mappings=api_stage_mappings,
                api_credentials=api_credentials if api_credentials else None,
                change_reason=config.change_reason,
                backup_reason="aistock_update",
            )
            
            if not result.get("ok"):
                raise HTTPException(
                    status_code=500,
                    detail=f"RD-Agent配置更新失败: {result.get('message', 'Unknown error')}"
                )
            
            conn.commit()
            cursor.close()
            
            return {
                "success": True,
                "message": "配置更新成功",
                "backup_path": result.get("backup_path"),
                "updated_stages": len(config.stage_mappings),
                "updated_env_vars": result.get("updated_keys", [])
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"配置更新失败: {str(e)}")
    finally:
        if client:
            await client.close()
