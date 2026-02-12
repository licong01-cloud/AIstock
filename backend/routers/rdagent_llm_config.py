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


async def verify_model_api(provider_name: str, model_name: str, api_key: str, api_base: Optional[str] = None) -> dict:
    """验证模型API可用性"""
    try:
        import litellm
        
        # 设置API密钥
        os.environ[f"{provider_name.upper()}_API_KEY"] = api_key
        if api_base:
            os.environ[f"{provider_name.upper()}_API_BASE"] = api_base
        
        # 构造完整模型ID
        full_model_id = f"{provider_name}/{model_name}"
        
        # 尝试调用模型
        response = await litellm.acompletion(
            model=full_model_id,
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=10,
            timeout=30
        )
        
        return {
            "success": True,
            "message": "模型验证成功",
            "model_id": response.model if hasattr(response, 'model') else full_model_id
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"模型验证失败: {str(e)}"
        }
    finally:
        # 清理环境变量
        os.environ.pop(f"{provider_name.upper()}_API_KEY", None)
        if api_base:
            os.environ.pop(f"{provider_name.upper()}_API_BASE", None)


# Pydantic模型
class ProviderCreate(BaseModel):
    provider_name: str
    display_name: str
    api_base_url: Optional[str] = None
    litellm_prefix: Optional[str] = None
    supports_chat: bool = True
    supports_embedding: bool = False
    supports_reasoner: bool = False


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


@router.get("/current-config")
async def get_current_config() -> dict[str, Any]:
    """获取当前RD-Agent的LLM配置（通过API调用RD-Agent）"""
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
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
            
            if model.verify_on_add and model.api_key:
                verification_result = await verify_model_api(
                    provider_name,
                    model.model_name,
                    model.api_key,
                    model.api_base
                )
                is_verified = verification_result["success"]
                verification_message = verification_result["message"]
                
                if not is_verified:
                    raise HTTPException(status_code=400, detail=f"模型验证失败: {verification_message}")
            
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
                    model_type = %s,
                    model_category = %s,
                    description = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                model_update.model_name,
                model_update.display_name,
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
                        "SELECT id, full_model_id, api_config_id FROM aistock_llm_models WHERE id = %s",
                        (mapping.model_id,)
                    )
                    row = cursor.fetchone()
                    if not row:
                        raise HTTPException(status_code=400, detail=f"模型ID {mapping.model_id} 不存在")
                    
                    # 检查模型是否有API配置
                    if not row[2]:  # api_config_id
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
                       ac.api_base, ac.api_key, ac.env_api_base_name, ac.env_api_key_name
                FROM aistock_llm_stage_mappings sm
                LEFT JOIN aistock_llm_models m ON sm.model_id = m.id
                LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
                WHERE sm.is_active = true
            """)
            
            stage_map = {}
            api_credentials = {}
            
            for row in cursor.fetchall():
                stage_name, full_model_id, temperature, max_tokens = row[0], row[1], row[2], row[3]
                api_base, api_key, env_api_base_name, env_api_key_name = row[4], row[5], row[6], row[7]
                
                if full_model_id:
                    stage_map[stage_name] = {
                        "model": full_model_id,
                        "temperature": str(temperature) if temperature is not None else None,
                        "max_tokens": str(max_tokens) if max_tokens is not None else None
                    }
                
                # 收集API凭证
                if api_base and env_api_base_name:
                    api_credentials[env_api_base_name] = api_base
                if api_key and env_api_key_name:
                    api_credentials[env_api_key_name] = api_key
            
            # 5. 处理embedding模型配置
            if config.embedding_model_id:
                cursor.execute("""
                    SELECT m.full_model_id, ac.api_base, ac.api_key, 
                           ac.env_api_base_name, ac.env_api_key_name
                    FROM aistock_llm_models m
                    LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
                    WHERE m.id = %s
                """, (config.embedding_model_id,))
                
                row = cursor.fetchone()
                if row:
                    full_model_id, api_base, api_key, env_api_base_name, env_api_key_name = row
                    if full_model_id:
                        stage_map["embedding"] = {
                            "model": full_model_id,
                        }
                    if api_base and env_api_base_name:
                        api_credentials[env_api_base_name] = api_base
                    if api_key and env_api_key_name:
                        api_credentials[env_api_key_name] = api_key
            
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
