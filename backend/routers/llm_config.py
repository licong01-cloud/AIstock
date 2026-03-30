"""
LLM配置管理API路由
包含服务商管理、模型管理、RDAgent配置、AIstock Agent配置
"""
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.db.pg_pool import get_conn
from backend.services.llm_model_sync import (
    batch_import_models,
    fetch_provider_models,
    sync_provider_models,
)

router = APIRouter(prefix="/api/v1/llm", tags=["LLM配置管理"])


# ============================================
# 数据模型
# ============================================


class ProviderCreate(BaseModel):
    provider_name: str
    display_name: str
    api_base_url: str
    litellm_prefix: str
    provider_type: str = "official"
    default_env_prefix: str | None = None
    use_proxy: bool = False
    proxy_model_prefix: str | None = None
    supports_chat: bool = True
    supports_embedding: bool = False
    supports_reasoner: bool = False
    supports_vision: bool = False


class ProviderUpdate(BaseModel):
    display_name: str | None = None
    api_base_url: str | None = None
    litellm_prefix: str | None = None
    provider_type: str | None = None
    default_env_prefix: str | None = None
    use_proxy: bool | None = None
    proxy_model_prefix: str | None = None
    supports_chat: bool | None = None
    supports_embedding: bool | None = None
    supports_reasoner: bool | None = None
    supports_vision: bool | None = None
    is_active: bool | None = None


class APIConfigCreate(BaseModel):
    provider_id: int
    api_base: str
    api_key: str
    env_api_base_name: str
    env_api_key_name: str
    config_purpose: str = "default"
    description: str | None = None


class ModelCreate(BaseModel):
    provider_id: int
    model_name: str
    display_name: str
    model_type: str = "chat"
    model_category: str | None = None
    context_window: int | None = None
    max_output_tokens: int | None = None
    input_price: float | None = None
    output_price: float | None = None
    proxy_model_alias: str | None = None
    api_config_id: int | None = None


class ModelBatchImport(BaseModel):
    provider_id: int
    models: list[dict[str, Any]]


class FetchModelsRequest(BaseModel):
    model_type: str | None = None  # chat/embedding/reasoner/vision/all
    api_base: str | None = None
    api_key: str | None = None


class SyncModelsRequest(BaseModel):
    model_type: str | None = None
    api_base: str | None = None
    api_key: str | None = None
    overwrite: bool = False


class RDAgentStageUpdate(BaseModel):
    provider_id: int
    model_id: int
    temperature: float | None = None
    max_tokens: int | None = None


class AIstockAgentUpdate(BaseModel):
    provider_id: int
    model_id: int
    temperature: float | None = None
    max_tokens: int | None = None
    system_prompt: str | None = None


# ============================================
# 服务商管理API
# ============================================


@router.get("/providers")
def list_providers() -> dict[str, Any]:
    """获取所有服务商列表"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, provider_name, display_name, api_base_url, litellm_prefix,
                   provider_type, default_env_prefix, use_proxy, proxy_model_prefix,
                   supports_chat, supports_embedding, supports_reasoner, supports_vision,
                   is_active, created_at, updated_at
            FROM aistock_llm_providers
            ORDER BY display_name
        """)
        providers = []
        for row in cursor.fetchall():
            providers.append({
                "id": row[0],
                "provider_name": row[1],
                "display_name": row[2],
                "api_base_url": row[3],
                "litellm_prefix": row[4],
                "provider_type": row[5],
                "default_env_prefix": row[6],
                "use_proxy": row[7],
                "proxy_model_prefix": row[8],
                "supports_chat": row[9],
                "supports_embedding": row[10],
                "supports_reasoner": row[11],
                "supports_vision": row[12],
                "is_active": row[13],
                "created_at": str(row[14]) if row[14] else None,
                "updated_at": str(row[15]) if row[15] else None,
            })
        cursor.close()
    return {"providers": providers}


@router.get("/providers/stats")
def get_providers_stats() -> dict[str, Any]:
    """获取服务商模型统计"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT provider_id, provider_name, display_name, litellm_prefix,
                   provider_type, use_proxy, total_models, chat_models,
                   embedding_models, reasoner_models, vision_models, synced_models
            FROM v_provider_model_stats
        """)
        stats = []
        for row in cursor.fetchall():
            stats.append({
                "provider_id": row[0],
                "provider_name": row[1],
                "display_name": row[2],
                "litellm_prefix": row[3],
                "provider_type": row[4],
                "use_proxy": row[5],
                "total_models": row[6],
                "chat_models": row[7],
                "embedding_models": row[8],
                "reasoner_models": row[9],
                "vision_models": row[10],
                "synced_models": row[11],
            })
        cursor.close()
    return {"stats": stats}


@router.post("/providers")
def create_provider(provider: ProviderCreate) -> dict[str, Any]:
    """创建服务商"""
    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO aistock_llm_providers (
                    provider_name, display_name, api_base_url, litellm_prefix,
                    provider_type, default_env_prefix, use_proxy, proxy_model_prefix,
                    supports_chat, supports_embedding, supports_reasoner, supports_vision
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                provider.provider_name, provider.display_name, provider.api_base_url,
                provider.litellm_prefix, provider.provider_type, provider.default_env_prefix,
                provider.use_proxy, provider.proxy_model_prefix,
                provider.supports_chat, provider.supports_embedding,
                provider.supports_reasoner, provider.supports_vision
            ))
            provider_id = cursor.fetchone()[0]
            conn.commit()
            return {"success": True, "provider_id": provider_id}
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            cursor.close()


@router.put("/providers/{provider_id}")
def update_provider(provider_id: int, provider: ProviderUpdate) -> dict[str, Any]:
    """更新服务商"""
    update_fields = []
    update_values = []

    for field, value in provider.model_dump(exclude_unset=True).items():
        if value is not None:
            update_fields.append(f"{field} = %s")
            update_values.append(value)

    if not update_fields:
        raise HTTPException(status_code=400, detail="没有要更新的字段")

    update_values.append(provider_id)

    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE aistock_llm_providers
                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, update_values)
            conn.commit()
            return {"success": True}
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            cursor.close()


@router.delete("/providers/{provider_id}")
def delete_provider(provider_id: int) -> dict[str, Any]:
    """删除服务商（软删除）"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE aistock_llm_providers SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (provider_id,))
        conn.commit()
        cursor.close()
    return {"success": True}


# ============================================
# 服务商模型同步API
# ============================================


@router.post("/providers/{provider_id}/models/fetch")
async def fetch_models(provider_id: int, request: FetchModelsRequest) -> dict[str, Any]:
    """从服务商API获取模型列表（不保存）"""
    result = await fetch_provider_models(
        provider_id=provider_id,
        model_type_filter=request.model_type,
        api_base_override=request.api_base,
        api_key_override=request.api_key
    )
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@router.post("/providers/{provider_id}/models/sync")
async def sync_models(provider_id: int, request: SyncModelsRequest) -> dict[str, Any]:
    """从服务商API同步模型到数据库"""
    result = await sync_provider_models(
        provider_id=provider_id,
        model_type_filter=request.model_type,
        api_base_override=request.api_base,
        api_key_override=request.api_key,
        overwrite=request.overwrite
    )
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


# ============================================
# API配置管理
# ============================================


@router.get("/api-configs")
def list_api_configs() -> dict[str, Any]:
    """获取所有API配置"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ac.id, ac.provider_id, p.display_name as provider_display_name,
                   ac.api_base, ac.env_api_base_name, ac.env_api_key_name,
                   ac.config_purpose, ac.priority, ac.description, ac.is_active
            FROM aistock_llm_api_configs ac
            JOIN aistock_llm_providers p ON ac.provider_id = p.id
            WHERE ac.is_active = TRUE
            ORDER BY p.display_name, ac.priority DESC
        """)
        configs = []
        for row in cursor.fetchall():
            configs.append({
                "id": row[0],
                "provider_id": row[1],
                "provider_display_name": row[2],
                "api_base": row[3],
                "env_api_base_name": row[4],
                "env_api_key_name": row[5],
                "config_purpose": row[6],
                "priority": row[7],
                "description": row[8],
                "is_active": row[9],
            })
        cursor.close()
    return {"configs": configs}


@router.post("/api-configs")
def create_api_config(config: APIConfigCreate) -> dict[str, Any]:
    """创建API配置"""
    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO aistock_llm_api_configs (
                    provider_id, api_base, api_key, env_api_base_name, env_api_key_name,
                    config_purpose, description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                config.provider_id, config.api_base, config.api_key,
                config.env_api_base_name, config.env_api_key_name,
                config.config_purpose, config.description
            ))
            config_id = cursor.fetchone()[0]
            conn.commit()
            return {"success": True, "config_id": config_id}
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            cursor.close()


# ============================================
# 模型管理API
# ============================================


@router.get("/models")
def list_models(
    provider_id: int | None = None,
    model_type: str | None = None,
    is_active: bool = True
) -> dict[str, Any]:
    """获取模型列表"""
    with get_conn() as conn:
        cursor = conn.cursor()

        where_clauses = ["m.is_active = %s"]
        params = [is_active]

        if provider_id:
            where_clauses.append("m.provider_id = %s")
            params.append(provider_id)

        if model_type and model_type != "all":
            where_clauses.append("m.model_type = %s")
            params.append(model_type)

        cursor.execute(f"""
            SELECT m.id, m.provider_id, p.display_name as provider_display_name,
                   m.model_name, m.display_name, m.full_model_id, m.model_type,
                   m.model_category, m.context_window, m.max_output_tokens,
                   m.input_price, m.output_price, m.proxy_model_alias, m.is_synced,
                   m.api_config_id
            FROM aistock_llm_models m
            JOIN aistock_llm_providers p ON m.provider_id = p.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY p.display_name, m.model_type, m.display_name
        """, params)

        models = []
        for row in cursor.fetchall():
            models.append({
                "id": row[0],
                "provider_id": row[1],
                "provider_display_name": row[2],
                "model_name": row[3],
                "display_name": row[4],
                "full_model_id": row[5],
                "model_type": row[6],
                "model_category": row[7],
                "context_window": row[8],
                "max_output_tokens": row[9],
                "input_price": float(row[10]) if row[10] else None,
                "output_price": float(row[11]) if row[11] else None,
                "proxy_model_alias": row[12],
                "is_synced": row[13],
                "api_config_id": row[14],
            })
        cursor.close()
    return {"models": models}


@router.post("/models")
def create_model(model: ModelCreate) -> dict[str, Any]:
    """创建模型"""
    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            # 获取服务商信息以生成full_model_id
            cursor.execute("""
                SELECT litellm_prefix, use_proxy, proxy_model_prefix
                FROM aistock_llm_providers WHERE id = %s
            """, (model.provider_id,))
            provider = cursor.fetchone()
            if not provider:
                raise ValueError("服务商不存在")

            litellm_prefix = provider[0]
            full_model_id = f"{litellm_prefix}/{model.model_name}"

            # 生成proxy_model_alias
            proxy_model_alias = model.proxy_model_alias
            if not proxy_model_alias and provider[1] and provider[2]:  # use_proxy and proxy_model_prefix
                proxy_model_alias = f"{provider[2]}-{model.model_name.replace('/', '-')}"

            cursor.execute("""
                INSERT INTO aistock_llm_models (
                    provider_id, model_name, display_name, full_model_id,
                    model_type, model_category, context_window, max_output_tokens,
                    input_price, output_price, proxy_model_alias, api_config_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                model.provider_id, model.model_name, model.display_name, full_model_id,
                model.model_type, model.model_category, model.context_window, model.max_output_tokens,
                model.input_price, model.output_price, proxy_model_alias, model.api_config_id
            ))
            model_id = cursor.fetchone()[0]
            conn.commit()
            return {"success": True, "model_id": model_id, "full_model_id": full_model_id}
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            cursor.close()


@router.post("/models/batch-import")
def batch_import_models_api(request: ModelBatchImport) -> dict[str, Any]:
    """批量导入模型"""
    result = batch_import_models(
        provider_id=request.provider_id,
        models=request.models
    )
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@router.delete("/models/{model_id}")
def delete_model(model_id: int) -> dict[str, Any]:
    """删除模型（软删除）"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE aistock_llm_models SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (model_id,))
        conn.commit()
        cursor.close()
    return {"success": True}


# ============================================
# RDAgent阶段配置API
# ============================================


@router.get("/rdagent/stages")
def list_rdagent_stages() -> dict[str, Any]:
    """获取RDAgent阶段配置列表"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.id, s.stage_name, s.stage_display_name, s.description,
                   s.model_id, m.display_name as model_display_name,
                   m.full_model_id, p.display_name as provider_display_name,
                   s.temperature, s.max_tokens, s.is_active
            FROM aistock_llm_rdagent_stages s
            LEFT JOIN aistock_llm_models m ON s.model_id = m.id
            LEFT JOIN aistock_llm_providers p ON m.provider_id = p.id
            WHERE s.is_active = TRUE
            ORDER BY s.stage_name
        """)
        stages = []
        for row in cursor.fetchall():
            stages.append({
                "id": row[0],
                "stage_name": row[1],
                "stage_display_name": row[2],
                "description": row[3],
                "model_id": row[4],
                "model_display_name": row[5],
                "full_model_id": row[6],
                "provider_display_name": row[7],
                "temperature": float(row[8]) if row[8] else None,
                "max_tokens": row[9],
                "is_active": row[10],
            })
        cursor.close()
    return {"stages": stages}


@router.put("/rdagent/stages/{stage_name}")
def update_rdagent_stage(stage_name: str, update: RDAgentStageUpdate) -> dict[str, Any]:
    """更新RDAgent阶段配置"""
    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            # 验证模型存在
            cursor.execute("""
                SELECT m.id, m.full_model_id, p.provider_name
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                WHERE m.id = %s AND m.is_active = TRUE
            """, (update.model_id,))
            model = cursor.fetchone()
            if not model:
                raise ValueError("模型不存在或未激活")

            # 记录变更日志
            cursor.execute("""
                SELECT model_id FROM aistock_llm_rdagent_stages WHERE stage_name = %s
            """, (stage_name,))
            old_model = cursor.fetchone()
            old_model_id = old_model[0] if old_model else None

            # 更新阶段配置
            cursor.execute("""
                UPDATE aistock_llm_rdagent_stages
                SET model_id = %s, temperature = %s, max_tokens = %s, updated_at = CURRENT_TIMESTAMP
                WHERE stage_name = %s
            """, (
                update.model_id,
                update.temperature,
                update.max_tokens,
                stage_name
            ))

            # 记录变更
            cursor.execute("""
                INSERT INTO aistock_llm_stage_change_log (stage_name, old_model_id, new_model_id, change_reason)
                VALUES (%s, %s, %s, %s)
            """, (stage_name, old_model_id, update.model_id, "UI更新"))

            conn.commit()
            return {
                "success": True,
                "stage_name": stage_name,
                "full_model_id": model[1],
                "provider_name": model[2]
            }
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            cursor.close()


@router.get("/rdagent/config-preview")
def get_rdagent_config_preview() -> dict[str, Any]:
    """获取RDAgent配置预览（生成的.env内容）"""
    with get_conn() as conn:
        cursor = conn.cursor()

        # 获取阶段配置
        cursor.execute("""
            SELECT s.stage_name, m.full_model_id, s.temperature, s.max_tokens,
                   p.litellm_prefix, ac.api_base, ac.api_key,
                   ac.env_api_base_name, ac.env_api_key_name
            FROM aistock_llm_rdagent_stages s
            LEFT JOIN aistock_llm_models m ON s.model_id = m.id
            LEFT JOIN aistock_llm_providers p ON m.provider_id = p.id
            LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id OR
                   (ac.provider_id = p.id AND ac.config_purpose = 'default')
            WHERE s.is_active = TRUE AND m.is_active = TRUE
        """)

        stage_map = {}
        env_vars = {}

        for row in cursor.fetchall():
            stage_name, full_model_id, temperature, max_tokens = row[0], row[1], row[2], row[3]
            _, api_base, api_key = row[4], row[5], row[6]
            env_api_base_name, env_api_key_name = row[7], row[8]

            if full_model_id:
                stage_map[stage_name] = {
                    "model": full_model_id,
                    "temperature": str(temperature) if temperature else None,
                    "max_tokens": str(max_tokens) if max_tokens else None
                }

            if api_base and env_api_base_name:
                env_vars[env_api_base_name] = api_base
            if api_key and env_api_key_name:
                env_vars[env_api_key_name] = api_key

        cursor.close()

    return {
        "stage_mappings": stage_map,
        "env_variables": env_vars,
        "litellm_chat_model_map": stage_map
    }


# ============================================
# AIstock Agent配置API
# ============================================


@router.get("/aistock/agents")
def list_aistock_agents() -> dict[str, Any]:
    """获取AIstock Agent配置列表"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.id, a.agent_key, a.agent_name, a.agent_type, a.description,
                   a.model_id, m.display_name as model_display_name,
                   m.full_model_id, p.display_name as provider_display_name,
                   a.temperature, a.max_tokens, a.system_prompt, a.is_active
            FROM aistock_llm_aistock_agents a
            LEFT JOIN aistock_llm_models m ON a.model_id = m.id
            LEFT JOIN aistock_llm_providers p ON m.provider_id = p.id
            WHERE a.is_active = TRUE
            ORDER BY a.agent_key
        """)
        agents = []
        for row in cursor.fetchall():
            agents.append({
                "id": row[0],
                "agent_key": row[1],
                "agent_name": row[2],
                "agent_type": row[3],
                "description": row[4],
                "model_id": row[5],
                "model_display_name": row[6],
                "full_model_id": row[7],
                "provider_display_name": row[8],
                "temperature": float(row[9]) if row[9] else None,
                "max_tokens": row[10],
                "system_prompt": row[11],
                "is_active": row[12],
            })
        cursor.close()
    return {"agents": agents}


@router.put("/aistock/agents/{agent_key}")
def update_aistock_agent(agent_key: str, update: AIstockAgentUpdate) -> dict[str, Any]:
    """更新AIstock Agent配置"""
    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            # 验证模型存在
            cursor.execute("""
                SELECT m.id, m.full_model_id, p.provider_name
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                WHERE m.id = %s AND m.is_active = TRUE
            """, (update.model_id,))
            model = cursor.fetchone()
            if not model:
                raise ValueError("模型不存在或未激活")

            # 更新Agent配置
            update_fields = ["model_id = %s", "updated_at = CURRENT_TIMESTAMP"]
            update_values = [update.model_id]

            if update.temperature is not None:
                update_fields.append("temperature = %s")
                update_values.append(update.temperature)
            if update.max_tokens is not None:
                update_fields.append("max_tokens = %s")
                update_values.append(update.max_tokens)
            if update.system_prompt is not None:
                update_fields.append("system_prompt = %s")
                update_values.append(update.system_prompt)

            update_values.append(agent_key)

            cursor.execute(f"""
                UPDATE aistock_llm_aistock_agents
                SET {', '.join(update_fields)}
                WHERE agent_key = %s
            """, update_values)

            conn.commit()
            return {
                "success": True,
                "agent_key": agent_key,
                "full_model_id": model[1],
                "provider_name": model[2]
            }
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            cursor.close()


@router.get("/aistock/agents/{agent_key}/config")
def get_aistock_agent_config(agent_key: str) -> dict[str, Any]:
    """获取单个AIstock Agent的完整配置（用于运行时调用）"""
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.agent_key, a.agent_name, a.temperature, a.max_tokens, a.system_prompt,
                   m.full_model_id, m.model_type,
                   ac.api_base, ac.api_key
            FROM aistock_llm_aistock_agents a
            LEFT JOIN aistock_llm_models m ON a.model_id = m.id
            LEFT JOIN aistock_llm_providers p ON m.provider_id = p.id
            LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id OR
                   (ac.provider_id = p.id AND ac.config_purpose = 'default')
            WHERE a.agent_key = %s AND a.is_active = TRUE
        """, (agent_key,))
        row = cursor.fetchone()
        cursor.close()

        if not row:
            raise HTTPException(status_code=404, detail=f"Agent {agent_key} 不存在")

        return {
            "agent_key": row[0],
            "agent_name": row[1],
            "temperature": float(row[2]) if row[2] else 0.7,
            "max_tokens": row[3] or 4000,
            "system_prompt": row[4],
            "model": row[5],
            "model_type": row[6],
            "api_base": row[7],
            "api_key": row[8],
        }
