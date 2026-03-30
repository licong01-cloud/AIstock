"""RD-Agent LLM Configuration Management API Router V2 - 增强版
通过RD-Agent API实现配置管理，不再直接操作.env文件
"""

from typing import Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.pg_pool import get_conn
from services.rdagent_llm_config_client import get_llm_config_client

router = APIRouter(prefix="/rdagent/llm-config-v2", tags=["rdagent-llm-config-v2"])


def _find_provider_api_config(
    cursor: Any,
    provider_id: int,
    model_type: str | None = None,
) -> dict[str, Any] | None:
    """按用途优先级查找服务商可用API配置。"""
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


class StageMappingUpdate(BaseModel):
    stage_name: str
    model_id: int | None = None
    temperature: float | None = None
    max_tokens: int | None = None


class ConfigUpdateV2(BaseModel):
    stage_mappings: list[StageMappingUpdate]
    embedding_model_id: int | None = None
    change_reason: str


@router.post("/update-config")
async def update_config_v2(config: ConfigUpdateV2) -> dict[str, Any]:
    """增强版配置更新，支持API配置联动
    通过RD-Agent API实现，不再直接操作.env文件

    功能：
    1. 更新 LITELLM_CHAT_MODEL_MAP
    2. 更新 LITELLM_EMBEDDING_MODEL
    3. 自动更新所有相关的 API_BASE 和 API_KEY
    4. 配置完整性验证
    5. 通过RD-Agent API管理备份与回滚
    """
    client = None

    try:
        # 1. 获取API客户端
        client = get_llm_config_client()

        with get_conn() as conn:
            cursor = conn.cursor()

            # 2. 验证所有模型ID是否存在
            for mapping in config.stage_mappings:
                if mapping.model_id:
                    cursor.execute(
                        "SELECT id, full_model_id, api_config_id, provider_id, model_type FROM aistock_llm_models WHERE id = %s",
                        (mapping.model_id,)
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
                            detail=f"模型 {row[1]} 缺少API配置，请先为该模型配置API"
                        )

            # 验证embedding模型
            if config.embedding_model_id:
                cursor.execute(
                    "SELECT id, full_model_id, api_config_id, provider_id, model_type FROM aistock_llm_models WHERE id = %s",
                    (config.embedding_model_id,)
                )
                row = cursor.fetchone()
                if not row:
                    raise HTTPException(status_code=400, detail=f"Embedding模型ID {config.embedding_model_id} 不存在")

                api_config_id = row[2]
                if not api_config_id:
                    provider_api = _find_provider_api_config(
                        cursor=cursor,
                        provider_id=row[3],
                        model_type=row[4],
                    )
                    api_config_id = provider_api["id"] if provider_api else None

                if not api_config_id:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Embedding模型 {row[1]} 缺少API配置，请先为该模型配置API"
                    )

            # 3. 更新数据库中的阶段映射
            for mapping in config.stage_mappings:
                cursor.execute(
                    "SELECT id, model_id FROM aistock_llm_stage_mappings WHERE stage_name = %s",
                    (mapping.stage_name,)
                )
                row = cursor.fetchone()

                old_model_id = row[1] if row else None

                if row:
                    cursor.execute("""
                        UPDATE aistock_llm_stage_mappings
                        SET model_id = %s, temperature = %s, max_tokens = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE stage_name = %s
                    """, (mapping.model_id, mapping.temperature, mapping.max_tokens, mapping.stage_name))
                else:
                    cursor.execute("""
                        INSERT INTO aistock_llm_stage_mappings
                        (stage_name, model_id, temperature, max_tokens)
                        VALUES (%s, %s, %s, %s)
                    """, (mapping.stage_name, mapping.model_id, mapping.temperature, mapping.max_tokens))

                # 记录变更历史
                if old_model_id != mapping.model_id:
                    cursor.execute("""
                        INSERT INTO aistock_llm_config_change_log
                        (stage_name, old_model_id, new_model_id, change_reason)
                        VALUES (%s, %s, %s, %s)
                    """, (mapping.stage_name, old_model_id, mapping.model_id, config.change_reason))

            # 4. 获取所有模型信息构建API更新
            cursor.execute("""
                SELECT sm.stage_name, m.full_model_id, sm.temperature, sm.max_tokens,
                       m.provider_id, m.model_type,
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
                provider_id, model_type = row[4], row[5]
                api_base, api_key, env_api_base_name, env_api_key_name = row[6], row[7], row[8], row[9]

                if (not api_key or not api_base) and provider_id:
                    provider_api = _find_provider_api_config(cursor, provider_id, model_type)
                    if provider_api:
                        api_base = api_base or provider_api.get("api_base")
                        api_key = api_key or provider_api.get("api_key")
                        env_api_base_name = env_api_base_name or provider_api.get("env_api_base_name")
                        env_api_key_name = env_api_key_name or provider_api.get("env_api_key_name")

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
                    
                # 自动为 LiteLLM 的路由前缀注入对应的环境变量（例如 openai/xxx -> OPENAI_API_KEY）
                if full_model_id and "/" in full_model_id:
                    litellm_prefix = full_model_id.split("/", 1)[0].upper()
                    if api_base:
                        api_credentials[f"{litellm_prefix}_API_BASE"] = api_base
                    if api_key:
                        api_credentials[f"{litellm_prefix}_API_KEY"] = api_key

            # 5. 处理embedding模型配置
            if config.embedding_model_id:
                cursor.execute(
                    "SELECT id, model_id FROM aistock_llm_stage_mappings WHERE stage_name = 'embedding'"
                )
                row = cursor.fetchone()
                old_embedding_model_id = row[1] if row else None
                
                if row:
                    cursor.execute("""
                        UPDATE aistock_llm_stage_mappings
                        SET model_id = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE stage_name = 'embedding'
                    """, (config.embedding_model_id,))
                else:
                    cursor.execute("""
                        INSERT INTO aistock_llm_stage_mappings
                        (stage_name, model_id)
                        VALUES ('embedding', %s)
                    """, (config.embedding_model_id,))
                    
                if old_embedding_model_id != config.embedding_model_id:
                    cursor.execute("""
                        INSERT INTO aistock_llm_config_change_log
                        (stage_name, old_model_id, new_model_id, change_reason)
                        VALUES ('embedding', %s, %s, %s)
                    """, (old_embedding_model_id, config.embedding_model_id, config.change_reason))

                cursor.execute("""
                    SELECT m.full_model_id, m.provider_id, m.model_type,
                           ac.api_base, ac.api_key,
                           ac.env_api_base_name, ac.env_api_key_name
                    FROM aistock_llm_models m
                    LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
                    WHERE m.id = %s
                """, (config.embedding_model_id,))

                row = cursor.fetchone()
                if row:
                    full_model_id, provider_id, model_type, api_base, api_key, env_api_base_name, env_api_key_name = row
                    if not api_key or not api_base:
                        provider_api = _find_provider_api_config(cursor, provider_id, model_type)
                        if provider_api:
                            api_base = api_base or provider_api.get("api_base")
                            api_key = api_key or provider_api.get("api_key")
                            env_api_base_name = env_api_base_name or provider_api.get("env_api_base_name")
                            env_api_key_name = env_api_key_name or provider_api.get("env_api_key_name")
                    if full_model_id:
                        stage_map["embedding"] = {"model": full_model_id}
                    if api_base and env_api_base_name:
                        api_credentials[env_api_base_name] = api_base
                    if api_key and env_api_key_name:
                        api_credentials[env_api_key_name] = api_key
                        
                    # 自动为 LiteLLM 的路由前缀注入对应的环境变量（例如 openai/xxx -> OPENAI_API_KEY）
                    if full_model_id and "/" in full_model_id:
                        litellm_prefix = full_model_id.split("/", 1)[0].upper()
                        if api_base:
                            api_credentials[f"{litellm_prefix}_API_BASE"] = api_base
                        if api_key:
                            api_credentials[f"{litellm_prefix}_API_KEY"] = api_key

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
                backup_reason="aistock_update_v2",
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
                "message": "配置更新成功（通过RD-Agent API）",
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


@router.get("/validate-config")
def validate_config() -> dict[str, Any]:
    """
    验证当前配置的完整性
    检查所有模型是否都有对应的API配置
    """
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            errors = []
            warnings = []
            
            # 检查所有激活的模型
            cursor.execute("""
                SELECT m.id, m.full_model_id, m.model_type, m.api_config_id,
                       p.provider_name, p.id
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                WHERE m.is_active = true
            """)
            
            for row in cursor.fetchall():
                model_id, full_model_id, model_type, api_config_id, provider_name, provider_id = row
                
                if not api_config_id:
                    provider_api = _find_provider_api_config(cursor, provider_id, model_type)
                    if not provider_api:
                        errors.append(f"模型 {full_model_id} 缺少API配置")
                else:
                    # 检查API配置是否完整
                    cursor.execute("""
                        SELECT api_base, api_key, env_api_base_name, env_api_key_name
                        FROM aistock_llm_api_configs
                        WHERE id = %s
                    """, (api_config_id,))
                    
                    config_row = cursor.fetchone()
                    if config_row:
                        api_base, api_key, env_base_name, env_key_name = config_row
                        
                        if not api_key:
                            errors.append(f"模型 {full_model_id} 的API配置缺少API Key")
                        if model_type != 'embedding' and not api_base:
                            warnings.append(f"模型 {full_model_id} 的API配置缺少API Base（某些服务商可使用默认值）")
            
            cursor.close()
            
            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
