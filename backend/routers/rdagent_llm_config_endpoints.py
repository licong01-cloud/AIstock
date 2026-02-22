"""
RDAgent LLM配置管理 - 模型API配置编辑和验证端点
新增功能：
1. 模型API配置编辑
2. 综合模型验证（LiteLLM + RDAgent健康检查）
"""

from typing import Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..db.pg_pool import get_conn

router = APIRouter(prefix="/rdagent/llm-config", tags=["rdagent-llm-config-extended"])


class ModelAPIConfigUpdate(BaseModel):
    """模型API配置更新"""
    model_id: int
    api_key: str
    api_base: str | None = None
    verify_before_save: bool = True


class ModelVerifyRequest(BaseModel):
    """模型验证请求"""
    model_id: int
    api_key: str | None = None
    api_base: str | None = None
    run_health_check: bool = True
    run_litellm_test: bool = True


RDAGENT_API_BASE = "http://127.0.0.1:9000"


def _find_provider_api_config(cursor: Any, provider_id: int) -> dict[str, Any] | None:
    cursor.execute(
        """
        SELECT id, api_key, api_base
        FROM aistock_llm_api_configs
        WHERE provider_id = %s
          AND is_active = true
        ORDER BY
            CASE WHEN config_purpose = 'chat' THEN 0 ELSE 1 END,
            priority DESC,
            updated_at DESC,
            id DESC
        LIMIT 1
        """,
        (provider_id,),
    )
    row = cursor.fetchone()
    if row:
        api_base = row[2]
        if not api_base:
            cursor.execute("SELECT api_base_url FROM aistock_llm_providers WHERE id = %s", (provider_id,))
            p_row = cursor.fetchone()
            if p_row and p_row[0]:
                api_base = p_row[0]
        return {"id": row[0], "api_key": row[1], "api_base": api_base}

    # 添加环境变量回退逻辑
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
            return {"id": new_id, "api_base": final_api_base, "api_key": env_key}

    return None


async def comprehensive_model_verification(
    provider_name: str,
    model_name: str,
    full_model_id: str,
    api_key: str,
    api_base: str | None = None,
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
            
            # 测试调用，直接传递 api_key 和 api_base 给 litellm，避免依赖环境变量前缀猜测
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


@router.post("/models/{model_id}/update-api-config")
async def update_model_api_config(model_id: int, config: ModelAPIConfigUpdate) -> dict[str, Any]:
    """
    更新模型的API配置
    
    功能：
    1. 验证模型是否存在
    2. 可选：验证API配置可用性
    3. 更新或创建API配置到数据库
    4. 关联模型与API配置
    """
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 1. 验证模型是否存在
            cursor.execute("""
                SELECT m.id, m.full_model_id, m.model_name, p.provider_name, p.default_env_prefix
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                WHERE m.id = %s
            """, (model_id,))
            
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"模型ID {model_id} 不存在")
            
            _, full_model_id, model_name, provider_name, env_prefix = row
            
            # 2. 验证API配置（如果需要）
            if config.verify_before_save:
                verification_result = await comprehensive_model_verification(
                    provider_name=provider_name,
                    model_name=model_name,
                    full_model_id=full_model_id,
                    api_key=config.api_key,
                    api_base=config.api_base,
                    run_health_check=True,
                    run_litellm_test=True
                )
                
                if not verification_result["overall_success"]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"API配置验证失败: {', '.join(verification_result['errors'])}"
                    )
            
            # 生成环境变量名
            env_api_base_name = f"{env_prefix.upper()}_API_BASE" if env_prefix else f"{provider_name.upper()}_API_BASE"
            env_api_key_name = f"{env_prefix.upper()}_API_KEY" if env_prefix else f"{provider_name.upper()}_API_KEY"

            # 3. 始终创建模型级覆盖配置，避免修改服务商默认配置影响其他模型
            cursor.execute("""
                INSERT INTO aistock_llm_api_configs
                (provider_id, api_base, api_key, env_api_base_name, env_api_key_name,
                 config_purpose, description)
                VALUES (
                    (SELECT provider_id FROM aistock_llm_models WHERE id = %s),
                    %s, %s, %s, %s, %s,
                    %s
                )
                RETURNING id
            """, (
                model_id,
                config.api_base or '',
                config.api_key,
                env_api_base_name,
                env_api_key_name,
                f'model_{model_id}',
                f'{provider_name} 模型 {model_id} 覆盖API配置',
            ))

            api_config_id = cursor.fetchone()[0]
            
            # 4. 关联模型与API配置
            cursor.execute("""
                UPDATE aistock_llm_models
                SET api_config_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (api_config_id, model_id))
            
            conn.commit()
            cursor.close()
            
            return {
                "success": True,
                "message": "API配置更新成功",
                "model_id": model_id,
                "api_config_id": api_config_id,
                "verification": verification_result if config.verify_before_save else None
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新API配置失败: {str(e)}")


@router.post("/models/verify")
async def verify_model(request: ModelVerifyRequest) -> dict[str, Any]:
    """
    验证模型API配置
    
    功能：
    1. LiteLLM测试调用
    2. RDAgent健康检查
    3. 综合验证结果
    """
    try:
        with get_conn() as conn:
            cursor = conn.cursor()
            
            # 获取模型信息
            cursor.execute("""
                SELECT m.full_model_id, m.model_name, p.provider_name, p.id,
                       ac.api_key, ac.api_base
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
                WHERE m.id = %s
            """, (request.model_id,))
            
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"模型ID {request.model_id} 不存在")
            
            full_model_id, model_name, provider_name, provider_id, db_api_key, db_api_base = row
            
            # 使用提供的API配置或数据库中的配置
            api_key = request.api_key or db_api_key
            api_base = request.api_base or db_api_base
            
            # 无论是否有模型级的 api_key，如果没有 api_base，都需要去尝试回退查找服务商级配置获取
            if not api_key or not api_base:
                provider_api = _find_provider_api_config(cursor, provider_id)
                if provider_api:
                    api_key = api_key or provider_api.get("api_key")
                    api_base = api_base or provider_api.get("api_base")

            if not api_key:
                raise HTTPException(status_code=400, detail="缺少API Key，请提供api_key参数、模型覆盖配置或服务商默认配置")
            
            cursor.close()
        
        # 执行综合验证
        verification_result = await comprehensive_model_verification(
            provider_name=provider_name,
            model_name=model_name,
            full_model_id=full_model_id,
            api_key=api_key,
            api_base=api_base,
            run_health_check=request.run_health_check,
            run_litellm_test=request.run_litellm_test
        )
        
        return {
            "success": True,
            "model_id": request.model_id,
            "full_model_id": full_model_id,
            "verification": verification_result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"模型验证失败: {str(e)}")
