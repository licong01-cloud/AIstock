"""
RDAgent LLM配置管理 - 模型API配置编辑和验证端点
新增功能：
1. 模型API配置编辑
2. 综合模型验证（LiteLLM + RDAgent健康检查）
"""

from typing import Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import os

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
            
            # 3. 查找或创建API配置
            # 先查找是否已有配置
            cursor.execute("""
                SELECT id FROM aistock_llm_api_configs
                WHERE provider_id = (SELECT provider_id FROM aistock_llm_models WHERE id = %s)
                AND config_purpose = 'chat'
                AND is_active = true
                LIMIT 1
            """, (model_id,))
            
            existing_config = cursor.fetchone()
            
            # 生成环境变量名
            env_api_base_name = f"{env_prefix.upper()}_API_BASE" if env_prefix else f"{provider_name.upper()}_API_BASE"
            env_api_key_name = f"{env_prefix.upper()}_API_KEY" if env_prefix else f"{provider_name.upper()}_API_KEY"
            
            if existing_config:
                # 更新现有配置
                api_config_id = existing_config[0]
                cursor.execute("""
                    UPDATE aistock_llm_api_configs
                    SET api_base = %s, api_key = %s, 
                        env_api_base_name = %s, env_api_key_name = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (config.api_base or '', config.api_key, env_api_base_name, env_api_key_name, api_config_id))
            else:
                # 创建新配置
                cursor.execute("""
                    INSERT INTO aistock_llm_api_configs
                    (provider_id, api_base, api_key, env_api_base_name, env_api_key_name, 
                     config_purpose, description)
                    VALUES (
                        (SELECT provider_id FROM aistock_llm_models WHERE id = %s),
                        %s, %s, %s, %s, 'chat',
                        %s
                    )
                    RETURNING id
                """, (model_id, config.api_base or '', config.api_key, 
                      env_api_base_name, env_api_key_name,
                      f'{provider_name}模型API配置'))
                
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
                SELECT m.full_model_id, m.model_name, p.provider_name,
                       ac.api_key, ac.api_base
                FROM aistock_llm_models m
                JOIN aistock_llm_providers p ON m.provider_id = p.id
                LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
                WHERE m.id = %s
            """, (request.model_id,))
            
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"模型ID {request.model_id} 不存在")
            
            full_model_id, model_name, provider_name, db_api_key, db_api_base = row
            
            # 使用提供的API配置或数据库中的配置
            api_key = request.api_key or db_api_key
            api_base = request.api_base or db_api_base
            
            if not api_key:
                raise HTTPException(status_code=400, detail="缺少API Key，请提供api_key参数或在数据库中配置")
            
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
