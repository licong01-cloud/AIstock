"""
LLM模型同步服务
从服务商API获取模型列表并同步到数据库
"""
import logging
from typing import Any

import httpx
from pydantic import BaseModel

from backend.db.pg_pool import get_conn

logger = logging.getLogger(__name__)


class ModelInfo(BaseModel):
    """模型信息"""
    model_name: str
    model_type: str = "chat"  # chat, embedding, reasoner, vision
    context_window: int | None = None
    max_output_tokens: int | None = None
    input_price: float | None = None
    output_price: float | None = None


class ProviderModelSync:
    """服务商模型同步服务"""

    # 模型类型推断规则（根据模型名关键词）
    MODEL_TYPE_KEYWORDS = {
        "embedding": ["embed", "bge", "e5", "text-embedding"],
        "reasoner": ["o1", "reasoner", "think"],
        "vision": ["vision", "vl", "gpt-4v", "gpt-4-vision", "qwen-vl", "glm-4v"],
    }

    def __init__(self, provider_id: int):
        self.provider_id = provider_id
        self.provider_info: dict[str, Any] = {}
        self.api_config: dict[str, Any] = {}

    def _load_provider_info(self) -> bool:
        """加载服务商信息"""
        with get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, provider_name, display_name, litellm_prefix, api_base_url,
                       provider_type, use_proxy, proxy_model_prefix
                FROM aistock_llm_providers
                WHERE id = %s AND is_active = TRUE
            """, (self.provider_id,))
            row = cursor.fetchone()
            if not row:
                return False
            self.provider_info = {
                "id": row[0],
                "provider_name": row[1],
                "display_name": row[2],
                "litellm_prefix": row[3],
                "api_base_url": row[4],
                "provider_type": row[5],
                "use_proxy": row[6],
                "proxy_model_prefix": row[7],
            }

            # 获取API配置
            cursor.execute("""
                SELECT api_base, api_key
                FROM aistock_llm_api_configs
                WHERE provider_id = %s AND is_active = TRUE
                ORDER BY priority DESC, id
                LIMIT 1
            """, (self.provider_id,))
            api_row = cursor.fetchone()
            if api_row:
                self.api_config = {
                    "api_base": api_row[0],
                    "api_key": api_row[1],
                }
            else:
                # 使用服务商默认配置
                self.api_config = {
                    "api_base": self.provider_info["api_base_url"],
                    "api_key": None,
                }

            cursor.close()
        return True

    def _infer_model_type(self, model_name: str) -> str:
        """根据模型名推断模型类型"""
        model_name_lower = model_name.lower()

        for model_type, keywords in self.MODEL_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in model_name_lower:
                    return model_type

        return "chat"  # 默认为对话模型

    async def fetch_models_from_api(
        self,
        model_type_filter: str | None = None,
        api_base_override: str | None = None,
        api_key_override: str | None = None
    ) -> list[ModelInfo]:
        """从服务商API获取模型列表"""
        api_base = api_base_override or self.api_config.get("api_base")
        api_key = api_key_override or self.api_config.get("api_key")

        if not api_base:
            raise ValueError(f"服务商 {self.provider_info['provider_name']} 缺少API Base配置")

        models = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            headers = {}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            try:
                response = await client.get(
                    f"{api_base.rstrip('/')}/models",
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()

                for model_data in data.get("data", []):
                    model_id = model_data.get("id", "")
                    if not model_id:
                        continue

                    model_type = self._infer_model_type(model_id)

                    # 类型过滤
                    if model_type_filter and model_type_filter != "all":
                        if model_type != model_type_filter:
                            continue

                    models.append(ModelInfo(
                        model_name=model_id,
                        model_type=model_type,
                        context_window=model_data.get("context_window"),
                        max_output_tokens=model_data.get("max_output_tokens"),
                    ))

            except httpx.HTTPStatusError as e:
                logger.error(f"获取模型列表失败: {e}")
                raise
            except httpx.RequestError as e:
                logger.error(f"请求服务商API失败: {e}")
                raise

        return models

    def sync_models_to_db(
        self,
        models: list[ModelInfo],
        overwrite: bool = False
    ) -> dict[str, Any]:
        """同步模型到数据库"""
        imported_count = 0
        skipped_count = 0
        updated_count = 0
        imported_models = []

        with get_conn() as conn:
            cursor = conn.cursor()

            for model in models:
                # 生成显示名称
                display_name = f"{model.model_name} ({self.provider_info['display_name']})"

                # 生成full_model_id
                litellm_prefix = self.provider_info["litellm_prefix"]
                full_model_id = f"{litellm_prefix}/{model.model_name}"

                # 生成Proxy模型别名（如果需要）
                proxy_model_alias = None
                if self.provider_info.get("use_proxy") and self.provider_info.get("proxy_model_prefix"):
                    proxy_model_alias = f"{self.provider_info['proxy_model_prefix']}-{model.model_name.replace('/', '-')}"

                # 检查是否已存在
                cursor.execute("""
                    SELECT id FROM aistock_llm_models
                    WHERE provider_id = %s AND model_name = %s
                """, (self.provider_id, model.model_name))

                existing = cursor.fetchone()

                if existing:
                    if overwrite:
                        # 更新现有记录
                        cursor.execute("""
                            UPDATE aistock_llm_models SET
                                display_name = %s,
                                full_model_id = %s,
                                model_type = %s,
                                context_window = %s,
                                max_output_tokens = %s,
                                input_price = %s,
                                output_price = %s,
                                proxy_model_alias = %s,
                                is_synced = TRUE,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (
                            display_name, full_model_id, model.model_type,
                            model.context_window, model.max_output_tokens,
                            model.input_price, model.output_price,
                            proxy_model_alias, existing[0]
                        ))
                        updated_count += 1
                    else:
                        skipped_count += 1
                        continue
                else:
                    # 插入新记录
                    cursor.execute("""
                        INSERT INTO aistock_llm_models (
                            provider_id, model_name, display_name, full_model_id,
                            model_type, context_window, max_output_tokens,
                            input_price, output_price, proxy_model_alias, is_synced, is_active
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, TRUE)
                        RETURNING id
                    """, (
                        self.provider_id, model.model_name, display_name, full_model_id,
                        model.model_type, model.context_window, model.max_output_tokens,
                        model.input_price, model.output_price, proxy_model_alias
                    ))
                    result = cursor.fetchone()
                    imported_count += 1
                    imported_models.append({
                        "id": result[0],
                        "model_name": model.model_name,
                        "full_model_id": full_model_id,
                    })

            conn.commit()
            cursor.close()

        return {
            "imported_count": imported_count,
            "skipped_count": skipped_count,
            "updated_count": updated_count,
            "imported_models": imported_models,
        }


async def fetch_provider_models(
    provider_id: int,
    model_type_filter: str | None = None,
    api_base_override: str | None = None,
    api_key_override: str | None = None
) -> dict[str, Any]:
    """
    从服务商API获取模型列表（不保存到数据库）

    Args:
        provider_id: 服务商ID
        model_type_filter: 模型类型过滤 (chat/embedding/reasoner/vision/all)
        api_base_override: 覆盖API Base
        api_key_override: 覆盖API Key

    Returns:
        包含模型列表的字典
    """
    sync_service = ProviderModelSync(provider_id)

    if not sync_service._load_provider_info():
        return {
            "success": False,
            "error": f"服务商ID {provider_id} 不存在或未激活"
        }

    try:
        models = await sync_service.fetch_models_from_api(
            model_type_filter=model_type_filter,
            api_base_override=api_base_override,
            api_key_override=api_key_override
        )

        return {
            "success": True,
            "provider_name": sync_service.provider_info["provider_name"],
            "provider_display_name": sync_service.provider_info["display_name"],
            "models": [m.model_dump() for m in models],
            "total": len(models),
        }

    except Exception as e:
        logger.error(f"获取服务商模型列表失败: {e}")
        return {
            "success": False,
            "error": str(e)
        }


async def sync_provider_models(
    provider_id: int,
    model_type_filter: str | None = None,
    api_base_override: str | None = None,
    api_key_override: str | None = None,
    overwrite: bool = False
) -> dict[str, Any]:
    """
    从服务商API获取模型列表并同步到数据库

    Args:
        provider_id: 服务商ID
        model_type_filter: 模型类型过滤
        api_base_override: 覆盖API Base
        api_key_override: 覆盖API Key
        overwrite: 是否覆盖已存在的模型

    Returns:
        同步结果
    """
    sync_service = ProviderModelSync(provider_id)

    if not sync_service._load_provider_info():
        return {
            "success": False,
            "error": f"服务商ID {provider_id} 不存在或未激活"
        }

    try:
        models = await sync_service.fetch_models_from_api(
            model_type_filter=model_type_filter,
            api_base_override=api_base_override,
            api_key_override=api_key_override
        )

        result = sync_service.sync_models_to_db(models, overwrite=overwrite)

        return {
            "success": True,
            "provider_name": sync_service.provider_info["provider_name"],
            "provider_display_name": sync_service.provider_info["display_name"],
            **result
        }

    except Exception as e:
        logger.error(f"同步服务商模型失败: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def batch_import_models(
    provider_id: int,
    models: list[dict[str, Any]]
) -> dict[str, Any]:
    """
    批量导入模型到数据库

    Args:
        provider_id: 服务商ID
        models: 模型列表，每个模型包含 model_name, display_name, model_type 等字段

    Returns:
        导入结果
    """
    sync_service = ProviderModelSync(provider_id)

    if not sync_service._load_provider_info():
        return {
            "success": False,
            "error": f"服务商ID {provider_id} 不存在或未激活"
        }

    model_infos = [ModelInfo(**m) for m in models]
    result = sync_service.sync_models_to_db(model_infos, overwrite=False)

    return {
        "success": True,
        "provider_name": sync_service.provider_info["provider_name"],
        **result
    }
