import logging
from typing import Any, Dict, Optional

from backend.db.pg_pool import get_conn
from backend.infra.deepseek_config import DEFAULT_DEEPSEEK_LITELLM_MODEL

logger = logging.getLogger(__name__)

def _find_provider_api_config(
    cursor: Any, provider_id: int, model_type: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """按用途优先级查找服务商可用API配置"""
    purposes = []
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
            """,
            (provider_id, purpose),
        )
        row = cursor.fetchone()
        if row:
            return {
                "id": row[0],
                "api_base": row[1],
                "api_key": row[2],
                "env_api_base_name": row[3],
                "env_api_key_name": row[4],
            }
    return None

def get_llm_kwargs(agent_type: str, default_model: str = DEFAULT_DEEPSEEK_LITELLM_MODEL) -> Dict[str, Any]:
    """
    获取传递给 litellm.completion 的 kwargs (model, api_key, api_base等)。
    通过查询 QE的agent配置 -> aistock_llm_models -> 对应的API凭证。
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 获取 agent 绑定的 model_id
                cur.execute(
                    "SELECT model_id FROM qe_agent_model_config WHERE agent_type = %s",
                    (agent_type,),
                )
                row = cur.fetchone()
                model_id_db = row[0] if row else None

                if model_id_db and str(model_id_db).isdigit():
                    # 2. 查询 aistock_llm_models
                    cur.execute(
                        """
                        SELECT m.full_model_id, m.provider_id, m.model_type, m.api_config_id,
                               ac.api_base, ac.api_key
                        FROM aistock_llm_models m
                        LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
                        WHERE m.id = %s AND m.is_active = true
                        """,
                        (int(model_id_db),),
                    )
                    m_row = cur.fetchone()
                    if m_row:
                        full_model_id, provider_id, model_type, api_config_id, api_base, api_key = m_row

                        # 如果模型自身没有绑定API配置，尝试使用服务商级别的API配置
                        if not api_key or not api_base:
                            if provider_id:
                                p_api = _find_provider_api_config(cur, provider_id, model_type)
                                if p_api:
                                    api_base = api_base or p_api.get("api_base")
                                    api_key = api_key or p_api.get("api_key")

                        kwargs = {"model": full_model_id}
                        if api_base:
                            kwargs["api_base"] = api_base
                        if api_key:
                            kwargs["api_key"] = api_key
                        return kwargs
                elif model_id_db and isinstance(model_id_db, str) and model_id_db.strip():
                    # Fallback for old str format (e.g. direct full_model_id like "deepseek/deepseek-v4-pro")
                    return {"model": model_id_db}

    except Exception as e:
        raise RuntimeError(f"获取Agent '{agent_type}' 的LLM配置失败: {e}") from e

    # 未找到任何配置 → 使用默认模型
    return {"model": default_model}
