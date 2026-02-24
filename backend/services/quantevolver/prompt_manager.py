"""
QuantEvolver: Agent Prompt Manager（提示词管理器）

功能：
1. 获取所有Agent提示词配置
2. 获取指定Agent的提示词
3. 更新提示词内容
4. 重置为默认提示词
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.prompt_manager")


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

def safe_format(template_str: str, **kwargs) -> str:
    """瀹夊叏鐨勫瓧绗︿覆鏍煎紡鍖栵紝閬垮厤鍥犵己灏戝崰浣嶇鍙橀噺鑰屽紩鍙?KeyError"""
    if not template_str:
        return ""
    return template_str.format_map(SafeDict(**kwargs))

class PromptManager:
    """Agent提示词管理器。"""

    def list_prompts(
        self,
        agent_type: Optional[str] = None,
        active_only: bool = True,
    ) -> Dict[str, Any]:
        """获取提示词列表。"""
        conditions = []
        params: list = []

        if agent_type:
            conditions.append("agent_type = %s")
            params.append(agent_type)
        if active_only:
            conditions.append("is_active = TRUE")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT id, agent_type, prompt_key, display_name, description,
                               system_prompt, user_prompt_template,
                               is_active, version, created_at, updated_at
                        FROM qe_agent_prompts
                        WHERE {where_clause}
                        ORDER BY agent_type, prompt_key""",
                    params,
                )
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 序列化datetime
        for row in rows:
            for k in ("created_at", "updated_at"):
                if row.get(k) and isinstance(row[k], datetime):
                    row[k] = row[k].isoformat()

        return {"ok": True, "items": rows, "total": len(rows)}

    def get_prompt(self, agent_type: str, prompt_key: str) -> Dict[str, Any]:
        """获取指定提示词。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, agent_type, prompt_key, display_name, description,
                              system_prompt, user_prompt_template,
                              is_active, version, created_at, updated_at
                       FROM qe_agent_prompts
                       WHERE agent_type = %s AND prompt_key = %s""",
                    (agent_type, prompt_key),
                )
                row = cur.fetchone()
                if not row:
                    return {"ok": False, "error": f"提示词 {agent_type}/{prompt_key} 不存在"}
                cols = [desc[0] for desc in cur.description]
                data = dict(zip(cols, row))

        for k in ("created_at", "updated_at"):
            if data.get(k) and isinstance(data[k], datetime):
                data[k] = data[k].isoformat()

        return {"ok": True, **data}

    def update_prompt(
        self,
        agent_type: str,
        prompt_key: str,
        system_prompt: Optional[str] = None,
        user_prompt_template: Optional[str] = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """更新提示词。"""
        updates = []
        params: list = []

        if system_prompt is not None:
            updates.append("system_prompt = %s")
            params.append(system_prompt)
        if user_prompt_template is not None:
            updates.append("user_prompt_template = %s")
            params.append(user_prompt_template)
        if display_name is not None:
            updates.append("display_name = %s")
            params.append(display_name)
        if description is not None:
            updates.append("description = %s")
            params.append(description)
        if is_active is not None:
            updates.append("is_active = %s")
            params.append(is_active)

        if not updates:
            return {"ok": False, "error": "没有需要更新的字段"}

        updates.append("version = version + 1")
        updates.append("updated_at = NOW()")

        set_clause = ", ".join(updates)
        params.extend([agent_type, prompt_key])

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE qe_agent_prompts
                        SET {set_clause}
                        WHERE agent_type = %s AND prompt_key = %s
                        RETURNING id, version""",
                    params,
                )
                result = cur.fetchone()
                if not result:
                    return {"ok": False, "error": f"提示词 {agent_type}/{prompt_key} 不存在"}

        return {"ok": True, "id": result[0], "version": result[1],
                "message": f"提示词 {agent_type}/{prompt_key} 已更新到版本 {result[1]}"}

    def create_prompt(
        self,
        agent_type: str,
        prompt_key: str,
        display_name: str,
        system_prompt: str = "",
        user_prompt_template: str = "",
        description: str = "",
    ) -> Dict[str, Any]:
        """创建新提示词。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """INSERT INTO qe_agent_prompts
                               (agent_type, prompt_key, display_name, description,
                                system_prompt, user_prompt_template)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           RETURNING id""",
                        (agent_type, prompt_key, display_name, description,
                         system_prompt, user_prompt_template),
                    )
                    new_id = cur.fetchone()[0]
                    return {"ok": True, "id": new_id,
                            "message": f"提示词 {agent_type}/{prompt_key} 已创建"}
                except Exception as e:
                    if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                        return {"ok": False, "error": f"提示词 {agent_type}/{prompt_key} 已存在"}
                    raise

    def delete_prompt(self, prompt_id: int) -> Dict[str, Any]:
        """删除提示词。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM qe_agent_prompts WHERE id = %s RETURNING agent_type, prompt_key",
                    (prompt_id,),
                )
                result = cur.fetchone()
                if not result:
                    return {"ok": False, "error": f"提示词ID {prompt_id} 不存在"}

        return {"ok": True, "message": f"提示词 {result[0]}/{result[1]} 已删除"}

    def get_active_prompt_text(self, agent_type: str, prompt_key: str) -> Optional[Dict[str, str]]:
        """获取激活状态的提示词文本（供其他服务调用）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT system_prompt, user_prompt_template
                       FROM qe_agent_prompts
                       WHERE agent_type = %s AND prompt_key = %s AND is_active = TRUE""",
                    (agent_type, prompt_key),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {"system_prompt": row[0], "user_prompt_template": row[1]}
