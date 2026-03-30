from typing import Any, Dict, List

from fastapi import APIRouter

from ..db.pg_pool import get_conn


router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("", summary="获取所有TASK列表")
def list_tasks() -> Dict[str, Any]:
    """获取所有TASK列表"""
    try:
        # 从数据库查询所有tasks
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, name, created_at, updated_at
            FROM app.tasks
            ORDER BY created_at DESC
        """)
        
        tasks = []
        for row in cursor.fetchall():
            tasks.append({
                "id": row[0],
                "name": row[1],
                "createdAt": row[2].isoformat() if row[2] else None,
                "updatedAt": row[3].isoformat() if row[3] else None
            })
        
        cursor.close()
        conn.close()
        
        return {"tasks": tasks}
    except Exception as e:
        # 如果表不存在，返回空列表
        return {"tasks": []}
