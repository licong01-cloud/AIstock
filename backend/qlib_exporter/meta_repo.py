from __future__ import annotations

"""Qlib 导出增量元数据管理.

记录每个 snapshot_id + data_type 的已导出的最后 datetime，用于增量导出。
支持分钟线和 TDX 板块数据的增量导出。
"""

from datetime import datetime
from typing import Dict, List, Optional

from app_pg import get_conn  # type: ignore[attr-defined]


class MetaRepo:
    """增量导出元数据管理.
    
    表结构：
    - snapshot_id: Snapshot 标识
    - data_type: 数据类型（minute_1m, board_daily, board_index, board_member）
    - last_datetime: 已导出的最后时间点
    """
    
    TABLE = "market.qlib_export_meta"

    def ensure_table(self) -> None:
        """确保元数据表存在."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE} (
            snapshot_id   text        NOT NULL,
            data_type     text        NOT NULL,
            last_datetime timestamptz NOT NULL,
            updated_at    timestamptz DEFAULT NOW(),
            PRIMARY KEY (snapshot_id, data_type)
        );
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

    def get_last_datetime(
        self, snapshot_id: str, data_type: str
    ) -> Optional[datetime]:
        """获取指定 snapshot + data_type 的最后导出时间."""
        sql = f"""
            SELECT last_datetime
            FROM {self.TABLE}
            WHERE snapshot_id = %s AND data_type = %s
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql, (snapshot_id, data_type))
                row = cur.fetchone()
        return row[0] if row else None

    def upsert_last_datetime(
        self, snapshot_id: str, data_type: str, dt: datetime
    ) -> None:
        """更新或插入最后导出时间."""
        sql = f"""
            INSERT INTO {self.TABLE} (snapshot_id, data_type, last_datetime, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (snapshot_id, data_type)
            DO UPDATE SET last_datetime = EXCLUDED.last_datetime, updated_at = NOW()
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql, (snapshot_id, data_type, dt))
            conn.commit()

    def get_all_meta(self, snapshot_id: str) -> Dict[str, datetime]:
        """获取指定 snapshot 的所有数据类型的最后导出时间."""
        sql = f"""
            SELECT data_type, last_datetime
            FROM {self.TABLE}
            WHERE snapshot_id = %s
        """
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql, (snapshot_id,))
                rows = cur.fetchall()
        return {row[0]: row[1] for row in rows}

    def delete_meta(self, snapshot_id: str, data_type: Optional[str] = None) -> int:
        """删除元数据记录.
        
        如果 data_type 为 None，删除该 snapshot 的所有记录。
        """
        if data_type:
            sql = f"DELETE FROM {self.TABLE} WHERE snapshot_id = %s AND data_type = %s"
            params = (snapshot_id, data_type)
        else:
            sql = f"DELETE FROM {self.TABLE} WHERE snapshot_id = %s"
            params = (snapshot_id,)
        
        with get_conn() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute(sql, params)
                deleted = cur.rowcount
            conn.commit()
        return deleted
