from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2.extras as pg_extras

from ..db.pg_pool import get_conn


class StockAnalysisRepoPG:
    """PostgreSQL-backed stock analysis repository for next_app.

    逻辑基本保持与根目录 pg_stock_analysis_repo.PgStockAnalysisRepository 一致，
    但内部使用 next_app.backend.db.pg_pool.get_conn 连接池。
    """

    def _sanitize_json(self, obj: Any) -> Any:
        try:
            import numpy as np  # type: ignore

            np_available = True
        except Exception:  # noqa: BLE001
            np_available = False

        if isinstance(obj, dict):
            return {k: self._sanitize_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            t = [self._sanitize_json(v) for v in obj]
            return type(obj)(t) if isinstance(obj, tuple) else t

        if np_available:
            import numpy as np  # type: ignore

            if isinstance(obj, np.floating):
                if np.isnan(obj) or np.isinf(obj):  # type: ignore[attr-defined]
                    return None
                return float(obj)
            if isinstance(obj, np.integer):  # type: ignore[attr-defined]
                return int(obj)
            if isinstance(obj, np.bool_):  # type: ignore[attr-defined]
                return bool(obj)

        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj

        return obj

    def _safe_dumps(self, payload: Any) -> str:
        cleaned = self._sanitize_json(payload)
        # allow_nan=False ensures 不产生 NaN/Infinity JSON
        return json.dumps(cleaned, ensure_ascii=False, allow_nan=False, default=str)

    def save_analysis(
        self,
        symbol: str,
        stock_name: str,
        period: str,
        stock_info: Dict[str, Any],
        agents_results: Dict[str, Any],
        discussion_result: Dict[str, Any],
        final_decision: Dict[str, Any],
    ) -> int:
        """Persist a single analysis record into app.analysis_records and return its id."""

        analysis_dt = datetime.now(timezone.utc)
        sql = (
            "INSERT INTO app.analysis_records (ts_code, stock_name, period, analysis_date, "
            "stock_info, agents_results, discussion_result, final_decision) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        symbol,
                        stock_name,
                        period,
                        analysis_dt,
                        pg_extras.Json(stock_info, dumps=self._safe_dumps),
                        pg_extras.Json(agents_results, dumps=self._safe_dumps),
                        pg_extras.Json(discussion_result, dumps=self._safe_dumps),
                        pg_extras.Json(final_decision, dumps=self._safe_dumps),
                    ),
                )
                rid = cur.fetchone()[0]
                return int(rid)

    def get_record_count(self) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM app.analysis_records")
                return int(cur.fetchone()[0])

    def get_all_records(self) -> List[Dict[str, Any]]:
        sql = (
            "SELECT id, ts_code, stock_name, analysis_date, period, final_decision, created_at "
            "FROM app.analysis_records ORDER BY created_at DESC"
        )
        out: List[Dict[str, Any]] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    fid = r[0]
                    symbol = r[1] or ""
                    stock_name = r[2] or ""
                    analysis_date = r[3]
                    period = r[4] or ""
                    final_decision = r[5]
                    created_at = r[6]
                    rating = "未知"
                    if isinstance(final_decision, dict):
                        rating = final_decision.get("rating", "未知")
                    out.append(
                        {
                            "id": fid,
                            "symbol": symbol,
                            "stock_name": stock_name,
                            "analysis_date": analysis_date.isoformat() if analysis_date else None,
                            "period": period,
                            "rating": rating,
                            "created_at": created_at.isoformat() if created_at else None,
                        }
                    )
        return out

    def list_records(
        self,
        symbol_or_name: Optional[str] = None,
        page: int = 1,
        page_size: int = 10,
        rating: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """分页查询历史分析记录，支持按代码/名称、评级与日期区间过滤。

        - symbol_or_name: 若提供，则在 ts_code 和 stock_name 上做 ILIKE 模糊匹配；
        - rating: 若提供，则按 final_decision.rating 精确过滤；
        - start_date / end_date: 若提供，则按 analysis_date::date 做闭区间筛选；
        - 结果按 created_at DESC 排序，并返回总条数与当前页记录列表。
        """

        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 100))

        where_clauses: list[str] = []
        params: list[Any] = []
        if symbol_or_name:
            where_clauses.append("(ts_code ILIKE %s OR stock_name ILIKE %s)")
            like = f"%{symbol_or_name}%"
            params.extend([like, like])
        if rating:
            where_clauses.append("(final_decision->>'rating' = %s)")
            params.append(rating)
        if start_date:
            where_clauses.append("(analysis_date::date >= %s)")
            params.append(start_date)
        if end_date:
            where_clauses.append("(analysis_date::date <= %s)")
            params.append(end_date)

        where = ""
        if where_clauses:
            where = "WHERE " + " AND ".join(where_clauses)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM app.analysis_records {where}",
                    params,
                )
                total = int(cur.fetchone()[0])

                offset = max(0, (page - 1) * page_size)
                limit = page_size

                sql = (
                    "SELECT id, ts_code, stock_name, analysis_date, period, "
                    "final_decision, created_at "
                    "FROM app.analysis_records "
                    f"{where} "
                    "ORDER BY created_at DESC "
                    "OFFSET %s LIMIT %s"
                )
                cur.execute(sql, params + [offset, limit])

                items: list[Dict[str, Any]] = []
                for r in cur.fetchall():
                    fid = r[0]
                    symbol = r[1] or ""
                    stock_name = r[2] or ""
                    analysis_date = r[3]
                    period = r[4] or ""
                    final_decision = r[5]
                    created_at = r[6]
                    rating = "未知"
                    if isinstance(final_decision, dict):
                        rating = final_decision.get("rating", "未知")
                    items.append(
                        {
                            "id": fid,
                            "symbol": symbol,
                            "stock_name": stock_name,
                            "analysis_date": analysis_date.isoformat()
                            if analysis_date
                            else None,
                            "period": period,
                            "rating": rating,
                            "created_at": created_at.isoformat()
                            if created_at
                            else None,
                        }
                    )

        return {"total": total, "items": items}

    def get_record_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        sql = (
            "SELECT id, ts_code, stock_name, analysis_date, period, stock_info, "
            "agents_results, discussion_result, final_decision, created_at "
            "FROM app.analysis_records WHERE id = %s ORDER BY created_at DESC LIMIT 1"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (record_id,))
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "id": r[0],
                    "symbol": r[1],
                    "stock_name": r[2],
                    "analysis_date": r[3].isoformat() if r[3] else None,
                    "period": r[4],
                    "stock_info": r[5] if isinstance(r[5], dict) else {},
                    "agents_results": r[6] if isinstance(r[6], dict) else {},
                    "discussion_result": r[7] if isinstance(r[7], dict) else {},
                    "final_decision": r[8] if isinstance(r[8], dict) else {},
                    "created_at": r[9].isoformat() if r[9] else None,
                }

    def delete_record(self, record_id: int) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM app.analysis_records WHERE id = %s", (record_id,))
                return cur.rowcount > 0


analysis_repo = StockAnalysisRepoPG()
