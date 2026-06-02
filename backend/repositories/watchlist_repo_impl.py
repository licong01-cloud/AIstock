from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

from ..db.pg_pool import get_conn
import psycopg2.extras as pg_extras


_SORT_MAP = {
    "code": "i.code",
    "name": "i.name",
    "category": "cat_names",
    "created_at": "i.created_at",
    "updated_at": "i.updated_at",
    "last_analysis_time": "a.analysis_date",
    "last_rating": "(a.rating)",
}


class WatchlistRepoPG:
    """PostgreSQL-backed watchlist repository for next_app.

    逻辑基本保持与根目录 pg_watchlist_repo.WatchlistRepoPG 一致，
    但内部使用 next_app.backend.db.pg_pool.get_conn 连接池。
    """

    def create_category(self, name: str, description: Optional[str] = None) -> int:
        sql = "INSERT INTO app.watchlist_categories(name, description) VALUES (%s,%s) RETURNING id"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (name, description))
                return int(cur.fetchone()[0])

    def rename_category(self, category_id: int, new_name: str, new_desc: Optional[str] = None) -> bool:
        sql = (
            "UPDATE app.watchlist_categories SET name=%s, description=%s, updated_at=now() "
            "WHERE id=%s"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (new_name, new_desc, category_id))
                return cur.rowcount > 0

    def delete_category(self, category_id: int) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM app.watchlist_item_categories WHERE category_id=%s",
                    (category_id,),
                )
                if int(cur.fetchone()[0]) > 0:
                    return False
                cur.execute(
                    "DELETE FROM app.watchlist_categories WHERE id=%s",
                    (category_id,),
                )
                return cur.rowcount > 0

    def list_categories(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name, description, created_at, updated_at "
                    "FROM app.watchlist_categories ORDER BY id ASC"
                )
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "name": r[1],
                            "description": r[2],
                            "created_at": r[3].isoformat() if r[3] else None,
                            "updated_at": r[4].isoformat() if r[4] else None,
                        }
                    )
        return out

    def add_item(
        self,
        code: str,
        name: str,
        category_id: int,
        note: Optional[str] = None,
        entry_price: Optional[float] = None,
        entry_rank: Optional[int] = None,
        entry_source: Optional[str] = None,
        entry_task_id: Optional[str] = None,
        entry_loop_id: Optional[int] = None,
        entry_as_of: Optional[Union[date, str]] = None,
    ) -> int:
        """Create or upsert item, then ensure mapping to category exists.

        Returns the item_id.
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.watchlist_items(
                        code,
                        name,
                        note,
                        entry_price,
                        entry_rank,
                        entry_source,
                        entry_task_id,
                        entry_loop_id,
                        entry_as_of
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (code) DO UPDATE
                    SET name = COALESCE(EXCLUDED.name, app.watchlist_items.name),
                        note = COALESCE(EXCLUDED.note, app.watchlist_items.note),
                        entry_price = COALESCE(EXCLUDED.entry_price, app.watchlist_items.entry_price),
                        entry_rank = COALESCE(EXCLUDED.entry_rank, app.watchlist_items.entry_rank),
                        entry_source = COALESCE(EXCLUDED.entry_source, app.watchlist_items.entry_source),
                        entry_task_id = COALESCE(EXCLUDED.entry_task_id, app.watchlist_items.entry_task_id),
                        entry_loop_id = COALESCE(EXCLUDED.entry_loop_id, app.watchlist_items.entry_loop_id),
                        entry_as_of = COALESCE(EXCLUDED.entry_as_of, app.watchlist_items.entry_as_of),
                        updated_at = now()
                    RETURNING id
                    """,
                    (
                        code,
                        name,
                        note,
                        entry_price,
                        entry_rank,
                        entry_source,
                        entry_task_id,
                        entry_loop_id,
                        entry_as_of,
                    ),
                )
                item_id = int(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO app.watchlist_item_categories(item_id, category_id)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (item_id, category_id),
                )
                cur.execute(
                    "UPDATE app.watchlist_items SET updated_at=now() WHERE id=%s",
                    (item_id,),
                )
                return item_id

    def add_items_bulk(
        self,
        codes: List[str],
        category_id: int,
        on_conflict: str = "ignore",
        names: Optional[Dict[str, str]] = None,
    ) -> Dict[str, int]:
        """Bulk add codes to a category.

        on_conflict:
          - "ignore": keep existing categories; only add mapping to the
            target category if missing.
          - "move": replace existing categories with ONLY the target
            category.
        Returns counters: added (newly mapped), skipped (already had
        mapping), moved (when move performed).
        """

        added = skipped = moved = 0
        codes = [c.strip() for c in codes if c and c.strip()]
        if not codes:
            return {"added": 0, "skipped": 0, "moved": 0}
        names = names or {}
        upsert_rows: List[Tuple[str, str]] = [(c, names.get(c) or c) for c in codes]

        with get_conn() as conn:
            with conn.cursor() as cur:
                pg_extras.execute_values(
                    cur,
                    """
                    INSERT INTO app.watchlist_items(code, name)
                    VALUES %s
                    ON CONFLICT (code) DO UPDATE
                    SET name = COALESCE(EXCLUDED.name, app.watchlist_items.name),
                        updated_at = now()
                    """,
                    upsert_rows,
                    page_size=1000,
                )
                cur.execute(
                    "SELECT id, code FROM app.watchlist_items WHERE code = ANY(%s)",
                    (codes,),
                )
                code_to_id = {r[1]: int(r[0]) for r in cur.fetchall()}
                item_ids = [code_to_id[c] for c in codes if c in code_to_id]
                if not item_ids:
                    return {"added": 0, "skipped": len(codes), "moved": 0}

                if on_conflict == "move":
                    cur.execute(
                        "SELECT COUNT(DISTINCT item_id) FROM app.watchlist_item_categories "
                        "WHERE item_id = ANY(%s)",
                        (item_ids,),
                    )
                    had_mappings = int(cur.fetchone()[0])
                    cur.execute(
                        "DELETE FROM app.watchlist_item_categories WHERE item_id = ANY(%s)",
                        (item_ids,),
                    )
                    moved = had_mappings
                    map_rows = [(iid, category_id) for iid in item_ids]
                    pg_extras.execute_values(
                        cur,
                        "INSERT INTO app.watchlist_item_categories(item_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
                        map_rows,
                        page_size=1000,
                    )
                    added = len(item_ids)
                    skipped = 0
                    cur.execute(
                        "UPDATE app.watchlist_items SET updated_at = now() WHERE id = ANY(%s)",
                        (item_ids,),
                    )
                else:
                    map_rows = [(iid, category_id) for iid in item_ids]
                    pg_extras.execute_values(
                        cur,
                        "INSERT INTO app.watchlist_item_categories(item_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
                        map_rows,
                        page_size=1000,
                    )
                    cur.execute(
                        "SELECT COUNT(*) FROM app.watchlist_item_categories "
                        "WHERE item_id = ANY(%s) AND category_id = %s",
                        (item_ids, category_id),
                    )
                    present = int(cur.fetchone()[0])
                    added = present
                    skipped = max(0, len(item_ids) - added)

        return {"added": added, "skipped": skipped, "moved": moved}

    def add_items_bulk_with_meta(
        self,
        *,
        category_id: int,
        items: List[Dict[str, Any]],
        on_conflict: str = "ignore",
    ) -> Dict[str, Any]:
        """Bulk add items with entry metadata.

        items: each item supports keys:
        - code (required)
        - name
        - note
        - entry_price
        - entry_rank
        - entry_source
        - entry_task_id
        - entry_loop_id
        - entry_as_of (date or ISO string)

        Returns: {added, skipped, moved, item_ids_by_code}
        """

        if not items:
            return {"added": 0, "skipped": 0, "moved": 0, "item_ids_by_code": {}}

        # normalize & de-dup by code (last wins)
        norm_map: Dict[str, Dict[str, Any]] = {}
        for it in items:
            code = str((it or {}).get("code") or "").strip()
            if not code:
                continue
            norm_map[code] = dict(it)
            norm_map[code]["code"] = code

        codes = list(norm_map.keys())
        if not codes:
            return {"added": 0, "skipped": 0, "moved": 0, "item_ids_by_code": {}}

        added = skipped = moved = 0
        upsert_rows: List[Tuple[Any, ...]] = []
        for code in codes:
            it = norm_map[code]
            upsert_rows.append(
                (
                    code,
                    it.get("name") or code,
                    it.get("note"),
                    it.get("entry_price"),
                    it.get("entry_rank"),
                    it.get("entry_source"),
                    it.get("entry_task_id"),
                    it.get("entry_loop_id"),
                    it.get("entry_as_of"),
                    it.get("lifecycle_status") or "CANDIDATE",
                    it.get("planned_entry_price") or it.get("entry_price"),
                    it.get("actual_entry_price") or it.get("entry_price"),
                    it.get("actual_entry_date") or it.get("entry_as_of"),
                    bool(it.get("advisory_enabled", False)),
                )
            )

        with get_conn() as conn:
            with conn.cursor() as cur:
                pg_extras.execute_values(
                    cur,
                    """
                    INSERT INTO app.watchlist_items(
                        code,
                        name,
                        note,
                        entry_price,
                        entry_rank,
                        entry_source,
                        entry_task_id,
                        entry_loop_id,
                        entry_as_of,
                        lifecycle_status,
                        planned_entry_price,
                        actual_entry_price,
                        actual_entry_date,
                        advisory_enabled
                    )
                    VALUES %s
                    ON CONFLICT (code) DO UPDATE
                    SET name = COALESCE(EXCLUDED.name, app.watchlist_items.name),
                        note = COALESCE(EXCLUDED.note, app.watchlist_items.note),
                        entry_price = COALESCE(EXCLUDED.entry_price, app.watchlist_items.entry_price),
                        entry_rank = COALESCE(EXCLUDED.entry_rank, app.watchlist_items.entry_rank),
                        entry_source = COALESCE(EXCLUDED.entry_source, app.watchlist_items.entry_source),
                        entry_task_id = COALESCE(EXCLUDED.entry_task_id, app.watchlist_items.entry_task_id),
                        entry_loop_id = COALESCE(EXCLUDED.entry_loop_id, app.watchlist_items.entry_loop_id),
                        entry_as_of = COALESCE(EXCLUDED.entry_as_of, app.watchlist_items.entry_as_of),
                        lifecycle_status = CASE
                            WHEN app.watchlist_items.lifecycle_status = 'EXITED' THEN app.watchlist_items.lifecycle_status
                            ELSE COALESCE(EXCLUDED.lifecycle_status, app.watchlist_items.lifecycle_status)
                        END,
                        planned_entry_price = COALESCE(EXCLUDED.planned_entry_price, app.watchlist_items.planned_entry_price),
                        actual_entry_price = COALESCE(EXCLUDED.actual_entry_price, app.watchlist_items.actual_entry_price),
                        actual_entry_date = COALESCE(EXCLUDED.actual_entry_date, app.watchlist_items.actual_entry_date),
                        advisory_enabled = COALESCE(EXCLUDED.advisory_enabled, app.watchlist_items.advisory_enabled),
                        updated_at = now()
                    """,
                    upsert_rows,
                    page_size=1000,
                )

                cur.execute(
                    "SELECT id, code FROM app.watchlist_items WHERE code = ANY(%s)",
                    (codes,),
                )
                code_to_id = {r[1]: int(r[0]) for r in cur.fetchall()}
                item_ids = [code_to_id[c] for c in codes if c in code_to_id]
                if not item_ids:
                    return {"added": 0, "skipped": len(codes), "moved": 0, "item_ids_by_code": {}}

                if on_conflict == "move":
                    cur.execute(
                        "SELECT COUNT(DISTINCT item_id) FROM app.watchlist_item_categories "
                        "WHERE item_id = ANY(%s)",
                        (item_ids,),
                    )
                    had_mappings = int(cur.fetchone()[0])
                    cur.execute(
                        "DELETE FROM app.watchlist_item_categories WHERE item_id = ANY(%s)",
                        (item_ids,),
                    )
                    moved = had_mappings
                    map_rows = [(iid, category_id) for iid in item_ids]
                    pg_extras.execute_values(
                        cur,
                        "INSERT INTO app.watchlist_item_categories(item_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
                        map_rows,
                        page_size=1000,
                    )
                    added = len(item_ids)
                    skipped = 0
                else:
                    map_rows = [(iid, category_id) for iid in item_ids]
                    pg_extras.execute_values(
                        cur,
                        "INSERT INTO app.watchlist_item_categories(item_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
                        map_rows,
                        page_size=1000,
                    )
                    cur.execute(
                        "SELECT COUNT(*) FROM app.watchlist_item_categories "
                        "WHERE item_id = ANY(%s) AND category_id = %s",
                        (item_ids, category_id),
                    )
                    present = int(cur.fetchone()[0])
                    added = present
                    skipped = max(0, len(item_ids) - added)

        return {
            "added": added,
            "skipped": skipped,
            "moved": moved,
            "item_ids_by_code": code_to_id,
        }

    def update_item_category(self, ids: List[int], new_category_id: int) -> int:
        """Replace categories for given items with ONLY the new_category_id."""

        if not ids:
            return 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app.watchlist_item_categories WHERE item_id = ANY(%s)",
                    (ids,),
                )
                if ids:
                    rows = [(iid, new_category_id) for iid in ids]
                    pg_extras.execute_values(
                        cur,
                        "INSERT INTO app.watchlist_item_categories(item_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
                        rows,
                        page_size=1000,
                    )
                    cur.execute(
                        "UPDATE app.watchlist_items SET updated_at=now() WHERE id = ANY(%s)",
                        (ids,),
                    )
                return len(ids)

    def delete_items(self, ids: List[int]) -> int:
        if not ids:
            return 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app.watchlist_items WHERE id = ANY(%s)",
                    (ids,),
                )
                return cur.rowcount

    def get_item_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT i.id, i.code, i.name, i.note, i.created_at, i.updated_at,
                           COALESCE(string_agg(DISTINCT c.name, ',' ORDER BY c.name), '') AS cat_names,
                           COALESCE(array_agg(DISTINCT c.id), ARRAY[]::BIGINT[]) AS cat_ids,
                           i.entry_price,
                           i.entry_rank,
                           i.entry_source,
                           i.entry_task_id,
                           i.entry_loop_id,
                           i.entry_as_of,
                           i.lifecycle_status,
                           i.planned_entry_price,
                           i.actual_entry_price,
                           i.actual_entry_date,
                           i.exited_at,
                           i.exit_reason,
                           i.advisory_enabled
                      FROM app.watchlist_items i
                 LEFT JOIN app.watchlist_item_categories w ON w.item_id = i.id
                 LEFT JOIN app.watchlist_categories c ON c.id = w.category_id
                     WHERE i.code = %s
                  GROUP BY i.id, i.code, i.name, i.note, i.created_at, i.updated_at, i.entry_price, i.entry_rank, i.entry_source, i.entry_task_id, i.entry_loop_id, i.entry_as_of, i.lifecycle_status, i.planned_entry_price, i.actual_entry_price, i.actual_entry_date, i.exited_at, i.exit_reason, i.advisory_enabled
                    """,
                    (code,),
                )
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "id": r[0],
                    "code": r[1],
                    "name": r[2],
                    "note": r[3],
                    "created_at": r[4].isoformat() if r[4] else None,
                    "updated_at": r[5].isoformat() if r[5] else None,
                    "category_names": r[6],
                    "category_ids": list(r[7]) if r[7] is not None else [],
                    "entry_price": r[8],
                    "entry_rank": r[9],
                    "entry_source": r[10],
                    "entry_task_id": r[11],
                    "entry_loop_id": r[12],
                    "entry_as_of": r[13].isoformat() if r[13] else None,
                    "lifecycle_status": r[14],
                    "planned_entry_price": float(r[15]) if r[15] is not None else None,
                    "actual_entry_price": float(r[16]) if r[16] is not None else None,
                    "actual_entry_date": r[17].isoformat() if r[17] else None,
                    "exited_at": r[18].isoformat() if r[18] else None,
                    "exit_reason": r[19],
                    "advisory_enabled": bool(r[20]),
                }

    def get_items_by_codes(self, codes: List[str]) -> List[Dict[str, Any]]:
        if not codes:
            return []
        out: List[Dict[str, Any]] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT i.id, i.code, i.name, i.note, i.created_at, i.updated_at,
                           COALESCE(string_agg(DISTINCT c.name, ',' ORDER BY c.name), '') AS cat_names,
                           COALESCE(array_agg(DISTINCT c.id), ARRAY[]::BIGINT[]) AS cat_ids,
                           i.entry_price,
                           i.entry_rank,
                           i.entry_source,
                           i.entry_task_id,
                           i.entry_loop_id,
                           i.entry_as_of,
                           i.lifecycle_status,
                           i.planned_entry_price,
                           i.actual_entry_price,
                           i.actual_entry_date,
                           i.exited_at,
                           i.exit_reason,
                           i.advisory_enabled
                      FROM app.watchlist_items i
                 LEFT JOIN app.watchlist_item_categories w ON w.item_id = i.id
                 LEFT JOIN app.watchlist_categories c ON c.id = w.category_id
                     WHERE i.code = ANY(%s)
                  GROUP BY i.id, i.code, i.name, i.note, i.created_at, i.updated_at, i.entry_price, i.entry_rank, i.entry_source, i.entry_task_id, i.entry_loop_id, i.entry_as_of, i.lifecycle_status, i.planned_entry_price, i.actual_entry_price, i.actual_entry_date, i.exited_at, i.exit_reason, i.advisory_enabled
                    """,
                    (codes,),
                )
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "code": r[1],
                            "name": r[2],
                            "note": r[3],
                            "created_at": r[4].isoformat() if r[4] else None,
                            "updated_at": r[5].isoformat() if r[5] else None,
                            "category_names": r[6],
                            "category_ids": list(r[7]) if r[7] is not None else [],
                            "entry_price": r[8],
                            "entry_rank": r[9],
                            "entry_source": r[10],
                            "entry_task_id": r[11],
                            "entry_loop_id": r[12],
                            "entry_as_of": r[13].isoformat() if r[13] else None,
                            "lifecycle_status": r[14],
                            "planned_entry_price": float(r[15]) if r[15] is not None else None,
                            "actual_entry_price": float(r[16]) if r[16] is not None else None,
                            "actual_entry_date": r[17].isoformat() if r[17] else None,
                            "exited_at": r[18].isoformat() if r[18] else None,
                            "exit_reason": r[19],
                            "advisory_enabled": bool(r[20]),
                        }
                    )
        return out

    def list_items(
        self,
        category_id: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "updated_at",
        sort_dir: str = "desc",
    ) -> Dict[str, Any]:
        order_expr = _SORT_MAP.get(sort_by, "i.updated_at")
        dir_kw = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
        params: List[Any] = []
        where = ""
        if category_id:
            where = (
                "WHERE EXISTS (SELECT 1 FROM app.watchlist_item_categories w2 "
                "WHERE w2.item_id = i.id AND w2.category_id = %s)"
            )
            params.append(category_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM app.watchlist_items i {where}", params
                )
                total = int(cur.fetchone()[0])
                offset = max(0, (int(page) - 1) * int(page_size))
                limit = int(page_size)
                sql = f"""
                    SELECT i.id, i.code, i.name, i.note, i.created_at, i.updated_at,
                           COALESCE(string_agg(DISTINCT c.name, ',' ORDER BY c.name), '') AS cat_names,
                           COALESCE(array_agg(DISTINCT c.id), ARRAY[]::BIGINT[]) AS cat_ids,
                           a.analysis_date AS last_analysis_time,
                           a.rating AS last_rating,
                           a.conclusion AS last_conclusion,
                           i.entry_price,
                           i.entry_rank,
                            i.entry_source,
                            i.entry_task_id,
                            i.entry_loop_id,
                            i.entry_as_of,
                            i.lifecycle_status,
                            i.planned_entry_price,
                            i.actual_entry_price,
                            i.actual_entry_date,
                            i.exited_at,
                            i.exit_reason,
                            i.advisory_enabled
                      FROM app.watchlist_items i
                 LEFT JOIN app.watchlist_item_categories w ON w.item_id = i.id
                 LEFT JOIN app.watchlist_categories c ON c.id = w.category_id
                 LEFT JOIN LATERAL (
                        SELECT ar.analysis_date,
                               COALESCE(ar.final_decision->>'rating', (ar.agents_results->'final_decision'->>'rating')) AS rating,
                               COALESCE(ar.final_decision->>'advice',  (ar.agents_results->'final_decision'->>'advice'), ar.discussion_result->>'summary') AS conclusion
                          FROM app.analysis_records ar
                         WHERE ar.ts_code = i.code OR ar.ts_code = split_part(i.code, '.', 1)
                         ORDER BY ar.analysis_date DESC
                         LIMIT 1
                   ) a ON TRUE
                   {where}
                  GROUP BY i.id, i.code, i.name, i.note, i.created_at, i.updated_at, a.analysis_date, a.rating, a.conclusion, i.entry_price, i.entry_rank, i.entry_source, i.entry_task_id, i.entry_loop_id, i.entry_as_of, i.lifecycle_status, i.planned_entry_price, i.actual_entry_price, i.actual_entry_date, i.exited_at, i.exit_reason, i.advisory_enabled
                  ORDER BY {order_expr} {dir_kw} NULLS LAST, i.code ASC
                  OFFSET %s LIMIT %s
                """
                cur.execute(sql, params + [offset, limit])
                items: List[Dict[str, Any]] = []
                for r in cur.fetchall():
                    items.append(
                        {
                            "id": r[0],
                            "code": r[1],
                            "name": r[2],
                            "note": r[3],
                            "created_at": r[4].isoformat() if r[4] else None,
                            "updated_at": r[5].isoformat() if r[5] else None,
                            "category_names": r[6],
                            "category_ids": list(r[7]) if r[7] is not None else [],
                            "last_analysis_time": r[8].isoformat() if r[8] else None,
                            "last_rating": r[9],
                            "last_conclusion": r[10],
                            "entry_price": r[11],
                            "entry_rank": r[12],
                            "entry_source": r[13],
                            "entry_task_id": r[14],
                            "entry_loop_id": r[15],
                            "entry_as_of": r[16].isoformat() if r[16] else None,
                            "lifecycle_status": r[17],
                            "planned_entry_price": float(r[18]) if r[18] is not None else None,
                            "actual_entry_price": float(r[19]) if r[19] is not None else None,
                            "actual_entry_date": r[20].isoformat() if r[20] else None,
                            "exited_at": r[21].isoformat() if r[21] else None,
                            "exit_reason": r[22],
                            "advisory_enabled": bool(r[23]),
                        }
                    )
        return {"total": total, "items": items}

    def add_categories_to_items(
        self,
        item_ids: List[int],
        category_ids: List[int],
    ) -> int:
        if not item_ids or not category_ids:
            return 0
        rows = [(iid, cid) for iid in item_ids for cid in category_ids]
        with get_conn() as conn:
            with conn.cursor() as cur:
                pg_extras.execute_values(
                    cur,
                    "INSERT INTO app.watchlist_item_categories(item_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
                    rows,
                    page_size=1000,
                )
                cur.execute(
                    "UPDATE app.watchlist_items SET updated_at=now() WHERE id = ANY(%s)",
                    (item_ids,),
                )
                return len(rows)

    def remove_categories_from_items(
        self,
        item_ids: List[int],
        category_ids: List[int],
    ) -> int:
        if not item_ids or not category_ids:
            return 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app.watchlist_item_categories WHERE item_id = ANY(%s) AND category_id = ANY(%s)",
                    (item_ids, category_ids),
                )
                cur.execute(
                    "UPDATE app.watchlist_items SET updated_at=now() WHERE id = ANY(%s)",
                    (item_ids,),
                )
                return cur.rowcount

    def set_item_categories(self, item_id: int, category_ids: List[int]) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app.watchlist_item_categories WHERE item_id = %s",
                    (item_id,),
                )
                if category_ids:
                    rows = [(item_id, cid) for cid in category_ids]
                    pg_extras.execute_values(
                        cur,
                        "INSERT INTO app.watchlist_item_categories(item_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
                        rows,
                        page_size=1000,
                    )
                cur.execute(
                    "UPDATE app.watchlist_items SET updated_at=now() WHERE id = %s",
                    (item_id,),
                )
                return len(category_ids or [])

    def get_latest_analysis_map(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (ar.ts_code)
                           ar.ts_code,
                           ar.analysis_date,
                           COALESCE(ar.final_decision->>'rating', (ar.agents_results->'final_decision'->>'rating')) AS rating,
                           COALESCE(ar.final_decision->>'advice',  (ar.agents_results->'final_decision'->>'advice'), ar.discussion_result->>'summary') AS conclusion
                      FROM app.analysis_records ar
                     WHERE ar.ts_code = ANY(%s)
                     ORDER BY ar.ts_code, ar.analysis_date DESC
                    """,
                    (codes,),
                )
                for r in cur.fetchall():
                    out[str(r[0])] = {
                        "last_analysis_time": r[1].isoformat() if r[1] else None,
                        "last_rating": r[2],
                        "last_conclusion": r[3],
                    }
        return out


watchlist_repo = WatchlistRepoPG()
