"""Read-only Shenwan industry tree for Paper/Selection runtime profiles."""

from __future__ import annotations

from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError

ConnFactory = Callable[[], Iterator[Any]]


class SelectionIndustryTreeService:
    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def sw2_tree(self) -> list[dict[str, Any]]:
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT l1_code, l1_name, l2_code, l2_name
                        FROM market.sw_index_member
                        WHERE l1_code IS NOT NULL AND l1_name IS NOT NULL
                          AND l2_code IS NOT NULL AND l2_name IS NOT NULL
                        ORDER BY l1_code, l2_code
                        """
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            raise DataUnavailableError(
                "selection industry tree query failed",
                context={"table": "market.sw_index_member", "error": str(exc)},
            ) from exc

        groups: dict[str, dict[str, Any]] = {}
        for row in rows:
            l1_code = str(row.get("l1_code") or "").strip()
            l1_name = str(row.get("l1_name") or "").strip()
            l2_code = str(row.get("l2_code") or "").strip()
            l2_name = str(row.get("l2_name") or "").strip()
            if not (l1_code and l1_name and l2_code and l2_name):
                continue
            group = groups.setdefault(
                l1_code,
                {"l1_code": l1_code, "l1_name": l1_name, "children": []},
            )
            if not any(item["l2_code"] == l2_code for item in group["children"]):
                group["children"].append({"l2_code": l2_code, "l2_name": l2_name})
        return list(groups.values())
