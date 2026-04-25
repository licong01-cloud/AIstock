from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.factor_eligibility")

_PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)


class FactorEligibilityService:
    """官方评估/相关性共享因子准入规则。"""

    def get_eligible_factor_names(
        self,
        factor_names: Optional[List[str]] = None,
        include_disabled: bool = False,
        only_without_correlation: bool = False,
    ) -> List[str]:
        records = self.list_eligible_factors(
            factor_names=factor_names,
            include_disabled=include_disabled,
            only_without_correlation=only_without_correlation,
        )
        return [rec["factor_name"] for rec in records]

    def list_eligible_factors(
        self,
        factor_names: Optional[List[str]] = None,
        include_disabled: bool = False,
        only_without_correlation: bool = False,
    ) -> List[Dict[str, Any]]:
        conditions = [
            "c.transformation_status = 'SUCCESS'",
            "c.qe_code_path IS NOT NULL",
        ]
        params: List[Any] = []

        if not include_disabled:
            conditions.append("COALESCE(c.is_available, TRUE) = TRUE")

        if only_without_correlation:
            conditions.append(
                "NOT EXISTS ("
                "SELECT 1 FROM qe_factor_correlations q "
                "WHERE q.factor_a_id = c.id OR q.factor_b_id = c.id"
                ")"
            )

        if factor_names:
            placeholders = ",".join(["%s"] * len(factor_names))
            conditions.append(f"c.factor_name IN ({placeholders})")
            params.extend(factor_names)

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT c.id, c.factor_name, c.transformation_status,
                   COALESCE(c.is_available, TRUE) AS is_available,
                   c.qe_code_path, c.correlation_computed_at
            FROM aistock_factor_catalog c
            WHERE {where_clause}
            ORDER BY c.factor_name
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or None)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        eligible: List[Dict[str, Any]] = []
        missing_files: List[str] = []
        for row in rows:
            abs_code_path = os.path.join(_PROJECT_ROOT, row["qe_code_path"])
            if not os.path.isfile(abs_code_path):
                missing_files.append(row["factor_name"])
                continue
            row["abs_code_path"] = abs_code_path
            eligible.append(row)

        if missing_files:
            logger.warning("跳过 qe_code_path 文件不存在的因子: %s", missing_files[:20])
        return eligible

    def get_factor_eligibility(self, factor_name: str) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT factor_name, transformation_status,
                           COALESCE(is_available, TRUE) AS is_available,
                           qe_code_path, correlation_computed_at
                    FROM aistock_factor_catalog
                    WHERE factor_name = %s
                    LIMIT 1
                    """,
                    (factor_name,),
                )
                row = cur.fetchone()
                if row is None:
                    return {
                        "factor_name": factor_name,
                        "eligible": False,
                        "reason": "factor_not_found",
                    }

        name, transformation_status, is_available, qe_code_path, correlation_computed_at = row
        if transformation_status != "SUCCESS":
            return {
                "factor_name": name,
                "eligible": False,
                "reason": "transformation_not_success",
                "transformation_status": transformation_status,
                "is_available": is_available,
                "qe_code_path": qe_code_path,
            }
        if not is_available:
            return {
                "factor_name": name,
                "eligible": False,
                "reason": "factor_disabled",
                "transformation_status": transformation_status,
                "is_available": is_available,
                "qe_code_path": qe_code_path,
            }
        if not qe_code_path:
            return {
                "factor_name": name,
                "eligible": False,
                "reason": "qe_code_path_missing",
                "transformation_status": transformation_status,
                "is_available": is_available,
                "qe_code_path": qe_code_path,
            }

        abs_code_path = os.path.join(_PROJECT_ROOT, qe_code_path)
        code_exists = os.path.isfile(abs_code_path)
        return {
            "factor_name": name,
            "eligible": code_exists,
            "reason": "ok" if code_exists else "qe_code_file_missing",
            "transformation_status": transformation_status,
            "is_available": is_available,
            "qe_code_path": qe_code_path,
            "code_exists": code_exists,
            "abs_code_path": abs_code_path,
            "correlation_computed_at": correlation_computed_at.isoformat() if correlation_computed_at else None,
        }
