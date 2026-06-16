from __future__ import annotations

import hashlib
import logging
import os
from typing import Any, Dict, List, Literal, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.factor_eligibility")

_PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)

EligibilitySourceMode = Literal["official_offline", "realtime_transformed"]


class FactorEligibilityService:
    """Shared factor eligibility rules for official offline and realtime paths."""

    def get_eligible_factor_names(
        self,
        factor_names: Optional[List[str]] = None,
        include_disabled: bool = False,
        only_without_correlation: bool = False,
        source_mode: EligibilitySourceMode = "official_offline",
    ) -> List[str]:
        records = self.list_eligible_factors(
            factor_names=factor_names,
            include_disabled=include_disabled,
            only_without_correlation=only_without_correlation,
            source_mode=source_mode,
        )
        return [rec["factor_name"] for rec in records]

    def list_eligible_factors(
        self,
        factor_names: Optional[List[str]] = None,
        include_disabled: bool = False,
        only_without_correlation: bool = False,
        source_mode: EligibilitySourceMode = "official_offline",
    ) -> List[Dict[str, Any]]:
        if source_mode not in {"official_offline", "realtime_transformed"}:
            raise ValueError(f"unsupported eligibility source_mode: {source_mode}")

        conditions: List[str] = []
        params: List[Any] = []
        if source_mode == "official_offline":
            conditions.append("c.code_text IS NOT NULL")
            conditions.append("length(trim(c.code_text)) > 0")
        else:
            conditions.extend([
                "c.transformation_status = 'SUCCESS'",
                "c.qe_code_path IS NOT NULL",
                "length(trim(c.qe_code_path)) > 0",
            ])

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

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        sql = f"""
            SELECT c.id, c.factor_name, c.transformation_status,
                   COALESCE(c.is_available, TRUE) AS is_available,
                   c.qe_code_path, c.code_text, c.correlation_computed_at
            FROM aistock_factor_catalog c
            WHERE {where_clause}
            ORDER BY c.factor_name
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or None)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        if source_mode == "official_offline":
            for row in rows:
                code_text = str(row.get("code_text") or "")
                row["code_source"] = "code_text"
                row["code_text_hash"] = hashlib.sha256(code_text.encode("utf-8")).hexdigest()[:16]
                row["eligible"] = True
                row["reason"] = "ok"
            return rows

        eligible: List[Dict[str, Any]] = []
        missing_files: List[str] = []
        for row in rows:
            qe_code_path = row.get("qe_code_path")
            abs_code_path = os.path.join(_PROJECT_ROOT, qe_code_path) if qe_code_path else ""
            if not abs_code_path or not os.path.isfile(abs_code_path):
                missing_files.append(row["factor_name"])
                continue
            row["abs_code_path"] = abs_code_path
            row["code_source"] = "qe_code_path"
            row["eligible"] = True
            row["reason"] = "ok"
            eligible.append(row)

        if missing_files:
            logger.warning("skip realtime-transformed factors with missing qe_code_path files: %s", missing_files[:20])
        return eligible

    def get_factor_eligibility(
        self,
        factor_name: str,
        source_mode: EligibilitySourceMode = "official_offline",
    ) -> Dict[str, Any]:
        if source_mode not in {"official_offline", "realtime_transformed"}:
            raise ValueError(f"unsupported eligibility source_mode: {source_mode}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT factor_name, transformation_status,
                           COALESCE(is_available, TRUE) AS is_available,
                           qe_code_path, code_text, correlation_computed_at
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
                        "source_mode": source_mode,
                    }

        name, transformation_status, is_available, qe_code_path, code_text, correlation_computed_at = row
        base = {
            "factor_name": name,
            "transformation_status": transformation_status,
            "is_available": is_available,
            "qe_code_path": qe_code_path,
            "correlation_computed_at": correlation_computed_at.isoformat() if correlation_computed_at else None,
            "source_mode": source_mode,
        }

        if source_mode == "official_offline":
            if not code_text or not str(code_text).strip():
                return {**base, "eligible": False, "reason": "code_text_missing", "code_source": "code_text"}
            return {
                **base,
                "eligible": True,
                "reason": "ok",
                "code_source": "code_text",
                "code_text_hash": hashlib.sha256(str(code_text).encode("utf-8")).hexdigest()[:16],
            }

        if transformation_status != "SUCCESS":
            return {**base, "eligible": False, "reason": "transformation_not_success"}
        if not qe_code_path:
            return {**base, "eligible": False, "reason": "qe_code_path_missing"}

        abs_code_path = os.path.join(_PROJECT_ROOT, qe_code_path)
        code_exists = os.path.isfile(abs_code_path)
        return {
            **base,
            "eligible": code_exists,
            "reason": "ok" if code_exists else "qe_code_file_missing",
            "code_exists": code_exists,
            "abs_code_path": abs_code_path,
            "code_source": "qe_code_path",
        }
