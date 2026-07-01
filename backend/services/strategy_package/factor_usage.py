"""StrategyPackage factor-reference queries for factor-library protection."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import psycopg2.extras

from backend.db.pg_pool import get_conn

STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON = "factor_referenced_by_strategy_package"
STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED = "strategy_package_factor_usage_check_failed"


class StrategyPackageFactorUsageQueryError(RuntimeError):
    """Raised when protected StrategyPackage factor usage cannot be checked."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}


def find_strategy_package_factor_usage(
    factor_name: str,
    *,
    conn: Any | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Return package references for a factor from package_asset and manifests.

    Deletion guards must fail closed if this query cannot run; callers should
    surface ``StrategyPackageFactorUsageQueryError`` instead of allowing delete.
    """

    normalized_factor = str(factor_name or "").strip()
    if not normalized_factor:
        raise StrategyPackageFactorUsageQueryError(
            "factor_name is required for StrategyPackage usage lookup",
            context={
                "reason_code": STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED,
                "factor_name": factor_name,
            },
        )

    safe_limit = max(1, min(int(limit if limit is not None else 20), 100))
    if conn is not None:
        return _query_with_conn(conn, normalized_factor, limit=safe_limit)

    try:
        with get_conn() as owned_conn:
            return _query_with_conn(owned_conn, normalized_factor, limit=safe_limit)
    except StrategyPackageFactorUsageQueryError:
        raise
    except Exception as exc:  # pragma: no cover - defensive DB boundary
        raise StrategyPackageFactorUsageQueryError(
            "StrategyPackage factor usage lookup failed",
            context={
                "reason_code": STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED,
                "factor_name": normalized_factor,
                "error": f"{type(exc).__name__}: {exc}",
            },
        ) from exc


def has_strategy_package_factor_usage(
    factor_name: str,
    *,
    conn: Any | None = None,
) -> bool:
    """Fast boolean wrapper used by write-path guards."""

    usage = find_strategy_package_factor_usage(factor_name, conn=conn, limit=1)
    return bool(usage["protected"])


def _query_with_conn(conn: Any, factor_name: str, *, limit: int) -> dict[str, Any]:
    encoded_name = quote(factor_name, safe="")
    path_pattern = _like_contains(f"/{factor_name}.py")
    logical_name_pattern = _like_contains(f"logical_name={encoded_name}")
    source_uri_pattern = _like_contains(f"/{factor_name}.py")
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                WITH package_asset_refs AS (
                    SELECT
                        p.package_id,
                        p.package_name,
                        p.package_status,
                        COALESCE(p.alpha_mode, p.manifest_json->>'alpha_mode', 'single_alpha') AS alpha_mode,
                        p.manifest_sha256,
                        'package_asset'::text AS reference_source,
                        COALESCE(
                            pa.metadata->>'logical_name',
                            pa.metadata->>'factor_name',
                            pa.metadata->>'factor_id',
                            %s
                        ) AS referenced_factor_name,
                        pa.metadata->>'factor_id' AS referenced_factor_id,
                        pa.asset_ref,
                        pa.asset_sha256,
                        pa.source_uri
                    FROM strategy_pkg.package_asset pa
                    JOIN strategy_pkg.package p ON p.package_id = pa.package_id
                    WHERE pa.asset_type = 'factor_code'
                      AND (
                          pa.metadata->>'logical_name' = %s
                          OR pa.metadata->>'factor_name' = %s
                          OR pa.metadata->>'factor_id' = %s
                          OR pa.asset_ref ILIKE %s ESCAPE '\\'
                          OR pa.asset_ref ILIKE %s ESCAPE '\\'
                          OR pa.source_uri ILIKE %s ESCAPE '\\'
                      )
                ),
                manifest_refs AS (
                    SELECT
                        p.package_id,
                        p.package_name,
                        p.package_status,
                        COALESCE(p.alpha_mode, p.manifest_json->>'alpha_mode', 'single_alpha') AS alpha_mode,
                        p.manifest_sha256,
                        'manifest_factor_set'::text AS reference_source,
                        factor_item->>'factor_name' AS referenced_factor_name,
                        factor_item->>'factor_id' AS referenced_factor_id,
                        factor_item->>'artifact_ref' AS asset_ref,
                        NULL::text AS asset_sha256,
                        NULL::text AS source_uri
                    FROM strategy_pkg.package p
                    CROSS JOIN LATERAL jsonb_array_elements(
                        CASE
                            WHEN jsonb_typeof(p.manifest_json->'factor_set') = 'array'
                            THEN p.manifest_json->'factor_set'
                            ELSE '[]'::jsonb
                        END
                    ) AS factor_item
                    WHERE factor_item->>'factor_name' = %s
                       OR factor_item->>'factor_id' = %s
                ),
                dedup AS (
                    SELECT DISTINCT
                        package_id,
                        package_name,
                        package_status,
                        alpha_mode,
                        manifest_sha256,
                        reference_source,
                        referenced_factor_name,
                        referenced_factor_id,
                        asset_ref,
                        asset_sha256,
                        source_uri
                    FROM (
                        SELECT * FROM package_asset_refs
                        UNION ALL
                        SELECT * FROM manifest_refs
                    ) refs
                ),
                counts AS (
                    SELECT
                        COUNT(*)::int AS reference_count,
                        COUNT(DISTINCT package_id)::int AS package_count
                    FROM dedup
                )
                SELECT
                    d.package_id,
                    d.package_name,
                    d.package_status,
                    d.alpha_mode,
                    d.manifest_sha256,
                    d.reference_source,
                    d.referenced_factor_name,
                    d.referenced_factor_id,
                    d.asset_ref,
                    d.asset_sha256,
                    d.source_uri,
                    c.reference_count,
                    c.package_count
                FROM counts c
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM dedup
                    ORDER BY package_id ASC, reference_source ASC, asset_ref ASC NULLS LAST
                    LIMIT %s
                ) d ON TRUE
                """,
                (
                    factor_name,
                    factor_name,
                    factor_name,
                    factor_name,
                    logical_name_pattern,
                    path_pattern,
                    source_uri_pattern,
                    factor_name,
                    factor_name,
                    limit,
                ),
            )
            rows = [dict(row) for row in cur.fetchall()]
    except Exception as exc:
        raise StrategyPackageFactorUsageQueryError(
            "StrategyPackage factor usage lookup failed",
            context={
                "reason_code": STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED,
                "factor_name": factor_name,
                "error": f"{type(exc).__name__}: {exc}",
            },
        ) from exc
    return _format_usage_summary(factor_name, rows, limit=limit)


def _format_usage_summary(factor_name: str, rows: list[dict[str, Any]], *, limit: int) -> dict[str, Any]:
    package_count = int(rows[0].get("package_count") or 0) if rows else 0
    reference_count = int(rows[0].get("reference_count") or 0) if rows else 0
    refs = [
        {
            "package_id": row.get("package_id"),
            "package_name": row.get("package_name"),
            "package_status": row.get("package_status"),
            "alpha_mode": row.get("alpha_mode"),
            "manifest_sha256": row.get("manifest_sha256"),
            "reference_source": row.get("reference_source"),
            "factor_name": row.get("referenced_factor_name") or factor_name,
            "factor_id": row.get("referenced_factor_id"),
            "asset_ref": row.get("asset_ref"),
            "asset_sha256": row.get("asset_sha256"),
            "source_uri": row.get("source_uri"),
        }
        for row in rows
        if row.get("package_id")
    ]
    return {
        "factor_name": factor_name,
        "protected": package_count > 0,
        "reason_code": STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON if package_count > 0 else None,
        "package_count": package_count,
        "reference_count": reference_count,
        "limit": limit,
        "references": refs,
        "query_sources": [
            "strategy_pkg.package_asset.asset_type=factor_code",
            "strategy_pkg.package.manifest_json.factor_set",
        ],
        "source_match_policy": "factor_name_or_factor_id; manifest refs are factor-name only and therefore fail closed",
    }


def _like_contains(value: str) -> str:
    escaped = str(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"
