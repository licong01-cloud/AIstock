"""StrategyPackage factor-reference guard for factor library destructive actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import PackageStatus

FACTOR_DELETE_BLOCKED_REASON_CODE = "factor_delete_blocked_referenced_by_strategy_package"
PACKAGE_ASSET_REFERENCE_SOURCE = "package_asset"
MANIFEST_REFERENCE_SOURCE = "manifest"


@dataclass(frozen=True)
class StrategyPackageFactorReference:
    package_id: str
    package_status: str
    reference_sources: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_id": self.package_id,
            "package_status": self.package_status,
            "reference_sources": list(self.reference_sources),
        }


def find_strategy_packages_referencing_factor(
    conn: Any,
    factor_name: str,
    *,
    include_retired: bool = False,
) -> list[StrategyPackageFactorReference]:
    """Return StrategyPackage references to ``factor_name`` from package_asset and manifest.

    The caller supplies the DB connection so destructive operations can run the
    guard in the same transaction before any DELETE statement. Query failures
    are intentionally not swallowed: hard-delete callers must fail closed.
    """

    merged: dict[tuple[str, str], set[str]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.package_id, p.package_status, %s AS reference_source
            FROM strategy_pkg.package_asset a
            JOIN strategy_pkg.package p ON p.package_id = a.package_id
            WHERE lower(a.asset_type) = 'factor_code'
              AND a.metadata->>'logical_name' = %s
              AND (%s OR p.package_status <> %s)
            ORDER BY p.package_id
            """,
            (
                PACKAGE_ASSET_REFERENCE_SOURCE,
                factor_name,
                include_retired,
                PackageStatus.RETIRED.value,
            ),
        )
        _merge_reference_rows(merged, cur.fetchall())

        cur.execute(
            """
            SELECT p.package_id, p.package_status, %s AS reference_source
            FROM strategy_pkg.package p
            WHERE (%s OR p.package_status <> %s)
              AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(COALESCE(p.manifest_json->'factor_set', '[]'::jsonb)) AS factor_item
                  WHERE factor_item->>'factor_name' = %s
              )
            ORDER BY p.package_id
            """,
            (
                MANIFEST_REFERENCE_SOURCE,
                include_retired,
                PackageStatus.RETIRED.value,
                factor_name,
            ),
        )
        _merge_reference_rows(merged, cur.fetchall())

    return [
        StrategyPackageFactorReference(
            package_id=package_id,
            package_status=package_status,
            reference_sources=tuple(sorted(sources)),
        )
        for (package_id, package_status), sources in sorted(merged.items())
    ]


def strategy_package_references_summary(
    references: list[StrategyPackageFactorReference],
) -> dict[str, Any]:
    return {
        "referenced": bool(references),
        "count": len(references),
        "packages": [reference.to_dict() for reference in references],
        "blocking_policy": "non_retired_packages_block_hard_delete",
        "reason_code": FACTOR_DELETE_BLOCKED_REASON_CODE,
    }


def _merge_reference_rows(
    merged: dict[tuple[str, str], set[str]],
    rows: list[Any],
) -> None:
    for row in rows:
        package_id, package_status, reference_source = _row_values(row)
        if not package_id or not package_status or not reference_source:
            raise ValueError(
                "strategy package factor reference query returned incomplete row: "
                f"package_id={package_id!r}, package_status={package_status!r}, reference_source={reference_source!r}"
            )
        merged.setdefault((str(package_id), str(package_status)), set()).add(str(reference_source))


def _row_values(row: Any) -> tuple[Any, Any, Any]:
    if isinstance(row, dict):
        return row.get("package_id"), row.get("package_status"), row.get("reference_source")
    return row[0], row[1], row[2]
