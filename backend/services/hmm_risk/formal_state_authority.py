"""One-time read-only industry catalog freeze for the current formal executor.

The database supplies official catalog metadata, not runtime training facts.
Training consumes the resulting existing C-013 projection authorities as files.
No private category numbering or stock classification authority is introduced.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.industry_pit_adapter import (
    HMM_G2A_DATA_A_CONTRACT_VERSION,
    HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA,
    HMM_L1_CODE_PROJECTION_VERSION,
    HMM_L2_CODE_PROJECTION_VERSION,
    build_l1_code_projection_authority,
    build_l2_code_projection_authority,
)

CATALOG_QUERY = """SELECT level,industry_code,index_code,industry_name,parent_code
FROM market.sw_index_classify
WHERE src='SW2021' AND level IN ('L1','L2') ORDER BY level,industry_code"""
MEMBER_QUERY = """SELECT DISTINCT l1_code,l2_code FROM market.sw_index_member
WHERE l1_code ~ '^801[0-9]{3}[.]SI$' AND l2_code ~ '^801[0-9]{3}[.]SI$'
ORDER BY l1_code,l2_code"""


def read_catalog(connection: Any) -> dict[str, Any]:
    """Read two small metadata queries in one repeatable, read-only transaction.

    Ownership of the connection stays with the caller. Rollback also occurs on
    failure. No mutable market facts or credential values enter the receipt.
    """
    connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='15000ms'")
            cursor.execute("SHOW transaction_read_only")
            if cursor.fetchone() != ("on",):
                raise ValueError("industry metadata transaction is not read-only")
            cursor.execute(CATALOG_QUERY)
            catalog = [
                dict(zip(("level", "industry_code", "index_code", "industry_name", "parent_code"), row, strict=True))
                for row in cursor.fetchall()
            ]
            cursor.execute(MEMBER_QUERY)
            members = [dict(zip(("l1_code", "l2_code"), row, strict=True)) for row in cursor.fetchall()]
        return {
            "schema_version": "hmm_risk_official_industry_catalog_read_v1",
            "catalog": catalog,
            "members": members,
            "query_sha256": canonical_sha256([CATALOG_QUERY, MEMBER_QUERY]),
            "transaction_read_only": True,
            "isolation": "repeatable_read",
            "database_written": False,
        }
    finally:
        connection.rollback()


def build_authority(
    *,
    taxonomy: Mapping[str, Any],
    database_read: Mapping[str, Any],
    artifact_root: str,
    identity: Mapping[str, Any],
    taxonomy_file_sha256: str,
) -> dict[str, Any]:
    """Join explicit numeric taxonomy codes to official SW textual identities."""
    if (
        taxonomy.get("schema_version") != "sw2021_taxonomy_catalog_v1"
        or taxonomy.get("contract_id") != "sw2021_classification_catalog_v1"
        or taxonomy.get("version") != "SW2021"
        or database_read.get("transaction_read_only") is not True
        or database_read.get("database_written") is not False
        or database_read.get("query_sha256") != canonical_sha256([CATALOG_QUERY, MEMBER_QUERY])
    ):
        raise ValueError("official catalog or read-only source contract differs")
    identities = taxonomy["identities"]
    # Full taxonomy catalog, not merely sectors observed in training intervals.
    l1_taxonomy = [
        {"industry_code": code, "industry_name": row["l1_name"]}
        for code, row in sorted(identities.items())
        if code == row["l1_code"]
    ]
    l2_taxonomy = [
        {
            "taxonomy_l1_code": row["l1_code"],
            "taxonomy_l1_name": row["l1_name"],
            "taxonomy_l2_code": code,
            "taxonomy_l2_name": row["l2_name"],
        }
        for code, row in sorted(identities.items())
        if code == row["l2_code"] and code != row["l1_code"]
    ]
    catalog: Sequence[Mapping[str, Any]] = database_read["catalog"]
    l1 = [dict(row) for row in catalog if row["level"] == "L1"]
    l2 = [dict(row) for row in catalog if row["level"] == "L2"]
    if len(l1) != 31 or len(l2) != 134 or len(catalog) != 165:
        raise ValueError("official catalog must close 31 L1 and 134 L2 identities")
    parents = {row["industry_code"]: row["index_code"] for row in l1}
    if len(parents) != 31:
        raise ValueError("duplicate official L1 taxonomy identity")
    for row in l2:
        # sw_index_classify.parent_code is a TAXONOMY code, not an index code.
        if row["parent_code"] not in parents:
            raise ValueError("official L2 parent taxonomy is unknown")
        row["parent_code"] = parents[row["parent_code"]]
    source_ids = ["market.sw_index_classify:SW2021", "market.sw_index_member:official_member_backed_catalog"]
    source_hashes = [taxonomy_file_sha256, canonical_sha256(dict(database_read))]
    common = {
        "taxonomy_contract_id": taxonomy["contract_id"],
        "taxonomy_version": taxonomy["version"],
        "source_ids": source_ids,
        "source_hashes": source_hashes,
    }
    l1_projection = build_l1_code_projection_authority(
        **common,
        projection_version=HMM_L1_CODE_PROJECTION_VERSION,
        taxonomy_rows=l1_taxonomy,
        published_index_rows=l1,
    )
    l2_projection = build_l2_code_projection_authority(
        **common,
        projection_version=HMM_L2_CODE_PROJECTION_VERSION,
        taxonomy_rows=l2_taxonomy,
        published_index_rows=l2,
        member_index_rows=database_read["members"],
        l1_projection_authority=l1_projection,
    )
    basis = {
        "schema_version": HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA,
        "contract_version": HMM_G2A_DATA_A_CONTRACT_VERSION,
        "active_mode": "historical_replay",
        "historical_classification_basis": "stable_taxonomy_backcast",
        "historical_non_as_known_taxonomy": True,
        "forward_classification_basis": "as_published_pit",
        "forward_non_as_known_taxonomy": False,
    }
    return {
        "artifact_root": artifact_root,
        "identity": dict(identity),
        "research_basis": {**basis, "canonical_hash": digest_named_fields(HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA, basis)},
        "l1_projection": l1_projection,
        "l2_projection": l2_projection,
    }
