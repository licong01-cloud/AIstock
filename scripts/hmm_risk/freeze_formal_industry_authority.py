"""Freeze official industry catalog metadata without modifying database/data release."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def main() -> int:
    from dotenv import dotenv_values
    import psycopg2

    from backend.services.hmm_risk.formal_state_authority import build_authority, read_catalog
    from backend.services.hmm_risk.formal_state_executor import PIT_BUNDLE, read_json, write_once
    from backend.services.hmm_risk.rotation_l1_input_bundle import _industry_adapter, _is_indirect_path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authority-root", required=True, type=Path)
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    authority_root, output = args.authority_root, args.output
    if (
        not authority_root.is_absolute()
        or not authority_root.is_dir()
        or _is_indirect_path(authority_root)
        or not output.is_absolute()
        or _is_indirect_path(output)
        or output.exists()
        or any(output.is_relative_to(root) for root in (ROOT, authority_root, authority_root.parents[3]))
    ):
        raise ValueError("authority/output must be explicit ordinary paths; output must be external and new")
    manifest = read_json(authority_root / "candidate_bundle_manifest.json")
    if manifest["bundle_hash"] != PIT_BUNDLE:
        raise ValueError("only approved full-v3 PIT authority may be frozen")
    taxonomy_path = authority_root / "taxonomy_catalog.json"
    taxonomy_hash = hashlib.sha256(taxonomy_path.read_bytes()).hexdigest()
    if taxonomy_hash != manifest["files"]["taxonomy_catalog.json"]["sha256"]:
        raise ValueError("taxonomy file differs from frozen full-v3 bundle")
    classification = read_json(authority_root / "classification_authority_receipt.json")
    membership = read_json(authority_root / "index_membership_authority_receipt.json")
    preflight = read_json(authority_root / "full_denominator_preflight.json")
    identity = {
        "schema_version": "hmm_risk_industry_pit_authority_v1",
        "bundle_hash": manifest["bundle_hash"],
        "classification_candidate_hash": manifest["classification_candidate_hash"],
        "index_membership_candidate_hash": manifest["index_membership_candidate_hash"],
        "classification_receipt_hash": classification["receipt_hash"],
        "index_membership_receipt_hash": membership["receipt_hash"],
        "preflight_canonical_hash": preflight["canonical_hash"],
    }
    config = {**dotenv_values(args.env_file), **os.environ}
    fields = {
        "host": "TDX_DB_HOST",
        "port": "TDX_DB_PORT",
        "user": "TDX_DB_USER",
        "password": "TDX_DB_PASSWORD",
        "dbname": "TDX_DB_NAME",
    }
    if any(not config.get(env) for env in fields.values()):
        raise ValueError("explicit database configuration is incomplete")
    connection = psycopg2.connect(
        **{field: config[env] for field, env in fields.items()},
        connect_timeout=8,
        application_name="hmm_formal_catalog_readonly",
    )
    try:
        database_read = read_catalog(connection)
    finally:
        connection.close()
    authority = build_authority(
        taxonomy=read_json(taxonomy_path),
        database_read=database_read,
        artifact_root=str(authority_root),
        identity=identity,
        taxonomy_file_sha256=taxonomy_hash,
    )
    _industry_adapter(authority, forbidden_roots=(ROOT,))
    # One compact current input; no historical evidence archive or raw-market export.
    write_once(output, authority)
    print(
        json.dumps(
            {
                "status": "PASS",
                "output": str(output),
                "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                "l1_count": len(authority["l1_projection"]["rows"]),
                "l2_count": len(authority["l2_projection"]["rows"]),
                "database_written": False,
                "shared_dataset_written": False,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
