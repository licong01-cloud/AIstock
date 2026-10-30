"""Insert the deterministic V2 catalog successor without overwriting any asset.

The parent JSON is a read-only export of the exact legacy catalog row. Its
source and behavioral metadata must match the reviewed parent. DEV is the
existing database, never a newly created test database. Production apply needs
the matching committed DEV receipt and an explicit authorization reference.
The default is a read-only preview. No service control or dataset mutation.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


PARENT_ID = "score_weighted_topk_v2"
STRATEGY_ID = "score_weighted_topk_v2_deterministic_v1"
CLASS_NAME = "ScoreWeightedTopkStrategyV2"
MODULE_PATH = "score_weighted_strategy_v2_deterministic_v1"
PARENT_SOURCE_SHA256 = "20f1d21b0f179c5f7f909a73b3f3c4d374affbcca66b754624a8c9c8b47fe0bf"
PARENT_BEHAVIOR_SHA256 = "696e5144c9ed39a249534274c4792d22e9ec1e68e6bcfb4548ebe66ec02e51a3"
SOURCE_FILE = Path(__file__).with_name(MODULE_PATH + ".py")
BEHAVIOR_FIELDS = (
    "scenario", "strategy_type", "portfolio_config", "default_kwargs",
    "param_schema", "python_implementation",
)
JSON_FIELDS = {
    "python_implementation", "portfolio_config", "default_kwargs",
    "param_schema", "raw_payload",
}
FIELDS = (
    "strategy_id", "catalog_version", "generated_at_utc", "catalog_source",
    "scenario", "strategy_type", "display_name", "description", "source_code",
    "source_code_relpath", "python_implementation", "portfolio_config",
    "default_kwargs", "param_schema", "in_selection_center", "raw_payload",
)


def digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate_parent(parent: dict[str, Any]) -> None:
    if parent.get("strategy_id") != PARENT_ID:
        raise ValueError("PARENT_STRATEGY_ID_MISMATCH")
    if hashlib.sha256(parent["source_code"].encode("utf-8")).hexdigest() != PARENT_SOURCE_SHA256:
        raise ValueError("PARENT_SOURCE_DRIFT")
    if digest({key: parent.get(key) for key in BEHAVIOR_FIELDS}) != PARENT_BEHAVIOR_SHA256:
        raise ValueError("PARENT_BEHAVIOR_DRIFT")


def build_payload(parent: dict[str, Any]) -> dict[str, Any]:
    validate_parent(parent)
    # Text normalization matches Composer and avoids a checkout's CRLF changing
    # the catalog identity. Frozen original workspace bytes are never rewritten.
    source = SOURCE_FILE.read_text(encoding="utf-8")
    compile(source, str(SOURCE_FILE), "exec")
    payload = {key: deepcopy(parent.get(key)) for key in FIELDS}
    payload.update(
        strategy_id=STRATEGY_ID,
        catalog_version="2.0-deterministic-v1",
        generated_at_utc="2026-10-30T00:00:00Z",
        catalog_source="aistock",
        display_name="ScoreWeightedTopk V2 确定性目录版本 v1",
        description="继承冻结V2交易语义，仅明确同分排名、候选及持仓顺序；不覆盖旧ID。",
        source_code=source,
        source_code_relpath="scripts/" + SOURCE_FILE.name,
        python_implementation={"module_path": MODULE_PATH, "class_name": CLASS_NAME},
        raw_payload={
            "schema_version": "qe_strategy_catalog_successor_v1",
            "parent_strategy_id": PARENT_ID,
            "parent_source_sha256": PARENT_SOURCE_SHA256,
            "parent_behavior_sha256": PARENT_BEHAVIOR_SHA256,
            "ordering_contract": "qe_score_weighted_topk_v2_order_v1",
            "bug_id": "BUG-2155",
        },
    )
    payload["portfolio_config"]["module_path"] = MODULE_PATH
    return payload


def validate_dev_receipt(receipt: dict[str, Any], payload: dict[str, Any]) -> None:
    if not (
        receipt.get("schema_version") == "qe_strategy_catalog_registration_v1"
        and receipt.get("target") == "dev"
        and receipt.get("database") == "aistock_dev"
        and receipt.get("committed") is True
        and receipt.get("readback_matches") is True
        and receipt.get("payload_sha256") == digest(payload)
        and receipt.get("strategy_id") == STRATEGY_ID
        and receipt.get("legacy_unchanged") is True
    ):
        raise ValueError("MATCHING_COMMITTED_DEV_RECEIPT_REQUIRED")


def connect(env_file: Path, target: str):
    from dotenv import dotenv_values
    import psycopg2

    values = dotenv_values(env_file)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    config = {key: values[prefix + name] for key, name in (
        ("host", "HOST"), ("port", "PORT"), ("dbname", "NAME"),
        ("user", "USER"), ("password", "PASSWORD"),
    )}
    config["port"] = int(config["port"])
    expected = ("aistock_dev", 5433) if target == "dev" else ("aistock", 5432)
    if config["host"] not in {"127.0.0.1", "localhost"} or (config["dbname"], config["port"]) != expected:
        raise ValueError("EXPLICIT_EXISTING_DATABASE_ENDPOINT_REQUIRED")
    return psycopg2.connect(**config, connect_timeout=10), {
        key: config[key] for key in ("host", "port", "dbname")
    }


def apply_catalog(conn, *, target: str, payload: dict[str, Any], execute: bool) -> dict[str, Any]:
    """Single-row insert/readback; rollback on all drift, no UPDATE or DELETE."""
    from psycopg2.extras import Json, RealDictCursor

    if target not in {"dev", "prod"} or payload.get("strategy_id") != STRATEGY_ID:
        raise ValueError("EXACT_TARGET_AND_SUCCESSOR_ID_REQUIRED")
    expected_database = "aistock_dev" if target == "dev" else "aistock"
    with conn:
        conn.set_session(readonly=not execute)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '30s'")
            cur.execute("SELECT current_database() AS database")
            database = cur.fetchone()["database"]
            if database != expected_database:
                raise ValueError("DATABASE_IDENTITY_MISMATCH")
            if execute:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (STRATEGY_ID,))
            cur.execute("SELECT * FROM aistock_strategy_catalog WHERE strategy_id=%s", (PARENT_ID,))
            legacy_before = cur.fetchone()
            if target == "prod":
                if legacy_before is None:
                    raise ValueError("PRODUCTION_PARENT_MISSING")
                validate_parent(dict(legacy_before))
            cur.execute("SELECT * FROM aistock_strategy_catalog WHERE strategy_id=%s", (STRATEGY_ID,))
            existing = cur.fetchone()
            if existing is not None and {key: existing[key] for key in FIELDS} != payload:
                raise ValueError("EXISTING_SUCCESSOR_DRIFT_REFUSE_OVERWRITE")
            inserted = False
            if execute and existing is None:
                placeholders = ["%s::jsonb" if key in JSON_FIELDS else "%s" for key in FIELDS]
                # All identifiers come from the static FIELDS tuple, never input.
                sql = "INSERT INTO aistock_strategy_catalog (" + ",".join(FIELDS) + ") VALUES (" + ",".join(placeholders) + ")"
                values = [Json(payload[key], dumps=lambda v: json.dumps(v, ensure_ascii=False))
                          if key in JSON_FIELDS else payload[key] for key in FIELDS]
                cur.execute(sql, values)
                if cur.rowcount != 1:
                    raise RuntimeError("SINGLE_INSERT_REQUIRED")
                inserted = True
            cur.execute("SELECT * FROM aistock_strategy_catalog WHERE strategy_id=%s", (STRATEGY_ID,))
            after = cur.fetchone()
            matches = after is not None and {key: after[key] for key in FIELDS} == payload
            if execute and not matches:
                raise RuntimeError("SUCCESSOR_READBACK_MISMATCH")
            cur.execute("SELECT * FROM aistock_strategy_catalog WHERE strategy_id=%s", (PARENT_ID,))
            legacy_after = cur.fetchone()
            if legacy_before != legacy_after:
                raise RuntimeError("LEGACY_ROW_CHANGED")
    return {
        "schema_version": "qe_strategy_catalog_registration_v1",
        "at": datetime.now(timezone.utc).isoformat(),
        "target": target, "database": database, "strategy_id": STRATEGY_ID,
        "payload_sha256": digest(payload),
        "source_sha256": hashlib.sha256(payload["source_code"].encode()).hexdigest(),
        "parent_source_sha256": PARENT_SOURCE_SHA256,
        "committed": execute, "inserted": inserted, "readback_matches": matches,
        "legacy_unchanged": True, "legacy_present": legacy_before is not None,
        "status": "APPLIED_OR_ALREADY_IDENTICAL" if execute else "READ_ONLY_PREVIEW",
        "dml_scope": {"table": "aistock_strategy_catalog", "insert_ids": [STRATEGY_ID],
                      "updates": [], "deletes": []},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("dev", "prod"), required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--parent-json", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dev-receipt", type=Path)
    parser.add_argument("--authorization-ref")
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Preserve existing receipt; reconcile state before retry.")
    if not args.output.parent.is_dir():
        raise FileNotFoundError("Receipt directory must exist before database access.")
    payload = build_payload(json.loads(args.parent_json.read_text(encoding="utf-8-sig")))
    if args.execute and args.target == "prod":
        if not args.dev_receipt or not (args.authorization_ref or "").strip():
            raise ValueError("PRODUCTION_AUTHORIZATION_AND_DEV_RECEIPT_REQUIRED")
        validate_dev_receipt(json.loads(args.dev_receipt.read_text(encoding="utf-8-sig")), payload)
    conn, endpoint = connect(args.env_file, args.target)
    try:
        receipt = apply_catalog(conn, target=args.target, payload=payload, execute=args.execute)
    finally:
        conn.close()
    receipt.update(endpoint=endpoint, authorization_ref=args.authorization_ref)
    with args.output.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(receipt, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps(receipt, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
