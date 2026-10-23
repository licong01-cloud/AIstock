"""Audited metadata-only repair for the three original L2 study registrations.

Default is preview. Never dispatches, changes status, or overwrites worker assets.
The caller must separately authorize production apply after DEV validation.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

from backend.services.quantevolver.runtime_contract import merge_qe_minute_runtime_contract  # noqa: E402

ROSTER = {
    "6ed6c1be": ("qe_l2_entry_20261023_s42_b", "completed"),
    "aeae55e7": ("qe_l2_entry_20261023_s42_l", "completed"),
    "d1ab9d9e": ("qe_l2_entry_20261023_s2024_b", "created"),
}
CONTRACT = "QE-L2-RISK-ENTRY-GATE-D1..D7"
AUDIT_KEY = "_qe_l2_registration_recovery_v1"
SCHEMA = "qe_l2_registration_recovery_v1"
RUNTIME_KEYS = {
    "runtime_mode",
    "bar_freq",
    "backtest_freq",
    "execution_algo",
    "execution_algo_params",
    "runtime_contract_version",
    "runtime_contract_source",
    "limit_suspend_authoritative",
}


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False, default=str
    ).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError("QE_L2_RECOVERY_" + message)


def read_json(path: Path) -> dict:
    before = path.stat()
    result = json.loads(path.read_bytes())
    after = path.stat()
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), "SOURCE_CHANGED")
    return result


def mapping(value: Any) -> dict:
    return json.loads(value) if isinstance(value, str) else dict(value or {})


def checked_record(record: dict, reservations: list[dict]) -> tuple[str, str]:
    identifier = record["experiment_id"]
    require(identifier in ROSTER, "OUT_OF_SCOPE")
    task, expected = ROSTER[identifier]
    require(record["status"] == expected and (record.get("alpha_mode") or "single") == "single", "SOURCE_STATUS")
    if expected == "completed":
        require(record.get("qe_task_id") == task and record.get("qe_loop_id") == "Loop1", "SOURCE_COORDINATES")
        require(bool(record.get("result_metrics")), "RESULT_MISSING")
        require(all(r["status"] == "released" for r in reservations), "RESERVATION_ACTIVE")
    else:
        require(
            not reservations
            and not record.get("qe_task_id")
            and not record.get("started_at")
            and not record.get("result_metrics"),
            "CREATED_ALREADY_ATTEMPTED",
        )
    return task, expected


def corrected_params(params: dict, conf: dict) -> dict:
    port = conf["port_analysis_config"]
    exchange = port["backtest"]["exchange_kwargs"]
    inner = port["executor"]["kwargs"]["inner_strategy"]
    require(
        exchange.get("freq") == "1min"
        and exchange.get("limit_threshold") == ["$limit_up", "$limit_down"]
        and inner.get("class") == "TailTWAPWithLimitStrategy"
        and inner.get("module_path") == "tail_twap_strategy"
        and port["strategy"]["kwargs"].get("suspend_filter_strict") is True,
        "FROZEN_EXECUTION_CONTRACT",
    )
    require(
        str(port["backtest"]["start_time"]) == "2026-04-01"
        and str(port["backtest"]["end_time"]) == "2026-09-30"
        and port["backtest"]["account"] == 100_000_000,
        "WINDOW_ACCOUNT",
    )
    result = merge_qe_minute_runtime_contract(
        params,
        config={"backtest_freq": "1min"},
        execution_algo="TWAP",
        execution_algo_params=inner["kwargs"],
        source="qe_l2_frozen_composed_config",
        require_minute=True,
    )
    result["limit_suspend_authoritative"] = True
    require(all(result[k] == v for k, v in params.items() if k not in RUNTIME_KEYS), "SEMANTIC_DRIFT")
    # This repair fills omitted facts; it cannot overwrite conflicting execution facts.
    for key in RUNTIME_KEYS & params.keys():
        require(params[key] == result[key], "EXISTING_RUNTIME_CONFLICT_" + key)
    return result


def build_plan(record: dict, reservations: list[dict], folder: Path) -> dict:
    task, status = checked_record(record, reservations)
    params = mapping(record["custom_params"])
    require(AUDIT_KEY not in params, "ALREADY_AMENDED_USE_ORIGINAL_PLAN")
    original = read_json(folder / "study_plan.json")
    preflight = read_json(folder / "remote_preflight.json")
    # Submission transports text after Python universal-newline reading. Match
    # those exact worker bytes and retain the local physical-file digest too.
    conf_disk = (folder / "conf.yaml").read_bytes()
    conf_bytes = (folder / "conf.yaml").read_text(encoding="utf-8").encode("utf-8")
    conf_sha = hashlib.sha256(conf_bytes).hexdigest()
    require(original["task_id"] == task and original["experiment_id"] == record["experiment_id"], "PLAN_COORDINATES")
    require(preflight["status"] == "PASS" and preflight["plan_sha256"] == digest(original), "PLAN_HASH")
    require(preflight["file_hashes"]["conf.yaml"] == conf_sha, "CONF_HASH")
    require(params == original["custom_params"], "REGISTRY_PLAN_DRIFT")
    evidence = {
        "original_plan_sha256": digest(original),
        "conf_sha256": conf_sha,
        "local_conf_file_sha256": hashlib.sha256(conf_disk).hexdigest(),
        "preflight_sha256": digest(preflight),
    }
    if status == "completed":
        preserved = read_json(folder / "preservation_receipt.json")
        require(
            preserved["task_id"] == task
            and preserved["status"] == "PRESERVED_AND_RETROSPECTIVE_MINUTE_GUARDS_PASS"
            and preserved["original_worker_packages_unchanged"] is True
            and preserved["files"]["conf.yaml"]["sha256"] == conf_sha,
            "PRESERVATION",
        )
        evidence["preservation_sha256"] = digest(preserved)
    corrected = corrected_params(params, yaml.safe_load(conf_bytes))
    require(corrected != params, "NOTHING_TO_REPAIR")
    audit = {
        "schema_version": SCHEMA,
        "contract": CONTRACT,
        "experiment_id": record["experiment_id"],
        "task_id": task,
        "before_params_sha256": digest(params),
        "corrected_params_sha256": digest(corrected),
        "evidence": evidence,
        "changed_keys": sorted(k for k in corrected if corrected.get(k) != params.get(k)),
        "original_worker_package_unchanged": True,
        "results_unchanged": True,
    }
    corrected[AUDIT_KEY] = audit
    return {
        "schema_version": SCHEMA,
        "experiment_id": record["experiment_id"],
        "task_id": task,
        "expected_status": status,
        "before_record_sha256": digest(record),
        "before_params": params,
        "after_params": corrected,
        "audit": audit,
        "created_not_dispatched_proof": status == "created",
        "dispatch_authorized": False,
    }


def fetch(conn, identifier: str, *, locked: bool = False) -> tuple[dict, list[dict]]:
    require(identifier in ROSTER, "OUT_OF_SCOPE")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM qe_experiments WHERE experiment_id=%s" + (" FOR UPDATE" if locked else ""), (identifier,)
        )
        row = cur.fetchone()
        require(row is not None, "SOURCE_MISSING")
        cur.execute(
            "SELECT status, reservation_id FROM infra.qe_execution_reservation WHERE source_kind='qe_experiment' AND source_execution_id=%s",
            (identifier,),
        )
        return dict(row), [dict(x) for x in cur.fetchall()]


def apply_plan(conn, plan: dict) -> dict:
    """Runs within caller-owned transaction. No status, metrics, or asset writes."""
    identifier = plan["experiment_id"]
    require(plan.get("schema_version") == SCHEMA and identifier in ROSTER, "PLAN_SCHEMA")
    require(plan["task_id"] == ROSTER[identifier][0] and plan["expected_status"] == ROSTER[identifier][1], "PLAN_SCOPE")
    before, reservations = fetch(conn, identifier, locked=True)
    checked_record(before, reservations)
    after_params = plan["after_params"]
    audit = after_params.get(AUDIT_KEY)
    require(audit == plan["audit"] and audit["before_params_sha256"] == digest(plan["before_params"]), "PLAN_AUDIT")
    corrected = {k: v for k, v in after_params.items() if k != AUDIT_KEY}
    require(audit["corrected_params_sha256"] == digest(corrected), "PLAN_AFTER_HASH")
    require(all(corrected.get(k) == v for k, v in plan["before_params"].items()), "PLAN_OVERWRITE")
    require(set(corrected) - set(plan["before_params"]) <= RUNTIME_KEYS, "PLAN_EXTRA_FIELDS")
    if mapping(before["custom_params"]) == after_params:
        original_row = deepcopy(before)
        original_row["custom_params"] = plan["before_params"]
        require(digest(original_row) == plan["before_record_sha256"], "AMENDED_SOURCE_DRIFT")
        return {"experiment_id": identifier, "status": "ALREADY_APPLIED", "audit": audit}
    require(
        digest(before) == plan["before_record_sha256"] and mapping(before["custom_params"]) == plan["before_params"],
        "CONCURRENT_SOURCE_CHANGE",
    )
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE qe_experiments SET custom_params=%s::jsonb WHERE experiment_id=%s",
            (json.dumps(after_params, ensure_ascii=False), identifier),
        )
        require(cur.rowcount == 1, "UPDATE_COUNT")
    after, _ = fetch(conn, identifier)
    require(mapping(after["custom_params"]) == after_params, "READBACK")
    unchanged = deepcopy(after)
    unchanged["custom_params"] = before["custom_params"]
    require(digest(unchanged) == digest(before), "NON_METADATA_WRITE")
    return {"experiment_id": identifier, "status": "APPLIED", "audit": audit, "status_results_identity_unchanged": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    # Production apply is deliberately separate from automatic preview/DEV execution.
    parser.add_argument("--mode", choices=["preview", "dev"], default="preview")
    args = parser.parse_args()
    from dotenv import dotenv_values
    import psycopg2

    values = dotenv_values(args.env_file)

    def connect(prefix):
        return psycopg2.connect(
            host=values[prefix + "HOST"],
            port=values[prefix + "PORT"],
            dbname=values[prefix + "NAME"],
            user=values[prefix + "USER"],
            password=values[prefix + "PASSWORD"],
            connect_timeout=10,
        )

    args.output.mkdir(parents=True, exist_ok=False)
    production = connect("TDX_DB_")
    production.set_session(readonly=True)
    try:
        records = [fetch(production, key) for key in ROSTER]
        plans = [
            build_plan(row, reservations, args.evidence / "composed" / ROSTER[row["experiment_id"]][0])
            for row, reservations in records
        ]
    finally:
        production.rollback()
        production.close()
    with (args.output / "plans.json").open("x", encoding="utf-8") as file:
        json.dump(plans, file, ensure_ascii=False, indent=2, default=str)
    receipt = {
        "schema_version": SCHEMA,
        "mode": args.mode,
        "plan_sha256": digest(plans),
        "experiments": list(ROSTER),
        "production_write": False,
        "backtests": 0,
        "observed_at": datetime.now().astimezone().isoformat(),
    }
    if args.mode == "dev":
        require(values["TDX_DB_DEV_NAME"] != values["TDX_DB_NAME"], "DEV_IS_PRODUCTION")
        conn = connect("TDX_DB_DEV_")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                require(cur.fetchone()[0] == values["TDX_DB_DEV_NAME"], "DEV_DATABASE")
                for row, _ in records:
                    cur.execute("SELECT 1 FROM qe_experiments WHERE experiment_id=%s", (row["experiment_id"],))
                    require(cur.fetchone() is None, "DEV_SOURCE_ALREADY_EXISTS")
                    columns = list(row)
                    from psycopg2 import sql

                    cur.execute(
                        sql.SQL("INSERT INTO qe_experiments ({}) VALUES ({})").format(
                            sql.SQL(",").join(map(sql.Identifier, columns)),
                            sql.SQL(",").join(sql.Placeholder() * len(columns)),
                        ),
                        [
                            json.dumps(row[k], default=str) if isinstance(row[k], (dict, list)) else row[k]
                            for k in columns
                        ],
                    )
            results = []
            for plan in plans:
                # DEV has its own row timestamps/defaults; compare the actual inserted original.
                row, _ = fetch(conn, plan["experiment_id"])
                dev_plan = dict(plan, before_record_sha256=digest(row))
                results.append(apply_plan(conn, dev_plan))
                require(apply_plan(conn, dev_plan)["status"] == "ALREADY_APPLIED", "DEV_IDEMPOTENCY")
            receipt.update(status="DEV_PASS_ROLLED_BACK", results=results, database=values["TDX_DB_DEV_NAME"])
        finally:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM qe_experiments WHERE experiment_id=ANY(%s)", (list(ROSTER),))
                require(cur.fetchone()[0] == 0, "DEV_ROLLBACK")
            conn.close()
    else:
        receipt["status"] = "PREVIEW_ONLY"
    with (args.output / "receipt.json").open("x", encoding="utf-8") as file:
        json.dump(receipt, file, ensure_ascii=False, indent=2)
    print(json.dumps({k: v for k, v in receipt.items() if k != "results"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
