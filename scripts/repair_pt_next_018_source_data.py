"""Apply the sealed PT-NEXT-018 source repair to DEV or production.

This is a bounded, idempotent data migration.  It never fetches a provider,
changes a dataset pointer, or starts a service.  Production apply requires the
exact plan/target digests and a clean canonical ``main`` at the authorized
merge commit.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import stat
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_RELATIVE_PATH = "scripts/repair_pt_next_018_source_data.py"
RECEIPT_SCHEMA = "pt_next_018_source_data_migration_receipt_v1"
ADJ_AUTHORITY_SCHEMA = "dataset_release_adj_factor_restatement_authority_v1"
RIGHTS_AUTHORITY_SCHEMA = "position_timing_rights_issue_authority_v1"
RIGHTS_EVENT_SCHEMA = "position_timing_rights_issue_event_v1"
RULE_VERSION = "position_timing_rights_issue_authority_v1"
RUN_ID = "pt_next_018_rights_issue_4a7cdb79e968f33a"
EXPECTED_ADJ_CANONICAL_SHA256 = "c40f3c991ac31b570e7a739bb1898a59f12e202f2e96e9bcd8399211e5323edd"
EXPECTED_ADJ_FILE_SHA256 = "1639b06a1e43998273ac21e6d5593671c0be934fd24714f8ebfe920bfe3a96ad"
EXPECTED_RIGHTS_CANONICAL_SHA256 = "4a7cdb79e968f33a000f2e9b81196986349cff26100688794b87f6a1f454f10c"
EXPECTED_RIGHTS_FILE_SHA256 = "545393b2e1bd7cba8f18979399152e15b1f34881eb851124b6ae3c67472e6d66"
EXPECTED_ADJ_SERIES = {
    "300506.SZ": ("2016-03-24", "2026-08-31", 2537),
    "688109.SH": ("2021-03-30", "2026-08-31", 1316),
}
EXPECTED_RIGHTS_KEYS = (
    "RIGHTS_ISSUE:000970.SZ:2022-02-15",
    "RIGHTS_ISSUE:600008.SH:2020-09-18",
    "RIGHTS_ISSUE:601236.SH:2021-07-26",
)
CONFIRMATIONS = {
    "dev": "APPLY_PT_NEXT_018_SOURCE_DATA_DEV",
    "production": "APPLY_PT_NEXT_018_SOURCE_DATA_PRODUCTION",
}
DB_ENV_SUFFIXES = ("HOST", "PORT", "NAME", "USER", "PASSWORD")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class RepairError(RuntimeError):
    """Fail-closed PT-NEXT-018 data migration error."""


@dataclass(frozen=True)
class AuthorityBundle:
    adj_rows: tuple[tuple[str, dt.date, float], ...]
    rights_events: tuple[Mapping[str, Any], ...]
    adj_canonical_sha256: str
    adj_file_sha256: str
    rights_canonical_sha256: str
    rights_file_sha256: str
    cutoff_trade_date: str


@dataclass(frozen=True)
class RepairPlan:
    schema_version: str
    target: str
    target_identity_sha256: str
    database_name: str
    cutoff_trade_date: str
    adj_authority_sha256: str
    rights_authority_sha256: str
    adj_authority_rows: int
    adj_existing_rows: int
    adj_missing_rows: int
    adj_mismatch_rows: int
    adj_unchanged_rows: int
    adj_existing_rows_sha256: str
    rights_expected_rows: int
    rights_existing_rows: int
    rights_mismatch_rows: int
    rights_existing_rows_sha256: str
    plan_digest: str


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")


def _json_dumps(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_sha256(value: Any, *, field: str) -> str:
    text = str(value or "")
    if SHA256_RE.fullmatch(text) is None:
        raise RepairError(f"{field} is not a canonical SHA-256")
    return text


def _assert_plain_existing_chain(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        metadata = current.lstat()
        attributes = int(getattr(metadata, "st_file_attributes", 0))
        reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if stat.S_ISLNK(metadata.st_mode) or attributes & reparse:
            raise RepairError("authority path contains a link or reparse point")


def _load_canonical_file(
    path: Path,
    *,
    expected_file_sha256: str,
    expected_canonical_sha256: str,
) -> Mapping[str, Any]:
    if not path.is_file():
        raise RepairError(f"authority file is missing: {path.name}")
    if sha256_file(path) != expected_file_sha256:
        raise RepairError(f"authority file identity differs: {path.name}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RepairError(f"authority file is unreadable: {path.name}") from exc
    if not isinstance(payload, Mapping):
        raise RepairError(f"authority root is not an object: {path.name}")
    canonical = _assert_sha256(payload.get("canonical_sha256"), field=f"{path.name} canonical_sha256")
    unsigned = {key: value for key, value in payload.items() if key != "canonical_sha256"}
    if canonical != expected_canonical_sha256 or sha256_bytes(canonical_json_bytes(unsigned)) != canonical:
        raise RepairError(f"authority canonical identity differs: {path.name}")
    return payload


def load_authorities(authority_root: Path) -> AuthorityBundle:
    if not authority_root.is_absolute():
        raise RepairError("authority root must be absolute")
    _assert_plain_existing_chain(authority_root)
    root = authority_root.resolve(strict=True)
    if not root.is_dir() or root.is_relative_to(REPOSITORY_ROOT.resolve(strict=True)):
        raise RepairError("authority root must be a repo-external directory")
    adj = _load_canonical_file(
        root / "adj_factor_restatement_authority.json",
        expected_file_sha256=EXPECTED_ADJ_FILE_SHA256,
        expected_canonical_sha256=EXPECTED_ADJ_CANONICAL_SHA256,
    )
    rights = _load_canonical_file(
        root / "rights_issue_authority.json",
        expected_file_sha256=EXPECTED_RIGHTS_FILE_SHA256,
        expected_canonical_sha256=EXPECTED_RIGHTS_CANONICAL_SHA256,
    )
    if (
        adj.get("schema_version") != ADJ_AUTHORITY_SCHEMA
        or adj.get("provider") != "tushare"
        or adj.get("cutoff_trade_date") != "2026-08-31"
        or adj.get("request_id") != "PT-NEXT-018"
    ):
        raise RepairError("adj-factor authority contract differs")
    raw_series = adj.get("series")
    if not isinstance(raw_series, list) or len(raw_series) != len(EXPECTED_ADJ_SERIES):
        raise RepairError("adj-factor authority series count differs")
    adj_rows: list[tuple[str, dt.date, float]] = []
    seen_keys: set[tuple[str, dt.date]] = set()
    for series in raw_series:
        if not isinstance(series, Mapping):
            raise RepairError("adj-factor authority series is invalid")
        code = str(series.get("ts_code", ""))
        expected = EXPECTED_ADJ_SERIES.get(code)
        rows = series.get("rows")
        if (
            expected is None
            or not isinstance(rows, list)
            or (series.get("start"), series.get("end"), series.get("row_count")) != expected
            or len(rows) != expected[2]
        ):
            raise RepairError(f"adj-factor authority series identity differs: {code}")
        portable: list[Mapping[str, Any]] = []
        previous: dt.date | None = None
        for raw in rows:
            if not isinstance(raw, Mapping) or set(raw) != {"ts_code", "trade_date", "adj_factor"}:
                raise RepairError(f"adj-factor row contract differs: {code}")
            day = dt.date.fromisoformat(str(raw["trade_date"]))
            factor = float(raw["adj_factor"])
            key = (str(raw["ts_code"]), day)
            if key[0] != code or key in seen_keys or (previous is not None and day <= previous):
                raise RepairError(f"adj-factor rows are duplicated or unordered: {code}")
            if not math.isfinite(factor) or factor <= 0:
                raise RepairError(f"adj-factor value is invalid: {code}:{day}")
            previous = day
            seen_keys.add(key)
            portable.append({"ts_code": code, "trade_date": day.isoformat(), "adj_factor": factor})
            adj_rows.append((code, day, factor))
        if (
            _assert_sha256(series.get("ordered_rows_sha256"), field=f"{code} ordered_rows_sha256")
            != sha256_bytes(canonical_json_bytes(portable))
        ):
            raise RepairError(f"adj-factor ordered row identity differs: {code}")
    if [code for code, _day, _factor in adj_rows] != sorted(code for code, _day, _factor in adj_rows):
        raise RepairError("adj-factor authority codes are unordered")

    raw_events = rights.get("events")
    if (
        rights.get("schema_version") != RIGHTS_AUTHORITY_SCHEMA
        or rights.get("event_count") != len(EXPECTED_RIGHTS_KEYS)
        or not isinstance(raw_events, list)
    ):
        raise RepairError("rights-issue authority contract differs")
    events = tuple(sorted(raw_events, key=lambda item: str(item.get("event_id", ""))))
    if tuple(str(item.get("event_id", "")) for item in events) != EXPECTED_RIGHTS_KEYS:
        raise RepairError("rights-issue authority event keys differ")
    required = {
        "schema_version", "event_id", "event_key", "event_type", "symbol",
        "announcement_date", "disclosure_available_at", "source_time_quality",
        "record_date", "payment_start_date", "payment_end_date", "ex_right_date",
        "resume_date", "listing_date", "entitlement_ratio", "subscription_price",
        "actual_offered_quantity", "actual_subscribed_quantity", "issue_success_status",
        "rounding_rule", "source_url", "source_content_sha256", "source_file_size",
        "source_document_type", "source_record_key", "captured_at", "versions",
    }
    for event in events:
        if (
            not isinstance(event, Mapping)
            or set(event) != required
            or event.get("schema_version") != RIGHTS_EVENT_SCHEMA
            or event.get("event_type") != "RIGHTS_ISSUE"
            or event.get("issue_success_status") != "SUCCESS"
            or not isinstance(event.get("versions"), list)
            or not event.get("versions")
        ):
            raise RepairError(f"rights-issue event contract differs: {event.get('event_id')}")
        _assert_sha256(event.get("source_content_sha256"), field="rights source_content_sha256")
    return AuthorityBundle(
        adj_rows=tuple(adj_rows),
        rights_events=events,
        adj_canonical_sha256=EXPECTED_ADJ_CANONICAL_SHA256,
        adj_file_sha256=EXPECTED_ADJ_FILE_SHA256,
        rights_canonical_sha256=EXPECTED_RIGHTS_CANONICAL_SHA256,
        rights_file_sha256=EXPECTED_RIGHTS_FILE_SHA256,
        cutoff_trade_date="2026-08-31",
    )


def database_config(target: str, env_file: Path) -> dict[str, Any]:
    values = {str(key): str(value) for key, value in dotenv_values(env_file).items() if value is not None}
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    required = [f"{prefix}{suffix}" for suffix in DB_ENV_SUFFIXES]
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise RepairError("database environment is incomplete: " + ",".join(missing))
    config = {
        "host": values[f"{prefix}HOST"],
        "port": int(values[f"{prefix}PORT"]),
        "dbname": values[f"{prefix}NAME"],
        "user": values[f"{prefix}USER"],
        "password": values[f"{prefix}PASSWORD"],
        "connect_timeout": 10,
        "application_name": f"aistock_pt_next_018_source_repair_{target}",
    }
    host = str(config["host"]).lower()
    if target == "dev":
        valid = host in {"127.0.0.1", "localhost"} and config["port"] == 5433 and config["dbname"] == "aistock_dev"
    else:
        valid = host in {"127.0.0.1", "localhost"} and config["port"] == 5432 and config["dbname"] == "aistock"
    if not valid:
        raise RepairError(f"{target} database target identity is invalid")
    return config


def target_identity_sha256(config: Mapping[str, Any]) -> str:
    identity = {
        "host": str(config["host"]).strip().lower(),
        "port": int(config["port"]),
        "dbname": str(config["dbname"]),
        "user": str(config["user"]),
    }
    return sha256_bytes(canonical_json_bytes(identity))


def _canonical_timestamp(value: Any) -> str:
    if isinstance(value, str):
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, dt.datetime):
        parsed = value
    else:
        raise RepairError("event timestamp type differs")
    if parsed.tzinfo is None:
        raise RepairError("event timestamp lacks timezone")
    return parsed.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _expected_fact(event: Mapping[str, Any], authority: AuthorityBundle) -> Mapping[str, Any]:
    payload = {
        "schema_version": "market_event_fact_rights_issue_v1",
        "authority_canonical_sha256": authority.rights_canonical_sha256,
        "event": dict(event),
    }
    return {
        "event_key": str(event["event_id"]),
        "ts_code": str(event["symbol"]),
        "event_family": "corporate_action",
        "event_type": "RIGHTS_ISSUE",
        "event_status": "ACTIVE",
        "source_type": "official_rights_issue_authority",
        "source_pk": str(event["event_id"]),
        "source_record_key": str(event["event_key"]),
        "source_event_date": str(event["announcement_date"]),
        "source_available_at": _canonical_timestamp(event["disclosure_available_at"]),
        "source_time_quality": "OBSERVED",
        "available_at": _canonical_timestamp(event["disclosure_available_at"]),
        "effective_trade_date": str(event["announcement_date"]),
        "time_mode": "backtest",
        "rule_version": RULE_VERSION,
        "run_id": RUN_ID,
        "fact_confidence": 1.0,
        "facts": payload,
        "source_payload_hash": sha256_bytes(canonical_json_bytes(event)),
    }


def _validate_database_contract(cursor: Any, *, expected_database: str) -> None:
    cursor.execute("SELECT current_database(), current_setting('transaction_read_only')")
    database, read_only = cursor.fetchone()
    if database != expected_database:
        raise RepairError(f"connected database identity differs: {database}")
    expected_adj = [("ts_code", "text"), ("trade_date", "date"), ("adj_factor", "double precision")]
    cursor.execute(
        """SELECT column_name, data_type FROM information_schema.columns
             WHERE table_schema='market' AND table_name='adj_factor' ORDER BY ordinal_position"""
    )
    if cursor.fetchall() != expected_adj:
        raise RepairError("market.adj_factor schema differs")
    cursor.execute(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='market.adj_factor'::regclass AND contype='p'"
    )
    row = cursor.fetchone()
    if row is None or row[0] != "PRIMARY KEY (ts_code, trade_date)":
        raise RepairError("market.adj_factor primary key differs")
    cursor.execute(
        """SELECT column_name FROM information_schema.columns
             WHERE table_schema='market' AND table_name='event_fact'"""
    )
    columns = {row[0] for row in cursor.fetchall()}
    required = {
        "event_key", "ts_code", "event_family", "event_type", "event_status", "source_type",
        "source_pk", "source_record_key", "source_event_date", "source_available_at",
        "source_time_quality", "available_at", "effective_trade_date", "time_mode", "rule_version",
        "run_id", "fact_confidence", "facts", "source_payload_hash",
    }
    if not required.issubset(columns):
        raise RepairError("market.event_fact schema is incomplete")
    if read_only not in {"on", "off"}:
        raise RepairError("database transaction mode is unavailable")


def _normalize_existing_fact(row: Sequence[Any]) -> Mapping[str, Any]:
    return {
        "event_key": row[0], "ts_code": row[1], "event_family": row[2], "event_type": row[3],
        "event_status": row[4], "source_type": row[5], "source_pk": row[6],
        "source_record_key": row[7], "source_event_date": row[8].isoformat(),
        "source_available_at": _canonical_timestamp(row[9]), "source_time_quality": row[10],
        "available_at": _canonical_timestamp(row[11]), "effective_trade_date": row[12].isoformat(),
        "time_mode": row[13], "rule_version": row[14], "run_id": row[15],
        "fact_confidence": float(row[16]), "facts": row[17], "source_payload_hash": row[18],
    }


def build_plan(connection: Any, *, target: str, config: Mapping[str, Any], authority: AuthorityBundle) -> RepairPlan:
    codes = sorted(EXPECTED_ADJ_SERIES)
    cutoff = dt.date.fromisoformat(authority.cutoff_trade_date)
    with connection.cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = '60s'")
        _validate_database_contract(cursor, expected_database=str(config["dbname"]))
        cursor.execute(
            """SELECT ts_code, trade_date, adj_factor FROM market.adj_factor
                 WHERE ts_code = ANY(%s) AND trade_date <= %s ORDER BY ts_code, trade_date""",
            (codes, cutoff),
        )
        existing_adj = tuple((str(code), day, float(value)) for code, day, value in cursor.fetchall())
        cursor.execute(
            """SELECT event_key, ts_code, event_family, event_type, event_status, source_type,
                      source_pk, source_record_key, source_event_date, source_available_at,
                      source_time_quality, available_at, effective_trade_date, time_mode,
                      rule_version, run_id, fact_confidence, facts, source_payload_hash
                 FROM market.event_fact WHERE event_key = ANY(%s) ORDER BY event_key""",
            (list(EXPECTED_RIGHTS_KEYS),),
        )
        existing_facts = tuple(_normalize_existing_fact(row) for row in cursor.fetchall())
    expected_adj = {(code, day): value for code, day, value in authority.adj_rows}
    unexpected = [(code, day.isoformat()) for code, day, _value in existing_adj if (code, day) not in expected_adj]
    if unexpected:
        raise RepairError(f"database has adj-factor keys outside sealed full series: {unexpected[:5]}")
    existing_map = {(code, day): value for code, day, value in existing_adj}
    missing = [key for key in expected_adj if key not in existing_map]
    mismatch = [
        key for key, value in expected_adj.items()
        if key in existing_map and not math.isclose(existing_map[key], value, rel_tol=0.0, abs_tol=1e-12)
    ]
    expected_facts = {str(event["event_id"]): _expected_fact(event, authority) for event in authority.rights_events}
    existing_fact_map = {str(item["event_key"]): item for item in existing_facts}
    foreign = [key for key, item in existing_fact_map.items() if item["source_type"] != "official_rights_issue_authority"]
    if foreign:
        raise RepairError(f"rights-issue event keys are owned by another source: {foreign}")
    fact_mismatch = [key for key, item in existing_fact_map.items() if item != expected_facts[key]]
    unsigned = {
        "schema_version": "pt_next_018_source_data_migration_plan_v1",
        "target": target,
        "target_identity_sha256": target_identity_sha256(config),
        "database_name": str(config["dbname"]),
        "cutoff_trade_date": authority.cutoff_trade_date,
        "adj_authority_sha256": authority.adj_canonical_sha256,
        "rights_authority_sha256": authority.rights_canonical_sha256,
        "adj_authority_rows": len(authority.adj_rows),
        "adj_existing_rows": len(existing_adj),
        "adj_missing_rows": len(missing),
        "adj_mismatch_rows": len(mismatch),
        "adj_unchanged_rows": len(authority.adj_rows) - len(missing) - len(mismatch),
        "adj_existing_rows_sha256": sha256_bytes(canonical_json_bytes(existing_adj)),
        "rights_expected_rows": len(expected_facts),
        "rights_existing_rows": len(existing_facts),
        "rights_mismatch_rows": len(fact_mismatch),
        "rights_existing_rows_sha256": sha256_bytes(canonical_json_bytes(existing_facts)),
    }
    return RepairPlan(**unsigned, plan_digest=sha256_bytes(canonical_json_bytes(unsigned)))


def _seed_rights_authority(connection: Any, authority: AuthorityBundle) -> int:
    config = {
        "schema_version": RIGHTS_AUTHORITY_SCHEMA,
        "authority_canonical_sha256": authority.rights_canonical_sha256,
        "event_count": len(authority.rights_events),
    }
    with connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO market.event_signal_rule_set
                   (rule_version, engine_name, rule_source, rule_scope, config_hash, config, source_rule_versions, is_active)
               VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,TRUE)
               ON CONFLICT (rule_version) DO NOTHING""",
            (
                RULE_VERSION,
                "position_timing_rights_issue_authority",
                "content_addressed_official_announcement_authority",
                "PT-NEXT-018 three approved rights issues",
                authority.rights_canonical_sha256,
                json.dumps(config, ensure_ascii=False, sort_keys=True),
                json.dumps({"rights_issue_authority": authority.rights_canonical_sha256}),
            ),
        )
        cursor.execute(
            "SELECT config_hash, config FROM market.event_signal_rule_set WHERE rule_version=%s",
            (RULE_VERSION,),
        )
        row = cursor.fetchone()
        if row is None or row[0] != authority.rights_canonical_sha256 or row[1] != config:
            raise RepairError("rights-issue rule-set identity conflicts")
        cursor.execute(
            """INSERT INTO market.event_signal_run
                   (run_id, rule_version, run_mode, time_mode, source_scope, date_from, date_to,
                    finished_at, status, source_input_rows, fact_rows, metrics)
               VALUES (%s,%s,'repair','backtest',%s::jsonb,%s,%s,NOW(),'SUCCESS',%s,%s,%s::jsonb)
               ON CONFLICT (run_id) DO NOTHING""",
            (
                RUN_ID,
                RULE_VERSION,
                json.dumps({"symbols": [event["symbol"] for event in authority.rights_events]}),
                min(str(event["announcement_date"]) for event in authority.rights_events),
                max(str(event["listing_date"]) for event in authority.rights_events),
                len(authority.rights_events),
                len(authority.rights_events),
                json.dumps({"authority_canonical_sha256": authority.rights_canonical_sha256}),
            ),
        )
    facts = [_expected_fact(event, authority) for event in authority.rights_events]
    values = [
        (
            item["event_key"], item["ts_code"], item["event_family"], item["event_type"], item["event_status"],
            item["source_type"], item["source_pk"], item["source_record_key"], item["source_event_date"],
            item["source_available_at"], item["source_time_quality"], item["available_at"],
            item["effective_trade_date"], item["time_mode"], item["rule_version"], item["run_id"],
            item["fact_confidence"], psycopg2.extras.Json(item["facts"], dumps=_json_dumps),
            item["source_payload_hash"],
        )
        for item in facts
    ]
    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            """INSERT INTO market.event_fact
                   (event_key,ts_code,event_family,event_type,event_status,source_type,source_pk,
                    source_record_key,source_event_date,source_available_at,source_time_quality,
                    available_at,effective_trade_date,time_mode,rule_version,run_id,fact_confidence,
                    facts,source_payload_hash)
               VALUES %s
               ON CONFLICT (event_key) DO UPDATE SET
                    ts_code=EXCLUDED.ts_code,event_family=EXCLUDED.event_family,event_type=EXCLUDED.event_type,
                    event_status=EXCLUDED.event_status,source_type=EXCLUDED.source_type,source_pk=EXCLUDED.source_pk,
                    source_record_key=EXCLUDED.source_record_key,source_event_date=EXCLUDED.source_event_date,
                    source_available_at=EXCLUDED.source_available_at,source_time_quality=EXCLUDED.source_time_quality,
                    available_at=EXCLUDED.available_at,effective_trade_date=EXCLUDED.effective_trade_date,
                    time_mode=EXCLUDED.time_mode,rule_version=EXCLUDED.rule_version,run_id=EXCLUDED.run_id,
                    fact_confidence=EXCLUDED.fact_confidence,facts=EXCLUDED.facts,
                    source_payload_hash=EXCLUDED.source_payload_hash,generated_at=NOW(),updated_at=NOW()
               WHERE (market.event_fact.ts_code,market.event_fact.event_family,market.event_fact.event_type,
                      market.event_fact.event_status,market.event_fact.source_type,market.event_fact.source_pk,
                      market.event_fact.source_record_key,market.event_fact.source_event_date,
                      market.event_fact.source_available_at,market.event_fact.source_time_quality,
                      market.event_fact.available_at,market.event_fact.effective_trade_date,
                      market.event_fact.time_mode,market.event_fact.rule_version,market.event_fact.run_id,
                      market.event_fact.fact_confidence,market.event_fact.facts,market.event_fact.source_payload_hash)
                     IS DISTINCT FROM
                     (EXCLUDED.ts_code,EXCLUDED.event_family,EXCLUDED.event_type,EXCLUDED.event_status,
                      EXCLUDED.source_type,EXCLUDED.source_pk,EXCLUDED.source_record_key,
                      EXCLUDED.source_event_date,EXCLUDED.source_available_at,EXCLUDED.source_time_quality,
                      EXCLUDED.available_at,EXCLUDED.effective_trade_date,EXCLUDED.time_mode,
                      EXCLUDED.rule_version,EXCLUDED.run_id,EXCLUDED.fact_confidence,EXCLUDED.facts,
                      EXCLUDED.source_payload_hash)""",
            values,
            page_size=100,
        )
        return int(cursor.rowcount)


def _apply_rows(connection: Any, authority: AuthorityBundle) -> tuple[int, int]:
    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            """INSERT INTO market.adj_factor (ts_code,trade_date,adj_factor) VALUES %s
               ON CONFLICT (ts_code,trade_date) DO UPDATE SET adj_factor=EXCLUDED.adj_factor
               WHERE market.adj_factor.adj_factor IS DISTINCT FROM EXCLUDED.adj_factor""",
            list(authority.adj_rows),
            page_size=5000,
        )
        adj_changed = int(cursor.rowcount)
    fact_changed = _seed_rights_authority(connection, authority)
    return adj_changed, fact_changed


def _assert_production_source(expected_commit: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", expected_commit) is None:
        raise RepairError("production apply requires a full merge commit")
    root = Path(
        subprocess.check_output(
            ["git", "-C", str(REPOSITORY_ROOT), "rev-parse", "--show-toplevel"],
            text=True,
            encoding="utf-8",
        ).strip()
    ).resolve()
    branch = subprocess.check_output(
        ["git", "-C", str(root), "branch", "--show-current"], text=True, encoding="utf-8"
    ).strip()
    head = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True, encoding="utf-8"
    ).strip()
    status = subprocess.check_output(
        ["git", "-C", str(root), "status", "--short"], text=True, encoding="utf-8"
    ).strip()
    if root != REPOSITORY_ROOT.resolve() or branch != "main" or head != expected_commit or status:
        raise RepairError("production apply source is not the clean canonical main at the merge commit")


def _receipt_path(path: Path) -> Path:
    if not path.is_absolute():
        raise RepairError("receipt path must be absolute")
    parent = path.parent.resolve(strict=True)
    if parent.is_relative_to(REPOSITORY_ROOT.resolve(strict=True)) or path.exists():
        raise RepairError("receipt must be create-exclusive and repo-external")
    return parent / path.name


def _write_receipt(path: Path, receipt: Mapping[str, Any]) -> None:
    payload = canonical_json_bytes(receipt) + b"\n"
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def apply_repair(
    *,
    target: str,
    config: Mapping[str, Any],
    authority: AuthorityBundle,
    expected_plan_digest: str,
    expected_target_identity: str,
    confirmation: str,
    receipt_path: Path,
    expected_source_commit: str | None,
) -> Mapping[str, Any]:
    if confirmation != CONFIRMATIONS[target]:
        raise RepairError("apply confirmation differs")
    if expected_target_identity != target_identity_sha256(config):
        raise RepairError("target identity authorization differs")
    if target == "production":
        _assert_production_source(str(expected_source_commit or ""))
    output = _receipt_path(receipt_path)
    connection = psycopg2.connect(**config)
    try:
        connection.set_session(readonly=False, autocommit=False)
        before = build_plan(connection, target=target, config=config, authority=authority)
        if before.plan_digest != expected_plan_digest:
            raise RepairError("database plan changed after authorization")
        adj_changed, fact_changed = _apply_rows(connection, authority)
        after = build_plan(connection, target=target, config=config, authority=authority)
        if after.adj_missing_rows or after.adj_mismatch_rows or after.rights_existing_rows != 3 or after.rights_mismatch_rows:
            raise RepairError("transactional post-write validation failed")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    with psycopg2.connect(**config) as readback_connection:
        readback_connection.set_session(readonly=True, autocommit=False)
        readback = build_plan(readback_connection, target=target, config=config, authority=authority)
        readback_connection.rollback()
    if readback != after:
        raise RepairError("independent post-commit readback differs")
    unsigned = {
        "schema_version": RECEIPT_SCHEMA,
        "target": target,
        "target_identity_sha256": expected_target_identity,
        "database_name": str(config["dbname"]),
        "source_commit": expected_source_commit,
        "adj_authority_canonical_sha256": authority.adj_canonical_sha256,
        "adj_authority_file_sha256": authority.adj_file_sha256,
        "rights_authority_canonical_sha256": authority.rights_canonical_sha256,
        "rights_authority_file_sha256": authority.rights_file_sha256,
        "before": asdict(before),
        "after": asdict(after),
        "adj_rows_changed": adj_changed,
        "rights_rows_changed": fact_changed,
        "committed": True,
        "independent_readback": True,
        "ddl_performed": False,
        "dataset_pointer_changed": False,
        "experiment_started": False,
        "runtime_action_performed": False,
        "completed_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
    }
    receipt = {**unsigned, "receipt_sha256": sha256_bytes(canonical_json_bytes(unsigned))}
    _write_receipt(output, receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    for mode in ("plan", "apply"):
        item = subparsers.add_parser(mode)
        item.add_argument("--target", choices=("dev", "production"), required=True)
        item.add_argument("--authority-root", required=True)
        item.add_argument("--env-file", required=True)
        if mode == "apply":
            item.add_argument("--confirm", required=True)
            item.add_argument("--plan-digest", required=True)
            item.add_argument("--target-identity-sha256", required=True)
            item.add_argument("--receipt-path", required=True)
            item.add_argument("--expected-source-commit")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    env_file = Path(args.env_file).expanduser().resolve(strict=True)
    authority = load_authorities(Path(args.authority_root))
    config = database_config(args.target, env_file)
    if args.mode == "plan":
        with psycopg2.connect(**config) as connection:
            connection.set_session(readonly=True, autocommit=False)
            plan = build_plan(connection, target=args.target, config=config, authority=authority)
            connection.rollback()
        print(json.dumps(asdict(plan), ensure_ascii=False, sort_keys=True))
        return 0
    receipt = apply_repair(
        target=args.target,
        config=config,
        authority=authority,
        expected_plan_digest=args.plan_digest,
        expected_target_identity=args.target_identity_sha256,
        confirmation=args.confirm,
        receipt_path=Path(args.receipt_path),
        expected_source_commit=args.expected_source_commit,
    )
    print(
        json.dumps(
            {
                "status": "PASS",
                "target": args.target,
                "receipt_sha256": receipt["receipt_sha256"],
                "adj_rows_changed": receipt["adj_rows_changed"],
                "rights_rows_changed": receipt["rights_rows_changed"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
