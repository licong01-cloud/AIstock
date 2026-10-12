"""Validate and import version-qualified SW recovery facts into EXISTING DEV.

Production receives the identical bounded SQL only after separate target-specific
authorization. This command cannot connect to or write a production database.
It never changes the compatibility tables, frozen candidates or active profile.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.sw_industry_history_store import canonical_bytes, digest  # noqa: E402

TABLES = ("sw_industry_source_fact_v1", "sw_industry_taxonomy_history_v1", "sw_industry_classification_history_v1")


def validate_bundle(bundle):
    if bundle.get("schema_version") != "sw_industry_recovery_bundle_v1":
        raise ValueError("Unsupported SW recovery bundle")
    if bundle.get("known_from_inferred") is not False:
        raise ValueError("Recovery must not infer historical publication time")
    sources = bundle["source_facts"]
    catalog = bundle["taxonomy_catalog"]
    intervals = bundle["effective_intervals"]
    if not sources or not catalog or not intervals:
        raise ValueError("Empty SW recovery component")
    source_keys = {(r["source_sha256"], r["source_row_sha256"]) for r in sources}
    dictionary = {}
    for row in catalog:
        key = row["taxonomy_version"], row["taxonomy_code"]
        if key in dictionary and dictionary[key] != row:
            raise ValueError("Conflicting version-qualified dictionary")
        dictionary[key] = row
    spans = {}
    lifecycle = {r["ts_code"]: r for r in bundle.get("security_lifecycle", [])}
    for row in bundle.get("non_consumable_intervals", []):
        delisted = lifecycle.get(row["canonical_symbol"], {}).get("delist_date")
        if row["status"] != "TAXONOMY_IDENTITY_UNRESOLVED" or not delisted or row["effective_from"] <= delisted:
            raise ValueError("Unresolved interval overlaps a real listed lifecycle; cannot import as resolved")
        if (row["source_sha256"], row["source_row_sha256"]) not in source_keys:
            raise ValueError("Unresolved raw fact lacks source identity")
    for row in intervals:
        if (row["source_sha256"], row["source_row_sha256"]) not in source_keys:
            raise ValueError("Interval lacks original source row identity")
        if row.get("known_from") is not None or row.get("knowledge_time_quality") != "FIRST_PUBLICATION_UNVERIFIED":
            raise ValueError("Recovery historical knowledge time was invented")
        start, end = date.fromisoformat(row["effective_from"]), date.fromisoformat(row["effective_to"])
        if start > end:
            raise ValueError("Reversed effective interval")
        if row["taxonomy_version"] == "SW2014" and end >= date(2021, 7, 30):
            raise ValueError("SW2014 interval crosses the taxonomy boundary")
        if row["taxonomy_version"] == "SW2021" and start < date(2021, 7, 30):
            raise ValueError("SW2021 current taxonomy was backcast")
        if not re.fullmatch(r"\d{6}[.](SH|SZ)", row["canonical_symbol"]):
            raise ValueError("Canonical security identity is invalid")
        for level in (1, 2, 3):
            mapping = dictionary.get((row["taxonomy_version"], row[f"l{level}_code"]))
            if not mapping or mapping["level"] != level or mapping["name"] != row[f"l{level}_name"]:
                raise ValueError("Version-qualified hierarchy/name differs")
            if level > 1 and mapping["parent_code"] != row[f"l{level - 1}_code"]:
                raise ValueError("Taxonomy parent ownership differs")
            if level == 2 and mapping["published_index_code"] != row["published_l2_code"]:
                raise ValueError("Published L2 crosswalk differs")
        spans.setdefault(row["canonical_symbol"], []).append((start, end))
    for entries in spans.values():
        entries.sort()
        if any(a[1] >= b[0] for a, b in zip(entries, entries[1:])):
            raise ValueError("Overlapping classification intervals")
    return {
        "source_rows": len(sources),
        "taxonomy_rows": len(catalog),
        "interval_rows": len(intervals),
        "symbols": len(spans),
        "versions": dict(Counter(r["taxonomy_version"] for r in intervals)),
        "remaining_gap_days": sum(r["trading_days"] for r in bundle["retained_source_gaps"]),
        "post_delisting_unresolved_raw_intervals_preserved": len(bundle.get("non_consumable_intervals", [])),
        "known_from_inferred": False,
    }


def capture_dev_bundle(conn, schema):
    if not re.fullmatch(r"repair_sw_pit_bug[0-9]+_[0-9]{8}_v[0-9]+", schema):
        raise ValueError("DEV recovery schema is not an explicitly named repair schema")
    conn.autocommit = False
    try:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")

        def table(name):
            with conn.cursor() as cur:
                cur.execute(f"SELECT payload FROM {schema}.{name} ORDER BY row_id")
                return [r[0] for r in cur.fetchall()]

        raw = table("source_history")
        provider = [
            dict(r, source_version="tushare_dated_member_capture_20261012") for r in table("full_provider_members_v3")
        ]
        public = [
            dict(
                r,
                source_version=r["selected_interval"]["source_version"],
                source_sha256=r["source_file"]["file_sha256"],
                source_row_sha256=r["source_rows_sha256"],
            )
            for r in table("public_history_resolutions_v4")
        ]
        full = table("full_effective_intervals_v4")
        unresolved = [r for r in full if r["status"] == "TAXONOMY_IDENTITY_UNRESOLVED"]
        return {
            "schema_version": "sw_industry_recovery_bundle_v1",
            "known_from_inferred": False,
            "recovery_schema": schema,
            "source_facts": raw + provider + public + unresolved,
            "taxonomy_catalog": table("taxonomy_catalog"),
            "effective_intervals": [r for r in full if r["status"] != "TAXONOMY_IDENTITY_UNRESOLVED"],
            "non_consumable_intervals": unresolved,
            "security_lifecycle": table("full_lifecycle_v3"),
            "retained_source_gaps": table("full_gap_spans_v4"),
            "gap_disposition": "USER_ACCEPTED_INITIAL_LISTING_GAPS_NO_FABRICATED_FACTS_OR_PROVIDER_WAIVER",
        }
    finally:
        conn.rollback()
        conn.set_session(readonly=False, isolation_level="READ COMMITTED")
        conn.autocommit = True


def _literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def import_sql(bundle):
    summary = validate_bundle(bundle)
    identity = digest(bundle)
    sql = ["BEGIN;", "SET LOCAL lock_timeout='10s';", "SET LOCAL statement_timeout='120s';"]
    sql.append("SELECT pg_advisory_xact_lock(hashtext('sw_industry_recovery_v1'));")
    expected = {}
    values = []
    for row in bundle["source_facts"]:
        payload = canonical_bytes(row).decode("utf-8")
        key = digest(row)
        values.append(
            (
                key,
                "recovery:" + row["source_version"],
                row.get("taxonomy_version") or "SOURCE_VERSION_UNQUALIFIED",
                row["source_sha256"],
                payload,
            )
        )
    expected[TABLES[0]] = sorted(set(v[0] for v in values))
    records = [(TABLES[0], "fact_sha256,source_id,taxonomy_version,source_sha256,payload", values)]
    values = []
    for row in bundle["taxonomy_catalog"]:
        key = digest({"bundle_sha256": identity, "row": row})
        values.append(
            (
                key,
                identity,
                row["taxonomy_version"],
                row["taxonomy_code"],
                row["level"],
                row.get("parent_code"),
                row.get("published_index_code"),
                canonical_bytes(row).decode("utf-8"),
            )
        )
    expected[TABLES[1]] = sorted(set(v[0] for v in values))
    records.append(
        (
            TABLES[1],
            "fact_sha256,bundle_sha256,taxonomy_version,taxonomy_code,level,parent_code,published_index_code,payload",
            values,
        )
    )
    values = []
    for row in bundle["effective_intervals"]:
        key = digest({"bundle_sha256": identity, "row": row})
        values.append(
            (
                key,
                identity,
                row["taxonomy_version"],
                row["canonical_symbol"],
                row["published_l2_code"],
                row["effective_from"],
                row["effective_to"],
                row["source_sha256"],
                row["source_row_sha256"],
                canonical_bytes(row).decode("utf-8"),
            )
        )
    expected[TABLES[2]] = sorted(v[0] for v in values)
    records.append(
        (
            TABLES[2],
            "fact_sha256,bundle_sha256,taxonomy_version,ts_code,published_l2_code,effective_from,effective_to,source_sha256,source_row_sha256,payload",
            values,
        )
    )
    for table, columns, rows in records:
        for offset in range(0, len(rows), 1000):
            vals = ",\n".join("(" + ",".join(_literal(v) for v in row) + ")" for row in rows[offset : offset + 1000])
            sql.append(f"INSERT INTO market.{table} ({columns}) VALUES\n{vals}\nON CONFLICT (fact_sha256) DO NOTHING;")
    sql.append("COMMIT;")
    return "\n".join(sql) + "\n", identity, summary, expected


def connect_dev(env_path):
    env = dotenv_values(env_path)
    if env.get("TDX_DB_DEV_NAME") != "aistock_dev" or str(env.get("TDX_DB_DEV_PORT")) != "5433":
        raise ValueError("Refusing any target except existing aistock_dev:5433")
    conn = psycopg2.connect(
        **{
            field: env["TDX_DB_DEV_" + name]
            for field, name in (
                ("host", "HOST"),
                ("port", "PORT"),
                ("dbname", "NAME"),
                ("user", "USER"),
                ("password", "PASSWORD"),
            )
        },
        connect_timeout=10,
        application_name="BUG1847-sw-history-dev-import",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        if cur.fetchone()[0] != "aistock_dev":
            conn.close()
            raise ValueError("DEV target identity differs")
    conn.rollback()
    conn.autocommit = True
    return conn


def readback(conn, bundle, identity, expected):
    out = {}
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute(
                f"SELECT fact_sha256,payload FROM market.{table} WHERE fact_sha256=ANY(%s) ORDER BY fact_sha256",
                (expected[table],),
            )
            rows = cur.fetchall()
            if [row[0] for row in rows] != expected[table]:
                raise ValueError("Import readback count/identity differs: " + table)
            if table == TABLES[0]:
                wanted = {digest(r): r for r in bundle["source_facts"]}
            else:
                key = "taxonomy_catalog" if table == TABLES[1] else "effective_intervals"
                wanted = {digest({"bundle_sha256": identity, "row": r}): r for r in bundle[key]}
            if any(row[1] != wanted[row[0]] for row in rows):
                raise ValueError("Import readback payload differs: " + table)
            out[table] = {"rows": len(rows), "rowset_sha256": digest([r[1] for r in rows]), "exact_readback": True}
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bundle", type=Path)
    source.add_argument("--capture-dev-schema")
    parser.add_argument("--expected-bundle-sha256")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--apply-dev", action="store_true")
    parser.add_argument("--apply-migration-dev", action="store_true")
    args = parser.parse_args()
    import hashlib

    if args.output_dir.resolve().drive.upper() != "X:" or args.output_dir.exists():
        raise ValueError("Output requires a NEW X-drive directory")
    args.output_dir.mkdir(parents=True)
    if args.capture_dev_schema:
        if not args.env_file:
            raise ValueError("DEV capture requires an explicit credential file location")
        with connect_dev(args.env_file) as conn:
            bundle = capture_dev_bundle(conn, args.capture_dev_schema)
        raw = canonical_bytes(bundle) + b"\n"
        args.expected_bundle_sha256 = hashlib.sha256(raw).hexdigest()
        with (args.output_dir / "recovery_bundle.json").open("xb") as handle:
            handle.write(raw)
    else:
        raw = args.bundle.read_bytes()
        if hashlib.sha256(raw).hexdigest() != args.expected_bundle_sha256:
            raise ValueError("Recovery bundle file pin differs")
        bundle = json.loads(raw)
    sql, identity, summary, expected = import_sql(bundle)
    sql_path = args.output_dir / "import_sw_industry_history.sql"
    with sql_path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(sql)
    result = {
        "schema_version": "sw_industry_history_import_receipt_v1",
        "bundle_sha256": identity,
        "bundle_file_sha256": args.expected_bundle_sha256,
        "summary": summary,
        "dev_database_write": False,
        "production_database_write": False,
        "candidate_write": False,
        "profile_write": False,
        "runtime_action": False,
        "production_sql": str(sql_path),
        "production_sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
    }
    if args.apply_dev or args.apply_migration_dev:
        if not args.env_file:
            raise ValueError("DEV requires an explicit credential file location")
        conn = connect_dev(args.env_file)
        try:
            if args.apply_migration_dev:
                with conn.cursor() as cur:
                    cur.execute("BEGIN; SET LOCAL lock_timeout='10s'; SET LOCAL statement_timeout='120s';")
                    cur.execute(
                        (ROOT / "backend/db/migrations/add_sw_industry_history_preservation_v1.sql").read_text(
                            encoding="utf-8"
                        )
                    )
                    cur.execute("COMMIT;")
            if args.apply_dev:
                with conn.cursor() as cur:
                    cur.execute(sql)
                first = readback(conn, bundle, identity, expected)
                with conn.cursor() as cur:
                    cur.execute(sql)
                second = readback(conn, bundle, identity, expected)
                if first != second:
                    raise ValueError("Repeat import changed immutable facts")
                result.update({"readback": second, "repeat_import_unchanged": True})
            result["dev_database_write"] = True
        finally:
            conn.close()
    with (args.output_dir / "import_receipt.json").open("xb") as handle:
        handle.write(canonical_bytes(result) + b"\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
