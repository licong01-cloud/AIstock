"""Activate the approved global PIT default, independently of price releases.

Uses the existing singleton CAS, with real five-field PIT readback and a DEV
rollback rehearsal. It does not issue a dataset W8 attestation, migrate stored
profiles/leases, change a policy, rebuild facts, or control any service.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date
import json
from pathlib import Path
import re
import subprocess
import sys

import psycopg2
import psycopg2.extras as pgx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.canonical_equity_pit import (  # noqa: E402
    CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_PIT_RULE_VERSION, LEGACY_PIT_UNIVERSE_KEY, CanonicalPitAuthorityResolver,
    PitActivationTarget, PitAuthorityStatus, PitConsumerBinding, _fetch_activation_target_state,
    _require_canonical_ready_state, activate_canonical_pit_authority,
    canonical_rule_parameters_digest, legacy_rule_parameters_digest,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot  # noqa: E402
from scripts.prepare_canonical_pit_monthly import (  # noqa: E402
    _canonical, _digest, _load_database_config, _write_receipt,
)

SCHEMA = "canonical_pit_default_operator_receipt_v1"
CONTRACT = {"schema_version": SCHEMA, "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "rule_version": CANONICAL_PIT_RULE_VERSION,
            "rule_parameters_digest": canonical_rule_parameters_digest(),
            "dev_rehearsal_required": True, "existing_singleton_cas": True,
            "dataset_profile_activation": False, "historical_lease_migration": False}
CONTRACT_DIGEST = _digest(CONTRACT)


def seal(value):
    payload = {key: item for key, item in value.items() if key != "receipt_digest"}
    return {**_canonical(payload), "receipt_digest": _digest(payload)}


def require_mode(target, mode, *, authorization_ref, dev_receipt):
    if mode == "rehearse" and target != "dev":
        raise ValueError("rehearsal is restricted to existing DEV")
    if mode == "apply" and (target != "production" or not dev_receipt or
                            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/#-]{2,255}", authorization_ref or "")):
        raise ValueError("production apply requires explicit authorization and actual DEV receipt")


def validate_dev_receipt(value, *, cutoff, source_commit):
    expected = {"schema_version": SCHEMA, "status": "PASS", "mode": "rehearse",
                "database_target": "dev", "operator_contract_digest": CONTRACT_DIGEST,
                "source_commit": source_commit, "cutoff": cutoff.isoformat(),
                "rule_parameters_digest": canonical_rule_parameters_digest(),
                "transaction_dml_rolled_back": True, "pointer_restored": True}
    if (not isinstance(value, dict) or any(value.get(key) != item for key, item in expected.items())
            or value.get("pointer_restored") is not True
            or value.get("transaction_dml_rolled_back") is not True
            or value.get("receipt_digest") != seal(value)["receipt_digest"]
            or value.get("activated_binding", {}).get("authority_status") != "ACTIVE_CANONICAL"
            or value.get("activated_binding", {}).get("activation_generation") != 1):
        raise ValueError("DEV receipt is not a matching successful activation/rollback rehearsal")
    return value["receipt_digest"]


def activation_target(live: PitConsumerBinding, *, bundle_digest, source_commit, expected_generation, expected_key, authorization_ref):
    if (live.activation_generation != expected_generation or live.universe_key != expected_key
            or expected_generation != 0 or expected_key != LEGACY_PIT_UNIVERSE_KEY
            or live.authority_status is not PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION
            or live.authority_id != CANONICAL_PIT_AUTHORITY_ID
            or live.rule_version != LEGACY_PIT_RULE_VERSION
            or live.rule_parameters_digest != legacy_rule_parameters_digest()):
        raise ValueError("initial activation previous pointer differs from explicit CAS preconditions")
    envelope = {"operator_contract_digest": CONTRACT_DIGEST, "pit_bundle_digest": bundle_digest,
                "expected_generation": expected_generation, "expected_key": expected_key,
                "source_commit": source_commit, "authorization_ref": authorization_ref,
                "dataset_profile_activation": False}
    target = PitActivationTarget(CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION,
        CANONICAL_PIT_UNIVERSE_KEY, canonical_rule_parameters_digest(), bundle_digest,
        _digest(envelope), source_commit, expected_generation, expected_key)
    return target, envelope


def require_sql_audit(audit):
    if (type(audit.get("span_count")) is not int or audit["span_count"] < 1
            or any(type(audit.get(key)) is not int or audit[key] != 0
                   for key in ("invalid_count", "overlap_count", "wrong_rule_count"))):
        raise ValueError("independent SQL PIT audit failed")


def capture(conn, cutoff, source_commit, database_identity):
    # The SQL audit independently checks persisted rows, rather than trusting a
    # builder's summary. Official canonicalization also validates every span.
    probe = PitActivationTarget(CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION,
        CANONICAL_PIT_UNIVERSE_KEY, canonical_rule_parameters_digest(), "0" * 64,
        "0" * 64, source_commit, 0, LEGACY_PIT_UNIVERSE_KEY)
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        state = _fetch_activation_target_state(cur, target=probe, lock=False)
        if not state:
            raise ValueError("canonical PIT source state is missing")
        _require_canonical_ready_state(state, target=probe)
        cur.execute("""SELECT start_date,end_date FROM market.stock_universe_pit_state
            WHERE universe_key=%s""", (CANONICAL_PIT_UNIVERSE_KEY,))
        bounds = cur.fetchone()
        cur.execute("""SELECT ts_code,eligible_start,eligible_end,entry_reason,exit_reason
            FROM market.stock_universe_pit_spans WHERE universe_key=%s
            ORDER BY ts_code,eligible_start,eligible_end""", (CANONICAL_PIT_UNIVERSE_KEY,))
        rows = list(cur.fetchall())
        cur.execute("""WITH s AS (
            SELECT *, max(eligible_end) OVER (PARTITION BY ts_code ORDER BY eligible_start,eligible_end
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_end
            FROM market.stock_universe_pit_spans WHERE universe_key=%s)
            SELECT count(*)::int span_count,count(DISTINCT ts_code)::int symbol_count,
              count(*) FILTER (WHERE eligible_start IS NULL OR eligible_end IS NULL
                OR eligible_start>eligible_end)::int invalid_count,
              count(*) FILTER (WHERE eligible_start<=previous_end)::int overlap_count,
              count(*) FILTER (WHERE rule_version IS DISTINCT FROM %s)::int wrong_rule_count
            FROM s""", (CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION))
        audit = dict(cur.fetchone())
    require_sql_audit(audit)
    snapshot = freeze_pit_snapshot(rows, universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION, scope_start=date(2018, 8, 1), cutoff=cutoff,
        state_identity=_digest(state), source_fingerprint_sha256=state["source_fingerprint_sha256"],
        parameter_hash=canonical_rule_parameters_digest(), state_start=bounds["start_date"],
        state_end=bounds["end_date"], state_status=state["status"], state_dirty=state["dirty"])
    bundle = {"snapshot": snapshot.as_dict(), "database_identity_digest": database_identity,
              "source_commit": source_commit, "operator_contract_digest": CONTRACT_DIGEST}
    return bundle, audit


def execute(conn, *, mode, cutoff, source_commit, database_identity, expected_generation,
            expected_key, authorization_ref, dev_receipt_digest):
    @contextmanager
    def factory():
        yield conn  # One caller-owned transaction; the official CAS never commits partially.

    resolver = CanonicalPitAuthorityResolver(factory)
    before = resolver.resolve_live_binding()
    write_mode = mode in {"apply", "rehearse"}
    if write_mode:
        with conn.cursor() as cur:
            # Preserve the same lock ordering as the shared activation API.
            cur.execute("""SELECT authority_id FROM market.stock_universe_pit_authority_pointer
                WHERE authority_id=%s FOR UPDATE""", (CANONICAL_PIT_AUTHORITY_ID,))
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                        (f"stock_universe_pit:{CANONICAL_PIT_UNIVERSE_KEY}",))
    bundle, audit = capture(conn, cutoff, source_commit, database_identity)
    receipt = {"schema_version": SCHEMA, "operator_contract_digest": CONTRACT_DIGEST,
               "mode": mode, "cutoff": cutoff.isoformat(), "source_commit": source_commit,
               "rule_parameters_digest": canonical_rule_parameters_digest(),
               "database_identity_digest": database_identity, "before_binding": asdict(before),
               "pit_bundle": bundle, "pit_bundle_digest": _digest(bundle), "sql_audit": audit,
               "dev_receipt_digest": dev_receipt_digest, "authorization_ref": authorization_ref,
               "dataset_write": False, "active_profile_write": False,
               "runtime_action": False, "database_ddl": False}
    if mode == "verify":
        if before.authority_status is not PitAuthorityStatus.ACTIVE_CANONICAL:
            raise ValueError("global default is not ACTIVE_CANONICAL")
        receipt.update(status="PASS", activated_binding=asdict(before), database_write=False)
        return receipt
    target, envelope = activation_target(before, bundle_digest=_digest(bundle), source_commit=source_commit,
        expected_generation=expected_generation, expected_key=expected_key, authorization_ref=authorization_ref)
    resolver.require_activation_target(target)
    receipt["activation_envelope"] = envelope
    receipt["activation_envelope_digest"] = target.activation_envelope_digest
    receipt["independent_receipt_digest"] = _digest({"pit_bundle_digest": _digest(bundle), "sql_audit": audit})
    if mode == "plan":
        receipt.update(status="PASS", database_write=False)
        return receipt
    try:
        result = activate_canonical_pit_authority(target, operator_intent=authorization_ref or "DEV_ROLLBACK_REHEARSAL",
            independent_receipt_digest=receipt["independent_receipt_digest"], connection_factory=factory)
        if (result.activation_generation != expected_generation + 1
                or result.authority_status is not PitAuthorityStatus.ACTIVE_CANONICAL):
            raise ValueError("activation readback differs from expected generation/status")
        receipt["activated_binding"] = asdict(result)
        if mode == "rehearse":
            conn.rollback()
            restored = resolver.resolve_live_binding()
            if restored != before:
                raise ValueError("DEV pointer was not restored after rollback")
            receipt.update(transaction_dml_rolled_back=True, pointer_restored=True, database_write=False)
        else:
            conn.commit()
            receipt.update(database_write=True, transaction_dml_rolled_back=False)
        receipt["status"] = "PASS"
        return receipt
    except Exception:
        conn.rollback()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", choices=("dev", "production"), required=True)
    parser.add_argument("--mode", choices=("plan", "rehearse", "apply", "verify"), required=True)
    parser.add_argument("--cutoff", type=date.fromisoformat, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--receipt-path", type=Path, required=True)
    parser.add_argument("--expected-generation", type=int, default=0)
    parser.add_argument("--expected-key", default=LEGACY_PIT_UNIVERSE_KEY)
    parser.add_argument("--authorization-ref")
    parser.add_argument("--dev-receipt", type=Path)
    args = parser.parse_args(argv)
    require_mode(args.database, args.mode, authorization_ref=args.authorization_ref, dev_receipt=args.dev_receipt)
    destination = args.receipt_path.absolute()
    if destination.drive.upper() != "X:" or destination.exists() or not destination.parent.is_dir():
        raise ValueError("receipt must be a new file in an existing X-drive directory")
    for path in (destination, *destination.parents):
        if path.is_symlink() or path.is_junction():
            raise ValueError("operator receipt must not use a symlink/junction")
    source_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if args.mode == "apply":
        subprocess.run(["git", "merge-base", "--is-ancestor", source_commit, "origin/main"], cwd=ROOT, check=True)
        subprocess.run(["git", "diff", "--quiet", "HEAD", "--", str(Path(__file__).relative_to(ROOT)),
                        "backend/services/canonical_equity_pit.py", "backend/services/dataset_release/pit.py",
                        "scripts/prepare_canonical_pit_monthly.py"], cwd=ROOT, check=True)
    dev_digest = None
    if args.mode == "apply":
        dev_digest = validate_dev_receipt(json.loads(args.dev_receipt.read_bytes()), cutoff=args.cutoff,
                                          source_commit=source_commit)
    config = _load_database_config(args.database, args.env_file)
    expected_endpoint = ("aistock_dev", 5433) if args.database == "dev" else ("aistock", 5432)
    if config.host not in {"localhost", "127.0.0.1"} or (config.dbname, config.port) != expected_endpoint:
        raise ValueError("database differs from the existing authorized endpoint")
    conn = psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname,
        user=config.user, password=config.password, application_name="canonical-PIT-default-operator",
        options="-c statement_timeout=30000 -c lock_timeout=5000")
    conn.set_session(readonly=args.mode in {"plan", "verify"}, isolation_level="REPEATABLE READ")
    try:
        receipt = execute(conn, mode=args.mode, cutoff=args.cutoff, source_commit=source_commit,
            database_identity=config.identity_digest, expected_generation=args.expected_generation,
            expected_key=args.expected_key, authorization_ref=args.authorization_ref, dev_receipt_digest=dev_digest)
    finally:
        conn.rollback()
        conn.close()
    receipt = seal({**receipt, "database_target": args.database})
    _write_receipt(destination, receipt, allowed_root=destination.parent)
    print(json.dumps({key: value for key, value in receipt.items()
                      if key not in {"pit_bundle", "before_binding"}}, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
