"""Freeze/verify independent PIT versions; rehearse singleton activation in DEV.

No production DDL/DML or data-profile activation is implemented by this CLI.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.prepare_canonical_pit_monthly import _load_database_config
from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
from backend.services.canonical_pit_release import (
    activation_plan, capture_release, list_releases, publish_release, read_release, rehearse_activation,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=("list", "freeze", "verify", "rehearse-activation"), required=True)
    parser.add_argument("--target", choices=("dev", "production"))
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 8, 1))
    parser.add_argument("--cutoff", type=date.fromisoformat)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--release", type=Path)
    parser.add_argument("--predecessor", type=Path)
    args = parser.parse_args()
    if args.action == "list":
        if not args.directory:
            parser.error("list requires --directory")
        print(json.dumps({"versions": list_releases(args.directory), "database_read": False,
                          "database_write": False, "runtime_action": False}, sort_keys=True))
        return 0
    if args.target is None or args.env_file is None:
        parser.error("database actions require explicit --target and --env-file")
    if args.action == "rehearse-activation" and args.target != "dev":
        parser.error("activation rehearsal is restricted to the existing DEV database")
    if args.action == "freeze" and (not args.directory or not args.cutoff):
        parser.error("freeze requires --directory and --cutoff")
    if args.action != "freeze" and not args.release:
        parser.error("verify/rehearsal requires --release")
    if args.directory and args.directory.absolute().drive.upper() != "X:":
        parser.error("PIT output must be an existing repo-external X-drive directory")
    config = _load_database_config(args.target, args.env_file)
    if args.target == "dev" and (config.dbname != "aistock_dev" or config.port != 5433
                                 or config.host not in {"localhost", "127.0.0.1"}):
        parser.error("DEV endpoint differs from the existing authorized database")
    import psycopg2
    from contextlib import contextmanager
    conn = psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname,
        user=config.user, password=config.password, application_name="independent-PIT-version-operator",
        options="-c statement_timeout=60000 -c lock_timeout=5000")
    conn.set_session(readonly=args.action != "rehearse-activation", isolation_level="REPEATABLE READ")
    try:
        if args.action == "freeze":
            predecessor = read_release(args.predecessor).digest if args.predecessor else None
            commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1], text=True).strip()
            release = capture_release(conn, start=args.start, cutoff=args.cutoff, source_commit=commit,
                                      predecessor_digest=predecessor)
            path = publish_release(release, args.directory)
        else:
            path = args.release
            release = read_release(path)
        if args.action == "rehearse-activation":
            result = rehearse_activation(conn, release)
        else:
            current = capture_release(conn, start=release.snapshot.scope_start, cutoff=release.snapshot.cutoff,
                source_commit=release.payload["source_commit"], predecessor_digest=release.payload["predecessor_digest"])
            if current.digest != release.digest:
                raise ValueError("independent PIT source readback differs")
            @contextmanager
            def factory():
                yield conn
            live = CanonicalPitAuthorityResolver(factory).resolve_live_binding()
            result = {"status": "VERIFIED", "target": args.target, "database_read": True,
                      "database_write": False, "runtime_action": False,
                      "global_default_key": live.universe_key, "global_default_generation": live.activation_generation,
                      "activation_plan": activation_plan(release, live) if live.activation_generation == 0 else None}
        result.update(release_path=str(path.absolute()), pit_release_digest=release.digest,
                      version_id=release.payload["version_id"], snapshot_digest=release.snapshot.spans_sha256,
                      symbol_count=release.snapshot.unique_instruments, span_count=len(release.snapshot.spans),
                      start=release.snapshot.scope_start.isoformat(), cutoff=release.snapshot.cutoff.isoformat(),
                      dataset_export_performed=False, active_profile_write=False)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    finally:
        conn.rollback()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
