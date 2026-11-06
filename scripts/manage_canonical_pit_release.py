"""Freeze/list/verify independent PIT files; no activation or database writes."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
from backend.services.canonical_pit_release import (
    capture_release, list_releases, publish_release, read_release,
)
from scripts.prepare_canonical_pit_monthly import _load_database_config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=("list", "freeze", "verify"), required=True)
    parser.add_argument("--target", choices=("dev", "production"))
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--cutoff", type=date.fromisoformat)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--release", type=Path)
    parser.add_argument("--predecessor", type=Path)
    args = parser.parse_args(argv)
    if args.action == "list":
        if not args.directory:
            parser.error("list requires --directory")
        print(json.dumps({"versions": list_releases(args.directory), "database_read": False,
                          "database_write": False, "runtime_action": False}, sort_keys=True))
        return 0
    if args.action == "verify":
        if not args.release:
            parser.error("verify requires --release")
        release = read_release(args.release)
        print(json.dumps({
            "status": "FILE_VERIFIED", "version_id": release.payload["version_id"],
            "pit_release_digest": release.digest,
            "file_sha256": hashlib.sha256(args.release.read_bytes()).hexdigest(),
            "database_read": False, "database_write": False, "runtime_action": False,
            "active_profile_write": False, "source_freshness_checked": False,
        }, sort_keys=True))
        return 0
    if args.target is None or args.env_file is None:
        parser.error("database actions require explicit --target and --env-file")
    if args.action == "freeze" and (not args.directory or not args.start or not args.cutoff):
        parser.error("freeze requires --directory, --start and --cutoff")
    if args.directory and args.directory.absolute().drive.upper() != "X:":
        parser.error("PIT output must be an existing repo-external X-drive directory")
    config = _load_database_config(args.target, args.env_file)
    expected_database = "aistock_dev" if args.target == "dev" else "aistock"
    if config.dbname != expected_database:
        parser.error("explicit database target differs from configured database")
    import psycopg2
    conn = psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname,
        user=config.user, password=config.password, application_name="independent-PIT-version-reader",
        connect_timeout=10, options="-c statement_timeout=60000 -c lock_timeout=5000")
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            if cursor.fetchone()[0] != expected_database:
                raise ValueError("database target readback differs")
        predecessor = read_release(args.predecessor).digest if args.predecessor else None
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[1], text=True).strip()
        release = capture_release(conn, start=args.start, cutoff=args.cutoff, source_commit=commit,
                                  predecessor_digest=predecessor)
        current = capture_release(conn, start=release.snapshot.scope_start, cutoff=release.snapshot.cutoff,
            source_commit=release.payload["source_commit"], predecessor_digest=release.payload["predecessor_digest"])
        if current.digest != release.digest:
            raise ValueError("independent PIT source readback differs")
        @contextmanager
        def factory():
            yield conn
        live = CanonicalPitAuthorityResolver(factory).resolve_live_binding()
        path = publish_release(release, args.directory)
        print(json.dumps({
            "status": "CAPTURE_VERIFIED", "target": args.target, "database_read": True, "database_write": False,
            "runtime_action": False, "active_profile_write": False, "dataset_export_performed": False,
            "global_default_key": live.universe_key, "global_default_generation": live.activation_generation,
            "release_path": str(path.absolute()), "pit_release_digest": release.digest,
            "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "version_id": release.payload["version_id"], "snapshot_digest": release.snapshot.spans_sha256,
            "symbol_count": release.snapshot.unique_instruments, "span_count": len(release.snapshot.spans),
            "start": release.snapshot.scope_start.isoformat(), "cutoff": release.snapshot.cutoff.isoformat(),
        }, sort_keys=True))
    finally:
        conn.rollback()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
