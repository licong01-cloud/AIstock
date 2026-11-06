"""Offline immutable PIT versions, independent of quote dataset releases.

The database singleton remains the only live default. This module never
rebuilds PIT facts, changes policy, activates a pointer, or rewrites a release.
Only the explicit operator CLI imports it; server/worker paths do not.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping
from uuid import uuid4

import psycopg2.extras as pgx

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY,
    PitActivationTarget, _require_canonical_ready_state, canonical_rule_parameters_digest,
)
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.pit import (
    FrozenPitSnapshot, build_dataset_pit_binding, freeze_pit_snapshot,
    frozen_pit_snapshot_from_mapping, require_canonical_source_snapshot,
)

SCHEMA = "aistock_canonical_pit_release_v1"
_SHA = re.compile(r"^[0-9a-f]{64}$")
_FIELDS = {"schema_version", "version_id", "authority_id", "rule_parameters_digest",
           "snapshot", "trading_calendar", "calendar_sha256", "source_commit", "predecessor_digest"}


def _sha(value: str) -> str:
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise ValueError("PIT release requires lowercase SHA256")
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True)
class PitRelease:
    payload: Mapping[str, Any]
    digest: str
    snapshot: FrozenPitSnapshot


def build_release(snapshot: FrozenPitSnapshot, *, calendar, source_commit: str,
                  predecessor_digest: str | None = None) -> PitRelease:
    require_canonical_source_snapshot(snapshot)
    if snapshot.parameter_hash != canonical_rule_parameters_digest():
        raise ValueError("independent PIT snapshot rule parameters differ")
    days = [date.fromisoformat(str(value)).isoformat() for value in calendar]
    if (not days or days != sorted(set(days))
            or days[0] < snapshot.scope_start.isoformat() or days[-1] > snapshot.cutoff.isoformat()):
        raise ValueError("PIT release calendar coverage/order is invalid")
    if not isinstance(source_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("PIT release source commit must be a full Git identity")
    if predecessor_digest is not None:
        _sha(predecessor_digest)
    payload = {"schema_version": SCHEMA, "authority_id": CANONICAL_PIT_AUTHORITY_ID,
               "rule_parameters_digest": canonical_rule_parameters_digest(),
               "snapshot": snapshot.as_dict(), "trading_calendar": days,
               "calendar_sha256": _digest(days), "source_commit": source_commit,
               "predecessor_digest": predecessor_digest}
    payload["version_id"] = "pit_" + _digest(payload)
    return PitRelease(payload, _digest(payload), snapshot)


def validate_release(value: Mapping[str, Any]) -> PitRelease:
    if not isinstance(value, Mapping) or set(value) != _FIELDS or value["schema_version"] != SCHEMA:
        raise ValueError("PIT release schema/fields differ")
    snapshot = frozen_pit_snapshot_from_mapping(value["snapshot"])
    result = build_release(snapshot, calendar=value["trading_calendar"], source_commit=value["source_commit"],
                           predecessor_digest=value["predecessor_digest"])
    if result.payload != dict(value):
        raise ValueError("PIT release identity/digest differs")
    return result


def _plain_chain(path: Path) -> None:
    for item in (path, *path.parents):
        if item.is_symlink() or item.is_junction():
            raise ValueError("PIT version path must not use symlink/junction")


def publish_release(release: PitRelease, directory: Path) -> Path:
    release = validate_release(release.payload)
    directory = directory.absolute()
    _plain_chain(directory)
    if not directory.is_dir():
        raise ValueError("PIT version directory must already exist")
    predecessor = release.payload["predecessor_digest"]
    if predecessor is not None and predecessor not in {item["digest"] for item in list_releases(directory)}:
        raise ValueError("PIT predecessor is not registered in this version directory")
    path = directory / f"{release.payload['version_id']}.json"
    temporary = directory / f".pit-{uuid4().hex}.partial"
    try:
        with temporary.open("xb") as handle:
            handle.write(canonical_json_bytes(release.payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        # Atomic create-exclusive registration; never replace a published file.
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def _unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("PIT release has duplicate JSON fields")
        result[key] = value
    return result


def read_release(path: Path) -> PitRelease:
    _plain_chain(path)
    if not path.is_file():
        raise ValueError("PIT version must be an existing regular file")
    before = path.stat()
    value = json.loads(path.read_bytes(), object_pairs_hook=_unique_fields)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
        raise ValueError("PIT version changed during read")
    result = validate_release(value)
    if path.name != result.payload["version_id"] + ".json":
        raise ValueError("PIT version filename identity differs")
    return result


def list_releases(directory: Path) -> list[dict]:
    _plain_chain(directory)
    if not directory.is_dir():
        raise ValueError("PIT version directory must exist")
    result = []
    for path in sorted(directory.glob("pit_*.json")):
        release = read_release(path)
        result.append({"version_id": release.payload["version_id"], "digest": release.digest,
                       "predecessor_digest": release.payload["predecessor_digest"],
                       "rule_version": release.snapshot.rule_version,
                       "start": release.snapshot.scope_start.isoformat(), "cutoff": release.snapshot.cutoff.isoformat()})
    return result


def bind_fact_release(release: PitRelease, *, dataset_manifest_sha256: str) -> dict:
    """Pair existing identities; this does not certify physical quote coverage."""
    release = validate_release(release.payload)
    binding = build_dataset_pit_binding(
        release.snapshot, release_id=release.payload["version_id"],
        rolling_cutoff_spans_sha256=release.snapshot.spans_sha256,
    )
    return {"dataset_manifest_sha256": _sha(dataset_manifest_sha256),
            "pit_release_digest": release.digest, "pit_binding": binding.as_dict()}


def capture_release(conn, *, start: date, cutoff: date, source_commit: str,
                    predecessor_digest: str | None = None) -> PitRelease:
    """Read the existing canonical authority, never rebuild it or change it."""
    if conn.readonly is not True or conn.isolation_level != 2:
        raise ValueError("PIT capture requires a read-only REPEATABLE READ connection")
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute("""SELECT s.universe_key,s.rule_version,s.scope,s.status,s.dirty,
            s.start_date,s.end_date,s.source_fingerprint_sha256,s.last_build_summary,
            EXISTS(SELECT 1 FROM market.stock_universe_pit_spans x WHERE x.universe_key=s.universe_key
                AND x.rule_version=s.rule_version) AS has_current_rule_span,
            NOT EXISTS(SELECT 1 FROM market.stock_universe_pit_spans x WHERE x.universe_key=s.universe_key
                AND x.rule_version<>s.rule_version) AS all_spans_current_rule
            FROM market.stock_universe_pit_state s WHERE s.universe_key=%s""", (CANONICAL_PIT_UNIVERSE_KEY,))
        state = cur.fetchone()
        if state is None:
            raise ValueError("canonical PIT source state is missing")
        target = PitActivationTarget(
            CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY,
            canonical_rule_parameters_digest(), "0" * 64, "0" * 64, source_commit, 0, "read_only_capture",
        )
        _require_canonical_ready_state(state, target=target)
        cur.execute("""SELECT ts_code,eligible_start,eligible_end,entry_reason,exit_reason
            FROM market.stock_universe_pit_spans WHERE universe_key=%s AND rule_version=%s
            AND eligible_start<=%s AND eligible_end>=%s ORDER BY ts_code,eligible_start,eligible_end""",
                    (CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION, cutoff, start))
        rows = list(cur.fetchall())
        cur.execute("""SELECT cal_date::date AS trade_date FROM market.trading_calendar
            WHERE is_trading=true AND cal_date BETWEEN %s AND %s ORDER BY cal_date""", (start, cutoff))
        calendar = [row["trade_date"] for row in cur.fetchall()]
    state_identity = {key: value.isoformat() if isinstance(value, date) else value for key, value in state.items()}
    snapshot = freeze_pit_snapshot(
        rows, universe_key=state["universe_key"], rule_version=state["rule_version"],
        scope_start=start, cutoff=cutoff, state_identity=_digest(state_identity),
        source_fingerprint_sha256=_sha(state["source_fingerprint_sha256"]),
        parameter_hash=canonical_rule_parameters_digest(), state_status=state["status"],
        state_dirty=state["dirty"], state_start=state["start_date"], state_end=state["end_date"],
    )
    return build_release(snapshot, calendar=calendar, source_commit=source_commit,
                         predecessor_digest=predecessor_digest)
