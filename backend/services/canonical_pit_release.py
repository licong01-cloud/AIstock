"""Independent immutable PIT versions; no quote export or module-local defaults.

The existing database singleton remains the sole live default. These files
version its cutoff observations, not a second active pointer. Old task bindings
remain immutable; publishing a file never activates it.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping

import psycopg2.extras as pgx

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_PIT_RULE_VERSION, LEGACY_PIT_UNIVERSE_KEY, PitAuthorityStatus,
    CanonicalPitAuthorityResolver, PitActivationTarget, PitConsumerBinding, _require_canonical_ready_state,
    activate_canonical_pit_authority, canonical_rule_parameters_digest, legacy_rule_parameters_digest,
)
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.pit import (
    DatasetPitBinding, FrozenPitSnapshot, freeze_pit_snapshot,
    frozen_pit_snapshot_from_mapping, require_canonical_frozen_snapshot,
    require_canonical_source_snapshot,
)

SCHEMA = "aistock_canonical_pit_release_v1"
_SHA = re.compile(r"^[0-9a-f]{64}$")
_FIELDS = {"schema_version", "version_id", "authority_id", "rule_parameters_digest",
           "snapshot", "trading_calendar", "calendar_sha256", "source_commit", "predecessor_digest"}


def _sha(value):
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise ValueError("PIT release requires lowercase SHA256")
    return value


def _digest(value):
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
    if not days or days != sorted(set(days)) or days[0] < snapshot.scope_start.isoformat() or days[-1] > snapshot.cutoff.isoformat():
        raise ValueError("PIT release calendar coverage/order is invalid")
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("PIT release source commit must be a full Git identity")
    if predecessor_digest is not None:
        _sha(predecessor_digest)
    payload = {"schema_version": SCHEMA, "authority_id": CANONICAL_PIT_AUTHORITY_ID,
               "rule_parameters_digest": canonical_rule_parameters_digest(),
               "snapshot": snapshot.as_dict(), "trading_calendar": days,
               "calendar_sha256": _digest(days), "source_commit": source_commit,
               "predecessor_digest": predecessor_digest}
    # A stable content identity, independent of download time or price manifest.
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


def _plain_chain(path: Path):
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
    # No active.json, no overwrite, no mutation of any referenced price release.
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(release.payload) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    return path


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


def read_release(path: Path) -> PitRelease:
    _plain_chain(path)
    if not path.is_file():
        raise ValueError("PIT version must be an existing regular file")
    before = path.stat()
    value = json.loads(path.read_bytes())
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
        raise ValueError("PIT version changed during read")
    result = validate_release(value)
    if path.name != result.payload["version_id"] + ".json":
        raise ValueError("PIT version filename identity differs")
    return result


def bind_fact_release(release: PitRelease, *, dataset_manifest_sha256: str,
                      start: date, cutoff: date) -> dict:
    """Pair identities without rewriting facts or claiming their completeness.

    Required field/key coverage remains the existing consumer's responsibility.
    A different PIT cannot restore facts previously omitted from a data file.
    """
    release = validate_release(release.payload)
    if not release.snapshot.scope_start <= start <= cutoff <= release.snapshot.cutoff:
        raise ValueError("PIT version coverage does not include requested data window")
    frozen = require_canonical_frozen_snapshot(
        release.snapshot, release_id=release.payload["version_id"],
        rolling_cutoff_spans_sha256=release.snapshot.spans_sha256,
    )
    binding = DatasetPitBinding(
        authority_id=frozen.authority_id, authority_status=str(frozen.authority_status), scope="full",
        rolling_universe_key=CANONICAL_PIT_UNIVERSE_KEY, frozen_universe_key=frozen.universe_key,
        rule_version=frozen.rule_version, rule_parameters_digest=frozen.rule_parameters_digest,
        cutoff=frozen.cutoff, rolling_cutoff_spans_sha256=frozen.snapshot_digest,
        frozen_snapshot_digest=frozen.snapshot_digest, release_id=frozen.release_id,
    )
    return {"dataset_manifest_sha256": _sha(dataset_manifest_sha256),
            "pit_release_digest": release.digest, "pit_binding": binding.as_dict()}


def capture_release(conn, *, start: date, cutoff: date, source_commit: str,
                    predecessor_digest: str | None = None) -> PitRelease:
    """Read only, in the caller's repeatable-read transaction; never rebuild."""
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
    release = build_release(snapshot, calendar=calendar, source_commit=source_commit,
                            predecessor_digest=predecessor_digest)
    # Reuse the same readiness rules as the production singleton CAS.
    target = PitActivationTarget(CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION,
        CANONICAL_PIT_UNIVERSE_KEY, canonical_rule_parameters_digest(), release.digest,
        release.digest, source_commit, 0, "shsz_st_pit_active_v1")
    _require_canonical_ready_state(state, target=target)
    return release


def activation_plan(release: PitRelease, live: PitConsumerBinding) -> dict:
    """Target-specific intent for existing CAS; does not perform any writes."""
    release = validate_release(release.payload)
    if (live.activation_generation != 0 or live.universe_key != LEGACY_PIT_UNIVERSE_KEY
            or live.authority_id != CANONICAL_PIT_AUTHORITY_ID
            or live.authority_status is not PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION
            or live.rule_version != LEGACY_PIT_RULE_VERSION
            or live.rule_parameters_digest != legacy_rule_parameters_digest()):
        raise ValueError("initial canonical migration requires the legacy generation-zero pointer")
    payload = {"schema_version": "canonical_pit_independent_activation_plan_v1",
               "pit_release_digest": release.digest,
               "expected_pointer_generation": live.activation_generation,
               "expected_pointer_key": live.universe_key,
               "expected_source_commit": release.payload["source_commit"],
               "rule_parameters_digest": release.payload["rule_parameters_digest"],
               "database_activation_performed": False, "dataset_activation_performed": False}
    return {**payload, "plan_digest": _digest(payload)}


def rehearse_activation(conn, release: PitRelease) -> dict:
    """Exercise the existing singleton CAS in DEV, always roll back the DML.

    The operator must select the existing DEV endpoint before calling. No
    price profile or historical runtime lease is changed by this rehearsal.
    """
    @contextmanager
    def factory():
        yield conn

    resolver = CanonicalPitAuthorityResolver(factory)
    before = resolver.resolve_live_binding()
    try:
        current = capture_release(conn, start=release.snapshot.scope_start, cutoff=release.snapshot.cutoff,
                                  source_commit=release.payload["source_commit"],
                                  predecessor_digest=release.payload["predecessor_digest"])
        if current.digest != release.digest:
            raise ValueError("PIT source drifted since independent version capture")
        plan = activation_plan(release, before)
        target = PitActivationTarget(CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION,
            CANONICAL_PIT_UNIVERSE_KEY, canonical_rule_parameters_digest(), release.digest,
            plan["plan_digest"], release.payload["source_commit"], before.activation_generation, before.universe_key)
        result = activate_canonical_pit_authority(target, operator_intent="DEV_ROLLBACK_REHEARSAL",
            independent_receipt_digest=_digest({"source_readback": current.digest, "plan": plan["plan_digest"]}),
            connection_factory=factory)
        if result.activation_generation != before.activation_generation + 1 or result.universe_key != CANONICAL_PIT_UNIVERSE_KEY:
            raise ValueError("PIT singleton activation readback differs")
        receipt = {"status": "DEV_ROLLBACK_VALIDATED", "plan_digest": plan["plan_digest"],
                   "pit_release_digest": release.digest, "validated_generation": result.activation_generation,
                   "database_write": False, "transaction_dml_rolled_back": True,
                   "dataset_export_performed": False, "active_profile_write": False, "runtime_action": False}
    finally:
        conn.rollback()
    restored = resolver.resolve_live_binding()
    if restored != before:
        raise ValueError("DEV rollback did not restore the original PIT pointer")
    return receipt
