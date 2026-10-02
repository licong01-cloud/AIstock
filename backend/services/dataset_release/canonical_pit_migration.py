"""Real PIT migration evidence and DEV-only, forward-only preparation.

No source fallback, candidate rebuild, training or production activation.
Historical Selection leases and per-date Paper activations stay immutable.
"""
from __future__ import annotations

from datetime import date
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import stat
from typing import Any, Iterable, Mapping

from .canonical import canonical_json_bytes
from .canonical_pit_candidate_bundle import validate_candidate_validation_bundle
from .canonical_pit_w8_attestation import bind_real_w8_attestation
from .canonical_pit_activation_envelope import build_activation_envelope, validate_activation_envelope


class PitMigrationError(ValueError):
    code = "CANONICAL_PIT_MIGRATION_BLOCKED"


def _file_stamp(info):
    # Reading immutable Windows files can update atime; this is not byte drift.
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            getattr(info, "st_file_attributes", 0))


def _plain_file(path: Path) -> Path:
    original = path.absolute()
    for item in (original, *original.parents):
        info = item.lstat()
        if item.is_symlink() or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise PitMigrationError("migration evidence must not use symlink/junction")
    if not original.is_file():
        raise PitMigrationError("migration evidence must be an existing ordinary file")
    return original


def read_sealed_json(path: Path, *, expected_digest: str | None = None) -> Any:
    path = _plain_file(path)
    before = path.stat()
    if before.st_size > 8 * 1024 * 1024:
        raise PitMigrationError("JSON evidence exceeds metadata size bound")
    raw = path.read_bytes()
    if _file_stamp(path.stat()) != _file_stamp(before) or len(raw) != before.st_size:
        raise PitMigrationError("JSON evidence changed during readback")
    if expected_digest is not None and hashlib.sha256(raw).hexdigest() != expected_digest:
        raise PitMigrationError("evidence byte digest differs")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise PitMigrationError("duplicate JSON evidence key")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: _invalid_constant())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PitMigrationError("invalid UTF-8 JSON evidence") from exc


def _invalid_constant():
    raise PitMigrationError("non-finite JSON evidence")


def audit_eligibility_intervals(frozen: Iterable, rolling: Iterable, *, start: date, cutoff: date) -> dict:
    """Independent eligibility projection, not a replacement for 5-field PIT digest."""
    if type(start) is not date or type(cutoff) is not date or start > cutoff:
        raise PitMigrationError("invalid migration coverage window")

    def intervals(rows):
        result = []
        for row in rows:
            if len(row) != 3 or not isinstance(row[0], str) or not re.fullmatch(r"\d{6}\.(SH|SZ)", row[0]):
                raise PitMigrationError("invalid canonical PIT interval")
            begin, end = date.fromisoformat(str(row[1])), date.fromisoformat(str(row[2]))
            if begin > end:
                raise PitMigrationError("inverted PIT interval")
            if end >= start and begin <= cutoff:
                result.append((row[0], max(begin, start).isoformat(), min(end, cutoff).isoformat()))
        result.sort()
        if len(result) != len(set(result)):
            raise PitMigrationError("duplicate PIT interval")
        previous = {}
        for code, begin, end in result:
            if code in previous and begin <= previous[code]:
                raise PitMigrationError("overlapping PIT intervals")
            previous[code] = end
        if not result:
            raise PitMigrationError("empty migration coverage denominator")
        return result

    lhs, rhs = intervals(frozen), intervals(rolling)
    only_lhs, only_rhs = sorted(set(lhs) - set(rhs)), sorted(set(rhs) - set(lhs))
    return {"status": "PASS" if not only_lhs and not only_rhs else "BLOCKED",
            "encoding": "canonical_pit_eligibility_intervals_v1",
            "start": start.isoformat(), "cutoff": cutoff.isoformat(),
            "frozen_span_count": len(lhs), "rolling_span_count": len(rhs),
            "frozen_projection_sha256": hashlib.sha256(canonical_json_bytes(lhs)).hexdigest(),
            "rolling_projection_sha256": hashlib.sha256(canonical_json_bytes(rhs)).hexdigest(),
            "frozen_only": [list(x) for x in only_lhs], "rolling_only": [list(x) for x in only_rhs]}


def build_real_candidate_validation_bundle(payload: Mapping[str, Any], *, evidence_files: Mapping[str, Path]):
    """Seal a real bundle only after reading every referenced immutable digest.

Evidence can use byte identities (binary assets) or canonical CAS identities.
Logical PIT rows are independently decoded and hashed below; fixture PASS is
never upgraded. This API does not create an independent W8 PASS.
"""
    bundle = validate_candidate_validation_bundle(payload, allow_real=True)
    value = bundle.as_dict()
    if value["candidate_identity"]["scope"] != "full":
        raise PitMigrationError("fixture cannot become a real migration asset")
    required = {value["artifact_root_digest"], value["resource_receipt_digest"],
                value["instrument_universe_digest"], value["historical_baseline_immutability_digest"],
                value["no_external_path_dependency_proof"]["proof_digest"],
                value["profile"]["profile_digest"], value["toolchain_sha"],
                value["source_runtime"]["consumer_inventory_digest"],
                *value["component_digests"].values(), *value["validation"].values(),
                value["frozen_release"]["manifest_digest"], value["frozen_release"]["calendar_digest"],
                value["frozen_release"]["signoff_receipt_digest"],
                value["pit_identity"]["frozen_snapshot_digest"], value["rolling_observation"]["digest"],
                value["rolling_observation"]["state_source_digest"]}
    if not required.issubset(evidence_files):
        raise PitMigrationError("real bundle is missing referenced immutable evidence")
    for digest in sorted(required):
        path = _plain_file(Path(evidence_files[digest]))
        before = path.stat()
        sha = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                sha.update(block)
        if _file_stamp(path.stat()) != _file_stamp(before):
            raise PitMigrationError("referenced immutable evidence changed during readback")
        if sha.hexdigest() != digest:
            # Content-addressed JSON ignores a terminal newline, never values.
            if hashlib.sha256(canonical_json_bytes(read_sealed_json(path))).hexdigest() != digest:
                raise PitMigrationError("real bundle referenced asset digest differs")
    rows = read_sealed_json(Path(evidence_files[value["pit_identity"]["frozen_snapshot_digest"]]))
    if not isinstance(rows, list) or len(rows) != value["rolling_observation"]["row_count"]:
        raise PitMigrationError("real bundle PIT row-count identity differs")
    if any(not isinstance(row, list) or len(row) != 5 for row in rows):
        raise PitMigrationError("real PIT snapshot requires the full five-field encoding")
    if (any(not isinstance(row[3], str) or not row[3] or (row[4] is not None and not isinstance(row[4], str)) for row in rows)
            or rows != sorted(rows, key=lambda row: (row[0], row[1], row[2]))):
        raise PitMigrationError("real PIT ordered span/reason encoding differs")
    audit_eligibility_intervals([row[:3] for row in rows], [row[:3] for row in rows],
        start=min(date.fromisoformat(row[1]) for row in rows), cutoff=date.fromisoformat(value["cutoff"]["effective"]))
    if (any(date.fromisoformat(row[2]) > date.fromisoformat(value["cutoff"]["effective"]) for row in rows)
            or hashlib.sha256(canonical_json_bytes(rows)).hexdigest() != value["pit_identity"]["frozen_snapshot_digest"]):
        raise PitMigrationError("real PIT logical digest/cutoff differs")
    # Require independent oracle's actual denominator/digest, not a generic ACK.
    oracle = read_sealed_json(Path(evidence_files[value["validation"]["independent_pit_receipt"]]))
    if (not isinstance(oracle, Mapping) or oracle.get("status") != "PASS" or oracle.get("frozen_snapshot_digest") != value["pit_identity"]["frozen_snapshot_digest"]
            or oracle.get("rolling_cutoff_spans_sha256") != value["rolling_observation"]["digest"]
            or oracle.get("row_count") != len(rows) or oracle.get("cutoff") != value["cutoff"]["effective"]):
        raise PitMigrationError("independent PIT oracle identity differs or is not PASS")
    return bundle


def seal_real_activation(bundle_payload: Mapping, w8_payload: Mapping, inputs: Mapping):
    """Build the W9 draft from real bundle/W8, never self-issue readiness.

The readbacks here close file identities, not owning distribution/drain
business acceptance. Only the W9 owner can sign and activate this draft.
"""
    evidence_files = inputs.get("evidence_files", {})
    bundle = build_real_candidate_validation_bundle(bundle_payload, evidence_files=evidence_files)
    w8 = bind_real_w8_attestation(bundle.as_dict(), w8_payload)
    kwargs = {key: value for key, value in inputs.items() if key != "evidence_files"}
    if kwargs.get("expected_source_commit") != bundle.payload["source_commit"]:
        raise PitMigrationError("activation source commit differs from real bundle")
    for key in ("inactive_distribution_readback", "node_readback", "session_drain_readiness", "rollback_target"):
        reference = kwargs.get(key) or {}
        digest = reference.get("digest")
        if digest not in evidence_files:
            raise PitMigrationError("activation readback evidence is missing")
        readback = read_sealed_json(Path(evidence_files[digest]))
        if (hashlib.sha256(canonical_json_bytes(readback)).hexdigest() != digest
                or reference.get("status") != "ready" or readback.get("status") != "ready"
                or readback.get("candidate_bundle_digest") != bundle.digest
                or readback.get("expected_pointer_generation") != kwargs.get("expected_pointer_generation")):
            raise PitMigrationError("activation readback is not ready or binds another target")
    envelope = build_activation_envelope(candidate_bundle_digest=bundle.digest, w8_receipt=w8.as_dict(), **kwargs)
    if envelope.payload["status"] != "w9_seal_required":
        raise PitMigrationError("activation envelope is not ready to seal")
    return validate_activation_envelope(envelope.as_dict())


def plan_forward_profiles(repository, records: Iterable[Mapping]) -> dict:
    from backend.services.paper_trading_v2.canonical_pit_control import (
        plan_paper_runtime_profile_migration, migrate_runtime_config_to_canonical_pointer,
    )
    from backend.services.paper_trading_v2.models import compute_runtime_config_sha256
    from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService

    service = PaperTradingV2PortfolioService(repository=repository)
    rows = []
    for record in records:
        version = repository.get_runtime_profile_version(record["current_version_id"])
        profile = repository.get_runtime_profile(record["profile_id"])
        if version.profile_id != record["profile_id"] or profile.portfolio_id != record["portfolio_id"]:
            raise PitMigrationError("profile/version/portfolio ownership differs")
        plan = plan_paper_runtime_profile_migration(version)
        if version.config_sha256 != plan["source_config_sha256"]:
            raise PitMigrationError("stored profile config hash differs from actual content")
        plan["owner_normalization_changed"] = False
        if plan["action"] == "CREATE_NEW_CANONICAL_VERSION":
            manifest = repository.get_portfolio(record["portfolio_id"]).frozen_manifest
            normalized_source = service._normalize_runtime_profile_config(version.config_json, manifest=manifest)
            normalized_target = service._normalize_runtime_profile_config(plan["target_config_json"], manifest=manifest)
            if canonical_json_bytes(normalized_target) != canonical_json_bytes(
                migrate_runtime_config_to_canonical_pointer(normalized_source)
            ):
                raise PitMigrationError("owning normalization changes non-PIT business semantics")
            target_sha = compute_runtime_config_sha256(normalized_target)
            plan["owner_normalization_changed"] = target_sha != plan["target_config_sha256"]
            plan["target_config_sha256"] = target_sha
        rows.append({**dict(record), **{key: value for key, value in plan.items() if key != "target_config_json"}})
    rows.sort(key=lambda row: (row["portfolio_id"], row["profile_id"]))
    payload = {"schema_version": "local_data_dev_forward_pit_profile_plan_v1", "profiles": rows,
               "historical_activation_update": False, "historical_lease_update": False}
    return {**payload, "canonical_sha256": hashlib.sha256(canonical_json_bytes(payload)).hexdigest()}


def read_forward_profile_records(conn, *, lock: bool = False) -> list:
    from psycopg2.extras import RealDictCursor

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT r.profile_id,r.current_version_id,p.portfolio_id,p.status AS portfolio_status "
                       "FROM paper_v2.runtime_profile r JOIN paper_v2.portfolio p ON p.portfolio_id=r.portfolio_id "
                       "WHERE r.status='ACTIVE' AND p.status='READY' ORDER BY p.portfolio_id,r.profile_id"
                       + (" FOR UPDATE OF r,p" if lock else ""))
        return [dict(row) for row in cursor.fetchall()]


def _history_fingerprints(conn, source_version_ids: list[str]) -> dict:
    queries = {
        "selection_native_configs": ("SELECT run_id,runtime_config FROM selection.run ORDER BY run_id", ()),
        "historical_config_activations": ("SELECT activation_id,row_to_json(a) FROM paper_v2.runtime_config_activation a ORDER BY activation_id", ()),
        "original_profile_versions": ("SELECT profile_version_id,row_to_json(v) FROM paper_v2.runtime_profile_version v WHERE profile_version_id=ANY(%s) ORDER BY profile_version_id", (source_version_ids,)),
    }
    result = {}
    with conn.cursor() as cursor:
        for key, (sql, params) in queries.items():
            cursor.execute(sql, params)
            rows = json.loads(json.dumps(cursor.fetchall(), default=str))
            result[key] = {"row_count": len(rows), "sha256": hashlib.sha256(canonical_json_bytes(rows)).hexdigest()}
    return result


def prepare_dev_forward_profiles(conn, *, expected_plan_digest: str) -> dict:
    """One atomic DEV transaction using owning versioned migration APIs.

The caller owns commit/rollback. This function never updates past activations,
Selection run receipts, the PIT singleton, the global profile or services.
"""
    from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
    from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService

    if conn.info.dbname != "aistock_dev" or conn.info.port != 5433 or conn.info.host not in {"127.0.0.1", "localhost"}:
        raise PitMigrationError("forward preparation only authorizes the existing local DEV endpoint")
    with conn.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        if cursor.fetchone()[0] != "aistock_dev":
            raise PitMigrationError("actual DEV database identity differs")
        cursor.execute("SELECT count(*) FROM paper_v2.trade_session WHERE status NOT IN ('SUCCEEDED','FAILED','STOPPED')")
        if cursor.fetchone()[0] != 0:
            raise PitMigrationError("DEV side-effecting sessions must drain before preparation")
        cursor.execute("SELECT count(*) FROM paper_v2.run WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
        if cursor.fetchone()[0] != 0:
            raise PitMigrationError("DEV side-effecting runs must drain before preparation")

    @contextmanager
    def factory():
        yield conn

    repository = PaperTradingV2Repository(conn_factory=factory)
    service = PaperTradingV2PortfolioService(repository=repository)
    records = read_forward_profile_records(conn, lock=True)
    plan = plan_forward_profiles(repository, records)
    if plan["canonical_sha256"] != expected_plan_digest:
        raise PitMigrationError("forward profile plan CAS digest differs; rollback required")
    original_ids = [row["current_version_id"] for row in records]
    before = _history_fingerprints(conn, original_ids)
    applied = []
    for item in plan["profiles"]:
        if item["action"] == "NO_OP_ALREADY_CANONICAL":
            continue
        version = service.migrate_runtime_profile_version_to_canonical_pit(
            portfolio_id=item["portfolio_id"], profile_version_id=item["current_version_id"],
            created_by="local_data/BUG-1685", reason="DEV forward-only canonical PIT preparation BUG-1685")
        if version.config_sha256 != item["target_config_sha256"]:
            raise PitMigrationError("owning API migration readback differs from dry-run")
        current = repository.get_runtime_profile(item["profile_id"])
        if current.current_version_id != version.profile_version_id:
            raise PitMigrationError("new profile current-version readback differs")
        applied.append({"portfolio_id": item["portfolio_id"], "profile_id": item["profile_id"],
                        "source_version_id": item["current_version_id"], "target_version_id": version.profile_version_id,
                        "source_sha256": item["source_config_sha256"], "target_sha256": version.config_sha256})
    after = _history_fingerprints(conn, original_ids)
    if before != after:
        raise PitMigrationError("historical identity changed; DEV transaction must rollback")
    return {"schema_version": "local_data_dev_forward_pit_profile_apply_v1",
            "target": "aistock_dev:5433", "plan_digest": expected_plan_digest,
            "new_version_count": len(applied), "applied": applied,
            "history_before": before, "history_after": after,
            "authority_cas_performed": False, "production_write": False, "runtime_action": False}
