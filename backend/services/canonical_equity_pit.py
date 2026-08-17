"""Single-authority contract for AIstock equity PIT consumers.

Rolling views and immutable dataset snapshots may have different physical
identities, but they are valid only when they share this rule authority.
Legacy universes are accepted solely for explicit immutable reproduction.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Callable, Mapping

import psycopg2.extras as pgx

from ..db.pg_pool import get_conn


CANONICAL_PIT_AUTHORITY_ID = "aistock_equity_pit_canonical"
CANONICAL_PIT_UNIVERSE_KEY = "aistock_equity_pit_canonical_v2"
CANONICAL_PIT_RULE_VERSION = "shsz_a_252td_st_delist_asof_v2"
CANONICAL_PIT_SCOPE = "canonical_all_listed"
CANONICAL_PIT_IPO_TRADING_SESSIONS = 252
CANONICAL_PIT_SNAPSHOT_PREFIX = "aistock_equity_pit_snapshot_"
CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT = "issuer_bound_stock_delisting_v2"

LEGACY_NONCANONICAL_UNIVERSE_KEYS = frozenset({"shsz_st_pit_active_v1"})
LEGACY_NONCANONICAL_SNAPSHOT_PREFIXES = ("shsz_st_pit_qe_dataset_",)
LEGACY_PIT_UNIVERSE_KEY = "shsz_st_pit_active_v1"
LEGACY_PIT_RULE_VERSION = "st_pub_next_trade_restore_active_l_v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CanonicalPitContractError(ValueError):
    """Raised when a consumer attempts to use a non-authoritative PIT."""


def _transactional_connection() -> Any:
    return get_conn(autocommit=False, manage_transaction=True)


class PitAuthorityStatus(str, Enum):
    ACTIVE_CANONICAL = "ACTIVE_CANONICAL"
    DEPLOYED_LEGACY_PENDING_MIGRATION = "DEPLOYED_LEGACY_PENDING_MIGRATION"
    SESSION_PINNED_DRAINING = "SESSION_PINNED_DRAINING"
    ARCHIVED_NONCANONICAL = "ARCHIVED_NONCANONICAL"
    EMERGENCY_LEGACY_ROLLBACK = "EMERGENCY_LEGACY_ROLLBACK"

    def __str__(self) -> str:
        return self.value


CANONICAL_RULE_PARAMETERS: Mapping[str, Any] = {
    "authority_id": CANONICAL_PIT_AUTHORITY_ID,
    "scope": CANONICAL_PIT_SCOPE,
    "exchanges": ["SSE", "SZSE"],
    "security_type": "A_SHARE_EQUITY",
    "excluded_boards": ["B_SHARE", "BSE"],
    "ipo_warmup": {"unit": "exchange_trading_session", "count": CANONICAL_PIT_IPO_TRADING_SESSIONS},
    "st_knowledge_policy": "announcement_asof_effective_date_confirmed_v2",
    "terminal_policy": "announcement_asof_then_delist_date_v2",
    "data_availability_changes_membership": False,
    "execution_tradability_changes_membership": False,
}

LEGACY_RULE_PARAMETERS: Mapping[str, Any] = {
    "authority_id": CANONICAL_PIT_AUTHORITY_ID,
    "scope": "st_only_active",
    "exchanges": ["SSE", "SZSE"],
    "security_type": "A_SHARE_EQUITY",
    "excluded_boards": ["B_SHARE", "BSE"],
    "ipo_warmup": {"unit": "calendar_day", "count": 365},
    "st_knowledge_policy": "publication_next_trade_restore_effective_v1",
    "terminal_policy": "generation_end_active_stock_filter_v1",
}


def canonical_rule_parameters_digest() -> str:
    payload = json.dumps(
        CANONICAL_RULE_PARAMETERS,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def legacy_rule_parameters_digest() -> str:
    payload = json.dumps(LEGACY_RULE_PARAMETERS, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class PitConsumerBinding:
    authority_id: str
    authority_status: PitAuthorityStatus
    universe_key: str
    rule_version: str
    rule_parameters_digest: str
    snapshot_digest: str | None = None
    cutoff: date | None = None
    release_id: str | None = None
    reproduction_mode: bool = False
    activation_generation: int | None = None
    activation_envelope_digest: str | None = None
    expected_source_commit: str | None = None
    state_source_digest: str | None = None
    coverage_start: date | None = None
    coverage_end: date | None = None


@dataclass(frozen=True, slots=True)
class PitActivationTarget:
    authority_id: str
    target_rule_version: str
    target_rolling_key: str
    rule_parameters_digest: str
    candidate_bundle_digest: str
    activation_envelope_digest: str
    expected_source_commit: str
    expected_previous_generation: int
    expected_previous_key: str


class CanonicalPitAuthorityResolver:
    """Resolve the singleton live PIT authority and fail closed on drift."""

    def __init__(self, connection_factory: Callable[[], Any] = get_conn) -> None:
        self._connection_factory = connection_factory

    def resolve_live_binding(self) -> PitConsumerBinding:
        with self._connection_factory() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT p.authority_id, p.current_rule_version AS rule_version,
                           p.current_rolling_key AS universe_key, p.activation_generation,
                           p.activation_envelope_digest, p.expected_source_commit,
                           v.rule_parameters_digest, v.status AS authority_status,
                           s.status AS state_status, s.dirty, s.rule_version AS state_rule_version,
                           s.scope, s.start_date, s.end_date,
                           s.source_fingerprint_sha256 AS state_source_digest,
                           s.last_build_summary,
                           EXISTS (
                               SELECT 1 FROM market.stock_universe_pit_spans x
                                WHERE x.universe_key = p.current_rolling_key
                                  AND x.rule_version = p.current_rule_version
                           ) AS has_current_rule_span,
                           CASE WHEN v.status = 'ACTIVE_CANONICAL' THEN NOT EXISTS (
                               SELECT 1 FROM market.stock_universe_pit_spans x
                                WHERE x.universe_key = p.current_rolling_key
                                  AND x.rule_version <> p.current_rule_version
                           ) ELSE TRUE END AS all_spans_current_rule
                      FROM market.stock_universe_pit_authority_pointer p
                      JOIN market.stock_universe_pit_authority_versions v
                        ON v.authority_id = p.authority_id
                       AND v.rule_version = p.current_rule_version
                       AND v.rolling_key = p.current_rolling_key
                      LEFT JOIN market.stock_universe_pit_state s
                        ON s.universe_key = p.current_rolling_key
                     WHERE p.authority_id = %s
                    """,
                    (CANONICAL_PIT_AUTHORITY_ID,),
                )
                rows = list(cur.fetchall())
        if len(rows) != 1:
            raise CanonicalPitContractError(f"authority pointer must resolve exactly one row; received {len(rows)}")
        return self._binding_from_row(dict(rows[0]))

    def require_activation_target(self, target: PitActivationTarget) -> PitActivationTarget:
        _validate_activation_target_shape(target)
        live = self.resolve_live_binding()
        if live.activation_generation != target.expected_previous_generation:
            raise CanonicalPitContractError("activation generation CAS precondition failed")
        if live.universe_key != target.expected_previous_key:
            raise CanonicalPitContractError("activation key CAS precondition failed")
        with self._connection_factory() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                row = _fetch_activation_target_state(cur, target=target, lock=False)
        if not row:
            raise CanonicalPitContractError("activation target state is missing")
        _require_canonical_ready_state(dict(row), target=target)
        return target

    @staticmethod
    def _binding_from_row(row: Mapping[str, Any]) -> PitConsumerBinding:
        try:
            status = PitAuthorityStatus(str(row.get("authority_status")))
        except ValueError as exc:
            raise CanonicalPitContractError(f"unknown authority status: {row.get('authority_status')!r}") from exc
        is_legacy = status in {
            PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION,
            PitAuthorityStatus.EMERGENCY_LEGACY_ROLLBACK,
        }
        if status is not PitAuthorityStatus.ACTIVE_CANONICAL and not is_legacy:
            raise CanonicalPitContractError(f"pointer references non-live authority status: {status.value}")
        if row.get("state_status") != "ready" or bool(row.get("dirty")):
            raise CanonicalPitContractError("pointer target PIT state must be ready and clean")
        if str(row.get("state_rule_version")) != str(row.get("rule_version")):
            raise CanonicalPitContractError("pointer version and PIT state rule_version differ")
        if not bool(row.get("has_current_rule_span")):
            raise CanonicalPitContractError("pointer target PIT spans are missing for the current rule")
        coverage_start = row.get("start_date")
        coverage_end = row.get("end_date")
        if not isinstance(coverage_start, date) or not isinstance(coverage_end, date) or coverage_start > coverage_end:
            raise CanonicalPitContractError("pointer target PIT state coverage is invalid")
        if is_legacy:
            if row.get("universe_key") != LEGACY_PIT_UNIVERSE_KEY or row.get("scope") != "st_only_active":
                raise CanonicalPitContractError("legacy migration pointer identity drift")
            if row.get("rule_version") != LEGACY_PIT_RULE_VERSION or row.get(
                "rule_parameters_digest"
            ) != legacy_rule_parameters_digest():
                raise CanonicalPitContractError("legacy migration rule identity drift")
        else:
            if not bool(row.get("all_spans_current_rule")):
                raise CanonicalPitContractError("active canonical PIT spans contain a non-current rule")
            target = PitActivationTarget(
                authority_id=str(row.get("authority_id")),
                target_rule_version=str(row.get("rule_version")),
                target_rolling_key=str(row.get("universe_key")),
                rule_parameters_digest=str(row.get("rule_parameters_digest")),
                candidate_bundle_digest="0" * 64,
                activation_envelope_digest=str(row.get("activation_envelope_digest") or ""),
                expected_source_commit=str(row.get("expected_source_commit") or ""),
                expected_previous_generation=max(int(row.get("activation_generation") or 0) - 1, 0),
                expected_previous_key=LEGACY_PIT_UNIVERSE_KEY,
            )
            _validate_activation_target_shape(target)
            active_state = dict(row)
            active_state["status"] = row.get("state_status")
            _require_canonical_ready_state(active_state, target=target)
        return PitConsumerBinding(
            authority_id=str(row["authority_id"]),
            authority_status=status,
            universe_key=str(row["universe_key"]),
            rule_version=str(row["rule_version"]),
            rule_parameters_digest=str(row["rule_parameters_digest"]),
            activation_generation=int(row["activation_generation"]),
            activation_envelope_digest=row.get("activation_envelope_digest"),
            expected_source_commit=row.get("expected_source_commit"),
            state_source_digest=row.get("state_source_digest"),
            coverage_start=coverage_start,
            coverage_end=coverage_end,
        )


def _validate_activation_target_shape(target: PitActivationTarget) -> None:
    expected = {
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "target_rule_version": CANONICAL_PIT_RULE_VERSION,
        "target_rolling_key": CANONICAL_PIT_UNIVERSE_KEY,
        "rule_parameters_digest": canonical_rule_parameters_digest(),
    }
    for field, value in expected.items():
        if getattr(target, field) != value:
            raise CanonicalPitContractError(f"activation target {field} differs from canonical contract")
    for field in ("candidate_bundle_digest", "activation_envelope_digest"):
        if not _SHA256_RE.fullmatch(str(getattr(target, field) or "")):
            raise CanonicalPitContractError(f"activation target {field} must be sha256")
    if not str(target.expected_source_commit or "").strip():
        raise CanonicalPitContractError("activation target expected_source_commit is required")
    if target.expected_previous_generation < 0 or not str(target.expected_previous_key or "").strip():
        raise CanonicalPitContractError("activation target CAS preconditions are invalid")


def _require_canonical_ready_state(row: Mapping[str, Any], *, target: PitActivationTarget) -> None:
    if row.get("universe_key") != target.target_rolling_key or row.get("rule_version") != target.target_rule_version:
        raise CanonicalPitContractError("activation target state identity drift")
    if row.get("scope") != CANONICAL_PIT_SCOPE or row.get("status") != "ready" or bool(row.get("dirty")):
        raise CanonicalPitContractError("activation target state is not canonical ready/clean")
    summary = row.get("last_build_summary") or {}
    if not isinstance(summary, Mapping):
        raise CanonicalPitContractError("activation target build summary is invalid")
    if not bool(row.get("has_current_rule_span")) or not bool(row.get("all_spans_current_rule")):
        raise CanonicalPitContractError("activation target PIT spans do not exclusively use the target rule")
    required = {
        "rule_parameters_digest": target.rule_parameters_digest,
        "exception_ledger_status": "ready",
    }
    for key, expected in required.items():
        if summary.get(key) != expected:
            raise CanonicalPitContractError(f"activation target build summary {key} is not {expected!r}")
    exception_ledger = summary.get("exception_ledger")
    if (
        not isinstance(exception_ledger, Mapping)
        or exception_ledger.get("status") != "ready"
        or not _has_exact_zero(exception_ledger, "unresolved_exception_count")
    ):
        raise CanonicalPitContractError("activation target exception ledger is incomplete")
    for key in ("st_snapshot_continuity", "terminal_evidence"):
        receipt = summary.get(key)
        if not isinstance(receipt, Mapping) or receipt.get("status") != "ready":
            raise CanonicalPitContractError(f"activation target {key} receipt is not ready")
    terminal_evidence = summary["terminal_evidence"]
    if not _has_exact_zero(terminal_evidence, "missing_terminal_evidence_count"):
        raise CanonicalPitContractError("activation target terminal evidence has unresolved securities")
    validation = summary.get("validation")
    if not isinstance(validation, Mapping):
        raise CanonicalPitContractError("activation target validation receipt is missing")
    failure_keys = (
        "invalid_span_count",
        "overlap_error_count",
        "event_action_violation_count",
        "terminal_reentry_violation_count",
    )
    if any(not _has_exact_zero(validation, key) for key in failure_keys):
        raise CanonicalPitContractError("activation target validation receipt contains failures")


def _has_exact_zero(receipt: Mapping[str, Any], key: str) -> bool:
    value = receipt.get(key)
    return type(value) is int and value == 0


def _fetch_activation_target_state(cur: Any, *, target: PitActivationTarget, lock: bool) -> Mapping[str, Any] | None:
    lock_clause = " FOR UPDATE OF s" if lock else ""
    cur.execute(
        f"""
        SELECT s.universe_key, s.rule_version, s.scope, s.status, s.dirty,
               s.source_fingerprint_sha256, s.last_build_summary,
               EXISTS (
                   SELECT 1 FROM market.stock_universe_pit_spans x
                    WHERE x.universe_key = s.universe_key
                      AND x.rule_version = s.rule_version
               ) AS has_current_rule_span,
               NOT EXISTS (
                   SELECT 1 FROM market.stock_universe_pit_spans x
                    WHERE x.universe_key = s.universe_key
                      AND x.rule_version <> s.rule_version
               ) AS all_spans_current_rule
          FROM market.stock_universe_pit_state s
         WHERE s.universe_key = %s{lock_clause}
        """,
        (target.target_rolling_key,),
    )
    return cur.fetchone()


def activate_canonical_pit_authority(
    target: PitActivationTarget,
    *,
    operator_intent: str,
    independent_receipt_digest: str,
    connection_factory: Callable[[], Any] = _transactional_connection,
) -> PitConsumerBinding:
    """Atomically activate v2 using the sealed W8/W9 CAS inputs.

    This is an operator-only source API. W1 adds it but never invokes it.
    """

    _validate_activation_target_shape(target)
    if not str(operator_intent or "").strip():
        raise CanonicalPitContractError("operator_intent is required")
    if not _SHA256_RE.fullmatch(str(independent_receipt_digest or "")):
        raise CanonicalPitContractError("independent_receipt_digest must be sha256")
    resolver = CanonicalPitAuthorityResolver(connection_factory)
    resolver.require_activation_target(target)
    with connection_factory() as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT current_rule_version, current_rolling_key, activation_generation
                  FROM market.stock_universe_pit_authority_pointer
                 WHERE authority_id = %s
                 FOR UPDATE
                """,
                (target.authority_id,),
            )
            before = cur.fetchone()
            if not before or int(before["activation_generation"]) != target.expected_previous_generation or before[
                "current_rolling_key"
            ] != target.expected_previous_key:
                raise CanonicalPitContractError("activation pointer changed after preflight")
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"stock_universe_pit:{target.target_rolling_key}",),
            )
            # Lock and revalidate the target state inside the same transaction
            # as the pointer CAS. A concurrent rebuild must not invalidate the
            # sealed activation evidence after the read-only preflight.
            locked_target = _fetch_activation_target_state(cur, target=target, lock=True)
            if not locked_target:
                raise CanonicalPitContractError("activation target state disappeared after preflight")
            _require_canonical_ready_state(dict(locked_target), target=target)
            cur.execute(
                """
                UPDATE market.stock_universe_pit_authority_versions
                   SET status = 'SESSION_PINNED_DRAINING', updated_at = NOW()
                 WHERE authority_id = %s AND rule_version = %s
                   AND status IN ('DEPLOYED_LEGACY_PENDING_MIGRATION', 'EMERGENCY_LEGACY_ROLLBACK')
                """,
                (target.authority_id, before["current_rule_version"]),
            )
            if cur.rowcount != 1:
                raise CanonicalPitContractError("activation must transition exactly one previous version")
            cur.execute(
                """
                INSERT INTO market.stock_universe_pit_authority_versions (
                    authority_id, rule_version, rolling_key, rule_parameters_digest, status,
                    first_candidate_bundle_digest, first_source_commit
                ) VALUES (%s, %s, %s, %s, 'ACTIVE_CANONICAL', %s, %s)
                ON CONFLICT (authority_id, rule_version) DO UPDATE
                    SET status = 'ACTIVE_CANONICAL', updated_at = NOW()
                  WHERE market.stock_universe_pit_authority_versions.rolling_key = EXCLUDED.rolling_key
                    AND market.stock_universe_pit_authority_versions.rule_parameters_digest = EXCLUDED.rule_parameters_digest
                    AND market.stock_universe_pit_authority_versions.first_candidate_bundle_digest = EXCLUDED.first_candidate_bundle_digest
                    AND market.stock_universe_pit_authority_versions.first_source_commit = EXCLUDED.first_source_commit
                    AND market.stock_universe_pit_authority_versions.status <> 'ACTIVE_CANONICAL'
                """,
                (
                    target.authority_id,
                    target.target_rule_version,
                    target.target_rolling_key,
                    target.rule_parameters_digest,
                    target.candidate_bundle_digest,
                    target.expected_source_commit,
                ),
            )
            if cur.rowcount != 1:
                raise CanonicalPitContractError("activation must register exactly one canonical version")
            cur.execute(
                """
                UPDATE market.stock_universe_pit_authority_pointer
                   SET current_rule_version = %s, current_rolling_key = %s,
                       activation_generation = activation_generation + 1,
                       activation_envelope_digest = %s, expected_source_commit = %s,
                       updated_at = NOW()
                 WHERE authority_id = %s AND activation_generation = %s AND current_rolling_key = %s
                """,
                (
                    target.target_rule_version,
                    target.target_rolling_key,
                    target.activation_envelope_digest,
                    target.expected_source_commit,
                    target.authority_id,
                    target.expected_previous_generation,
                    target.expected_previous_key,
                ),
            )
            if cur.rowcount != 1:
                raise CanonicalPitContractError("activation pointer CAS affected a non-unit row count")
            cur.execute(
                """
                INSERT INTO market.stock_universe_pit_authority_events (
                    authority_id, event_type, before_generation, after_generation,
                    before_rule_version, after_rule_version, before_rolling_key, after_rolling_key,
                    candidate_bundle_digest, activation_envelope_digest, independent_receipt_digest,
                    expected_source_commit, operator_intent
                ) VALUES (%s, 'ACTIVATE', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    target.authority_id,
                    target.expected_previous_generation,
                    target.expected_previous_generation + 1,
                    before["current_rule_version"],
                    target.target_rule_version,
                    target.expected_previous_key,
                    target.target_rolling_key,
                    target.candidate_bundle_digest,
                    target.activation_envelope_digest,
                    independent_receipt_digest,
                    target.expected_source_commit,
                    operator_intent.strip(),
                ),
            )
            if cur.rowcount != 1:
                raise CanonicalPitContractError("activation audit event affected a non-unit row count")
    return resolver.resolve_live_binding()


def require_canonical_consumer_binding(
    binding: PitConsumerBinding,
    *,
    consumer: str,
    immutable_snapshot_required: bool = False,
) -> PitConsumerBinding:
    """Validate a normal consumer or an explicitly isolated reproduction.

    Reproduction is deliberately not a second active universe.  It requires a
    frozen release and cannot be used by selection or trading consumers.
    """

    consumer_name = str(consumer or "").strip().lower()
    if not consumer_name:
        raise CanonicalPitContractError("consumer must be non-empty")

    if binding.reproduction_mode:
        if consumer_name != "reproduction":
            raise CanonicalPitContractError(
                "archived PIT is restricted to consumer='reproduction' and cannot drive training or trading"
            )
        if binding.authority_status is not PitAuthorityStatus.ARCHIVED_NONCANONICAL:
            raise CanonicalPitContractError("reproduction must declare ARCHIVED_NONCANONICAL")
        if (
            binding.universe_key not in LEGACY_NONCANONICAL_UNIVERSE_KEYS
            and not binding.universe_key.startswith(LEGACY_NONCANONICAL_SNAPSHOT_PREFIXES)
        ):
            raise CanonicalPitContractError("reproduction PIT identity is not an allowlisted archived universe")
        _require_frozen_release_identity(binding)
        return binding

    expected_digest = canonical_rule_parameters_digest()
    expected = {
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "authority_status": PitAuthorityStatus.ACTIVE_CANONICAL,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "rule_parameters_digest": expected_digest,
    }
    actual = {
        "authority_id": binding.authority_id,
        "authority_status": binding.authority_status,
        "rule_version": binding.rule_version,
        "rule_parameters_digest": binding.rule_parameters_digest,
    }
    mismatches = {key: {"expected": value, "actual": actual[key]} for key, value in expected.items() if actual[key] != value}
    if mismatches:
        raise CanonicalPitContractError(f"non-canonical PIT binding for {consumer_name}: {mismatches}")
    if binding.universe_key != CANONICAL_PIT_UNIVERSE_KEY and not binding.universe_key.startswith(
        CANONICAL_PIT_SNAPSHOT_PREFIX
    ):
        raise CanonicalPitContractError(f"unsupported canonical PIT materialization: {binding.universe_key!r}")
    if immutable_snapshot_required:
        if not binding.universe_key.startswith(CANONICAL_PIT_SNAPSHOT_PREFIX):
            raise CanonicalPitContractError(f"{consumer_name} requires an immutable canonical PIT snapshot")
        _require_frozen_release_identity(binding)
    return binding


def require_canonical_rolling_universe_key(universe_key: str) -> str:
    normalized = str(universe_key or "").strip()
    if normalized != CANONICAL_PIT_UNIVERSE_KEY:
        raise CanonicalPitContractError(
            f"rolling consumers must use {CANONICAL_PIT_UNIVERSE_KEY!r}; received {normalized!r}"
        )
    return normalized


def _require_frozen_release_identity(binding: PitConsumerBinding) -> None:
    if not str(binding.release_id or "").strip() or binding.cutoff is None:
        raise CanonicalPitContractError("immutable/reproduction PIT requires release_id and cutoff")
    if not _SHA256_RE.fullmatch(str(binding.snapshot_digest or "")):
        raise CanonicalPitContractError("immutable/reproduction PIT requires a sha256 snapshot_digest")


__all__ = [
    "CANONICAL_PIT_AUTHORITY_ID",
    "CANONICAL_PIT_IPO_TRADING_SESSIONS",
    "CANONICAL_PIT_RULE_VERSION",
    "CANONICAL_PIT_SCOPE",
    "CANONICAL_PIT_SNAPSHOT_PREFIX",
    "CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT",
    "CANONICAL_PIT_UNIVERSE_KEY",
    "LEGACY_PIT_RULE_VERSION",
    "LEGACY_PIT_UNIVERSE_KEY",
    "CanonicalPitContractError",
    "CanonicalPitAuthorityResolver",
    "PitActivationTarget",
    "PitAuthorityStatus",
    "PitConsumerBinding",
    "canonical_rule_parameters_digest",
    "legacy_rule_parameters_digest",
    "activate_canonical_pit_authority",
    "require_canonical_consumer_binding",
    "require_canonical_rolling_universe_key",
]
