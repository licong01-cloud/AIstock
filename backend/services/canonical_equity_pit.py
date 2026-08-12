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
from enum import StrEnum
from typing import Any, Mapping


CANONICAL_PIT_AUTHORITY_ID = "aistock_equity_pit_canonical"
CANONICAL_PIT_UNIVERSE_KEY = "aistock_equity_pit_canonical_v2"
CANONICAL_PIT_RULE_VERSION = "shsz_a_252td_st_delist_asof_v2"
CANONICAL_PIT_SCOPE = "canonical_all_listed"
CANONICAL_PIT_IPO_TRADING_SESSIONS = 252
CANONICAL_PIT_SNAPSHOT_PREFIX = "aistock_equity_pit_snapshot_"

LEGACY_NONCANONICAL_UNIVERSE_KEYS = frozenset({"shsz_st_pit_active_v1"})
LEGACY_NONCANONICAL_SNAPSHOT_PREFIXES = ("shsz_st_pit_qe_dataset_",)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CanonicalPitContractError(ValueError):
    """Raised when a consumer attempts to use a non-authoritative PIT."""


class PitAuthorityStatus(StrEnum):
    ACTIVE_CANONICAL = "ACTIVE_CANONICAL"
    DEPLOYED_LEGACY_PENDING_MIGRATION = "DEPLOYED_LEGACY_PENDING_MIGRATION"
    ARCHIVED_NONCANONICAL = "ARCHIVED_NONCANONICAL"


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


def canonical_rule_parameters_digest() -> str:
    payload = json.dumps(
        CANONICAL_RULE_PARAMETERS,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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
    "CANONICAL_PIT_UNIVERSE_KEY",
    "CanonicalPitContractError",
    "PitAuthorityStatus",
    "PitConsumerBinding",
    "canonical_rule_parameters_digest",
    "require_canonical_consumer_binding",
    "require_canonical_rolling_universe_key",
]
