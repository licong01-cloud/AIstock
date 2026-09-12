"""Advisory-owned index-universe admission contract.

The public selection shape intentionally matches QE's ``universe_selection``
semantic contract, while membership resolution remains read-only and is
delegated to the shared core-index PIT authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Protocol, Sequence

import psycopg2.extras as pgx

from backend.db.pg_pool import get_conn
from backend.services.canonical_equity_pit import CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY
from backend.services.core_index_membership import (
    P0_POOL_IDS,
    POOL_DEFINITIONS,
    CanonicalEquityInterval,
    CoreIndexMembershipRepository,
    CoreIndexMembershipUnavailable,
    MembershipInterval,
    PoolCoverage,
    ResolvedUniverse,
    UniverseMode,
    UniverseSelection,
    UniverseUnavailableReason,
    resolve_universe,
)
from backend.services.stock_universe_pit_service import (
    DEFAULT_ST_PIT_RULE_VERSION,
    DEFAULT_ST_PIT_UNIVERSE_KEY,
)


ADVISORY_UNIVERSE_SCHEMA_VERSION = "advisory_universe_selection_v1"
ADVISORY_UNIVERSE_RECEIPT_SCHEMA_VERSION = "advisory_universe_admission_receipt_v1"
ADVISORY_SUPPORTED_POOL_IDS = tuple(P0_POOL_IDS)
ADVISORY_PIT_SOURCE_FROZEN_CANONICAL = "FROZEN_CANONICAL_PIT"
ADVISORY_PIT_SOURCE_LIVE_ROLLING = "LIVE_SELECTION_ROLLING_PIT"
ADVISORY_PIT_SOURCE_SELECTION_PASSTHROUGH = "SELECTION_RUNTIME_PASSTHROUGH"
_SELECTION_FIELDS = frozenset({"mode", "pool_ids"})
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class AdvisoryUniverseContractError(ValueError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True, slots=True)
class AdvisoryUniverseSnapshot:
    selection: dict[str, Any]
    trade_date: date
    membership_revision: str
    eligible_symbols: frozenset[str]
    source_pool_ids_by_symbol: Mapping[str, tuple[str, ...]]
    pit_source: str = ADVISORY_PIT_SOURCE_FROZEN_CANONICAL
    pit_universe_key: str = CANONICAL_PIT_UNIVERSE_KEY
    pit_rule_version: str = CANONICAL_PIT_RULE_VERSION
    pit_revision: str | None = None

    @property
    def symbol_set_sha256(self) -> str:
        payload = json.dumps(sorted(self.eligible_symbols), ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AdvisoryUniverseResolver(Protocol):
    def resolve(self, selection: Mapping[str, Any], trade_date: date) -> AdvisoryUniverseSnapshot: ...


class AdvisoryLiveRollingPitRepository:
    """Advisory-only adapter over Selection's ready live rolling PIT.

    Index membership and calendar reads stay delegated to the shared resolver
    repository.  Only the final equity-eligibility intersection switches to the
    live rolling authority after the frozen canonical PIT no longer covers the
    requested trading day.
    """

    def __init__(self, connection_factory: Callable[[], Any] = get_conn) -> None:
        self._connection_factory = connection_factory
        self._membership = CoreIndexMembershipRepository(connection_factory)
        self.pit_revision: str | None = None

    def fetch_pool_coverage(self, pool_ids: Sequence[str]) -> Mapping[str, PoolCoverage]:
        return self._membership.fetch_pool_coverage(pool_ids)

    def fetch_membership_intervals(
        self,
        pool_ids: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> Sequence[MembershipInterval]:
        return self._membership.fetch_membership_intervals(pool_ids, start_date, end_date)

    def fetch_trading_dates(self, start_date: date, end_date: date) -> Sequence[date]:
        return self._membership.fetch_trading_dates(start_date, end_date)

    def fetch_canonical_intervals(
        self,
        start_date: date,
        end_date: date,
    ) -> Sequence[CanonicalEquityInterval]:
        with self._connection_factory() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT rule_version, status, dirty, start_date, end_date,
                           source_fingerprint_sha256, updated_at
                      FROM market.stock_universe_pit_state
                     WHERE universe_key = %s
                    """,
                    (DEFAULT_ST_PIT_UNIVERSE_KEY,),
                )
                state = cur.fetchone()
                start_covered = isinstance(state and state.get("start_date"), date)
                end_covered = isinstance(state and state.get("end_date"), date)
                revision = str((state or {}).get("source_fingerprint_sha256") or "").strip().lower()
                if (
                    not state
                    or state["rule_version"] != DEFAULT_ST_PIT_RULE_VERSION
                    or state["status"] != "ready"
                    or bool(state["dirty"])
                    or not start_covered
                    or not end_covered
                    or state["start_date"] > start_date
                    or state["end_date"] < end_date
                    or _SHA256_PATTERN.fullmatch(revision) is None
                ):
                    raise CoreIndexMembershipUnavailable(
                        UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE,
                        "live Selection rolling PIT is not ready for requested window",
                    )
                self.pit_revision = revision
                cur.execute(
                    """
                    SELECT ts_code, eligible_start, eligible_end
                      FROM market.stock_universe_pit_spans
                     WHERE universe_key = %s
                       AND rule_version = %s
                       AND eligible_start <= %s
                       AND eligible_end >= %s
                     ORDER BY ts_code, eligible_start, eligible_end
                    """,
                    (DEFAULT_ST_PIT_UNIVERSE_KEY, DEFAULT_ST_PIT_RULE_VERSION, end_date, start_date),
                )
                rows = list(cur.fetchall())
        return tuple(
            CanonicalEquityInterval(
                ts_code=str(row["ts_code"]).strip().upper(),
                eligible_start=row["eligible_start"],
                eligible_end=row["eligible_end"],
            )
            for row in rows
        )


def normalize_advisory_universe_selection(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is not None and not isinstance(value, Mapping):
        raise AdvisoryUniverseContractError(
            "ADVISORY_UNIVERSE_SELECTION_INVALID",
            "advisory universe_selection must be an object",
        )
    raw = dict(value or {"mode": UniverseMode.STOCK_UNIVERSE.value, "pool_ids": []})
    unknown_fields = sorted(set(raw) - _SELECTION_FIELDS)
    if unknown_fields:
        raise AdvisoryUniverseContractError(
            "ADVISORY_UNIVERSE_FIELDS_INVALID",
            "advisory universe_selection contains unsupported fields",
            context={"fields": unknown_fields},
        )
    raw_pool_ids = raw.get("pool_ids", [])
    if not isinstance(raw_pool_ids, list):
        raise AdvisoryUniverseContractError(
            "ADVISORY_UNIVERSE_POOL_IDS_INVALID",
            "advisory universe_selection.pool_ids must be a list",
        )
    try:
        selection = UniverseSelection.from_mapping(
            {
                "mode": raw.get("mode") or UniverseMode.STOCK_UNIVERSE.value,
                "pool_ids": raw_pool_ids,
            }
        )
    except CoreIndexMembershipUnavailable as exc:
        raise AdvisoryUniverseContractError(
            "ADVISORY_UNIVERSE_SELECTION_INVALID",
            str(exc),
        ) from exc
    unsupported = sorted(set(selection.pool_ids) - set(ADVISORY_SUPPORTED_POOL_IDS))
    if unsupported:
        raise AdvisoryUniverseContractError(
            "ADVISORY_UNIVERSE_POOL_UNSUPPORTED",
            "advisory supports only QE-compatible P0 core-index pools",
            context={"pool_ids": unsupported, "supported_pool_ids": list(ADVISORY_SUPPORTED_POOL_IDS)},
        )
    return {"mode": selection.mode.value, "pool_ids": list(selection.pool_ids)}


def advisory_universe_catalog() -> dict[str, Any]:
    labels = {
        "csi300": "沪深300",
        "csi500": "中证500",
        "csi1000": "中证1000",
        "star50": "科创50",
        "star100": "科创100",
    }
    return {
        "schema_version": ADVISORY_UNIVERSE_SCHEMA_VERSION,
        "default_selection": {"mode": UniverseMode.STOCK_UNIVERSE.value, "pool_ids": []},
        "modes": [mode.value for mode in UniverseMode],
        "pools": [
            {
                "pool_id": pool_id,
                "index_code": POOL_DEFINITIONS[pool_id].index_code,
                "label": labels.get(pool_id, pool_id),
                "priority": POOL_DEFINITIONS[pool_id].priority,
            }
            for pool_id in ADVISORY_SUPPORTED_POOL_IDS
        ],
    }


class CoreIndexAdvisoryUniverseResolver:
    def __init__(
        self,
        *,
        resolve_fn: Callable[..., ResolvedUniverse] = resolve_universe,
        live_repository_factory: Callable[[], AdvisoryLiveRollingPitRepository] = AdvisoryLiveRollingPitRepository,
    ) -> None:
        self._resolve_fn = resolve_fn
        self._live_repository_factory = live_repository_factory

    def resolve(self, selection: Mapping[str, Any], trade_date: date) -> AdvisoryUniverseSnapshot:
        normalized = normalize_advisory_universe_selection(selection)
        pit_source = ADVISORY_PIT_SOURCE_FROZEN_CANONICAL
        pit_universe_key = CANONICAL_PIT_UNIVERSE_KEY
        pit_rule_version = CANONICAL_PIT_RULE_VERSION
        pit_revision: str | None = None
        try:
            resolved = self._resolve_fn(
                UniverseSelection.from_mapping(normalized),
                trade_date,
                trade_date,
            )
        except CoreIndexMembershipUnavailable as exc:
            if exc.reason is not UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE:
                raise AdvisoryUniverseContractError(
                    "ADVISORY_UNIVERSE_MEMBERSHIP_UNAVAILABLE",
                    str(exc),
                    context={"trade_date": trade_date.isoformat(), "universe_selection": normalized},
                ) from exc
            live_repository = self._live_repository_factory()
            try:
                resolved = self._resolve_fn(
                    UniverseSelection.from_mapping(normalized),
                    trade_date,
                    trade_date,
                    repository=live_repository,
                )
            except CoreIndexMembershipUnavailable as live_exc:
                raise AdvisoryUniverseContractError(
                    "ADVISORY_UNIVERSE_MEMBERSHIP_UNAVAILABLE",
                    str(live_exc),
                    context={
                        "trade_date": trade_date.isoformat(),
                        "universe_selection": normalized,
                        "frozen_pit_error": str(exc),
                        "live_pit_error": str(live_exc),
                    },
                ) from live_exc
            pit_source = ADVISORY_PIT_SOURCE_LIVE_ROLLING
            pit_universe_key = DEFAULT_ST_PIT_UNIVERSE_KEY
            pit_rule_version = DEFAULT_ST_PIT_RULE_VERSION
            pit_revision = live_repository.pit_revision
        eligible = frozenset(
            row.ts_code for row in resolved.intervals if row.eligible_start <= trade_date <= row.eligible_end
        )
        if not eligible:
            raise AdvisoryUniverseContractError(
                "ADVISORY_UNIVERSE_MEMBERSHIP_UNAVAILABLE",
                "resolved advisory index universe is empty for trade_date",
                context={"trade_date": trade_date.isoformat(), "universe_selection": normalized},
            )
        return AdvisoryUniverseSnapshot(
            selection=normalized,
            trade_date=trade_date,
            membership_revision=resolved.membership_revision,
            eligible_symbols=eligible,
            source_pool_ids_by_symbol=resolved.source_pool_ids_by_symbol,
            pit_source=pit_source,
            pit_universe_key=pit_universe_key,
            pit_rule_version=pit_rule_version,
            pit_revision=pit_revision,
        )


def build_universe_admission_receipt(
    *,
    selection: Mapping[str, Any],
    trade_date: date,
    universe_as_of_trade_date: date | None = None,
    input_count: int,
    output_count: int,
    membership_revision: str,
    symbol_set_sha256: str | None,
    pit_source: str = ADVISORY_PIT_SOURCE_SELECTION_PASSTHROUGH,
    pit_universe_key: str | None = None,
    pit_rule_version: str | None = None,
    pit_revision: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_advisory_universe_selection(selection)
    return {
        "schema_version": ADVISORY_UNIVERSE_RECEIPT_SCHEMA_VERSION,
        "universe_selection": normalized,
        "trade_date": trade_date.isoformat(),
        "universe_as_of_trade_date": (universe_as_of_trade_date or trade_date).isoformat(),
        "membership_revision": membership_revision,
        "symbol_set_sha256": symbol_set_sha256,
        "pit_source": pit_source,
        "pit_universe_key": pit_universe_key,
        "pit_rule_version": pit_rule_version,
        "pit_revision": pit_revision,
        "input_candidate_count": int(input_count),
        "output_candidate_count": int(output_count),
        "excluded_candidate_count": int(input_count - output_count),
        "admission_stage": "AFTER_SELECTION_BEFORE_ADVISORY_RANKING",
    }


__all__ = [
    "ADVISORY_SUPPORTED_POOL_IDS",
    "ADVISORY_PIT_SOURCE_FROZEN_CANONICAL",
    "ADVISORY_PIT_SOURCE_LIVE_ROLLING",
    "ADVISORY_PIT_SOURCE_SELECTION_PASSTHROUGH",
    "ADVISORY_UNIVERSE_RECEIPT_SCHEMA_VERSION",
    "ADVISORY_UNIVERSE_SCHEMA_VERSION",
    "AdvisoryUniverseContractError",
    "AdvisoryUniverseResolver",
    "AdvisoryUniverseSnapshot",
    "AdvisoryLiveRollingPitRepository",
    "CoreIndexAdvisoryUniverseResolver",
    "advisory_universe_catalog",
    "build_universe_admission_receipt",
    "normalize_advisory_universe_selection",
]
