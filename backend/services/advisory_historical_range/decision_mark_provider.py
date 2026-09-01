"""Read and seal T-cutoff recommendation marks for Phase 1R R3.

This module owns only historical database reads and immutable evidence.  It
does not inspect current Advisory state, infer candidates, create orders, or
write any application table.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Callable, Protocol

import psycopg2.extras

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    DECISION_MARK_SET_PAYLOAD_SCHEMA_VERSION,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeDecisionMarkSetV1,
    HistoricalRangeDecisionMarkV2,
    HistoricalRangeEpisodeMarkV2,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionRefV1,
)
from backend.services.advisory_historical_range.source_roles import (
    DECISION_MARK_SOURCE_ROLES_V1,
    select_day_source_roles,
)
from backend.services.strategy_package.advisory_input_projection import SELECTION_PIT_UNIVERSE_KEY


DECISION_MARK_PROVIDER_CONTRACT_VERSION = "advisory_historical_range_decision_mark_provider_v1"
DECISION_MARK_POLICY_VERSION = "pit_decision_then_mature_mark_v1"

_MARKET_SQL = """
    SELECT COALESCE(price.ts_code, adj.ts_code) AS ts_code,
           price.close_li,
           adj.adj_factor
    FROM market.kline_daily_raw AS price
    FULL OUTER JOIN market.adj_factor AS adj
      ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
    WHERE COALESCE(price.trade_date, adj.trade_date) = %s
    ORDER BY COALESCE(price.ts_code, adj.ts_code)
"""

_STATE_SQL = """
    WITH suspended AS (
        SELECT DISTINCT ts_code
        FROM market.suspend_d
        WHERE trade_date = %s AND suspend_type = 'S'
    ), pit AS (
        SELECT ts_code
        FROM market.stock_universe_pit_spans
        WHERE universe_key = %s
          AND eligible_start <= %s
          AND eligible_end >= %s
    )
    SELECT basic.ts_code, basic.list_date, basic.delist_date, basic.list_status,
           suspended.ts_code IS NOT NULL AS suspended,
           pit.ts_code IS NOT NULL AS pit_eligible
    FROM market.stock_basic AS basic
    LEFT JOIN suspended ON suspended.ts_code = basic.ts_code
    LEFT JOIN pit ON pit.ts_code = basic.ts_code
    WHERE basic.list_date IS NULL OR basic.list_date <= %s
    ORDER BY basic.ts_code
"""


class HistoricalRangeDecisionMarkSourceVerifier(Protocol):
    def verify_program_day(
        self,
        *,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        research_program_id: str,
        package_id: str,
        component_ids: set[str],
        decision_trade_date: date,
        source_roles: frozenset[str] | None = None,
    ) -> tuple[HistoricalRangeSourceRevisionRefV1, ...]: ...


class HistoricalRangeDecisionMarkReader(Protocol):
    def read(
        self,
        *,
        decision_trade_date: date,
        universe_key: str,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], datetime]: ...


class PostgresHistoricalRangeDecisionMarkReader:
    """One read-only repeatable-read window for the two mark source tables."""

    def __init__(self, *, conn_factory: Callable[[], Any]) -> None:
        if conn_factory is None:
            raise ValueError("conn_factory is required")
        self._conn_factory = conn_factory

    def read(
        self,
        *,
        decision_trade_date: date,
        universe_key: str,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], datetime]:
        normalized_universe_key = str(universe_key or "").strip()
        if not normalized_universe_key:
            raise ValueError("universe_key is required for historical decision marks")
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT transaction_timestamp() AS observed_at")
                observed_at = cur.fetchone()["observed_at"].astimezone(UTC)
                cur.execute(_MARKET_SQL, (decision_trade_date,))
                market = {
                    str(row["ts_code"]).upper(): dict(row)
                    for row in cur.fetchall()
                    if str(row.get("ts_code") or "").strip()
                }
                cur.execute(
                    _STATE_SQL,
                    (
                        decision_trade_date,
                        normalized_universe_key,
                        decision_trade_date,
                        decision_trade_date,
                        decision_trade_date,
                    ),
                )
                states = {
                    str(row["ts_code"]).upper(): dict(row)
                    for row in cur.fetchall()
                    if str(row.get("ts_code") or "").strip()
                }
            conn.rollback()
        return market, states, observed_at


@dataclass(frozen=True)
class HistoricalRangeDecisionMarkProductionResultV1:
    mark_set: HistoricalRangeDecisionMarkSetV1
    artifact_ref: HistoricalRangeArtifactRefV1


class HistoricalRangeDecisionMarkProvider:
    def __init__(
        self,
        *,
        reader: HistoricalRangeDecisionMarkReader,
        source_verifier: HistoricalRangeDecisionMarkSourceVerifier,
        artifact_store: HistoricalRangeArtifactStore,
        mark_policy_version: str = DECISION_MARK_POLICY_VERSION,
    ) -> None:
        if reader is None or source_verifier is None or artifact_store is None:
            raise ValueError("decision mark provider requires explicit read-only dependencies")
        self._reader = reader
        self._source_verifier = source_verifier
        self._artifact_store = artifact_store
        self._mark_policy_version = str(mark_policy_version or "").strip()
        if not self._mark_policy_version:
            raise ValueError("mark_policy_version is required")
        self._mark_policy_hash = canonical_json_sha256(
            {
                "schema_version": "advisory_historical_range_decision_mark_policy_v1",
                "mark_policy_version": self._mark_policy_version,
                "raw_unit": "yuan",
                "currency": "CNY",
                "adjustment_basis": "corporate_action_normalized_from_raw",
            }
        )

    @property
    def mark_policy_hash(self) -> str:
        return self._mark_policy_hash

    def produce(
        self,
        *,
        resolved_request_hash: str,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        program: HistoricalRangeFrozenProgramV1,
        range_run_id: str,
        day_run_id: str,
        decision_trade_date: date,
        request_ref: HistoricalRangeArtifactRefV1,
        included_symbols: set[str],
        previous_marks_by_symbol: Mapping[str, HistoricalRangeEpisodeMarkV2],
        predecessor_day_receipt_ref: HistoricalRangeArtifactRefV1 | None,
        decision_cutoff: datetime,
    ) -> HistoricalRangeDecisionMarkProductionResultV1:
        component_ids = {item.component_id for item in program.admitted_package_projection.components}
        selection = select_day_source_roles(
            catalog=catalog,
            research_program_id=program.research_program_id,
            package_id=program.package_id,
            component_ids=component_ids,
            decision_trade_date=decision_trade_date,
        )
        expected_refs = tuple(
            sorted(
                (
                    HistoricalRangeSourceRevisionRefV1(revision_id=item.revision_id, revision_hash=item.revision_hash)
                    for item in selection.decision_mark_members
                ),
                key=lambda item: (item.revision_id, item.revision_hash),
            )
        )
        initial_refs = self._source_verifier.verify_program_day(
            catalog=catalog,
            research_program_id=program.research_program_id,
            package_id=program.package_id,
            component_ids=component_ids,
            decision_trade_date=decision_trade_date,
            source_roles=DECISION_MARK_SOURCE_ROLES_V1,
        )
        if initial_refs != expected_refs:
            raise ValueError("pre-read decision-mark source refs differ from the sealed role selection")

        binding = program.admitted_package_projection.canonical_pit_binding
        universe_key = binding.frozen_universe_key if binding is not None else SELECTION_PIT_UNIVERSE_KEY
        market_rows, state_rows, observed_at = self._reader.read(
            decision_trade_date=decision_trade_date,
            universe_key=universe_key,
        )
        subjects = tuple(sorted({str(item).upper() for item in included_symbols} | {str(item).upper() for item in previous_marks_by_symbol}))
        marks = tuple(
            self._build_mark(
                symbol=symbol,
                decision_trade_date=decision_trade_date,
                market_row=market_rows.get(symbol),
                state_row=state_rows.get(symbol),
                previous_mark=previous_marks_by_symbol.get(symbol),
                source_refs=expected_refs,
                observed_at=observed_at,
                decision_cutoff=decision_cutoff,
            )
            for symbol in subjects
        )
        final_refs = self._source_verifier.verify_program_day(
            catalog=catalog,
            research_program_id=program.research_program_id,
            package_id=program.package_id,
            component_ids=component_ids,
            decision_trade_date=decision_trade_date,
            source_roles=DECISION_MARK_SOURCE_ROLES_V1,
        )
        if final_refs != initial_refs:
            raise ValueError("decision-mark source refs changed during the read-only mark projection")
        source_set_hash = canonical_json_sha256([item.model_dump(mode="json") for item in expected_refs])
        mark_set = HistoricalRangeDecisionMarkSetV1(
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            decision_trade_date=decision_trade_date,
            subject_set_hash=canonical_json_sha256([item.symbol for item in marks]),
            mark_policy_version=self._mark_policy_version,
            mark_policy_hash=self._mark_policy_hash,
            source_revision_set_hash=source_set_hash,
            source_revision_refs=expected_refs,
            upstream_request_ref=request_ref,
            predecessor_day_receipt_ref=predecessor_day_receipt_ref,
            marks=marks,
        )
        upstream = tuple(
            sorted(
                (request_ref,) if predecessor_day_receipt_ref is None else (request_ref, predecessor_day_receipt_ref),
                key=lambda item: (item.artifact_kind.value, item.semantic_content_hash, item.relative_path),
            )
        )
        stored = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DECISION_MARK_SET,
            producer_contract_version=DECISION_MARK_PROVIDER_CONTRACT_VERSION,
            payload_schema_version=DECISION_MARK_SET_PAYLOAD_SCHEMA_VERSION,
            resolved_request_hash=resolved_request_hash,
            payload=mark_set.model_dump(mode="json"),
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            source_revision_refs=expected_refs,
            upstream_refs=upstream,
        )
        envelope = self._artifact_store.load(stored.ref)
        parsed = HistoricalRangeDecisionMarkSetV1.model_validate(envelope.payload)
        if (
            parsed != mark_set
            or envelope.source_revision_refs != expected_refs
            or envelope.upstream_refs != upstream
            or envelope.range_run_id != range_run_id
            or envelope.day_run_id != day_run_id
            or envelope.resolved_request_hash != resolved_request_hash
        ):
            raise ValueError("decision-mark artifact readback does not close its typed payload and lineage")
        return HistoricalRangeDecisionMarkProductionResultV1(mark_set=parsed, artifact_ref=stored.ref)

    @staticmethod
    def _build_mark(
        *,
        symbol: str,
        decision_trade_date: date,
        market_row: Mapping[str, Any] | None,
        state_row: Mapping[str, Any] | None,
        previous_mark: HistoricalRangeEpisodeMarkV2 | None,
        source_refs: tuple[HistoricalRangeSourceRevisionRefV1, ...],
        observed_at: datetime,
        decision_cutoff: datetime,
    ) -> HistoricalRangeDecisionMarkV2:
        raw = _decimal_or_none((market_row or {}).get("close_li"))
        adjustment = _decimal_or_none((market_row or {}).get("adj_factor"))
        state = dict(state_row or {})
        legal_no_quote = bool(state.get("suspended")) or not bool(state.get("pit_eligible", False)) or _is_terminal(state, decision_trade_date)
        evidence_hash = canonical_json_sha256(
            {
                "symbol": symbol,
                "market": dict(market_row or {}),
                "state": state,
                "previous_mark_hash": canonical_json_sha256(previous_mark.model_dump(mode="json")) if previous_mark else None,
                "source_refs": [item.model_dump(mode="json") for item in source_refs],
            }
        )
        if raw is not None and adjustment is not None:
            tradability_status = (
                "OUTSIDE_PIT_UNIVERSE_WITH_QUOTE"
                if not bool(state.get("pit_eligible", False))
                else "SUSPENDED_WITH_QUOTE"
                if bool(state.get("suspended"))
                else "TRADABLE"
            )
            return HistoricalRangeDecisionMarkV2(
                symbol=symbol,
                decision_trade_date=decision_trade_date,
                availability="AVAILABLE",
                raw_reference_yuan=raw / Decimal("1000"),
                adjustment_factor_as_of_t=adjustment,
                normalized_reference_mark=(raw / Decimal("1000")) * adjustment,
                mark_quality="T_CLOSE",
                tradability_status=tradability_status,
                source_revision_refs=source_refs,
                source_evidence_hash=evidence_hash,
                fact_effective_at=decision_cutoff,
                decision_cutoff=decision_cutoff,
                source_observed_at=observed_at,
                revision_admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
            )
        if (
            legal_no_quote
            and previous_mark is not None
            and adjustment is not None
            and previous_mark.current_raw_reference_yuan is not None
        ):
            quality = "TERMINAL_CARRY_FORWARD" if _is_terminal(state, decision_trade_date) else "SUSPENDED_CARRY_FORWARD"
            tradability_status = (
                "OUTSIDE_PIT_UNIVERSE"
                if not bool(state.get("pit_eligible", False))
                else "TERMINAL"
                if quality == "TERMINAL_CARRY_FORWARD"
                else "SUSPENDED"
            )
            return HistoricalRangeDecisionMarkV2(
                symbol=symbol,
                decision_trade_date=decision_trade_date,
                availability="MARKET_STATE_NO_QUOTE",
                raw_reference_yuan=previous_mark.current_raw_reference_yuan,
                adjustment_factor_as_of_t=adjustment,
                normalized_reference_mark=previous_mark.current_raw_reference_yuan * adjustment,
                mark_quality=quality,
                tradability_status=tradability_status,
                source_revision_refs=source_refs,
                source_evidence_hash=evidence_hash,
                fact_effective_at=decision_cutoff,
                decision_cutoff=decision_cutoff,
                source_observed_at=observed_at,
                revision_admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
            )
        return HistoricalRangeDecisionMarkV2(
            symbol=symbol,
            decision_trade_date=decision_trade_date,
            availability="DATA_UNAVAILABLE",
            mark_quality="UNAVAILABLE",
            tradability_status=(
                "OUTSIDE_PIT_UNIVERSE"
                if not bool(state.get("pit_eligible", False))
                else "MARKET_STATE_UNRESOLVED"
                if legal_no_quote
                else "QUOTE_OR_ADJUSTMENT_UNAVAILABLE"
            ),
            source_revision_refs=source_refs,
            source_evidence_hash=evidence_hash,
            fact_effective_at=decision_cutoff,
            decision_cutoff=decision_cutoff,
            source_observed_at=observed_at,
            revision_admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        )


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    normalized = Decimal(str(value))
    return normalized if normalized > 0 else None


def _is_terminal(state: Mapping[str, Any], decision_trade_date: date) -> bool:
    delist_date = state.get("delist_date")
    if delist_date is not None:
        try:
            if date.fromisoformat(str(delist_date)[:10]) <= decision_trade_date:
                return True
        except ValueError:
            return False
    return str(state.get("list_status") or "").upper() == "D"
