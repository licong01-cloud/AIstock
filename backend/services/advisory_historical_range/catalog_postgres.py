"""Read-only PostgreSQL source catalog execution for Phase 1R."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from typing import Any

import orjson
import psycopg2.extensions
import psycopg2.extras

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.catalog_planner import (
    HistoricalRangeCatalogChunkResult,
    HistoricalRangeCatalogPlanner,
    HistoricalRangeSourceInputUnavailable,
    HistoricalRangeSourceRequirementResolver,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactRefV1,
    HistoricalRangeCatalogPhase,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceCatalogCheckpointV1,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionRefV1,
    HistoricalRangeSourceRevisionMemberV1,
    HistoricalRangeContractError,
    REASON_SOURCE_REVISION_MISMATCH,
    normalize_hmm_binding_metadata,
)


ConnFactory = Callable[[], Any]

_DEFAULT_CATALOG_WORKERS = 6
_MAX_CATALOG_WORKERS = 6
_DEFAULT_STREAM_FETCH_SIZE = 256
_MAX_STREAM_FETCH_SIZE = 1000
_ORJSON_EXPONENT_NUMBER = re.compile(rb"e-?\d")


_UNIVERSE_SQL = """
    SELECT jsonb_build_object('ts_code', span.ts_code) AS payload
    FROM market.stock_universe_pit_spans AS span
    WHERE span.universe_key = %s
      AND span.eligible_start <= %s
      AND span.eligible_end >= %s
    ORDER BY span.ts_code
"""

_CALENDAR_SQL = """
    SELECT jsonb_build_object('cal_date', cal_date, 'is_trading', is_trading) AS payload
    FROM market.trading_calendar
    WHERE cal_date >= %s AND cal_date <= %s
    ORDER BY cal_date
"""

_MARKET_HISTORY_SQL = """
    SELECT jsonb_build_object(
        'trade_date', price.trade_date,
        'ts_code', price.ts_code,
        'open_li', price.open_li,
        'high_li', price.high_li,
        'low_li', price.low_li,
        'close_li', price.close_li,
        'volume_hand', price.volume_hand,
        'amount_li', price.amount_li,
        'adj_factor', adj.adj_factor
    ) AS payload
    FROM market.kline_daily_raw AS price
    JOIN market.stock_universe_pit_spans AS span
      ON span.ts_code = price.ts_code
     AND span.universe_key = %s
     AND span.eligible_start <= %s
     AND span.eligible_end >= %s
    LEFT JOIN market.adj_factor AS adj
      ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
    WHERE price.trade_date >= %s AND price.trade_date <= %s
    ORDER BY price.trade_date, price.ts_code
"""

_DECISION_MARK_DAILY_MARKET_SQL = """
    SELECT jsonb_build_object(
        'trade_date', COALESCE(price.trade_date, adj.trade_date),
        'ts_code', COALESCE(price.ts_code, adj.ts_code),
        'close_li', price.close_li,
        'adj_factor', adj.adj_factor
    ) AS payload
    FROM market.kline_daily_raw AS price
    FULL OUTER JOIN market.adj_factor AS adj
      ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
    WHERE COALESCE(price.trade_date, adj.trade_date) = %s
    ORDER BY COALESCE(price.ts_code, adj.ts_code)
"""

_DECISION_MARK_MARKET_STATE_SQL = """
    WITH suspended AS (
        SELECT DISTINCT ts_code
        FROM market.suspend_d
        WHERE trade_date = %s AND suspend_type = 'S'
    ), pit AS (
        SELECT ts_code
        FROM market.stock_universe_pit_spans
        WHERE universe_key = 'shsz_st_pit_active_v1'
          AND eligible_start <= %s
          AND eligible_end >= %s
    )
    SELECT jsonb_build_object(
        'ts_code', basic.ts_code,
        'list_date', basic.list_date,
        'delist_date', basic.delist_date,
        'list_status', basic.list_status,
        'suspended', suspended.ts_code IS NOT NULL,
        'pit_eligible', pit.ts_code IS NOT NULL
    ) AS payload
    FROM market.stock_basic AS basic
    LEFT JOIN suspended ON suspended.ts_code = basic.ts_code
    LEFT JOIN pit ON pit.ts_code = basic.ts_code
    WHERE basic.list_date IS NULL OR basic.list_date <= %s
    ORDER BY basic.ts_code
"""

_SUSPEND_SQL = """
    SELECT jsonb_build_object(
        'trade_date', trade_date,
        'ts_code', ts_code,
        'suspend_type', suspend_type,
        'suspend_timing', suspend_timing
    ) AS payload
    FROM market.suspend_d
    WHERE trade_date = %s AND suspend_type = 'S'
    ORDER BY ts_code, suspend_timing NULLS FIRST
"""

_INDUSTRY_SQL = """
    SELECT to_jsonb(row_data) AS payload FROM (
        SELECT ts_code, l1_code, l1_name, l2_code, l2_name,
               l3_code, l3_name, in_date, out_date
        FROM market.sw_index_member
        WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
        ORDER BY ts_code, in_date, l3_code NULLS LAST
    ) AS row_data
"""

_FUNDAMENTAL_QUERIES: tuple[tuple[str, str], ...] = (
    (
        "daily_basic",
        """
        SELECT to_jsonb(row_data) AS payload FROM (
            SELECT data.trade_date, data.ts_code, data.close, data.turnover_rate,
                   data.turnover_rate_f, data.volume_ratio, data.pe, data.pe_ttm,
                   data.pb, data.ps, data.ps_ttm, data.dv_ratio, data.dv_ttm,
                   data.total_share, data.float_share, data.free_share, data.total_mv, data.circ_mv
            FROM market.daily_basic AS data
            JOIN market.stock_universe_pit_spans AS span ON span.ts_code = data.ts_code
            WHERE span.universe_key = %s AND span.eligible_start <= %s AND span.eligible_end >= %s
              AND data.trade_date >= %s AND data.trade_date <= %s
            ORDER BY data.trade_date, data.ts_code
        ) AS row_data
        """,
    ),
    (
        "moneyflow_ts",
        """
        SELECT to_jsonb(row_data) AS payload FROM (
            SELECT data.trade_date, data.ts_code,
                   data.buy_sm_vol, data.buy_sm_amount, data.sell_sm_vol, data.sell_sm_amount,
                   data.buy_md_vol, data.buy_md_amount, data.sell_md_vol, data.sell_md_amount,
                   data.buy_lg_vol, data.buy_lg_amount, data.sell_lg_vol, data.sell_lg_amount,
                   data.buy_elg_vol, data.buy_elg_amount, data.sell_elg_vol, data.sell_elg_amount,
                   data.net_mf_vol, data.net_mf_amount
            FROM market.moneyflow_ts AS data
            JOIN market.stock_universe_pit_spans AS span ON span.ts_code = data.ts_code
            WHERE span.universe_key = %s AND span.eligible_start <= %s AND span.eligible_end >= %s
              AND data.trade_date >= %s AND data.trade_date <= %s
            ORDER BY data.trade_date, data.ts_code
        ) AS row_data
        """,
    ),
    (
        "bak_basic",
        """
        SELECT to_jsonb(row_data) AS payload FROM (
            SELECT data.trade_date, data.ts_code, data.pe_dyn, data.total_assets, data.liquid_assets,
                   data.fixed_assets, data.reserved, data.reserved_pershare, data.eps, data.bvps,
                   data.undp, data.per_undp, data.rev_yoy, data.profit_yoy, data.gpr, data.npr,
                   data.holder_num
            FROM market.bak_basic AS data
            JOIN market.stock_universe_pit_spans AS span ON span.ts_code = data.ts_code
            WHERE span.universe_key = %s AND span.eligible_start <= %s AND span.eligible_end >= %s
              AND data.trade_date >= %s AND data.trade_date <= %s
            ORDER BY data.trade_date, data.ts_code
        ) AS row_data
        """,
    ),
    (
        "cyq_perf",
        """
        SELECT to_jsonb(row_data) AS payload FROM (
            SELECT data.trade_date, data.ts_code, data.his_low, data.his_high, data.cost_5pct,
                   data.cost_15pct, data.cost_50pct, data.cost_85pct, data.cost_95pct,
                   data.weight_avg, data.winner_rate
            FROM market.cyq_perf AS data
            JOIN market.stock_universe_pit_spans AS span ON span.ts_code = data.ts_code
            WHERE span.universe_key = %s AND span.eligible_start <= %s AND span.eligible_end >= %s
              AND data.trade_date >= %s AND data.trade_date <= %s
            ORDER BY data.trade_date, data.ts_code
        ) AS row_data
        """,
    ),
    (
        "sector_data",
        """
        SELECT to_jsonb(row_data) AS payload FROM (
            SELECT data.trade_date, data.ts_code, data.sw2_open, data.sw2_high, data.sw2_low,
                   data.sw2_close, data.sw2_pct_change, data.sw2_vol, data.sw2_amount,
                   data.sw2_pe, data.sw2_pb, data.sw2_total_mv, data.sw2_mf_net_amt,
                   data.sw2_mf_net_vol, data.sw2_mf_buy_elg_amt, data.sw2_mf_buy_elg_vol,
                   data.sw2_mf_sell_elg_amt, data.sw2_mf_sell_elg_vol, data.sw2_mf_buy_lg_amt,
                   data.sw2_mf_sell_lg_amt, data.sw2_mf_buy_md_amt, data.sw2_mf_sell_md_amt,
                   data.sw2_mf_buy_sm_amt, data.sw2_mf_sell_sm_amt
            FROM market.sector_data AS data
            JOIN market.stock_universe_pit_spans AS span ON span.ts_code = data.ts_code
            WHERE span.universe_key = %s AND span.eligible_start <= %s AND span.eligible_end >= %s
              AND data.trade_date >= %s AND data.trade_date <= %s
            ORDER BY data.trade_date, data.ts_code
        ) AS row_data
        """,
    ),
)


class PostgresHistoricalRangeCatalogExecutor:
    """Resolve one bounded chunk concurrently against one exported read-only snapshot.

    Only the at-most-32 member results live in memory. Existing content-addressed
    checkpoints provide the durable file cache between chunks; raw database rows are
    deliberately not reused across snapshots because VERIFY must detect source drift.
    """

    def __init__(
        self,
        *,
        conn_factory: ConnFactory,
        planner: HistoricalRangeCatalogPlanner | None = None,
        max_workers: int = _DEFAULT_CATALOG_WORKERS,
        stream_fetch_size: int = _DEFAULT_STREAM_FETCH_SIZE,
    ) -> None:
        if conn_factory is None:
            raise ValueError("conn_factory is required")
        if not 1 <= max_workers <= _MAX_CATALOG_WORKERS:
            raise ValueError(f"max_workers must be between 1 and {_MAX_CATALOG_WORKERS}")
        if not 1 <= stream_fetch_size <= _MAX_STREAM_FETCH_SIZE:
            raise ValueError(f"stream_fetch_size must be between 1 and {_MAX_STREAM_FETCH_SIZE}")
        self._conn_factory = conn_factory
        self._planner = planner or HistoricalRangeCatalogPlanner()
        self._max_workers = max_workers
        self._stream_fetch_size = stream_fetch_size

    def resolve_chunk(
        self,
        *,
        plan: HistoricalRangeSourceRequirementPlanV1,
        catalog_generation: int,
        phase: HistoricalRangeCatalogPhase,
        start_ordinal: int,
        resolved_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        expected_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1] | None = None,
        previous_checkpoint_ref: HistoricalRangeArtifactRefV1 | None = None,
        previous_checkpoint: HistoricalRangeSourceCatalogCheckpointV1 | None = None,
        chunk_size: int = 32,
    ) -> HistoricalRangeCatalogChunkResult:
        if not 1 <= chunk_size <= 32:
            raise ValueError("catalog chunk_size must be between 1 and 32")
        if not 1 <= start_ordinal <= len(plan.requirements):
            raise ValueError("start_ordinal must reference a planned requirement")
        if phase is HistoricalRangeCatalogPhase.VERIFY and expected_members is None:
            raise ValueError("VERIFY requires the complete DISCOVER member set")
        if phase is HistoricalRangeCatalogPhase.DISCOVER and expected_members is not None:
            raise ValueError("DISCOVER cannot accept a pre-resolved verification set")
        if catalog_generation < 1:
            raise ValueError("catalog_generation must be positive")
        self._planner._validate_previous_checkpoint(
            plan=plan,
            catalog_generation=catalog_generation,
            phase=phase,
            start_ordinal=start_ordinal,
            resolved_members=dict(resolved_members),
            previous_checkpoint_ref=previous_checkpoint_ref,
            previous_checkpoint=previous_checkpoint,
        )
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT transaction_timestamp() AS observed_at, pg_export_snapshot() AS snapshot_id"
                )
                snapshot = cur.fetchone()
                cached = self._resolve_chunk_requirements(
                    plan=plan,
                    phase=phase,
                    start_ordinal=start_ordinal,
                    chunk_size=chunk_size,
                    observed_at=snapshot["observed_at"],
                    snapshot_id=str(snapshot["snapshot_id"]),
                    resolved_members=resolved_members,
                    expected_members=expected_members,
                )
                result = self._planner.resolve_chunk(
                    plan=plan,
                    catalog_generation=catalog_generation,
                    phase=phase,
                    start_ordinal=start_ordinal,
                    resolver=_CachedRequirementResolver(cached),
                    resolved_members=resolved_members,
                    expected_members=expected_members,
                    previous_checkpoint_ref=previous_checkpoint_ref,
                    previous_checkpoint=previous_checkpoint,
                    chunk_size=chunk_size,
                )
            conn.rollback()
            return result

    def _resolve_chunk_requirements(
        self,
        *,
        plan: HistoricalRangeSourceRequirementPlanV1,
        phase: HistoricalRangeCatalogPhase,
        start_ordinal: int,
        chunk_size: int,
        observed_at: datetime,
        snapshot_id: str,
        resolved_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        expected_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1] | None,
    ) -> dict[str, HistoricalRangeSourceRevisionMemberV1 | Exception]:
        ordinal_end = min(len(plan.requirements), start_ordinal + chunk_size - 1)
        pending = list(plan.requirements[start_ordinal - 1 : ordinal_end])
        dependency_source = dict(expected_members or resolved_members)
        cached: dict[str, HistoricalRangeSourceRevisionMemberV1 | Exception] = {}
        with ThreadPoolExecutor(
            max_workers=min(self._max_workers, len(pending)),
            thread_name_prefix="advisory-catalog",
        ) as executor:
            while pending:
                ready = [
                    requirement
                    for requirement in pending
                    if all(
                        dependency in dependency_source
                        for dependency in requirement.depends_on_requirement_ids
                    )
                ]
                if not ready:
                    break
                worker_count = min(self._max_workers, len(ready))
                batches: list[list[HistoricalRangeSourceRequirementV1]] = [
                    [] for _ in range(worker_count)
                ]
                batch_weights = [0 for _ in range(worker_count)]
                for requirement in sorted(ready, key=_requirement_work_weight, reverse=True):
                    target = min(range(worker_count), key=batch_weights.__getitem__)
                    batches[target].append(requirement)
                    batch_weights[target] += _requirement_work_weight(requirement)
                futures = [
                    (
                        tuple(batch),
                        executor.submit(
                            self._resolve_requirement_batch,
                            requirements=tuple(batch),
                            dependency_source=dependency_source,
                            phase=phase,
                            expected_members=expected_members or {},
                            observed_at=observed_at,
                            snapshot_id=snapshot_id,
                        ),
                    )
                    for batch in batches
                    if batch
                ]
                for batch, future in futures:
                    try:
                        batch_result = future.result()
                    except Exception as exc:
                        batch_result = {
                            requirement.requirement_id: exc for requirement in batch
                        }
                    cached.update(batch_result)
                    dependency_source.update(
                        {
                            requirement_id: value
                            for requirement_id, value in batch_result.items()
                            if isinstance(value, HistoricalRangeSourceRevisionMemberV1)
                        }
                    )
                ready_ids = {requirement.requirement_id for requirement in ready}
                pending = [
                    requirement
                    for requirement in pending
                    if requirement.requirement_id not in ready_ids
                ]
        return cached

    def _resolve_requirement_batch(
        self,
        *,
        requirements: tuple[HistoricalRangeSourceRequirementV1, ...],
        dependency_source: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        phase: HistoricalRangeCatalogPhase,
        expected_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        observed_at: datetime,
        snapshot_id: str,
    ) -> dict[str, HistoricalRangeSourceRevisionMemberV1 | Exception]:
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_id,))
                    resolver = _PostgresRequirementResolver(
                        cur=cur,
                        conn=conn,
                        observed_at=observed_at,
                        stream_fetch_size=self._stream_fetch_size,
                    )
                    results: dict[str, HistoricalRangeSourceRevisionMemberV1 | Exception] = {}
                    for requirement in requirements:
                        try:
                            results[requirement.requirement_id] = resolver.resolve(
                                requirement=requirement,
                                dependency_members={
                                    dependency: dependency_source[dependency]
                                    for dependency in requirement.depends_on_requirement_ids
                                },
                                phase=phase,
                                expected_member=expected_members.get(requirement.requirement_id),
                            )
                        except HistoricalRangeSourceInputUnavailable as exc:
                            exc.__traceback__ = None
                            exc.__context__ = None
                            exc.__cause__ = None
                            results[requirement.requirement_id] = exc
                        except Exception as exc:  # retain the original stack for diagnosis
                            results[requirement.requirement_id] = exc
                    return results
            finally:
                conn.rollback()


class _CachedRequirementResolver(HistoricalRangeSourceRequirementResolver):
    def __init__(
        self,
        cached: Mapping[str, HistoricalRangeSourceRevisionMemberV1 | Exception],
    ) -> None:
        self._cached = dict(cached)

    def resolve(
        self,
        *,
        requirement: HistoricalRangeSourceRequirementV1,
        dependency_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        phase: HistoricalRangeCatalogPhase,
        expected_member: HistoricalRangeSourceRevisionMemberV1 | None,
    ) -> HistoricalRangeSourceRevisionMemberV1:
        del dependency_members, phase, expected_member
        if requirement.requirement_id not in self._cached:
            raise RuntimeError(
                f"catalog requirement was not prefetched: {requirement.requirement_id}"
            )
        value = self._cached[requirement.requirement_id]
        if isinstance(value, Exception):
            raise value
        return value


def _requirement_work_weight(requirement: HistoricalRangeSourceRequirementV1) -> int:
    if requirement.query_template_id == "historical_fundamental_moneyflow_window":
        query_weight = 5
    elif requirement.query_template_id == "historical_market_history_window":
        query_weight = 2
    else:
        return 1
    parameters = requirement.parameter_template
    start_raw = parameters.get("start_date") or parameters.get("range_start")
    end_raw = parameters.get("trade_date")
    try:
        span_days = (date.fromisoformat(str(end_raw)) - date.fromisoformat(str(start_raw))).days + 1
    except (TypeError, ValueError):
        return query_weight
    return query_weight * max(span_days, 1)


class PostgresHistoricalRangeSourceRevisionVerifier:
    """Re-read sealed Program/day partitions and reject any catalog drift."""

    def __init__(self, *, conn_factory: ConnFactory) -> None:
        if conn_factory is None:
            raise ValueError("conn_factory is required")
        self._conn_factory = conn_factory

    def verify_program_day(
        self,
        *,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        research_program_id: str,
        package_id: str,
        component_ids: set[str],
        decision_trade_date: date,
        source_roles: frozenset[str] | None = None,
    ) -> tuple[HistoricalRangeSourceRevisionRefV1, ...]:
        selected = tuple(
            member
            for member in catalog.members
            if member.decision_trade_date in {None, decision_trade_date}
            and member.package_id in {None, package_id}
            and member.component_id in {None, *component_ids}
            and _member_matches_research_program(member, research_program_id=research_program_id)
            and (source_roles is None or member.source_role in source_roles)
        )
        if not selected:
            raise HistoricalRangeContractError(
                REASON_SOURCE_REVISION_MISMATCH,
                "sealed source catalog has no Program/day members",
                context={"package_id": package_id, "decision_trade_date": decision_trade_date.isoformat()},
            )
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT transaction_timestamp() AS observed_at")
                observed_at = cur.fetchone()["observed_at"]
                resolver = _PostgresRequirementResolver(cur=cur, conn=conn, observed_at=observed_at)
                for expected in selected:
                    if expected.bound_parameters is None:
                        raise HistoricalRangeContractError(
                            REASON_SOURCE_REVISION_MISMATCH,
                            "sealed source member lacks executable bound parameters",
                            context={"revision_id": expected.revision_id, "source_role": expected.source_role},
                        )
                    requirement = HistoricalRangeSourceRequirementV1(
                        requirement_id=expected.requirement_id,
                        source_role=expected.source_role,
                        dataset_id=expected.dataset_id,
                        query_template_id=expected.query_template_id,
                        query_template_version=expected.query_template_version,
                        query_template_hash=expected.query_template_hash,
                        parameter_template=expected.bound_parameters,
                        parameter_template_hash=expected.parameter_hash,
                        partition_ref_template=expected.partition_ref,
                        package_id=expected.package_id,
                        component_id=expected.component_id,
                        decision_trade_date=expected.decision_trade_date,
                        required_for=HistoricalRangeRequirementPurpose.DAY_EXECUTION,
                        missing_reason_code=REASON_SOURCE_REVISION_MISMATCH,
                    )
                    actual = resolver.resolve(
                        requirement=requirement,
                        dependency_members={},
                        phase=HistoricalRangeCatalogPhase.VERIFY,
                        expected_member=expected,
                    )
                    if actual.revision_hash != expected.revision_hash:
                        raise HistoricalRangeContractError(
                            REASON_SOURCE_REVISION_MISMATCH,
                            "historical source partition changed after request seal",
                            context={
                                "revision_id": expected.revision_id,
                                "source_role": expected.source_role,
                                "expected_revision_hash": expected.revision_hash,
                                "actual_revision_hash": actual.revision_hash,
                            },
                        )
            conn.rollback()
        return tuple(
            HistoricalRangeSourceRevisionRefV1(
                revision_id=str(member.revision_id),
                revision_hash=str(member.revision_hash),
            )
            for member in selected
        )


class _PostgresRequirementResolver(HistoricalRangeSourceRequirementResolver):
    def __init__(
        self,
        *,
        cur: Any,
        observed_at: datetime,
        conn: Any | None = None,
        stream_fetch_size: int = _DEFAULT_STREAM_FETCH_SIZE,
    ) -> None:
        if not 1 <= stream_fetch_size <= _MAX_STREAM_FETCH_SIZE:
            raise ValueError(f"stream_fetch_size must be between 1 and {_MAX_STREAM_FETCH_SIZE}")
        self._cur = cur
        self._conn = conn
        self._observed_at = observed_at.astimezone(UTC)
        self._stream_fetch_size = stream_fetch_size
        if isinstance(conn, psycopg2.extensions.connection):
            psycopg2.extras.register_default_jsonb(conn, loads=orjson.loads)

    def resolve(
        self,
        *,
        requirement: HistoricalRangeSourceRequirementV1,
        dependency_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        phase: HistoricalRangeCatalogPhase,
        expected_member: HistoricalRangeSourceRevisionMemberV1 | None,
    ) -> HistoricalRangeSourceRevisionMemberV1:
        del phase
        bound_parameters = _bind_requirement_parameters(
            requirement=requirement,
            dependency_members=dependency_members,
        )
        partition_ref = _bound_partition_ref(
            requirement.partition_ref_template,
            bound_parameters=bound_parameters,
            has_dependencies=bool(requirement.depends_on_requirement_ids),
        )
        if requirement.query_template_id == "historical_hmm_frozen_evidence_bundle":
            return self._hmm_frozen_evidence_member(
                requirement=requirement,
                bound_parameters=bound_parameters,
                partition_ref=partition_ref,
                expected_member=expected_member,
            )
        formal = self._formal_event(requirement, bound_parameters=bound_parameters, partition_ref=partition_ref)
        if formal is not None:
            return formal
        row_count, content_hash, schema_fingerprint = self._retrospective_content(
            requirement,
            parameters=bound_parameters,
        )
        if row_count == 0 and requirement.source_role not in {"suspend", "st_risk"}:
            raise HistoricalRangeSourceInputUnavailable(
                requirement.missing_reason_code,
                "historical source requirement returned no rows",
                context={"requirement_id": requirement.requirement_id, "source_role": requirement.source_role},
            )
        admissibility = (
            HistoricalRangeRevisionAdmissibility.FROZEN_ARTIFACT
            if requirement.query_template_id == "frozen_artifact_identity"
            else HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH
        )
        return HistoricalRangeSourceRevisionMemberV1(
            requirement_id=requirement.requirement_id,
            source_role=requirement.source_role,
            dataset_id=requirement.dataset_id,
            partition_ref=partition_ref,
            package_id=requirement.package_id,
            component_id=requirement.component_id,
            decision_trade_date=requirement.decision_trade_date,
            query_template_id=requirement.query_template_id,
            query_template_version=requirement.query_template_version,
            query_template_hash=requirement.query_template_hash,
            bound_parameters=bound_parameters,
            parameter_hash=canonical_json_sha256(bound_parameters),
            schema_fingerprint=schema_fingerprint,
            row_count=row_count,
            content_hash=content_hash,
            admissibility=admissibility,
            observed_at=self._observed_at,
        )

    def _hmm_frozen_evidence_member(
        self,
        *,
        requirement: HistoricalRangeSourceRequirementV1,
        bound_parameters: Mapping[str, Any],
        partition_ref: str,
        expected_member: HistoricalRangeSourceRevisionMemberV1 | None,
    ) -> HistoricalRangeSourceRevisionMemberV1:
        if requirement.decision_trade_date is None:
            raise ValueError("historical HMM evidence requirement requires decision_trade_date")
        selector = bound_parameters.get("selector")
        if not isinstance(selector, Mapping) or not selector:
            raise ValueError("historical HMM evidence requirement requires a selector")
        metadata = bound_parameters.get("phase0a_hmm_metadata")
        event_row: Mapping[str, Any] | None = None
        require_formal_event = (
            expected_member is not None
            and expected_member.admissibility is HistoricalRangeRevisionAdmissibility.FORMAL_EVENT
        )
        if require_formal_event or not isinstance(metadata, Mapping) or not metadata:
            formal_selector = bound_parameters.get("formal_partition_selector")
            if not isinstance(formal_selector, Mapping) or not formal_selector:
                raise ValueError("historical HMM evidence requirement requires a formal partition selector")
            self._cur.execute(
                """
                SELECT partition_key, event_content_hash, schema_fingerprint, row_count,
                       partition_content_hash, first_observed_at, event_type, quality_status
                FROM app.advisory_source_availability_event
                WHERE dataset_name = %s
                  AND source_role = %s
                  AND partition_key @> %s::jsonb
                ORDER BY formal_available_at DESC, event_revision_no DESC, event_content_hash DESC
                LIMIT 1
                """,
                (
                    requirement.dataset_id,
                    requirement.source_role,
                    psycopg2.extras.Json(dict(formal_selector)),
                ),
            )
            event_row = self._cur.fetchone()
            if event_row is None:
                raise HistoricalRangeSourceInputUnavailable(
                    requirement.missing_reason_code,
                    "explicit historical HMM evidence bundle is unavailable",
                    context={
                        "requirement_id": requirement.requirement_id,
                        "package_id": requirement.package_id,
                        "decision_trade_date": requirement.decision_trade_date.isoformat(),
                    },
                )
            if str(event_row["event_type"]) == "INVALIDATED" or str(event_row["quality_status"]) != "PASS":
                raise HistoricalRangeSourceInputUnavailable(
                    requirement.missing_reason_code,
                    "latest historical HMM evidence event is not usable",
                    context={
                        "requirement_id": requirement.requirement_id,
                        "event_type": event_row["event_type"],
                        "quality_status": event_row["quality_status"],
                    },
                )
            partition_key = event_row["partition_key"]
            if not isinstance(partition_key, Mapping):
                raise HistoricalRangeSourceInputUnavailable(
                    requirement.missing_reason_code,
                    "historical HMM evidence event has no structured partition key",
                    context={"requirement_id": requirement.requirement_id},
                )
            metadata = partition_key.get("phase0a_hmm_metadata")
        if not isinstance(metadata, Mapping):
            raise HistoricalRangeSourceInputUnavailable(
                requirement.missing_reason_code,
                "historical HMM evidence bundle has no Phase 0A metadata",
                context={"requirement_id": requirement.requirement_id},
            )
        try:
            normalized_metadata = normalize_hmm_binding_metadata(
                dict(metadata),
                decision_trade_date=requirement.decision_trade_date,
            )
        except (TypeError, ValueError) as exc:
            raise HistoricalRangeSourceInputUnavailable(
                requirement.missing_reason_code,
                "historical HMM evidence bundle is invalid",
                context={"requirement_id": requirement.requirement_id, "error": str(exc)},
            ) from exc
        for key in ("model_config_id", "model_snapshot_id", "signal_preset"):
            expected = selector.get(key)
            actual = normalized_metadata.get(key)
            if expected is not None and str(actual or "") != str(expected):
                raise HistoricalRangeSourceInputUnavailable(
                    requirement.missing_reason_code,
                    "historical HMM evidence differs from the frozen selector",
                    context={
                        "requirement_id": requirement.requirement_id,
                        "field": key,
                        "expected": expected,
                        "actual": actual,
                    },
                )
        content_hash = canonical_json_sha256(normalized_metadata)
        parameters = dict(bound_parameters)
        parameters["phase0a_hmm_metadata"] = normalized_metadata
        if event_row is not None:
            if int(event_row["row_count"]) != 1 or str(event_row["partition_content_hash"]) != content_hash:
                raise HistoricalRangeSourceInputUnavailable(
                    requirement.missing_reason_code,
                    "historical HMM evidence event does not close its exact metadata",
                    context={"requirement_id": requirement.requirement_id},
                )
            availability_event_hash = str(event_row["event_content_hash"])
            schema_fingerprint = _normalize_optional_sha256(event_row["schema_fingerprint"])
            admissibility = HistoricalRangeRevisionAdmissibility.FORMAL_EVENT
            observed_at = event_row["first_observed_at"]
        else:
            availability_event_hash = None
            schema_fingerprint = canonical_json_sha256({"fields": sorted(normalized_metadata)})
            admissibility = HistoricalRangeRevisionAdmissibility.FROZEN_ARTIFACT
            observed_at = self._observed_at
        return HistoricalRangeSourceRevisionMemberV1(
            requirement_id=requirement.requirement_id,
            source_role=requirement.source_role,
            dataset_id=requirement.dataset_id,
            partition_ref=partition_ref,
            package_id=requirement.package_id,
            component_id=requirement.component_id,
            decision_trade_date=requirement.decision_trade_date,
            query_template_id=requirement.query_template_id,
            query_template_version=requirement.query_template_version,
            query_template_hash=requirement.query_template_hash,
            bound_parameters=parameters,
            parameter_hash=canonical_json_sha256(parameters),
            schema_fingerprint=schema_fingerprint,
            row_count=1,
            content_hash=content_hash,
            availability_event_hash=availability_event_hash,
            admissibility=admissibility,
            observed_at=observed_at,
        )

    def _formal_event(
        self,
        requirement: HistoricalRangeSourceRequirementV1,
        *,
        bound_parameters: Mapping[str, Any],
        partition_ref: str,
    ) -> HistoricalRangeSourceRevisionMemberV1 | None:
        partition_key = bound_parameters.get("formal_partition_key")
        if not isinstance(partition_key, Mapping) or not partition_key:
            return None
        partition_key_hash = canonical_json_sha256(dict(partition_key))
        self._cur.execute(
            """
            SELECT event_content_hash, schema_fingerprint, row_count, partition_content_hash,
                   first_observed_at, event_type, quality_status
            FROM app.advisory_source_availability_event
            WHERE dataset_name = %s AND source_role = %s AND partition_key_hash = %s
            ORDER BY formal_available_at DESC, event_revision_no DESC
            LIMIT 1
            """,
            (requirement.dataset_id, requirement.source_role, partition_key_hash),
        )
        row = self._cur.fetchone()
        if row is None:
            return None
        if str(row["event_type"]) == "INVALIDATED" or str(row["quality_status"]) != "PASS":
            raise HistoricalRangeSourceInputUnavailable(
                requirement.missing_reason_code,
                "latest formal source availability event is not usable",
                context={
                    "requirement_id": requirement.requirement_id,
                    "event_type": row["event_type"],
                    "quality_status": row["quality_status"],
                },
            )
        return HistoricalRangeSourceRevisionMemberV1(
            requirement_id=requirement.requirement_id,
            source_role=requirement.source_role,
            dataset_id=requirement.dataset_id,
            partition_ref=partition_ref,
            package_id=requirement.package_id,
            component_id=requirement.component_id,
            decision_trade_date=requirement.decision_trade_date,
            query_template_id=requirement.query_template_id,
            query_template_version=requirement.query_template_version,
            query_template_hash=requirement.query_template_hash,
            bound_parameters=dict(bound_parameters),
            parameter_hash=canonical_json_sha256(dict(bound_parameters)),
            schema_fingerprint=_normalize_optional_sha256(row["schema_fingerprint"]),
            row_count=int(row["row_count"]),
            content_hash=str(row["partition_content_hash"]),
            availability_event_hash=str(row["event_content_hash"]),
            admissibility=HistoricalRangeRevisionAdmissibility.FORMAL_EVENT,
            observed_at=row["first_observed_at"],
        )

    def _retrospective_content(
        self,
        requirement: HistoricalRangeSourceRequirementV1,
        *,
        parameters: Mapping[str, Any],
    ) -> tuple[int, str, str]:
        query_id = requirement.query_template_id
        if query_id == "frozen_artifact_identity":
            content_hash = str(parameters.get("content_hash") or "").strip().lower()
            if len(content_hash) != 64 or any(character not in "0123456789abcdef" for character in content_hash):
                raise HistoricalRangeSourceInputUnavailable(
                    requirement.missing_reason_code,
                    "frozen historical input has no exact content hash",
                    context={"requirement_id": requirement.requirement_id, "source_role": requirement.source_role},
                )
            return (
                int(parameters["row_count"]),
                content_hash,
                canonical_json_sha256({"fields": sorted(parameters)}),
            )
        trade_date = date.fromisoformat(str(parameters["trade_date"]))
        universe_key = str(parameters.get("universe_key") or "shsz_st_pit_active_v1")
        if query_id == "historical_pit_universe_existing_readonly":
            return self._stream_universe(_UNIVERSE_SQL, (universe_key, trade_date, trade_date))
        if query_id == "historical_st_risk_existing_readonly":
            return self._stream_universe(_UNIVERSE_SQL, (universe_key, trade_date, trade_date))
        if query_id == "historical_trading_calendar_window":
            start = date.fromisoformat(str(parameters["range_start"]))
            return self._stream_query(_CALENDAR_SQL, (start, trade_date))
        if query_id == "historical_market_history_window":
            start = date.fromisoformat(str(parameters["start_date"]))
            return self._stream_query(
                _MARKET_HISTORY_SQL,
                (universe_key, trade_date, trade_date, start, trade_date),
                required_non_null=("adj_factor",),
            )
        if query_id == "historical_decision_mark_daily_market":
            return self._stream_query(_DECISION_MARK_DAILY_MARKET_SQL, (trade_date,))
        if query_id == "historical_decision_mark_market_state":
            return self._stream_query(
                _DECISION_MARK_MARKET_STATE_SQL,
                (trade_date, trade_date, trade_date, trade_date),
            )
        if query_id == "historical_fundamental_moneyflow_window":
            start = date.fromisoformat(str(parameters["start_date"]))
            return self._stream_composite(
                _FUNDAMENTAL_QUERIES,
                (universe_key, trade_date, trade_date, start, trade_date),
            )
        if query_id == "historical_suspend_lookup":
            return self._stream_query(_SUSPEND_SQL, (trade_date,))
        if query_id == "historical_industry_membership":
            return self._stream_query(_INDUSTRY_SQL, (trade_date, trade_date))
        raise ValueError(f"unsupported historical catalog query template: {query_id}")

    def _stream_universe(self, sql: str, params: tuple[Any, ...]) -> tuple[int, str, str]:
        with self._open_stream_cursor(sql, params) as stream_cur:
            rows = stream_cur.fetchmany(self._stream_fetch_size)
            schema_fingerprint = self._schema_fingerprint(stream_cur)
            symbols: list[str] = []
            while rows:
                for row in rows:
                    payload = row["payload"]
                    symbol = str(payload.get("ts_code") or "").strip()
                    if not symbol:
                        raise HistoricalRangeSourceInputUnavailable(
                            "ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
                            "historical PIT universe row lacks ts_code",
                        )
                    symbols.append(symbol)
                rows = stream_cur.fetchmany(self._stream_fetch_size)
        if len(symbols) != len(set(symbols)):
            raise HistoricalRangeSourceInputUnavailable(
                "ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
                "historical PIT universe contains duplicate symbols",
            )
        ordered = sorted(symbols)
        return len(ordered), canonical_json_sha256(ordered), schema_fingerprint

    def _stream_composite(
        self,
        queries: tuple[tuple[str, str], ...],
        params: tuple[Any, ...],
    ) -> tuple[int, str, str]:
        digest = hashlib.sha256()
        total = 0
        schemas: list[dict[str, Any]] = []
        for dataset_name, sql in queries:
            count, content_hash, schema_hash = self._stream_query(sql, params)
            total += count
            marker = _canonical_bytes({"dataset_name": dataset_name, "content_hash": content_hash, "row_count": count})
            digest.update(len(marker).to_bytes(8, "big"))
            digest.update(marker)
            schemas.append({"dataset_name": dataset_name, "schema_hash": schema_hash})
        return total, digest.hexdigest(), canonical_json_sha256(schemas)

    def _stream_query(
        self,
        sql: str,
        params: tuple[Any, ...],
        *,
        required_non_null: tuple[str, ...] = (),
    ) -> tuple[int, str, str]:
        with self._open_stream_cursor(sql, params) as stream_cur:
            rows = stream_cur.fetchmany(self._stream_fetch_size)
            schema_fingerprint = self._schema_fingerprint(stream_cur)
            digest = hashlib.sha256()
            row_count = 0
            while rows:
                for row in rows:
                    payload = row["payload"]
                    if any(payload.get(field) is None for field in required_non_null):
                        raise HistoricalRangeSourceInputUnavailable(
                            "ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
                            "historical source row lacks a required database value",
                            context={"missing_fields": list(required_non_null)},
                        )
                    encoded = _canonical_bytes(payload)
                    digest.update(len(encoded).to_bytes(8, "big"))
                    digest.update(encoded)
                    row_count += 1
                rows = stream_cur.fetchmany(self._stream_fetch_size)
        return row_count, digest.hexdigest(), schema_fingerprint

    def _open_stream_cursor(self, sql: str, params: tuple[Any, ...]) -> Any:
        if self._conn is None:
            raise RuntimeError("streaming historical catalog queries require an explicit PostgreSQL connection")
        stream_cur = self._conn.cursor(
            name=f"ahr_catalog_{uuid.uuid4().hex}",
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        stream_cur.itersize = self._stream_fetch_size
        stream_cur.execute(sql, params)
        return stream_cur

    @staticmethod
    def _schema_fingerprint(cur: Any) -> str:
        return canonical_json_sha256(
            [
                {"name": str(item.name), "type_code": int(item.type_code)}
                for item in cur.description
            ]
        )


def _canonical_bytes(value: Any) -> bytes:
    # Query payloads originate as PostgreSQL JSONB, so non-finite numbers are impossible.
    # CPython and orjson differ only on exponent spelling for this domain; preserve the
    # frozen hash contract by falling back whenever an exponent token is present.
    encoded = orjson.dumps(
        value,
        option=orjson.OPT_SORT_KEYS | orjson.OPT_PASSTHROUGH_DATETIME,
        default=str,
    )
    if _ORJSON_EXPONENT_NUMBER.search(encoded) is None:
        return encoded
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False).encode("utf-8")


def _member_matches_research_program(
    member: HistoricalRangeSourceRevisionMemberV1,
    *,
    research_program_id: str,
) -> bool:
    if member.source_role != "hmm_frozen_evidence":
        return True
    parameters = member.bound_parameters or {}
    selector = parameters.get("selector")
    return isinstance(selector, Mapping) and str(selector.get("research_program_id") or "") == research_program_id


def _bind_requirement_parameters(
    *,
    requirement: HistoricalRangeSourceRequirementV1,
    dependency_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
) -> dict[str, Any]:
    parameters = dict(requirement.parameter_template)
    existing = parameters.get("dependency_revision_refs")
    if existing is not None:
        if dependency_members:
            raise ValueError("pre-bound source requirement cannot accept dependency members again")
        return parameters
    if not requirement.depends_on_requirement_ids:
        return parameters
    if set(dependency_members) != set(requirement.depends_on_requirement_ids):
        raise ValueError("source requirement dependency members do not exactly cover its dependency graph")
    parameters["dependency_revision_refs"] = [
        {
            "requirement_id": requirement_id,
            "revision_id": dependency_members[requirement_id].revision_id,
            "revision_hash": dependency_members[requirement_id].revision_hash,
        }
        for requirement_id in requirement.depends_on_requirement_ids
    ]
    return parameters


def _bound_partition_ref(
    template: str,
    *,
    bound_parameters: Mapping[str, Any],
    has_dependencies: bool,
) -> str:
    if "|deps:" in template or not has_dependencies:
        return template
    dependency_hash = canonical_json_sha256(bound_parameters.get("dependency_revision_refs"))
    return f"{template}|deps:{dependency_hash[:24]}"


def _normalize_optional_sha256(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if len(text) == 64 and all(character in "0123456789abcdef" for character in text):
        return text
    return canonical_json_sha256({"schema_fingerprint": text})
