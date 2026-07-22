"""Fail-closed repair of incomplete ``market.sector_data`` source facts.

The repair reconstructs persisted SW2 market and money-flow facts from the
same point-in-time membership sources used by :mod:`sector_data_builder`.
It never repairs a safe subset: every incomplete row, every relevant member,
and every supplied ambiguity resolution must pass the audit before one row is
updated.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Sequence

from ..db.pg_pool import get_conn


class SectorDataRepairContractError(RuntimeError):
    """Raised when source facts cannot support an atomic repair."""


class SectorDataRepairTargetError(RuntimeError):
    """Raised before mutation when the connected database is not the target."""


@dataclass(frozen=True)
class MappingResolution:
    """Exact evidence selecting one candidate from an ambiguous PIT mapping."""

    trade_date: dt.date
    ts_code: str
    l1_code: str
    l2_code: str
    mapping_in_date: dt.date

    def as_json(self) -> Dict[str, str]:
        return {
            "trade_date": self.trade_date.isoformat(),
            "ts_code": self.ts_code,
            "l1_code": self.l1_code,
            "l2_code": self.l2_code,
            "mapping_in_date": self.mapping_in_date.isoformat(),
        }


@dataclass(frozen=True)
class RepairAudit:
    """Complete audit result for the current transaction snapshot."""

    incomplete_rows: int
    repairable_rows: int
    blocker_count: int
    status_counts: Mapping[str, int] = field(default_factory=dict)
    samples: Mapping[str, Sequence[Mapping[str, Any]]] = field(default_factory=dict)

    @property
    def can_apply(self) -> bool:
        return self.blocker_count == 0 and self.repairable_rows == self.incomplete_rows


@dataclass(frozen=True)
class RepairResult:
    """Mutation and independent readback receipt."""

    audit: RepairAudit
    updated_rows: int
    readback_incomplete_rows: int
    readback_source_mismatches: int


_TARGET_INCOMPLETE_PREDICATE = """\
target.sw2_open IS NULL
 OR target.sw2_high IS NULL
 OR target.sw2_low IS NULL
 OR target.sw2_close IS NULL
 OR target.sw2_pct_change IS NULL
 OR target.sw2_vol IS NULL
 OR target.sw2_amount IS NULL
 OR target.sw2_pe IS NULL
 OR target.sw2_pb IS NULL
 OR target.sw2_total_mv IS NULL
 OR target.sw2_mf_buy_sm_amt IS NULL
 OR target.sw2_mf_sell_sm_amt IS NULL
 OR target.sw2_mf_buy_md_amt IS NULL
 OR target.sw2_mf_sell_md_amt IS NULL
 OR target.sw2_mf_buy_lg_amt IS NULL
 OR target.sw2_mf_sell_lg_amt IS NULL
 OR target.sw2_mf_buy_elg_amt IS NULL
 OR target.sw2_mf_sell_elg_amt IS NULL
 OR target.sw2_mf_net_amt IS NULL
 OR target.sw2_mf_buy_elg_vol IS NULL
 OR target.sw2_mf_sell_elg_vol IS NULL
 OR target.sw2_mf_net_vol IS NULL
"""


def _source_ctes(target_rows_sql: str) -> str:
    """Return the shared deterministic source-selection graph."""

    return f"""\
WITH input_resolutions AS (
    SELECT trade_date, ts_code, l1_code, l2_code, mapping_in_date
    FROM jsonb_to_recordset(%(resolutions)s::jsonb) AS resolution(
        trade_date DATE,
        ts_code TEXT,
        l1_code TEXT,
        l2_code TEXT,
        mapping_in_date DATE
    )
),
target_rows AS (
    {target_rows_sql}
),
target_dates AS (
    SELECT DISTINCT trade_date FROM target_rows
),
active_candidates AS (
    SELECT
        dates.trade_date,
        member.ts_code,
        member.l1_code,
        member.l2_code,
        member.in_date,
        MAX(member.in_date) OVER (
            PARTITION BY dates.trade_date, member.ts_code
        ) AS latest_in_date
    FROM target_dates AS dates
    JOIN market.sw_index_member AS member
      ON member.in_date <= dates.trade_date
     AND (member.out_date >= dates.trade_date OR member.out_date IS NULL)
),
latest_candidates AS (
    SELECT trade_date, ts_code, l1_code, l2_code, in_date
    FROM active_candidates
    WHERE in_date = latest_in_date
),
candidate_stats AS (
    SELECT
        trade_date,
        ts_code,
        COUNT(*) AS candidate_count,
        COUNT(*) FILTER (
            WHERE NULLIF(BTRIM(l1_code), '') IS NULL
               OR NULLIF(BTRIM(l2_code), '') IS NULL
               OR in_date IS NULL
        ) AS invalid_identity_count
    FROM latest_candidates
    GROUP BY trade_date, ts_code
),
resolution_matches AS (
    SELECT
        resolution.trade_date,
        resolution.ts_code,
        COUNT(candidate.*) AS match_count
    FROM input_resolutions AS resolution
    LEFT JOIN latest_candidates AS candidate
      ON candidate.trade_date = resolution.trade_date
     AND candidate.ts_code = resolution.ts_code
     AND candidate.l1_code = resolution.l1_code
     AND candidate.l2_code = resolution.l2_code
     AND candidate.in_date = resolution.mapping_in_date
    GROUP BY resolution.trade_date, resolution.ts_code
),
valid_resolutions AS (
    SELECT resolution.*
    FROM input_resolutions AS resolution
    JOIN candidate_stats AS stats
      ON stats.trade_date = resolution.trade_date
     AND stats.ts_code = resolution.ts_code
     AND stats.candidate_count > 1
    JOIN resolution_matches AS matches
      ON matches.trade_date = resolution.trade_date
     AND matches.ts_code = resolution.ts_code
     AND matches.match_count = 1
),
invalid_resolutions AS (
    SELECT resolution.trade_date, resolution.ts_code
    FROM input_resolutions AS resolution
    LEFT JOIN candidate_stats AS stats
      ON stats.trade_date = resolution.trade_date
     AND stats.ts_code = resolution.ts_code
    LEFT JOIN resolution_matches AS matches
      ON matches.trade_date = resolution.trade_date
     AND matches.ts_code = resolution.ts_code
    WHERE stats.candidate_count IS NULL
       OR stats.candidate_count <= 1
       OR COALESCE(matches.match_count, 0) <> 1
),
selected_mappings AS (
    SELECT candidate.trade_date, candidate.ts_code,
           candidate.l1_code, candidate.l2_code,
           candidate.in_date AS mapping_in_date
    FROM latest_candidates AS candidate
    JOIN candidate_stats AS stats
      ON stats.trade_date = candidate.trade_date
     AND stats.ts_code = candidate.ts_code
     AND stats.candidate_count = 1
    UNION ALL
    SELECT candidate.trade_date, candidate.ts_code,
           candidate.l1_code, candidate.l2_code,
           candidate.in_date AS mapping_in_date
    FROM latest_candidates AS candidate
    JOIN valid_resolutions AS resolution
      ON resolution.trade_date = candidate.trade_date
     AND resolution.ts_code = candidate.ts_code
     AND resolution.l1_code = candidate.l1_code
     AND resolution.l2_code = candidate.l2_code
     AND resolution.mapping_in_date = candidate.in_date
),
needed_l2 AS (
    SELECT DISTINCT mapping.trade_date, mapping.l2_code
    FROM target_rows AS target
    JOIN selected_mappings AS mapping
      ON mapping.trade_date = target.trade_date
     AND mapping.ts_code = target.ts_code
),
unresolved_l2_membership AS (
    SELECT DISTINCT candidate.trade_date, candidate.l2_code
    FROM latest_candidates AS candidate
    JOIN candidate_stats AS stats
      ON stats.trade_date = candidate.trade_date
     AND stats.ts_code = candidate.ts_code
     AND stats.candidate_count > 1
    LEFT JOIN valid_resolutions AS resolution
      ON resolution.trade_date = candidate.trade_date
     AND resolution.ts_code = candidate.ts_code
    JOIN needed_l2 AS needed
      ON needed.trade_date = candidate.trade_date
     AND needed.l2_code = candidate.l2_code
    WHERE resolution.ts_code IS NULL
),
member_moneyflow AS (
    SELECT
        mapping.trade_date,
        mapping.l2_code,
        mapping.ts_code,
        moneyflow.buy_sm_amount,
        moneyflow.sell_sm_amount,
        moneyflow.buy_md_amount,
        moneyflow.sell_md_amount,
        moneyflow.buy_lg_amount,
        moneyflow.sell_lg_amount,
        moneyflow.buy_elg_amount,
        moneyflow.sell_elg_amount,
        moneyflow.net_mf_amount,
        moneyflow.buy_elg_vol,
        moneyflow.sell_elg_vol,
        moneyflow.net_mf_vol,
        CASE WHEN moneyflow.ts_code IS NULL
               OR moneyflow.buy_sm_amount IS NULL
               OR moneyflow.sell_sm_amount IS NULL
               OR moneyflow.buy_md_amount IS NULL
               OR moneyflow.sell_md_amount IS NULL
               OR moneyflow.buy_lg_amount IS NULL
               OR moneyflow.sell_lg_amount IS NULL
               OR moneyflow.buy_elg_amount IS NULL
               OR moneyflow.sell_elg_amount IS NULL
               OR moneyflow.net_mf_amount IS NULL
               OR moneyflow.buy_elg_vol IS NULL
               OR moneyflow.sell_elg_vol IS NULL
               OR moneyflow.net_mf_vol IS NULL
             THEN 1 ELSE 0 END AS incomplete_source
    FROM selected_mappings AS mapping
    JOIN needed_l2 AS needed
      ON needed.trade_date = mapping.trade_date
     AND needed.l2_code = mapping.l2_code
    LEFT JOIN market.moneyflow_ts AS moneyflow
      ON moneyflow.trade_date = mapping.trade_date
     AND moneyflow.ts_code = mapping.ts_code
),
l2_moneyflow AS (
    SELECT
        trade_date,
        l2_code,
        SUM(incomplete_source) AS incomplete_member_count,
        SUM(buy_sm_amount) AS buy_sm_amount,
        SUM(sell_sm_amount) AS sell_sm_amount,
        SUM(buy_md_amount) AS buy_md_amount,
        SUM(sell_md_amount) AS sell_md_amount,
        SUM(buy_lg_amount) AS buy_lg_amount,
        SUM(sell_lg_amount) AS sell_lg_amount,
        SUM(buy_elg_amount) AS buy_elg_amount,
        SUM(sell_elg_amount) AS sell_elg_amount,
        SUM(net_mf_amount) AS net_mf_amount,
        SUM(buy_elg_vol) AS buy_elg_vol,
        SUM(sell_elg_vol) AS sell_elg_vol,
        SUM(net_mf_vol) AS net_mf_vol
    FROM member_moneyflow
    GROUP BY trade_date, l2_code
),
repair_sources AS (
    SELECT
        target.trade_date,
        target.ts_code,
        mapping.l1_code,
        mapping.l2_code,
        mapping.mapping_in_date,
        daily.open AS sw2_open,
        daily.high AS sw2_high,
        daily.low AS sw2_low,
        daily.close AS sw2_close,
        daily.pct_change AS sw2_pct_change,
        daily.vol AS sw2_vol,
        daily.amount AS sw2_amount,
        daily.pe AS sw2_pe,
        daily.pb AS sw2_pb,
        daily.total_mv AS sw2_total_mv,
        flow.buy_sm_amount AS sw2_mf_buy_sm_amt,
        flow.sell_sm_amount AS sw2_mf_sell_sm_amt,
        flow.buy_md_amount AS sw2_mf_buy_md_amt,
        flow.sell_md_amount AS sw2_mf_sell_md_amt,
        flow.buy_lg_amount AS sw2_mf_buy_lg_amt,
        flow.sell_lg_amount AS sw2_mf_sell_lg_amt,
        flow.buy_elg_amount AS sw2_mf_buy_elg_amt,
        flow.sell_elg_amount AS sw2_mf_sell_elg_amt,
        flow.net_mf_amount AS sw2_mf_net_amt,
        flow.buy_elg_vol AS sw2_mf_buy_elg_vol,
        flow.sell_elg_vol AS sw2_mf_sell_elg_vol,
        flow.net_mf_vol AS sw2_mf_net_vol,
        CASE
          WHEN stats.candidate_count IS NULL THEN 'no_mapping'
          WHEN stats.invalid_identity_count <> 0 THEN 'invalid_mapping_identity'
          WHEN stats.candidate_count > 1 AND mapping.ts_code IS NULL THEN 'ambiguous_mapping'
          WHEN ambiguity.l2_code IS NOT NULL THEN 'unresolved_l2_membership'
          WHEN daily.ts_code IS NULL
            OR daily.open IS NULL OR daily.high IS NULL OR daily.low IS NULL
            OR daily.close IS NULL OR daily.pct_change IS NULL
            OR daily.vol IS NULL OR daily.amount IS NULL OR daily.pe IS NULL
            OR daily.pb IS NULL OR daily.total_mv IS NULL THEN 'missing_sw_daily'
          WHEN flow.l2_code IS NULL OR flow.incomplete_member_count <> 0
            OR flow.buy_sm_amount IS NULL OR flow.sell_sm_amount IS NULL
            OR flow.buy_md_amount IS NULL OR flow.sell_md_amount IS NULL
            OR flow.buy_lg_amount IS NULL OR flow.sell_lg_amount IS NULL
            OR flow.buy_elg_amount IS NULL OR flow.sell_elg_amount IS NULL
            OR flow.net_mf_amount IS NULL OR flow.buy_elg_vol IS NULL
            OR flow.sell_elg_vol IS NULL OR flow.net_mf_vol IS NULL
            THEN 'missing_l2_moneyflow'
          ELSE 'repairable'
        END AS repair_status
    FROM target_rows AS target
    LEFT JOIN candidate_stats AS stats
      ON stats.trade_date = target.trade_date
     AND stats.ts_code = target.ts_code
    LEFT JOIN selected_mappings AS mapping
      ON mapping.trade_date = target.trade_date
     AND mapping.ts_code = target.ts_code
    LEFT JOIN unresolved_l2_membership AS ambiguity
      ON ambiguity.trade_date = mapping.trade_date
     AND ambiguity.l2_code = mapping.l2_code
    LEFT JOIN market.sw_daily AS daily
      ON daily.trade_date = mapping.trade_date
     AND daily.ts_code = mapping.l2_code
    LEFT JOIN l2_moneyflow AS flow
      ON flow.trade_date = mapping.trade_date
     AND flow.l2_code = mapping.l2_code
)
"""


_INCOMPLETE_TARGET_ROWS_SQL = f"""\
SELECT target.*
FROM market.sector_data AS target
WHERE {_TARGET_INCOMPLETE_PREDICATE}
"""

_AUDIT_SQL = _source_ctes(_INCOMPLETE_TARGET_ROWS_SQL) + """\
, audit_rows AS (
    SELECT trade_date, ts_code, l2_code, repair_status FROM repair_sources
    UNION ALL
    SELECT trade_date, ts_code, NULL::TEXT, 'invalid_resolution'
    FROM invalid_resolutions
),
status_summary AS (
    SELECT repair_status, COUNT(*)::BIGINT AS row_count
    FROM audit_rows
    GROUP BY repair_status
),
ranked_samples AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY repair_status ORDER BY trade_date, ts_code
    ) AS sample_rank
    FROM audit_rows
),
sample_groups AS (
    SELECT repair_status,
           JSONB_AGG(JSONB_BUILD_OBJECT(
               'trade_date', trade_date,
               'ts_code', ts_code,
               'l2_code', l2_code
           ) ORDER BY trade_date, ts_code) AS rows
    FROM ranked_samples
    WHERE sample_rank <= %(sample_limit)s
    GROUP BY repair_status
)
SELECT
    (SELECT COUNT(*)::BIGINT FROM target_rows) AS incomplete_rows,
    COALESCE((SELECT row_count FROM status_summary WHERE repair_status = 'repairable'), 0)
        AS repairable_rows,
    COALESCE((SELECT SUM(row_count) FROM status_summary WHERE repair_status <> 'repairable'), 0)
        AS blocker_count,
    COALESCE((SELECT JSONB_OBJECT_AGG(repair_status, row_count) FROM status_summary), '{}'::JSONB)
        AS status_counts,
    COALESCE((SELECT JSONB_OBJECT_AGG(repair_status, rows) FROM sample_groups), '{}'::JSONB)
        AS samples
"""

_LOCK_SQL = """\
LOCK TABLE market.sector_data IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE market.sw_index_member IN SHARE MODE;
LOCK TABLE market.sw_daily IN SHARE MODE;
LOCK TABLE market.moneyflow_ts IN SHARE MODE
"""

_UPDATE_SQL = _source_ctes(_INCOMPLETE_TARGET_ROWS_SQL) + """\
UPDATE market.sector_data AS target
SET sw2_open = source.sw2_open,
    sw2_high = source.sw2_high,
    sw2_low = source.sw2_low,
    sw2_close = source.sw2_close,
    sw2_pct_change = source.sw2_pct_change,
    sw2_vol = source.sw2_vol,
    sw2_amount = source.sw2_amount,
    sw2_pe = source.sw2_pe,
    sw2_pb = source.sw2_pb,
    sw2_total_mv = source.sw2_total_mv,
    sw2_mf_buy_sm_amt = source.sw2_mf_buy_sm_amt,
    sw2_mf_sell_sm_amt = source.sw2_mf_sell_sm_amt,
    sw2_mf_buy_md_amt = source.sw2_mf_buy_md_amt,
    sw2_mf_sell_md_amt = source.sw2_mf_sell_md_amt,
    sw2_mf_buy_lg_amt = source.sw2_mf_buy_lg_amt,
    sw2_mf_sell_lg_amt = source.sw2_mf_sell_lg_amt,
    sw2_mf_buy_elg_amt = source.sw2_mf_buy_elg_amt,
    sw2_mf_sell_elg_amt = source.sw2_mf_sell_elg_amt,
    sw2_mf_net_amt = source.sw2_mf_net_amt,
    sw2_mf_buy_elg_vol = source.sw2_mf_buy_elg_vol,
    sw2_mf_sell_elg_vol = source.sw2_mf_sell_elg_vol,
    sw2_mf_net_vol = source.sw2_mf_net_vol
FROM repair_sources AS source
WHERE source.repair_status = 'repairable'
  AND target.trade_date = source.trade_date
  AND target.ts_code = source.ts_code
RETURNING target.trade_date, target.ts_code
"""

_KEY_TARGET_ROWS_SQL = """\
SELECT target.*
FROM market.sector_data AS target
JOIN jsonb_to_recordset(%(updated_keys)s::jsonb) AS key(
    trade_date DATE,
    ts_code TEXT
)
  ON key.trade_date = target.trade_date
 AND key.ts_code = target.ts_code
"""

_READBACK_SQL = _source_ctes(_KEY_TARGET_ROWS_SQL) + f"""\
SELECT
    COUNT(*) FILTER (WHERE {_TARGET_INCOMPLETE_PREDICATE})::BIGINT AS incomplete_rows,
    COUNT(*) FILTER (
        WHERE source.repair_status <> 'repairable'
           OR target.sw2_open IS DISTINCT FROM source.sw2_open
           OR target.sw2_high IS DISTINCT FROM source.sw2_high
           OR target.sw2_low IS DISTINCT FROM source.sw2_low
           OR target.sw2_close IS DISTINCT FROM source.sw2_close
           OR target.sw2_pct_change IS DISTINCT FROM source.sw2_pct_change
           OR target.sw2_vol IS DISTINCT FROM source.sw2_vol
           OR target.sw2_amount IS DISTINCT FROM source.sw2_amount
           OR target.sw2_pe IS DISTINCT FROM source.sw2_pe
           OR target.sw2_pb IS DISTINCT FROM source.sw2_pb
           OR target.sw2_total_mv IS DISTINCT FROM source.sw2_total_mv
           OR target.sw2_mf_buy_sm_amt IS DISTINCT FROM source.sw2_mf_buy_sm_amt
           OR target.sw2_mf_sell_sm_amt IS DISTINCT FROM source.sw2_mf_sell_sm_amt
           OR target.sw2_mf_buy_md_amt IS DISTINCT FROM source.sw2_mf_buy_md_amt
           OR target.sw2_mf_sell_md_amt IS DISTINCT FROM source.sw2_mf_sell_md_amt
           OR target.sw2_mf_buy_lg_amt IS DISTINCT FROM source.sw2_mf_buy_lg_amt
           OR target.sw2_mf_sell_lg_amt IS DISTINCT FROM source.sw2_mf_sell_lg_amt
           OR target.sw2_mf_buy_elg_amt IS DISTINCT FROM source.sw2_mf_buy_elg_amt
           OR target.sw2_mf_sell_elg_amt IS DISTINCT FROM source.sw2_mf_sell_elg_amt
           OR target.sw2_mf_net_amt IS DISTINCT FROM source.sw2_mf_net_amt
           OR target.sw2_mf_buy_elg_vol IS DISTINCT FROM source.sw2_mf_buy_elg_vol
           OR target.sw2_mf_sell_elg_vol IS DISTINCT FROM source.sw2_mf_sell_elg_vol
           OR target.sw2_mf_net_vol IS DISTINCT FROM source.sw2_mf_net_vol
    )::BIGINT AS source_mismatches
FROM target_rows AS target
LEFT JOIN repair_sources AS source
  ON source.trade_date = target.trade_date
 AND source.ts_code = target.ts_code
"""

_DATABASE_IDENTITY_SQL = """\
SELECT current_database(), COALESCE(inet_server_addr()::TEXT, 'local'), inet_server_port()
"""


def _resolution_payload(resolutions: Iterable[MappingResolution]) -> str:
    rows: List[Dict[str, str]] = []
    seen = set()
    for resolution in resolutions:
        key = (resolution.trade_date, resolution.ts_code)
        if key in seen:
            raise SectorDataRepairContractError(
                "SECTOR_DATA_REPAIR_DUPLICATE_RESOLUTION: "
                f"trade_date={resolution.trade_date}, ts_code={resolution.ts_code}"
            )
        if not resolution.ts_code.strip() or not resolution.l1_code.strip() or not resolution.l2_code.strip():
            raise SectorDataRepairContractError(
                "SECTOR_DATA_REPAIR_EMPTY_RESOLUTION_IDENTITY: "
                f"trade_date={resolution.trade_date}, ts_code={resolution.ts_code!r}"
            )
        if resolution.mapping_in_date > resolution.trade_date:
            raise SectorDataRepairContractError(
                "SECTOR_DATA_REPAIR_FUTURE_RESOLUTION: "
                f"trade_date={resolution.trade_date}, ts_code={resolution.ts_code}, "
                f"mapping_in_date={resolution.mapping_in_date}"
            )
        seen.add(key)
        rows.append(resolution.as_json())
    return json.dumps(rows, ensure_ascii=False, sort_keys=True)


def _assert_apply_target(
    connection: Any,
    cursor: Any,
    *,
    target: Literal["dev", "production"],
    production_authorized: bool,
) -> None:
    if target == "production" and not production_authorized:
        raise SectorDataRepairTargetError("SECTOR_DATA_REPAIR_PRODUCTION_NOT_AUTHORIZED")
    cursor.execute(_DATABASE_IDENTITY_SQL)
    database, server_host, server_port = cursor.fetchone()
    dsn = connection.get_dsn_parameters()
    client_host = str(dsn.get("host") or "")
    try:
        client_port = int(dsn.get("port"))
    except (TypeError, ValueError) as error:
        raise SectorDataRepairTargetError(
            "SECTOR_DATA_REPAIR_CLIENT_DSN_INVALID"
        ) from error
    if target == "dev":
        valid = (
            client_host in {"127.0.0.1", "::1", "localhost"}
            and client_port == 5433
            and "dev" in str(database).lower()
        )
    else:
        valid = (
            client_host in {"127.0.0.1", "::1", "localhost"}
            and client_port == 5432
            and str(database).lower() == "aistock"
        )
    if not valid:
        raise SectorDataRepairTargetError(
            "SECTOR_DATA_REPAIR_TARGET_MISMATCH: "
            f"expected={target}, client={client_host}:{client_port}/{database}, "
            f"server={server_host}:{server_port}"
        )


class SectorDataRepairService:
    """Audit and atomically repair incomplete persisted sector facts."""

    def audit(
        self,
        resolutions: Sequence[MappingResolution] = (),
        *,
        connection: Optional[Any] = None,
        sample_limit: int = 20,
    ) -> RepairAudit:
        """Run a read-only audit; supplied resolutions are validated as evidence."""

        if sample_limit < 1 or sample_limit > 100:
            raise ValueError("sample_limit must be between 1 and 100")
        payload = _resolution_payload(resolutions)
        if connection is not None:
            return self._audit_connection(connection, payload, sample_limit)
        with get_conn() as conn:
            return self._audit_connection(conn, payload, sample_limit)

    def repair(
        self,
        resolutions: Sequence[MappingResolution] = (),
        *,
        target: Literal["dev", "production"],
        production_authorized: bool = False,
        connection: Optional[Any] = None,
        commit: bool = True,
        sample_limit: int = 20,
    ) -> RepairResult:
        """Repair all incomplete rows atomically or perform zero updates.

        ``commit=False`` is intended for an existing DEV transaction used by a
        rollback-only validation fixture. Production still requires an explicit
        caller acknowledgement and exact database identity match.
        """

        payload = _resolution_payload(resolutions)
        if connection is not None:
            return self._repair_connection(
                connection,
                payload,
                target=target,
                production_authorized=production_authorized,
                commit=commit,
                sample_limit=sample_limit,
            )
        with get_conn() as conn:
            return self._repair_connection(
                conn,
                payload,
                target=target,
                production_authorized=production_authorized,
                commit=commit,
                sample_limit=sample_limit,
            )

    @staticmethod
    def _audit_connection(connection: Any, payload: str, sample_limit: int) -> RepairAudit:
        with connection.cursor() as cursor:
            cursor.execute(
                _AUDIT_SQL,
                {"resolutions": payload, "sample_limit": sample_limit},
            )
            row = cursor.fetchone()
        counts = dict(row[3] or {})
        samples = dict(row[4] or {})
        return RepairAudit(
            incomplete_rows=int(row[0]),
            repairable_rows=int(row[1]),
            blocker_count=int(row[2]),
            status_counts={str(key): int(value) for key, value in counts.items()},
            samples=samples,
        )

    def _repair_connection(
        self,
        connection: Any,
        payload: str,
        *,
        target: Literal["dev", "production"],
        production_authorized: bool,
        commit: bool,
        sample_limit: int,
    ) -> RepairResult:
        if sample_limit < 1 or sample_limit > 100:
            raise ValueError("sample_limit must be between 1 and 100")
        try:
            with connection.cursor() as cursor:
                _assert_apply_target(
                    connection,
                    cursor,
                    target=target,
                    production_authorized=production_authorized,
                )
                cursor.execute(_LOCK_SQL)
            audit = self._audit_connection(connection, payload, sample_limit)
            if not audit.can_apply:
                raise SectorDataRepairContractError(
                    "SECTOR_DATA_REPAIR_BLOCKED: "
                    f"incomplete_rows={audit.incomplete_rows}, "
                    f"repairable_rows={audit.repairable_rows}, "
                    f"blocker_count={audit.blocker_count}, "
                    f"status_counts={dict(audit.status_counts)}"
                )

            with connection.cursor() as cursor:
                cursor.execute(_UPDATE_SQL, {"resolutions": payload})
                updated_keys = [
                    {"trade_date": row[0].isoformat(), "ts_code": row[1]}
                    for row in cursor.fetchall()
                ]
                if len(updated_keys) != audit.incomplete_rows:
                    raise SectorDataRepairContractError(
                        "SECTOR_DATA_REPAIR_ROWCOUNT_MISMATCH: "
                        f"expected={audit.incomplete_rows}, actual={len(updated_keys)}"
                    )
                if updated_keys:
                    cursor.execute(
                        _READBACK_SQL,
                        {
                            "resolutions": payload,
                            "updated_keys": json.dumps(updated_keys, sort_keys=True),
                        },
                    )
                    readback_incomplete, source_mismatches = cursor.fetchone()
                else:
                    readback_incomplete, source_mismatches = 0, 0
            if readback_incomplete or source_mismatches:
                raise SectorDataRepairContractError(
                    "SECTOR_DATA_REPAIR_READBACK_FAILED: "
                    f"incomplete_rows={readback_incomplete}, "
                    f"source_mismatches={source_mismatches}"
                )
            result = RepairResult(
                audit=audit,
                updated_rows=len(updated_keys),
                readback_incomplete_rows=int(readback_incomplete),
                readback_source_mismatches=int(source_mismatches),
            )
            if commit:
                connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
