"""Read-only PostgreSQL source for C-007-A L1 stock-fact preparation."""

from __future__ import annotations

import hashlib
import heapq
import itertools
import json
import math
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from backend.services.industry_pit.candidate_builder import FrozenDenominator, UniverseSpan
from backend.services.industry_pit.contracts import IndustryPitContractError

from .state_model_set import StateModelSetError, canonical_json_bytes
from .security_identity import SecuritySourceIdentityManifest
from .provider_absence import ProviderAbsenceManifest
from .stock_fact_observation import (
    MIN_COVERAGE,
    L1DailyAggregate,
    ObservationCoverageError,
    aggregate_l1_day,
    build_classification_lookup,
)

if TYPE_CHECKING:
    from .industry_pit_adapter import HMMIndustryPitAdapter, HMMIndustryProjection


CIRC_MV_LOOKBACK_CONTRACT_VERSION = "hmm_risk_causal_circ_mv_source_window_v1"
_CANONICAL_SW_L1_CODE = re.compile(r"^801\d{3}\.SI$")
_INDUSTRY_SW_L1_CODE = re.compile(r"^\d{6}$")


MEMBER_CLASSIFICATION_CTES = """
canonical_l1_catalog AS (
  SELECT l1_code index_code,min(BTRIM(l1_name)) industry_name
  FROM market.sw_index_member
  WHERE l1_code ~ '^801[0-9]{3}[.]SI$'
  GROUP BY l1_code
), l2_catalog AS (
  SELECT l2_code index_code,min(BTRIM(l2_name)) industry_name
  FROM market.sw_index_member
  GROUP BY l2_code
), l2_owner AS (
  SELECT l2_code,
         min(l1_code) FILTER (WHERE l1_code ~ '^801[0-9]{3}[.]SI$') canonical_l1_code,
         count(DISTINCT l1_code) FILTER (WHERE l1_code ~ '^801[0-9]{3}[.]SI$') canonical_l1_count
  FROM market.sw_index_member
  GROUP BY l2_code
)
"""


def _build_member_classification_lookup(rows: list[tuple[Any, ...]]) -> dict[tuple[str, str], dict[str, str]]:
    """Derive the exact SW L1/L2 identity closure from durable member facts."""

    normalized: list[tuple[str, str, str, str]] = []
    canonical_l1_names: dict[str, set[str]] = {}
    l2_names: dict[str, set[str]] = {}
    l2_canonical_owners: dict[str, set[str]] = {}
    for raw_row in rows:
        if len(raw_row) != 4:
            raise StateModelSetError("sw_index_member classification row shape is invalid")
        l1_code, l1_name, l2_code, l2_name = (str(value or "").strip() for value in raw_row)
        if not l1_code or not l2_code or not l2_name:
            raise StateModelSetError("sw_index_member classification row is incomplete")
        if not (_CANONICAL_SW_L1_CODE.fullmatch(l1_code) or _INDUSTRY_SW_L1_CODE.fullmatch(l1_code)):
            raise StateModelSetError(f"sw_index_member L1 code representation is invalid: {l1_code}")
        normalized.append((l1_code, l1_name, l2_code, l2_name))
        l2_names.setdefault(l2_code, set()).add(l2_name)
        if _CANONICAL_SW_L1_CODE.fullmatch(l1_code):
            if not l1_name:
                raise StateModelSetError("sw_index_member canonical L1 name is missing")
            canonical_l1_names.setdefault(l1_code, set()).add(l1_name)
            l2_canonical_owners.setdefault(l2_code, set()).add(l1_code)

    if not normalized:
        raise StateModelSetError("sw_index_member classification authority is empty")
    if len(canonical_l1_names) != 31 or len(l2_names) != 131:
        raise StateModelSetError(
            "sw_index_member classification authority must contain canonical L1=31/L2=131; "
            f"actual={len(canonical_l1_names)}/{len(l2_names)}"
        )
    conflicting_l1_names = sorted(code for code, names in canonical_l1_names.items() if len(names) != 1)
    conflicting_l2_names = sorted(code for code, names in l2_names.items() if len(names) != 1)
    if conflicting_l1_names or conflicting_l2_names:
        raise StateModelSetError(
            f"sw_index_member classification names conflict: l1={conflicting_l1_names},l2={conflicting_l2_names}"
        )
    invalid_l2_owners = sorted(code for code in l2_names if len(l2_canonical_owners.get(code, set())) != 1)
    if invalid_l2_owners:
        raise StateModelSetError(
            f"sw_index_member L2 identity must resolve to exactly one canonical L1: {invalid_l2_owners}"
        )

    aliases_by_l1: dict[str, set[str]] = {code: {code} for code in canonical_l1_names}
    for l1_code, _l1_name, l2_code, _l2_name in normalized:
        canonical_l1 = next(iter(l2_canonical_owners[l2_code]))
        aliases_by_l1.setdefault(canonical_l1, set()).add(l1_code)
    ambiguous_aliases: dict[str, set[str]] = {}
    for l1_code, _l1_name, l2_code, _l2_name in normalized:
        canonical_l1 = next(iter(l2_canonical_owners[l2_code]))
        ambiguous_aliases.setdefault(l1_code, set()).add(canonical_l1)
    conflicts = sorted(alias for alias, owners in ambiguous_aliases.items() if len(owners) != 1)
    if conflicts:
        raise StateModelSetError(f"sw_index_member L1 alias maps to multiple canonical identities: {conflicts}")

    classification_rows: list[dict[str, str]] = []
    for canonical_l1 in sorted(canonical_l1_names):
        noncanonical_aliases = sorted(
            alias for alias in aliases_by_l1[canonical_l1] if not _CANONICAL_SW_L1_CODE.fullmatch(alias)
        )
        if len(noncanonical_aliases) > 1:
            raise StateModelSetError(
                f"sw_index_member canonical L1 has multiple industry aliases: {canonical_l1}={noncanonical_aliases}"
            )
        classification_rows.append(
            {
                "level": "L1",
                "index_code": canonical_l1,
                "industry_code": noncanonical_aliases[0] if noncanonical_aliases else canonical_l1,
                "industry_name": next(iter(canonical_l1_names[canonical_l1])),
            }
        )
    for l2_code in sorted(l2_names):
        classification_rows.append(
            {
                "level": "L2",
                "index_code": l2_code,
                "industry_code": l2_code,
                "industry_name": next(iter(l2_names[l2_code])),
            }
        )
    return build_classification_lookup(classification_rows)


def _full_day_suspension_exists_sql(*, trade_date: str, ts_code: str) -> str:
    """Return the authoritative full-day suspension predicate for internal SQL aliases."""

    allowed_identities = {
        ("calendar_base.trade_date", "spans.ts_code"),
        ("price.trade_date", "price.ts_code"),
        ("c.trade_date", "c.ts_code"),
    }
    if (trade_date, ts_code) not in allowed_identities:
        raise ValueError("unsupported full-day suspension SQL identity")
    no_trade_clause = (
        "AND COALESCE(price.volume_hand,0)=0 AND COALESCE(price.amount_li,0)=0"
        if (trade_date, ts_code) == ("price.trade_date", "price.ts_code")
        else ""
    )
    return f"""
        EXISTS (
          SELECT 1
          FROM market.suspend_d suspension
          WHERE suspension.trade_date={trade_date}
            AND suspension.ts_code={ts_code}
            AND suspension.suspend_type='S'
            AND COALESCE(BTRIM(suspension.suspend_timing),'') IN ('','09:30-09:30')
            {no_trade_clause}
        )
    """


@dataclass(frozen=True)
class StockFactSourceSpec:
    universe_key: str
    universe_rule_version: str
    source_start: date
    source_end: date
    circ_mv_history_start: date | None = None

    @property
    def effective_circ_mv_history_start(self) -> date:
        return self.circ_mv_history_start or self.source_start

    def validate(self) -> None:
        if not self.universe_key.strip() or not self.universe_rule_version.strip():
            raise StateModelSetError("universe key/rule version are required")
        if self.source_start > self.source_end:
            raise StateModelSetError("stock-fact source window is invalid")
        if self.effective_circ_mv_history_start > self.source_start:
            raise StateModelSetError("circ-mv history window must start no later than the stock-fact source window")


@dataclass(frozen=True)
class CircMvEvidence:
    accepted_value: Any
    source_date: date | None
    staleness_trading_days: int | None
    crossed_pit_entry_boundary: bool
    fact_status: str
    reason_code: str | None


def _build_circ_mv_evidence(
    *,
    raw_value: Any,
    source_date: Any,
    staleness_trading_days: Any,
    trade_date: date,
    pit_eligible_start: date,
    history_start: date,
) -> CircMvEvidence:
    normalized_source_date = source_date if isinstance(source_date, date) else None
    normalized_staleness = staleness_trading_days if isinstance(staleness_trading_days, int) else None
    crossed_boundary = bool(normalized_source_date is not None and normalized_source_date < pit_eligible_start)
    if normalized_source_date is None:
        return CircMvEvidence(
            accepted_value=None,
            source_date=None,
            staleness_trading_days=None,
            crossed_pit_entry_boundary=False,
            fact_status="source_unavailable",
            reason_code="hmm_risk_stock_fact_circ_mv_source_unavailable",
        )
    if not (
        history_start <= normalized_source_date < trade_date
        and normalized_staleness is not None
        and normalized_staleness >= 0
    ):
        return CircMvEvidence(
            accepted_value=None,
            source_date=normalized_source_date,
            staleness_trading_days=normalized_staleness,
            crossed_pit_entry_boundary=crossed_boundary,
            fact_status="causal_source_invalid",
            reason_code="hmm_risk_stock_fact_circ_mv_causal_source_invalid",
        )
    if raw_value is None:
        return CircMvEvidence(
            accepted_value=None,
            source_date=normalized_source_date,
            staleness_trading_days=normalized_staleness,
            crossed_pit_entry_boundary=crossed_boundary,
            fact_status="latest_value_missing",
            reason_code="hmm_risk_stock_fact_circ_mv_latest_value_missing",
        )
    if isinstance(raw_value, bool):
        return CircMvEvidence(
            accepted_value=None,
            source_date=normalized_source_date,
            staleness_trading_days=normalized_staleness,
            crossed_pit_entry_boundary=crossed_boundary,
            fact_status="latest_value_non_numeric",
            reason_code="hmm_risk_stock_fact_circ_mv_latest_value_non_numeric",
        )
    try:
        normalized_value = float(raw_value)
    except (TypeError, ValueError, OverflowError):
        return CircMvEvidence(
            accepted_value=None,
            source_date=normalized_source_date,
            staleness_trading_days=normalized_staleness,
            crossed_pit_entry_boundary=crossed_boundary,
            fact_status="latest_value_non_numeric",
            reason_code="hmm_risk_stock_fact_circ_mv_latest_value_non_numeric",
        )
    if not math.isfinite(normalized_value):
        return CircMvEvidence(
            accepted_value=None,
            source_date=normalized_source_date,
            staleness_trading_days=normalized_staleness,
            crossed_pit_entry_boundary=crossed_boundary,
            fact_status="latest_value_non_finite",
            reason_code="hmm_risk_stock_fact_circ_mv_latest_value_non_finite",
        )
    if normalized_value <= 0:
        return CircMvEvidence(
            accepted_value=None,
            source_date=normalized_source_date,
            staleness_trading_days=normalized_staleness,
            crossed_pit_entry_boundary=crossed_boundary,
            fact_status="latest_value_non_positive",
            reason_code="hmm_risk_stock_fact_circ_mv_latest_value_non_positive",
        )
    return CircMvEvidence(
        accepted_value=raw_value,
        source_date=normalized_source_date,
        staleness_trading_days=normalized_staleness,
        crossed_pit_entry_boundary=crossed_boundary,
        fact_status="available",
        reason_code=None,
    )


class PostgresStockFactReader:
    """Stream explicit stock facts from an already read-only connection."""

    def __init__(
        self,
        conn: Any,
        spec: StockFactSourceSpec,
        *,
        security_identity_manifest: SecuritySourceIdentityManifest,
        provider_absence_manifest: ProviderAbsenceManifest,
        industry_pit_adapter: HMMIndustryPitAdapter | None = None,
    ) -> None:
        spec.validate()
        self._conn = conn
        self.spec = spec
        self.security_identity_manifest = security_identity_manifest
        self.provider_absence_manifest = provider_absence_manifest
        self.industry_pit_adapter = industry_pit_adapter
        self.industry_pit_preflight: Mapping[str, Any] | None = None
        self._classification_lookup: dict[tuple[str, str], dict[str, str]] | None = None

    def _identity_alias_json(self, source_dataset: str) -> str:
        return json.dumps(
            self.security_identity_manifest.alias_rows(source_dataset),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _month_windows(start: date, end: date) -> Iterator[tuple[date, date]]:
        window_start = start
        while window_start <= end:
            next_month = (window_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            yield window_start, min(next_month - timedelta(days=1), end)
            window_start = next_month

    def load_industry_pit_denominator(self, *, window_start: date, window_end: date) -> FrozenDenominator:
        if window_start > window_end:
            raise StateModelSetError("HMM industry PIT denominator window is invalid")
        if window_start < self.spec.source_start or window_end > self.spec.source_end:
            raise StateModelSetError("HMM industry PIT denominator escapes the frozen stock-fact source window")
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT cal_date::date
                FROM market.trading_calendar
                WHERE is_trading=true AND cal_date BETWEEN %s AND %s
                ORDER BY cal_date
                """,
                (window_start, window_end),
            )
            trading_dates = tuple(row[0] for row in cursor.fetchall())
            cursor.execute(
                """
                SELECT ts_code,eligible_start,eligible_end
                FROM market.stock_universe_pit_spans
                WHERE universe_key=%s AND eligible_start<=%s
                  AND (eligible_end IS NULL OR eligible_end>=%s)
                ORDER BY ts_code,eligible_start,eligible_end NULLS LAST
                """,
                (self.spec.universe_key, window_end, window_start),
            )
            raw_spans = cursor.fetchall()
        try:
            spans = tuple(UniverseSpan(row[0], row[1], row[2]) for row in raw_spans)
            return FrozenDenominator.build(
                window_start=window_start,
                window_end=window_end,
                trading_dates=trading_dates,
                universe_spans=spans,
            )
        except (IndustryPitContractError, TypeError, ValueError) as exc:
            raise StateModelSetError(f"HMM industry PIT denominator is invalid: {exc}") from exc

    def run_industry_pit_preflight(
        self,
        *,
        window_start: date,
        window_end: date,
        expected_trading_days: int,
    ) -> Mapping[str, Any]:
        if self.industry_pit_adapter is None:
            raise StateModelSetError("HMM shared industry PIT adapter is missing")
        denominator = self.load_industry_pit_denominator(window_start=window_start, window_end=window_end)
        self.industry_pit_preflight = self.industry_pit_adapter.preflight(
            denominator,
            expected_trading_days=expected_trading_days,
        )
        return self.industry_pit_preflight

    def _industry_projection(self, symbol: str, trade_date: date) -> HMMIndustryProjection | None:
        if self.industry_pit_adapter is None:
            return None
        return self.industry_pit_adapter.resolve(symbol, trade_date)

    def _load_trading_date_ordinals(self, eligible_start: date) -> dict[date, int]:
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT cal_date::date
                FROM market.trading_calendar
                WHERE is_trading=true AND cal_date BETWEEN %s AND %s
                ORDER BY cal_date
                """,
                (eligible_start, self.spec.source_end),
            )
            rows = cursor.fetchall()
        if not rows:
            raise StateModelSetError("stock-fact trading calendar is empty")
        return {row[0]: ordinal for ordinal, row in enumerate(rows, start=1)}

    @staticmethod
    def _unique_fact_map(
        rows: list[tuple[Any, ...]],
        *,
        source_name: str,
    ) -> dict[tuple[date, str], tuple[Any, ...]]:
        result: dict[tuple[date, str], tuple[Any, ...]] = {}
        for row in rows:
            key = (row[0], str(row[1]))
            value = tuple(row[2:])
            existing = result.get(key)
            if existing is not None and existing != value:
                raise StateModelSetError(f"hmm_risk_stock_fact_conflicting_duplicate: {source_name} {key[1]}/{key[0]}")
            result[key] = value
        return result

    def _load_initial_circ_mv_state(
        self,
        *,
        canonical_codes: set[str],
        before_date: date,
    ) -> dict[str, tuple[date, Any]]:
        if not canonical_codes:
            return {}
        request_rows = [{"ts_code": code} for code in sorted(canonical_codes)]
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                WITH requested AS (
                  SELECT ts_code
                  FROM jsonb_to_recordset(%s::jsonb) AS item(ts_code text)
                )
                SELECT requested.ts_code,previous.trade_date,previous.circ_mv
                FROM requested
                LEFT JOIN LATERAL (
                  SELECT db.trade_date,db.circ_mv
                  FROM market.daily_basic db
                  WHERE db.ts_code=requested.ts_code
                    AND db.trade_date>=%s
                    AND db.trade_date<%s
                  ORDER BY db.trade_date DESC
                  LIMIT 1
                ) previous ON true
                ORDER BY requested.ts_code
                """,
                (
                    json.dumps(request_rows, separators=(",", ":")),
                    self.spec.effective_circ_mv_history_start,
                    before_date,
                ),
            )
            rows = cursor.fetchall()
        return {str(code): (source_date, circ_mv) for code, source_date, circ_mv in rows if source_date is not None}

    def _load_window_fact_maps(
        self,
        *,
        window_start: date,
        window_end: date,
        canonical_codes: set[str],
        moneyflow_source_codes: set[str],
    ) -> tuple[
        dict[tuple[date, str], tuple[Any, ...]],
        dict[tuple[date, str], tuple[Any, ...]],
        dict[tuple[date, str], tuple[Any, ...]],
    ]:
        codes = sorted(canonical_codes)
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT trade_date,ts_code,total_mv,circ_mv
                FROM market.daily_basic
                WHERE trade_date BETWEEN %s AND %s AND ts_code=ANY(%s)
                ORDER BY trade_date,ts_code
                """,
                (window_start, window_end, codes),
            )
            daily_basic_rows = cursor.fetchall()
            cursor.execute(
                """
                SELECT trade_date,ts_code,buy_sm_amount,sell_sm_amount,
                       buy_elg_amount,sell_elg_amount,net_mf_amount
                FROM market.moneyflow_ts
                WHERE trade_date BETWEEN %s AND %s AND ts_code=ANY(%s)
                ORDER BY trade_date,ts_code
                """,
                (window_start, window_end, sorted(moneyflow_source_codes)),
            )
            moneyflow_rows = cursor.fetchall()
            cursor.execute(
                """
                SELECT trade_date,ts_code,up_limit
                FROM market.stk_limit
                WHERE trade_date BETWEEN %s AND %s AND ts_code=ANY(%s)
                ORDER BY trade_date,ts_code
                """,
                (window_start, window_end, codes),
            )
            limit_rows = cursor.fetchall()
        return (
            self._unique_fact_map(daily_basic_rows, source_name="daily_basic"),
            self._unique_fact_map(moneyflow_rows, source_name="moneyflow_ts"),
            self._unique_fact_map(limit_rows, source_name="stk_limit"),
        )

    def _load_daily_basic_map(
        self,
        *,
        window_start: date,
        window_end: date,
        canonical_codes: set[str],
    ) -> dict[tuple[date, str], tuple[Any, ...]]:
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT trade_date,ts_code,total_mv,circ_mv
                FROM market.daily_basic
                WHERE trade_date BETWEEN %s AND %s AND ts_code=ANY(%s)
                ORDER BY trade_date,ts_code
                """,
                (window_start, window_end, sorted(canonical_codes)),
            )
            rows = cursor.fetchall()
        return self._unique_fact_map(rows, source_name="daily_basic")

    def _load_missing_price_base_rows(
        self,
        *,
        window_start: date,
        window_end: date,
        fetch_size: int,
        sector_level: str,
    ) -> list[tuple[Any, ...]]:
        if self.industry_pit_adapter is not None:
            return self._load_unclassified_missing_price_base_rows(
                window_start=window_start,
                window_end=window_end,
                fetch_size=fetch_size,
            )
        cursor_prefix = "hmm_risk_missing_price_base" if sector_level == "L1" else "hmm_risk_missing_price_base_l2"
        cursor = self._conn.cursor(name=f"{cursor_prefix}_{window_start:%Y%m%d}_{window_end:%Y%m%d}")
        cursor.itersize = fetch_size
        order_by = (
            "c.trade_date,c.l1_code,c.ts_code,c.l2_code"
            if sector_level == "L1"
            else "c.trade_date,c.l2_code,c.ts_code,c.l1_code"
        )
        cursor.execute(
            f"""
            WITH {MEMBER_CLASSIFICATION_CTES}, calendar_history AS (
              SELECT cal_date::date trade_date,
                     lag(cal_date::date,1) OVER (ORDER BY cal_date) previous_trade_date
              FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            ), calendar_base AS (
              SELECT trade_date,previous_trade_date FROM calendar_history
              WHERE trade_date BETWEEN %s AND %s
            ), missing_keys AS MATERIALIZED (
              SELECT calendar_base.trade_date,calendar_base.previous_trade_date,
                     spans.ts_code,spans.eligible_start,spans.eligible_end
              FROM calendar_base
              JOIN market.stock_universe_pit_spans spans
                ON spans.universe_key=%s AND spans.eligible_start<=calendar_base.trade_date
               AND (spans.eligible_end IS NULL OR spans.eligible_end>=calendar_base.trade_date)
              WHERE NOT EXISTS (
                SELECT 1 FROM market.kline_daily_raw price
                WHERE price.trade_date=calendar_base.trade_date AND price.ts_code=spans.ts_code
              )
                AND NOT ({_full_day_suspension_exists_sql(trade_date="calendar_base.trade_date", ts_code="spans.ts_code")})
            ), mapping_source AS (
              SELECT missing_keys.trade_date,missing_keys.previous_trade_date,
                     missing_keys.ts_code,missing_keys.eligible_start,missing_keys.eligible_end,
                     l1.index_code l1_code,l1.industry_name l1_name,
                     l2.index_code l2_code,l2.industry_name l2_name
              FROM missing_keys
              JOIN market.sw_index_member member
                ON member.ts_code=missing_keys.ts_code AND member.in_date<=missing_keys.trade_date
               AND (member.out_date IS NULL OR member.out_date>=missing_keys.trade_date)
              JOIN l2_owner owner
                ON owner.l2_code=member.l2_code AND owner.canonical_l1_count=1
              JOIN canonical_l1_catalog l1 ON l1.index_code=owner.canonical_l1_code
              JOIN l2_catalog l2 ON l2.index_code=member.l2_code
            ), canonical_identity AS (
              SELECT trade_date,previous_trade_date,ts_code,eligible_start,eligible_end,
                     l1_code,l1_name,l2_code,l2_name
              FROM mapping_source
              GROUP BY trade_date,previous_trade_date,ts_code,eligible_start,eligible_end,
                       l1_code,l1_name,l2_code,l2_name
            ), counted AS (
              SELECT c.*,count(*) OVER (PARTITION BY trade_date,ts_code) canonical_identity_count
              FROM canonical_identity c
            )
            SELECT c.trade_date,c.ts_code,c.l1_code,c.l1_name,c.l2_code,c.l2_name,
                   c.canonical_identity_count,c.eligible_start,c.previous_trade_date
            FROM counted c
            ORDER BY {order_by}
            """,
            (
                window_start - timedelta(days=60),
                window_end,
                window_start,
                window_end,
                self.spec.universe_key,
            ),
        )
        try:
            return list(cursor)
        finally:
            cursor.close()

    def _load_unclassified_missing_price_base_rows(
        self,
        *,
        window_start: date,
        window_end: date,
        fetch_size: int,
    ) -> list[tuple[Any, ...]]:
        cursor = self._conn.cursor(name=f"hmm_risk_pit_missing_price_base_{window_start:%Y%m%d}_{window_end:%Y%m%d}")
        cursor.itersize = fetch_size
        cursor.execute(
            f"""
            WITH calendar_history AS (
              SELECT cal_date::date trade_date,
                     lag(cal_date::date,1) OVER (ORDER BY cal_date) previous_trade_date
              FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            ), calendar_base AS (
              SELECT trade_date,previous_trade_date FROM calendar_history
              WHERE trade_date BETWEEN %s AND %s
            )
            SELECT calendar_base.trade_date,spans.ts_code,
                   NULL::text,NULL::text,NULL::text,NULL::text,
                   1 canonical_identity_count,spans.eligible_start,
                   calendar_base.previous_trade_date
            FROM calendar_base
            JOIN market.stock_universe_pit_spans spans
              ON spans.universe_key=%s AND spans.eligible_start<=calendar_base.trade_date
             AND (spans.eligible_end IS NULL OR spans.eligible_end>=calendar_base.trade_date)
            WHERE NOT EXISTS (
              SELECT 1 FROM market.kline_daily_raw price
              WHERE price.trade_date=calendar_base.trade_date AND price.ts_code=spans.ts_code
            )
              AND NOT ({_full_day_suspension_exists_sql(trade_date="calendar_base.trade_date", ts_code="spans.ts_code")})
            ORDER BY calendar_base.trade_date,spans.ts_code
            """,
            (
                window_start - timedelta(days=60),
                window_end,
                window_start,
                window_end,
                self.spec.universe_key,
            ),
        )
        try:
            return list(cursor)
        finally:
            cursor.close()

    def _iter_missing_price_rows_separated(
        self,
        *,
        fetch_size: int,
        sector_level: str,
    ) -> Iterator[dict[str, Any]]:
        trading_ordinals: dict[date, int] | None = None
        for window_start, window_end in self._month_windows(self.spec.source_start, self.spec.source_end):
            base_rows = self._load_missing_price_base_rows(
                window_start=window_start,
                window_end=window_end,
                fetch_size=fetch_size,
                sector_level=sector_level,
            )
            if not base_rows:
                continue
            canonical_codes = {str(row[1]) for row in base_rows}
            if trading_ordinals is None:
                trading_ordinals = self._load_trading_date_ordinals(self.spec.effective_circ_mv_history_start)
            circ_mv_state = self._load_initial_circ_mv_state(
                canonical_codes=canonical_codes,
                before_date=window_start,
            )
            daily_basic = self._load_daily_basic_map(
                window_start=window_start,
                window_end=window_end,
                canonical_codes=canonical_codes,
            )
            daily_events = sorted((key[0], key[1], value[1]) for key, value in daily_basic.items())
            event_index = 0
            assert trading_ordinals is not None
            for row in base_rows:
                trade_date_value = row[0]
                while event_index < len(daily_events) and daily_events[event_index][0] < trade_date_value:
                    event_date, event_code, event_circ_mv = daily_events[event_index]
                    if event_date not in trading_ordinals:
                        raise StateModelSetError(
                            f"hmm_risk_stock_fact_calendar_mismatch: daily_basic {event_code}/{event_date}"
                        )
                    circ_mv_state[event_code] = (event_date, event_circ_mv)
                    event_index += 1
                if int(row[6]) != 1:
                    raise StateModelSetError(
                        f"symbol/date resolves to multiple canonical identities: {row[1]}/{row[0]}"
                    )
                code = str(row[1])
                industry_projection = self._industry_projection(code, trade_date_value)
                if industry_projection is not None and industry_projection.status == "unavailable":
                    continue
                l1_code = row[2] if industry_projection is None else industry_projection.l1_code
                l1_name = row[3] if industry_projection is None else industry_projection.l1_name
                l2_code = row[4] if industry_projection is None else industry_projection.l2_code
                l2_name = row[5] if industry_projection is None else industry_projection.l2_name
                eligible_start = row[7]
                previous_market_date = row[8]
                circ_state = circ_mv_state.get(code)
                circ_mv_source_date = None if circ_state is None else circ_state[0]
                previous_circ_mv = None if circ_state is None else circ_state[1]
                circ_mv_staleness = None
                if isinstance(circ_mv_source_date, date) and isinstance(previous_market_date, date):
                    try:
                        circ_mv_staleness = (
                            trading_ordinals[previous_market_date] - trading_ordinals[circ_mv_source_date]
                        )
                    except KeyError as exc:
                        raise StateModelSetError("hmm_risk_stock_fact_calendar_mismatch: circ_mv source date") from exc
                    if circ_mv_staleness < 0:
                        raise StateModelSetError("hmm_risk_stock_fact_causal_circ_mv_invalid: negative staleness")
                circ_mv_evidence = _build_circ_mv_evidence(
                    raw_value=previous_circ_mv,
                    source_date=circ_mv_source_date,
                    staleness_trading_days=circ_mv_staleness,
                    trade_date=trade_date_value,
                    pit_eligible_start=eligible_start,
                    history_start=self.spec.effective_circ_mv_history_start,
                )
                current_basic = daily_basic.get((trade_date_value, code))
                moneyflow_resolution = self.security_identity_manifest.resolve(
                    code, trade_date_value, "market.moneyflow_ts"
                )
                yield {
                    "trade_date": trade_date_value,
                    "symbol": code,
                    "l1_code": l1_code,
                    "l1_name": l1_name,
                    "l2_code": l2_code,
                    "l2_name": l2_name,
                    "industry_pit_resolution": (None if industry_projection is None else industry_projection.as_dict()),
                    "is_suspended": False,
                    "open_yuan": None,
                    "high_yuan": None,
                    "low_yuan": None,
                    "close_yuan": None,
                    "volume_shares": None,
                    "amount_cny": None,
                    "prev_close_yuan": None,
                    "prev_close_5_yuan": None,
                    "prev_close_10_yuan": None,
                    "total_mv_cny": _scaled(None if current_basic is None else current_basic[0], 0.0001),
                    "prev_circ_mv_cny": _scaled(circ_mv_evidence.accepted_value, 0.0001),
                    "circ_mv_source_date": circ_mv_evidence.source_date,
                    "circ_mv_staleness_trading_days": circ_mv_evidence.staleness_trading_days,
                    "circ_mv_crossed_pit_entry_boundary": circ_mv_evidence.crossed_pit_entry_boundary,
                    "circ_mv_pit_eligible_start": eligible_start,
                    "circ_mv_history_start": self.spec.effective_circ_mv_history_start,
                    "circ_mv_lookback_contract_version": CIRC_MV_LOOKBACK_CONTRACT_VERSION,
                    "circ_mv_fact_status": circ_mv_evidence.fact_status,
                    "circ_mv_reason_code": circ_mv_evidence.reason_code,
                    "buy_sm_amount_cny": None,
                    "sell_sm_amount_cny": None,
                    "buy_elg_amount_cny": None,
                    "sell_elg_amount_cny": None,
                    "net_mf_amount_cny": None,
                    "moneyflow_fact_status": "not_evaluated_missing_price",
                    "moneyflow_source_identity": moneyflow_resolution.evidence(),
                    "moneyflow_provider_absence": None,
                    "up_limit_yuan": None,
                }

    def _load_stock_base_rows(
        self,
        *,
        window_start: date,
        window_end: date,
        fetch_size: int,
        sector_level: str,
    ) -> list[tuple[Any, ...]]:
        if self.industry_pit_adapter is not None:
            return self._load_unclassified_stock_base_rows(
                window_start=window_start,
                window_end=window_end,
                fetch_size=fetch_size,
            )
        price_history_start = window_start - timedelta(days=60)
        cursor_prefix = "hmm_risk_stock_fact_base" if sector_level == "L1" else "hmm_risk_stock_fact_base_l2"
        cursor = self._conn.cursor(name=f"{cursor_prefix}_{window_start:%Y%m%d}_{window_end:%Y%m%d}")
        cursor.itersize = fetch_size
        order_by = (
            "c.trade_date,c.l1_code,c.ts_code,c.l2_code"
            if sector_level == "L1"
            else "c.trade_date,c.l2_code,c.ts_code,c.l1_code"
        )
        cursor.execute(
            f"""
            WITH {MEMBER_CLASSIFICATION_CTES}, calendar_history AS (
              SELECT cal_date::date trade_date,
                     lag(cal_date::date,1) OVER (ORDER BY cal_date) previous_trade_date
              FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            ), price_base AS (
              SELECT DISTINCT price.trade_date,price.ts_code,price.open_li,price.high_li,price.low_li,
                              price.close_li,price.volume_hand,price.amount_li
              FROM market.kline_daily_raw price
              WHERE price.trade_date BETWEEN %s AND %s
                AND NOT ({_full_day_suspension_exists_sql(trade_date="price.trade_date", ts_code="price.ts_code")})
            ), price_history AS (
              SELECT trade_date,ts_code,open_li,high_li,low_li,close_li,volume_hand,amount_li,
                     lag(trade_date,1) OVER w previous_price_date,
                     lag(close_li,1) OVER w previous_close_li,
                     lag(trade_date,5) OVER w previous_price_5_date,
                     lag(close_li,5) OVER w previous_close_5_li,
                     lag(trade_date,10) OVER w previous_price_10_date,
                     lag(close_li,10) OVER w previous_close_10_li
              FROM price_base
              WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
            ), mapping_source AS (
              SELECT p.trade_date,s.ts_code,s.eligible_start,s.eligible_end,
                     p.open_li,p.high_li,p.low_li,p.close_li,p.volume_hand,p.amount_li,
                     p.previous_price_date,p.previous_close_li,p.previous_price_5_date,p.previous_close_5_li,
                     p.previous_price_10_date,p.previous_close_10_li,
                     l1.index_code l1_code,l1.industry_name l1_name,
                     l2.index_code l2_code,l2.industry_name l2_name
              FROM price_history p
              JOIN market.stock_universe_pit_spans s
                ON s.ts_code=p.ts_code AND s.universe_key=%s AND s.eligible_start<=p.trade_date
               AND (s.eligible_end IS NULL OR s.eligible_end>=p.trade_date)
              JOIN market.sw_index_member m
                ON m.ts_code=s.ts_code AND m.in_date<=p.trade_date
               AND (m.out_date IS NULL OR m.out_date>=p.trade_date)
              JOIN l2_owner owner
                ON owner.l2_code=m.l2_code AND owner.canonical_l1_count=1
              JOIN canonical_l1_catalog l1 ON l1.index_code=owner.canonical_l1_code
              JOIN l2_catalog l2 ON l2.index_code=m.l2_code
              WHERE p.trade_date BETWEEN %s AND %s
            ), canonical_identity AS (
              SELECT trade_date,ts_code,eligible_start,eligible_end,
                     open_li,high_li,low_li,close_li,volume_hand,amount_li,
                     previous_price_date,previous_close_li,previous_price_5_date,previous_close_5_li,
                     previous_price_10_date,previous_close_10_li,l1_code,l1_name,l2_code,l2_name
              FROM mapping_source
              GROUP BY trade_date,ts_code,eligible_start,eligible_end,
                       open_li,high_li,low_li,close_li,volume_hand,amount_li,
                       previous_price_date,previous_close_li,previous_price_5_date,previous_close_5_li,
                       previous_price_10_date,previous_close_10_li,l1_code,l1_name,l2_code,l2_name
            ), counted AS (
              SELECT c.*,count(*) OVER (PARTITION BY trade_date,ts_code) canonical_identity_count
              FROM canonical_identity c
            )
            SELECT c.trade_date,c.ts_code,c.l1_code,c.l1_name,c.l2_code,c.l2_name,
                   c.eligible_start,c.canonical_identity_count,
                   c.open_li,c.high_li,c.low_li,c.close_li,c.volume_hand,c.amount_li,
                   c.previous_price_date,c.previous_close_li,c.previous_price_5_date,c.previous_close_5_li,
                   c.previous_price_10_date,c.previous_close_10_li,ch.previous_trade_date
            FROM counted c
            LEFT JOIN calendar_history ch ON ch.trade_date=c.trade_date
            ORDER BY {order_by}
            """,
            (
                price_history_start,
                window_end,
                price_history_start,
                window_end,
                self.spec.universe_key,
                window_start,
                window_end,
            ),
        )
        try:
            return list(cursor)
        finally:
            cursor.close()

    def _load_unclassified_stock_base_rows(
        self,
        *,
        window_start: date,
        window_end: date,
        fetch_size: int,
    ) -> list[tuple[Any, ...]]:
        price_history_start = window_start - timedelta(days=60)
        cursor = self._conn.cursor(name=f"hmm_risk_pit_stock_fact_base_{window_start:%Y%m%d}_{window_end:%Y%m%d}")
        cursor.itersize = fetch_size
        cursor.execute(
            f"""
            WITH calendar_history AS (
              SELECT cal_date::date trade_date,
                     lag(cal_date::date,1) OVER (ORDER BY cal_date) previous_trade_date
              FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            ), price_base AS (
              SELECT DISTINCT price.trade_date,price.ts_code,price.open_li,price.high_li,price.low_li,
                              price.close_li,price.volume_hand,price.amount_li
              FROM market.kline_daily_raw price
              WHERE price.trade_date BETWEEN %s AND %s
                AND NOT ({_full_day_suspension_exists_sql(trade_date="price.trade_date", ts_code="price.ts_code")})
            ), price_history AS (
              SELECT trade_date,ts_code,open_li,high_li,low_li,close_li,volume_hand,amount_li,
                     lag(trade_date,1) OVER w previous_price_date,
                     lag(close_li,1) OVER w previous_close_li,
                     lag(trade_date,5) OVER w previous_price_5_date,
                     lag(close_li,5) OVER w previous_close_5_li,
                     lag(trade_date,10) OVER w previous_price_10_date,
                     lag(close_li,10) OVER w previous_close_10_li
              FROM price_base
              WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
            )
            SELECT p.trade_date,spans.ts_code,
                   NULL::text,NULL::text,NULL::text,NULL::text,
                   spans.eligible_start,1 canonical_identity_count,
                   p.open_li,p.high_li,p.low_li,p.close_li,p.volume_hand,p.amount_li,
                   p.previous_price_date,p.previous_close_li,p.previous_price_5_date,p.previous_close_5_li,
                   p.previous_price_10_date,p.previous_close_10_li,calendar_history.previous_trade_date
            FROM price_history p
            JOIN market.stock_universe_pit_spans spans
              ON spans.ts_code=p.ts_code AND spans.universe_key=%s AND spans.eligible_start<=p.trade_date
             AND (spans.eligible_end IS NULL OR spans.eligible_end>=p.trade_date)
            LEFT JOIN calendar_history ON calendar_history.trade_date=p.trade_date
            WHERE p.trade_date BETWEEN %s AND %s
            ORDER BY p.trade_date,spans.ts_code
            """,
            (
                price_history_start,
                window_end,
                price_history_start,
                window_end,
                self.spec.universe_key,
                window_start,
                window_end,
            ),
        )
        try:
            return list(cursor)
        finally:
            cursor.close()

    def _iter_stock_fact_rows_separated(
        self,
        *,
        fetch_size: int,
        sector_level: str,
    ) -> Iterator[dict[str, Any]]:
        circ_mv_state: dict[str, tuple[date, Any]] = {}
        trading_ordinals: dict[date, int] | None = None
        for window_start, window_end in self._month_windows(self.spec.source_start, self.spec.source_end):
            base_rows = self._load_stock_base_rows(
                window_start=window_start,
                window_end=window_end,
                fetch_size=fetch_size,
                sector_level=sector_level,
            )
            if not base_rows:
                continue
            canonical_codes = {str(row[1]) for row in base_rows}
            moneyflow_source_codes = set(canonical_codes)
            for row in base_rows:
                code = str(row[1])
                resolution = self.security_identity_manifest.resolve(code, row[0], "market.moneyflow_ts")
                moneyflow_source_codes.add(resolution.source_ts_code)
            prior_state = self._load_initial_circ_mv_state(
                canonical_codes=canonical_codes,
                before_date=window_start,
            )
            for code, candidate in prior_state.items():
                existing = circ_mv_state.get(code)
                if existing is None or candidate[0] > existing[0]:
                    circ_mv_state[code] = candidate
            if trading_ordinals is None:
                trading_ordinals = self._load_trading_date_ordinals(self.spec.effective_circ_mv_history_start)
            daily_basic, moneyflow, limits = self._load_window_fact_maps(
                window_start=window_start,
                window_end=window_end,
                canonical_codes=canonical_codes,
                moneyflow_source_codes=moneyflow_source_codes,
            )
            daily_events = sorted((key[0], key[1], value[1]) for key, value in daily_basic.items())
            event_index = 0
            assert trading_ordinals is not None
            for row in base_rows:
                trade_date_value = row[0]
                while event_index < len(daily_events) and daily_events[event_index][0] < trade_date_value:
                    event_date, event_code, event_circ_mv = daily_events[event_index]
                    if event_date not in trading_ordinals:
                        raise StateModelSetError(
                            f"hmm_risk_stock_fact_calendar_mismatch: daily_basic {event_code}/{event_date}"
                        )
                    circ_mv_state[event_code] = (event_date, event_circ_mv)
                    event_index += 1
                if int(row[7]) != 1:
                    raise StateModelSetError(
                        f"symbol/date resolves to multiple canonical identities: {row[1]}/{row[0]}"
                    )
                code = str(row[1])
                industry_projection = self._industry_projection(code, trade_date_value)
                if industry_projection is not None and industry_projection.status == "unavailable":
                    continue
                l1_code = row[2] if industry_projection is None else industry_projection.l1_code
                l1_name = row[3] if industry_projection is None else industry_projection.l1_name
                l2_code = row[4] if industry_projection is None else industry_projection.l2_code
                l2_name = row[5] if industry_projection is None else industry_projection.l2_name
                eligible_start = row[6]
                previous_close = row[15] if row[14] is not None and row[14] >= eligible_start else None
                previous_close_5 = row[17] if row[16] is not None and row[16] >= eligible_start else None
                previous_close_10 = row[19] if row[18] is not None and row[18] >= eligible_start else None
                previous_market_date = row[20]
                circ_state = circ_mv_state.get(code)
                circ_mv_source_date = None if circ_state is None else circ_state[0]
                previous_circ_mv = None if circ_state is None else circ_state[1]
                circ_mv_staleness = None
                if isinstance(circ_mv_source_date, date) and isinstance(previous_market_date, date):
                    try:
                        circ_mv_staleness = (
                            trading_ordinals[previous_market_date] - trading_ordinals[circ_mv_source_date]
                        )
                    except KeyError as exc:
                        raise StateModelSetError("hmm_risk_stock_fact_calendar_mismatch: circ_mv source date") from exc
                    if circ_mv_staleness < 0:
                        raise StateModelSetError("hmm_risk_stock_fact_causal_circ_mv_invalid: negative staleness")
                circ_mv_evidence = _build_circ_mv_evidence(
                    raw_value=previous_circ_mv,
                    source_date=circ_mv_source_date,
                    staleness_trading_days=circ_mv_staleness,
                    trade_date=trade_date_value,
                    pit_eligible_start=eligible_start,
                    history_start=self.spec.effective_circ_mv_history_start,
                )
                current_basic = daily_basic.get((trade_date_value, code))
                total_mv = None if current_basic is None else current_basic[0]
                moneyflow_resolution = self.security_identity_manifest.resolve(
                    code, trade_date_value, "market.moneyflow_ts"
                )
                moneyflow_row = moneyflow.get((trade_date_value, moneyflow_resolution.source_ts_code))
                if moneyflow_resolution.source_ts_code != code and (trade_date_value, code) in moneyflow:
                    raise StateModelSetError(
                        "hmm_risk_stock_fact_source_identity_ambiguous: canonical and aliased moneyflow rows coexist"
                    )
                if moneyflow_row is None:
                    provider_absence = self.provider_absence_manifest.resolve(
                        canonical_ts_code=code,
                        source_dataset="market.moneyflow_ts",
                        source_ts_code=moneyflow_resolution.source_ts_code,
                        trade_date=trade_date_value,
                    )
                    moneyflow_fact_status = "provider_absence"
                    moneyflow_values = (None, None, None, None, None)
                elif any(value is None or not math.isfinite(float(value)) for value in moneyflow_row):
                    provider_absence = None
                    moneyflow_fact_status = "required_fields_invalid"
                    moneyflow_values = moneyflow_row
                else:
                    provider_absence = None
                    moneyflow_fact_status = "available"
                    moneyflow_values = moneyflow_row
                limit_row = limits.get((trade_date_value, code))
                yield {
                    "trade_date": trade_date_value,
                    "symbol": code,
                    "l1_code": l1_code,
                    "l1_name": l1_name,
                    "l2_code": l2_code,
                    "l2_name": l2_name,
                    "industry_pit_resolution": (None if industry_projection is None else industry_projection.as_dict()),
                    "is_suspended": False,
                    "open_yuan": _scaled(row[8], 1000.0),
                    "high_yuan": _scaled(row[9], 1000.0),
                    "low_yuan": _scaled(row[10], 1000.0),
                    "close_yuan": _scaled(row[11], 1000.0),
                    "volume_shares": _scaled(row[12], 0.01),
                    "amount_cny": _scaled(row[13], 1000.0),
                    "prev_close_yuan": _scaled(previous_close, 1000.0),
                    "prev_close_5_yuan": _scaled(previous_close_5, 1000.0),
                    "prev_close_10_yuan": _scaled(previous_close_10, 1000.0),
                    "total_mv_cny": _scaled(total_mv, 0.0001),
                    "prev_circ_mv_cny": _scaled(circ_mv_evidence.accepted_value, 0.0001),
                    "circ_mv_source_date": circ_mv_evidence.source_date,
                    "circ_mv_staleness_trading_days": circ_mv_evidence.staleness_trading_days,
                    "circ_mv_crossed_pit_entry_boundary": circ_mv_evidence.crossed_pit_entry_boundary,
                    "circ_mv_pit_eligible_start": eligible_start,
                    "circ_mv_history_start": self.spec.effective_circ_mv_history_start,
                    "circ_mv_lookback_contract_version": CIRC_MV_LOOKBACK_CONTRACT_VERSION,
                    "circ_mv_fact_status": circ_mv_evidence.fact_status,
                    "circ_mv_reason_code": circ_mv_evidence.reason_code,
                    "buy_sm_amount_cny": _scaled(moneyflow_values[0], 0.0001),
                    "sell_sm_amount_cny": _scaled(moneyflow_values[1], 0.0001),
                    "buy_elg_amount_cny": _scaled(moneyflow_values[2], 0.0001),
                    "sell_elg_amount_cny": _scaled(moneyflow_values[3], 0.0001),
                    "net_mf_amount_cny": _scaled(moneyflow_values[4], 0.0001),
                    "moneyflow_fact_status": moneyflow_fact_status,
                    "moneyflow_source_identity": moneyflow_resolution.evidence(),
                    "moneyflow_provider_absence": None if provider_absence is None else provider_absence.evidence(),
                    "up_limit_yuan": None if limit_row is None or limit_row[0] is None else float(limit_row[0]),
                }
            while event_index < len(daily_events):
                event_date, event_code, event_circ_mv = daily_events[event_index]
                if event_date not in trading_ordinals:
                    raise StateModelSetError(
                        f"hmm_risk_stock_fact_calendar_mismatch: daily_basic {event_code}/{event_date}"
                    )
                circ_mv_state[event_code] = (event_date, event_circ_mv)
                event_index += 1

    @staticmethod
    def _circ_mv_asof_fragments(*, previous_date: str, history_start: str, has_alias: bool) -> tuple[str, str, str]:
        if not has_alias:
            return (
                "",
                "pb.trade_date,pb.circ_mv",
                f"""
                LEFT JOIN LATERAL (
                  SELECT canonical_db.trade_date,canonical_db.circ_mv
                  FROM market.daily_basic canonical_db
                  WHERE canonical_db.ts_code=c.ts_code
                    AND canonical_db.trade_date<={previous_date}
                    AND canonical_db.trade_date>={history_start}
                  ORDER BY canonical_db.trade_date DESC
                  LIMIT 1
                ) pb ON true
                LEFT JOIN calendar_ordinal pb_cal ON pb_cal.trade_date=pb.trade_date
                """,
            )
        return (
            "",
            "COALESCE(alias_pb.trade_date,canonical_pb.trade_date),COALESCE(alias_pb.circ_mv,canonical_pb.circ_mv)",
            f"""
            LEFT JOIN daily_basic_alias_identity alias_identity
              ON alias_identity.canonical_ts_code=c.ts_code
            LEFT JOIN LATERAL (
              SELECT canonical_db.trade_date,canonical_db.circ_mv
              FROM market.daily_basic canonical_db
              WHERE alias_identity.canonical_ts_code IS NULL
                AND canonical_db.ts_code=c.ts_code
                AND canonical_db.trade_date<={previous_date}
                AND canonical_db.trade_date>={history_start}
              ORDER BY canonical_db.trade_date DESC
              LIMIT 1
            ) canonical_pb ON true
            LEFT JOIN LATERAL (
              SELECT identity_candidate.trade_date,identity_candidate.circ_mv
              FROM (
                SELECT canonical_db.trade_date,canonical_db.circ_mv
                FROM market.daily_basic canonical_db
                WHERE canonical_db.ts_code=c.ts_code
                  AND canonical_db.trade_date<={previous_date}
                  AND canonical_db.trade_date>={history_start}
                  AND NOT EXISTS (
                    SELECT 1 FROM daily_basic_identity_alias identity_alias
                    WHERE identity_alias.canonical_ts_code=c.ts_code
                      AND canonical_db.trade_date BETWEEN identity_alias.effective_start AND identity_alias.effective_end
                  )
                UNION ALL
                SELECT aliased_db.trade_date,aliased_db.circ_mv
                FROM daily_basic_identity_alias identity_alias
                JOIN market.daily_basic aliased_db
                  ON aliased_db.ts_code=identity_alias.source_ts_code
                 AND aliased_db.trade_date BETWEEN identity_alias.effective_start AND identity_alias.effective_end
                WHERE identity_alias.canonical_ts_code=c.ts_code
                  AND aliased_db.trade_date<={previous_date}
                  AND aliased_db.trade_date>={history_start}
              ) identity_candidate
              WHERE alias_identity.canonical_ts_code IS NOT NULL
              ORDER BY identity_candidate.trade_date DESC
              LIMIT 1
            ) alias_pb ON true
            LEFT JOIN calendar_ordinal pb_cal
              ON pb_cal.trade_date=COALESCE(alias_pb.trade_date,canonical_pb.trade_date)
            """,
        )

    def validate_source(self) -> dict[str, Any]:
        with self._conn.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            row = cursor.fetchone()
            if not row or str(row[0]).lower() not in {"on", "true"}:
                raise StateModelSetError("stock-fact source connection must be transaction_read_only")
            cursor.execute(
                """
                SELECT universe_key,rule_version,scope,start_date,end_date,status,dirty,
                       source_fingerprint_sha256,generated_at
                FROM market.stock_universe_pit_state WHERE universe_key=%s
                """,
                (self.spec.universe_key,),
            )
            state = cursor.fetchone()
            if not state:
                raise StateModelSetError("requested PIT universe state is missing")
            if (
                str(state[1]) != self.spec.universe_rule_version
                or str(state[5]) != "ready"
                or bool(state[6])
                or state[3] > self.spec.source_start
                or state[4] < self.spec.source_end
            ):
                raise StateModelSetError(
                    "requested PIT universe is not ready/clean or does not cover the source window"
                )
            cursor.execute(
                """
                SELECT table_name,column_name,data_type
                FROM information_schema.columns
                WHERE table_schema='market' AND table_name=ANY(%s)
                ORDER BY table_name,ordinal_position
                """,
                (
                    [
                        "kline_daily_raw",
                        "daily_basic",
                        "moneyflow_ts",
                        "stk_limit",
                        "suspend_d",
                        "trading_calendar",
                        "sw_index_member",
                        "stock_universe_pit_spans",
                    ],
                ),
            )
            column_contract = [tuple(item) for item in cursor.fetchall()]
        return {
            "schema_version": "hmm_risk_postgres_stock_fact_source_v1",
            "universe_key": str(state[0]),
            "universe_rule_version": str(state[1]),
            "universe_scope": str(state[2]),
            "universe_start": state[3].isoformat(),
            "universe_end": state[4].isoformat(),
            "universe_source_fingerprint_sha256": str(state[7] or ""),
            "universe_generated_at": None if state[8] is None else state[8].isoformat(),
            "column_contract_sha256": hashlib.sha256(canonical_json_bytes(column_contract)).hexdigest(),
        }

    def load_classification_lookup(self) -> dict[tuple[str, str], dict[str, str]]:
        if self._classification_lookup is not None:
            return self._classification_lookup
        if self.industry_pit_adapter is not None:
            self._classification_lookup = dict(self.industry_pit_adapter.classification_lookup)
            return self._classification_lookup
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT l1_code,l1_name,l2_code,l2_name
                FROM market.sw_index_member
                ORDER BY l1_code,l2_code,l1_name,l2_name
                """
            )
            rows = [tuple(item) for item in cursor.fetchall()]
        self._classification_lookup = _build_member_classification_lookup(rows)
        return self._classification_lookup

    def validate_fact_uniqueness(self) -> None:
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_name,conflict_groups FROM (
                  SELECT 'kline_daily_raw' source_name,count(*) conflict_groups FROM (
                    SELECT trade_date,ts_code FROM market.kline_daily_raw t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                  UNION ALL
                  SELECT 'daily_basic',count(*) FROM (
                    SELECT trade_date,ts_code FROM market.daily_basic t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                  UNION ALL
                  SELECT 'moneyflow_ts',count(*) FROM (
                    SELECT trade_date,ts_code FROM market.moneyflow_ts t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                  UNION ALL
                  SELECT 'stk_limit',count(*) FROM (
                    SELECT trade_date,ts_code FROM market.stk_limit t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                ) duplicates WHERE conflict_groups>0
                """,
                (
                    self.spec.source_start,
                    self.spec.source_end,
                    self.spec.source_start - timedelta(days=10),
                    self.spec.source_end,
                    self.spec.source_start,
                    self.spec.source_end,
                    self.spec.source_start,
                    self.spec.source_end,
                ),
            )
            duplicates = cursor.fetchall()
        if duplicates:
            raise StateModelSetError(f"stock-fact source contains conflicting duplicate keys: {duplicates}")

    def iter_mapping_source_rows(self, *, fetch_size: int = 10_000) -> Iterator[dict[str, Any]]:
        self.load_classification_lookup()
        cursor = self._conn.cursor(name="hmm_risk_mapping_source")
        cursor.itersize = fetch_size
        cursor.execute(
            f"""
            WITH {MEMBER_CLASSIFICATION_CTES}, calendar AS (
              SELECT cal_date::date trade_date FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            )
            SELECT c.trade_date,s.ts_code,m.l1_code,m.l2_code,m.in_date,m.out_date,
                   s.eligible_start,s.eligible_end,
                   l1.index_code canonical_l1_code,l1.industry_name canonical_l1_name,
                   l2.index_code canonical_l2_code,l2.industry_name canonical_l2_name
            FROM calendar c
            JOIN market.stock_universe_pit_spans s
              ON s.universe_key=%s AND s.eligible_start<=c.trade_date
             AND (s.eligible_end IS NULL OR s.eligible_end>=c.trade_date)
            JOIN market.sw_index_member m
              ON m.ts_code=s.ts_code AND m.in_date<=c.trade_date
             AND (m.out_date IS NULL OR m.out_date>=c.trade_date)
            JOIN l2_owner owner
              ON owner.l2_code=m.l2_code AND owner.canonical_l1_count=1
            JOIN canonical_l1_catalog l1 ON l1.index_code=owner.canonical_l1_code
            JOIN l2_catalog l2 ON l2.index_code=m.l2_code
            ORDER BY c.trade_date,s.ts_code,l1.index_code,l2.index_code,m.in_date,m.out_date NULLS LAST
            """,
            (self.spec.source_start, self.spec.source_end, self.spec.universe_key),
        )
        try:
            for row in cursor:
                yield {
                    "trade_date": row[0],
                    "symbol": row[1],
                    "source_l1_code": row[2],
                    "source_l2_code": row[3],
                    "in_date": row[4],
                    "out_date": row[5],
                    "eligible_start": row[6],
                    "eligible_end": row[7],
                    "l1_code": row[8],
                    "l1_name": row[9],
                    "l2_code": row[10],
                    "l2_name": row[11],
                }
        finally:
            cursor.close()

    def iter_stock_fact_rows(
        self,
        *,
        fetch_size: int = 100_000,
        sector_level: str = "L1",
        _window_start: date | None = None,
        _window_end: date | None = None,
    ) -> Iterator[dict[str, Any]]:
        if sector_level not in {"L1", "L2"}:
            raise StateModelSetError("stock fact read level must be L1 or L2")
        if self.industry_pit_adapter is not None and sector_level != "L1":
            raise StateModelSetError("HMM shared industry PIT adapter supports only direct L1 stock facts")
        self.load_classification_lookup()
        if self.industry_pit_adapter is not None and self.security_identity_manifest.alias_rows("market.daily_basic"):
            raise StateModelSetError(
                "HMM shared industry PIT adapter cannot use the legacy combined daily-basic alias query path"
            )
        if (_window_start is None) != (_window_end is None):
            raise StateModelSetError("stock fact query window must provide both boundaries")
        if (
            _window_start is None
            and _window_end is None
            and not self.security_identity_manifest.alias_rows("market.daily_basic")
        ):
            yield from self._iter_stock_fact_rows_separated(
                fetch_size=fetch_size,
                sector_level=sector_level,
            )
            return
        if _window_start is None or _window_end is None:
            window_start = self.spec.source_start
            while window_start <= self.spec.source_end:
                next_month = (window_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                window_end = min(next_month - timedelta(days=1), self.spec.source_end)
                yield from self.iter_stock_fact_rows(
                    fetch_size=fetch_size,
                    sector_level=sector_level,
                    _window_start=window_start,
                    _window_end=window_end,
                )
                window_start = next_month
            return
        if _window_start > _window_end:
            raise StateModelSetError("stock fact query window is invalid")
        price_history_start = _window_start - timedelta(days=60)
        daily_basic_alias_rows = self.security_identity_manifest.alias_rows("market.daily_basic")
        daily_basic_alias_json = json.dumps(
            daily_basic_alias_rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        moneyflow_alias_json = self._identity_alias_json("market.moneyflow_ts")
        circ_mv_history_cte, circ_mv_select_sql, circ_mv_join_sql = self._circ_mv_asof_fragments(
            previous_date="ch.previous_trade_date",
            history_start="(SELECT history_start FROM circ_mv_contract)",
            has_alias=bool(daily_basic_alias_rows),
        )
        cursor_prefix = "hmm_risk_stock_fact_source" if sector_level == "L1" else "hmm_risk_stock_fact_source_l2"
        cursor_name = f"{cursor_prefix}_{_window_start:%Y%m%d}_{_window_end:%Y%m%d}"
        cursor = self._conn.cursor(name=cursor_name)
        cursor.itersize = fetch_size
        order_by = (
            "c.trade_date,c.l1_code,c.ts_code,c.l2_code"
            if sector_level == "L1"
            else "c.trade_date,c.l2_code,c.ts_code,c.l1_code"
        )
        cursor.execute(
            f"""
            WITH {MEMBER_CLASSIFICATION_CTES}, source_bounds AS (
              SELECT LEAST(%s::date,COALESCE(min(eligible_start),%s::date)) history_start,
                     %s::date history_end
              FROM market.stock_universe_pit_spans WHERE universe_key=%s
            ), circ_mv_contract AS (
              SELECT %s::date history_start
            ), calendar_ordinal AS (
              SELECT cal_date::date trade_date,row_number() OVER (ORDER BY cal_date) trade_ordinal
              FROM market.trading_calendar,source_bounds
              WHERE is_trading=true AND cal_date BETWEEN source_bounds.history_start AND source_bounds.history_end
            ), calendar_history AS (
              SELECT trade_date,trade_ordinal,
                     lag(trade_date,1) OVER (ORDER BY trade_date) previous_trade_date,
                     lag(trade_ordinal,1) OVER (ORDER BY trade_date) previous_trade_ordinal
              FROM calendar_ordinal
            ), price_base AS (
              SELECT DISTINCT price.trade_date,price.ts_code,price.open_li,price.high_li,price.low_li,
                              price.close_li,price.volume_hand,price.amount_li
              FROM market.kline_daily_raw price
              WHERE price.trade_date BETWEEN %s AND %s
                AND NOT ({_full_day_suspension_exists_sql(trade_date="price.trade_date", ts_code="price.ts_code")})
            ), price_history AS (
              SELECT trade_date,ts_code,open_li,high_li,low_li,close_li,volume_hand,amount_li,
                     lag(trade_date,1) OVER w previous_price_date,
                     lag(close_li,1) OVER w previous_close_li,
                     lag(trade_date,5) OVER w previous_price_5_date,
                     lag(close_li,5) OVER w previous_close_5_li,
                     lag(trade_date,10) OVER w previous_price_10_date,
                     lag(close_li,10) OVER w previous_close_10_li
              FROM price_base
              WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
            ), daily_basic_identity_alias AS (
              SELECT canonical_ts_code,source_ts_code,effective_start::date,effective_end::date,
                     security_identity_id,row_hash
              FROM jsonb_to_recordset(%s::jsonb) AS item(
                canonical_ts_code text,source_ts_code text,effective_start text,effective_end text,
                security_identity_id text,row_hash text
              )
            ), daily_basic_alias_identity AS (
              SELECT DISTINCT canonical_ts_code FROM daily_basic_identity_alias
            )
            {circ_mv_history_cte}
            , moneyflow_identity_alias AS (
              SELECT canonical_ts_code,source_ts_code,effective_start::date,effective_end::date,
                     security_identity_id,row_hash
              FROM jsonb_to_recordset(%s::jsonb) AS item(
                canonical_ts_code text,source_ts_code text,effective_start text,effective_end text,
                security_identity_id text,row_hash text
              )
            ), moneyflow_base AS (
              SELECT DISTINCT trade_date,ts_code,buy_sm_amount,sell_sm_amount,
                              buy_elg_amount,sell_elg_amount,net_mf_amount
              FROM market.moneyflow_ts
              WHERE trade_date BETWEEN %s AND %s
            ), limit_base AS (
              SELECT DISTINCT trade_date,ts_code,up_limit
              FROM market.stk_limit
              WHERE trade_date BETWEEN %s AND %s
            ), mapping_source AS (
              SELECT p.trade_date,s.ts_code,s.eligible_start,s.eligible_end,
                     p.open_li,p.high_li,p.low_li,p.close_li,p.volume_hand,p.amount_li,
                     p.previous_price_date,p.previous_close_li,p.previous_price_5_date,p.previous_close_5_li,
                     p.previous_price_10_date,p.previous_close_10_li,
                     l1.index_code l1_code,l1.industry_name l1_name,
                     l2.index_code l2_code,l2.industry_name l2_name
              FROM price_history p
              JOIN market.stock_universe_pit_spans s
                ON s.ts_code=p.ts_code AND s.universe_key=%s AND s.eligible_start<=p.trade_date
               AND (s.eligible_end IS NULL OR s.eligible_end>=p.trade_date)
              JOIN market.sw_index_member m
                ON m.ts_code=s.ts_code AND m.in_date<=p.trade_date
               AND (m.out_date IS NULL OR m.out_date>=p.trade_date)
              JOIN l2_owner owner
                ON owner.l2_code=m.l2_code AND owner.canonical_l1_count=1
              JOIN canonical_l1_catalog l1 ON l1.index_code=owner.canonical_l1_code
              JOIN l2_catalog l2 ON l2.index_code=m.l2_code
              WHERE p.trade_date BETWEEN %s AND %s
            ), canonical_identity AS (
              SELECT trade_date,ts_code,eligible_start,eligible_end,
                     open_li,high_li,low_li,close_li,volume_hand,amount_li,
                     previous_price_date,previous_close_li,previous_price_5_date,previous_close_5_li,
                     previous_price_10_date,previous_close_10_li,
                     l1_code,l1_name,l2_code,l2_name
              FROM mapping_source
              GROUP BY trade_date,ts_code,eligible_start,eligible_end,
                       open_li,high_li,low_li,close_li,volume_hand,amount_li,
                       previous_price_date,previous_close_li,previous_price_5_date,previous_close_5_li,
                       previous_price_10_date,previous_close_10_li,
                       l1_code,l1_name,l2_code,l2_name
            ), counted AS (
              SELECT c.*,count(*) OVER (PARTITION BY trade_date,ts_code) canonical_identity_count
              FROM canonical_identity c
            )
            SELECT c.trade_date,c.ts_code,c.l1_code,c.l1_name,c.l2_code,c.l2_name,
                   c.eligible_start,c.canonical_identity_count,
                   c.open_li,c.high_li,c.low_li,c.close_li,c.volume_hand,c.amount_li,
                   c.previous_price_date,c.previous_close_li,c.previous_price_5_date,c.previous_close_5_li,
                   c.previous_price_10_date,c.previous_close_10_li,
                    db.total_mv,ch.previous_trade_date,{circ_mv_select_sql},
                    ch.previous_trade_ordinal-pb_cal.trade_ordinal circ_mv_staleness_trading_days,
                    mf.ts_code,mf.buy_sm_amount,mf.sell_sm_amount,mf.buy_elg_amount,mf.sell_elg_amount,mf.net_mf_amount,
                    canonical_mf.ts_code canonical_moneyflow_ts_code,lim.up_limit
            FROM counted c
            LEFT JOIN calendar_history ch ON ch.trade_date=c.trade_date
            LEFT JOIN daily_basic_identity_alias current_db_alias
              ON current_db_alias.canonical_ts_code=c.ts_code
             AND c.trade_date BETWEEN current_db_alias.effective_start AND current_db_alias.effective_end
            LEFT JOIN market.daily_basic db
              ON db.trade_date=c.trade_date
             AND db.ts_code=COALESCE(current_db_alias.source_ts_code,c.ts_code)
            {circ_mv_join_sql}
            LEFT JOIN moneyflow_identity_alias mf_alias
              ON mf_alias.canonical_ts_code=c.ts_code
             AND c.trade_date BETWEEN mf_alias.effective_start AND mf_alias.effective_end
            LEFT JOIN moneyflow_base mf
              ON mf.trade_date=c.trade_date AND mf.ts_code=COALESCE(mf_alias.source_ts_code,c.ts_code)
            LEFT JOIN moneyflow_base canonical_mf
              ON canonical_mf.trade_date=c.trade_date AND canonical_mf.ts_code=c.ts_code
            LEFT JOIN limit_base lim ON lim.trade_date=c.trade_date AND lim.ts_code=c.ts_code
            ORDER BY {order_by}
            """,
            (
                self.spec.effective_circ_mv_history_start,
                self.spec.effective_circ_mv_history_start,
                _window_end,
                self.spec.universe_key,
                self.spec.effective_circ_mv_history_start,
                price_history_start,
                _window_end,
                daily_basic_alias_json,
                moneyflow_alias_json,
                _window_start,
                _window_end,
                _window_start,
                _window_end,
                self.spec.universe_key,
                _window_start,
                _window_end,
            ),
        )
        try:
            for row in cursor:
                if int(row[7]) != 1:
                    raise StateModelSetError(
                        f"symbol/date resolves to multiple canonical identities: {row[1]}/{row[0]}"
                    )
                eligible_start = row[6]
                previous_close = row[15] if row[14] is not None and row[14] >= eligible_start else None
                previous_close_5 = row[17] if row[16] is not None and row[16] >= eligible_start else None
                previous_close_10 = row[19] if row[18] is not None and row[18] >= eligible_start else None
                circ_mv_source_date = row[22]
                circ_mv_staleness = row[24]
                circ_mv_evidence = _build_circ_mv_evidence(
                    raw_value=row[23],
                    source_date=circ_mv_source_date,
                    staleness_trading_days=circ_mv_staleness,
                    trade_date=row[0],
                    pit_eligible_start=eligible_start,
                    history_start=self.spec.effective_circ_mv_history_start,
                )
                moneyflow_resolution = self.security_identity_manifest.resolve(
                    str(row[1]), row[0], "market.moneyflow_ts"
                )
                if row[25] is not None and str(row[25]) != moneyflow_resolution.source_ts_code:
                    raise StateModelSetError(
                        "hmm_risk_stock_fact_source_identity_ambiguous: resolved moneyflow source row is inconsistent"
                    )
                if moneyflow_resolution.source_ts_code != str(row[1]) and row[31] is not None:
                    raise StateModelSetError(
                        "hmm_risk_stock_fact_source_identity_ambiguous: canonical and aliased moneyflow rows coexist"
                    )
                moneyflow_values = row[26:31]
                if row[25] is None:
                    provider_absence = self.provider_absence_manifest.resolve(
                        canonical_ts_code=str(row[1]),
                        source_dataset="market.moneyflow_ts",
                        source_ts_code=moneyflow_resolution.source_ts_code,
                        trade_date=row[0],
                    )
                    moneyflow_fact_status = "provider_absence"
                elif any(value is None or not math.isfinite(float(value)) for value in moneyflow_values):
                    provider_absence = None
                    moneyflow_fact_status = "required_fields_invalid"
                else:
                    provider_absence = None
                    moneyflow_fact_status = "available"
                yield {
                    "trade_date": row[0],
                    "symbol": row[1],
                    "l1_code": row[2],
                    "l1_name": row[3],
                    "l2_code": row[4],
                    "l2_name": row[5],
                    "is_suspended": False,
                    "open_yuan": _scaled(row[8], 1000.0),
                    "high_yuan": _scaled(row[9], 1000.0),
                    "low_yuan": _scaled(row[10], 1000.0),
                    "close_yuan": _scaled(row[11], 1000.0),
                    "volume_shares": _scaled(row[12], 0.01),
                    "amount_cny": _scaled(row[13], 1000.0),
                    "prev_close_yuan": _scaled(previous_close, 1000.0),
                    "prev_close_5_yuan": _scaled(previous_close_5, 1000.0),
                    "prev_close_10_yuan": _scaled(previous_close_10, 1000.0),
                    "total_mv_cny": _scaled(row[20], 0.0001),
                    "prev_circ_mv_cny": _scaled(circ_mv_evidence.accepted_value, 0.0001),
                    "circ_mv_source_date": circ_mv_evidence.source_date,
                    "circ_mv_staleness_trading_days": circ_mv_evidence.staleness_trading_days,
                    "circ_mv_crossed_pit_entry_boundary": circ_mv_evidence.crossed_pit_entry_boundary,
                    "circ_mv_pit_eligible_start": eligible_start,
                    "circ_mv_history_start": self.spec.effective_circ_mv_history_start,
                    "circ_mv_lookback_contract_version": CIRC_MV_LOOKBACK_CONTRACT_VERSION,
                    "circ_mv_fact_status": circ_mv_evidence.fact_status,
                    "circ_mv_reason_code": circ_mv_evidence.reason_code,
                    "buy_sm_amount_cny": _scaled(row[26], 0.0001),
                    "sell_sm_amount_cny": _scaled(row[27], 0.0001),
                    "buy_elg_amount_cny": _scaled(row[28], 0.0001),
                    "sell_elg_amount_cny": _scaled(row[29], 0.0001),
                    "net_mf_amount_cny": _scaled(row[30], 0.0001),
                    "moneyflow_fact_status": moneyflow_fact_status,
                    "moneyflow_source_identity": moneyflow_resolution.evidence(),
                    "moneyflow_provider_absence": (None if provider_absence is None else provider_absence.evidence()),
                    "up_limit_yuan": None if row[32] is None else float(row[32]),
                }
        finally:
            cursor.close()

    def iter_missing_price_rows(
        self,
        *,
        fetch_size: int = 2_000,
        sector_level: str = "L1",
    ) -> Iterator[dict[str, Any]]:
        """Yield eligible, non-suspended symbol-days missing canonical price facts."""

        if sector_level not in {"L1", "L2"}:
            raise StateModelSetError("missing-price read level must be L1 or L2")
        if self.industry_pit_adapter is not None and sector_level != "L1":
            raise StateModelSetError("HMM shared industry PIT adapter supports only direct L1 missing-price facts")
        self.load_classification_lookup()
        if self.industry_pit_adapter is not None and self.security_identity_manifest.alias_rows("market.daily_basic"):
            raise StateModelSetError(
                "HMM shared industry PIT adapter cannot use the legacy combined daily-basic alias query path"
            )

        if not self.security_identity_manifest.alias_rows("market.daily_basic"):
            yield from self._iter_missing_price_rows_separated(
                fetch_size=fetch_size,
                sector_level=sector_level,
            )
            return

        price_history_start = self.spec.source_start - timedelta(days=60)
        daily_basic_alias_rows = self.security_identity_manifest.alias_rows("market.daily_basic")
        daily_basic_alias_json = json.dumps(
            daily_basic_alias_rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        circ_mv_history_cte, circ_mv_select_sql, circ_mv_join_sql = self._circ_mv_asof_fragments(
            previous_date="c.previous_trade_date",
            history_start="(SELECT history_start FROM circ_mv_contract)",
            has_alias=bool(daily_basic_alias_rows),
        )
        cursor_name = "hmm_risk_missing_price_source" if sector_level == "L1" else "hmm_risk_missing_price_source_l2"
        cursor = self._conn.cursor(name=cursor_name)
        cursor.itersize = fetch_size
        order_by = (
            "c.trade_date,c.l1_code,c.ts_code,c.l2_code"
            if sector_level == "L1"
            else "c.trade_date,c.l2_code,c.ts_code,c.l1_code"
        )
        cursor.execute(
            f"""
            WITH {MEMBER_CLASSIFICATION_CTES}, source_bounds AS (
              SELECT LEAST(%s::date,COALESCE(min(eligible_start),%s::date)) history_start,
                     %s::date history_end
              FROM market.stock_universe_pit_spans WHERE universe_key=%s
            ), circ_mv_contract AS (
              SELECT %s::date history_start
            ), calendar_ordinal AS (
              SELECT cal_date::date trade_date,row_number() OVER (ORDER BY cal_date) trade_ordinal
              FROM market.trading_calendar,source_bounds
              WHERE is_trading=true AND cal_date BETWEEN source_bounds.history_start AND source_bounds.history_end
            ), calendar_history AS (
              SELECT trade_date,trade_ordinal,
                     lag(trade_date,1) OVER (ORDER BY trade_date) previous_trade_date,
                     lag(trade_ordinal,1) OVER (ORDER BY trade_date) previous_trade_ordinal
              FROM calendar_ordinal
            ), calendar_base AS (
              SELECT trade_date,previous_trade_date,previous_trade_ordinal FROM calendar_history
              WHERE trade_date BETWEEN %s AND %s
            ), daily_basic_identity_alias AS (
              SELECT canonical_ts_code,source_ts_code,effective_start::date,effective_end::date
              FROM jsonb_to_recordset(%s::jsonb) AS item(
                canonical_ts_code text,source_ts_code text,effective_start text,effective_end text,
                security_identity_id text,row_hash text
              )
            ), daily_basic_alias_identity AS (
              SELECT DISTINCT canonical_ts_code FROM daily_basic_identity_alias
            )
            {circ_mv_history_cte}
            , price_base AS (
              SELECT DISTINCT trade_date,ts_code FROM market.kline_daily_raw
              WHERE trade_date BETWEEN %s AND %s
            ), mapping_source AS (
              SELECT c.trade_date,c.previous_trade_date,c.previous_trade_ordinal,
                     s.ts_code,s.eligible_start,s.eligible_end,
                     l1.index_code l1_code,l1.industry_name l1_name,
                     l2.index_code l2_code,l2.industry_name l2_name
              FROM calendar_base c
              JOIN market.stock_universe_pit_spans s
                ON s.universe_key=%s AND s.eligible_start<=c.trade_date
               AND (s.eligible_end IS NULL OR s.eligible_end>=c.trade_date)
              JOIN market.sw_index_member m
                ON m.ts_code=s.ts_code AND m.in_date<=c.trade_date
               AND (m.out_date IS NULL OR m.out_date>=c.trade_date)
              JOIN l2_owner owner
                ON owner.l2_code=m.l2_code AND owner.canonical_l1_count=1
              JOIN canonical_l1_catalog l1 ON l1.index_code=owner.canonical_l1_code
              JOIN l2_catalog l2 ON l2.index_code=m.l2_code
            ), canonical_identity AS (
              SELECT trade_date,previous_trade_date,previous_trade_ordinal,ts_code,eligible_start,eligible_end,
                     l1_code,l1_name,l2_code,l2_name
              FROM mapping_source
              GROUP BY trade_date,previous_trade_date,previous_trade_ordinal,ts_code,eligible_start,eligible_end,
                       l1_code,l1_name,l2_code,l2_name
            ), counted AS (
              SELECT c.*,count(*) OVER (PARTITION BY trade_date,ts_code) canonical_identity_count
              FROM canonical_identity c
            )
            SELECT c.trade_date,c.ts_code,c.l1_code,c.l1_name,c.l2_code,c.l2_name,
                   c.canonical_identity_count,c.eligible_start,db.total_mv,c.previous_trade_date,
                   {circ_mv_select_sql},
                   c.previous_trade_ordinal-pb_cal.trade_ordinal
            FROM counted c
            LEFT JOIN daily_basic_identity_alias current_db_alias
              ON current_db_alias.canonical_ts_code=c.ts_code
             AND c.trade_date BETWEEN current_db_alias.effective_start AND current_db_alias.effective_end
            LEFT JOIN market.daily_basic db
              ON db.trade_date=c.trade_date
             AND db.ts_code=COALESCE(current_db_alias.source_ts_code,c.ts_code)
            {circ_mv_join_sql}
            LEFT JOIN price_base p ON p.trade_date=c.trade_date AND p.ts_code=c.ts_code
            WHERE p.ts_code IS NULL
              AND NOT ({_full_day_suspension_exists_sql(trade_date="c.trade_date", ts_code="c.ts_code")})
            ORDER BY {order_by}
            """,
            (
                self.spec.effective_circ_mv_history_start,
                self.spec.effective_circ_mv_history_start,
                self.spec.source_end,
                self.spec.universe_key,
                self.spec.effective_circ_mv_history_start,
                self.spec.source_start,
                self.spec.source_end,
                daily_basic_alias_json,
                price_history_start,
                self.spec.source_end,
                self.spec.universe_key,
            ),
        )
        try:
            for row in cursor:
                if int(row[6]) != 1:
                    raise StateModelSetError(
                        f"symbol/date resolves to multiple canonical identities: {row[1]}/{row[0]}"
                    )
                circ_mv_source_date = row[10]
                circ_mv_staleness = row[12]
                circ_mv_evidence = _build_circ_mv_evidence(
                    raw_value=row[11],
                    source_date=circ_mv_source_date,
                    staleness_trading_days=circ_mv_staleness,
                    trade_date=row[0],
                    pit_eligible_start=row[7],
                    history_start=self.spec.effective_circ_mv_history_start,
                )
                moneyflow_resolution = self.security_identity_manifest.resolve(
                    str(row[1]), row[0], "market.moneyflow_ts"
                )
                yield {
                    "trade_date": row[0],
                    "symbol": row[1],
                    "l1_code": row[2],
                    "l1_name": row[3],
                    "l2_code": row[4],
                    "l2_name": row[5],
                    "is_suspended": False,
                    "open_yuan": None,
                    "high_yuan": None,
                    "low_yuan": None,
                    "close_yuan": None,
                    "volume_shares": None,
                    "amount_cny": None,
                    "prev_close_yuan": None,
                    "prev_close_5_yuan": None,
                    "prev_close_10_yuan": None,
                    "total_mv_cny": _scaled(row[8], 0.0001),
                    "prev_circ_mv_cny": _scaled(circ_mv_evidence.accepted_value, 0.0001),
                    "circ_mv_source_date": circ_mv_evidence.source_date,
                    "circ_mv_staleness_trading_days": circ_mv_evidence.staleness_trading_days,
                    "circ_mv_crossed_pit_entry_boundary": circ_mv_evidence.crossed_pit_entry_boundary,
                    "circ_mv_pit_eligible_start": row[7],
                    "circ_mv_history_start": self.spec.effective_circ_mv_history_start,
                    "circ_mv_lookback_contract_version": CIRC_MV_LOOKBACK_CONTRACT_VERSION,
                    "circ_mv_fact_status": circ_mv_evidence.fact_status,
                    "circ_mv_reason_code": circ_mv_evidence.reason_code,
                    "buy_sm_amount_cny": None,
                    "sell_sm_amount_cny": None,
                    "buy_elg_amount_cny": None,
                    "sell_elg_amount_cny": None,
                    "net_mf_amount_cny": None,
                    "moneyflow_fact_status": "not_evaluated_missing_price",
                    "moneyflow_source_identity": moneyflow_resolution.evidence(),
                    "moneyflow_provider_absence": None,
                    "up_limit_yuan": None,
                }
        finally:
            cursor.close()


def _scaled(value: Any, divisor: float) -> float | None:
    if value is None:
        return None
    return float(value) / divisor


def load_mapping_manifest(reader: PostgresStockFactReader) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    industry_pit_adapter = getattr(reader, "industry_pit_adapter", None)
    if industry_pit_adapter is not None:
        return (
            dict(
                industry_pit_adapter.mapping_manifest(
                    universe_key=reader.spec.universe_key,
                    source_start=reader.spec.source_start,
                    source_end=reader.spec.source_end,
                )
            ),
            dict(industry_pit_adapter.constituents),
        )
    digest = hashlib.sha256()
    count = 0
    l1_l2: dict[str, set[str]] = {}
    prior: tuple[Any, ...] | None = None
    canonical_identity_count = 0
    previous_canonical: tuple[date, str, str, str] | None = None
    for row in reader.iter_mapping_source_rows():
        serialized = {key: (value.isoformat() if isinstance(value, date) else value) for key, value in row.items()}
        digest.update(canonical_json_bytes(serialized))
        digest.update(b"\n")
        count += 1
        l1_l2.setdefault(str(row["l1_code"]), set()).add(str(row["l2_code"]))
        canonical = (row["trade_date"], row["symbol"], row["l1_code"], row["l2_code"])
        if canonical != previous_canonical:
            canonical_identity_count += 1
            previous_canonical = canonical
        ordering = (
            row["trade_date"],
            row["symbol"],
            row["l1_code"],
            row["l2_code"],
            row["in_date"],
            row["out_date"] or date.max,
        )
        if prior is not None and ordering < prior:
            raise StateModelSetError("mapping source rows are not in canonical order")
        prior = ordering
    if len(l1_l2) != 31 or len({code for codes in l1_l2.values() for code in codes}) != 131:
        raise StateModelSetError("mapping source does not cover canonical L1=31/L2=131")
    constituents = {
        code: {
            "schema_version": "hmm_risk_l1_pit_l2_constituents_v1",
            "l1_code": code,
            "l2_codes": sorted(codes),
            "source_window_start": reader.spec.source_start.isoformat(),
            "source_window_end": reader.spec.source_end.isoformat(),
        }
        for code, codes in sorted(l1_l2.items())
    }
    manifest = {
        "schema_version": "hmm_risk_pit_mapping_manifest_v1",
        "universe_key": reader.spec.universe_key,
        "source_window_start": reader.spec.source_start.isoformat(),
        "source_window_end": reader.spec.source_end.isoformat(),
        "source_row_count": count,
        "canonical_identity_count": canonical_identity_count,
        "source_jsonl_sha256": digest.hexdigest(),
        "canonical_l1_count": len(l1_l2),
        "canonical_l2_count": len({code for codes in l1_l2.values() for code in codes}),
        "constituent_manifest_hash": hashlib.sha256(canonical_json_bytes(constituents)).hexdigest(),
    }
    return manifest, constituents


def _record_source_evidence(
    row: dict[str, Any],
    *,
    expected_circ_mv_history_start: date,
    provider_absence_keys: list[dict[str, str]],
    alias_resolution_keys: list[dict[str, str]],
    circ_mv_stale_keys: list[dict[str, Any]],
    circ_mv_pit_boundary_crossing_keys: list[dict[str, Any]],
) -> None:
    trade_date_value = row.get("trade_date")
    symbol = str(row.get("symbol") or "")
    if row.get("moneyflow_fact_status") == "provider_absence" and isinstance(trade_date_value, date):
        provider_absence = row.get("moneyflow_provider_absence")
        if not isinstance(provider_absence, dict):
            raise StateModelSetError("hmm_risk_stock_fact_provider_absence_unverified: row evidence is missing")
        provider_absence_keys.append(
            {
                "trade_date": trade_date_value.isoformat(),
                "canonical_ts_code": symbol,
                "source_ts_code": str(provider_absence.get("source_ts_code") or ""),
                "provider_audit_receipt_sha256": str(provider_absence.get("provider_audit_receipt_sha256") or ""),
                "row_hash": str(provider_absence.get("row_hash") or ""),
            }
        )
    identity = row.get("moneyflow_source_identity")
    if (
        isinstance(identity, dict)
        and identity.get("resolution_kind") == "explicit_effective_alias"
        and isinstance(trade_date_value, date)
    ):
        alias_resolution_keys.append(
            {
                "trade_date": trade_date_value.isoformat(),
                "canonical_ts_code": symbol,
                "source_ts_code": str(identity.get("source_ts_code") or ""),
                "row_hash": str(identity.get("row_hash") or ""),
            }
        )
    staleness = row.get("circ_mv_staleness_trading_days")
    circ_mv_source_date = row.get("circ_mv_source_date")
    pit_eligible_start = row.get("circ_mv_pit_eligible_start")
    circ_mv_history_start = row.get("circ_mv_history_start")
    declared_crossing = row.get("circ_mv_crossed_pit_entry_boundary")
    fact_status = row.get("circ_mv_fact_status")
    reason_code = row.get("circ_mv_reason_code")
    status_reason_codes = {
        "source_unavailable": "hmm_risk_stock_fact_circ_mv_source_unavailable",
        "causal_source_invalid": "hmm_risk_stock_fact_circ_mv_causal_source_invalid",
        "latest_value_missing": "hmm_risk_stock_fact_circ_mv_latest_value_missing",
        "latest_value_non_numeric": "hmm_risk_stock_fact_circ_mv_latest_value_non_numeric",
        "latest_value_non_finite": "hmm_risk_stock_fact_circ_mv_latest_value_non_finite",
        "latest_value_non_positive": "hmm_risk_stock_fact_circ_mv_latest_value_non_positive",
    }
    if not (
        isinstance(trade_date_value, date)
        and isinstance(pit_eligible_start, date)
        and isinstance(circ_mv_history_start, date)
        and circ_mv_history_start == expected_circ_mv_history_start
        and isinstance(declared_crossing, bool)
        and row.get("circ_mv_lookback_contract_version") == CIRC_MV_LOOKBACK_CONTRACT_VERSION
    ):
        raise StateModelSetError("hmm_risk_stock_fact_circ_mv_evidence_contract_invalid")
    derived_crossing = bool(isinstance(circ_mv_source_date, date) and circ_mv_source_date < pit_eligible_start)
    if declared_crossing != derived_crossing:
        raise StateModelSetError("hmm_risk_stock_fact_circ_mv_pit_boundary_evidence_invalid")
    if fact_status == "available":
        try:
            available_value = float(row.get("prev_circ_mv_cny"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise StateModelSetError("hmm_risk_stock_fact_circ_mv_evidence_contract_invalid") from exc
        if not math.isfinite(available_value) or available_value <= 0 or reason_code is not None:
            raise StateModelSetError("hmm_risk_stock_fact_circ_mv_evidence_contract_invalid")
    elif fact_status in status_reason_codes:
        if row.get("prev_circ_mv_cny") is not None or reason_code != status_reason_codes[fact_status]:
            raise StateModelSetError("hmm_risk_stock_fact_circ_mv_evidence_contract_invalid")
    else:
        raise StateModelSetError("hmm_risk_stock_fact_circ_mv_evidence_contract_invalid")
    if (
        isinstance(staleness, int)
        and staleness > 0
        and isinstance(circ_mv_source_date, date)
        and isinstance(trade_date_value, date)
    ):
        circ_mv_stale_keys.append(
            {
                "trade_date": trade_date_value.isoformat(),
                "canonical_ts_code": symbol,
                "circ_mv_source_date": circ_mv_source_date.isoformat(),
                "staleness_trading_days": staleness,
            }
        )
    if derived_crossing:
        if not (
            isinstance(circ_mv_source_date, date)
            and isinstance(staleness, int)
            and staleness >= 0
            and circ_mv_history_start <= circ_mv_source_date < pit_eligible_start <= trade_date_value
            and row.get("circ_mv_lookback_contract_version") == CIRC_MV_LOOKBACK_CONTRACT_VERSION
        ):
            raise StateModelSetError("hmm_risk_stock_fact_circ_mv_pit_boundary_evidence_invalid")
        circ_mv_pit_boundary_crossing_keys.append(
            {
                "trade_date": trade_date_value.isoformat(),
                "canonical_ts_code": symbol,
                "circ_mv_source_date": circ_mv_source_date.isoformat(),
                "circ_mv_history_start": circ_mv_history_start.isoformat(),
                "pit_eligible_start": pit_eligible_start.isoformat(),
                "staleness_trading_days": staleness,
                "lookback_contract_version": CIRC_MV_LOOKBACK_CONTRACT_VERSION,
                "fact_status": fact_status,
                "reason_code": reason_code,
            }
        )


def _stock_fact_manifest(
    reader: PostgresStockFactReader,
    *,
    sector_level: str,
    min_coverage: float,
    raw_count: int,
    missing_row_count: int,
    raw_jsonl_sha256: str,
    aggregates: list[L1DailyAggregate],
    invalid_sector_dates: list[dict[str, Any]],
    provider_absence_keys: list[dict[str, str]],
    alias_resolution_keys: list[dict[str, str]],
    circ_mv_stale_keys: list[dict[str, Any]],
    circ_mv_pit_boundary_crossing_keys: list[dict[str, Any]],
) -> dict[str, Any]:
    provider_absence_keys.sort(key=lambda item: (item["trade_date"], item["canonical_ts_code"], item["source_ts_code"]))
    alias_resolution_keys.sort(key=lambda item: (item["trade_date"], item["canonical_ts_code"], item["source_ts_code"]))
    circ_mv_stale_keys.sort(
        key=lambda item: (item["trade_date"], item["canonical_ts_code"], item["circ_mv_source_date"])
    )
    circ_mv_pit_boundary_crossing_keys.sort(
        key=lambda item: (item["trade_date"], item["canonical_ts_code"], item["circ_mv_source_date"])
    )
    circ_mv_pit_boundary_available_count = sum(
        item["fact_status"] == "available" for item in circ_mv_pit_boundary_crossing_keys
    )
    circ_mv_pit_boundary_invalid_count = len(circ_mv_pit_boundary_crossing_keys) - circ_mv_pit_boundary_available_count
    manifest = {
        "schema_version": "hmm_risk_stock_fact_dataset_manifest_v1",
        "source_window_start": reader.spec.source_start.isoformat(),
        "source_window_end": reader.spec.source_end.isoformat(),
        "raw_row_count": raw_count,
        "missing_non_suspended_price_row_count": missing_row_count,
        "security_source_identity": reader.security_identity_manifest.evidence(),
        "provider_absence_authority": reader.provider_absence_manifest.evidence(),
        "moneyflow_provider_absence_count": len(provider_absence_keys),
        "moneyflow_provider_absence_key_sha256": hashlib.sha256(
            canonical_json_bytes(provider_absence_keys)
        ).hexdigest(),
        "moneyflow_alias_resolution_count": len(alias_resolution_keys),
        "moneyflow_alias_resolution_key_sha256": hashlib.sha256(
            canonical_json_bytes(alias_resolution_keys)
        ).hexdigest(),
        "circ_mv_asof_stale_count": len(circ_mv_stale_keys),
        "circ_mv_asof_max_staleness_trading_days": max(
            (int(item["staleness_trading_days"]) for item in circ_mv_stale_keys),
            default=0,
        ),
        "circ_mv_asof_stale_key_sha256": hashlib.sha256(canonical_json_bytes(circ_mv_stale_keys)).hexdigest(),
        "circ_mv_lookback_contract_version": CIRC_MV_LOOKBACK_CONTRACT_VERSION,
        "circ_mv_history_start": reader.spec.effective_circ_mv_history_start.isoformat(),
        "circ_mv_pit_boundary_crossing_count": len(circ_mv_pit_boundary_crossing_keys),
        "circ_mv_pit_boundary_crossing_available_count": circ_mv_pit_boundary_available_count,
        "circ_mv_pit_boundary_crossing_invalid_count": circ_mv_pit_boundary_invalid_count,
        "circ_mv_pit_boundary_crossing_key_sha256": hashlib.sha256(
            canonical_json_bytes(circ_mv_pit_boundary_crossing_keys)
        ).hexdigest(),
        "raw_jsonl_sha256": raw_jsonl_sha256,
        "aggregate_row_count": len(aggregates),
        "invalid_l1_date_count": len(invalid_sector_dates),
        "invalid_l1_dates": invalid_sector_dates,
        "aggregate_sha256": hashlib.sha256(
            canonical_json_bytes([{**item.__dict__, "trade_date": item.trade_date.isoformat()} for item in aggregates])
        ).hexdigest(),
        "min_count_coverage": min_coverage,
        "min_weight_coverage": min_coverage,
    }
    industry_pit_adapter = getattr(reader, "industry_pit_adapter", None)
    if industry_pit_adapter is not None:
        industry_pit_preflight = getattr(reader, "industry_pit_preflight", None)
        if industry_pit_preflight is None:
            raise StateModelSetError("HMM industry PIT stock-fact read requires a completed zero-fit preflight")
        manifest["industry_pit_authority"] = {
            "candidate_bundle_hash": industry_pit_adapter.authority_bundle.manifest["bundle_hash"],
            "classification_authority_receipt_hash": (
                industry_pit_adapter.classification_resolver.receipt.receipt_hash
            ),
            "index_membership_authority_receipt_hash": (
                industry_pit_adapter.index_membership_resolver.receipt.receipt_hash
            ),
            "preflight_canonical_hash": industry_pit_preflight["canonical_hash"],
            "preflight_resolved": industry_pit_preflight["resolved"],
            "preflight_unavailable": industry_pit_preflight["unavailable"],
        }
    if sector_level == "L2":
        manifest["schema_version"] = "hmm_risk_direct_l2_stock_fact_dataset_manifest_v1"
        manifest["direct_sector_level"] = "L2"
        manifest["invalid_sector_date_count"] = manifest.pop("invalid_l1_date_count")
        manifest["invalid_sector_dates"] = manifest.pop("invalid_l1_dates")
    return manifest


def _append_aggregate(
    rows: list[dict[str, Any]],
    *,
    min_coverage: float,
    sector_level: str,
    aggregates: list[L1DailyAggregate],
    invalid_sector_dates: list[dict[str, Any]],
) -> None:
    try:
        aggregates.append(aggregate_l1_day(rows, min_coverage=min_coverage))
    except ObservationCoverageError as exc:
        identity = (
            {"l1_code": exc.l1_code} if sector_level == "L1" else {"sector_level": "L2", "sector_code": exc.l1_code}
        )
        invalid_sector_dates.append(
            {
                "trade_date": exc.trade_date.isoformat(),
                **identity,
                "reason": "stock_coverage_insufficient",
                "count_coverage": exc.count_coverage,
                "weight_coverage": exc.weight_coverage,
                "missing_evidence": list(exc.missing_evidence),
            }
        )


def load_daily_aggregates(
    reader: PostgresStockFactReader,
    *,
    min_coverage: float = MIN_COVERAGE,
    sector_level: str = "L1",
) -> tuple[list[L1DailyAggregate], dict[str, Any]]:
    if sector_level not in {"L1", "L2"}:
        raise StateModelSetError("daily aggregate level must be L1 or L2")
    digest = hashlib.sha256()
    raw_count = 0
    aggregates: list[L1DailyAggregate] = []
    invalid_sector_dates: list[dict[str, Any]] = []
    moneyflow_provider_absence_keys: list[dict[str, str]] = []
    moneyflow_alias_resolution_keys: list[dict[str, str]] = []
    circ_mv_stale_keys: list[dict[str, Any]] = []
    circ_mv_pit_boundary_crossing_keys: list[dict[str, Any]] = []

    missing_rows = list(
        reader.iter_missing_price_rows()
        if sector_level == "L1"
        else reader.iter_missing_price_rows(sector_level=sector_level)
    )
    sort_code = "l1_code" if sector_level == "L1" else "l2_code"
    merged_rows = heapq.merge(
        reader.iter_stock_fact_rows()
        if sector_level == "L1"
        else reader.iter_stock_fact_rows(sector_level=sector_level),
        iter(missing_rows),
        key=lambda row: (row["trade_date"], row[sort_code], row["symbol"], row["l1_code"], row["l2_code"]),
    )

    def rows_with_hash() -> Iterator[dict[str, Any]]:
        nonlocal raw_count
        for row in merged_rows:
            _record_source_evidence(
                row,
                expected_circ_mv_history_start=reader.spec.effective_circ_mv_history_start,
                provider_absence_keys=moneyflow_provider_absence_keys,
                alias_resolution_keys=moneyflow_alias_resolution_keys,
                circ_mv_stale_keys=circ_mv_stale_keys,
                circ_mv_pit_boundary_crossing_keys=circ_mv_pit_boundary_crossing_keys,
            )
            serialized = {key: (value.isoformat() if isinstance(value, date) else value) for key, value in row.items()}
            digest.update(canonical_json_bytes(serialized))
            digest.update(b"\n")
            raw_count += 1
            if sector_level == "L2":
                projected = dict(row)
                projected["l1_code"] = row["l2_code"]
                projected["l1_name"] = row["l2_name"]
                yield projected
            else:
                yield row

    for _, group in itertools.groupby(
        rows_with_hash(),
        key=lambda row: (row["trade_date"], row["l1_code"]),
    ):
        _append_aggregate(
            list(group),
            min_coverage=min_coverage,
            sector_level=sector_level,
            aggregates=aggregates,
            invalid_sector_dates=invalid_sector_dates,
        )
    if not aggregates:
        raise StateModelSetError("PostgreSQL stock-fact source produced no aggregates")
    manifest = _stock_fact_manifest(
        reader,
        sector_level=sector_level,
        min_coverage=min_coverage,
        raw_count=raw_count,
        missing_row_count=len(missing_rows),
        raw_jsonl_sha256=digest.hexdigest(),
        aggregates=aggregates,
        invalid_sector_dates=invalid_sector_dates,
        provider_absence_keys=moneyflow_provider_absence_keys,
        alias_resolution_keys=moneyflow_alias_resolution_keys,
        circ_mv_stale_keys=circ_mv_stale_keys,
        circ_mv_pit_boundary_crossing_keys=circ_mv_pit_boundary_crossing_keys,
    )
    return aggregates, manifest


def load_direct_daily_aggregates(
    reader: PostgresStockFactReader,
    *,
    min_coverage: float = MIN_COVERAGE,
) -> tuple[list[L1DailyAggregate], dict[str, Any], list[L1DailyAggregate], dict[str, Any]]:
    """Build direct L1/L2 aggregates from one canonical stock-fact database stream."""

    missing_rows = list(reader.iter_missing_price_rows())
    merged_rows = heapq.merge(
        reader.iter_stock_fact_rows(),
        iter(missing_rows),
        key=lambda row: (row["trade_date"], row["l1_code"], row["symbol"], row["l2_code"]),
    )
    l1_digest = hashlib.sha256()
    l2_digest = hashlib.sha256()
    raw_count = 0
    l1_aggregates: list[L1DailyAggregate] = []
    l2_aggregates: list[L1DailyAggregate] = []
    l1_invalid: list[dict[str, Any]] = []
    l2_invalid: list[dict[str, Any]] = []
    provider_absence_keys: list[dict[str, str]] = []
    alias_resolution_keys: list[dict[str, str]] = []
    circ_mv_stale_keys: list[dict[str, Any]] = []
    circ_mv_pit_boundary_crossing_keys: list[dict[str, Any]] = []

    for _, day_group in itertools.groupby(merged_rows, key=lambda row: row["trade_date"]):
        day_rows = list(day_group)
        l1_rows = sorted(day_rows, key=lambda row: (row["l1_code"], row["symbol"], row["l2_code"]))
        l2_rows = sorted(day_rows, key=lambda row: (row["l2_code"], row["symbol"], row["l1_code"]))
        for row in l1_rows:
            _record_source_evidence(
                row,
                expected_circ_mv_history_start=reader.spec.effective_circ_mv_history_start,
                provider_absence_keys=provider_absence_keys,
                alias_resolution_keys=alias_resolution_keys,
                circ_mv_stale_keys=circ_mv_stale_keys,
                circ_mv_pit_boundary_crossing_keys=circ_mv_pit_boundary_crossing_keys,
            )
            serialized = {key: (value.isoformat() if isinstance(value, date) else value) for key, value in row.items()}
            l1_digest.update(canonical_json_bytes(serialized))
            l1_digest.update(b"\n")
            raw_count += 1
        for row in l2_rows:
            serialized = {key: (value.isoformat() if isinstance(value, date) else value) for key, value in row.items()}
            l2_digest.update(canonical_json_bytes(serialized))
            l2_digest.update(b"\n")
        for _, group in itertools.groupby(l1_rows, key=lambda row: row["l1_code"]):
            _append_aggregate(
                list(group),
                min_coverage=min_coverage,
                sector_level="L1",
                aggregates=l1_aggregates,
                invalid_sector_dates=l1_invalid,
            )
        projected_l2_rows = []
        for row in l2_rows:
            projected = dict(row)
            projected["l1_code"] = row["l2_code"]
            projected["l1_name"] = row["l2_name"]
            projected_l2_rows.append(projected)
        for _, group in itertools.groupby(projected_l2_rows, key=lambda row: row["l1_code"]):
            _append_aggregate(
                list(group),
                min_coverage=min_coverage,
                sector_level="L2",
                aggregates=l2_aggregates,
                invalid_sector_dates=l2_invalid,
            )
    if not l1_aggregates or not l2_aggregates:
        raise StateModelSetError("PostgreSQL stock-fact source produced no direct L1/L2 aggregates")
    l1_manifest = _stock_fact_manifest(
        reader,
        sector_level="L1",
        min_coverage=min_coverage,
        raw_count=raw_count,
        missing_row_count=len(missing_rows),
        raw_jsonl_sha256=l1_digest.hexdigest(),
        aggregates=l1_aggregates,
        invalid_sector_dates=l1_invalid,
        provider_absence_keys=list(provider_absence_keys),
        alias_resolution_keys=list(alias_resolution_keys),
        circ_mv_stale_keys=list(circ_mv_stale_keys),
        circ_mv_pit_boundary_crossing_keys=list(circ_mv_pit_boundary_crossing_keys),
    )
    l2_manifest = _stock_fact_manifest(
        reader,
        sector_level="L2",
        min_coverage=min_coverage,
        raw_count=raw_count,
        missing_row_count=len(missing_rows),
        raw_jsonl_sha256=l2_digest.hexdigest(),
        aggregates=l2_aggregates,
        invalid_sector_dates=l2_invalid,
        provider_absence_keys=list(provider_absence_keys),
        alias_resolution_keys=list(alias_resolution_keys),
        circ_mv_stale_keys=list(circ_mv_stale_keys),
        circ_mv_pit_boundary_crossing_keys=list(circ_mv_pit_boundary_crossing_keys),
    )
    return l1_aggregates, l1_manifest, l2_aggregates, l2_manifest
