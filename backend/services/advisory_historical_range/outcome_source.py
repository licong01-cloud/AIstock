"""Read-only PostgreSQL source provider for Phase 1R outcome evidence."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import require_sha256
from backend.services.advisory_phase1.outcome_engine import DailyPriceBar, PricePath, SourceMemberBinding
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    SourceRevisionSet,
    build_source_revision_set,
)


class HistoricalRangeOutcomeSourceError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = context or {}


class HistoricalRangeSymbolPathRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str = Field(min_length=1, max_length=32)
    start_trade_date: date
    end_trade_date: date
    label_as_of_trade_date: date
    source_available_at: datetime
    price_source: SourceMemberBinding
    adjustment_source: SourceMemberBinding
    tradability_source: SourceMemberBinding
    expected_source_revision_set_hash: str = Field(min_length=64, max_length=64)

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("expected_source_revision_set_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return require_sha256(value, field_name="expected_source_revision_set_hash")

    @model_validator(mode="after")
    def _range(self) -> "HistoricalRangeSymbolPathRequestV1":
        if self.end_trade_date < self.start_trade_date:
            raise ValueError("outcome source date range is invalid")
        if self.end_trade_date > self.label_as_of_trade_date:
            raise ValueError("outcome source request exceeds label-as-of")
        if self.source_available_at.tzinfo is None or self.source_available_at.utcoffset() is None:
            raise ValueError("source_available_at must be timezone-aware")
        return self


class HistoricalRangeSymbolPathReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    price_path: PricePath
    missing_trade_dates: tuple[date, ...] = ()
    row_count: int = Field(ge=0)
    content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("request_hash", "source_revision_set_hash", "content_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeSymbolPathReceiptV1":
        if self.row_count != len(self.price_path.bars):
            raise ValueError("source receipt row count differs from price path")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"content_hash"}))
        if self.content_hash is not None and self.content_hash != digest:
            raise ValueError("source receipt content hash differs")
        object.__setattr__(self, "content_hash", digest)
        return self


class HistoricalRangeOutcomeSourceRevisionBundleV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_revision_set: SourceRevisionSet
    price_source: SourceMemberBinding
    adjustment_source: SourceMemberBinding
    tradability_source: SourceMemberBinding


class PostgresHistoricalRangeOutcomeSourceProvider:
    """Queries only historical market relations through an injected connection factory."""

    def __init__(self, *, conn_factory: Callable[[], Any]) -> None:
        if conn_factory is None:
            raise ValueError("conn_factory is required; database settings cannot be guessed")
        self._conn_factory = conn_factory
        self._active_operation_hash: str | None = None
        self._operation_row_cache: dict[tuple[str, date, date], tuple[dict[str, Any], ...]] = {}

    def begin_operation(self, request_hash: str) -> None:
        """Scope historical row reuse to one exact refresh request."""

        request_hash = require_sha256(request_hash, field_name="request_hash")
        if request_hash != self._active_operation_hash:
            self._operation_row_cache.clear()
            self._active_operation_hash = request_hash

    def load_symbol_path(self, request: HistoricalRangeSymbolPathRequestV1) -> HistoricalRangeSymbolPathReceiptV1:
        request_hash = canonical_json_sha256(request.model_dump(mode="json"))
        rows = self._load_symbol_rows_as_of(
            symbol=request.symbol,
            start_trade_date=request.start_trade_date,
            end_trade_date=request.end_trade_date,
            label_as_of_trade_date=request.label_as_of_trade_date,
        )
        hashes = _source_partition_hashes(rows)
        bindings = {
            "PRICE_PATH": request.price_source,
            "ADJUSTMENT_PATH": request.adjustment_source,
            "TRADABILITY_PATH": request.tradability_source,
        }
        for role, binding in bindings.items():
            if binding.source_role != role or binding.partition_content_hash != hashes[role]:
                raise HistoricalRangeOutcomeSourceError(
                    "ADVISORY_HR_OUTCOME_SOURCE_REVISION_CONFLICT",
                    "historical source content differs from its frozen binding",
                    context={"symbol": request.symbol, "source_role": role},
                )

        missing_adjustment_dates = tuple(row["trade_date"] for row in rows if not row.get("adjustment_present"))
        missing_limit_dates = tuple(
            row["trade_date"] for row in rows if not row.get("suspended") and not row.get("limit_present")
        )
        if missing_adjustment_dates or missing_limit_dates:
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE",
                "historical adjustment or tradability evidence is incomplete",
                context={
                    "symbol": request.symbol,
                    "missing_adjustment_dates": tuple(item.isoformat() for item in missing_adjustment_dates),
                    "missing_limit_dates": tuple(item.isoformat() for item in missing_limit_dates),
                },
            )

        bars: list[DailyPriceBar] = []
        for row in rows:
            open_li = _decimal(row.get("open_li"))
            up_limit = _decimal(row.get("up_limit"))
            down_limit = _decimal(row.get("down_limit"))
            suspended = bool(row.get("suspended"))
            open_yuan = open_li / Decimal("1000") if open_li is not None else None
            entry_executable = not suspended and open_yuan is not None and (up_limit is None or open_yuan < up_limit)
            sell_executable = not suspended and open_yuan is not None and (down_limit is None or open_yuan > down_limit)
            bars.append(
                DailyPriceBar(
                    trade_date=row["trade_date"],
                    open_li=open_li,
                    high_li=_decimal(row.get("high_li")),
                    low_li=_decimal(row.get("low_li")),
                    close_li=_decimal(row.get("close_li")),
                    adj_factor=_decimal(row.get("adj_factor")),
                    entry_executable=entry_executable,
                    sell_executable=sell_executable,
                    source_available_at=request.source_available_at,
                    price_source=request.price_source,
                    adjustment_source=request.adjustment_source,
                    tradability_source=request.tradability_source,
                )
            )
        if not bars:
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE",
                "historical price path is unavailable",
                context={
                    "symbol": request.symbol,
                    "start": request.start_trade_date.isoformat(),
                    "end": request.end_trade_date.isoformat(),
                },
            )
        actual_dates = {item.trade_date for item in bars}
        missing_dates = tuple(
            item
            for item in self._trading_dates(request.start_trade_date, request.end_trade_date)
            if item not in actual_dates
        )
        return HistoricalRangeSymbolPathReceiptV1(
            request_hash=request_hash,
            source_revision_set_hash=request.expected_source_revision_set_hash,
            price_path=PricePath(symbol=request.symbol, bars=tuple(bars)),
            missing_trade_dates=missing_dates,
            row_count=len(bars),
        )

    def resolve_source_revision_bundle(
        self,
        *,
        symbol: str,
        start_trade_date: date,
        end_trade_date: date,
        label_as_of_ts: datetime,
    ) -> HistoricalRangeOutcomeSourceRevisionBundleV1:
        """Freeze exact retrospective DB partitions before outcome calculation."""

        if label_as_of_ts.tzinfo is None or label_as_of_ts.utcoffset() is None:
            raise ValueError("label_as_of_ts must be timezone-aware")
        rows = self._load_symbol_rows_as_of(
            symbol=symbol,
            start_trade_date=start_trade_date,
            end_trade_date=end_trade_date,
            label_as_of_trade_date=label_as_of_ts.date(),
        )
        partition = {
            "symbol": symbol.upper(),
            "start_trade_date": start_trade_date,
            "end_trade_date": end_trade_date,
            "label_as_of_trade_date": label_as_of_ts.date(),
        }
        parameter_hash = canonical_json_sha256(partition)
        cutoff_hash = canonical_json_sha256({"predicate": "trade_date <= label_as_of_trade_date", "version": "r4_v1"})
        hashes = _source_partition_hashes(rows)
        role_specs = (
            ("PRICE_PATH", "market.kline_daily_raw"),
            ("ADJUSTMENT_PATH", "market.adj_factor"),
            ("TRADABILITY_PATH", "market.suspend_d+market.stk_limit"),
        )
        role_row_counts = {
            "PRICE_PATH": len(rows),
            "ADJUSTMENT_PATH": sum(1 for row in rows if row.get("adjustment_present")),
            "TRADABILITY_PATH": sum(1 for row in rows if row.get("suspended") or row.get("limit_present")),
        }
        members: list[SourceRevisionMemberInput] = []
        for role, dataset in role_specs:
            query_hash = canonical_json_sha256(
                {"query_template_id": "advisory_hr_r4_symbol_path", "role": role, "version": "1"}
            )
            members.append(
                SourceRevisionMemberInput(
                    source_role=role,
                    dataset_name=dataset,
                    query_template_id=f"advisory_hr_r4_{role.lower()}",
                    query_template_version="1",
                    query_template_hash=query_hash,
                    bound_parameter_hash=parameter_hash,
                    enforced_cutoff_predicate_hash=cutoff_hash,
                    partition_key=partition,
                    revision_kind=SourceRevisionKind.PARTITION_CONTENT_HASH,
                    revision_id=f"ahr-r4-{role.lower()}-{hashes[role][:32]}",
                    availability_requirement=AvailabilityRequirement.LABEL_AS_OF,
                    business_min_date=start_trade_date,
                    business_max_date=end_trade_date,
                    available_at_min=label_as_of_ts,
                    available_at_max=label_as_of_ts,
                    schema_fingerprint=f"advisory_hr_r4_{role.lower()}_v1",
                    row_count=role_row_counts[role],
                    partition_content_hash=hashes[role],
                    quality_status=("PASS" if rows and role_row_counts[role] == len(rows) else "PENDING"),
                    reason_codes=(
                        ()
                        if rows and role_row_counts[role] == len(rows)
                        else ("ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE",)
                    ),
                    research_only=True,
                )
            )
        source_set = build_source_revision_set(
            query_registry_hash=canonical_json_sha256(
                {member.source_role: member.query_template_hash for member in members}
            ),
            requested_source_cutoff=label_as_of_ts,
            label_as_of_ts=label_as_of_ts,
            research_only=True,
            members=members,
        )
        by_role = {member.source_role: member for member in source_set.members}

        def binding(role: str) -> SourceMemberBinding:
            member = by_role[role]
            return SourceMemberBinding(
                source_role=role,
                source_member_key=member.member_key,
                partition_content_hash=member.partition_content_hash,
            )

        return HistoricalRangeOutcomeSourceRevisionBundleV1(
            source_revision_set=source_set,
            price_source=binding("PRICE_PATH"),
            adjustment_source=binding("ADJUSTMENT_PATH"),
            tradability_source=binding("TRADABILITY_PATH"),
        )

    def _load_symbol_rows_as_of(
        self,
        *,
        symbol: str,
        start_trade_date: date,
        end_trade_date: date,
        label_as_of_trade_date: date,
    ) -> tuple[dict[str, Any], ...]:
        if end_trade_date > label_as_of_trade_date:
            raise ValueError("outcome source path exceeds label-as-of")
        key = (symbol.upper(), start_trade_date, label_as_of_trade_date)
        rows = self._operation_row_cache.get(key)
        if rows is None:
            rows = self._load_symbol_rows(
                symbol=symbol,
                start_trade_date=start_trade_date,
                end_trade_date=label_as_of_trade_date,
            )
            self._operation_row_cache[key] = rows
        return tuple(row for row in rows if row["trade_date"] <= end_trade_date)

    def _load_symbol_rows(
        self,
        *,
        symbol: str,
        start_trade_date: date,
        end_trade_date: date,
    ) -> tuple[dict[str, Any], ...]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT price.trade_date, price.open_li, price.high_li, price.low_li, price.close_li,
                           adjustment.adj_factor,
                           (adjustment.ts_code IS NOT NULL) AS adjustment_present,
                           EXISTS (
                               SELECT 1 FROM market.suspend_d suspended
                               WHERE suspended.ts_code = price.ts_code
                                 AND suspended.trade_date = price.trade_date
                                 AND suspended.suspend_type = 'S'
                           ) AS suspended,
                           limits.up_limit, limits.down_limit,
                           (limits.ts_code IS NOT NULL) AS limit_present
                    FROM market.kline_daily_raw AS price
                    LEFT JOIN market.adj_factor AS adjustment
                      ON adjustment.ts_code = price.ts_code
                     AND adjustment.trade_date = price.trade_date
                    LEFT JOIN market.stk_limit AS limits
                      ON limits.ts_code = price.ts_code
                     AND limits.trade_date = price.trade_date
                    WHERE price.ts_code = %s
                      AND price.trade_date >= %s
                      AND price.trade_date <= %s
                    ORDER BY price.trade_date
                    """,
                    (symbol.upper(), start_trade_date, end_trade_date),
                )
                rows = tuple(dict(row) for row in cur.fetchall())
            conn.rollback()
        return rows

    def load_industry_at_t(self, *, symbol: str, decision_trade_date: date) -> tuple[str, str]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, in_date, out_date
                    FROM market.sw_index_member
                    WHERE ts_code = %s AND in_date <= %s AND (out_date IS NULL OR out_date >= %s)
                    ORDER BY in_date DESC, out_date DESC NULLS LAST,
                             l3_code NULLS LAST, l2_code NULLS LAST, l1_code NULLS LAST
                    """,
                    (symbol.upper(), decision_trade_date, decision_trade_date),
                )
                rows = tuple(dict(item) for item in cur.fetchall())
            conn.rollback()
        if not rows:
            return "UNKNOWN_AT_T", canonical_json_sha256(
                {"symbol": symbol.upper(), "decision_trade_date": decision_trade_date, "industry": "UNKNOWN_AT_T"}
            )
        industry_fields = ("l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name")
        distinct_industries = {tuple(row.get(field) for field in industry_fields) for row in rows}
        if len(distinct_industries) > 1:
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_INDUSTRY_MEMBERSHIP_CONFLICT",
                "multiple conflicting PIT industry memberships are valid at T",
                context={
                    "symbol": symbol.upper(),
                    "decision_trade_date": decision_trade_date.isoformat(),
                    "membership_count": len(rows),
                    "distinct_industry_count": len(distinct_industries),
                },
            )
        # Duplicate equivalent memberships are deterministic after the explicit ordering.
        payload = rows[0]
        industry = str(payload.get("l3_code") or payload.get("l2_code") or payload.get("l1_code") or "UNKNOWN_AT_T")
        return industry, canonical_json_sha256(payload)

    def load_pit_eligible_symbols(self, *, universe_key: str, decision_trade_date: date) -> tuple[tuple[str, ...], str]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_code FROM market.stock_universe_pit_spans
                    WHERE universe_key = %s AND eligible_start <= %s AND eligible_end >= %s
                    ORDER BY ts_code
                    """,
                    (universe_key, decision_trade_date, decision_trade_date),
                )
                symbols = tuple(str(row[0]).upper() for row in cur.fetchall())
            conn.rollback()
        if not symbols:
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE",
                "PIT eligible universe is unavailable",
                context={"universe_key": universe_key, "decision_trade_date": decision_trade_date.isoformat()},
            )
        return symbols, canonical_json_sha256(
            {"universe_key": universe_key, "decision_trade_date": decision_trade_date, "symbols": symbols}
        )

    def _trading_dates(self, start: date, end: date) -> tuple[date, ...]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cal_date FROM market.trading_calendar
                    WHERE cal_date >= %s AND cal_date <= %s AND is_trading = TRUE
                    ORDER BY cal_date
                    """,
                    (start, end),
                )
                dates = tuple(row[0] for row in cur.fetchall())
            conn.rollback()
        return dates


def _decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def _source_partition_hashes(rows: tuple[dict[str, Any], ...]) -> dict[str, str]:
    return {
        "PRICE_PATH": canonical_json_sha256(
            [{key: row.get(key) for key in ("trade_date", "open_li", "high_li", "low_li", "close_li")} for row in rows]
        ),
        "ADJUSTMENT_PATH": canonical_json_sha256(
            [
                {
                    "trade_date": row.get("trade_date"),
                    "adj_factor": row.get("adj_factor"),
                    "adjustment_present": bool(row.get("adjustment_present")),
                }
                for row in rows
            ]
        ),
        "TRADABILITY_PATH": canonical_json_sha256(
            [
                {
                    key: row.get(key)
                    for key in (
                        "trade_date",
                        "suspended",
                        "up_limit",
                        "down_limit",
                        "limit_present",
                    )
                }
                for row in rows
            ]
        ),
    }
