"""SOURCE gates over verified frozen rows, with independent PIT/calendar keys.

No database, provider or repair calls are made here. Minute rows are reduced to
one summary per stock/day; only a calendar month is resident at a time.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
import hashlib
import math
from pathlib import Path
import re
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from backend.data_service.security_source_identity import (
    load_default_security_source_identity_manifest,
    load_security_source_identity_manifest,
)

from .canonical import canonical_json_bytes
from .canonical_stock_transformer import MINUTE_OPENING_AUCTION_TIME
from .monthly_source_audit import (
    MonthlySourceAuditError,
    SourceGateEvidence,
    TypedGap,
    validate_daily_minute_aggregates,
    validate_trading_calendar,
)
from .monthly_source_producer import SourceArtifact
from .monthly_unified import SOURCE_GATES
from .sealed_source_reader import CASSealedPartitionReader
from .monthly_sector_mapping import build_bound_sector_enricher, frozen_sector_mapping_binding
from .shared_sector_context import (
    build_release_sw_l2_code_map_payload,
    validate_release_sw_l2_code_map,
)
from .source_authority import (
    _BAK_BASIC_VALUES,
    _CYQ_VALUES,
    _MARGIN_DETAIL_VALUES,
    _MONEYFLOW_VALUES,
    SOURCE_REFRESH_AUDIT_RECEIPT_SCHEMA,
)
from .sw_l2_quote_policy import build_quote_availability_payload
from .shared_consumer_coverage import audit_causal_source_history


AUDIT_SCHEMA = "aistock_monthly_frozen_source_audit_v1"
_BOUNDS = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})")
_NUMERIC = (int, float, Decimal)
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _day(value: Any) -> date:
    return value if type(value) is date else date.fromisoformat(str(value)[:10])


def _finite(value: Any) -> bool:
    return isinstance(value, _NUMERIC) and not isinstance(value, bool) and math.isfinite(float(value))


def _ohlcv(row: Mapping[str, Any]) -> dict[str, float]:
    source = ("open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li")
    if not all(_finite(row.get(key)) for key in source):
        raise MonthlySourceAuditError("raw OHLCV has null/non-finite fields")
    values = {
        key: float(row[src]) / divisor
        for key, src, divisor in zip(
            ("open", "high", "low", "close", "vol", "amount"),
            source,
            (1000, 1000, 1000, 1000, 0.01, 1000),
            strict=True,
        )
    }
    if (
        min(values[key] for key in ("open", "high", "low", "close")) <= 0
        or values["high"] < max(values["open"], values["low"], values["close"])
        or values["low"] > min(values["open"], values["high"], values["close"])
        or values["vol"] < 0
        or values["amount"] < 0
    ):
        raise MonthlySourceAuditError("raw OHLCV violates domain")
    return values


@dataclass(slots=True)
class GateCounter:
    gate: str
    expected_count: int = 0
    observed_count: int = 0
    explained_count: int = 0
    missing_count: int = 0
    invalid_count: int = 0
    duplicate_count: int = 0
    exceptions: list[TypedGap] = field(default_factory=list)
    emit: Callable[[Mapping[str, Any]], None] | None = None
    expected_keys: Any = field(default_factory=hashlib.sha256)

    @property
    def status(self) -> str:
        return "BLOCKED" if self.missing_count or self.invalid_count or self.duplicate_count else "PASS"

    def issue(self, key: tuple[str, date], reason: str, dataset: str = "") -> None:
        if self.emit is not None:
            self.emit(
                {
                    "gate": self.gate,
                    "dataset": dataset,
                    "symbol": key[0],
                    "trade_date": key[1].isoformat(),
                    "reason": reason,
                }
            )

    def check(
        self,
        key: tuple[str, date],
        rows: list[Mapping[str, Any]],
        *,
        valid: Callable[[Mapping[str, Any]], bool],
        exception: TypedGap | None = None,
        dataset: str = "",
    ) -> None:
        self.expected_count += 1
        self.expected_keys.update(canonical_json_bytes((dataset, key[0], key[1].isoformat())) + b"\n")
        if not rows:
            if exception is not None:
                self.explained_count += 1
                self.exceptions.append(exception)
            else:
                self.missing_count += 1
                self.issue(key, "missing_fact", dataset)
            return
        self.observed_count += 1
        if len(rows) > 1:
            self.duplicate_count += len(rows) - 1
            self.issue(key, "duplicate_fact", dataset)
        if not all(valid(row) for row in rows):
            self.invalid_count += 1
            self.issue(key, "invalid_fact", dataset)


@dataclass(slots=True)
class MinuteSummary:
    trade_date: date
    count: int = 0
    labels: int = 0
    duplicate_count: int = 0
    invalid: bool = False
    open: float = math.nan
    high: float = -math.inf
    low: float = math.inf
    close: float = math.nan
    vol: float = 0
    amount: float = 0
    auction: dict[str, float] | None = None

    def add(self, row: Mapping[str, Any]) -> None:
        stamp = row["trade_time"]
        stamp = stamp if isinstance(stamp, datetime) else datetime.fromisoformat(str(stamp))
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(_SHANGHAI).replace(tzinfo=None)
        # The producer excludes one exact auction from its 240 core bars.
        # Raw daily economics still include that real opening trade.
        if stamp.time() == MINUTE_OPENING_AUCTION_TIME:
            if stamp.date() != self.trade_date:
                self.invalid = True
            elif self.auction is not None:
                self.duplicate_count += 1
                self.invalid = True
            else:
                try:
                    self.auction = _ohlcv(row)
                except MonthlySourceAuditError:
                    self.invalid = True
            return
        self.count += 1
        minute = stamp.hour * 60 + stamp.minute
        index = minute - 571 if 571 <= minute <= 690 else minute - 781 + 120 if 781 <= minute <= 900 else -1
        if stamp.date() != self.trade_date or stamp.second or stamp.microsecond or index < 0:
            self.invalid = True
            return
        bit = 1 << index
        if self.labels & bit:
            self.duplicate_count += 1
        self.labels |= bit
        try:
            value = _ohlcv(row)
        except MonthlySourceAuditError:
            self.invalid = True
            return
        if index == 0:
            self.open = value["open"]
        if index == 239:
            self.close = value["close"]
        self.high = max(self.high, value["high"])
        self.low = min(self.low, value["low"])
        self.vol += value["vol"]
        self.amount += value["amount"]

    def aggregates(self) -> dict[str, float]:
        values = {key: getattr(self, key) for key in ("open", "high", "low", "close", "vol", "amount")}
        if self.auction is not None:
            values.update(
                open=self.auction["open"],
                high=max(self.high, self.auction["high"]),
                low=min(self.low, self.auction["low"]),
                vol=self.vol + self.auction["vol"],
                amount=self.amount + self.auction["amount"],
            )
        return values


def _index(rows: Iterable[Mapping[str, Any]], sessions: set[date]) -> dict[tuple[str, date], list[Mapping[str, Any]]]:
    output: dict[tuple[str, date], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        day = _day(row["trade_date"])
        if day in sessions:
            output[(str(row["ts_code"]), day)].append(row)
    return output


def audit_month_rows(
    rows: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    sessions: tuple[date, ...],
    pools: Mapping[str, Mapping[date, set[str]]],
    gates: Mapping[str, GateCounter],
    authority_sha256: str,
    minute_start: date,
    aliases: Any = None,
) -> tuple[dict[str, Any], set[tuple[str, date]]]:
    """Audit independent expected stock/date keys; never infer a denominator from rows."""
    dates = set(sessions)
    indexed = {name: _index(value, dates) for name, value in rows.items() if name != "kline_minute_raw"}
    summaries: dict[tuple[str, date], MinuteSummary] = {}
    for row in rows.get("kline_minute_raw", ()):
        stamp = row["trade_time"]
        stamp = stamp if isinstance(stamp, datetime) else datetime.fromisoformat(str(stamp))
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(_SHANGHAI)
        day = stamp.date()
        if day in dates:
            key = (str(row["ts_code"]), day)
            if key not in summaries:
                summaries[key] = MinuteSummary(day)
            summaries[key].add(row)
    expected = {(symbol, day) for day in sessions for symbol in pools["stock_universe"].get(day, ())}
    suspended = {
        key
        for key, values in indexed.get("suspend_d", {}).items()
        if any(row.get("suspend_type") == "S" and not row.get("suspend_timing") for row in values)
    }

    def facts(dataset: str, key: tuple[str, date]) -> list[Mapping[str, Any]]:
        source = (
            aliases.resolve(key[0], key[1], f"market.{dataset}").source_ts_code
            if aliases is not None and dataset in {"kline_daily_raw", "daily_basic", "stk_limit", "moneyflow_ts"}
            else key[0]
        )
        values = indexed.get(dataset, {}).get((source, key[1]), [])
        if source != key[0]:
            # A canonical row alongside its effective source alias is ambiguous;
            # do not silently prefer one of two purported economic facts.
            values = [*values, *indexed.get(dataset, {}).get(key, [])]
        return values

    for key in sorted(expected):

        def exception(dataset: str) -> TypedGap | None:
            return (
                TypedGap(
                    dataset, key[0], key[1].isoformat(), key[1].isoformat(), "*", "SUSPEND_FULL_DAY", authority_sha256
                )
                if key in suspended
                else None
            )

        daily = facts("kline_daily_raw", key)

        def daily_valid(row: Mapping[str, Any]) -> bool:
            try:
                _ohlcv(row)
                return True
            except MonthlySourceAuditError:
                return False

        gates["daily_price"].check(key, daily, valid=daily_valid, exception=exception("kline_daily_raw"))
        gates["adj_factor_history"].check(
            key,
            facts("adj_factor", key),
            valid=lambda row: _finite(row.get("adj_factor")) and float(row["adj_factor"]) > 0,
            exception=exception("adj_factor"),
        )
        gates["daily_basic_required_fields"].check(
            key,
            facts("daily_basic", key),
            valid=lambda row: all(
                _finite(row.get(name)) for name in ("turnover_rate", "turnover_rate_f", "volume_ratio")
            ) and all(
                _finite(row.get(name)) and float(row[name]) > 0 for name in ("total_mv", "circ_mv")
            ),
            exception=exception("daily_basic"),
        )
        limit = facts("stk_limit", key)
        gates["suspend_limit"].check(
            key,
            limit,
            valid=lambda row: (
                all(_finite(row.get(name)) and float(row[name]) > 0 for name in ("pre_close", "up_limit", "down_limit"))
                and float(row["up_limit"]) >= float(row["down_limit"])
            ),
            exception=exception("stk_limit"),
        )
        if key in suspended and (
            any(_finite(row.get("volume_hand")) and float(row["volume_hand"]) > 0 for row in daily)
            or (key in summaries and summaries[key].aggregates()["vol"] > 0)
        ):
            gates["suspend_limit"].invalid_count += 1
            gates["suspend_limit"].issue(key, "suspend_conflicts_with_trade")
        if key[1] >= minute_start:
            gate = gates["minute_price"]
            summary = summaries.get(key)
            gate.check(
                key, [{}] if summary is not None else [], valid=lambda _: True, exception=exception("kline_minute_raw")
            )
            if summary is not None:
                bad = summary.invalid or summary.count != 240 or summary.labels.bit_count() != 240
                gate.duplicate_count += summary.duplicate_count
                if not bad and daily and daily_valid(daily[0]):
                    try:
                        validate_daily_minute_aggregates(
                            symbol=key[0], daily=_ohlcv(daily[0]), aggregates=summary.aggregates()
                        )
                    except MonthlySourceAuditError:
                        bad = True
                if bad:
                    gate.invalid_count += 1
                    gate.issue(key, "minute_session_or_daily_parity_invalid")
        for pool, membership in pools.items():
            if key[0] not in membership.get(key[1], ()):
                continue
            gate = gates["pit_stock_pools"]
            present = bool(daily) and (key[1] < minute_start or key in summaries)
            gate.check(key, [{}] if present else [], valid=lambda _: True, exception=exception(pool), dataset=pool)
    return indexed, suspended


def _write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(payload) + b"\n")


def audit_margin_publication(
    *,
    day: date,
    rows: list[Mapping[str, Any]],
    receipt: Mapping[str, Any],
    gate: GateCounter,
    minimum_rows: int = 0,
    deferred_authority_sha256: str | None = None,
) -> None:
    """Independently declared provider denominator, not all-equity eligibility.

    Historical dates without a declared positive denominator remain blocked.
    A success label or a non-empty partial publication cannot manufacture PASS.
    """
    if not rows and deferred_authority_sha256 is not None:
        gate.check(("margin_detail", day), [], valid=lambda _: False, dataset="margin_detail",
            exception=TypedGap(dataset="margin_detail", symbol="margin_detail", start=day.isoformat(), end=day.isoformat(),
                field="*", reason_code="USER_DEFERRED_COLLECTION", authority_sha256=deferred_authority_sha256))
        return
    declarations = [
        source
        for entry in receipt.get("rows", ())
        if entry.get("dataset") == "margin_detail" and entry.get("trade_date") == day.isoformat()
        for source in entry.get("sources", ())
        if source.get("status") == "success"
        and source.get("error_present") is False
        and source.get("data_source") in receipt.get("eligible_sources", {}).get("margin_detail", ())
        and source.get("quality_status") in receipt.get("eligible_quality_statuses", {}).get("margin_detail", ())
    ]
    bounds = {source.get("expected_rows") for source in declarations}
    declared = len(bounds) == 1 and all(type(value) is int and value > 0 for value in bounds)
    expected = next(iter(bounds)) if declared else None
    keys = [str(row.get("ts_code")) for row in rows]
    unique = set(keys)
    valid = (
        declared
        and len(unique) >= max(expected, minimum_rows)
        and len(keys) == len(unique)
        and all(all(_finite(row.get(name)) for name in _MARGIN_DETAIL_VALUES) for row in rows)
    )
    gate.check(("margin_detail", day), [{}] if rows else [], valid=lambda _: valid, dataset="margin_detail")
    if not declared and not rows:
        gate.issue(("margin_detail", day), "provider_denominator_unproven", "margin_detail")
    gate.duplicate_count += len(keys) - len(unique)


def audit_frozen_source(
    *,
    cas: Any,
    frozen: Any,
    profile: Any,
    input_root: Path,
    artifact_root: Path,
    snapshot_group_id: str,
    changes: Any,
    predecessor_cutoff: date,
    checkpoint: Callable[[], None] = lambda: None,
    audit_causal_history: bool = True,
    deferred_margin_authority_sha256: str | None = None,
) -> tuple[tuple[SourceGateEvidence, ...], tuple[SourceArtifact, ...]]:
    # The shared builders depend on the build bridge, which imports the SOURCE
    # bundle schema. Load them only after adapter module initialization.
    from .monthly_shared_components import _index_pool_intervals, _pit_intervals
    from backend.services.tushare_dataset_specs import MARGIN_DETAIL

    descriptors = sorted(
        (item.as_build_input() for item in frozen.partitions),
        key=lambda item: (item["dataset"], str(item["partition_key"])),
    )
    reader = CASSealedPartitionReader(cas, descriptors, max_partition_rows=1_000_000)

    def stream(dataset: str, left: date | None = None, right: date | None = None):
        for descriptor in descriptors:
            if descriptor["dataset"] != dataset:
                continue
            bounds = _BOUNDS.search(str(descriptor["partition_key"]))
            if left is not None and bounds is not None and (_day(bounds[2]) < left or _day(bounds[1]) > right):
                continue
            checkpoint()
            with reader.iter_rows(dataset, str(descriptor["partition_key"])) as values:
                for index, value in enumerate(values, 1):
                    yield value
                    if index % 100_000 == 0:
                        checkpoint()
            checkpoint()

    calendar = tuple(_day(row["cal_date"]) for row in stream("trading_calendar"))
    audit_start = frozen.official_cutoff.replace(day=1)
    validate_trading_calendar(
        sessions=tuple(day for day in calendar if day >= audit_start), cutoff=frozen.official_cutoff,
    )
    pit = _pit_intervals(frozen.pit_snapshot, calendar)
    spans = {
        "stock_universe": pit,
        **_index_pool_intervals(
            membership_rows=tuple(stream("index_membership_pit")),
            pit_rows=pit,
            calendar=calendar,
            cutoff=frozen.official_cutoff,
        ),
    }
    stocks: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in stream("stock_basic"):
        stocks[str(row["ts_code"])].append(row)
    refresh = cas.get_json_bounded(frozen.source_audit_ref, max_bytes=64 * 1024 * 1024)
    if (
        not isinstance(refresh, Mapping)
        or refresh.get("schema_version") != SOURCE_REFRESH_AUDIT_RECEIPT_SCHEMA
        or refresh.get("cutoff") != frozen.official_cutoff.isoformat()
        or refresh.get("profile") != profile.profile
    ):
        raise MonthlySourceAuditError("frozen refresh audit identity differs")
    enricher = build_bound_sector_enricher(
        stream("sw_index_classify"), stream("sw_index_member"),
        binding=frozen_sector_mapping_binding(cas, frozen),
    )
    map_payload = build_release_sw_l2_code_map_payload(
        code_to_id=enricher.code_map,
        member_backed_codes=sorted({span.l2_code for values in enricher.memberships.values() for span in values}),
        authority_id="dataset_release_source_authority_v1",
        authority_sha256=frozen.source_manifest_ref.sha256,
    )
    quote = build_quote_availability_payload(
        code_map=validate_release_sw_l2_code_map(map_payload),
        required_start=profile.start_date,
        cutoff=frozen.official_cutoff,
    )
    availability = {row["canonical_l2_code"]: row["availability_spans"] for row in quote["entries"]}
    reverse_map = {value: code for code, value in enricher.code_map.items()}
    aliases = load_default_security_source_identity_manifest()
    alias_path = input_root / "security-source-identity.json"
    raw = aliases.source_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != aliases.file_sha256:
        raise MonthlySourceAuditError("security source authority changed during read")
    with alias_path.open("xb") as handle:
        handle.write(raw)
    aliases = load_security_source_identity_manifest(alias_path)
    quote_path = input_root / "quote-availability.json"
    _write(quote_path, quote)
    issues_path = input_root / "source-issues.ndjson"
    counters = {name: GateCounter(name) for name in SOURCE_GATES}
    # Historical repairs are separate operations. They must not expand an
    # ordinary monthly SOURCE audit back over the sealed predecessor.
    sessions = tuple(day for day in calendar if audit_start <= day <= frozen.official_cutoff)
    audited_months: list[str] = []
    with issues_path.open("xb") as issues:

        def emit(payload: Mapping[str, Any]) -> None:
            issues.write(canonical_json_bytes(payload) + b"\n")

        for counter in counters.values():
            counter.emit = emit
        # Check causal weights for this operation's affected sessions. Source
        # facts remain outside executable spans and are streamed once; no
        # database fallback, historical DataFrame or synthesized fact is used.
        if sessions and audit_causal_history:
            full_day_suspensions = frozenset(
                (str(row["ts_code"]), _day(row["trade_date"]))
                for row in stream("suspend_d")
                if row.get("suspend_type") == "S"
                and str(row.get("suspend_timing") or "").strip() in {"", "09:30-09:30"}
            )
            def causal_key(symbol: str, day: date, prior: Any) -> None:
                counters["daily_basic_required_fields"].check(
                    (symbol, day), [] if prior is None else [{"circ_mv": prior[1]}],
                    valid=lambda row: _finite(row.get("circ_mv")) and float(row["circ_mv"]) > 0,
                    dataset="daily_basic_strict_prior_circ_mv",
                )

            causal = audit_causal_source_history(
                stream("daily_basic"), calendar=calendar, spans=pit,
                history_start=profile.start_date,
                not_applicable_keys=full_day_suspensions, source_identity=aliases,
                required_dates=frozenset(sessions),
                on_required_key=causal_key,
                emit=lambda issue: emit({"gate": "daily_basic_required_fields",
                    "dataset": "daily_basic", "field": "strict_prior_circ_mv", **issue}),
            )
            causal_path = input_root / "causal-daily-basic-readback.json"
            _write(causal_path, causal)
        months = sorted({day.strftime("%Y-%m") for day in sessions})
        for month in months:
            checkpoint()
            dates = tuple(day for day in sessions if day.strftime("%Y-%m") == month)
            audited_months.append(month)
            pools = {
                name: {day: {code for code, start, end in values if start <= day <= end} for day in dates}
                for name, values in spans.items()
            }
            data = {
                name: stream(name, dates[0], dates[-1])
                for name in (
                    "kline_daily_raw",
                    "kline_minute_raw",
                    "adj_factor",
                    "daily_basic",
                    "suspend_d",
                    "stk_limit",
                    "moneyflow_ts",
                    "bak_basic",
                    "cyq_perf",
                    "margin_detail",
                    "sector_data",
                )
            }
            indexed, suspended = audit_month_rows(
                data,
                sessions=dates,
                pools=pools,
                gates=counters,
                authority_sha256=frozen.source_manifest_ref.sha256,
                minute_start=profile.minute_start_date,
                aliases=aliases,
            )
            for day in dates:
                checkpoint()
                counters["calendar_lifecycle"].check(
                    ("stock_universe", day),
                    [{}] if pools["stock_universe"][day] else [],
                    valid=lambda _: True,
                    dataset="stock_universe_pit",
                )
                for symbol in sorted(pools["stock_universe"][day]):
                    key = (symbol, day)
                    counters["calendar_lifecycle"].check(
                        key, stocks.get(symbol, []), valid=lambda row: row.get("list_date") is not None
                    )
                    for dataset, fields in (
                        ("moneyflow_ts", _MONEYFLOW_VALUES),
                        ("cyq_perf", _CYQ_VALUES),
                        ("bak_basic", _BAK_BASIC_VALUES),
                    ):
                        if (
                            dataset == "bak_basic"
                            and symbol in stocks
                            and stocks[symbol][0].get("list_date") is not None
                            and day < _day(stocks[symbol][0]["list_date"]) + date.resolution * 365
                        ):
                            continue  # Exact existing source query applicability contract.
                        source = (
                            aliases.resolve(symbol, day, "market.moneyflow_ts").source_ts_code
                            if dataset == "moneyflow_ts"
                            else symbol
                        )
                        values = indexed.get(dataset, {}).get((source, day), [])
                        if source != symbol:
                            values = [*values, *indexed.get(dataset, {}).get(key, [])]
                        exception = (
                            TypedGap(
                                dataset,
                                symbol,
                                day.isoformat(),
                                day.isoformat(),
                                "*",
                                "SUSPEND_FULL_DAY",
                                frozen.source_manifest_ref.sha256,
                            )
                            if key in suspended
                            else None
                        )
                        counters["financial_moneyflow"].check(
                            key,
                            values,
                            valid=lambda row, fields=fields, dataset=dataset: all(
                                _finite(row.get(name)) or (dataset == "bak_basic" and row.get(name) is None)
                                for name in fields
                            ),
                            exception=exception,
                            dataset=dataset,
                        )
                    values = indexed.get("sector_data", {}).get(key, [])
                    expected_id = enricher.enrich({"ts_code": symbol, "trade_date": day})["l2_code_id"]

                    def sector_valid(row: Mapping[str, Any]) -> bool:
                        sector_id = row.get("l2_code_id")
                        if sector_id != expected_id or sector_id not in reverse_map:
                            return False
                        code = reverse_map[sector_id]
                        available = any(
                            _day(span["start_date"]) <= day <= _day(span["end_date"]) for span in availability[code]
                        )
                        quotes = ("sw2_pct_change", "sw2_vol", "sw2_amount")
                        money = ("sw2_mf_net_amt", "sw2_mf_buy_elg_amt", "sw2_mf_sell_elg_amt")
                        return all(_finite(row.get(name)) for name in money) and (
                            all(_finite(row.get(name)) for name in quotes)
                            if available
                            else all(
                                row.get(name) is None
                                or (isinstance(row.get(name), _NUMERIC) and math.isnan(float(row[name])))
                                for name in quotes
                            )
                        )

                    counters["sector_authority"].check(key, values, valid=sector_valid)
                # margin_detail is not an all-equity source. A per-date audit must
                # bind a separately declared provider denominator, never its own
                # observed row count. The sealed ingestion receipt owns that bound.
                margin = [
                    row
                    for (symbol, row_day), values in indexed.get("margin_detail", {}).items()
                    if row_day == day
                    for row in values
                ]
                audit_margin_publication(
                    day=day,
                    rows=margin,
                    receipt=refresh,
                    gate=counters["financial_moneyflow"],
                    minimum_rows=MARGIN_DETAIL.min_expected_rows if day > predecessor_cutoff else 0,
                    deferred_authority_sha256=deferred_margin_authority_sha256 if day == frozen.official_cutoff else None,
                )
    result: list[SourceGateEvidence] = []
    artifacts = [
        SourceArtifact(path.relative_to(artifact_root).as_posix(), path)
        for path in (alias_path, quote_path, issues_path)
    ]
    if sessions and audit_causal_history:
        artifacts.append(SourceArtifact(causal_path.relative_to(artifact_root).as_posix(), causal_path))
    for name, counter in counters.items():
        expectation = input_root / "gates" / f"{name}-expectation.json"
        readback = input_root / "gates" / f"{name}-readback.json"
        expectation_id = expectation.relative_to(artifact_root).as_posix()
        readback_id = readback.relative_to(artifact_root).as_posix()
        gate = SourceGateEvidence(
            name,
            snapshot_group_id,
            expectation_id,
            readback_id,
            counter.expected_count,
            counter.observed_count,
            counter.explained_count,
            counter.missing_count,
            counter.duplicate_count,
            counter.invalid_count,
            tuple(counter.exceptions),
        )
        _write(
            expectation,
            {
                "schema_version": AUDIT_SCHEMA,
                "gate_id": name,
                "snapshot_group_id": snapshot_group_id,
                "expected_count": counter.expected_count,
                "expected_keys_sha256": counter.expected_keys.hexdigest(),
                "audited_months": audited_months,
                "session_count": len(sessions),
                "pit_snapshot_digest": frozen.pit_snapshot_digest,
                "source_manifest_ref": frozen.source_manifest_ref.as_dict(),
                "source_refresh_audit_ref": frozen.source_audit_ref.as_dict(),
                "alias_file_sha256": aliases.file_sha256,
                "quote_availability_digest": quote["quote_availability_digest"],
                "denominator_rule": "frozen_calendar_intersect_pit_spans_v1",
            },
        )
        _write(
            readback,
            {
                **gate.payload(),
                "issues_ref": issues_path.relative_to(artifact_root).as_posix(),
                "database_write_performed": False,
                "runtime_fallback": False,
            },
        )
        result.append(gate)
        artifacts.extend(
            SourceArtifact(path.relative_to(artifact_root).as_posix(), path) for path in (expectation, readback)
        )
    return tuple(result), tuple(artifacts)
