"""Bounded physical validators. Called by the shared worker, never a GET.

Provider observations are not themselves canonical certification. Dense stock
facts use independent dated scope; sparse scans must be tied to this revision.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import math

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from backend.data_service.security_source_identity import load_default_security_source_identity_manifest
from backend.services.tushare_dataset_specs import DATASET_REGISTRY
from .catalog import Obligation
from .contracts import CheckCounts, PartitionState, ValidationResult, finite_number, effective_partition


def rows(conn, query, parameters=()):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, parameters)
        return [dict(row) for row in cur.fetchall()]


def unverified(reason: str, **checks):
    return ValidationResult(PartitionState.UNVERIFIED,
        (CheckCounts("contract_check", 1, 0, 0, 0, 0, 1),), checks, reason=reason)


def key_result(expected, facts, *, required_fields=(), positive_fields=(), checks=None):
    """Denominator supplied independently. Facts may retain extra real securities."""
    expected = set(expected)
    if not expected:
        return unverified("independent_scope_empty", **(checks or {}))
    missing = sorted(expected - facts.keys())
    invalid = sorted(code for code in expected & facts.keys()
        if any(not finite_number(facts[code].get(field), positive=field in positive_fields)
               for field in required_fields))
    counts = CheckCounts("security_date", len(expected), len(expected)-len(missing)-len(invalid), 0,
                         len(missing), len(invalid), 0)
    gaps = tuple({"classification": kind, "symbols": values} for kind, values in
                 (("MISSING", missing), ("INVALID", invalid)) if values)
    return ValidationResult(PartitionState.VALID if counts.complete else PartitionState.INCOMPLETE,
                            (counts,), {**(checks or {}), "physical_rows": len(facts)}, gaps)


def index_quote_result(expected, facts):
    """A published index needs close; close-only currency/return variants have no OHLC.

    Keep source NULLs explicit. Any provided quote must still be finite and
    economically valid; this is not a stock/minute or consumer-field exemption.
    """
    expected = set(expected)
    base = key_result(expected, facts, required_fields=("close",), positive_fields=("close",),
        checks={"required_fields": ["close"], "nullable_fields": ["open", "high", "low", "vol", "amount"],
                "schema_version": "published_index_quote_fields_v1"})
    legal, invalid, gaps = 0, 0, list(base.gaps)
    for field in ("open", "high", "low", "vol", "amount"):
        bad = []
        for code in expected & facts.keys():
            value = facts[code].get(field)
            if value is None:
                legal += 1
            elif (not finite_number(value, positive=field in {"open", "high", "low"})
                    or value < 0):
                bad.append(code)
        invalid += len(bad)
        if bad:
            gaps.append({"classification": "INVALID", "symbols": sorted(bad), "fields": [field]})
    total = 5*len(expected & facts.keys())
    optional = CheckCounts("published_optional_index_quote_cell", total, total-legal-invalid, legal, 0, invalid, 0)
    return ValidationResult(PartitionState.VALID if base.state == PartitionState.VALID and optional.complete
        else PartitionState.INCOMPLETE, base.counts+(optional,), base.checks, tuple(gaps))


def adj_factor_result(expected, facts, history=None):
    """A finite latest value cannot certify a known unresolved factor baseline."""
    result = key_result(expected, facts, required_fields=("adj_factor",), positive_fields=("adj_factor",))
    history = history or {}
    state = history.get("status")
    if state in {"RUNNING", "INTERRUPTED"}:
        return unverified("adj_factor_history_not_completed", physical_rows=len(facts), history_status=state)
    blocked = set(history.get("failed_symbols", ())) & set(expected)
    if state == "PARTIAL" and blocked:
        return ValidationResult(PartitionState.INCOMPLETE, result.counts + (
            CheckCounts("history_baseline", len(blocked), 0, 0, 0, len(blocked), 0),),
            {**result.checks, "history_status": state}, result.gaps + (
                {"classification": "HISTORY_CONFLICT", "symbols": sorted(blocked)},))
    return result


def _volume_ratio_na(value):
    # Legacy pandas imports stored SQL numeric NaN instead of NULL. This is
    # only a representation test; independent listing/calendar proof is still
    # required. Infinity, strings and other required fields are not exempted.
    return (value is None or isinstance(value, Decimal) and value.is_nan()
            or isinstance(value, float) and math.isnan(value))


def daily_basic_result(expected, facts, *, volume_ratio_warmup=None):
    """Keep ingestion's admission ratio, but never call residual gaps complete.

    Nullable valuations remain legal NA; the existing five mandatory fields
    have no blanket five-percent missing-data exemption in the day ledger.
    A new listing has no five-prior-session volume ratio yet; only proven
    calendar/listing warmup may retain the real source NULL as explicit NA.
    """
    from backend.services.tushare_sync_engine import (
        DAILY_BASIC_REQUIRED_FIELDS, DAILY_BASIC_REQUIRED_RATIO, _daily_basic_required_field_coverage_receipt,
    )
    expected = set(expected)
    keys = key_result(expected, facts)
    matched = expected & facts.keys()
    coverage = _daily_basic_required_field_coverage_receipt([facts[code] for code in sorted(matched)])
    field_counts, gaps = [], list(keys.gaps)
    warmup = volume_ratio_warmup or {}
    for field in DAILY_BASIC_REQUIRED_FIELDS:
        legal = {code for code in matched if field == "volume_ratio" and code in warmup
                 and _volume_ratio_na(facts[code].get(field))}
        missing = sorted(code for code in expected-legal if code not in facts or facts[code].get(field) is None)
        invalid = sorted(code for code in matched-legal if facts[code].get(field) is not None
            and not finite_number(facts[code][field], positive=field in {"total_mv", "circ_mv"}))
        field_counts.append(CheckCounts("field_cell:"+field, len(expected),
            len(expected)-len(missing)-len(invalid)-len(legal), len(legal), len(missing), len(invalid), 0))
        if legal:
            gaps.append({"classification": "LEGAL_NA", "symbols": sorted(legal), "fields": [field],
                         "reason": "new_listing_fewer_than_five_prior_trading_days"})
        gaps.extend({"classification": kind, "symbols": symbols, "fields": [field]}
                    for kind, symbols in (("MISSING", missing), ("INVALID", invalid)) if symbols)
    complete = keys.state == PartitionState.VALID and all(count.complete for count in field_counts)
    return ValidationResult(PartitionState.VALID if complete else PartitionState.INCOMPLETE,
        keys.counts+tuple(field_counts), {**keys.checks, "required_fields": list(DAILY_BASIC_REQUIRED_FIELDS),
            "existing_ingestion_admission_ratio": DAILY_BASIC_REQUIRED_RATIO,
            "coverage": coverage, "nullable_valuation_threshold_added": False,
            "volume_ratio_warmup": warmup}, tuple(gaps))


class PhysicalValidator:
    def _volume_ratio_warmup(self, conn, day, facts):
        suspects = [code for code, fact in facts.items() if _volume_ratio_na(fact.get("volume_ratio"))]
        if not suspects:
            return {}
        # stock_basic is the shared listing authority, not a consumer buy pool.
        # Calendar continuity and both endpoints must be proven: an incomplete
        # calendar cannot turn an old stock or an absent row into legal NA.
        listing = rows(conn, """SELECT b.ts_code,b.list_date,
                   count(*) FILTER (WHERE c.is_trading) AS listed_trading_days
            FROM market.stock_basic b JOIN market.trading_calendar c
              ON c.cal_date BETWEEN b.list_date AND %s
            WHERE b.ts_code=ANY(%s::text[]) AND b.list_date<=%s
            GROUP BY b.ts_code,b.list_date
            HAVING count(c.cal_date)=(%s::date-b.list_date+1)
               AND count(*) FILTER (WHERE c.is_trading) BETWEEN 1 AND 5
               AND bool_or(c.cal_date=b.list_date AND c.is_trading)
               AND bool_or(c.cal_date=%s AND c.is_trading)""", (day, suspects, day, day, day))
        return {str(r["ts_code"]).strip(): {"list_date": str(r["list_date"]),
            "listed_trading_days": int(r["listed_trading_days"])} for r in listing}

    def _raw_scheduler(self, conn, *, source_aliases=True):
        # Reuse the existing exact date/key contract without opening its usual
        # independent connections or initializing/running a scheduler.
        from backend.ingestion.tdx_scheduler import TDXScheduler
        adapter = object.__new__(TDXScheduler)
        adapter._fetchall = lambda query, parameters=(): rows(conn, query, parameters)
        original_codes = adapter._go_provider_codes
        authority = load_default_security_source_identity_manifest()

        def source_codes(start, end, **kwargs):
            if start != end:
                raise ValueError("integrity validation only accepts an exact fact date")
            return sorted({authority.resolve(code, start, "market.kline_daily_raw").source_ts_code
                           for code in original_codes(start, end, **kwargs)})
        if source_aliases:
            adapter._go_provider_codes = source_codes
        return adapter

    @staticmethod
    def _canonical_raw_codes(authority, codes, day):
        from backend.services.market_data.instrument_validator import normalize_ts_code
        from .stock_scope import is_excluded_stock_code
        # Same notation contract as shared QE/Selection/Paper readers; this
        # changes no economic identity, pool, date or stored quote.
        codes = {normalize_ts_code(code) for code in codes}
        codes = {code for code in codes if not is_excluded_stock_code(code)}
        aliases = {}
        for row in authority.rows:
            if (row.source_dataset == "market.kline_daily_raw" and row.effective_start is not None
                    and row.effective_end is not None and row.effective_start <= day <= row.effective_end):
                previous = aliases.setdefault(row.source_ts_code, row.canonical_ts_code)
                if previous != row.canonical_ts_code:
                    raise ValueError("ambiguous shared raw security identity")
        return {aliases.get(code, code) for code in codes}

    def _traded(self, conn, day, dataset):
        from scripts.build_stock_universe_pit_spans import a_share_ts_code_filter
        codes = rows(conn, "SELECT DISTINCT ts_code FROM market.kline_daily_raw WHERE trade_date=%s "
            "AND adjust_type='none' AND close_li>0 AND (volume_hand>0 OR volume_shares>0)"
            + a_share_ts_code_filter("ts_code"), (day,))
        authority = load_default_security_source_identity_manifest()
        canonical = self._canonical_raw_codes(authority, [str(r["ts_code"]).strip() for r in codes], day)
        return {authority.resolve(code, day, dataset).source_ts_code for code in canonical}

    def validate(self, conn, obligation: Obligation, day: date, revision: int) -> ValidationResult:
        dataset = obligation.dataset
        if dataset in {"kline_minute_raw", "daily_basic", "stk_limit", "moneyflow_ts", "cyq_perf", "bak_basic"}:
            from .catalog import SCOPE_VERSION, RULE_VERSION
            dependency = rows(conn, """SELECT * FROM market.dataset_date_refresh_audit
                WHERE dataset='kline_daily_raw' AND trade_date=%s AND data_source='canonical_db'""", (day,))
            status = effective_partition(dependency[0] if dependency else None,
                scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
            if not status["source_complete"]:
                return unverified("independent_traded_scope_pending",
                                  required_dependencies=["kline_daily_raw"])
        if dataset in {"kline_daily_raw", "kline_minute_raw"}:
            raw = self._raw_scheduler(conn)
            observed, _maximum, counts = raw._raw_date_observation(dataset, day, include_details=True)
            expected_keys = set(counts["expected_symbol_keys"])
            missing, invalid = set(counts["missing_symbols"]), set(counts["invalid_symbols"])
            legal = set(counts["legal_na_symbols"])
            if dataset == "kline_minute_raw" and missing:
                # Only the aggregate's suspect keys need timestamp readback;
                # reuse shared halt semantics, never exempt by row count alone.
                from backend.data_service.suspension_day_authority import required_minute_end_times
                from .catalog import CN
                start = datetime.combine(day, datetime.min.time(), CN)
                events = rows(conn, """SELECT ts_code,trade_date,suspend_type,suspend_timing
                    FROM market.suspend_d WHERE trade_date=%s AND ts_code=ANY(%s::character(9)[])""", (day, sorted(missing)))
                observations = rows(conn, """SELECT ts_code,(trade_time AT TIME ZONE 'Asia/Shanghai')::time AS clock
                    FROM market.kline_minute_raw WHERE ts_code=ANY(%s::character(9)[]) AND freq='1m'
                    AND adjust_type='none' AND trade_time>=%s AND trade_time<%s""", (sorted(missing), start, start+timedelta(days=1)))
                times = {}
                for row in observations:
                    times.setdefault(str(row["ts_code"]).strip(), set()).add(row["clock"])
                for code in sorted(missing-invalid):
                    required = required_minute_end_times(code, day, events)
                    if required is not None and required <= times.get(code, set()):
                        missing.remove(code)
                        legal.add(code)
            if not expected_keys and not legal:
                return unverified("independent_provider_scope_empty", row_count=observed)
            population = expected_keys | legal
            bad = invalid & population
            absent = (missing & population)-bad
            valid_legal = legal-bad-absent
            check = CheckCounts("security_date", len(population), len(population-bad-absent-valid_legal),
                                len(valid_legal), len(absent), len(bad), 0)
            extra_bad = invalid-population
            extra = CheckCounts("invalid_extra_physical_fact", len(extra_bad), 0, 0, 0, len(extra_bad), 0)
            return ValidationResult(PartitionState.VALID if check.complete and extra.complete else PartitionState.INCOMPLETE,
                (check, extra), {"raw_date_completeness": counts, "row_count": observed,
                           "scope": "full_provider_not_buy_universe"},
                tuple({"classification": kind, "symbols": sorted(values), "fact_date": str(day)}
                      for kind, values in (("MISSING", absent), ("INVALID", invalid)) if values))
        if dataset in {"daily_basic", "adj_factor", "stk_limit", "moneyflow_ts", "cyq_perf", "bak_basic"}:
            table = f"market.{dataset}"
            if dataset == "adj_factor":
                authority = load_default_security_source_identity_manifest()
                expected = self._canonical_raw_codes(authority,
                    self._raw_scheduler(conn, source_aliases=False)._go_provider_codes(day, day), day)
            else:
                expected = self._traded(conn, day, table if dataset != "adj_factor" else "market.kline_daily_raw")
            if not expected:
                return unverified("independent_stock_scope_empty")
            observed = rows(conn, sql.SQL("SELECT * FROM {}.{} WHERE trade_date=%s").format(
                sql.Identifier("market"), sql.Identifier(dataset)), (day,))
            from .stock_scope import filter_stock_rows
            observed = filter_stock_rows(observed)
            facts = {str(row["ts_code"]).strip(): row for row in observed}
            if len(facts) != len(observed):
                return ValidationResult(PartitionState.ERROR, (), {}, reason="duplicate_physical_key")
            if dataset == "daily_basic":
                return daily_basic_result(expected, facts,
                    volume_ratio_warmup=self._volume_ratio_warmup(conn, day, facts))
            if dataset in {"cyq_perf", "bak_basic"}:
                # These are dated stock panels, not sparse event catalogues.
                # Independent traded keys certify stored history; never invent
                # an old provider scan or use a present-day listing snapshot.
                return key_result(expected, facts,
                    required_fields=("his_low", "his_high", "cost_15pct", "cost_50pct", "cost_85pct", "weight_avg", "winner_rate")
                        if dataset == "cyq_perf" else (),
                    checks={"coverage_basis": "independent_dated_traded_stock_keys",
                            "historical_provider_scan_required": False,
                            "required_fields": ["his_low", "his_high", "cost_15pct", "cost_50pct", "cost_85pct", "weight_avg", "winner_rate"]
                                if dataset == "cyq_perf" else []})
            if dataset == "stk_limit":
                from backend.services.tushare_sync_engine import _stk_limit_source_prices_valid
                result = key_result(expected, facts)
                invalid = sorted(code for code in set(expected) & facts.keys()
                    if not _stk_limit_source_prices_valid(*(facts[code].get(f) for f in ("pre_close", "up_limit", "down_limit"))))
                missing = len(set(expected)-facts.keys())
                count = CheckCounts("traded_equity_limit", len(set(expected)), len(set(expected))-len(invalid)-missing, 0,
                                    missing, len(invalid), 0)
                return ValidationResult(PartitionState.VALID if count.complete else PartitionState.INCOMPLETE,
                    (count,), {"price_contract": "source_reference_and_limits_no_estimation"},
                    result.gaps+(({"classification": "INVALID", "symbols": invalid},) if invalid else ()))
            if dataset == "moneyflow_ts":
                from backend.data_service.moneyflow_contract import TUSHARE_MONEYFLOW_AMOUNT_COLUMNS, TUSHARE_MONEYFLOW_VOLUME_COLUMNS
                return key_result(expected, facts, required_fields=TUSHARE_MONEYFLOW_AMOUNT_COLUMNS+TUSHARE_MONEYFLOW_VOLUME_COLUMNS,
                    checks={"source_units": {"amount": "10k_cny", "volume": "hand"}, "alias_authority": "shared_security_source_identity"})
            history_rows = rows(conn, """SELECT metadata->'adj_factor_history' AS history
                FROM market.dataset_date_refresh_audit WHERE dataset='adj_factor'
                  AND trade_date=%s AND data_source='canonical_db'""", (day,))
            history = history_rows[0].get("history") if history_rows else None
            return adj_factor_result(expected, facts, history)
        if dataset == "index_daily":
            # index_basic is a request catalogue (including nonquoted variants),
            # not a dated publication authority. Never turn it into fake gaps.
            return self._scan_contract(conn, obligation, day, revision)
        if dataset == "sw_daily":
            spec = DATASET_REGISTRY[dataset]
            with conn.cursor() as cur:
                cur.execute(spec.code_source_sql)
                expected = [str(row[0]).strip() for row in cur.fetchall()]
            actual = rows(conn, sql.SQL("SELECT * FROM {} WHERE trade_date=%s").format(
                sql.Identifier(*spec.target_table.split("."))), (day,))
            facts = {str(r["ts_code"]).strip(): r for r in actual}
            # The reviewed source query selects only published SW2021 catalog
            # indices; member-only/nonpublished sectors are not missing quotes.
            return key_result(expected, facts, required_fields=("open", "high", "low", "close", "vol", "amount"),
                positive_fields=("open", "high", "low", "close"),
                checks={"scope": "formal_provider_query", "query_sha256": hashlib.sha256(spec.code_source_sql.encode()).hexdigest()})
        if dataset == "sector_data":
            from backend.services.sector_data_builder import _PREFLIGHT_DAY_SQL, sector_fact_comparison_sql
            from backend.services.canonical_equity_pit import CANONICAL_PIT_AUTHORITY_ID
            from .stock_scope import stock_scope_sql
            with conn.cursor() as cur:
                cur.execute(_PREFLIGHT_DAY_SQL, {"trade_date": day, "authority_id": CANONICAL_PIT_AUTHORITY_ID})
                values = list(cur.fetchone())
                cur.execute("SELECT count(*) FROM market.sector_data WHERE trade_date=%s"+stock_scope_sql(), (day,))
                actual = cur.fetchone()[0]
            mismatches = rows(conn, sector_fact_comparison_sql(), {"trade_date": day, "authority_id": CANONICAL_PIT_AUTHORITY_ID})
            # Keep the activated producer's existing six semantic checks and
            # its separate unpublished-quote count; no new non-null blanket.
            bad = sum(bool(value) for value in values[:6])
            checks = {"producer_contract_counts": values[:6], "unpublished_quote_legal_na": values[6],
                      "physical_rows": actual, "scope": "activated_sector_producer_contract", "mismatch_count": len(mismatches)}
            invalid = bad+int(actual == 0)+int(bool(mismatches))
            count = CheckCounts("derived_producer_contract", 8, 8-invalid, 0, 0, invalid, 0)
            legal = int(values[6] or 0)
            return ValidationResult(PartitionState.VALID if count.complete else PartitionState.INCOMPLETE,
                                    (count, CheckCounts("unpublished_quote_stock_date", legal, 0, legal, 0, 0, 0)), checks, tuple(mismatches))
        return self._scan_contract(conn, obligation, day, revision)

    def _scan_contract(self, conn, obligation, day, revision):
        """Only a completed formal provider scan bound to R can certify sparsity.

        A row-count seed, old provider success or three empty retries is NOT a
        full-source scan. Unknown formal scopes remain explicit UNVERIFIED.
        """
        dataset = obligation.dataset
        observed = rows(conn, """SELECT * FROM market.dataset_date_refresh_audit
            WHERE dataset=%s AND trade_date=%s AND data_source<>'canonical_db' ORDER BY refreshed_at DESC""", (dataset, day))
        current = [r for r in observed if (r.get("metadata") or {}).get("integrity_observed_revision") == revision]
        if not current:
            return unverified("complete_source_scan_not_bound_to_revision")
        source = current[0]
        metadata = source.get("metadata") or {}
        if source["status"] != "success":
            return ValidationResult(PartitionState.WAIT_UPSTREAM, (), {"provider_observation": source["data_source"]},
                                    reason=source.get("failure_category") or "source_scan_failed")
        if source["quality_status"] not in {"ok", "empty_valid"}:
            return ValidationResult(PartitionState.INCOMPLETE, (), {"provider_quality": source["quality_status"]},
                                    reason=source.get("failure_category") or "source_quality_incomplete")
        # Physical seeds and post-hoc table COUNTs do not prove provider query completeness.
        if metadata.get("mode") in {"physical_audit_seed", "seed_existing_rows"} or metadata.get("audit_from_target_table"):
            return unverified("row_count_seed_is_not_a_complete_source_scan")
        spec = DATASET_REGISTRY.get(dataset)
        if dataset == "anns_metadata":
            from .provider import announcement_spec
            spec = announcement_spec()
        if spec is None:
            return unverified("formal_physical_validator_required", dataset=dataset)
        scan = metadata.get("provider_scan")
        if dataset == "index_daily" and metadata.get("provider_query_scope_complete") is not True:
            return unverified("index_quote_query_scope_incomplete")
        if scan:
            from .provider import verify_scan, scoped_scan_entries
            if obligation.category == "reference" and spec.snapshot_date_column is None:
                query = sql.SQL("SELECT {} FROM {}").format(
                    sql.SQL(",").join(sql.Identifier(c) for c in spec.columns), sql.Identifier(*spec.target_table.split(".")))
                actual = rows(conn, query)
            else:
                query = sql.SQL("SELECT {} FROM {} WHERE {}=%s").format(
                    sql.SQL(",").join(sql.Identifier(c) for c in spec.columns), sql.Identifier(*spec.target_table.split(".")),
                    sql.Identifier(spec.date_column))
                actual = rows(conn, query, (day,))
            missing, invalid = verify_scan(spec, scan, actual)
            from .stock_scope import filter_dataset_rows
            actual = filter_dataset_rows(spec, actual)
            expected = len(scoped_scan_entries(spec, scan))
            if obligation.category != "event" and expected == 0:
                return unverified("reference_or_series_provider_scope_empty")
            check = CheckCounts("provider_fact_identity", expected, expected-len(missing)-len(invalid), 0,
                                len(missing), len(invalid), 0)
            complete_scan = CheckCounts("completed_provider_scan", 1, 1, 0, 0, 0, 0)
            checks = {"provider": source["data_source"], "scan_sha256": scan["scan_sha256"],
                      "physical_rows": len(actual), "as_of_policy": "CURRENT_SOURCE_NOT_HISTORICAL_VINTAGE"}
            if dataset == "index_daily":
                quotes = index_quote_result({entry["key"][0] for entry in scan["entries"]},
                    {str(r["ts_code"]).strip(): r for r in actual})
                checks.update(coverage_basis="complete_query_returned_keys_not_catalogue_availability",
                              quote_field_contract=quotes.checks)
                gaps = tuple({"classification": kind, "keys": keys} for kind, keys in
                             (("MISSING", missing), ("INVALID", invalid)) if keys)+quotes.gaps
                return ValidationResult(PartitionState.VALID if check.complete and quotes.state == PartitionState.VALID
                    else PartitionState.INCOMPLETE, (check, complete_scan)+quotes.counts, checks,
                    gaps)
            if dataset == "suspend_d":
                from backend.data_service.suspension_day_authority import publication_quality
                publication, quality = publication_quality(actual, day)
                checks["suspension_day_authority"] = publication
                if quality["quality_status"] not in {"ok", "empty_valid"}:
                    return ValidationResult(PartitionState.INCOMPLETE, (check,), checks,
                                            reason="suspension_semantics_unresolved")
            return ValidationResult(PartitionState.VALID if check.complete else PartitionState.INCOMPLETE,
                (check, complete_scan), checks, tuple({"classification": category, "keys": keys}
                    for category, keys in (("MISSING", missing), ("INVALID", invalid)) if keys))
        if obligation.category == "event":
            if metadata.get("mode") != spec.query_mode.value:
                return unverified("event_scan_scope_not_proven")
            actual = rows(conn, sql.SQL("SELECT * FROM {} WHERE {}=%s").format(
                sql.Identifier(*spec.target_table.split(".")), sql.Identifier(spec.date_column)), (day,))
            from .stock_scope import filter_dataset_rows
            actual = filter_dataset_rows(spec, actual)
            if dataset == "suspend_d":
                from backend.data_service.suspension_day_authority import publication_quality
                publication, quality = publication_quality(actual, day)
                if quality["quality_status"] not in {"ok", "empty_valid"}:
                    return ValidationResult(PartitionState.INCOMPLETE, (), {"suspension_day_authority": publication},
                                            reason="suspension_semantics_unresolved")
            # Scan completion is a contract check, NOT an invented number of events.
            return ValidationResult(PartitionState.VALID, (CheckCounts("complete_event_scan", 1, 1, 0, 0, 0, 0),),
                {"provider": source["data_source"], "physical_event_rows": len(actual), "no_event_is_legal": True})
        expected = source.get("expected_rows")
        if expected is None or expected <= 0:
            return unverified("independent_series_or_reference_scope_unproven")
        if obligation.category == "reference":
            return unverified("reference_snapshot_contract_required")
        actual = rows(conn, sql.SQL("SELECT count(*) AS n FROM {} WHERE {}=%s").format(
            sql.Identifier(*spec.target_table.split(".")), sql.Identifier(spec.date_column)), (day,))[0]["n"]
        missing = max(0, expected-actual)
        count = CheckCounts("provider_published_row", expected, expected-missing, 0, missing, 0, 0)
        return ValidationResult(PartitionState.VALID if count.complete else PartitionState.INCOMPLETE,
                                (count,), {"provider": source["data_source"], "physical_rows": actual})
