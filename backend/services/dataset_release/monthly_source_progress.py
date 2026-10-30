"""Expose actual SOURCE work without changing its data or sealing contract."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
import json
import math
from typing import Any, Callable, Mapping

from .canonical import canonical_json_bytes, digest_named_fields
from .source_authority import MonthlySourceAuthority, PRODUCTION_QUERY_SPECS, SourceProviderContractError
from backend.data_service.security_source_identity import MONEYFLOW_DATASET, load_default_security_source_identity_manifest
from .monthly_unified import MonthlyReleaseCancelled, MonthlyReleaseSourceBlocked


class MonthlySourcePartitionError(RuntimeError):
    def __init__(self, *, query_id: str, partition_key: str):
        super().__init__("monthly SOURCE partition failed; see exception_chain and partition context")
        self.context = {"query_id": query_id, "partition_key": partition_key}


class MonthlyObservedSourceAuthority(MonthlySourceAuthority):
    def __init__(self, *args: Any, progress: Callable[[Mapping[str, Any]], None],
                 month_start: date | None = None, construction_anchors=(), deferred_margin_cutoff: date | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        if month_start is not None and (type(month_start) is not date or month_start.day != 1):
            raise SourceProviderContractError("monthly source start must be the natural month start")
        self._month_start = month_start
        if deferred_margin_cutoff is not None and (
            type(deferred_margin_cutoff) is not date or deferred_margin_cutoff.replace(day=1) != month_start
            or deferred_margin_cutoff <= month_start
        ):
            raise SourceProviderContractError("deferred financing date is outside the target month")
        self._deferred_margin_cutoff = deferred_margin_cutoff
        self._construction_anchors = construction_anchors if callable(construction_anchors) else tuple(construction_anchors)
        self._progress = progress
        self._rows_validated = self._rows_sealed = self._partitions_sealed = 0

    def _database_query_specs(self):
        specs = super()._database_query_specs()
        return (*specs, PRODUCTION_QUERY_SPECS["adj_factor_construction"]) if self._month_start is not None else specs

    def _refresh_audit_start(self, cutoff):
        if self._month_start is None:
            return super()._refresh_audit_start(cutoff)
        if cutoff.replace(day=1) != self._month_start:
            raise SourceProviderContractError("refresh-audit cutoff differs from target month")
        return self._month_start

    def _freeze_refresh_audit(self, session, *, cutoff, checkpoint, read_chunk_rows=None):
        ledger = super()._freeze_refresh_audit(session, cutoff=cutoff, checkpoint=checkpoint,
                                             read_chunk_rows=read_chunk_rows)
        if self._month_start is None:
            return ledger
        # A source-wide verdict is never rewritten as consumer completeness.
        # These bounded reads share the SOURCE snapshot; later frozen fact gates
        # still check every required key and export still includes all facts.
        start = self._month_start
        fetch = read_chunk_rows or self.profile.resource_policy.validation_read_chunk_rows
        params = {"start": start, "end": cutoff, "cutoff": cutoff}
        days = tuple(d for d in ledger.trading_dates if start <= d <= cutoff)
        projections = {}

        def rows(query, extra=None):
            for row in session.stream(query, {**params, **(extra or {})}, fetch_rows=fetch):
                checkpoint()
                value = row.get("row_payload", row)
                yield json.loads(value) if isinstance(value, str) else value

        def stamp(value):
            return value if type(value) is date else date.fromisoformat(str(value))

        def proof(dataset, day, scope, evidence, reason):
            projections[(dataset, day)] = {
                "schema_version": "aistock_monthly_required_fact_projection_v1",
                "dataset": dataset, "trade_date": day.isoformat(), "reason": reason,
                "required_scope_sha256": digest_named_fields("monthly_required_fact_scope_v1", {"scope": scope}),
                "evidence_sha256": digest_named_fields("monthly_required_fact_evidence_v1", {"evidence": evidence}),
                "audit_rows_sha256": digest_named_fields("monthly_partition_audits_v1", {"rows": list(ledger.rows.get((dataset, day), ()))}),
                "unexplained_gap_count": 0, "global_source_state_preserved": True,
            }

        calendar = list(rows("trading_calendar"))
        calendar_days = {stamp(row["cal_date"]): row["is_trading"] for row in calendar}
        # The registered calendar SOURCE exports trading sessions only, not a
        # natural-day panel. Compare that native result with its independent
        # trading-date query; do not demand invented holiday audit jobs.
        if (days and len(calendar_days) == len(calendar) and set(calendar_days) == set(days)
                and all(value is True for value in calendar_days.values())):
            for day in days:
                proof("trading_calendar", day, list(days), calendar,
                      "official_calendar_same_snapshot_complete")

        index = {}
        required_from = {item.daily_code: item.required_from.isoformat() for item in self.profile.indices}
        for row in rows("index_daily", {"codes": list(self.profile.index_codes),
                    "index_required_from_json": canonical_json_bytes(required_from).decode("utf-8")}):
            index.setdefault((str(row["ts_code"]), stamp(row["trade_date"])), []).append(row)
        fields = PRODUCTION_QUERY_SPECS["index_daily"].non_null_value_columns
        for day in days:
            codes = [item.daily_code for item in self.profile.indices if item.required_from <= day]
            facts = [index.get((code, day), []) for code in codes]
            if facts and all(len(values) == 1 and all(
                    isinstance(values[0].get(name), (int, float)) and not isinstance(values[0][name], bool)
                    and math.isfinite(values[0][name]) for name in fields)
                    and min(values[0][name] for name in ("open", "high", "low", "close", "pre_close")) > 0
                    and values[0]["high"] >= max(values[0][name] for name in ("open", "low", "close"))
                    and values[0]["low"] <= min(values[0][name] for name in ("open", "high", "close"))
                    and min(values[0]["vol"], values[0]["amount"]) >= 0 for values in facts):
                proof("index_daily", day, codes, facts,
                      "fixed_release_indices_same_snapshot_complete")

        from backend.services.data_integrity.catalog import RULE_VERSION, SCOPE_VERSION
        from backend.services.data_integrity.contracts import effective_partition
        spans = list(rows("pit_spans", {"universe_key": self.profile.universe_key, "codes": None}))
        for day in days:
            members = {str(row["ts_code"]) for row in spans
                       if stamp(row["eligible_start"]) <= day <= stamp(row["eligible_end"])}
            if not members:
                continue
            for dataset in ("bak_basic", "cyq_perf"):
                for row in ledger.rows.get((dataset, day), ()):
                    if row.get("data_source") != "canonical_db":
                        continue
                    validation = row.get("source_validation") or {}
                    revision = {**dict(row.get("integrity_revision") or {}), "dataset": dataset,
                                "metadata": {"validation": validation}}
                    effective = effective_partition(revision, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
                    counts, gaps = validation.get("counts") or [], validation.get("gaps") or []
                    missing = {str(code) for gap in gaps for code in gap.get("symbols", ())}
                    if (effective["current"] and effective["state"] == "INCOMPLETE" and len(counts) == 1
                            and (validation.get("checks") or {}).get("coverage_basis") == "independent_dated_traded_stock_keys"
                            and missing and not missing & members and gaps
                            and all(gap.get("classification") == "MISSING" and gap.get("symbols")
                                    and gap.get("fact_date", day.isoformat()) == day.isoformat() for gap in gaps)
                            and all(type(counts[0].get(name)) is int and counts[0][name] >= 0
                                    for name in ("expected", "valid", "legal_na", "missing", "invalid", "unverified"))
                            and counts[0]["missing"] == len(missing)
                            and counts[0]["invalid"] == counts[0]["unverified"] == 0
                            and counts[0]["expected"] == counts[0]["valid"] + counts[0]["legal_na"] + len(missing)):
                        proof(dataset, day, {"universe_key": self.profile.universe_key,
                              "semantic_profile_digest": self.profile.semantic_profile_digest, "members": sorted(members)},
                              row, "verified_source_gaps_outside_release_consumer_scope")
        return replace(ledger, consumer_partition_projections=projections)

    def _partition_requests(self, query, cutoff, *, pit_snapshot, selected_stock_codes=()):
        if self._month_start is not None and cutoff.replace(day=1) != self._month_start:
            raise SourceProviderContractError("monthly SOURCE cutoff differs from natural month")
        if query.query_id == "adj_factor_construction":
            if self._month_start is None:
                raise SourceProviderContractError("construction facts require a monthly source request")
            codes = list(self._stock_source_codes(pit_snapshot))
            if selected_stock_codes:
                if not set(selected_stock_codes) <= set(codes):
                    raise SourceProviderContractError("construction source sample escapes fact population")
                codes = sorted(selected_stock_codes)
            anchors = []
            source_anchors = self._construction_anchors(pit_snapshot) if callable(self._construction_anchors) else self._construction_anchors
            for code, day in sorted(set(source_anchors)):
                if not isinstance(code, str) or type(day) is not date or day >= self._month_start:
                    raise SourceProviderContractError("construction anchor is not an exact historical boundary")
                if code in codes:
                    anchors.append({"ts_code": code, "trade_date": day.isoformat()})
            yield f"construction-{cutoff.isoformat()}", {
                "cutoff": cutoff, "source_start": self.profile.start_date, "codes": codes,
                "anchors_json": canonical_json_bytes(anchors).decode("utf-8"),
            }
            return
        source_cutoff = cutoff
        if query.query_id == "margin_detail" and self._deferred_margin_cutoff is not None:
            if self._deferred_margin_cutoff != cutoff:
                raise SourceProviderContractError("deferred financing cutoff differs from SOURCE")
            source_cutoff = cutoff - date.resolution
        requests = super()._partition_requests(
            query, source_cutoff, pit_snapshot=pit_snapshot, selected_stock_codes=selected_stock_codes,
            # Sparse suspension facts must repair inherited PIT-clipped prefixes.
            # This is materialization, not an historical price/refresh audit.
            start_override=self._month_start if query.date_expression is not None and query.query_id != "suspend_d" else None,
        )
        identity = load_default_security_source_identity_manifest() if query.query_id == "moneyflow_ts" else None
        for key, params in requests:
            if source_cutoff != cutoff:
                # Keep the shared logical month backing identity. Its actual
                # query end and the deliberately withheld date remain explicit
                # parameters; raw row counts/min/max never claim a cutoff fact.
                key = key.replace(source_cutoff.isoformat(), cutoff.isoformat())
                params = {**params, "user_deferred_trade_date": cutoff}
            if identity is not None:
                source_codes = list(identity.query_source_codes(
                    params["codes"], params["start"], params["end"], MONEYFLOW_DATASET,
                ))
                params = {**params, "codes": source_codes, "code_membership_digest": digest_named_fields(
                    "aistock_monthly_moneyflow_source_codes_v1", {
                        "authority_sha256": identity.manifest_sha256, "canonical_codes": params["codes"],
                        "source_codes": source_codes, "start": params["start"], "end": params["end"],
                    },
                )}
            yield key, params

    def _seal_query_partition(self, session, *, query, partition_key, **kwargs):
        if kwargs.get("payload_observer") is not None:
            # Private preparation owns its counters. Never reset/interleave
            # them with this authority's normal-SOURCE stream.
            return self._seal_with_context(session, query=query, partition_key=partition_key, **kwargs)
        checkpoint = kwargs.get("checkpoint") or (lambda: None)

        def report():
            self._progress({
                "phase": "SOURCE_ROWS", "query_id": query.query_id, "partition_key": partition_key,
                "rows_validated": self._rows_validated, "rows_sealed": self._rows_sealed,
                "partitions_sealed": self._partitions_sealed,
            })
            checkpoint()

        def observe(_row):
            self._rows_validated += 1

        report()
        result = self._seal_with_context(
            session, query=query, partition_key=partition_key,
            **{**kwargs, "checkpoint": report, "payload_observer": observe},
        )
        self._rows_sealed += result.summary.row_count
        self._partitions_sealed += 1
        report()
        return result

    def _seal_with_context(self, session, *, query, partition_key, **kwargs):
        try:
            return super()._seal_query_partition(session, query=query, partition_key=partition_key, **kwargs)
        except (MonthlyReleaseCancelled, MonthlyReleaseSourceBlocked):
            raise
        except Exception as exc:
            raise MonthlySourcePartitionError(query_id=query.query_id, partition_key=partition_key) from exc
