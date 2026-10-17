"""Expose actual SOURCE work without changing its data or sealing contract."""

from __future__ import annotations

from datetime import date
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

    def _partition_requests(self, query, cutoff, *, pit_snapshot, selected_stock_codes=()):
        if self._month_start is not None and cutoff.replace(day=1) != self._month_start:
            raise SourceProviderContractError("monthly SOURCE cutoff differs from natural month")
        if query.query_id == "adj_factor_construction":
            if self._month_start is None:
                raise SourceProviderContractError("construction facts require a monthly source request")
            codes = sorted({span.ts_code for span in pit_snapshot.spans})
            if selected_stock_codes:
                if not set(selected_stock_codes) <= set(codes):
                    raise SourceProviderContractError("construction source sample escapes PIT")
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
            start_override=self._month_start if query.date_expression is not None else None,
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
