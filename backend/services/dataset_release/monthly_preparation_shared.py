"""Unpublished logical components from the same healthy frozen SOURCE view.

This module produces no full release manifest, coverage receipt, candidate
identity or consumer registration. Final SOURCE and final BUILD still own
publication and must rebind/verify every prepared input before adoption.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import groupby
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from .canonical import digest_named_fields, ensure_sha256
from .cas_store import CASStore
from .monthly_component_preparation import (
    ComponentInputIdentity,
    ComponentPreparationError,
    _file_ref,
    _plain_path,
    component_dependencies,
    seal_prepared_component,
)
from .monthly_preparation_artifacts import require_preparation_domain_audit
from .monthly_preparation_sector import materialize_preparation_sector_facts
from .monthly_preparation_source import PreparationSourceSnapshot
from .monthly_shared_components import (
    _build_suspend,
    _index_pool_intervals,
    _pit_intervals,
    _source_rows,
    _write_json,
    _write_sidecar,
)
from .sector_enrichment import FrozenSectorEnricher, UNKNOWN_L2_CODE_ID
from .shared_sector_context import (
    build_release_sw_l2_code_map_payload,
    validate_release_sw_l2_code_map,
    validate_sector_quote_availability,
    validate_market_context_frame,
    validate_membership_frame,
)
from .sw_l2_quote_policy import build_quote_availability_payload
from .monthly_worker import ProducerContext


PRIVATE_SHARED_DOMAIN_SCHEMA = "aistock_monthly_private_shared_domain_v1"
_GATES = {
    "stock_pools": frozenset({"calendar_lifecycle", "pit_stock_pools"}),
    "benchmark": frozenset({"calendar_lifecycle", "daily_price", "adj_factor_history"}),
    "suspend": frozenset({"calendar_lifecycle", "suspend_limit"}),
    "sector_context": frozenset({"calendar_lifecycle", "pit_stock_pools", "sector_authority"}),
}


def shared_preparation_identity(
    *, snapshot: Any, profile: Any, component: str, predecessor_sha256: str, sector_membership_start: date
) -> ComponentInputIdentity:
    """Bind only this domain's canonical frozen inputs, never the margin root.

    The same routine is used with the complete SOURCE snapshot for adoption.
    Snapshot IDs and capture times are provenance, not an equality shortcut.
    """
    if component not in _GATES:
        raise ComponentPreparationError("private shared identity domain differs")
    required = set(component_dependencies()[component]) - {"stock_universe_pit"}
    rows = []
    present = set()
    for part in snapshot.partitions:
        if part.spec.dataset not in required:
            continue
        descriptor = part.as_build_input()
        present.add(part.spec.dataset)
        rows.append(
            {
                key: descriptor[key]
                for key in (
                    "dataset",
                    "partition_key",
                    "schema_digest",
                    "content_digest",
                    "merkle_root",
                    "row_count",
                    "source_partition_params_digest",
                    "source_code_membership_digest",
                )
            }
        )
        if type(rows[-1]["row_count"]) is not int or rows[-1]["row_count"] < 0:
            raise ComponentPreparationError("private shared partition count is invalid")
        for field in ("schema_digest", "content_digest", "merkle_root"):
            ensure_sha256(str(rows[-1][field] or ""), field=field)
    if present != required:
        raise ComponentPreparationError(f"private shared identity inputs are incomplete: {sorted(required - present)}")
    rows.sort(key=lambda row: (row["dataset"], row["partition_key"]))
    if len({(row["dataset"], row["partition_key"]) for row in rows}) != len(rows):
        raise ComponentPreparationError("private shared identity partitions are duplicated")
    code_root = Path(__file__).parent
    names = (
        "monthly_preparation_shared.py",
        "monthly_preparation_sector.py",
        "monthly_preparation_executor.py",
        "monthly_preparation_composition.py",
        "monthly_production.py",
        "monthly_component_preparation.py",
        "monthly_preparation_artifacts.py",
        "monthly_shared_components.py",
        "shared_sector_context.py",
        "sector_enrichment.py",
        "sw_l2_quote_policy.py",
        "streaming_artifacts.py",
        "factor_materializer.py",
        "external_ordered_rows.py",
    )
    return ComponentInputIdentity(
        component=component,
        cutoff=snapshot.official_cutoff,
        predecessor_profile_sha256=predecessor_sha256,
        effective_source_sha256=digest_named_fields("aistock_monthly_private_shared_inputs_v1", {"partitions": rows}),
        pit_sha256=snapshot.pit_snapshot_digest,
        qfq_sha256=digest_named_fields(
            "aistock_monthly_private_benchmark_qfq_v1",
            {"partitions": [row for row in rows if row["dataset"] == "adj_factor"]},
        )
        if component == "benchmark"
        else None,
        producer_sha256=digest_named_fields(
            "aistock_monthly_private_shared_producer_v1",
            {
                "files": {name: _file_ref(code_root, name) for name in names},
                "toolchain": profile.qlib_toolchain.digest,
            },
        ),
        schema_sha256=digest_named_fields(
            "aistock_monthly_private_shared_schema_v1",
            {
                "source_schemas": sorted({row["schema_digest"] for row in rows}),
                "domain": PRIVATE_SHARED_DOMAIN_SCHEMA,
            },
        ),
        build_parameters_sha256=digest_named_fields(
            "aistock_monthly_private_shared_parameters_v1",
            {
                "semantic_profile": profile.semantic_profile_digest,
                "sector_membership_start": sector_membership_start.isoformat(),
            },
        ),
        validation_policy_sha256=digest_named_fields(
            "aistock_monthly_private_shared_validation_v1",
            {
                "required_gates": sorted(_GATES[component]),
                "full_source_required_before_adoption": True,
            },
        ),
    )


@dataclass(frozen=True, slots=True)
class MonthlyPrivateSharedPreparationExecutor:
    profile: Any
    cas: CASStore
    sector_membership_start: date

    def execute(
        self,
        *,
        context: ProducerContext,
        snapshot: PreparationSourceSnapshot,
        audit: Mapping[str, Any],
        component: str,
        source_snapshot_id: str,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> Mapping[str, Any]:
        from .monthly_preparation_executor import (
            MonthlyPrivatePhysicalPreparationExecutor,
            PREPARATION_RECORD_SCHEMA,
            _mkdir_private,
            _write_exclusive,
        )

        if (
            context.stage != "SOURCE"
            or type(context.attempt) is not int
            or context.attempt < 1
            or component not in _GATES
            or re.fullmatch(r"dmr_[0-9a-f]{32}", context.operation_id) is None
            or not isinstance(snapshot, PreparationSourceSnapshot)
            or context.operation_id != snapshot.operation_id
            or context.plan.get("target_cutoff") != snapshot.official_cutoff.isoformat()
            or not isinstance(source_snapshot_id, str)
            or not source_snapshot_id.startswith("postgres:")
            or not source_snapshot_id[9:]
            or not isinstance(context.plan.get("predecessor"), Mapping)
        ):
            raise ComponentPreparationError("private shared execution binding differs")
        require_preparation_domain_audit(
            profile=self.profile, snapshot=snapshot, audit=audit, required_gates=_GATES[component]
        )
        identity = shared_preparation_identity(
            snapshot=snapshot,
            profile=self.profile,
            component=component,
            predecessor_sha256=str(context.plan["predecessor"].get("profile_sha256") or ""),
            sector_membership_start=self.sector_membership_start,
        )
        root = _plain_path(Path(self.profile.candidate_root), root=Path(self.profile.candidate_root), directory=True)
        records = _mkdir_private(root / ".staging" / "preparation-records" / context.operation_id, root=root)
        checkpoint()
        recovered = MonthlyPrivatePhysicalPreparationExecutor._recover(
            root=root,
            records=records,
            context=context,
            identity=identity,
        )
        if recovered is not None:
            return recovered.record
        staging = _mkdir_private(
            root / ".staging" / f"{context.operation_id}-preparation-attempt-{context.attempt}", root=root
        )
        component_root = staging / component
        readback = prepare_shared_domain_files(
            cas=self.cas,
            snapshot=snapshot,
            audit=audit,
            profile=self.profile,
            component=component,
            output_root=component_root,
            sector_membership_start=self.sector_membership_start,
            max_rows_in_memory=self.profile.resource_policy.validation_read_chunk_rows,
            checkpoint=checkpoint,
        )
        checkpoint()
        if identity != shared_preparation_identity(
            snapshot=snapshot,
            profile=self.profile,
            component=component,
            predecessor_sha256=context.plan["predecessor"]["profile_sha256"],
            sector_membership_start=self.sector_membership_start,
        ):
            raise ComponentPreparationError("private shared producer/input changed during materialization")
        validation = _write_exclusive(
            component_root / "private-domain-validation.json",
            {
                "identity_digest": identity.digest,
                "source_audit": dict(audit),
                "domain_readback": dict(readback),
                "publication_allowed": False,
            },
        )
        receipt = seal_prepared_component(
            preparation_root=root,
            component_root=component_root,
            operation_id=context.operation_id,
            source_snapshot_id=source_snapshot_id,
            identity=identity,
            output_paths=readback["output_paths"],
            validation_paths=[validation["path"]],
        )
        body = {
            "schema_version": PREPARATION_RECORD_SCHEMA,
            "operation_id": context.operation_id,
            "attempt": context.attempt,
            "component": component,
            "component_root": component_root.relative_to(root).as_posix(),
            "identity_digest": identity.digest,
            "receipt_ref": receipt,
            "publication_allowed": False,
        }
        record = {**body, "canonical_digest": digest_named_fields(PREPARATION_RECORD_SCHEMA, body)}
        _write_exclusive(records / f"attempt-{context.attempt}-{component}.json", record)
        return record


def recover_prepared_shared_components(
    *, context: ProducerContext, snapshot: Any, profile: Any, sector_membership_start: date,
    staging_root: Path, checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any]:
    """Read matching pins only after the caller loads the full SOURCE receipt.

    This deliberately excludes a private SOURCE snapshot. Missing/drifted
    preparation is a normal build, not an exception or a waiver. Recovered
    outputs still need current full-candidate validation and new release pins.
    """
    from .monthly_preparation_executor import MonthlyPrivatePhysicalPreparationExecutor
    from .source_authority import FrozenSourceAuthoritySnapshot

    predecessor = context.plan.get("predecessor")
    if not isinstance(predecessor, Mapping):
        return {}
    if not isinstance(snapshot, FrozenSourceAuthoritySnapshot) or context.stage != "BUILD":
        raise ComponentPreparationError("shared adoption requires complete SOURCE")
    if (type(context.attempt) is not int or context.attempt < 1
        or re.fullmatch(r"dmr_[0-9a-f]{32}", context.operation_id) is None
        or context.plan.get("target_cutoff") != snapshot.official_cutoff.isoformat()
        or snapshot.artifact_ready_contract_ref is None):
        raise ComponentPreparationError("shared adoption operation binding differs")
    root = _plain_path(Path(profile.candidate_root), root=Path(profile.candidate_root), directory=True)
    _plain_path(staging_root, root=root, directory=True)
    if staging_root.parent != root / ".staging":
        raise ComponentPreparationError("shared adoption staging binding differs")
    records = root / ".staging" / "preparation-records" / context.operation_id
    if not records.exists():
        return {}
    _plain_path(records, root=root, directory=True)
    result = {}
    for component in _GATES:
        checkpoint()
        identity = shared_preparation_identity(
            snapshot=snapshot, profile=profile, component=component,
            predecessor_sha256=str(predecessor.get("profile_sha256") or ""),
            sector_membership_start=sector_membership_start,
        )
        verified = MonthlyPrivatePhysicalPreparationExecutor._recover(
            root=root, records=records, context=context, identity=identity,
        )
        if verified is not None:
            result[component] = verified
    return result


def _sector_rows(
    path: Path, *, start: date, end: date, bound: int,
    source_start_row: int | None = None, source_month_rows: int | None = None,
):
    """Read only narrow rows in the policy window, never a whole-history DF."""
    with pd.HDFStore(path, "r") as store:
        if not store.get_storer("data").is_table:
            raise ComponentPreparationError("private sector H5 must support bounded reads")
        positional = {}
        if source_start_row is not None:
            if (type(source_start_row) is not int or source_start_row < 0
                or type(source_month_rows) is not int or source_month_rows <= 0
                or source_start_row + source_month_rows != int(store.get_storer("data").nrows)):
                raise ComponentPreparationError("sector month physical row boundary differs")
            positional = {"start": source_start_row, "stop": source_start_row + source_month_rows}
        elif source_month_rows is not None:
            raise ComponentPreparationError("sector month physical row boundary is incomplete")
        where = [f"datetime >= Timestamp('{start.isoformat()}')", f"datetime <= Timestamp('{end.isoformat()}')"]
        for frame in store.select(
            "data", where=None if positional else where,
            columns=["l2_code_id", "sw2_pct_change", "sw2_vol", "sw2_amount"], chunksize=bound, **positional,
        ):
            if positional:
                stamps = frame.index.get_level_values("datetime")
                if frame.empty or stamps.min().date() < start or stamps.max().date() > end:
                    raise ComponentPreparationError("sector physical tail contains an out-of-month date")
            for row in frame.reset_index().itertuples(index=False):
                yield row


def _sector_context_files(
    *,
    cas: CASStore,
    snapshot: PreparationSourceSnapshot,
    audit: Mapping[str, Any],
    profile: Any,
    root: Path,
    calendar: Sequence[date],
    pit_rows: Sequence[tuple[str, date, date]],
    start: date,
    bound: int,
    checkpoint: Callable[[], None],
) -> tuple[list[str], Mapping[str, Any]]:
    if not profile.start_date <= start <= snapshot.official_cutoff:
        raise ComponentPreparationError("private sector policy window differs")
    facts = materialize_preparation_sector_facts(
        cas=cas,
        snapshot=snapshot,
        audit=audit,
        profile=profile,
        output_root=root / "facts",
        max_rows_in_memory=bound,
        checkpoint=checkpoint,
    )
    classify = _source_rows(cas, snapshot, "sw_index_classify")
    members = _source_rows(cas, snapshot, "sw_index_member")
    enricher = FrozenSectorEnricher.build(classify, members)
    authority = digest_named_fields(
        "aistock_monthly_sw_l2_mapping_authority_v1",
        {
            "source_content_root": snapshot.source_content_root,
            "code_map_digest": enricher.code_map_digest,
            "membership_digest": enricher.membership_digest,
        },
    )
    code_payload = build_release_sw_l2_code_map_payload(
        code_to_id=dict(enricher.code_map),
        member_backed_codes=tuple(enricher.code_map),
        authority_id=f"monthly-source:{authority[:16]}",
        authority_sha256=authority,
    )
    code_map = validate_release_sw_l2_code_map(code_payload)
    quote_payload = build_quote_availability_payload(
        code_map=code_map, required_start=start, cutoff=snapshot.official_cutoff
    )
    quote = validate_sector_quote_availability(quote_payload, code_map=code_map, required_end=snapshot.official_cutoff)
    membership, market, counts = summarize_sector_context(
        sector_h5=root / "facts" / "sector_data.h5", profile=profile,
        enricher=enricher, code_map=code_map, quote=quote,
        calendar=calendar, pit_rows=pit_rows, start=start,
        cutoff=snapshot.official_cutoff, bound=bound, checkpoint=checkpoint,
    )
    _write_json(root / "sector_code_map.json", code_payload)
    _write_json(root / "sector_quote_availability.json", quote_payload)
    membership.to_parquet(root / "sector_membership_spans.parquet", index=False)
    market.to_parquet(root / "market_context.parquet", index=False)
    return [
        "facts/sector_data.h5", "sector_code_map.json", "sector_quote_availability.json",
        "sector_membership_spans.parquet", "market_context.parquet",
    ], {"sector_facts": facts, **counts}


def summarize_sector_context(
    *, sector_h5: Path, profile: Any, enricher: FrozenSectorEnricher,
    code_map: Any, quote: Any, calendar: Sequence[date],
    pit_rows: Sequence[tuple[str, date, date]], start: date, cutoff: date,
    bound: int, checkpoint: Callable[[], None],
    market_start: date | None = None,
    source_start_row: int | None = None, source_month_rows: int | None = None,
):
    """Single bounded file scan shared by preparation and final validation."""
    by_symbol: dict[str, list[tuple[date, date]]] = defaultdict(list)
    for symbol, left, right in pit_rows:
        by_symbol[symbol].append((left, right))
    states: dict[str, tuple[date, date, int]] = {}
    spans = []
    markets = []
    frozen_days = authority_resolved_days = required_quotes = 0
    used_ids = set()

    def close(symbol):
        left, right, sector_id = states.pop(symbol)
        spans.append({"instrument": symbol, "l2_code_id": sector_id, "start_date": left, "end_date": right})

    # Market context belongs to the complete frozen source history, not only
    # the narrower membership/blacklist policy window.
    scan_start = profile.start_date if market_start is None else market_start
    if not profile.start_date <= scan_start <= start <= cutoff:
        raise ComponentPreparationError("sector summary month window is invalid")
    stream = _sector_rows(
        sector_h5, start=scan_start, end=cutoff, bound=bound,
        source_start_row=source_start_row, source_month_rows=source_month_rows,
    )
    groups = iter(groupby(stream, key=lambda row: row.datetime.date()))
    upcoming = next(groups, None)
    try:
        for day in (value for value in calendar if scan_start <= value <= cutoff):
            checkpoint()
            if upcoming is not None and upcoming[0] < day:
                raise ComponentPreparationError("private sector facts contain a non-calendar day")
            assignments = {}
            volumes = {}
            finite = set()
            quotes = {}
            rows = upcoming[1] if upcoming is not None and upcoming[0] == day else ()
            for row in rows:
                if row.instrument in assignments:
                    raise ComponentPreparationError("private sector has duplicate stock-date keys")
                if not np.isfinite(row.l2_code_id) or row.l2_code_id % 1 or row.l2_code_id < -1:
                    raise ComponentPreparationError("sector facts contain an invalid code ID")
                sector_id = int(row.l2_code_id)
                assignments[row.instrument] = sector_id
                if sector_id < 0:
                    continue
                if sector_id not in code_map.id_to_code:
                    raise ComponentPreparationError("private sector has an unknown code ID")
                used_ids.add(sector_id)
                triple = (row.sw2_pct_change, row.sw2_vol, row.sw2_amount)
                if np.isinf(triple).any():
                    raise ComponentPreparationError("sector quote is infinite")
                normalized_quote = tuple(None if pd.isna(value) else float(value) for value in triple)
                if quotes.setdefault(sector_id, normalized_quote) != normalized_quote:
                    raise ComponentPreparationError("private sector quote fields conflict")
                if day >= start:
                    code = code_map.id_to_code[sector_id]
                    available = any(left <= day <= right for left, right in quote.entries[code])
                    if available and not np.isfinite(triple).all():
                        raise ComponentPreparationError("private quote-available stock row lacks facts")
                    if not available and not pd.isna(list(triple)).all():
                        raise ComponentPreparationError("private stopped sector has unexpected quote facts")
                if np.isfinite(row.sw2_vol):
                    prior = volumes.setdefault(sector_id, float(row.sw2_vol))
                    if prior != float(row.sw2_vol) or prior < 0:
                        raise ComponentPreparationError("private sector quote volume conflicts")
                if np.isfinite([row.sw2_pct_change, row.sw2_vol, row.sw2_amount]).all():
                    finite.add(sector_id)
            active = {
                symbol
                for symbol, intervals in by_symbol.items()
                if day >= start and any(left <= day <= right for left, right in intervals)
            }
            for symbol in set(states) - active:
                close(symbol)
            needed = set()
            for symbol in sorted(active):
                sector_id = assignments.get(symbol, UNKNOWN_L2_CODE_ID)
                if sector_id >= 0:
                    frozen_days += 1
                else:
                    sector_id = int(enricher.enrich({"ts_code": symbol, "trade_date": day})["l2_code_id"])
                    authority_resolved_days += 1
                if sector_id not in code_map.id_to_code:
                    raise ComponentPreparationError(f"private PIT membership is unresolved: {symbol}/{day}")
                if symbol in states and states[symbol][2] != sector_id:
                    close(symbol)
                left = states[symbol][0] if symbol in states else day
                states[symbol] = (left, day, sector_id)
                code = code_map.id_to_code[sector_id]
                if any(left <= day <= right for left, right in quote.entries[code]):
                    needed.add(sector_id)
            if needed - finite:
                raise ComponentPreparationError(
                    f"private quote-available sectors lack facts: {day}/{sorted(needed - finite)}"
                )
            required_quotes += len(needed)
            total = sum(volumes.values())
            if not np.isfinite(total) or total <= 0:
                raise ComponentPreparationError("private market volume is missing or invalid")
            markets.append({"trade_date": day, "sw_daily_total_vol": total})
            if upcoming is not None and upcoming[0] == day:
                upcoming = next(groups, None)
        if upcoming is not None:
            raise ComponentPreparationError("private sector facts exceed the calendar")
    finally:
        stream.close()
    for symbol in tuple(states):
        close(symbol)
    membership = pd.DataFrame(spans, columns=["instrument", "start_date", "end_date", "l2_code_id"])
    membership = membership.sort_values(["instrument", "start_date", "end_date", "l2_code_id"]).reset_index(drop=True)
    membership["l2_code_id"] = membership["l2_code_id"].astype("int32")
    market = pd.DataFrame(markets)
    validate_membership_frame(
        membership, id_to_code=code_map.id_to_code, required_start=start, required_end=cutoff
    )
    validate_market_context_frame(market, required_start=start, required_end=cutoff)
    return membership, market, {
        "catalog_count": len(code_map.id_to_code),
        "used_l2_code_id_count": len(used_ids),
        "membership_symbol_count": int(membership["instrument"].nunique()),
        "membership_span_count": len(spans),
        "frozen_stock_trading_day_count": frozen_days,
        "member_gap_fill_stock_trading_day_count": authority_resolved_days,
        "quote_required_sector_date_count": required_quotes,
        "quote_gap_count": 0,
        "membership_gap_count": 0,
        "unknown_code_count": 0,
    }


def prepare_shared_domain_files(
    *,
    cas: CASStore,
    snapshot: PreparationSourceSnapshot,
    audit: Mapping[str, Any],
    profile: Any,
    component: str,
    output_root: Path,
    sector_membership_start: date,
    max_rows_in_memory: int = 100_000,
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any]:
    """Materialize one logical domain and return a private scoped readback."""
    if component not in _GATES:
        raise ComponentPreparationError("private shared component is unknown")
    require_preparation_domain_audit(profile=profile, snapshot=snapshot, audit=audit, required_gates=_GATES[component])
    _plain_path(output_root.parent, root=output_root.parent, directory=True)
    if not output_root.is_absolute() or output_root.exists():
        raise ComponentPreparationError("private shared output already exists or is relative")
    calendar = tuple(
        sorted(date.fromisoformat(str(row["cal_date"])[:10]) for row in _source_rows(cas, snapshot, "trading_calendar"))
    )
    if not calendar or len(set(calendar)) != len(calendar) or calendar[-1] != snapshot.official_cutoff:
        raise ComponentPreparationError("private shared calendar differs")
    pit_rows = _pit_intervals(snapshot.pit_snapshot, calendar)
    output_root.mkdir(exist_ok=False)
    _plain_path(output_root, root=output_root.parent, directory=True)
    checkpoint()
    counts = {}
    if component == "stock_pools":
        pools = _index_pool_intervals(
            membership_rows=_source_rows(cas, snapshot, "index_membership_pit"),
            pit_rows=pit_rows,
            calendar=calendar,
            cutoff=snapshot.official_cutoff,
        )
        outputs = []
        for name, spans in {"stock_universe": pit_rows, **pools}.items():
            path = _write_sidecar(
                output_root / ("stock_universe.txt" if name == "stock_universe" else f"index_pool__{name}.txt"), spans
            )
            outputs.append(path.name)
            counts[name] = {"symbol_count": len({row[0] for row in spans}), "span_count": len(spans)}
        if len(outputs) != 6:
            raise ComponentPreparationError("private stock pool set differs")
    elif component == "benchmark":
        path = _write_sidecar(output_root / "benchmark.txt", (("000300.SH", calendar[0], snapshot.official_cutoff),))
        outputs = [path.name]
        counts = {"benchmark_code": "000300.SH", "provider_catalog_only": True}
    elif component == "suspend":
        parquet, metadata, keys, input_count = _build_suspend(
            root=output_root,
            rows=_source_rows(cas, snapshot, "suspend_d"),
            pit_rows=pit_rows,
            calendar=calendar,
            profile=profile,
            cutoff=snapshot.official_cutoff,
        )
        outputs = [parquet.relative_to(output_root).as_posix(), metadata.relative_to(output_root).as_posix()]
        counts = {"suspended_stock_date_count": len(keys), "source_rows_read": input_count}
    else:
        outputs, counts = _sector_context_files(
            cas=cas,
            snapshot=snapshot,
            audit=audit,
            profile=profile,
            root=output_root,
            calendar=calendar,
            pit_rows=pit_rows,
            start=sector_membership_start,
            bound=max_rows_in_memory,
            checkpoint=checkpoint,
        )
    body = {
        "schema_version": PRIVATE_SHARED_DOMAIN_SCHEMA,
        "operation_id": snapshot.operation_id,
        "cutoff": snapshot.official_cutoff.isoformat(),
        "component": component,
        "source_manifest_ref": snapshot.source_manifest_ref.as_dict(),
        "pit_snapshot_digest": snapshot.pit_snapshot_digest,
        "output_paths": outputs,
        "domain_counts": counts,
        "publication_allowed": False,
        "consistent_input_set_complete": False,
        "database_write_performed": False,
    }
    return {**body, "canonical_digest": digest_named_fields(PRIVATE_SHARED_DOMAIN_SCHEMA, body)}
