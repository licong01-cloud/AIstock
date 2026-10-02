"""Freeze the current file-source C-010 panels into the formal train/D6 request.

Only complete, validated C-010/A5 source receipts may cross this boundary.
The smaller rotation-product bundle (nine features) is not a substitute.
"""

from __future__ import annotations

import hashlib
import itertools
import math
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, BASE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_calendar import build_calendar_carrier
from backend.services.hmm_risk.formal_state_domains import authority_identity, build_a5_receipts, partition_entry
from backend.services.hmm_risk.formal_state_executor import (
    FROZEN_GENERATION,
    FROZEN_MANIFEST,
    TRAIN_CALENDAR_HASH,
    frozen_release_binding,
)
from backend.services.hmm_risk.formal_state_model import CONTRACTS, FAMILIES, VERSION, FormalStateError, receipt
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest
from backend.services.hmm_risk.stock_fact_observation import (
    C010_AGGREGATE_RECEIPT_VERSION,
    C010_FORMULA_VERSION,
    C010_POLICY_VERSION,
    MONEYFLOW_STOCK_FIELDS,
    PRICE_STOCK_FIELDS,
    ObservationCoverageError,
    aggregate_l1_day,
    build_c010_feature_domain_panel,
    complete_c010_domain_receipts,
    project_stock_fact_rows_for_direct_level,
    validate_c010_policy_manifest,
    _missing_row_evidence,
    _row_complete,
)

SOURCE_START = date(2020, 7, 30)
SOURCE_END = date(2025, 4, 30)
TRAIN_START = date(2022, 1, 1)
TRAIN_END = date(2024, 6, 30)


def _source_a5(
    *,
    month_paths: Sequence[Path],
    spans: Mapping[str, Sequence[tuple[date, date]]],
    security: Any,
    projection: Any,
    provider: Any,
    authorities: Mapping[str, Mapping[str, Any]],
    calendar: Sequence[date],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Resolve every provider key independently of its opportunity denominator.

    A fully missing Qlib sentinel denotes no raw-price row, not a price of zero.
    Neither suspension nor numeric price completeness changes raw row presence.
    """
    from backend.services.hmm_risk import rotation_l1_input_bundle as source_reader

    opportunity_keys = []
    prices_for_provider = {}
    provider_rows = [r for r in provider.rows if TRAIN_START <= r.trade_date <= TRAIN_END]
    provider_keys = {(r.canonical_ts_code, r.trade_date) for r in provider_rows}
    for path in month_paths:
        for raw in source_reader._read_spooled_month(path):
            day = source_reader._date_from_yyyymmdd(int(raw["trade_date"]), "formal.raw_price_date")
            if not TRAIN_START <= day <= TRAIN_END:
                continue
            symbol = bytes(raw["symbol"]).rstrip(b"\0").decode("ascii")
            if source_reader._qlib_row_is_fully_missing(raw):
                continue
            if (symbol, day) in provider_keys:
                prices_for_provider[(symbol, day)] = {"row_sha256": hashlib.sha256(raw.tobytes()).hexdigest()}
            intervals = [span for span in spans.get(symbol, ()) if span[0] <= day <= span[1]]
            if len(intervals) != 1:
                raise FormalStateError("hmm_risk_c010_expected_opportunity_invalid", "ambiguous PIT price row")
            industry = projection.resolve(symbol, day)
            if industry.status == "resolved":
                opportunity_keys.append((symbol, day.isoformat()))
            elif industry.status != "unavailable":
                raise FormalStateError("hmm_risk_c010_expected_opportunity_invalid", "invalid industry resolution")
    entries = []
    for row in provider_rows:
        if row.trade_date not in calendar:
            raise FormalStateError(
                "hmm_risk_c010_provider_absence_domain_partition_invalid", "non-calendar provider key"
            )
        provider_resolution = security.resolve(row.canonical_ts_code, row.trade_date, row.source_dataset).evidence()
        price_resolution = security.resolve(row.canonical_ts_code, row.trade_date, "market.kline_daily_raw").evidence()
        industry = dict(projection.resolve(row.canonical_ts_code, row.trade_date).as_dict())
        # The shared interval cache stores its first date; receipt identity is this key's date.
        industry["trade_date"] = row.trade_date.isoformat()
        entries.append(
            partition_entry(
                provider_row=row.evidence(),
                authorities=authorities,
                provider_resolution=provider_resolution,
                price_resolution=price_resolution,
                pit_candidates=[
                    {"eligible_interval": [start.isoformat(), end.isoformat()]}
                    for start, end in spans.get(row.canonical_ts_code, ())
                    if start <= row.trade_date <= end
                ],
                price_candidates=(
                    [prices_for_provider[(row.canonical_ts_code, row.trade_date)]]
                    if (row.canonical_ts_code, row.trade_date) in prices_for_provider
                    else []
                ),
                sw_candidates=[industry],
            )
        )
    return build_a5_receipts(
        authorities=authorities,
        partition_entries=entries,
        opportunity_keys=opportunity_keys,
        provider_keys=[(r.canonical_ts_code, r.trade_date.isoformat()) for r in provider_rows],
    )


def _domain_entry(aggregate: Any, *, level: str) -> dict[str, Any]:
    body = {
        "direct_sector_level": level,
        "trade_date": aggregate.trade_date.isoformat(),
        "sector_code": aggregate.l1_code,
        "price_domain_status": "available",
        "price_domain_reason_code": None,
        "price_expected_symbols": list(aggregate.price_expected_symbols),
        "price_complete_symbols": list(aggregate.price_complete_symbols),
        "price_count_coverage": aggregate.count_coverage,
        "price_weight_coverage": aggregate.weight_coverage,
        "price_expected_weight": aggregate.price_expected_weight,
        "price_complete_weight": aggregate.price_complete_weight,
        "moneyflow_expected_symbols": list(aggregate.moneyflow_expected_symbols),
        "moneyflow_complete_symbols": list(aggregate.moneyflow_complete_symbols),
        "moneyflow_excluded_symbols": list(aggregate.moneyflow_excluded_symbols),
        "moneyflow_count_coverage": aggregate.moneyflow_count_coverage,
        "moneyflow_weight_coverage": aggregate.moneyflow_weight_coverage,
        "moneyflow_expected_weight": aggregate.moneyflow_expected_weight,
        "moneyflow_complete_weight": aggregate.moneyflow_complete_weight,
        "moneyflow_domain_status": aggregate.moneyflow_domain_status,
        "moneyflow_domain_reason_code": {
            "available": None,
            "structurally_unavailable": "hmm_risk_c010_moneyflow_domain_structurally_unavailable",
            "coverage_insufficient": "hmm_risk_c010_moneyflow_domain_coverage_insufficient",
            "denominator_invalid": "hmm_risk_c010_moneyflow_denominator_invalid",
        }[aggregate.moneyflow_domain_status],
        "moneyflow_contributor_amount": aggregate.moneyflow_amount,
        "missing_evidence": list(aggregate.missing_evidence),
    }
    for field in ("price_expected", "price_complete", "moneyflow_expected", "moneyflow_complete"):
        body[f"{field}_symbol_sha256"] = canonical_sha256(body[f"{field}_symbols"])
    return {**body, "entry_sha256": canonical_sha256(body)}


def _collect_domains(
    day: date,
    rows: Sequence[Mapping[str, Any]],
    *,
    eligibility: Mapping[str, bool],
    aggregates: Mapping[str, list[Any]],
    evidence: Mapping[str, list[dict[str, Any]]],
) -> None:
    for level, prefix in (("L1", "l1"), ("L2", "l2")):
        projected = project_stock_fact_rows_for_direct_level(rows, sector_level=level)
        for code, group in itertools.groupby(projected, key=lambda row: row["l1_code"]):
            values = list(group)
            expected = [row for row in values if not row.get("is_suspended")]
            if not expected:
                # Calendar completion preserves an explicit no-opportunity entry.
                continue
            try:
                aggregate = aggregate_l1_day(values, moneyflow_contributor_eligibility=eligibility)
            except ObservationCoverageError as exc:
                symbols = sorted(row["symbol"] for row in expected)
                # A denominator failure occurs before the shared aggregator
                # inspects price fields. Do not call its weight-only remainder
                # "price complete"; independently reuse the shared row checker.
                missing = []
                for row in expected:
                    fields = [field for field in _row_complete(row)[1] if field in PRICE_STOCK_FIELDS]
                    if fields:
                        missing.append(_missing_row_evidence(row, fields))
                missing_symbols = {entry.get("symbol") for entry in missing}
                complete = [symbol for symbol in symbols if symbol not in missing_symbols]
                weights = [row.get("prev_circ_mv_cny") for row in expected]
                known_weights = all(isinstance(v, (int, float)) and math.isfinite(v) and v > 0 for v in weights)
                body = {
                    "direct_sector_level": level,
                    "trade_date": day.isoformat(),
                    "sector_code": code,
                    "price_domain_status": "invalid",
                    "price_domain_reason_code": exc.reason_code,
                    "price_expected_symbols": symbols,
                    "price_expected_symbol_sha256": canonical_sha256(symbols),
                    "price_complete_symbols": complete,
                    "price_complete_symbol_sha256": canonical_sha256(complete),
                    "price_count_coverage": len(complete) / len(symbols),
                    "price_weight_coverage": exc.weight_coverage if known_weights else None,
                    "price_expected_weight": math.fsum(weights) if known_weights else None,
                    "price_complete_weight": (
                        math.fsum(row["prev_circ_mv_cny"] for row in expected if row["symbol"] in complete)
                        if known_weights
                        else None
                    ),
                    "missing_evidence": missing,
                }
                evidence[f"{prefix}_invalid_price_domain"].append({**body, "entry_sha256": canonical_sha256(body)})
            else:
                aggregates[level].append(aggregate)
                evidence[f"{prefix}_domain_receipts"].append(_domain_entry(aggregate, level=level))


def _circ_mv_crossings(day: date, rows: Sequence[Mapping[str, Any]], calendar: Sequence[date]) -> list[dict[str, Any]]:
    """Retain approved C-009 cross-entry provenance without persisting every stock row."""
    positions = {d: i for i, d in enumerate(calendar)}
    crossings = []
    for row in rows:
        if row.get("is_suspended") or row.get("circ_mv_fact_status") != "available":
            continue
        source_day = row.get("circ_mv_source_date")
        entry_day = row.get("circ_mv_pit_eligible_start")
        crossed = source_day is not None and entry_day is not None and source_day < entry_day
        if (
            not isinstance(source_day, date)
            or source_day not in positions
            or not SOURCE_START <= source_day < day
            or row.get("circ_mv_history_start") != SOURCE_START
            or row.get("circ_mv_crossed_pit_entry_boundary") is not crossed
            or row.get("circ_mv_staleness_trading_days") != positions[day] - positions[source_day]
            or row.get("circ_mv_lookback_contract_version") != "hmm_risk_causal_circ_mv_source_window_v1"
        ):
            raise FormalStateError(
                "hmm_risk_stock_fact_circ_mv_pit_boundary_evidence_invalid", "causal circ_mv lineage differs"
            )
        if crossed:
            crossings.append(
                {
                    "symbol": row["symbol"],
                    "trade_date": day.isoformat(),
                    "source_date": source_day.isoformat(),
                    "pit_eligible_start": entry_day.isoformat(),
                    "staleness_trading_days": row["circ_mv_staleness_trading_days"],
                }
            )
    return crossings


def _policy_from_source(
    *,
    calendar: Sequence[date],
    codes: Mapping[str, Sequence[str]],
    definitions: Mapping[str, Mapping[str, Any]],
    crosses: Mapping[str, Mapping[str, Any]],
    domain_entries: Mapping[str, list[dict[str, Any]]],
    partition: Mapping[str, Any],
    opportunity: Mapping[str, Any],
    eligibility: Mapping[str, Any],
    dataset_manifest: Mapping[str, Any],
    mapping: Mapping[str, Any],
    security: Mapping[str, Any],
    provider: Mapping[str, Any],
    producer_commit: str,
    circ_mv_crossings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ledger = eligibility["entries"]
    excluded = eligibility["excluded_moneyflow_symbols"]
    aggregate = complete_c010_domain_receipts(
        receipt(
            {
                "schema_version": C010_AGGREGATE_RECEIPT_VERSION,
                "formula_version": C010_FORMULA_VERSION,
                "formal_policy_activated": True,
                **dict(domain_entries),
                "l1_aggregate_count": len(domain_entries["l1_domain_receipts"]),
                "l2_aggregate_count": len(domain_entries["l2_domain_receipts"]),
            }
        ),
        trading_dates=calendar,
        l1_sector_codes=codes["L1"],
        l2_sector_codes=codes["L2"],
    )
    causal = {
        level: {
            "contract_version": "hmm_risk_causal_circ_mv_source_window_v1",
            "source": "frozen_daily_basic",
            "history_start": SOURCE_START.isoformat(),
            "as_of_rule": "latest_valid_source_date_strictly_before_t_within_source_window",
            "cross_pit_entry": "allowed_for_circ_mv_only_with_frozen_source_lineage",
            "crossing_receipt": {
                "count": len(circ_mv_crossings),
                "ordered_key_sha256": canonical_sha256(list(circ_mv_crossings)),
            },
        }
        for level in ("L1", "L2")
    }
    dates = [day.isoformat() for day in calendar]
    order = {FAMILIES[0]: list(BASE_FEATURES), FAMILIES[1]: list(ALL_CORE_FEATURES)}
    return validate_c010_policy_manifest(
        receipt(
            {
                "schema_version": C010_POLICY_VERSION,
                "formula_version": C010_FORMULA_VERSION,
                "producer_commit": producer_commit,
                "train_start": TRAIN_START.isoformat(),
                "train_end": TRAIN_END.isoformat(),
                "receipt_trading_dates": dates,
                "receipt_trading_date_count": len(dates),
                "receipt_trading_date_sha256": canonical_sha256(dates),
                "contributor_min_availability": 0.9,
                "domain_min_count_coverage": 0.9,
                "domain_min_weight_coverage": 0.9,
                "feature_cross_section_min_coverage": 0.9,
                "moneyflow_mandatory_fields": list(MONEYFLOW_STOCK_FIELDS),
                "eligibility_receipt": eligibility,
                "eligibility_receipt_sha256": eligibility["receipt_sha256"],
                "eligibility_entry_count": len(ledger),
                "contributor_ledger": ledger,
                "contributor_ledger_sha256": canonical_sha256(ledger),
                "excluded_moneyflow_symbols": excluded,
                "excluded_moneyflow_symbol_sha256": canonical_sha256(excluded),
                "aggregate_receipt": aggregate,
                "aggregate_receipt_sha256": aggregate["receipt_sha256"],
                "l1_cross_section_receipt": crosses["L1"],
                "l1_cross_section_receipt_sha256": crosses["L1"]["receipt_sha256"],
                "l2_cross_section_receipt": crosses["L2"],
                "l2_cross_section_receipt_sha256": crosses["L2"]["receipt_sha256"],
                "l1_feature_definition": definitions["L1"],
                "l1_feature_definition_sha256": canonical_sha256(definitions["L1"]),
                "l2_feature_definition": definitions["L2"],
                "l2_feature_definition_sha256": canonical_sha256(definitions["L2"]),
                "feature_order_by_family": order,
                "feature_order_sha256": canonical_sha256(order),
                "dataset_manifest_hash": canonical_sha256(dataset_manifest),
                "mapping_manifest_hash": canonical_sha256(mapping),
                "l2_stock_fact_manifest_hash": canonical_sha256(
                    {"mapping": mapping, "aggregate": aggregate["receipt_sha256"]}
                ),
                "calendar_manifest_hash": canonical_sha256(dataset_manifest["calendar_benchmark"]),
                "security_identity_manifest_sha256": security["manifest_sha256"],
                "provider_absence_manifest_sha256": provider["manifest_sha256"],
                "causal_circ_mv_identity": causal,
                "causal_circ_mv_identity_sha256": canonical_sha256(causal),
                "pit_universe_changed": False,
                "selection_universe_changed": False,
                "runtime_prediction_eligibility_changed": False,
                "expected_opportunity_receipt": opportunity,
                "expected_opportunity_receipt_sha256": opportunity["receipt_sha256"],
                "provider_absence_partition_receipt": partition,
                "provider_absence_partition_receipt_sha256": partition["receipt_sha256"],
            }
        )
    )


def prepare_file_request(
    *,
    candidate_root: Path,
    security_identity_manifest: Path,
    provider_absence_manifest: Path,
    industry_authority: Mapping[str, Any],
    work_parent: Path,
    producer_commit: str,
) -> dict[str, Any]:
    """Current formal source constructor. No DB, fits, target search or release writes."""
    from backend.services.hmm_risk import rotation_l1_input_bundle as source_reader

    assets = source_reader.load_rotation_l1_direct_v2_source_assets(
        candidate_root,
        security_identity_manifest=security_identity_manifest,
        provider_absence_manifest=provider_absence_manifest,
        data_window_end=SOURCE_END,
        frozen_release_binding=frozen_release_binding(),
    )
    identity = assets["release_identity"]
    if (
        identity["frozen_release_generation"] != FROZEN_GENERATION
        or identity["dataset_manifest_sha256"] != FROZEN_MANIFEST
    ):
        raise FormalStateError("hmm_risk_formal_identity_mismatch", "only approved frozen v16 allowed")
    calendar_all = source_reader._load_qlib_calendar(assets["qlib_root"] / "calendars/day.txt")
    calendar = tuple(day for day in calendar_all if SOURCE_START <= day <= SOURCE_END)
    if not calendar or calendar[0] != SOURCE_START or calendar[-1] != SOURCE_END:
        raise FormalStateError("hmm_risk_formal_input_invalid", "approved warmup/utility source calendar incomplete")
    forbidden = (Path(__file__).resolve().parents[3], assets["release_root"])
    from backend.services.hmm_risk.formal_state_executor import validate_output_location

    work_parent = validate_output_location(work_parent, dataset_root=assets["release_root"])
    work_parent.mkdir(parents=True, exist_ok=True)
    adapter = source_reader._industry_adapter(industry_authority, forbidden_roots=(forbidden[0],))
    l1, l2 = source_reader._canonical_sector_codes(adapter)
    codes = {"L1": list(l1), "L2": list(l2)}
    mapping = adapter.mapping_manifest(
        universe_key=assets["universe_key"], source_start=SOURCE_START, source_end=SOURCE_END
    )
    security = source_reader._SecurityResolutionIndex(
        load_security_source_identity_manifest(
            assets["files"]["security_identity"],
            expected_sha256=canonical_sha256(source_reader._read_json_object(assets["files"]["security_identity"])),
        )
    )
    provider = load_provider_absence_manifest(
        assets["files"]["provider_absence"],
        expected_sha256=canonical_sha256(source_reader._read_json_object(assets["files"]["provider_absence"])),
    )
    spans = source_reader._parse_instrument_spans(assets["instrument_universe_path"])
    projection = source_reader._IndustryProjectionIndex(adapter, calendar=calendar)
    authorities = {
        "provider_absence_manifest_identity": authority_identity("provider_absence_manifest", provider.evidence()),
        "security_resolver_identity": authority_identity("security_source_identity_manifest", security.evidence()),
        "pit_authority_identity": authority_identity(
            "stock_universe_pit_state_and_spans",
            {"manifest_sha256": assets["inventory"]["qlib"]["training_instruments_sha256"]},
        ),
        "price_source_identity": authority_identity(
            "market.kline_daily_raw", {"manifest_sha256": assets["inventory"]["inventory_sha256"]}
        ),
        "sw_mapping_classify_identity": authority_identity("hmm_industry_pit_classification_projection", mapping),
    }
    suspension = source_reader._load_suspend_keys(
        assets["files"]["suspend_data"],
        assets["files"]["suspend_manifest"],
        calendar=calendar,
        expected_release_cutoff=assets["release_cutoff"],
        expected_universe_key=assets["universe_key"],
    )
    aggregates = {"L1": [], "L2": []}
    domain_entries = {
        f"{level}_{kind}": [] for level in ("l1", "l2") for kind in ("domain_receipts", "invalid_price_domain")
    }
    circ_mv_crossings = []

    def collect(day: date, rows: Sequence[Mapping[str, Any]]) -> None:
        circ_mv_crossings.extend(_circ_mv_crossings(day, rows, calendar))
        _collect_domains(day, rows, eligibility=eligibility_map, aggregates=aggregates, evidence=domain_entries)

    with tempfile.TemporaryDirectory(prefix="hmm-formal-source-", dir=work_parent) as temp:
        month_paths = source_reader._spool_qlib_months(
            assets["qlib_root"],
            calendar=calendar_all,
            spans=spans,
            spool_root=Path(temp) / "months",
            window_start=SOURCE_START,
            window_end=SOURCE_END,
        )
        partition, opportunity, eligibility = _source_a5(
            month_paths=month_paths,
            spans=spans,
            security=security,
            projection=projection,
            provider=provider,
            authorities=authorities,
            calendar=calendar,
        )
        eligibility_map = {
            item["canonical_ts_code"]: item["moneyflow_contributor_eligible"] for item in eligibility["entries"]
        }
        source_reader._build_stock_fact_aggregates(
            month_paths=month_paths,
            assets=assets,
            calendar=calendar,
            spans=spans,
            adapter=projection,
            security=security,
            provider_absence=provider,
            suspension_keys=suspension,
            contributor_eligibility=eligibility_map,
            window_start=SOURCE_START,
            window_end=SOURCE_END,
            build_feature_domain_aggregates=False,
            day_rows_callback=collect,
        )
    panels, definitions, crosses = {}, {}, {}
    benchmark = {day: assets["benchmark"][day] for day in calendar}
    for level, count in (("L1", 31), ("L2", 131)):
        panels[level], definitions[level], crosses[level] = build_c010_feature_domain_panel(
            aggregates[level],
            trading_dates=calendar,
            csi300_returns=benchmark,
            expected_sector_count=count,
            direct_sector_level=level,
        )
        if list(sorted(panels[level].index.get_level_values(1).unique())) != codes[level]:
            raise FormalStateError(
                "hmm_risk_formal_input_invalid", "observed features do not close frozen sector catalog"
            )
    dataset_manifest = {
        "schema_version": "hmm_risk_formal_stock_fact_source_v1",
        "source_window": [SOURCE_START.isoformat(), SOURCE_END.isoformat()],
        "calendar_benchmark": {"rows": [[day.isoformat(), benchmark[day]] for day in calendar]},
        "source_inventory": assets["inventory"],
    }
    policy = _policy_from_source(
        calendar=calendar,
        codes=codes,
        definitions=definitions,
        crosses=crosses,
        domain_entries=domain_entries,
        partition=partition,
        opportunity=opportunity,
        eligibility=eligibility,
        dataset_manifest=dataset_manifest,
        mapping=mapping,
        security=security.evidence(),
        provider=provider.evidence(),
        producer_commit=producer_commit,
        circ_mv_crossings=circ_mv_crossings,
    )
    source_identity = {
        "generation": FROZEN_GENERATION,
        "manifest_sha256": FROZEN_MANIFEST,
        "cutoff": assets["release_cutoff"].isoformat(),
        "dataset_root": str(assets["release_root"]),
        "dataset_manifest_hash": canonical_sha256(dataset_manifest),
        "source_inventory_sha256": assets["inventory"]["inventory_sha256"],
        "source_window": [SOURCE_START.isoformat(), SOURCE_END.isoformat()],
    }
    return prepare_request(
        panels=panels,
        calendar=[day.isoformat() for day in calendar],
        sector_codes=codes,
        source_identity=source_identity,
        industry_authority=industry_authority,
        policy=policy,
    )


def _frame(panel: pd.DataFrame, *, codes: Sequence[str], calendar: Sequence[str]) -> pd.DataFrame:
    if not isinstance(panel, pd.DataFrame) or not isinstance(panel.index, pd.MultiIndex):
        raise FormalStateError("hmm_risk_formal_input_invalid", "direct feature panel must have date/sector index")
    if panel.index.nlevels != 2 or panel.index.has_duplicates:
        raise FormalStateError("hmm_risk_formal_input_invalid", "direct feature panel key is not unique")
    expected = pd.MultiIndex.from_product([pd.to_datetime(calendar), codes], names=panel.index.names)
    if not panel.index.sort_values().equals(expected):
        raise FormalStateError("hmm_risk_formal_input_invalid", "panel must retain full calendar/sector denominator")
    if not set(ALL_CORE_FEATURES) <= set(panel.columns) or "benchmark_return" not in panel:
        raise FormalStateError("hmm_risk_formal_input_invalid", "full 20D source and benchmark required")
    return panel.reindex(expected)


def prepare_request(
    *,
    panels: Mapping[str, pd.DataFrame],
    calendar: Sequence[str],
    sector_codes: Mapping[str, Sequence[str]],
    source_identity: Mapping[str, Any],
    industry_authority: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    """No fitting; full validation calendar is retained with compact finite payloads."""
    policy = validate_c010_policy_manifest(policy)
    if (
        policy["schema_version"] != "hmm_risk_c010_feature_domain_policy_v2"
        or policy["dataset_manifest_hash"] != source_identity["dataset_manifest_hash"]
        or source_identity["generation"] != FROZEN_GENERATION
        or source_identity["manifest_sha256"] != FROZEN_MANIFEST
    ):
        raise FormalStateError("hmm_risk_c010_policy_identity_mismatch", "current A5/source identity required")
    if list(calendar) != sorted(set(calendar)) or any(date.fromisoformat(d) > date(2025, 4, 30) for d in calendar):
        raise FormalStateError("hmm_risk_formal_input_invalid", "source calendar exceeds approved utility watermark")
    train_calendar = [d for d in calendar if "2022-01-01" <= d <= "2024-06-30"]
    validation_calendar = [d for d in calendar if "2024-07-01" <= d <= "2025-03-31"]
    if (
        len(train_calendar) != 601
        or canonical_sha256(train_calendar) != TRAIN_CALENDAR_HASH
        or len(validation_calendar) != 182
    ):
        raise FormalStateError("hmm_risk_formal_input_invalid", "frozen calendars differ")
    if set(panels) != {"L1", "L2"} or set(sector_codes) != {"L1", "L2"}:
        raise FormalStateError("hmm_risk_formal_input_invalid", "both direct levels required")
    series = {}
    insufficient = []
    for level, count in (("L1", 31), ("L2", 131)):
        codes = list(sector_codes[level])
        if len(codes) != count or codes != sorted(set(codes)):
            raise FormalStateError("hmm_risk_formal_input_invalid", f"{level} canonical denominator differs")
        panel = _frame(panels[level], codes=codes, calendar=calendar)
        for family in FAMILIES:
            features = list(BASE_FEATURES if family == FAMILIES[0] else ALL_CORE_FEATURES)
            key = f"{family}:{level}"
            series[key] = {}
            for code in codes:
                frame = panel.xs(code, level=1)
                train = frame.reindex(pd.to_datetime(train_calendar))[features]
                train_mask = np.isfinite(train.to_numpy(dtype=np.float64)).all(axis=1)
                train = train.loc[train_mask]
                if len(train) < 120:
                    insufficient.append({"family": family, "level": level, "sector": code, "rows": len(train)})
                validation = frame.reindex(pd.to_datetime(validation_calendar))[features].to_numpy(dtype=np.float64)
                daily = frame["daily_return"].to_numpy(dtype=np.float64)
                benchmark = frame["benchmark_return"].to_numpy(dtype=np.float64)
                calendar_position = {day: index for index, day in enumerate(calendar)}
                components = {}
                for horizon in (5, 10, 20):
                    available_positions, utility = [], []
                    for p, day in enumerate(validation_calendar):
                        i = calendar_position[day]
                        # Existing C-007-A utility: forward SUM of daily excess,
                        # not a newly introduced compounded-return target.
                        sector_window = daily[i + 1 : i + horizon + 1]
                        market_window = benchmark[i + 1 : i + horizon + 1]
                        if (
                            len(sector_window) == horizon
                            and np.isfinite(sector_window).all()
                            and np.isfinite(market_window).all()
                        ):
                            value = float(np.sum(sector_window - market_window))
                            if np.isfinite(value):
                                available_positions.append(p)
                                utility.append(value)
                    components[f"excess_return_{horizon}d"] = {"positions": available_positions, "values": utility}
                body = {
                    "feature_names": features,
                    "train_dates": [d.date().isoformat() for d in train.index],
                    "train_values": train.to_numpy(dtype=np.float64).tolist(),
                    "validation": build_calendar_carrier(
                        dates=validation_calendar,
                        feature_names=features,
                        observations=validation,
                        components=components,
                        source_identity_sha256=canonical_sha256(source_identity),
                        source_receipt_sha256=policy["receipt_sha256"],
                    ),
                }
                series[key][code] = {**body, "source_receipt_sha256": canonical_sha256(body)}
    if insufficient:
        raise FormalStateError(
            "hmm_risk_model_train_coverage_insufficient", "complete 31/131 grid cannot be frozen", evidence=insufficient
        )
    return receipt(
        {
            "schema_version": VERSION,
            "contracts": CONTRACTS,
            "source_identity": dict(source_identity),
            "industry_authority": dict(industry_authority),
            "policy": policy,
            "train_calendar": train_calendar,
            "validation_calendar": validation_calendar,
            "sector_codes": dict(sector_codes),
            "series": series,
        }
    )
