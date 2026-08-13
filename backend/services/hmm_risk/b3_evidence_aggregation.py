"""Zero-refit aggregation of frozen TRANSITION-DWELL-B child evidence.

The source children are intentionally large canonical JSON objects.  This module
reads each source once, extracts only the approved compact evidence arrays, and
never loads or copies the embedded model payloads.
"""

from __future__ import annotations

import hashlib
import json
import math
import mmap
import os
import re
import statistics
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from backend.services.hmm_risk.state_model_set import canonical_json_bytes, canonical_sha256

AGGREGATION_SCHEMA_VERSION = "hmm_risk_phase2_p2_2_evidence_aggregation_v1"
SOURCE_PARENT_SCHEMA_VERSION = "hmm_risk_c008_b3_transition_dwell_b_diagnostic_v1"
SOURCE_CHILD_SCHEMA_VERSION = "hmm_risk_c008_b3_transition_dwell_b_single_pass_v1"
SOURCE_CONTRACT_VERSION = "hmm_risk_c008_b3_transition_dwell_b_v1"
K2_HYPOTHESIS = "K3_STRUCTURE_COLLAPSE_SUGGESTS_K2_HYPOTHESIS"
EXPECTED_SEEDS = tuple(range(42, 50))
EXPECTED_SECTOR_COUNT = 131
EXPECTED_ENTRY_COUNT = EXPECTED_SECTOR_COUNT * len(EXPECTED_SEEDS)


class B3EvidenceAggregationError(ValueError):
    """Raised when frozen evidence cannot be aggregated without guessing."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise B3EvidenceAggregationError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _load_parent(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_keys)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise B3EvidenceAggregationError(f"TRANSITION-DWELL-B parent is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise B3EvidenceAggregationError("TRANSITION-DWELL-B parent is not an object")
    body = {key: item for key, item in value.items() if key != "report_sha256"}
    forbidden_true = (
        "selection_performed",
        "d5_executed",
        "d6_executed",
        "semantic_mapping_performed",
        "formal_d5_stability_gate_applied",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    )
    if (
        value.get("schema_version") != SOURCE_PARENT_SCHEMA_VERSION
        or value.get("contract_version") != SOURCE_CONTRACT_VERSION
        or value.get("status") != "diagnostic_complete_no_complete_candidate"
        or value.get("planned_fit_count") != 2096
        or value.get("terminal_entry_count") != 2096
        or value.get("canonical_payload_bitwise_equal") is not True
        or value.get("candidate_seed_count") != 0
        or value.get("diagnostic_complete_candidate_seeds") != []
        or value.get("report_sha256") != canonical_sha256(body)
        or any(value.get(field) is not False for field in forbidden_true)
    ):
        raise B3EvidenceAggregationError("TRANSITION-DWELL-B parent contract is not eligible for P2-2")
    receipts = value.get("fresh_process_receipts")
    if not isinstance(receipts, list) or len(receipts) != 2:
        raise B3EvidenceAggregationError("TRANSITION-DWELL-B parent child closure is incomplete")
    identities = [item.get("process_identity") for item in receipts if isinstance(item, Mapping)]
    if identities != ["fresh_process_1", "fresh_process_2"]:
        raise B3EvidenceAggregationError("TRANSITION-DWELL-B child process order is invalid")
    return value


def _entry_rejections(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    if entry.get("fit_status") != "accepted":
        reasons = list(entry.get("failure_reason_codes") or ())
        return [
            {
                "stage": str(entry.get("failure_stage") or "fit"),
                "status": "failed",
                "reason_codes": reasons or ["hmm_risk_model_fit_failed"],
            }
        ]
    rejected: list[dict[str, Any]] = []
    likelihood = entry.get("likelihood")
    if isinstance(likelihood, Mapping) and not (
        likelihood.get("convergence_valid") is True and likelihood.get("likelihood_valid") is True
    ):
        rejected.append(
            {
                "stage": "likelihood",
                "status": str(likelihood.get("likelihood_status") or "failed"),
                "reason_codes": list(likelihood.get("failure_reason_codes") or ())
                + list(likelihood.get("blocking_reason_codes") or ()),
            }
        )
    covariance = entry.get("covariance")
    if isinstance(covariance, Mapping) and covariance.get("covariance_valid") is not True:
        rejected.append(
            {
                "stage": "covariance",
                "status": str(covariance.get("covariance_status") or "failed"),
                "reason_codes": list(covariance.get("failure_reason_codes") or ())
                + list(covariance.get("blocking_reason_codes") or ()),
            }
        )
    occupancy = entry.get("train_occupancy")
    if isinstance(occupancy, Mapping) and occupancy.get("train_occupancy_valid") is not True:
        rejected.append(
            {
                "stage": "train_occupancy",
                "status": str(occupancy.get("train_occupancy_status") or "failed"),
                "reason_codes": list(occupancy.get("failure_reason_codes") or ())
                + list(occupancy.get("blocking_reason_codes") or ()),
            }
        )
    if entry.get("model_entry_valid") is not True and not rejected:
        rejected.append(
            {
                "stage": "model_entry",
                "status": str(entry.get("model_entry_status") or "failed"),
                "reason_codes": ["hmm_risk_model_selection_contract_unsatisfied"],
            }
        )
    for value in rejected:
        if not value["reason_codes"]:
            value["reason_codes"] = ["hmm_risk_model_selection_contract_unsatisfied"]
    return rejected


def _window_summary(value: Mapping[str, Any]) -> dict[str, Any]:
    reasons = tuple(sorted(str(item) for item in value.get("reason_codes") or ()))
    return {
        "status": str(value.get("status") or ""),
        "reason_codes": list(reasons),
        "structurally_observed": value.get("status") == "train_window_structurally_observed",
    }


def _dominant_failure(stages: Sequence[Mapping[str, Any]], early: Mapping[str, Any], late: Mapping[str, Any]) -> str:
    if stages:
        order = {"fit": 0, "likelihood": 1, "covariance": 2, "train_occupancy": 3, "model_entry": 4}
        stage = min((str(item.get("stage") or "model_entry") for item in stages), key=lambda item: order.get(item, 9))
        if stage not in order:
            stage = "fit"
        return stage
    reasons = Counter(str(item) for value in (early, late) for item in value.get("reason_codes") or ())
    if not reasons:
        return "accepted"
    if any("posterior" in reason for reason in reasons):
        return "posterior"
    if any("transition" in reason or "run" in reason for reason in reasons):
        return "transition_run_dwell"
    if any("count" in reason or "occupancy" in reason or "month" in reason for reason in reasons):
        return "occupancy_coverage"
    return "other_structure"


def _compact_entry_source(value: dict[str, Any]) -> dict[str, Any]:
    stages = _entry_rejections(value)
    return {
        "seed": value.get("seed"),
        "sector_code": value.get("sector_code"),
        "d3_d4_status": "accepted" if not stages else "rejected",
        "d3_d4_rejections": stages,
    }


def _compact_profile_source(value: dict[str, Any]) -> dict[str, Any]:
    early = _window_summary(value.get("early") if isinstance(value.get("early"), Mapping) else {})
    late = _window_summary(value.get("late") if isinstance(value.get("late"), Mapping) else {})
    return {
        "seed": value.get("seed"),
        "sector_code": value.get("sector_code"),
        "early": early,
        "late": late,
    }


def _quintile_labels(values: Mapping[str, float]) -> dict[str, str]:
    ordered = sorted(values, key=lambda code: (values[code], code))
    count = len(ordered)
    if count != EXPECTED_SECTOR_COUNT:
        raise B3EvidenceAggregationError("coverage proxy does not cover the canonical 131-sector denominator")
    return {code: f"Q{min(4, index * 5 // count) + 1}" for index, code in enumerate(ordered)}


def _coverage_proxy(
    receipts: Sequence[Mapping[str, Any]],
    invalid_receipts: Sequence[Mapping[str, Any]],
    sector_codes: Sequence[str],
) -> dict[str, Any]:
    size_values: dict[str, list[float]] = defaultdict(list)
    liquidity_values: dict[str, list[float]] = defaultdict(list)
    for receipt in receipts:
        code = str(receipt.get("sector_code") or "")
        if code not in sector_codes:
            raise B3EvidenceAggregationError("L2 domain receipt contains a non-canonical sector")
        size = receipt.get("price_expected_weight")
        liquidity = receipt.get("moneyflow_contributor_amount")
        if isinstance(size, (int, float)) and math.isfinite(float(size)) and float(size) > 0:
            size_values[code].append(float(size))
        if isinstance(liquidity, (int, float)) and math.isfinite(float(liquidity)) and float(liquidity) >= 0:
            liquidity_values[code].append(float(liquidity))
    if set(size_values) != set(sector_codes) or set(liquidity_values) != set(sector_codes):
        raise B3EvidenceAggregationError("size/liquidity proxy coverage is incomplete")
    invalid_counts: Counter[str] = Counter()
    invalid_reasons: Counter[str] = Counter()
    for receipt in invalid_receipts:
        code = str(receipt.get("sector_code") or "")
        reason = str(receipt.get("price_domain_reason_code") or "")
        if code not in sector_codes or not reason:
            raise B3EvidenceAggregationError("invalid L2 domain receipt is incomplete")
        invalid_counts[code] += 1
        invalid_reasons[reason] += 1
    total_counts = {code: len(size_values[code]) + invalid_counts[code] for code in sector_codes}
    if len(set(total_counts.values())) != 1 or next(iter(total_counts.values()), 0) <= 0:
        raise B3EvidenceAggregationError("L2 domain receipt date denominator differs by sector")
    size_median = {code: statistics.median(values) for code, values in size_values.items()}
    liquidity_median = {code: statistics.median(values) for code, values in liquidity_values.items()}
    return {
        "proxy_contract": "frozen_train_l2_daily_median_v1",
        "size_proxy": "median_price_expected_weight",
        "liquidity_proxy": "median_moneyflow_contributor_amount",
        "domain_date_count_per_sector": next(iter(total_counts.values())),
        "valid_domain_receipt_count": len(receipts),
        "invalid_domain_receipt_count": len(invalid_receipts),
        "invalid_domain_reason_counts": dict(sorted(invalid_reasons.items())),
        "invalid_domain_sector_counts": [
            {"sector_code": code, "invalid_date_count": invalid_counts[code]}
            for code in sorted(invalid_counts)
            if invalid_counts[code]
        ],
        "size_quintile_by_sector": _quintile_labels(size_median),
        "liquidity_quintile_by_sector": _quintile_labels(liquidity_median),
        "l1_parent_and_industry_mapping_status": "insufficient_evidence",
        "l1_parent_and_industry_mapping_reason_code": "hmm_risk_p2_2_child_artifact_l1_mapping_unavailable",
    }


_FINITE_NUMBER_PATTERN = rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
_SECTOR_PATTERN = re.compile(rb'"sector_code":"([^"]+)"')
_SIZE_PATTERN = re.compile(rb'"price_expected_weight":(' + _FINITE_NUMBER_PATTERN + rb")")
_LIQUIDITY_PATTERN = re.compile(rb'"moneyflow_contributor_amount":(' + _FINITE_NUMBER_PATTERN + rb")")
_PRICE_DOMAIN_REASON_PATTERN = re.compile(rb'"price_domain_reason_code":"([^"]+)"')


def _canonical_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    pending = b""
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            combined = pending + chunk
            digest.update(combined[:-1])
            pending = combined[-1:]
    if pending != b"\n":
        raise B3EvidenceAggregationError(f"canonical source must end in exactly one LF: {path}")
    return digest.hexdigest()


def _array_bounds(
    source: mmap.mmap,
    *,
    marker: bytes,
    end_marker: bytes,
    start: int = 0,
) -> tuple[int, int]:
    marker_at = source.find(marker, start)
    if marker_at < 0:
        raise B3EvidenceAggregationError(f"canonical JSON marker is missing: {marker!r}")
    array_start = marker_at + len(marker) - 1
    array_end = source.find(end_marker, array_start)
    if array_end < 0 or source[array_start : array_start + 1] != b"[":
        raise B3EvidenceAggregationError(f"canonical JSON array boundary is invalid: {marker!r}")
    return array_start, array_end


def _iter_prefixed_objects(
    source: mmap.mmap,
    *,
    array_start: int,
    array_end: int,
    object_prefix: bytes,
):
    position = array_start + 1
    delimiter = b"," + object_prefix
    if position == array_end:
        return
    if source[position : position + len(object_prefix)] != object_prefix:
        raise B3EvidenceAggregationError("canonical evidence array object prefix is invalid")
    while position < array_end:
        next_position = source.find(delimiter, position)
        object_end = array_end if next_position < 0 or next_position >= array_end else next_position
        yield memoryview(source)[position:object_end]
        if object_end == array_end:
            return
        position = next_position + 1


def _parse_compact_objects(
    source: mmap.mmap,
    *,
    array_start: int,
    array_end: int,
    object_prefix: bytes,
    transform: Callable[[dict[str, Any]], Any],
) -> list[Any]:
    values: list[Any] = []
    for raw_view in _iter_prefixed_objects(
        source,
        array_start=array_start,
        array_end=array_end,
        object_prefix=object_prefix,
    ):
        raw = bytes(raw_view)
        try:
            parsed = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise B3EvidenceAggregationError("canonical evidence array item is invalid JSON") from exc
        if not isinstance(parsed, dict) or canonical_json_bytes(parsed) != raw:
            raise B3EvidenceAggregationError("canonical evidence array item is not canonical")
        values.append(transform(parsed))
    return values


def _compact_domain_objects(
    source: mmap.mmap,
    *,
    array_start: int,
    array_end: int,
) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for raw in _iter_prefixed_objects(
        source,
        array_start=array_start,
        array_end=array_end,
        object_prefix=b'{"direct_sector_level":"L2"',
    ):
        sector = _SECTOR_PATTERN.search(raw)
        size = _SIZE_PATTERN.search(raw)
        liquidity = _LIQUIDITY_PATTERN.search(raw)
        if not sector or not size or not liquidity:
            raise B3EvidenceAggregationError("L2 domain receipt is missing a coverage proxy field")
        values.append(
            {
                "sector_code": sector.group(1).decode("utf-8"),
                "price_expected_weight": float(size.group(1)),
                "moneyflow_contributor_amount": float(liquidity.group(1)),
            }
        )
    return values


def _compact_invalid_domain_objects(
    source: mmap.mmap,
    *,
    array_start: int,
    array_end: int,
) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for raw in _iter_prefixed_objects(
        source,
        array_start=array_start,
        array_end=array_end,
        object_prefix=b'{"direct_sector_level":"L2"',
    ):
        sector = _SECTOR_PATTERN.search(raw)
        reason = _PRICE_DOMAIN_REASON_PATTERN.search(raw)
        if not sector or not reason:
            raise B3EvidenceAggregationError("invalid L2 domain receipt is missing its typed reason")
        values.append(
            {
                "sector_code": sector.group(1).decode("utf-8"),
                "price_domain_reason_code": reason.group(1).decode("utf-8"),
            }
        )
    return values


def _extract_child(path: Path, *, include_coverage: bool) -> dict[str, Any]:
    canonical_file_sha256 = _canonical_file_sha256(path)
    with path.open("rb") as handle, mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as source:
        domain_receipts: list[dict[str, Any]] = []
        invalid_domain_receipts: list[dict[str, Any]] = []
        if include_coverage:
            aggregate_at = source.find(b'"aggregate_receipt":{')
            domain_start, domain_end = _array_bounds(
                source,
                marker=b'"l2_domain_receipts":[',
                end_marker=b'],"l2_invalid_price_domain"',
                start=aggregate_at,
            )
            domain_receipts = _compact_domain_objects(
                source,
                array_start=domain_start,
                array_end=domain_end,
            )
            invalid_start, invalid_end = _array_bounds(
                source,
                marker=b'"l2_invalid_price_domain":[',
                end_marker=b'],"missing_price_row_count"',
                start=domain_end,
            )
            invalid_domain_receipts = _compact_invalid_domain_objects(
                source,
                array_start=invalid_start,
                array_end=invalid_end,
            )
        level_at = source.find(b'"level_repeat":{')
        entry_start, entry_end = _array_bounds(
            source,
            marker=b'"entries":[',
            end_marker=b'],"entry_count"',
            start=level_at,
        )
        entries = _parse_compact_objects(
            source,
            array_start=entry_start,
            array_end=entry_end,
            object_prefix=b'{"artifact_write_performed":',
            transform=_compact_entry_source,
        )
        profile_start, profile_end = _array_bounds(
            source,
            marker=b'"profiles":[',
            end_marker=b'],"provider_absence_partition_receipt"',
            start=entry_end,
        )
        profiles = _parse_compact_objects(
            source,
            array_start=profile_start,
            array_end=profile_end,
            object_prefix=b'{"both_windows_structurally_observed":',
            transform=_compact_profile_source,
        )
        entry_payload_sha256 = hashlib.sha256(memoryview(source)[entry_start : entry_end + 1]).hexdigest()
        profile_payload_sha256 = hashlib.sha256(memoryview(source)[profile_start : profile_end + 1]).hexdigest()
    return {
        "entries": entries,
        "profiles": profiles,
        "l2_domain_receipts": domain_receipts,
        "l2_invalid_price_domain": invalid_domain_receipts,
        "entry_payload_sha256": entry_payload_sha256,
        "profile_payload_sha256": profile_payload_sha256,
        "canonical_file_sha256": canonical_file_sha256,
    }


def _compact_child(value: Mapping[str, Any]) -> dict[str, Any]:
    entries = value["entries"]
    profiles = value["profiles"]
    if not isinstance(entries, Sequence) or not isinstance(profiles, Sequence):
        raise B3EvidenceAggregationError("child compact evidence arrays are invalid")
    entry_by_key: dict[tuple[int, str], Mapping[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise B3EvidenceAggregationError("child entry is invalid")
        try:
            key = (int(entry.get("seed")), str(entry.get("sector_code") or ""))
        except (TypeError, ValueError) as exc:
            raise B3EvidenceAggregationError("child entry identity is invalid") from exc
        if key in entry_by_key:
            raise B3EvidenceAggregationError("child entry identity is duplicated")
        entry_by_key[key] = entry
    profile_by_key: dict[tuple[int, str], Mapping[str, Any]] = {}
    for profile in profiles:
        if not isinstance(profile, Mapping):
            raise B3EvidenceAggregationError("child profile is invalid")
        try:
            key = (int(profile.get("seed")), str(profile.get("sector_code") or ""))
        except (TypeError, ValueError) as exc:
            raise B3EvidenceAggregationError("child profile identity is invalid") from exc
        if key in profile_by_key:
            raise B3EvidenceAggregationError("child profile identity is duplicated")
        profile_by_key[key] = profile
    if set(entry_by_key) != set(profile_by_key) or len(entry_by_key) != EXPECTED_ENTRY_COUNT:
        raise B3EvidenceAggregationError("child entry/profile closure is incomplete")
    sector_codes = sorted({code for _, code in entry_by_key})
    if len(sector_codes) != EXPECTED_SECTOR_COUNT or {seed for seed, _ in entry_by_key} != set(EXPECTED_SEEDS):
        raise B3EvidenceAggregationError("child seed/sector denominator is not the canonical 8 x 131 grid")
    compact: list[dict[str, Any]] = []
    for key in sorted(entry_by_key):
        seed, code = key
        entry = entry_by_key[key]
        profile = profile_by_key[key]
        stages = list(entry.get("d3_d4_rejections") or ())
        early = profile.get("early") if isinstance(profile.get("early"), Mapping) else {}
        late = profile.get("late") if isinstance(profile.get("late"), Mapping) else {}
        both_unobserved = not early["structurally_observed"] and not late["structurally_observed"]
        compact.append(
            {
                "seed": seed,
                "sector_code": code,
                "d3_d4_status": str(entry.get("d3_d4_status") or "rejected"),
                "d3_d4_rejections": stages,
                "early": early,
                "late": late,
                "persistent_cross_window_failure": both_unobserved,
                "dominant_failure_type": _dominant_failure(stages, early, late),
                "k_hypothesis": K2_HYPOTHESIS if both_unobserved and not stages else None,
            }
        )
    return {"sector_codes": sector_codes, "records": compact}


def _bias_summary(
    records: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    *,
    label_name: str,
) -> list[dict[str, Any]]:
    sectors_by_label: dict[str, set[str]] = defaultdict(set)
    persistent_by_label: dict[str, set[str]] = defaultdict(set)
    record_failures: Counter[str] = Counter()
    record_counts: Counter[str] = Counter()
    for record in records:
        code = str(record["sector_code"])
        label = labels[code]
        sectors_by_label[label].add(code)
        record_counts[label] += 1
        if record.get("persistent_cross_window_failure") is True:
            persistent_by_label[label].add(code)
            record_failures[label] += 1
    return [
        {
            label_name: label,
            "sector_count": len(sectors_by_label[label]),
            "persistent_failure_sector_count": len(persistent_by_label[label]),
            "persistent_failure_record_count": record_failures[label],
            "record_count": record_counts[label],
        }
        for label in sorted(sectors_by_label)
    ]


def aggregate_transition_dwell_evidence(parent_path: Path) -> dict[str, Any]:
    """Aggregate the two frozen children without fitting or selecting anything."""

    parent = _load_parent(parent_path.resolve())
    source_receipts = parent["fresh_process_receipts"]
    extracted: list[dict[str, Any]] = []
    compact_children: list[dict[str, Any]] = []
    source_summaries: list[dict[str, Any]] = []
    for index, receipt in enumerate(source_receipts):
        if not isinstance(receipt, Mapping):
            raise B3EvidenceAggregationError("TRANSITION-DWELL-B child receipt is invalid")
        path = Path(str(receipt.get("receipt_path") or "")).resolve()
        if not path.is_file():
            raise B3EvidenceAggregationError(f"TRANSITION-DWELL-B child is missing: {path}")
        child = _extract_child(path, include_coverage=index == 0)
        if child["canonical_file_sha256"] != receipt.get("receipt_sha256"):
            raise B3EvidenceAggregationError("TRANSITION-DWELL-B child canonical hash differs from parent")
        if child["entry_payload_sha256"] != receipt.get("entry_payload_sha256"):
            raise B3EvidenceAggregationError("TRANSITION-DWELL-B child entry payload differs from parent")
        if child["profile_payload_sha256"] != receipt.get("profile_payload_sha256"):
            raise B3EvidenceAggregationError("TRANSITION-DWELL-B child profile payload differs from parent")
        compact = _compact_child(child)
        extracted.append(child)
        compact_children.append(compact)
        source_summaries.append(
            {
                "process_identity": receipt["process_identity"],
                "receipt_path": str(path),
                "receipt_sha256": receipt["receipt_sha256"],
                "entry_payload_sha256": child["entry_payload_sha256"],
                "profile_payload_sha256": child["profile_payload_sha256"],
            }
        )
    first_records = compact_children[0]["records"]
    second_records = compact_children[1]["records"]
    if canonical_sha256(first_records) != canonical_sha256(second_records):
        raise B3EvidenceAggregationError("fresh-process compact evidence differs")
    sector_codes = compact_children[0]["sector_codes"]
    coverage = _coverage_proxy(
        extracted[0]["l2_domain_receipts"],
        extracted[0]["l2_invalid_price_domain"],
        sector_codes,
    )
    dominant_counts = Counter(str(item["dominant_failure_type"]) for item in first_records)
    persistent_sector_counts = Counter(
        str(item["sector_code"]) for item in first_records if item["persistent_cross_window_failure"] is True
    )
    k2_hypothesis_sectors = sorted(
        {str(item["sector_code"]) for item in first_records if item.get("k_hypothesis") == K2_HYPOTHESIS}
    )
    size_labels = coverage.pop("size_quintile_by_sector")
    liquidity_labels = coverage.pop("liquidity_quintile_by_sector")
    body = {
        "schema_version": AGGREGATION_SCHEMA_VERSION,
        "status": "diagnostic_complete",
        "source_parent_path": str(parent_path.resolve()),
        "source_parent_report_sha256": parent["report_sha256"],
        "source_contract_version": parent["contract_version"],
        "source_processes": source_summaries,
        "source_payload_bitwise_equal": True,
        "canonical_family": str(parent["control_authority"]["family"]),
        "canonical_level": str(parent["control_authority"]["level"]),
        "schedule": list(parent["control_authority"]["schedule"]),
        "sector_count": len(sector_codes),
        "record_count": len(first_records),
        "records": first_records,
        "dominant_failure_type_counts": dict(sorted(dominant_counts.items())),
        "persistent_cross_window_sector_seed_counts": [
            {"sector_code": code, "failed_seed_count": persistent_sector_counts[code]}
            for code in sorted(persistent_sector_counts)
        ],
        "k3_structure_collapse_hypothesis": {
            "status": "diagnostic_hypothesis_only" if k2_hypothesis_sectors else "not_observed",
            "reason_code": K2_HYPOTHESIS if k2_hypothesis_sectors else None,
            "sector_codes": k2_hypothesis_sectors,
            "k2_fit_performed": False,
            "k_selected": False,
        },
        "coverage_bias": {
            **coverage,
            "canonical_l2_denominator": len(sector_codes),
            "size_quintiles": _bias_summary(first_records, size_labels, label_name="size_quintile"),
            "liquidity_quintiles": _bias_summary(
                first_records,
                liquidity_labels,
                label_name="liquidity_quintile",
            ),
        },
        "selection_performed": False,
        "family_selection_performed": False,
        "refit_performed": False,
        "d5_executed": False,
        "d6_executed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
        "formal_product_thresholds_applied": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def build_aggregation_failure(*, parent_path: Path, error: Exception) -> dict[str, Any]:
    """Build a typed durable failure without claiming source completion."""

    body = {
        "schema_version": AGGREGATION_SCHEMA_VERSION,
        "status": "insufficient_evidence",
        "primary_reason_code": "hmm_risk_p2_2_evidence_aggregation_failed",
        "source_parent_path": str(parent_path.resolve()),
        "error_type": type(error).__name__,
        "error": str(error)[-4000:],
        "selection_performed": False,
        "family_selection_performed": False,
        "refit_performed": False,
        "d5_executed": False,
        "d6_executed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
        "formal_product_thresholds_applied": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def write_aggregation_report(path: Path, report: Mapping[str, Any]) -> None:
    """Persist a compact report once; conflicting retries fail closed."""

    payload = canonical_json_bytes(dict(report)) + b"\n"
    target = path.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.read_bytes() != payload:
            raise B3EvidenceAggregationError(f"aggregation report collision: {target}")
        return
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise
