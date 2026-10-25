"""Build a forward C-013 binding from a pinned, actually observed official source.

Observation proves availability no earlier than the next Shanghai session. It
does not establish first publication, authorize historical backcasting, or turn
database member assignments into a classification authority.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from dataclasses import dataclass, fields, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

from backend.services.industry_pit.artifact_store import CandidateBundleReadback, write_candidate_bundle
from backend.services.industry_pit.candidate_builder import FrozenDenominator, full_denominator_preflight
from backend.services.industry_pit.contracts import (
    AuthorityType,
    TaxonomyIdentity,
    make_candidate_interval,
    require_symbol,
)
from backend.services.industry_pit.resolver import IndustryPitResolver

from .contracts import canonical_sha256
from .formal_state_executor import read_json, validate_output_location, write_once
from .rotation_l1_input_bundle import _industry_adapter

VERSION = "hmm_risk_observed_industry_authority_v1"
OFFICIAL_URL = "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"
SHANGHAI = ZoneInfo("Asia/Shanghai")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def pinned_bytes(path: Path, expected: str) -> bytes:
    require(path.is_absolute(), "source path must be absolute")
    junction = getattr(os.path, "isjunction", lambda _: False)
    require(not any(p.is_symlink() or junction(p) for p in (path, *path.parents)), "indirect source path")
    require(path.is_file(), f"required ordinary source file missing: {path}")
    before = path.stat()
    data = path.read_bytes()
    after = path.stat()
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), "source changed during read")
    require(hashlib.sha256(data).hexdigest() == expected, f"source hash differs: {path}")
    return data


def known_day(captured_at: datetime, calendar: tuple[date, ...]) -> date:
    require(captured_at.tzinfo is not None and captured_at.utcoffset() is not None, "capture timezone required")
    local_day = captured_at.astimezone(SHANGHAI).date()
    result = next((day for day in calendar if day > local_day), None)
    require(result is not None, "no next session within explicit denominator")
    return result


@dataclass(frozen=True)
class Observation:
    captured_at: datetime
    data_date: date
    rows: Mapping[str, Mapping[str, Any]]
    source_hashes: tuple[str, ...]
    conflicts: tuple[Mapping[str, Any], ...]


def _object(data: bytes) -> dict[str, Any]:
    result = json.loads(data, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    require(isinstance(result, dict), "source JSON object required")
    return result


def _timestamp(value: Any) -> datetime:
    require(isinstance(value, str), "explicit capture timestamp required")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(result.tzinfo is not None and result.utcoffset() is not None, "capture timezone required")
    return result


def load_observation(
    mapping_path: Path,
    mapping_sha256: str,
    readback_path: Path,
    readback_sha256: str,
    authority: Mapping[str, Any],
) -> Observation:
    """Check both handoff files against the captured workbook, not DB assignments."""
    mapping = _object(pinned_bytes(mapping_path, mapping_sha256))
    readback = _object(pinned_bytes(readback_path, readback_sha256))
    require(
        mapping.get("artifact_role") == "OFFICIAL_SOURCE_EFFECTIVE_MAPPING_READBACK_NOT_RUNTIME_AUTHORITY",
        "source mapping role differs",
    )
    require(
        readback.get("status") == "SOURCE_READBACK_COMPLETE_FORWARD_AUTHORITY_NOT_REBUILT",
        "source readback status differs",
    )
    require(readback.get("mapping_file", {}).get("file_sha256") == mapping_sha256, "mapping/readback hash differs")
    require(Path(readback["mapping_file"]["path"]).resolve() == mapping_path.resolve(), "mapping/readback path differs")
    require(mapping.get("taxonomy_version") == "SW2021", "official taxonomy version differs")
    require(mapping.get("data_date") == readback.get("data_date"), "readback data date differs")
    require(
        mapping.get("code_projection", {}).get("canonical_hash") == authority["l2_projection"]["canonical_hash"],
        "official text-code projection differs",
    )
    source = mapping["official_source"]
    pins = readback["source_file_identities"]
    capture_path = Path(source["capture_receipt"])
    capture_hash = pins.get(str(capture_path))
    require(isinstance(capture_hash, str), "capture receipt pin missing")
    capture = _object(pinned_bytes(capture_path, capture_hash))
    require(
        capture.get("status") == 200 and capture.get("tls_verification_disabled") is False,
        "official capture did not verify transport",
    )
    require(
        source.get("url") == capture.get("source_url") == capture.get("final_url") == OFFICIAL_URL,
        "official classification source URL differs",
    )
    require(
        source.get("sha256") == capture.get("sha256") and source.get("captured_at") == capture.get("captured_at"),
        "official source/capture identity differs",
    )
    workbook_path = Path(source["path"])
    require(workbook_path.resolve() == Path(capture["path"]).resolve(), "captured workbook path differs")
    raw = pinned_bytes(workbook_path, source["sha256"])
    require(len(raw) == capture.get("bytes"), "captured workbook byte count differs")
    captured_at = _timestamp(capture["captured_at"])
    frame = pd.read_excel(io.BytesIO(raw), dtype=object)
    require(
        frame.shape == (capture["row_count"], 4)
        and list(frame.columns) == ["股票代码", "计入日期", "行业代码", "更新日期"],
        "captured workbook schema differs",
    )
    # Source fields remain source-reported (timezone unknown). Only the proven
    # capture timestamp establishes a timezone-aware knowledge lower bound.
    latest: dict[str, tuple[pd.Timestamp, set[str]]] = {}
    for stock, included, code, _ in frame.itertuples(index=False, name=None):
        numeric = str(stock).strip().zfill(6)
        stamp = pd.Timestamp(included)
        require(len(numeric) == 6 and numeric.isdigit() and not pd.isna(stamp), "invalid workbook identity/date")
        require(stamp.tzinfo is None, "source-reported timestamp timezone unexpectedly changed")
        if stamp.date() > date.fromisoformat(mapping["data_date"]):
            continue
        previous = latest.get(numeric)
        text_code = str(code).strip().zfill(6)
        if previous is None or stamp > previous[0]:
            latest[numeric] = (stamp, {text_code})
        elif stamp == previous[0]:
            previous[1].add(text_code)
    projection = {row["canonical_l2_code"]: row["taxonomy_l2_code"] for row in authority["l2_projection"]["rows"]}
    rows: dict[str, Mapping[str, Any]] = {}
    for row in mapping["rows"]:
        symbol = require_symbol(row["symbol"])
        require(symbol not in rows, "duplicate observed symbol")
        source_row = int(row["source_excel_row"]) - 2
        require(0 <= source_row < len(frame), "source row reference escapes workbook")
        stock, included, code, updated = frame.iloc[source_row].tolist()
        require(str(stock).strip().zfill(6) == symbol[:6], "source row symbol differs")
        require(str(code).strip().zfill(6) == row["taxonomy_l3_code"], "source row taxonomy differs")
        require(pd.Timestamp(included) == pd.Timestamp(row["source_reported_inclusion_at"]), "source inclusion differs")
        recorded_update = pd.Timestamp(row["source_reported_update_at"])
        require(
            pd.Timestamp(updated) == recorded_update or (pd.isna(updated) and pd.isna(recorded_update)),
            "source update differs",
        )
        require(
            latest.get(symbol[:6]) == (pd.Timestamp(included), {row["taxonomy_l3_code"]}),
            "observed row is not the unique latest official assignment",
        )
        require(
            projection.get(row["canonical_l2_code"]) == row["taxonomy_l2_code"]
            and row["taxonomy_l3_code"][:4] + "00" == row["taxonomy_l2_code"],
            "L2 projection differs",
        )
        require(
            row.get("publication_at") is None and row.get("publication_time_quality") == "FIRST_PUBLICATION_UNVERIFIED",
            "first publication must remain unverified",
        )
        require(_timestamp(row["first_proven_observation_at"]) == captured_at, "row observation differs")
        rows[symbol] = dict(row)
    require(
        len(rows) > 0 and len(rows) == mapping.get("stock_count") == readback.get("expected_symbols"),
        "observed stock denominator differs",
    )
    conflicts = tuple(
        {
            "symbol": row["symbol"],
            "official_l2_code": row["official_classification"]["canonical_l2_code"],
            "database_l2_code": row["database_member_projection"]["canonical_l2_code"],
        }
        for row in readback["official_database_l2_disagreements"]
    )
    require(len({row["symbol"] for row in conflicts}) == len(conflicts), "duplicate DB diagnostic conflict")
    require(
        all(
            row["symbol"] in rows
            and rows[row["symbol"]]["canonical_l2_code"] == row["official_l2_code"]
            and row["official_l2_code"] != row["database_l2_code"]
            for row in conflicts
        ),
        "DB diagnostic differs",
    )
    require(
        readback.get("missing_mapping_symbols") == readback.get("unresolved_l2_projection_symbols") == 0
        and not readback.get("ambiguous_latest_assignment_symbols")
        and readback.get("official_database_l2_match_symbols") + len(conflicts) == len(rows),
        "source readback closure differs",
    )
    return Observation(
        captured_at,
        date.fromisoformat(mapping["data_date"]),
        rows,
        (mapping_sha256, readback_sha256, capture_hash, source["sha256"]),
        conflicts,
    )


def _rebind(row, receipt, *, start: date, end: date):
    values = {
        field.name: getattr(row, field.name)
        for field in fields(row)
        if field.name not in {"schema_version", "row_hash"}
    }
    values.update(
        authority_receipt_hash=receipt.receipt_hash,
        eligible_from=start,
        eligible_to_exclusive=end,
        lineage_hashes=(*row.lineage_hashes, row.row_hash),
    )
    return make_candidate_interval(**values)


def build_intervals(
    base: CandidateBundleReadback,
    denominator: FrozenDenominator,
    observation: Observation,
    catalog: Mapping[str, TaxonomyIdentity],
):
    """Preserve old causal facts and append only the post-observation assignments."""
    require(observation.data_date == denominator.window_end, "observation/denominator window differs")
    require(bool(observation.source_hashes), "observation source pins are missing")
    require(
        all(len(h) == 64 and set(h) <= set("0123456789abcdef") for h in observation.source_hashes),
        "invalid observation source hash",
    )
    known = known_day(observation.captured_at, denominator.trading_dates)
    targets = {
        span.canonical_symbol
        for span in denominator.universe_spans
        if span.eligible_start <= denominator.window_end
        and (span.eligible_end is None or span.eligible_end >= denominator.window_end)
    }
    require(set(observation.rows) == targets, "observed mapping does not close the target PIT population")
    receipt = replace(
        base.classification_receipt,
        authority_version=VERSION,
        source_ids=(*base.classification_receipt.source_ids, "official:sw2021:observed-next-session"),
        source_hashes=(
            *base.classification_receipt.source_hashes,
            *observation.source_hashes,
            base.classification_receipt.receipt_hash,
        ),
        frozen_denominator=denominator.total_opportunities,
        denominator_digest=denominator.digest,
    )
    index_receipt = replace(
        base.index_membership_receipt,
        source_hashes=(*base.index_membership_receipt.source_hashes, base.index_membership_receipt.receipt_hash),
        frozen_denominator=denominator.total_opportunities,
        denominator_digest=denominator.digest,
    )
    end = denominator.window_end + timedelta(days=1)
    classification, membership = [], []
    for old in base.classification_intervals:
        stop = (
            min(old.eligible_to_exclusive or end, end, known)
            if old.canonical_symbol in observation.rows
            else min(old.eligible_to_exclusive or end, end)
        )
        start = max(old.eligible_from, denominator.window_start)
        if start < stop:
            classification.append(_rebind(old, receipt, start=start, end=stop))
    for old in base.index_membership_intervals:
        start, stop = max(old.eligible_from, denominator.window_start), min(old.eligible_to_exclusive or end, end)
        if start < stop:
            membership.append(_rebind(old, index_receipt, start=start, end=stop))
    for span in denominator.universe_spans:
        row = observation.rows.get(span.canonical_symbol)
        if row is None:
            continue
        require(row["taxonomy_l3_code"] in catalog, "observed taxonomy is unknown")
        identity = catalog[row["taxonomy_l3_code"]]
        valid = date.fromisoformat(row["source_reported_inclusion_at"][:10])
        start = max(span.eligible_start, denominator.window_start, known, valid)
        stop = min(span.end_exclusive or end, end)
        if start >= stop:
            continue
        classification.append(
            make_candidate_interval(
                canonical_symbol=span.canonical_symbol,
                authority_type=AuthorityType.CLASSIFICATION,
                taxonomy_contract_id=receipt.taxonomy_contract_id,
                taxonomy_version=receipt.taxonomy_version,
                authority_receipt_hash=receipt.receipt_hash,
                valid_from=valid,
                valid_to_exclusive=None,
                eligible_from=start,
                eligible_to_exclusive=stop,
                causal_use_from=max(valid, known),
                causal_use_to_exclusive=None,
                known_from=known,
                source_effective_field="source_reported_inclusion_at",
                source_last_updated_at=row["source_reported_update_at"],
                research_basis=receipt.research_basis,
                non_as_known_taxonomy=False,
                identity=identity,
                authority_identity={
                    "classification_l1_code": identity.l1_code,
                    "classification_l2_code": identity.l2_code,
                    "classification_l3_code": identity.l3_code,
                },
                unavailable_reason=None,
                source_ids=("official:sw2021:observed-next-session",),
                source_hashes=observation.source_hashes,
                lineage_hashes=(canonical_sha256(dict(row)),),
            )
        )
    return receipt, tuple(classification), index_receipt, tuple(membership)


def build_authority(
    *,
    base_authority: Mapping[str, Any],
    observation: Observation,
    denominator: FrozenDenominator,
    output_root: Path,
    source_commit: str,
    source_tree: str,
    forbidden_roots: tuple[Path, ...],
) -> dict[str, Any]:
    """Write a new standard bundle and binding, without runtime activation."""
    output_root = validate_output_location(output_root, dataset_root=Path(base_authority["artifact_root"]))
    require(not output_root.exists(), "authority output already exists")
    base = _industry_adapter(base_authority, forbidden_roots=forbidden_roots).authority_bundle
    taxonomy = read_json(base.artifact_root / "taxonomy_catalog.json")
    from backend.services.industry_pit.candidate_builder import taxonomy_catalog_from_mapping

    catalog = taxonomy_catalog_from_mapping(taxonomy)
    classification_receipt, classification, index_receipt, membership = build_intervals(
        base, denominator, observation, catalog.identities
    )

    def resolver(receipt, intervals):
        return IndustryPitResolver(
            receipt=receipt,
            intervals=intervals,
            known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
        )

    preflight = full_denominator_preflight(
        denominator=denominator,
        classification_resolver=resolver(classification_receipt, classification),
        index_membership_resolver=resolver(index_receipt, membership),
        conflict_inventory={},
        mandatory_symbols=(),
    )
    bundle = write_candidate_bundle(
        artifact_root=output_root / "bundle",
        forbidden_roots=forbidden_roots,
        taxonomy_catalog=taxonomy,
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=classification,
        index_membership_intervals=membership,
        preflight_report=preflight,
        producer_commit=source_commit,
        producer_tree=source_tree,
    )
    authority = dict(base_authority)
    authority["artifact_root"] = str(bundle.artifact_root)
    authority["identity"] = {
        "schema_version": "hmm_risk_industry_pit_authority_v1",
        "bundle_hash": bundle.manifest["bundle_hash"],
        "classification_candidate_hash": bundle.manifest["classification_candidate_hash"],
        "index_membership_candidate_hash": bundle.manifest["index_membership_candidate_hash"],
        "classification_receipt_hash": classification_receipt.receipt_hash,
        "index_membership_receipt_hash": index_receipt.receipt_hash,
        "preflight_canonical_hash": preflight["canonical_hash"],
    }
    checked = _industry_adapter(authority, forbidden_roots=forbidden_roots)
    target = denominator.window_end
    reasons: dict[str, int] = {}
    resolved = 0
    for symbol in sorted(observation.rows):
        result = checked.resolve(symbol, target)
        if result.status == "resolved":
            resolved += 1
        else:
            reasons[result.reason_code] = reasons.get(result.reason_code, 0) + 1
    require(resolved == len(observation.rows) and not reasons, "rebuilt target classification did not resolve")
    write_once(output_root / "authority.json", authority)
    result = {
        "schema_version": VERSION,
        "source_commit": source_commit,
        "source_tree": source_tree,
        "source_observed_at": observation.captured_at.isoformat(),
        "first_publication_verified": False,
        "causal_daily_from": known_day(observation.captured_at, denominator.trading_dates).isoformat(),
        "data_date": target.isoformat(),
        "target_symbols": len(observation.rows),
        "resolved_symbols": resolved,
        "target_unavailable": reasons,
        "classification_source": OFFICIAL_URL,
        "official_database_disagreements": list(observation.conflicts),
        "database_is_classification_fallback": False,
        "authority_path": str(output_root / "authority.json"),
        "authority_canonical_sha256": canonical_sha256(authority),
        "authority_file_sha256": hashlib.sha256((output_root / "authority.json").read_bytes()).hexdigest(),
        "identity": authority["identity"],
        "preflight": preflight,
        "model_fit_count": 0,
        "database_write": False,
        "dataset_write": False,
        "runtime_activation": False,
    }
    write_once(output_root / "build_receipt.json", result)
    return result
