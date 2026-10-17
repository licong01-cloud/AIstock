"""Release-scoped, consumer-neutral SW L2 sector context contracts."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Mapping

from .canonical import digest_named_fields, ensure_sha256


RELEASE_SW_L2_CODE_MAP_SCHEMA = "aistock_release_sw_l2_code_map_v1"
RELEASE_SW_L2_MEMBER_BACKED_SCHEMA = "aistock_release_sw_l2_member_backed_v1"
SECTOR_CONTEXT_PINS_SCHEMA = "aistock_sector_context_pins_v1"
SECTOR_CONTEXT_COMPONENT_ROOT = "components/sector_context_candidate_v1"
MARKET_VOLUME_DEFINITION = "sum_market_sw_daily_vol_all_rows_v1"
MARKET_CONTEXT_SCHEMA = "aistock_market_context_v1"
SECTOR_MEMBERSHIP_SPANS_SCHEMA = "aistock_sector_membership_spans_v1"
SECTOR_QUOTE_AVAILABILITY_SCHEMA = "aistock_release_sw_l2_quote_availability_v1"

_CODE_RE = re.compile(r"^801[0-9]{3}[.]SI$")
_SYMBOL_RE = re.compile(r"^[0-9]{6}[.](?:SH|SZ|BJ)$")
_BASE_MAP_FIELDS = {
    "schema_version",
    "mapping_authority",
    "entries",
    "code_map_digest",
}
_MAP_FIELDS = {
    *_BASE_MAP_FIELDS,
    "member_backed_codes",
    "member_backed_digest",
}
_AUTHORITY_FIELDS = {"authority_id", "authority_sha256"}
_ENTRY_FIELDS = {"l2_code_id", "canonical_l2_code"}
_QUOTE_ROOT_FIELDS = {
    "schema_version",
    "mapping_authority",
    "entries",
    "quote_availability_digest",
}
_QUOTE_ENTRY_FIELDS = {"canonical_l2_code", "availability_spans"}
_QUOTE_SPAN_FIELDS = {"start_date", "end_date"}
_PIN_FIELDS = {
    "schema_version",
    "component_root",
    "code_map_file",
    "code_map_sha256",
    "code_map_digest",
    "market_context_file",
    "market_context_sha256",
    "market_volume_definition",
    "market_start",
    "market_end",
    "membership_file",
    "membership_sha256",
    "membership_start",
    "membership_end",
    "quote_availability_file",
    "quote_availability_sha256",
    "quote_availability_digest",
    "quote_availability_schema",
    "receipt_file",
    "receipt_sha256",
    "sector_data_sha256",
    "source_dataset_manifest_sha256",
    "authority_id",
    "authority_sha256",
}


@dataclass(frozen=True, slots=True)
class ReleaseSWL2CodeMap:
    id_to_code: Mapping[int, str]
    code_to_id: Mapping[str, int]
    member_backed_codes: tuple[str, ...]
    mapping_authority: Mapping[str, str]
    code_map_digest: str
    member_backed_digest: str | None


@dataclass(frozen=True, slots=True)
class SectorQuoteAvailability:
    entries: Mapping[str, tuple[tuple[dt.date, dt.date], ...]]
    mapping_authority: Mapping[str, str]
    quote_availability_digest: str


def build_release_sw_l2_code_map_payload(
    *,
    code_to_id: Mapping[str, int],
    member_backed_codes: list[str] | tuple[str, ...],
    authority_id: str,
    authority_sha256: str,
) -> dict[str, Any]:
    """Build one deterministic shared-ID map from an already frozen producer snapshot."""

    normalized_authority = {
        "authority_id": str(authority_id).strip(),
        "authority_sha256": ensure_sha256(
            authority_sha256,
            field="mapping_authority.authority_sha256",
        ),
    }
    if not normalized_authority["authority_id"]:
        raise ValueError("mapping_authority.authority_id is empty")
    entries = sorted(
        (
            {
                "l2_code_id": sector_id,
                "canonical_l2_code": str(code).strip().upper(),
            }
            for code, sector_id in code_to_id.items()
        ),
        key=lambda row: (row["l2_code_id"], row["canonical_l2_code"]),
    )
    member_codes = sorted(str(code).strip().upper() for code in member_backed_codes)
    payload = {
        "schema_version": RELEASE_SW_L2_CODE_MAP_SCHEMA,
        "mapping_authority": normalized_authority,
        "entries": entries,
        "member_backed_codes": member_codes,
        "code_map_digest": digest_named_fields(
            RELEASE_SW_L2_CODE_MAP_SCHEMA,
            {"mapping_authority": normalized_authority, "entries": entries},
        ),
        "member_backed_digest": digest_named_fields(
            RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
            {
                "mapping_authority": normalized_authority,
                "member_backed_codes": member_codes,
            },
        ),
    }
    validate_release_sw_l2_code_map(payload)
    return payload


def _exact_mapping(value: Any, fields: set[str], field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError(f"{field} fields differ")
    return dict(value)


def _date(value: Any, field: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def validate_release_sw_l2_code_map(value: Any, *, require_member_backed: bool = True) -> ReleaseSWL2CodeMap:
    if not isinstance(value, Mapping):
        raise ValueError("sector code map fields differ")
    fields = set(value)
    if fields != _MAP_FIELDS and (require_member_backed or fields != _BASE_MAP_FIELDS):
        raise ValueError("sector code map fields differ")
    root = dict(value)
    if root["schema_version"] != RELEASE_SW_L2_CODE_MAP_SCHEMA:
        raise ValueError("invalid release SW L2 code-map schema")
    authority = _exact_mapping(root["mapping_authority"], _AUTHORITY_FIELDS, "mapping_authority")
    authority_id = str(authority["authority_id"] or "").strip()
    if not authority_id:
        raise ValueError("mapping_authority.authority_id is empty")
    authority_sha256 = ensure_sha256(str(authority["authority_sha256"]), field="mapping_authority.authority_sha256")
    normalized_authority = {
        "authority_id": authority_id,
        "authority_sha256": authority_sha256,
    }

    raw_entries = root["entries"]
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ValueError("sector code map entries are empty")
    entries: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    seen_codes: set[str] = set()
    for raw in raw_entries:
        entry = _exact_mapping(raw, _ENTRY_FIELDS, "sector code-map entry")
        raw_id = entry["l2_code_id"]
        if type(raw_id) is not int or raw_id < 0:
            raise ValueError("l2_code_id must be a non-negative integer")
        code = str(entry["canonical_l2_code"] or "").strip().upper()
        if _CODE_RE.fullmatch(code) is None:
            raise ValueError(f"invalid canonical_l2_code: {code!r}")
        if raw_id in seen_ids:
            raise ValueError(f"duplicated l2_code_id: {raw_id}")
        if code in seen_codes:
            raise ValueError(f"duplicated canonical_l2_code: {code}")
        seen_ids.add(raw_id)
        seen_codes.add(code)
        entries.append({"l2_code_id": raw_id, "canonical_l2_code": code})
    entries.sort(key=lambda row: (row["l2_code_id"], row["canonical_l2_code"]))
    if raw_entries != entries and (require_member_backed or fields == _MAP_FIELDS):
        raise ValueError("sector code-map entries are not in canonical order")

    actual_map_digest = digest_named_fields(
        RELEASE_SW_L2_CODE_MAP_SCHEMA,
        {"mapping_authority": normalized_authority, "entries": entries},
    )
    expected_map_digest = ensure_sha256(str(root["code_map_digest"]), field="code_map_digest")
    if expected_map_digest != actual_map_digest:
        raise ValueError("sector code-map digest differs")
    member_codes: tuple[str, ...] = ()
    actual_member_digest: str | None = None
    if set(root) == _MAP_FIELDS:
        member_codes_raw = root["member_backed_codes"]
        if not isinstance(member_codes_raw, list):
            raise ValueError("member_backed_codes must be a list")
        member_codes = tuple(str(value).strip().upper() for value in member_codes_raw)
        if len(member_codes) != 131 or list(member_codes) != sorted(set(member_codes)):
            raise ValueError("member_backed_codes must contain exactly 131 sorted unique codes")
        if any(_CODE_RE.fullmatch(code) is None for code in member_codes):
            raise ValueError("member_backed_codes contains an invalid code")
        if set(member_codes) - seen_codes:
            raise ValueError("member_backed_codes contains a code absent from entries")
        actual_member_digest = digest_named_fields(
            RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
            {
                "mapping_authority": normalized_authority,
                "member_backed_codes": list(member_codes),
            },
        )
        expected_member_digest = ensure_sha256(str(root["member_backed_digest"]), field="member_backed_digest")
        if expected_member_digest != actual_member_digest:
            raise ValueError("member-backed digest differs")
    elif require_member_backed:
        raise ValueError("member-backed projection is required")
    id_to_code = {row["l2_code_id"]: row["canonical_l2_code"] for row in entries}
    return ReleaseSWL2CodeMap(
        id_to_code=id_to_code,
        code_to_id={code: sector_id for sector_id, code in id_to_code.items()},
        member_backed_codes=member_codes,
        mapping_authority=normalized_authority,
        code_map_digest=actual_map_digest,
        member_backed_digest=actual_member_digest,
    )


def load_release_sw_l2_code_map(path: Path, *, require_member_backed: bool = True) -> ReleaseSWL2CodeMap:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("release SW L2 code map is not valid UTF-8 JSON") from exc
    return validate_release_sw_l2_code_map(payload, require_member_backed=require_member_backed)


def validate_sector_quote_availability(
    value: Any,
    *,
    code_map: ReleaseSWL2CodeMap,
    required_end: dt.date | None = None,
) -> SectorQuoteAvailability:
    root = _exact_mapping(value, _QUOTE_ROOT_FIELDS, "sector quote availability")
    if root["schema_version"] != SECTOR_QUOTE_AVAILABILITY_SCHEMA:
        raise ValueError("sector quote availability schema differs")
    authority = _exact_mapping(root["mapping_authority"], _AUTHORITY_FIELDS, "mapping_authority")
    normalized_authority = {
        "authority_id": str(authority["authority_id"] or "").strip(),
        "authority_sha256": ensure_sha256(
            str(authority["authority_sha256"]),
            field="mapping_authority.authority_sha256",
        ),
    }
    if not normalized_authority["authority_id"]:
        raise ValueError("mapping_authority.authority_id is empty")
    if normalized_authority != dict(code_map.mapping_authority):
        raise ValueError("sector quote availability mapping authority differs")
    raw_entries = root["entries"]
    if not isinstance(raw_entries, list) or len(raw_entries) != len(code_map.code_to_id):
        raise ValueError("sector quote availability must cover the full code map")
    normalized_entries: list[dict[str, Any]] = []
    parsed: dict[str, tuple[tuple[dt.date, dt.date], ...]] = {}
    for raw_entry in raw_entries:
        entry = _exact_mapping(raw_entry, _QUOTE_ENTRY_FIELDS, "sector quote availability entry")
        code = str(entry["canonical_l2_code"] or "").strip().upper()
        if code not in code_map.code_to_id or code in parsed:
            raise ValueError(f"sector quote availability code differs: {code!r}")
        raw_spans = entry["availability_spans"]
        if not isinstance(raw_spans, list):
            raise ValueError("sector quote availability spans must be a list")
        normalized_spans: list[dict[str, str]] = []
        parsed_spans: list[tuple[dt.date, dt.date]] = []
        prior_end: dt.date | None = None
        for raw_span in raw_spans:
            span = _exact_mapping(raw_span, _QUOTE_SPAN_FIELDS, "sector quote availability span")
            start = _date(span["start_date"], "start_date")
            end = _date(span["end_date"], "end_date")
            if end < start or (prior_end is not None and start <= prior_end):
                raise ValueError(f"sector quote availability spans overlap or invert: {code}")
            prior_end = end
            parsed_spans.append((start, end))
            normalized_spans.append({"start_date": start.isoformat(), "end_date": end.isoformat()})
        parsed[code] = tuple(parsed_spans)
        normalized_entries.append(
            {"canonical_l2_code": code, "availability_spans": normalized_spans}
        )
    expected_entries = sorted(normalized_entries, key=lambda row: row["canonical_l2_code"])
    if normalized_entries != expected_entries or set(parsed) != set(code_map.code_to_id):
        raise ValueError("sector quote availability entries are not canonical or complete")
    expected_digest = digest_named_fields(
        SECTOR_QUOTE_AVAILABILITY_SCHEMA,
        {"mapping_authority": normalized_authority, "entries": normalized_entries},
    )
    actual_digest = ensure_sha256(
        str(root["quote_availability_digest"]),
        field="quote_availability_digest",
    )
    if actual_digest != expected_digest:
        raise ValueError("sector quote availability digest differs")
    if required_end is not None:
        late = sorted(
            code
            for code, spans in parsed.items()
            if spans and max(end for _, end in spans) > required_end
        )
        if late:
            raise ValueError(f"sector quote availability exceeds release cutoff: {late[:10]}")
    return SectorQuoteAvailability(
        entries=parsed,
        mapping_authority=normalized_authority,
        quote_availability_digest=expected_digest,
    )


def load_sector_quote_availability(
    path: Path,
    *,
    code_map: ReleaseSWL2CodeMap,
    required_end: dt.date | None = None,
) -> SectorQuoteAvailability:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("sector quote availability is not valid UTF-8 JSON") from exc
    return validate_sector_quote_availability(payload, code_map=code_map, required_end=required_end)


def validate_sector_context_pins(value: Any) -> dict[str, str]:
    root = _exact_mapping(value, _PIN_FIELDS, "sector_context_pins")
    pins = {key: str(root[key]) for key in sorted(_PIN_FIELDS)}
    if pins["schema_version"] != SECTOR_CONTEXT_PINS_SCHEMA:
        raise ValueError("sector context pins schema differs")
    expected_names = {
        "component_root": SECTOR_CONTEXT_COMPONENT_ROOT,
        "code_map_file": "sector_code_map.json",
        "market_context_file": "market_context.parquet",
        "membership_file": "sector_membership_spans.parquet",
        "quote_availability_file": "sector_quote_availability.json",
        "receipt_file": "component_receipt.json",
        "market_volume_definition": MARKET_VOLUME_DEFINITION,
    }
    for field, expected in expected_names.items():
        if pins[field] != expected:
            raise ValueError(f"sector context {field} differs")
    for field in (
        "code_map_sha256",
        "code_map_digest",
        "market_context_sha256",
        "membership_sha256",
        "quote_availability_sha256",
        "quote_availability_digest",
        "receipt_sha256",
        "sector_data_sha256",
        "source_dataset_manifest_sha256",
        "authority_sha256",
    ):
        pins[field] = ensure_sha256(pins[field], field=field)
    if not pins["authority_id"].strip():
        raise ValueError("sector context authority_id is empty")
    if pins["quote_availability_schema"] != SECTOR_QUOTE_AVAILABILITY_SCHEMA:
        raise ValueError("sector quote availability pin schema differs")
    market_start = _date(pins["market_start"], "market_start")
    market_end = _date(pins["market_end"], "market_end")
    membership_start = _date(pins["membership_start"], "membership_start")
    membership_end = _date(pins["membership_end"], "membership_end")
    if market_end < market_start or membership_end < membership_start:
        raise ValueError("sector context date range is invalid")
    return pins


def require_pinned_sector_context_files(candidate_root: Path, pins: Mapping[str, Any]) -> dict[str, Path]:
    normalized = validate_sector_context_pins(pins)
    component_root = candidate_root / Path(normalized["component_root"])
    if _is_link_or_junction(component_root) or not component_root.is_dir():
        raise ValueError("sector context component root is missing or linked")
    specs = {
        "code_map": (normalized["code_map_file"], normalized["code_map_sha256"]),
        "market_context": (
            normalized["market_context_file"],
            normalized["market_context_sha256"],
        ),
        "membership": (normalized["membership_file"], normalized["membership_sha256"]),
        "quote_availability": (
            normalized["quote_availability_file"],
            normalized["quote_availability_sha256"],
        ),
        "receipt": (normalized["receipt_file"], normalized["receipt_sha256"]),
    }
    paths: dict[str, Path] = {}
    for key, (filename, expected_sha256) in specs.items():
        path = component_root / filename
        if _is_link_or_junction(path) or not path.is_file():
            raise ValueError(f"pinned sector context file is missing: {path}")
        actual = _sha256_file(path)
        if actual != expected_sha256:
            raise ValueError(
                f"pinned sector context hash differs: {filename} expected={expected_sha256} actual={actual}"
            )
        paths[key] = path
    code_map = load_release_sw_l2_code_map(paths["code_map"])
    if code_map.code_map_digest != normalized["code_map_digest"]:
        raise ValueError("pinned sector code-map digest differs")
    if code_map.mapping_authority != {
        "authority_id": normalized["authority_id"],
        "authority_sha256": normalized["authority_sha256"],
    }:
        raise ValueError("pinned sector mapping authority differs")
    quote_availability = load_sector_quote_availability(
        paths["quote_availability"],
        code_map=code_map,
        required_end=_date(normalized["membership_end"], "membership_end"),
    )
    if quote_availability.quote_availability_digest != normalized["quote_availability_digest"]:
        raise ValueError("pinned sector quote availability digest differs")
    return paths


def validate_market_context_frame(
    frame: Any, *, required_start: dt.date | None = None, required_end: dt.date | None = None
) -> tuple[dt.date, dt.date, int]:
    import pandas as pd

    if list(frame.columns) != ["trade_date", "sw_daily_total_vol"] or frame.empty:
        raise ValueError("market context columns or rows are invalid")
    normalized = frame.copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], errors="coerce").dt.date
    normalized["sw_daily_total_vol"] = pd.to_numeric(normalized["sw_daily_total_vol"], errors="coerce")
    if normalized.isna().any().any():
        raise ValueError("market context contains null or invalid values")
    dates = normalized["trade_date"].tolist()
    if dates != sorted(set(dates)):
        raise ValueError("market context dates are duplicated or non-canonical")
    if any(not math.isfinite(float(value)) or float(value) <= 0 for value in normalized["sw_daily_total_vol"]):
        raise ValueError("market context contains a non-positive or non-finite volume")
    start, end = dates[0], dates[-1]
    if required_start is not None and start > required_start:
        raise ValueError("market context starts after the required window")
    if required_end is not None and end < required_end:
        raise ValueError("market context ends before the required window")
    return start, end, len(normalized)


def validate_membership_frame(
    frame: Any,
    *,
    id_to_code: Mapping[int, str],
    required_start: dt.date | None = None,
    required_end: dt.date | None = None,
) -> tuple[dt.date, dt.date, int, int]:
    import pandas as pd

    columns = ["instrument", "start_date", "end_date", "l2_code_id"]
    if list(frame.columns) != columns or frame.empty:
        raise ValueError("sector membership columns or rows are invalid")
    normalized = frame.copy()
    normalized["instrument"] = normalized["instrument"].astype(str).str.strip().str.upper()
    normalized["start_date"] = pd.to_datetime(normalized["start_date"], errors="coerce").dt.date
    normalized["end_date"] = pd.to_datetime(normalized["end_date"], errors="coerce").dt.date
    numeric_ids = pd.to_numeric(normalized["l2_code_id"], errors="coerce")
    if (
        normalized[["start_date", "end_date"]].isna().any().any()
        or numeric_ids.isna().any()
        or (numeric_ids % 1 != 0).any()
        or normalized["instrument"].map(lambda value: _SYMBOL_RE.fullmatch(value) is None).any()
    ):
        raise ValueError("sector membership contains an invalid value")
    normalized["l2_code_id"] = numeric_ids.astype(int)
    if (normalized["end_date"] < normalized["start_date"]).any():
        raise ValueError("sector membership contains an inverted interval")
    unknown = sorted(set(normalized["l2_code_id"]) - set(id_to_code))
    if unknown:
        raise ValueError(f"sector membership contains unknown l2_code_id values: {unknown[:10]}")
    ordered = normalized.sort_values(columns, kind="stable").reset_index(drop=True)
    if not normalized.reset_index(drop=True)[columns].equals(ordered[columns]):
        raise ValueError("sector membership rows are not in canonical order")
    for symbol, group in ordered.groupby("instrument", sort=False):
        prior_end: dt.date | None = None
        for row in group.itertuples(index=False):
            if prior_end is not None and row.start_date <= prior_end:
                raise ValueError(f"sector membership intervals overlap for {symbol}")
            prior_end = row.end_date
    start = min(normalized["start_date"])
    end = max(normalized["end_date"])
    if required_start is not None and start > required_start:
        raise ValueError("sector membership starts after the required window")
    if required_end is not None and end < required_end:
        raise ValueError("sector membership ends before the required window")
    return start, end, len(normalized), normalized["instrument"].nunique()
