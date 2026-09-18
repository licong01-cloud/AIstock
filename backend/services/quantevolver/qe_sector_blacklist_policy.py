"""Materialize a QE sector blacklist into a run-scoped PIT universe.

Only immutable, hash-pinned release files are consumed here.  The control
plane may persist the resulting diagnostics, but experiment data-plane
construction never queries PostgreSQL or any other online source.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.shared_sector_context import (
    RELEASE_SW_L2_CODE_MAP_SCHEMA,
    load_release_sw_l2_code_map,
)


SECTOR_BLACKLIST_POLICY_PARAM = "_qe_sector_blacklist_policy"
SECTOR_POLICY_PINS_SCHEMA = "qe_sector_policy_input_v1"
SECTOR_CODE_MAP_SCHEMA = "qe_sw_l2_code_map_v1"
SECTOR_CODE_MAP_DIGEST_SCHEMA = "dataset_release_sw_l2_code_map_v1"

_SECTOR_CODE_RE = re.compile(r"^801[0-9]{3}[.]SI$")
_SYMBOL_RE = re.compile(r"^[0-9]{6}[.](?:SH|SZ|BJ)$")
_PIN_FIELDS = {
    "schema_version",
    "membership_file",
    "membership_sha256",
    "code_map_file",
    "code_map_sha256",
    "start",
    "end",
    "universe_key",
}


class QESectorBlacklistPolicyError(RuntimeError):
    def __init__(self, reason_code: str, message: str, **context: Any) -> None:
        super().__init__(f"reason_code={reason_code}: {message}")
        self.reason_code = reason_code
        self.context = context


@dataclass(frozen=True, slots=True)
class SectorBlacklistPolicyResult:
    instruments_content: str
    diagnostics: Mapping[str, Any]


def _fail(reason_code: str, message: str, **context: Any) -> QESectorBlacklistPolicyError:
    return QESectorBlacklistPolicyError(reason_code, message, **context)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def validate_sector_policy_pins(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != _PIN_FIELDS:
        raise _fail("qe_sector_blacklist_frozen_mapping_missing", "sector_policy_pins fields are invalid")
    pins = {key: str(value[key]) for key in sorted(_PIN_FIELDS)}
    if pins["schema_version"] != SECTOR_POLICY_PINS_SCHEMA:
        raise _fail("qe_sector_blacklist_frozen_mapping_missing", "sector policy schema differs")
    if pins["membership_file"] != "sector_membership_spans.parquet":
        raise _fail("qe_sector_blacklist_frozen_mapping_missing", "membership filename is invalid")
    if pins["code_map_file"] != "sector_code_map.json":
        raise _fail("qe_sector_blacklist_frozen_mapping_missing", "code-map filename is invalid")
    for field in ("membership_sha256", "code_map_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", pins[field]):
            raise _fail("qe_sector_blacklist_frozen_mapping_missing", f"{field} is not canonical SHA256")
    try:
        start = dt.date.fromisoformat(pins["start"])
        end = dt.date.fromisoformat(pins["end"])
    except ValueError as exc:
        raise _fail("qe_sector_blacklist_frozen_mapping_missing", "sector policy dates are invalid") from exc
    if end < start or not pins["universe_key"].strip():
        raise _fail("qe_sector_blacklist_frozen_mapping_missing", "sector policy identity is invalid")
    return pins


def require_pinned_sector_policy_files(root: Path, pins: Mapping[str, Any]) -> tuple[Path, Path]:
    normalized = validate_sector_policy_pins(pins)
    paths = (root / normalized["membership_file"], root / normalized["code_map_file"])
    for path, hash_field in zip(paths, ("membership_sha256", "code_map_sha256"), strict=True):
        if _is_link_or_junction(path) or not path.is_file():
            raise _fail("qe_sector_blacklist_frozen_mapping_missing", f"required frozen file is absent: {path}")
        try:
            actual = _sha256_file(path)
        except OSError as exc:
            raise _fail("qe_sector_blacklist_frozen_mapping_missing", f"cannot read frozen file: {path}") from exc
        if actual != normalized[hash_field]:
            raise _fail(
                "qe_sector_blacklist_frozen_mapping_hash_mismatch",
                f"frozen file hash differs: {path}",
                expected=normalized[hash_field],
                actual=actual,
            )
    return paths


def requested_sector_codes(custom_params: Mapping[str, Any] | None) -> tuple[str, ...]:
    params = dict(custom_params or {})
    raw = params.get("sector_blacklist")
    if raw in (None, []):
        if params.get("sector_blacklist_enabled") is True:
            raise _fail(
                "qe_sector_blacklist_request_invalid",
                "enabled sector blacklist requires at least one canonical sector code",
            )
        return ()
    if not isinstance(raw, list):
        raise _fail("qe_sector_blacklist_request_invalid", "sector_blacklist must be a list")
    codes = tuple(sorted({str(item).strip().upper() for item in raw}))
    if not codes or any(_SECTOR_CODE_RE.fullmatch(code) is None for code in codes):
        raise _fail("qe_sector_blacklist_request_invalid", "sector_blacklist contains a non-canonical SW L2 code")
    if params.get("sector_blacklist_enabled") is False:
        raise _fail("qe_sector_blacklist_request_invalid", "non-empty sector_blacklist conflicts with disabled flag")
    snapshot = params.get("sector_blacklist_snapshot")
    if isinstance(snapshot, Mapping) and isinstance(snapshot.get("items"), list):
        snapshot_codes = {
            str(item.get("sw2_code") or "").strip().upper()
            for item in snapshot["items"]
            if isinstance(item, Mapping) and item.get("sw2_code")
        }
        if snapshot_codes and snapshot_codes != set(codes):
            raise _fail("qe_sector_blacklist_request_invalid", "blacklist snapshot differs from requested sectors")
    return codes


def _load_code_map(path: Path) -> tuple[dict[int, str], dict[str, int], str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail("qe_sector_blacklist_code_map_invalid", "frozen sector code map is invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise _fail("qe_sector_blacklist_code_map_invalid", "frozen sector code-map schema differs")
    if payload.get("schema_version") == RELEASE_SW_L2_CODE_MAP_SCHEMA:
        try:
            code_map = load_release_sw_l2_code_map(path)
        except ValueError as exc:
            raise _fail("qe_sector_blacklist_code_map_invalid", str(exc)) from exc
        return dict(code_map.id_to_code), dict(code_map.code_to_id), code_map.code_map_digest
    if payload.get("schema_version") != SECTOR_CODE_MAP_SCHEMA:
        raise _fail("qe_sector_blacklist_code_map_invalid", "frozen sector code-map schema differs")
    ordered = payload.get("ordered_codes")
    if not isinstance(ordered, list) or not ordered:
        raise _fail("qe_sector_blacklist_code_map_invalid", "ordered_codes must be a non-empty list")
    codes = [str(value).strip().upper() for value in ordered]
    if codes != sorted(set(codes)) or any(_SECTOR_CODE_RE.fullmatch(code) is None for code in codes):
        raise _fail("qe_sector_blacklist_code_map_invalid", "ordered_codes are not canonical and unique")
    actual_digest = digest_named_fields(
        SECTOR_CODE_MAP_DIGEST_SCHEMA,
        {"ordered_codes": codes},
    )
    if str(payload.get("code_map_digest") or "").strip().lower() != actual_digest:
        raise _fail("qe_sector_blacklist_code_map_invalid", "sector code-map digest differs")
    return (
        {index: code for index, code in enumerate(codes)},
        {code: index for index, code in enumerate(codes)},
        actual_digest,
    )


def _load_membership_spans(
    path: Path, *, calendar_index: Mapping[dt.date, int]
) -> dict[str, list[tuple[int, int, int]]]:
    try:
        import pandas as pd

        frame = pd.read_parquet(
            path,
            columns=["instrument", "start_date", "end_date", "l2_code_id"],
        )
    except Exception as exc:
        raise _fail("qe_sector_blacklist_membership_invalid", "cannot read frozen membership spans") from exc
    if frame.empty or set(frame.columns) != {"instrument", "start_date", "end_date", "l2_code_id"}:
        raise _fail("qe_sector_blacklist_membership_invalid", "membership spans schema or rows are invalid")
    frame = frame.copy()
    frame["instrument"] = frame["instrument"].astype(str).str.strip().str.upper()
    frame["start_date"] = pd.to_datetime(frame["start_date"], errors="coerce").dt.date
    frame["end_date"] = pd.to_datetime(frame["end_date"], errors="coerce").dt.date
    numeric_ids = pd.to_numeric(frame["l2_code_id"], errors="coerce")
    if (
        frame["instrument"].map(lambda item: _SYMBOL_RE.fullmatch(item) is None).any()
        or frame[["start_date", "end_date"]].isna().any().any()
        or numeric_ids.isna().any()
        or (numeric_ids % 1 != 0).any()
    ):
        raise _fail("qe_sector_blacklist_membership_invalid", "membership span values are invalid")
    frame["l2_code_id"] = numeric_ids.astype(int)
    order_fields = ["instrument", "start_date", "end_date", "l2_code_id"]
    ordered = frame.sort_values(order_fields, kind="stable").reset_index(drop=True)
    if not frame.reset_index(drop=True)[order_fields].equals(ordered[order_fields]):
        raise _fail(
            "qe_sector_blacklist_membership_invalid",
            "membership spans are not in canonical instrument/date order",
        )
    result: dict[str, list[tuple[int, int, int]]] = {}
    for row in frame.itertuples(index=False):
        start = calendar_index.get(row.start_date)
        end = calendar_index.get(row.end_date)
        if start is None or end is None or end < start:
            raise _fail("qe_sector_blacklist_membership_invalid", "membership span is outside the frozen calendar")
        spans = result.setdefault(row.instrument, [])
        if spans and start <= spans[-1][1]:
            raise _fail("qe_sector_blacklist_membership_invalid", f"membership spans overlap for {row.instrument}")
        spans.append((start, end, int(row.l2_code_id)))
    return result


def materialize_sector_blacklist_universe(
    *,
    base_intervals: Sequence[tuple[str, dt.date, dt.date]],
    calendar: Sequence[dt.date],
    window_start: dt.date,
    window_end: dt.date,
    factor_root: Path,
    pins: Mapping[str, Any] | None,
    blacklist_codes: Sequence[str],
) -> SectorBlacklistPolicyResult:
    requested = tuple(sorted(set(blacklist_codes)))
    if not requested:
        raise _fail("qe_sector_blacklist_request_invalid", "materialization requires at least one sector")
    if pins is None:
        raise _fail(
            "qe_sector_blacklist_frozen_mapping_missing",
            "active dataset profile does not pin sector membership and code-map files",
        )
    normalized_pins = validate_sector_policy_pins(pins)
    if normalized_pins["start"] > window_start.isoformat() or normalized_pins["end"] < window_end.isoformat():
        raise _fail(
            "qe_sector_blacklist_membership_incomplete", "sector policy files do not cover the experiment window"
        )
    membership_path, code_map_path = require_pinned_sector_policy_files(factor_root, normalized_pins)
    id_to_code, code_to_id, code_map_digest = _load_code_map(code_map_path)
    unknown_codes = sorted(set(requested) - set(code_to_id))
    if unknown_codes:
        raise _fail(
            "qe_sector_blacklist_request_invalid", f"requested sectors are absent from frozen code map: {unknown_codes}"
        )
    blacklist_ids = {code_to_id[code] for code in requested}

    ordered_calendar = list(calendar)
    if not ordered_calendar or ordered_calendar != sorted(set(ordered_calendar)):
        raise _fail("qe_sector_blacklist_membership_invalid", "frozen calendar is empty or non-canonical")
    calendar_index = {day: index for index, day in enumerate(ordered_calendar)}
    memberships = _load_membership_spans(membership_path, calendar_index=calendar_index)
    left_bound = bisect_left(ordered_calendar, window_start)
    right_bound = bisect_right(ordered_calendar, window_end) - 1
    if left_bound > right_bound:
        raise _fail("qe_sector_blacklist_membership_incomplete", "experiment window has no trading dates")

    retained: list[tuple[str, int, int]] = []
    base_symbols: set[str] = set()
    retained_symbols: set[str] = set()
    excluded_symbols: set[str] = set()
    excluded_days = 0
    for symbol, base_start, base_end in base_intervals:
        start_index = max(left_bound, bisect_left(ordered_calendar, base_start))
        end_index = min(right_bound, bisect_right(ordered_calendar, base_end) - 1)
        if start_index > end_index:
            continue
        base_symbols.add(symbol)
        spans = memberships.get(symbol)
        if not spans:
            raise _fail("qe_sector_blacklist_membership_incomplete", f"no PIT sector membership for {symbol}")
        cursor = start_index
        symbol_retained: list[tuple[int, int]] = []
        for sector_start, sector_end, sector_id in spans:
            if sector_end < start_index:
                continue
            if sector_start > end_index:
                break
            current_start = max(start_index, sector_start)
            current_end = min(end_index, sector_end)
            if current_start > cursor:
                raise _fail(
                    "qe_sector_blacklist_membership_incomplete",
                    f"PIT sector membership has a trading-day gap for {symbol}",
                    missing_date=ordered_calendar[cursor].isoformat(),
                )
            if current_start < cursor:
                current_start = cursor
            if sector_id not in id_to_code:
                raise _fail(
                    "qe_sector_blacklist_membership_unknown",
                    f"PIT sector membership is unknown for {symbol}",
                    trade_date=ordered_calendar[current_start].isoformat(),
                    l2_code_id=sector_id,
                )
            if sector_id in blacklist_ids:
                excluded_symbols.add(symbol)
                excluded_days += current_end - current_start + 1
            elif symbol_retained and current_start == symbol_retained[-1][1] + 1:
                symbol_retained[-1] = (symbol_retained[-1][0], current_end)
            else:
                symbol_retained.append((current_start, current_end))
            cursor = current_end + 1
            if cursor > end_index:
                break
        if cursor <= end_index:
            raise _fail(
                "qe_sector_blacklist_membership_incomplete",
                f"PIT sector membership ends before the experiment window for {symbol}",
                missing_date=ordered_calendar[cursor].isoformat(),
            )
        if symbol_retained:
            retained_symbols.add(symbol)
            retained.extend((symbol, start, end) for start, end in symbol_retained)
    if not base_symbols:
        raise _fail("qe_sector_blacklist_membership_incomplete", "base universe has no rows in the experiment window")
    if not retained:
        raise _fail("qe_sector_blacklist_universe_empty", "sector blacklist removed the entire executable universe")

    content = "".join(
        f"{symbol}\t{ordered_calendar[start].isoformat()}\t{ordered_calendar[end].isoformat()}\n"
        for symbol, start, end in sorted(retained)
    )
    diagnostics = {
        "schema_version": "qe_sector_blacklist_policy_v1",
        "requested": True,
        "enabled": True,
        "effective": bool(excluded_symbols),
        "requested_sector_codes": list(requested),
        "requested_sector_count": len(requested),
        "base_instrument_count": len(base_symbols),
        "retained_instrument_count": len(retained_symbols),
        "blacklist_excluded_count": len(excluded_symbols),
        "blacklist_excluded_membership_days": excluded_days,
        "membership_sha256": normalized_pins["membership_sha256"],
        "code_map_sha256": normalized_pins["code_map_sha256"],
        "code_map_digest": code_map_digest,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }
    return SectorBlacklistPolicyResult(instruments_content=content, diagnostics=diagnostics)
