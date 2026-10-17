"""Build core-index PIT authority rows from official constituent workbooks.

The manifest is repo-external and names one current official constituent
workbook plus the official adjustment events for each pool.  The builder
reconstructs history backwards, then emits the JSON array consumed by
``prepare_core_index_membership_pit.py``.  Tushare snapshots are deliberately
outside this builder: they remain a later cross-check and never define an
effective boundary.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.core_index_membership import POOL_DEFINITIONS  # noqa: E402


SCHEMA_VERSION = "core_index_membership_authority_manifest_v1"


class AuthorityBuildError(RuntimeError):
    """Raised when official source inputs cannot prove a deterministic history."""


@dataclass(frozen=True, slots=True)
class AdjustmentEvent:
    effective_from: date
    source_reference: str
    additions: frozenset[str]
    removals: frozenset[str]


def _parse_date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise AuthorityBuildError(f"{field} must be an ISO date") from exc


def _canonical_symbol(value: Any) -> str:
    text = str(value).strip().upper()
    if text.endswith(".0"):
        text = text[:-2]
    supplied_suffix: str | None = None
    if len(text) == 9 and text[6] == ".":
        text, supplied_suffix = text[:6], text[6:]
    if not text.isdigit() or len(text) > 6:
        raise AuthorityBuildError(f"invalid A-share constituent code: {value!r}")
    code = text.zfill(6)
    suffix = ".SH" if code.startswith(("5", "6", "9")) else ".SZ"
    if supplied_suffix is not None and supplied_suffix != suffix:
        raise AuthorityBuildError(f"constituent exchange suffix is inconsistent: {value!r}")
    return f"{code}{suffix}"


def _resolve_source_path(manifest_path: Path, value: Any) -> Path:
    raw = Path(str(value))
    resolved = raw if raw.is_absolute() else manifest_path.parent / raw
    try:
        return resolved.resolve(strict=True)
    except OSError as exc:
        raise AuthorityBuildError(f"official source file is unavailable: {resolved}") from exc


def _read_current_members(
    path: Path,
    *,
    expected_index_code: str,
    expected_as_of: date,
    code_column: int,
    index_column: int,
    as_of_column: int,
) -> frozenset[str]:
    frame = pd.read_excel(path, sheet_name=0, header=0, dtype=object)
    required = max(code_column, index_column, as_of_column)
    if min(code_column, index_column, as_of_column) < 0 or required >= len(frame.columns):
        raise AuthorityBuildError(f"current snapshot columns are outside {path.name}")
    index_values = {
        str(value).strip().removesuffix(".0").zfill(6)
        for value in frame.iloc[:, index_column].dropna().tolist()
    }
    if index_values != {expected_index_code}:
        raise AuthorityBuildError(
            f"current snapshot index differs in {path.name}: {sorted(index_values)}"
        )
    as_of_values = {
        str(value).strip().removesuffix(".0").replace("-", "")
        for value in frame.iloc[:, as_of_column].dropna().tolist()
    }
    if as_of_values != {expected_as_of.strftime('%Y%m%d')}:
        raise AuthorityBuildError(
            f"current snapshot date differs in {path.name}: {sorted(as_of_values)}"
        )
    values = frame.iloc[:, code_column].dropna().tolist()
    members = frozenset(_canonical_symbol(value) for value in values)
    if not members:
        raise AuthorityBuildError(f"current constituent workbook is empty: {path.name}")
    return members


def _read_split_sheet_codes(
    path: Path,
    *,
    sheet: int | str,
    index_code: str,
    index_column: int,
    symbol_column: int,
) -> frozenset[str]:
    frame = pd.read_excel(path, sheet_name=sheet, header=0, dtype=object)
    required = max(index_column, symbol_column)
    if min(index_column, symbol_column) < 0 or required >= len(frame.columns):
        raise AuthorityBuildError(f"adjustment columns are outside {path.name}/{sheet}")
    index_values = frame.iloc[:, index_column].map(
        lambda value: str(value).strip().removesuffix(".0").zfill(6)
    )
    values = frame.loc[index_values == index_code, frame.columns[symbol_column]].dropna().tolist()
    return frozenset(_canonical_symbol(value) for value in values)


def _read_event(
    manifest_path: Path,
    pool_id: str,
    value: Mapping[str, Any],
) -> AdjustmentEvent:
    definition = POOL_DEFINITIONS[pool_id]
    index_code = definition.index_code[:6]
    reference = str(value.get("source_reference") or "").strip()
    if not reference:
        raise AuthorityBuildError(f"source_reference is empty for {pool_id}")
    if "additions" in value or "removals" in value:
        additions = frozenset(_canonical_symbol(item) for item in value.get("additions", ()))
        removals = frozenset(_canonical_symbol(item) for item in value.get("removals", ()))
    else:
        source = _resolve_source_path(manifest_path, value.get("workbook"))
        layout = str(value.get("layout") or "split_sheets")
        if layout == "split_sheets":
            common = {
                "index_code": index_code,
                "index_column": int(value.get("index_column", 0)),
                "symbol_column": int(value.get("symbol_column", 2)),
            }
            additions = _read_split_sheet_codes(
                source, sheet=value.get("additions_sheet", 0), **common
            )
            removals = _read_split_sheet_codes(
                source, sheet=value.get("removals_sheet", 1), **common
            )
        elif layout == "paired_columns":
            frame = pd.read_excel(source, sheet_name=value.get("sheet", 0), header=None, dtype=object)
            index_column = int(value.get("index_column", 0))
            additions_column = int(value.get("additions_column", 4))
            removals_column = int(value.get("removals_column", 2))
            required = max(index_column, additions_column, removals_column)
            if min(index_column, additions_column, removals_column) < 0 or required >= len(frame.columns):
                raise AuthorityBuildError(f"paired adjustment columns are outside {source.name}")
            index_values = frame.iloc[:, index_column].map(
                lambda item: str(item).strip().removesuffix(".0").zfill(6)
            )
            selected = frame.loc[index_values == index_code]
            additions = frozenset(
                _canonical_symbol(item) for item in selected.iloc[:, additions_column].dropna().tolist()
            )
            removals = frozenset(
                _canonical_symbol(item) for item in selected.iloc[:, removals_column].dropna().tolist()
            )
        else:
            raise AuthorityBuildError(f"unsupported event layout: {layout}")
    if not additions and not removals:
        raise AuthorityBuildError(f"official event has no {pool_id} rows: {reference}")
    overlap = additions & removals
    if overlap:
        raise AuthorityBuildError(f"event adds and removes the same symbols: {sorted(overlap)}")
    return AdjustmentEvent(
        effective_from=_parse_date(value.get("effective_from"), "effective_from"),
        source_reference=reference,
        additions=additions,
        removals=removals,
    )


def _build_pool_rows(
    *,
    pool_id: str,
    current_members: frozenset[str],
    events: Sequence[AdjustmentEvent],
    window_start: date,
    cutoff: date,
    baseline_reference: str,
) -> list[dict[str, Any]]:
    by_date: dict[date, AdjustmentEvent] = {}
    for event in events:
        if not window_start < event.effective_from <= cutoff:
            raise AuthorityBuildError(
                f"event date is outside the requested history window for "
                f"{pool_id}/{event.effective_from}"
            )
        if event.effective_from in by_date:
            raise AuthorityBuildError(f"multiple unmerged events for {pool_id}/{event.effective_from}")
        by_date[event.effective_from] = event

    state = set(current_members)
    for event in sorted(events, key=lambda item: item.effective_from, reverse=True):
        missing_additions = event.additions - state
        unexpected_removals = event.removals & state
        if missing_additions or unexpected_removals:
            raise AuthorityBuildError(
                f"reverse continuity failed for {pool_id}/{event.effective_from}: "
                f"missing_additions={sorted(missing_additions)} "
                f"unexpected_removals={sorted(unexpected_removals)}"
            )
        state.difference_update(event.additions)
        state.update(event.removals)

    open_intervals = {symbol: (window_start, baseline_reference) for symbol in state}
    rows: list[dict[str, Any]] = []
    definition = POOL_DEFINITIONS[pool_id]
    for event in sorted(events, key=lambda item: item.effective_from):
        for symbol in sorted(event.removals):
            start_reference = open_intervals.pop(symbol, None)
            if start_reference is None:
                raise AuthorityBuildError(
                    f"forward removal is not active for {pool_id}/{symbol}/{event.effective_from}"
                )
            start, reference = start_reference
            if event.effective_from > window_start:
                rows.append(
                    {
                        "pool_id": pool_id,
                        "index_code": definition.index_code,
                        "ts_code": symbol,
                        "effective_from": start.isoformat(),
                        "effective_to_exclusive": event.effective_from.isoformat(),
                        "source_provider": definition.source_provider,
                        "source_reference": reference,
                    }
                )
        for symbol in sorted(event.additions):
            if symbol in open_intervals:
                raise AuthorityBuildError(
                    f"forward addition is already active for {pool_id}/{symbol}/{event.effective_from}"
                )
            open_intervals[symbol] = (event.effective_from, event.source_reference)

    if set(open_intervals) != set(current_members):
        raise AuthorityBuildError(f"forward reconstruction differs from current snapshot for {pool_id}")
    for symbol, (start, reference) in sorted(open_intervals.items()):
        if start <= cutoff:
            rows.append(
                {
                    "pool_id": pool_id,
                    "index_code": definition.index_code,
                    "ts_code": symbol,
                    "effective_from": max(start, window_start).isoformat(),
                    "effective_to_exclusive": None,
                    "source_provider": definition.source_provider,
                    "source_reference": reference,
                }
            )
    return rows


def build_authority(manifest_path: Path) -> list[dict[str, Any]]:
    resolved = manifest_path.resolve(strict=True)
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuthorityBuildError("manifest is unreadable or invalid JSON") from exc
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise AuthorityBuildError("unsupported manifest schema_version")
    window_start = _parse_date(payload.get("window_start"), "window_start")
    cutoff = _parse_date(payload.get("cutoff"), "cutoff")
    if cutoff < window_start:
        raise AuthorityBuildError("cutoff precedes window_start")
    pools = payload.get("pools")
    if not isinstance(pools, list) or not pools:
        raise AuthorityBuildError("manifest pools must be a non-empty array")

    rows: list[dict[str, Any]] = []
    seen_pools: set[str] = set()
    for value in pools:
        if not isinstance(value, Mapping):
            raise AuthorityBuildError("every pool entry must be an object")
        pool_id = str(value.get("pool_id") or "").strip().lower()
        if pool_id not in POOL_DEFINITIONS or pool_id in seen_pools:
            raise AuthorityBuildError(f"unknown or duplicate pool_id: {pool_id!r}")
        seen_pools.add(pool_id)
        snapshot = _resolve_source_path(resolved, value.get("current_workbook"))
        current_as_of = _parse_date(value.get("current_as_of"), "current_as_of")
        if current_as_of < cutoff:
            raise AuthorityBuildError(f"current snapshot predates cutoff for {pool_id}")
        current = _read_current_members(
            snapshot,
            expected_index_code=POOL_DEFINITIONS[pool_id].index_code[:6],
            expected_as_of=current_as_of,
            code_column=int(value.get("current_code_column", 4)),
            index_column=int(value.get("current_index_column", 1)),
            as_of_column=int(value.get("current_as_of_column", 0)),
        )
        expected_count = value.get("current_expected_count")
        if expected_count is not None and len(current) != int(expected_count):
            raise AuthorityBuildError(
                f"current member count differs for {pool_id}: {len(current)} != {expected_count}"
            )
        events = tuple(_read_event(resolved, pool_id, item) for item in value.get("events", ()))
        baseline_reference = str(value.get("baseline_source_reference") or "").strip()
        if not baseline_reference:
            raise AuthorityBuildError(f"baseline_source_reference is empty for {pool_id}")
        pool_window_start = max(window_start, POOL_DEFINITIONS[pool_id].history_start)
        rows.extend(
            _build_pool_rows(
                pool_id=pool_id,
                current_members=current,
                events=events,
                window_start=pool_window_start,
                cutoff=cutoff,
                baseline_reference=baseline_reference,
            )
        )
    return sorted(
        rows,
        key=lambda item: (item["pool_id"], item["ts_code"], item["effective_from"]),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    output = Path(args.output).resolve()
    try:
        output.relative_to(REPO_ROOT.resolve())
    except ValueError:
        pass
    else:
        raise AuthorityBuildError("authority output must be repo-external")
    rows = build_authority(Path(args.manifest))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "PASS", "row_count": len(rows), "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
