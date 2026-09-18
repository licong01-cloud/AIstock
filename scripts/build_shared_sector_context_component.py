"""Build a consumer-neutral sector-context component from frozen release files."""

from __future__ import annotations

import argparse
from bisect import bisect_left
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.dataset_release.canonical import (  # noqa: E402
    canonical_json_bytes,
    digest_named_fields,
    ensure_sha256,
)
from backend.services.dataset_release.shared_sector_context import (  # noqa: E402
    MARKET_CONTEXT_SCHEMA,
    MARKET_VOLUME_DEFINITION,
    RELEASE_SW_L2_CODE_MAP_SCHEMA,
    SECTOR_MEMBERSHIP_SPANS_SCHEMA,
    build_release_sw_l2_code_map_payload,
    load_release_sw_l2_code_map,
    validate_market_context_frame,
    validate_membership_frame,
    validate_release_sw_l2_code_map,
)
from backend.services.hmm_risk.industry_pit_adapter import (  # noqa: E402
    HMMIndustryPitAdapter,
)


COMPONENT_RECEIPT_SCHEMA = "aistock_sector_context_receipt_v1"
DERIVED_MAPPING_AUTHORITY_SCHEMA = "aistock_release_sw_l2_derived_authority_v1"
PRODUCER_ORDER_ALGORITHM = "sorted_market_sw_index_classify_index_code_zero_based_v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_file_bytes(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def _write_exclusive(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()


def _read_sector_frame(path: Path):
    import numpy as np
    import pandas as pd

    frame = pd.read_hdf(path, "data", columns=["l2_code_id", "sw2_vol"]).reset_index()
    required = {"datetime", "instrument", "l2_code_id", "sw2_vol"}
    if set(frame.columns) != required or frame.empty:
        raise ValueError("sector_data.h5 schema or rows are invalid")
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="raise").dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str).str.strip().str.upper()
    frame["l2_code_id"] = pd.to_numeric(frame["l2_code_id"], errors="raise")
    frame["sw2_vol"] = pd.to_numeric(frame["sw2_vol"], errors="raise")
    valid_ids = frame.loc[frame["l2_code_id"] >= 0, "l2_code_id"]
    if (~np.isfinite(valid_ids)).any() or (valid_ids % 1 != 0).any() or valid_ids.gt(32767).any():
        raise ValueError("sector_data contains an invalid l2_code_id")
    frame["l2_code_id"] = frame["l2_code_id"].astype("int32")
    return frame.loc[frame["l2_code_id"] >= 0].copy()


def _build_market_context(frame):
    import numpy as np
    import pandas as pd

    conflicts = frame.groupby(["datetime", "l2_code_id"], sort=True)["sw2_vol"].nunique(dropna=False)
    if bool(conflicts.gt(1).any()):
        first = conflicts.loc[conflicts.gt(1)].index[0]
        raise ValueError(f"sector volume conflicts for datetime/id={first}")
    unique_rows = frame.drop_duplicates(["datetime", "l2_code_id"], keep="first")
    volume = pd.to_numeric(unique_rows["sw2_vol"], errors="coerce")
    if np.isinf(volume).any() or volume.lt(0).any():
        raise ValueError("sector_data contains a negative or infinite sw2_vol")
    unique_rows = unique_rows.assign(sw2_vol=volume.fillna(0.0))
    market = (
        unique_rows.groupby("datetime", sort=True, as_index=False)["sw2_vol"]
        .sum()
        .rename(columns={"datetime": "trade_date", "sw2_vol": "sw_daily_total_vol"})
    )
    market["trade_date"] = market["trade_date"].dt.date
    return market[["trade_date", "sw_daily_total_vol"]]


def _parse_calendar(path: Path) -> list[dt.date]:
    dates = [
        dt.date.fromisoformat(line.strip()[:10])
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not dates or dates != sorted(set(dates)):
        raise ValueError("release calendar is empty, duplicated, or non-canonical")
    return dates


def _parse_universe_spans(path: Path) -> list[tuple[str, dt.date, dt.date]]:
    spans: list[tuple[str, dt.date, dt.date]] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        parts = raw.split("\t")
        if len(parts) != 3:
            raise ValueError(f"stock universe row {line_number} is invalid")
        symbol = parts[0].strip().upper()
        start = dt.date.fromisoformat(parts[1])
        end = dt.date.fromisoformat(parts[2])
        if end < start:
            raise ValueError(f"stock universe row {line_number} has an inverted interval")
        spans.append((symbol, start, end))
    if not spans or spans != sorted(set(spans)):
        raise ValueError("stock universe spans are empty, duplicated, or non-canonical")
    return spans


def _load_industry_adapter(path: Path) -> tuple[HMMIndustryPitAdapter, dict[str, Any]]:
    envelope = json.loads(path.read_text(encoding="utf-8"))
    expected_fields = {
        "artifact_root",
        "identity",
        "research_basis",
        "l1_projection",
        "l2_projection",
    }
    if not isinstance(envelope, dict) or set(envelope) != expected_fields:
        raise ValueError("industry PIT authority envelope fields differ")
    adapter = HMMIndustryPitAdapter.from_artifact_root(
        artifact_root=Path(str(envelope["artifact_root"])),
        forbidden_roots=(PROJECT_ROOT,),
        expected_identity=envelope["identity"],
    )
    adapter.bind_research_basis_contract(envelope["research_basis"])
    adapter.bind_l1_code_projection(envelope["l1_projection"])
    adapter.bind_l2_code_projection(envelope["l2_projection"])
    return adapter, envelope


def _derive_release_code_map(
    *,
    frame,
    adapter: Any,
    envelope: dict[str, Any],
    start: dt.date,
    end: dt.date,
    sector_data_sha256: str,
    authority_envelope_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    selected = frame.loc[(frame["datetime"].dt.date >= start) & (frame["datetime"].dt.date <= end)]
    if selected.empty:
        raise ValueError("sector_data has no rows in the code-map derivation window")
    projection = envelope.get("l2_projection")
    raw_rows = projection.get("rows") if isinstance(projection, dict) else None
    if not isinstance(raw_rows, list) or len(raw_rows) != 131:
        raise ValueError("industry PIT authority lacks the formal 131-code L2 projection")
    formal_codes = sorted(str(row["canonical_l2_code"]) for row in raw_rows)
    if len(set(formal_codes)) != 131:
        raise ValueError("industry PIT authority L2 projection is not one-to-one")

    observed_by_id: dict[int, str] = {}
    observed_by_code: dict[str, int] = {}
    representatives = selected.sort_values(["l2_code_id", "datetime", "instrument"], kind="stable").drop_duplicates(
        "l2_code_id", keep="last"
    )
    for row in representatives.itertuples(index=False):
        resolved = adapter.resolve(row.instrument, row.datetime.date())
        if resolved.status != "resolved" or not resolved.l2_code:
            raise ValueError(
                "cannot derive release code map from an unresolved H5 row: "
                f"instrument={row.instrument} trade_date={row.datetime.date()}"
            )
        sector_id = int(row.l2_code_id)
        code = str(resolved.l2_code)
        if code not in formal_codes:
            raise ValueError(f"derived H5 code is absent from formal projection: {code}")
        if sector_id in observed_by_id or code in observed_by_code:
            raise ValueError("derived H5 mapping is not one-to-one")
        observed_by_id[sector_id] = code
        observed_by_code[code] = sector_id

    rank_by_code = {code: rank for rank, code in enumerate(formal_codes)}
    known = sorted((rank_by_code[code], sector_id, code) for code, sector_id in observed_by_code.items())
    if any(right[1] <= left[1] for left, right in zip(known, known[1:])):
        raise ValueError("observed release IDs contradict the producer ordering algorithm")
    derived = dict(observed_by_code)
    prior_rank = -1
    prior_id = -1
    derivation_segments: list[dict[str, Any]] = []
    non_member_catalog_ids: list[int] = []
    for rank, sector_id, code in known:
        missing_codes = formal_codes[prior_rank + 1 : rank]
        available_ids = list(range(prior_id + 1, sector_id))
        if missing_codes and len(missing_codes) != len(available_ids):
            raise ValueError(
                "release code-map derivation is ambiguous between observed IDs: "
                f"left_rank={prior_rank} left_id={prior_id} right_code={code} "
                f"right_id={sector_id} missing_codes={len(missing_codes)} "
                f"available_ids={len(available_ids)}"
            )
        for missing_code, missing_id in zip(missing_codes, available_ids):
            derived[missing_code] = missing_id
        if not missing_codes:
            non_member_catalog_ids.extend(available_ids)
        if missing_codes:
            derivation_segments.append(
                {
                    "left_rank": prior_rank,
                    "left_id": prior_id,
                    "right_rank": rank,
                    "right_id": sector_id,
                    "codes": missing_codes,
                    "ids": available_ids,
                }
            )
        prior_rank, prior_id = rank, sector_id
    trailing = formal_codes[prior_rank + 1 :]
    if trailing:
        raise ValueError(f"release code-map derivation has unbounded trailing formal codes: {trailing[:5]}")
    if set(derived) != set(formal_codes) or len(set(derived.values())) != 131:
        raise ValueError("derived release code map does not close 131 unique identities")

    authority_body = {
        "producer_order_algorithm": PRODUCER_ORDER_ALGORITHM,
        "derivation_window": {"start": start.isoformat(), "end": end.isoformat()},
        "sector_data_sha256": sector_data_sha256,
        "industry_pit_authority_envelope_sha256": authority_envelope_sha256,
        "l2_projection_canonical_hash": str(projection["canonical_hash"]),
        "observed_pairs": [
            {"l2_code_id": sector_id, "canonical_l2_code": code} for sector_id, code in sorted(observed_by_id.items())
        ],
        "derived_segments": derivation_segments,
        "non_member_catalog_ids": non_member_catalog_ids,
    }
    authority_sha256 = digest_named_fields(
        DERIVED_MAPPING_AUTHORITY_SCHEMA,
        authority_body,
    )
    payload = build_release_sw_l2_code_map_payload(
        code_to_id=derived,
        member_backed_codes=formal_codes,
        authority_id=f"r7-c013-producer-order:{authority_sha256[:16]}",
        authority_sha256=authority_sha256,
    )
    return payload, {
        "schema_version": DERIVED_MAPPING_AUTHORITY_SCHEMA,
        **authority_body,
        "canonical_hash": authority_sha256,
        "observed_count": len(observed_by_code),
        "derived_count": len(derived) - len(observed_by_code),
        "ambiguity_count": 0,
    }


def _build_authority_membership_spans(
    *,
    adapter: Any,
    universe_spans: list[tuple[str, dt.date, dt.date]],
    calendar: list[dt.date],
    code_to_id: dict[str, int],
    start: dt.date,
    end: dt.date,
):
    import pandas as pd

    target_calendar = [value for value in calendar if start <= value <= end]
    if not target_calendar:
        raise ValueError("release calendar does not cover the requested membership window")
    output: list[dict[str, Any]] = []
    for symbol, eligible_start, eligible_end in universe_spans:
        dates = [value for value in target_calendar if max(start, eligible_start) <= value <= min(end, eligible_end)]
        if not dates:
            continue
        boundaries = {0, len(dates)}
        for transition in adapter.classification_resolver.transition_dates(symbol):
            position = bisect_left(dates, transition)
            if 0 < position < len(dates):
                boundaries.add(position)
        ordered = sorted(boundaries)
        for left, right in zip(ordered, ordered[1:]):
            first = dates[left]
            last = dates[right - 1]
            resolved = adapter.resolve(symbol, first)
            if resolved.status != "resolved" or not resolved.l2_code:
                raise ValueError(
                    "industry PIT authority cannot resolve an active stock-date: "
                    f"instrument={symbol} trade_date={first} reason={resolved.reason_code}"
                )
            sector_id = code_to_id.get(str(resolved.l2_code))
            if sector_id is None:
                raise ValueError(
                    "industry PIT authority resolves a code absent from the release map: "
                    f"instrument={symbol} trade_date={first} code={resolved.l2_code}"
                )
            output.append(
                {
                    "instrument": symbol,
                    "start_date": first,
                    "end_date": last,
                    "l2_code_id": sector_id,
                }
            )
    if not output:
        raise ValueError("industry PIT authority produced no membership spans")
    frame = pd.DataFrame(output)
    frame["l2_code_id"] = frame["l2_code_id"].astype("int32")
    return frame[["instrument", "start_date", "end_date", "l2_code_id"]]


def _factor_membership_denominator(
    frame,
    *,
    universe_spans: list[tuple[str, dt.date, dt.date]],
    start: dt.date,
    end: dt.date,
) -> list[tuple[str, dt.date, dt.date]]:
    selected = frame.loc[
        (frame["datetime"].dt.date >= start) & (frame["datetime"].dt.date <= end),
        ["datetime", "instrument"],
    ]
    if selected.empty:
        raise ValueError("sector_data has no membership denominator rows")
    eligible_by_symbol: dict[str, list[tuple[dt.date, dt.date]]] = {}
    for symbol, eligible_start, eligible_end in universe_spans:
        eligible_by_symbol.setdefault(symbol, []).append((eligible_start, eligible_end))
    denominator: list[tuple[str, dt.date, dt.date]] = []
    for symbol, rows in selected.groupby("instrument", sort=True):
        observed_dates = rows["datetime"].dt.date
        eligible = eligible_by_symbol.get(str(symbol), [])
        covered = observed_dates.map(
            lambda day: any(eligible_start <= day <= eligible_end for eligible_start, eligible_end in eligible)
        )
        if not bool(covered.all()):
            first = observed_dates.loc[~covered].iloc[0]
            raise ValueError(f"sector_data row escapes the PIT stock universe: instrument={symbol} trade_date={first}")
        for eligible_start, eligible_end in eligible:
            in_interval = observed_dates.loc[observed_dates.map(lambda day: eligible_start <= day <= eligible_end)]
            if not in_interval.empty:
                denominator.append((str(symbol), min(in_interval), max(in_interval)))
    return denominator


def _validate_h5_membership_alignment(frame, membership, *, start: dt.date, end: dt.date) -> None:
    selected = frame.loc[
        (frame["datetime"].dt.date >= start) & (frame["datetime"].dt.date <= end),
        ["datetime", "instrument", "l2_code_id"],
    ].copy()
    selected["trade_date"] = selected.pop("datetime").dt.date
    selected = selected.drop_duplicates(["trade_date", "instrument", "l2_code_id"])
    by_symbol = {str(symbol): group for symbol, group in membership.groupby("instrument", sort=False)}
    for symbol, rows in selected.groupby("instrument", sort=False):
        spans = by_symbol.get(str(symbol))
        if spans is None:
            raise ValueError(f"sector_data instrument is absent from PIT membership: {symbol}")
        expected = rows["trade_date"].map(
            lambda day: next(
                (
                    int(span.l2_code_id)
                    for span in spans.itertuples(index=False)
                    if span.start_date <= day <= span.end_date
                ),
                None,
            )
        )
        if expected.isna().any() or not rows["l2_code_id"].astype(int).reset_index(drop=True).equals(
            expected.astype(int).reset_index(drop=True)
        ):
            raise ValueError(f"sector_data membership differs from PIT authority: {symbol}")


def build_component(
    *,
    sector_data_h5: Path,
    code_map_json: Path | None,
    industry_pit_authority_envelope: Path,
    stock_universe_sidecar: Path,
    calendar_path: Path,
    output_root: Path,
    source_dataset_manifest_sha256: str,
    membership_start: dt.date,
    membership_end: dt.date,
) -> dict[str, Any]:
    if output_root.exists():
        raise FileExistsError(f"create-exclusive output already exists: {output_root}")
    inputs = (
        sector_data_h5,
        industry_pit_authority_envelope,
        stock_universe_sidecar,
        calendar_path,
    )
    if any(not path.is_file() for path in inputs) or (code_map_json is not None and not code_map_json.is_file()):
        raise FileNotFoundError("one or more frozen sector-context inputs are absent")
    source_dataset_manifest_sha256 = ensure_sha256(
        source_dataset_manifest_sha256,
        field="source_dataset_manifest_sha256",
    )
    frame = _read_sector_frame(sector_data_h5)
    adapter, authority_envelope = _load_industry_adapter(industry_pit_authority_envelope)
    if code_map_json is None:
        code_map_payload, map_derivation = _derive_release_code_map(
            frame=frame,
            adapter=adapter,
            envelope=authority_envelope,
            start=membership_start,
            end=membership_end,
            sector_data_sha256=_sha256(sector_data_h5),
            authority_envelope_sha256=_sha256(industry_pit_authority_envelope),
        )
        code_map_source_sha256 = None
        code_map = validate_release_sw_l2_code_map(code_map_payload)
    else:
        code_map_payload = json.loads(code_map_json.read_text(encoding="utf-8"))
        code_map = load_release_sw_l2_code_map(code_map_json)
        code_map_source_sha256 = _sha256(code_map_json)
        map_derivation = {
            "schema_version": "aistock_release_sw_l2_frozen_input_v1",
            "source_sha256": code_map_source_sha256,
            "ambiguity_count": 0,
        }
    used_ids = set(frame["l2_code_id"].astype(int))
    unknown_ids = sorted(used_ids - set(code_map.id_to_code))
    if unknown_ids:
        raise ValueError(f"sector_data contains IDs absent from release map: {unknown_ids[:10]}")
    market = _build_market_context(frame)
    universe_spans = _parse_universe_spans(stock_universe_sidecar)
    membership_denominator = _factor_membership_denominator(
        frame,
        universe_spans=universe_spans,
        start=membership_start,
        end=membership_end,
    )
    membership = _build_authority_membership_spans(
        adapter=adapter,
        universe_spans=membership_denominator,
        calendar=_parse_calendar(calendar_path),
        code_to_id=dict(code_map.code_to_id),
        start=membership_start,
        end=membership_end,
    )
    _validate_h5_membership_alignment(
        frame,
        membership,
        start=membership_start,
        end=membership_end,
    )
    market_start, market_end, market_rows = validate_market_context_frame(
        market,
        required_start=membership_start,
        required_end=membership_end,
    )
    membership_first, membership_last, span_rows, symbol_count = validate_membership_frame(
        membership,
        id_to_code=code_map.id_to_code,
        required_start=membership_start,
        required_end=membership_end,
    )

    output_root.mkdir(parents=True, exist_ok=False)
    code_map_target = output_root / "sector_code_map.json"
    _write_exclusive(code_map_target, _canonical_file_bytes(code_map_payload))
    market_path = output_root / "market_context.parquet"
    membership_path = output_root / "sector_membership_spans.parquet"
    market.to_parquet(market_path, index=False)
    membership.to_parquet(membership_path, index=False)
    receipt = {
        "schema_version": COMPONENT_RECEIPT_SCHEMA,
        "source_dataset_manifest_sha256": source_dataset_manifest_sha256,
        "sector_data": {
            "path": "components/factor_h5_static_candidate_v2/sector_data.h5",
            "sha256": _sha256(sector_data_h5),
            "byte_size": sector_data_h5.stat().st_size,
            "used_l2_code_id_count": len(used_ids),
        },
        "sector_code_map": {
            "schema_version": RELEASE_SW_L2_CODE_MAP_SCHEMA,
            "path": "sector_code_map.json",
            "sha256": _sha256(code_map_target),
            "byte_size": code_map_target.stat().st_size,
            "source_sha256": code_map_source_sha256,
            "code_map_digest": code_map.code_map_digest,
            "member_backed_digest": code_map.member_backed_digest,
            "member_backed_count": len(code_map.member_backed_codes),
            "authority": dict(code_map.mapping_authority),
            "derivation": map_derivation,
        },
        "market_context": {
            "schema_version": MARKET_CONTEXT_SCHEMA,
            "path": "market_context.parquet",
            "sha256": _sha256(market_path),
            "byte_size": market_path.stat().st_size,
            "definition": MARKET_VOLUME_DEFINITION,
            "start": market_start.isoformat(),
            "end": market_end.isoformat(),
            "row_count": market_rows,
        },
        "membership": {
            "schema_version": SECTOR_MEMBERSHIP_SPANS_SCHEMA,
            "path": "sector_membership_spans.parquet",
            "sha256": _sha256(membership_path),
            "byte_size": membership_path.stat().st_size,
            "start": membership_first.isoformat(),
            "end": membership_last.isoformat(),
            "span_count": span_rows,
            "symbol_count": symbol_count,
            "denominator": "sector_data_observed_instrument_ranges_with_pit_universe_validation_v1",
            "industry_pit_authority_envelope_sha256": _sha256(industry_pit_authority_envelope),
            "industry_pit_identity": authority_envelope["identity"],
            "stock_universe_sha256": _sha256(stock_universe_sidecar),
            "calendar_sha256": _sha256(calendar_path),
        },
        "database_read": False,
        "database_write": False,
        "runtime_action": False,
    }
    receipt_path = output_root / "component_receipt.json"
    _write_exclusive(receipt_path, _canonical_file_bytes(receipt))
    return {
        **receipt,
        "receipt_sha256": _sha256(receipt_path),
        "component_root": str(output_root),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sector-data-h5", type=Path, required=True)
    parser.add_argument("--code-map-json", type=Path)
    parser.add_argument("--industry-pit-authority-envelope", type=Path, required=True)
    parser.add_argument("--stock-universe-sidecar", type=Path, required=True)
    parser.add_argument("--calendar-path", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--source-dataset-manifest-sha256", required=True)
    parser.add_argument("--membership-start", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--membership-end", type=dt.date.fromisoformat, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = build_component(
        sector_data_h5=args.sector_data_h5,
        code_map_json=args.code_map_json,
        industry_pit_authority_envelope=args.industry_pit_authority_envelope,
        stock_universe_sidecar=args.stock_universe_sidecar,
        calendar_path=args.calendar_path,
        output_root=args.output_root,
        source_dataset_manifest_sha256=args.source_dataset_manifest_sha256,
        membership_start=args.membership_start,
        membership_end=args.membership_end,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
