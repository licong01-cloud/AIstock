#!/usr/bin/env python3
"""Audit every shared moneyflow alias over a frozen candidate window."""

from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import date
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.data_service.security_source_identity import (  # noqa: E402
    MONEYFLOW_DATASET,
    canonical_json_bytes,
    load_security_source_identity_manifest,
)
from backend.data_service.moneyflow_contract import (  # noqa: E402
    MONEYFLOW_FIELD_MAP,
    normalize_tushare_moneyflow_units,
)
from backend.db.pg_pool import get_conn  # noqa: E402


SCHEMA_VERSION = "qe_moneyflow_alias_coverage_receipt_v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_symbol(path: Path, symbol: str, columns: list[str]) -> pd.DataFrame:
    try:
        return pd.read_hdf(path, key="data", where=f"instrument == '{symbol}'", columns=columns)
    except (KeyError, TypeError, ValueError):
        frame = pd.read_hdf(path, key="data", columns=columns)
        if not isinstance(frame.index, pd.MultiIndex) or "instrument" not in frame.index.names:
            raise ValueError(f"{path} does not expose a canonical instrument index")
        return frame[frame.index.get_level_values("instrument") == symbol]


def _provider_absence_keys(path: Path) -> tuple[set[tuple[str, str, str, date]], dict[str, Any]]:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    rows = payload.get("rows")
    if not isinstance(payload, dict) or not isinstance(rows, list):
        raise ValueError("provider absence manifest is invalid")
    keys: set[tuple[str, str, str, date]] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("provider absence row is invalid")
        key = (
            str(row.get("canonical_ts_code")),
            str(row.get("source_dataset")),
            str(row.get("source_ts_code")),
            date.fromisoformat(str(row.get("trade_date"))),
        )
        if key in keys:
            raise ValueError(f"duplicate provider absence key: {key}")
        keys.add(key)
    return keys, {
        "path": str(path.resolve()),
        "schema_version": payload.get("schema_version"),
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "row_count": len(rows),
    }


def _load_authoritative_source_facts(identity, *, audit_start, audit_end, connection=None):
    """One read-only snapshot, including canonical dates after an alias ends."""
    symbols = sorted({r.canonical_ts_code for r in identity.rows if r.source_dataset == MONEYFLOW_DATASET})
    codes = identity.query_source_codes(symbols, audit_start, audit_end, MONEYFLOW_DATASET)
    with nullcontext(connection) if connection is not None else get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(
                f"SELECT trade_date,ts_code,{','.join(MONEYFLOW_FIELD_MAP)} "
                "FROM market.moneyflow_ts WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s "
                "ORDER BY trade_date,ts_code", (codes, audit_start, audit_end),
            )
            frame = pd.DataFrame(cursor.fetchall(), columns=["trade_date", "ts_code", *MONEYFLOW_FIELD_MAP])
    frame = identity.annotate_source_rows(frame, canonical_codes=symbols, source_dataset=MONEYFLOW_DATASET)
    return normalize_tushare_moneyflow_units(frame).rename(columns=MONEYFLOW_FIELD_MAP)


def classify_sessions(*, identity, symbol, sessions, price_dates, suspension_dates,
                      source, frozen, absence_keys):
    columns = list(MONEYFLOW_FIELD_MAP.values())

    def index(frame):
        values = {}
        for _, row in frame.iterrows():
            key = (str(row["ts_code"]), pd.Timestamp(row["trade_date"]).date())
            if key in values:
                raise ValueError(f"duplicate source/frozen key: {key}")
            values[key] = row[columns].to_numpy(dtype=float)
        return values

    src, target = index(source), index(frozen)
    results = []
    for observed in sorted(set(sessions)):
        resolution = identity.resolve(symbol, observed, MONEYFLOW_DATASET)
        key = (resolution.source_ts_code, observed)
        original, value = src.get(key), target.get(key)
        absent = (symbol, MONEYFLOW_DATASET, key[0], observed) in absence_keys
        if absent and (original is not None or value is not None):
            raise ValueError(f"provider absence contradicts observed fact: {key}")
        other_frozen_identity = any(code != key[0] and day == observed for code, day in target)
        if other_frozen_identity:
            kind = "SOURCE_IDENTITY_CONFLICT"
        elif original is not None and not np.isfinite(original).all():
            kind = "NONFINITE_SOURCE"
        elif value is not None and not np.isfinite(value).all():
            kind = "NONFINITE_FROZEN"
        elif original is not None and value is not None:
            kind = "MATCHED" if np.allclose(original, value, rtol=1e-6, atol=1e-3) else "VALUE_MISMATCH"
        elif original is not None:
            kind = "EXPORT_MISSING"
        elif value is not None:
            kind = "SOURCE_UNVERIFIED"
        elif observed in suspension_dates and observed not in price_dates:
            kind = "SUSPENDED"
        elif absent:
            kind = "PROVIDER_ABSENCE"
        else:
            kind = "SOURCE_AND_FROZEN_MISSING"
        results.append({"canonical_ts_code": symbol, "trade_date": observed.isoformat(),
                        "source_dataset": MONEYFLOW_DATASET, "effective_source_code": key[0],
                        "classification": kind, "identity_row_sha256": resolution.row_hash})
    return results


def audit(*, candidate_root, identity_manifest_path, provider_absence_path,
          audit_start, audit_end, source_facts=None):
    root = Path(candidate_root).resolve(strict=True)
    factor = root / "components/factor_h5_static_candidate_v2"
    paths = {"daily_pv": factor / "daily_pv.h5", "moneyflow": factor / "moneyflow.h5",
             "calendar": root / "components/daily_bin_candidate/calendars/day.txt",
             "pit_pool": root / "stock_pools/stock_universe.txt",
             "suspend": root / "components/suspend_d_daily_candidate_v2/suspend_d.parquet"}
    for path in [*paths.values(), identity_manifest_path, provider_absence_path]:
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"audit input must be a regular file: {path}")
    identity = load_security_source_identity_manifest(identity_manifest_path)
    absence_keys, absence_evidence = _provider_absence_keys(provider_absence_path)
    if audit_start > audit_end:
        raise ValueError("audit window is invalid")
    calendar = [date.fromisoformat(line.strip()) for line in paths["calendar"].read_text().splitlines()]
    if calendar != sorted(set(calendar)) or audit_start < min(calendar) or audit_end > max(calendar):
        raise ValueError("frozen calendar does not uniquely cover requested window")
    spans = {}
    for line in paths["pit_pool"].read_text().splitlines():
        code, start, end = line.split()
        if date.fromisoformat(start) > date.fromisoformat(end):
            raise ValueError("PIT span is inverted")
        spans.setdefault(code, []).append((date.fromisoformat(start), date.fromisoformat(end)))
    suspension = pd.read_parquet(paths["suspend"])
    if source_facts is None:
        source_facts = _load_authoritative_source_facts(identity, audit_start=audit_start, audit_end=audit_end)
    results = []
    for symbol in sorted({r.canonical_ts_code for r in identity.rows if r.source_dataset == MONEYFLOW_DATASET}):
        sessions = [d for d in calendar if audit_start <= d <= audit_end
                    and any(start <= d <= end for start, end in spans.get(symbol, []))]
        prices = _load_symbol(paths["daily_pv"], symbol, ["close"])
        if prices.index.has_duplicates:
            raise ValueError("duplicate frozen price index")
        price_dates = {pd.Timestamp(i[0]).date() for i, row in prices.iterrows()
                       if pd.notna(row["close"]) and np.isfinite(row["close"]) and row["close"] > 0}
        suspended = suspension[(suspension["ts_code"] == symbol) & (suspension["suspend_type"] == "S")
                               & suspension["suspend_timing"].isna()]
        suspension_dates = set(pd.to_datetime(suspended["trade_date"]).dt.date)
        codes = identity.query_source_codes([symbol], audit_start, audit_end, MONEYFLOW_DATASET)
        frozen = pd.concat([_load_symbol(paths["moneyflow"], c, list(MONEYFLOW_FIELD_MAP.values()))
                            for c in codes]).reset_index().rename(columns={"datetime": "trade_date", "instrument": "ts_code"})
        source = source_facts[source_facts["_canonical_ts_code"] == symbol]
        results.extend(classify_sessions(identity=identity, symbol=symbol, sessions=sessions,
                       price_dates=price_dates, suspension_dates=suspension_dates,
                       source=source, frozen=frozen, absence_keys=absence_keys))
    counts = {kind: sum(r["classification"] == kind for r in results)
              for kind in sorted({r["classification"] for r in results})}
    good = {"MATCHED", "SUSPENDED", "PROVIDER_ABSENCE"}
    unknown = [r for r in results if r["classification"] not in good]
    expected_keys = [{k: row[k] for k in ("canonical_ts_code", "trade_date", "source_dataset", "effective_source_code")}
                     for row in results if row["classification"] != "SUSPENDED"]
    receipt = {"schema_version": SCHEMA_VERSION, "status": "BLOCKED" if unknown else "PASS",
               "candidate_root": str(root), "audit_start": audit_start.isoformat(), "audit_end": audit_end.isoformat(),
               "source_dataset": MONEYFLOW_DATASET, "identity_authority": identity.evidence(),
               "provider_absence_authority": absence_evidence,
               "input_sha256": {key: _sha256(path) for key, path in paths.items()},
               "moneyflow_sha256": _sha256(paths["moneyflow"]),
               "daily_pv_sha256": _sha256(paths["daily_pv"]),
               "alias_count": sum(r.source_dataset == MONEYFLOW_DATASET for r in identity.rows),
               "symbol_count": len({r["canonical_ts_code"] for r in results}),
               "session_count": len(results), "expected": len(results) - counts.get("SUSPENDED", 0),
               "resolved": counts.get("MATCHED", 0), "suspended": counts.get("SUSPENDED", 0),
               "provider_absence": counts.get("PROVIDER_ABSENCE", 0), "unknown": len(unknown),
               "nonfinite": counts.get("NONFINITE_SOURCE", 0) + counts.get("NONFINITE_FROZEN", 0),
               "mismatched": counts.get("VALUE_MISMATCH", 0),
               "classifications": counts, "unknown_rows": unknown,
               "source_fact_count": len(source_facts), "database_read": True, "database_write": False,
               "expected_key_sha256": hashlib.sha256(canonical_json_bytes(expected_keys)).hexdigest()}
    for label, start, end in [("train", date(2022, 1, 1), date(2024, 6, 30)),
                              ("validation", date(2024, 7, 1), date(2025, 3, 31))]:
        selected = [r for r in results if max(start, audit_start).isoformat() <= r["trade_date"] <= min(end, audit_end).isoformat()]
        receipt[label] = {"sessions": len(selected), "resolved": sum(r["classification"] == "MATCHED" for r in selected),
                          "suspended": sum(r["classification"] == "SUSPENDED" for r in selected),
                          "unknown": sum(r["classification"] not in good for r in selected)}
    return receipt

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--security-identity-manifest", type=Path, required=True)
    parser.add_argument("--provider-absence-manifest", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if args.env_file is not None:
        if not args.env_file.is_file():
            parser.error("--env-file must be an existing file")
        load_dotenv(args.env_file, override=False)
    receipt = audit(
        candidate_root=args.candidate_root,
        identity_manifest_path=args.security_identity_manifest,
        provider_absence_path=args.provider_absence_manifest,
        audit_start=args.start_date,
        audit_end=args.end_date,
    )
    payload = canonical_json_bytes(receipt) + b"\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("xb") as stream:
            stream.write(payload)
    sys.stdout.buffer.write(payload)
    return 0 if receipt["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
