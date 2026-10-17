#!/usr/bin/env python3
"""Audit every shared moneyflow alias over a frozen candidate window."""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any

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
    TUSHARE_MONEYFLOW_AMOUNT_COLUMNS,
    TUSHARE_MONEYFLOW_VOLUME_COLUMNS,
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


def _load_authoritative_source_facts(identity, *, audit_start: date, audit_end: date) -> pd.DataFrame:
    columns = [*TUSHARE_MONEYFLOW_VOLUME_COLUMNS, *TUSHARE_MONEYFLOW_AMOUNT_COLUMNS]
    frames: list[pd.DataFrame] = []
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
        for alias in identity.rows:
            if alias.source_dataset != MONEYFLOW_DATASET:
                continue
            start = max(audit_start, alias.effective_start)
            end = min(audit_end, alias.effective_end)
            if start > end:
                continue
            sql = f"""
                SELECT trade_date, ts_code, {', '.join(columns)}
                FROM market.moneyflow_ts
                WHERE ts_code = %s
                  AND trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date, ts_code
            """
            frames.append(
                pd.read_sql(
                    sql,
                    conn,
                    params=[
                        alias.source_ts_code,
                        start.isoformat(),
                        end.isoformat(),
                    ],
                )
            )
    source = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if source.empty:
        return source
    canonical_codes = sorted({row.canonical_ts_code for row in identity.rows if row.source_dataset == MONEYFLOW_DATASET})
    source = identity.annotate_source_rows(
        source,
        canonical_codes=canonical_codes,
        source_dataset=MONEYFLOW_DATASET,
    )
    return normalize_tushare_moneyflow_units(source, copy=False).rename(columns=MONEYFLOW_FIELD_MAP)


def audit(
    *,
    candidate_root: Path,
    identity_manifest_path: Path,
    provider_absence_path: Path,
    audit_start: date,
    audit_end: date,
) -> dict[str, Any]:
    root = candidate_root.resolve(strict=True)
    daily_path = root / "components" / "factor_h5_static_candidate_v2" / "daily_pv.h5"
    moneyflow_path = root / "components" / "factor_h5_static_candidate_v2" / "moneyflow.h5"
    for path in (daily_path, moneyflow_path, identity_manifest_path, provider_absence_path):
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"audit input must be a regular file: {path}")

    identity = load_security_source_identity_manifest(identity_manifest_path)
    absence_keys, absence_evidence = _provider_absence_keys(provider_absence_path)
    if audit_start > audit_end:
        raise ValueError("audit window is invalid")
    source_facts = _load_authoritative_source_facts(identity, audit_start=audit_start, audit_end=audit_end)
    expected: set[tuple[str, date, str, str]] = set()
    resolved: set[tuple[str, date, str, str]] = set()
    authorized_absence: set[tuple[str, date, str, str]] = set()
    nonfinite: list[dict[str, str]] = []
    mismatched: list[dict[str, str]] = []
    factor_columns = list(MONEYFLOW_FIELD_MAP.values())

    for alias in identity.rows:
        if alias.source_dataset != MONEYFLOW_DATASET:
            continue
        if alias.effective_start is None or alias.effective_end is None:
            raise ValueError("explicit alias interval is incomplete")
        moneyflow = _load_symbol(moneyflow_path, alias.source_ts_code, factor_columns)
        moneyflow_by_date = {
            pd.Timestamp(index[0]).date(): row
            for index, row in moneyflow.iterrows()
            if alias.effective_start <= pd.Timestamp(index[0]).date() <= alias.effective_end
        }
        alias_facts = source_facts[
            (source_facts["_canonical_ts_code"] == alias.canonical_ts_code)
            & (source_facts["ts_code"] == alias.source_ts_code)
            & (pd.to_datetime(source_facts["trade_date"]).dt.date >= max(alias.effective_start, audit_start))
            & (pd.to_datetime(source_facts["trade_date"]).dt.date <= min(alias.effective_end, audit_end))
        ]
        fact_by_date = {pd.Timestamp(row["trade_date"]).date(): row for _, row in alias_facts.iterrows()}
        alias_absence_dates = {
            item[3]
            for item in absence_keys
            if item[0] == alias.canonical_ts_code
            and item[1] == alias.source_dataset
            and item[2] == alias.source_ts_code
            and max(alias.effective_start, audit_start) <= item[3] <= min(alias.effective_end, audit_end)
        }
        for trade_date in sorted(set(fact_by_date) | alias_absence_dates):
            key = (alias.canonical_ts_code, trade_date, alias.source_dataset, alias.source_ts_code)
            expected.add(key)
            source_row = fact_by_date.get(trade_date)
            frozen_row = moneyflow_by_date.get(trade_date)
            if source_row is not None and frozen_row is not None:
                source_values = pd.to_numeric(source_row[factor_columns], errors="coerce")
                frozen_values = pd.to_numeric(frozen_row[factor_columns], errors="coerce")
                if not pd.notna(frozen_values["mf_net_amt"]) or not math.isfinite(
                    float(frozen_values["mf_net_amt"])
                ):
                    nonfinite.append(
                        {"canonical_ts_code": alias.canonical_ts_code, "trade_date": trade_date.isoformat()}
                    )
                elif not all(
                    (pd.isna(source_values[column]) and pd.isna(frozen_values[column]))
                    or (
                        pd.notna(source_values[column])
                        and pd.notna(frozen_values[column])
                        and math.isclose(
                            float(source_values[column]),
                            float(frozen_values[column]),
                            rel_tol=1e-6,
                            abs_tol=1e-3,
                        )
                    )
                    for column in factor_columns
                ):
                    mismatched.append(
                        {"canonical_ts_code": alias.canonical_ts_code, "trade_date": trade_date.isoformat()}
                    )
                else:
                    resolved.add(key)
            elif source_row is not None:
                continue
            elif frozen_row is not None:
                mismatched.append(
                    {"canonical_ts_code": alias.canonical_ts_code, "trade_date": trade_date.isoformat()}
                )
            elif (alias.canonical_ts_code, alias.source_dataset, alias.source_ts_code, trade_date) in absence_keys:
                authorized_absence.add(key)

    unknown = sorted(expected - resolved - authorized_absence)
    ordered_expected = [
        {
            "canonical_ts_code": item[0],
            "trade_date": item[1].isoformat(),
            "source_dataset": item[2],
            "effective_source_code": item[3],
        }
        for item in sorted(expected)
    ]
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if not unknown and not nonfinite and not mismatched else "BLOCKED",
        "candidate_root": str(root),
        "source_dataset": MONEYFLOW_DATASET,
        "audit_start": audit_start.isoformat(),
        "audit_end": audit_end.isoformat(),
        "identity_authority": identity.evidence(),
        "provider_absence_authority": absence_evidence,
        "daily_pv_sha256": _sha256(daily_path),
        "moneyflow_sha256": _sha256(moneyflow_path),
        "alias_count": sum(1 for row in identity.rows if row.source_dataset == MONEYFLOW_DATASET),
        "expected": len(expected),
        "resolved": len(resolved),
        "provider_absence": len(authorized_absence),
        "unknown": len(unknown),
        "nonfinite": len(nonfinite),
        "mismatched": len(mismatched),
        "expected_key_sha256": hashlib.sha256(canonical_json_bytes(ordered_expected)).hexdigest(),
        "unknown_sample": [
            {
                "canonical_ts_code": item[0],
                "trade_date": item[1].isoformat(),
                "source_dataset": item[2],
                "effective_source_code": item[3],
            }
            for item in unknown[:20]
        ],
        "nonfinite_sample": nonfinite[:20],
        "mismatched_sample": mismatched[:20],
        "source_fact_count": int(len(source_facts)),
        "source_fact_sha256": hashlib.sha256(canonical_json_bytes(
            source_facts.sort_values(["trade_date", "ts_code"])
            .assign(trade_date=lambda value: value["trade_date"].astype(str))
            .astype(object)
            .where(pd.notna(source_facts.sort_values(["trade_date", "ts_code"])), None)
            .to_dict("records")
        )).hexdigest(),
        "database_read": True,
        "database_write": False,
    }
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
