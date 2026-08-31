#!/usr/bin/env python
"""File-only P0-D2 sector/stock four-cell ceiling diagnostic.

The output is a signal-level research receipt.  Oracle cells are permanently
non-deployable and the tool deliberately does not synthesize TWAP portfolio,
cost, turnover, fill, or active-exposure evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


INPUT_SCHEMA = "qe_p0_d2_sector_oracle_input_v1"
RECEIPT_SCHEMA = "qe_p0_d2_sector_oracle_receipt_v1"
OUTCOME_COMPUTABLE = "FOUR_CELL_COMPUTABLE"
OUTCOME_NOT_COMPUTABLE = "NOT_COMPUTABLE"
ORACLE_MARKER = "QE_ONLY_FUTURE_INFORMATION_CEILING"
PORTFOLIO_STATUS = "NOT_COMPUTABLE_WAITING_D1R_TWAP"

PANEL_COLUMNS = (
    "datetime",
    "instrument",
    "score",
    "label",
    "l2_code_id",
    "tradable",
)
IDENTITY_KEYS = (
    "dataset",
    "taxonomy",
    "prediction",
    "label",
    "execution_contract",
)
CONFIG_KEYS = (
    "top_m",
    "top_k",
    "tail_fraction",
    "bootstrap_block_days",
    "bootstrap_samples",
    "bootstrap_seed",
    "max_rows",
    "max_file_bytes",
)
CELL_SPECS = (
    ("D2-RR", "reality", "reality"),
    ("D2-OR", "oracle", "reality"),
    ("D2-RO", "reality", "oracle"),
    ("D2-OO", "oracle", "oracle"),
)
GATING_MODES = ("hard", "soft")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
INSTRUMENT_RE = re.compile(r"^(?:\d{6}\.(?:SH|SZ)|(?:SH|SZ)\d{6})$")
HARD_MAX_ROWS = 5_000_000
HARD_MAX_FILE_BYTES = 2 * 1024 * 1024 * 1024
HARD_MAX_BOOTSTRAP_SAMPLES = 10_000


@dataclass(frozen=True)
class D2InputError(ValueError):
    reason_code: str
    detail: str

    def __str__(self) -> str:
        return f"{self.reason_code}: {self.detail}"


@dataclass(frozen=True)
class ValidatedInput:
    manifest_sha256: str
    panel_sha256: str
    identities: Mapping[str, Mapping[str, str]]
    config: Mapping[str, Any]


def _canonical_json_bytes(payload: Any) -> bytes:
    try:
        text = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise D2InputError("qe_p0_d2_json_not_canonical", str(exc)) from exc
    return text.encode("utf-8")


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _require_mapping(value: Any, *, reason_code: str, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise D2InputError(reason_code, f"{label} must be a JSON object")
    return value


def _require_text(value: Any, *, reason_code: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise D2InputError(reason_code, f"{label} must be a non-empty string")
    return value.strip()


def _require_sha(value: Any, *, reason_code: str, label: str) -> str:
    text = _require_text(value, reason_code=reason_code, label=label)
    if not SHA256_RE.fullmatch(text):
        raise D2InputError(reason_code, f"{label} must be a lowercase SHA256")
    return text


def _require_int(value: Any, *, label: str, minimum: int = 1, maximum: int | None = None) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        bound = f"[{minimum}, {maximum}]" if maximum is not None else f">= {minimum}"
        raise D2InputError("qe_p0_d2_config_invalid", f"{label} must be an integer in {bound}")
    return value


def _require_float(value: Any, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise D2InputError("qe_p0_d2_config_invalid", f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise D2InputError("qe_p0_d2_config_invalid", f"{label} must be finite")
    return result


def validate_input_manifest(payload: Any, *, panel_path: Path) -> ValidatedInput:
    manifest = _require_mapping(
        payload,
        reason_code="qe_p0_d2_manifest_invalid",
        label="input manifest",
    )
    expected_fields = {
        "schema_version",
        "panel_sha256",
        "identities",
        "config",
        "manifest_sha256",
    }
    if set(manifest) != expected_fields:
        raise D2InputError(
            "qe_p0_d2_manifest_fields_invalid",
            f"manifest must contain exactly {sorted(expected_fields)}",
        )
    if manifest.get("schema_version") != INPUT_SCHEMA:
        raise D2InputError(
            "qe_p0_d2_manifest_schema_invalid",
            f"schema_version must be {INPUT_SCHEMA}",
        )
    supplied_manifest_sha = _require_sha(
        manifest.get("manifest_sha256"),
        reason_code="qe_p0_d2_manifest_sha_invalid",
        label="manifest_sha256",
    )
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256")
    calculated_manifest_sha = canonical_sha256(unsigned)
    if supplied_manifest_sha != calculated_manifest_sha:
        raise D2InputError(
            "qe_p0_d2_manifest_sha_mismatch",
            f"supplied={supplied_manifest_sha} calculated={calculated_manifest_sha}",
        )
    if panel_path.is_symlink() or not panel_path.is_file():
        raise D2InputError(
            "qe_p0_d2_panel_file_invalid",
            f"panel must be an existing non-symlink file: {panel_path}",
        )
    if panel_path.stat().st_size > HARD_MAX_FILE_BYTES:
        raise D2InputError(
            "qe_p0_d2_resource_limit_exceeded",
            f"panel exceeds hard byte limit={HARD_MAX_FILE_BYTES}",
        )
    supplied_panel_sha = _require_sha(
        manifest.get("panel_sha256"),
        reason_code="qe_p0_d2_panel_sha_invalid",
        label="panel_sha256",
    )
    calculated_panel_sha = file_sha256(panel_path)
    if supplied_panel_sha != calculated_panel_sha:
        raise D2InputError(
            "qe_p0_d2_panel_sha_mismatch",
            f"supplied={supplied_panel_sha} calculated={calculated_panel_sha}",
        )
    identities = _require_mapping(
        manifest.get("identities"),
        reason_code="qe_p0_d2_identity_invalid",
        label="identities",
    )
    if set(identities) != set(IDENTITY_KEYS):
        raise D2InputError(
            "qe_p0_d2_identity_set_invalid",
            f"identities must contain exactly {list(IDENTITY_KEYS)}",
        )
    normalized_identities: dict[str, Mapping[str, str]] = {}
    for key in IDENTITY_KEYS:
        identity = _require_mapping(
            identities[key],
            reason_code="qe_p0_d2_identity_invalid",
            label=f"identities.{key}",
        )
        if set(identity) != {"identity", "sha256"}:
            raise D2InputError(
                "qe_p0_d2_identity_fields_invalid",
                f"identities.{key} must contain exactly identity/sha256",
            )
        normalized_identities[key] = {
            "identity": _require_text(
                identity.get("identity"),
                reason_code="qe_p0_d2_identity_invalid",
                label=f"identities.{key}.identity",
            ),
            "sha256": _require_sha(
                identity.get("sha256"),
                reason_code="qe_p0_d2_identity_sha_invalid",
                label=f"identities.{key}.sha256",
            ),
        }
    config = _require_mapping(
        manifest.get("config"),
        reason_code="qe_p0_d2_config_invalid",
        label="config",
    )
    if set(config) != set(CONFIG_KEYS):
        raise D2InputError(
            "qe_p0_d2_config_fields_invalid",
            f"config must contain exactly {list(CONFIG_KEYS)}",
        )
    tail_fraction = _require_float(config["tail_fraction"], label="config.tail_fraction")
    if not 0 < tail_fraction <= 1:
        raise D2InputError("qe_p0_d2_config_invalid", "tail_fraction must be in (0, 1]")
    normalized_config = {
        "top_m": _require_int(config["top_m"], label="config.top_m", maximum=200),
        "top_k": _require_int(config["top_k"], label="config.top_k", maximum=1_000),
        "tail_fraction": tail_fraction,
        "bootstrap_block_days": _require_int(
            config["bootstrap_block_days"], label="config.bootstrap_block_days", maximum=252
        ),
        "bootstrap_samples": _require_int(
            config["bootstrap_samples"],
            label="config.bootstrap_samples",
            minimum=100,
            maximum=HARD_MAX_BOOTSTRAP_SAMPLES,
        ),
        "bootstrap_seed": _require_int(
            config["bootstrap_seed"], label="config.bootstrap_seed", minimum=0
        ),
        "max_rows": _require_int(
            config["max_rows"], label="config.max_rows", maximum=HARD_MAX_ROWS
        ),
        "max_file_bytes": _require_int(
            config["max_file_bytes"],
            label="config.max_file_bytes",
            maximum=HARD_MAX_FILE_BYTES,
        ),
    }
    if panel_path.stat().st_size > normalized_config["max_file_bytes"]:
        raise D2InputError(
            "qe_p0_d2_resource_limit_exceeded",
            f"panel bytes exceed max_file_bytes={normalized_config['max_file_bytes']}",
        )
    try:
        parquet_file = pq.ParquetFile(panel_path)
        metadata = parquet_file.metadata
    except Exception as exc:
        raise D2InputError("qe_p0_d2_panel_parquet_invalid", str(exc)) from exc
    parquet_columns = tuple(parquet_file.schema_arrow.names)
    if set(parquet_columns) != set(PANEL_COLUMNS) or len(parquet_columns) != len(PANEL_COLUMNS):
        raise D2InputError(
            "qe_p0_d2_panel_columns_invalid",
            f"Parquet columns must be exactly {list(PANEL_COLUMNS)}; observed={list(parquet_columns)}",
        )
    if metadata is None or metadata.num_rows > normalized_config["max_rows"]:
        rows = None if metadata is None else metadata.num_rows
        raise D2InputError(
            "qe_p0_d2_resource_limit_exceeded",
            f"panel rows={rows} exceed max_rows={normalized_config['max_rows']}",
        )
    return ValidatedInput(
        manifest_sha256=supplied_manifest_sha,
        panel_sha256=supplied_panel_sha,
        identities=normalized_identities,
        config=normalized_config,
    )


def validate_panel(panel: pd.DataFrame, *, config: Mapping[str, Any]) -> pd.DataFrame:
    if set(panel.columns) != set(PANEL_COLUMNS):
        missing = sorted(set(PANEL_COLUMNS) - set(panel.columns))
        extra = sorted(set(panel.columns) - set(PANEL_COLUMNS))
        raise D2InputError(
            "qe_p0_d2_panel_columns_invalid",
            f"missing={missing} extra={extra}",
        )
    frame = panel.loc[:, PANEL_COLUMNS].copy()
    try:
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="raise")
    except Exception as exc:
        raise D2InputError("qe_p0_d2_datetime_invalid", str(exc)) from exc
    if frame["datetime"].dt.tz is not None or not frame["datetime"].eq(frame["datetime"].dt.normalize()).all():
        raise D2InputError("qe_p0_d2_datetime_invalid", "datetime must be timezone-naive daily midnight")
    if frame[["datetime", "instrument"]].duplicated().any():
        raise D2InputError("qe_p0_d2_panel_duplicate_key", "datetime/instrument keys must be unique")
    if not frame["instrument"].map(lambda value: isinstance(value, str) and bool(INSTRUMENT_RE.fullmatch(value))).all():
        raise D2InputError("qe_p0_d2_instrument_invalid", "instrument must be a canonical SH/SZ symbol")
    for column in ("score", "label", "l2_code_id"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if not np.isfinite(frame[["score", "label", "l2_code_id"]].to_numpy(dtype=float)).all():
        raise D2InputError("qe_p0_d2_numeric_invalid", "score/label/l2_code_id must be finite")
    if not frame["l2_code_id"].mod(1).eq(0).all() or not frame["l2_code_id"].gt(0).all():
        raise D2InputError("qe_p0_d2_taxonomy_invalid", "l2_code_id must be a positive integer")
    frame["l2_code_id"] = frame["l2_code_id"].astype("int32")
    if frame["tradable"].dtype != bool or not frame["tradable"].all():
        raise D2InputError(
            "qe_p0_d2_tradability_invalid",
            "panel must contain only explicitly tradable boolean rows",
        )
    if len(frame) > int(config["max_rows"]):
        raise D2InputError("qe_p0_d2_resource_limit_exceeded", "loaded rows exceed max_rows")
    counts = frame.groupby("datetime", sort=True).size()
    sectors = frame.groupby("datetime", sort=True)["l2_code_id"].nunique()
    if counts.empty:
        raise D2InputError("qe_p0_d2_panel_empty", "panel has no rows")
    if counts.lt(int(config["top_k"])).any():
        raise D2InputError("qe_p0_d2_daily_coverage_insufficient", "a date has fewer rows than top_k")
    if sectors.lt(int(config["top_m"])).any():
        raise D2InputError("qe_p0_d2_sector_coverage_insufficient", "a date has fewer sectors than top_m")
    sector_sizes = frame.groupby(["datetime", "l2_code_id"], sort=True).size()
    if sector_sizes.lt(2).any():
        raise D2InputError(
            "qe_p0_d2_within_sector_coverage_insufficient",
            "every date/sector requires at least two stocks",
        )
    return frame.sort_values(["datetime", "l2_code_id", "instrument"], kind="mergesort").reset_index(drop=True)


def _rank_order(frame: pd.DataFrame, score_column: str) -> pd.DataFrame:
    columns = [score_column, "l2_code_id"]
    ascending = [False, True]
    if "instrument" in frame.columns:
        columns.append("instrument")
        ascending.append(True)
    return frame.sort_values(
        columns,
        ascending=ascending,
        kind="mergesort",
    )


def _ndcg_at_m(predicted: Sequence[int], oracle_order: Sequence[int], top_m: int) -> float:
    relevance = {sector: len(oracle_order) - rank for rank, sector in enumerate(oracle_order)}

    def dcg(order: Sequence[int]) -> float:
        return float(
            sum(relevance.get(sector, 0) / math.log2(position + 2) for position, sector in enumerate(order[:top_m]))
        )

    ideal = dcg(oracle_order)
    return dcg(predicted) / ideal if ideal > 0 else 0.0


def _moving_block_ci(
    values: Sequence[float],
    *,
    block_days: int,
    samples: int,
    seed: int,
) -> Mapping[str, float]:
    array = np.asarray(values, dtype=float)
    if len(array) == 0 or not np.isfinite(array).all():
        raise D2InputError("qe_p0_d2_bootstrap_input_invalid", "bootstrap values must be finite")
    block = min(block_days, len(array))
    rng = np.random.default_rng(seed)
    means = np.empty(samples, dtype=float)
    starts_needed = math.ceil(len(array) / block)
    offsets = np.arange(block)
    for index in range(samples):
        starts = rng.integers(0, len(array), size=starts_needed)
        positions = ((starts[:, None] + offsets[None, :]) % len(array)).reshape(-1)[: len(array)]
        means[index] = float(array[positions].mean())
    return {
        "mean": float(array.mean()),
        "ci_lower_95": float(np.quantile(means, 0.025)),
        "ci_upper_95": float(np.quantile(means, 0.975)),
    }


def _evaluate_date(
    day: pd.DataFrame,
    *,
    sector_mode: str,
    stock_mode: str,
    gating: str,
    top_m: int,
    top_k: int,
    tail_fraction: float,
) -> Mapping[str, float]:
    sector_table = (
        day.groupby("l2_code_id", sort=True)
        .agg(reality_sector_score=("score", "mean"), oracle_sector_score=("label", "mean"))
        .reset_index()
    )
    sector_score_column = f"{sector_mode}_sector_score"
    predicted_sectors = _rank_order(sector_table, sector_score_column)
    oracle_sectors = _rank_order(sector_table, "oracle_sector_score")
    predicted_order = predicted_sectors["l2_code_id"].astype(int).tolist()
    oracle_order = oracle_sectors["l2_code_id"].astype(int).tolist()
    predicted_top = set(predicted_order[:top_m])
    oracle_top = set(oracle_order[:top_m])
    tail_sector_count = max(1, math.ceil(len(oracle_order) * tail_fraction))
    oracle_tail_sectors = set(oracle_order[:tail_sector_count])

    stock_column = "score" if stock_mode == "reality" else "label"
    if gating == "hard":
        candidates = day.loc[day["l2_code_id"].isin(predicted_top)].copy()
        selected = _rank_order(candidates, stock_column).head(top_k)
    else:
        sector_percentile = sector_table.set_index("l2_code_id")[sector_score_column].rank(
            method="first", pct=True
        )
        candidates = day.copy()
        candidates["sector_percentile"] = candidates["l2_code_id"].map(sector_percentile)
        candidates["stock_percentile"] = candidates.groupby("l2_code_id", sort=True)[stock_column].rank(
            method="first", pct=True
        )
        candidates["soft_score"] = candidates["sector_percentile"] * candidates["stock_percentile"]
        selected = _rank_order(candidates, "soft_score").head(top_k)
    if len(selected) != top_k:
        raise D2InputError(
            "qe_p0_d2_selection_coverage_insufficient",
            f"selected rows={len(selected)} expected top_k={top_k}",
        )

    correlations: list[float] = []
    selected_sector_panel = day.loc[day["l2_code_id"].isin(predicted_top)]
    for _sector, group in selected_sector_panel.groupby("l2_code_id", sort=True):
        correlation = group[stock_column].rank(method="average").corr(
            group["label"].rank(method="average")
        )
        if pd.notna(correlation):
            correlations.append(float(correlation))
    if not correlations:
        raise D2InputError("qe_p0_d2_within_sector_rankic_invalid", "no finite within-sector RankIC")

    tail_stock_count = max(1, math.ceil(len(day) * tail_fraction))
    oracle_tail_stocks = set(_rank_order(day, "label").head(tail_stock_count)["instrument"])
    selected_stocks = set(selected["instrument"])
    return {
        "sector_recall_at_m": len(predicted_top & oracle_top) / len(oracle_top),
        "sector_ndcg_at_m": _ndcg_at_m(predicted_order, oracle_order, top_m),
        "tail_sector_capture": len(predicted_top & oracle_tail_sectors) / len(oracle_tail_sectors),
        "within_sector_rankic": float(np.mean(correlations)),
        "stock_tail_recall": len(selected_stocks & oracle_tail_stocks) / len(oracle_tail_stocks),
        "selected_label_mean": float(selected["label"].mean()),
        "selected_label_hit_rate": float(selected["label"].gt(0).mean()),
    }


def evaluate_panel(panel: pd.DataFrame, *, validated_input: ValidatedInput) -> Mapping[str, Any]:
    frame = validate_panel(panel, config=validated_input.config)
    config = validated_input.config
    daily_sample_counts = {
        pd.Timestamp(current_date).date().isoformat(): {
            "rows": int(len(group)),
            "sectors": int(group["l2_code_id"].nunique()),
        }
        for current_date, group in frame.groupby("datetime", sort=True)
    }
    cell_receipts: list[Mapping[str, Any]] = []
    for cell_id, sector_mode, stock_mode in CELL_SPECS:
        for gating in GATING_MODES:
            daily_rows = [
                _evaluate_date(
                    group,
                    sector_mode=sector_mode,
                    stock_mode=stock_mode,
                    gating=gating,
                    top_m=int(config["top_m"]),
                    top_k=int(config["top_k"]),
                    tail_fraction=float(config["tail_fraction"]),
                )
                for _date, group in frame.groupby("datetime", sort=True)
            ]
            metrics = {
                key: float(np.mean([row[key] for row in daily_rows]))
                for key in (
                    "sector_recall_at_m",
                    "sector_ndcg_at_m",
                    "tail_sector_capture",
                    "within_sector_rankic",
                    "stock_tail_recall",
                    "selected_label_mean",
                    "selected_label_hit_rate",
                )
            }
            metrics["selected_label_mean_block_bootstrap"] = _moving_block_ci(
                [row["selected_label_mean"] for row in daily_rows],
                block_days=int(config["bootstrap_block_days"]),
                samples=int(config["bootstrap_samples"]),
                seed=int(config["bootstrap_seed"]),
            )
            cell_receipts.append(
                {
                    "cell_id": cell_id,
                    "sector_mode": sector_mode,
                    "stock_mode": stock_mode,
                    "gating": gating,
                    "deployability": "REALITY_BASELINE" if cell_id == "D2-RR" else ORACLE_MARKER,
                    "date_count": len(daily_rows),
                    "metrics": metrics,
                    "portfolio_status": PORTFOLIO_STATUS,
                }
            )
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "outcome": OUTCOME_COMPUTABLE,
        "reason_codes": ["qe_p0_d2_all_eight_signal_cells_computed"],
        "input_manifest_sha256": validated_input.manifest_sha256,
        "panel_sha256": validated_input.panel_sha256,
        "identities": validated_input.identities,
        "config": validated_input.config,
        "row_count": len(frame),
        "date_count": int(frame["datetime"].nunique()),
        "daily_sample_counts": daily_sample_counts,
        "cell_count": len(cell_receipts),
        "cells": cell_receipts,
        "portfolio_status": PORTFOLIO_STATUS,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return receipt


def _load_manifest(path: Path) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise D2InputError(
            "qe_p0_d2_manifest_file_invalid",
            f"manifest must be an existing non-symlink file: {path}",
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise D2InputError("qe_p0_d2_manifest_file_invalid", str(exc)) from exc
    return _require_mapping(
        value,
        reason_code="qe_p0_d2_manifest_invalid",
        label="input manifest",
    )


def _not_computable_receipt(error: D2InputError) -> Mapping[str, Any]:
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "outcome": OUTCOME_NOT_COMPUTABLE,
        "reason_codes": [error.reason_code],
        "detail": error.detail,
        "cell_count": 0,
        "cells": [],
        "portfolio_status": PORTFOLIO_STATUS,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return receipt


def _write_receipt(path: Path, receipt: Mapping[str, Any], *, protected_inputs: Sequence[Path]) -> None:
    resolved_output = path.resolve(strict=False)
    if any(resolved_output == item.resolve(strict=False) for item in protected_inputs):
        raise D2InputError("qe_p0_d2_output_overlaps_input", "output must not replace an input")
    if path.exists() and path.is_symlink():
        raise D2InputError("qe_p0_d2_output_symlink_forbidden", f"output is a symlink: {path}")
    if not path.parent.is_dir() or path.parent.is_symlink():
        raise D2InputError(
            "qe_p0_d2_output_parent_invalid",
            f"output parent must be an existing non-symlink directory: {path.parent}",
        )
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(_canonical_json_bytes(receipt) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute the file-only P0-D2 sector oracle signal ceiling.")
    parser.add_argument("--input-manifest", type=Path, required=True)
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    protected = (args.input_manifest, args.panel)
    try:
        manifest = _load_manifest(args.input_manifest)
        validated = validate_input_manifest(manifest, panel_path=args.panel)
        try:
            panel = pd.read_parquet(args.panel, columns=list(PANEL_COLUMNS))
        except Exception as exc:
            raise D2InputError("qe_p0_d2_panel_parquet_invalid", str(exc)) from exc
        receipt = evaluate_panel(panel, validated_input=validated)
        exit_code = 0
    except D2InputError as exc:
        receipt = _not_computable_receipt(exc)
        exit_code = 2
    try:
        _write_receipt(args.output, receipt, protected_inputs=protected)
    except D2InputError as exc:
        print(f"outcome={OUTCOME_NOT_COMPUTABLE} reason_code={exc.reason_code} detail={exc.detail}")
        return 2
    print(
        f"outcome={receipt['outcome']} reason_code={receipt['reason_codes'][0]} "
        f"receipt_sha256={receipt['receipt_sha256']} output={args.output}"
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
