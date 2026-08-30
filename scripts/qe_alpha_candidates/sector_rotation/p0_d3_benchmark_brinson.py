#!/usr/bin/env python
"""File-only benchmark-relative and Brinson-Fachler attribution for QE P0-D3."""

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


INPUT_SCHEMA = "qe_p0_d3_brinson_input_v1"
RECEIPT_SCHEMA = "qe_p0_d3_brinson_receipt_v1"
OUTCOME_RECONCILED = "ABSOLUTE_ACTIVE_BRINSON_RECONCILED"
OUTCOME_NOT_COMPUTABLE = "NOT_COMPUTABLE"
PANEL_COLUMNS = (
    "datetime",
    "l2_code_id",
    "portfolio_weight",
    "benchmark_weight",
    "portfolio_sector_return",
    "benchmark_sector_return",
)
IDENTITY_KEYS = (
    "dataset",
    "taxonomy",
    "portfolio_holdings",
    "benchmark",
    "execution_contract",
)
CONFIG_KEYS = (
    "annualization_days",
    "weight_tolerance",
    "reconciliation_tolerance",
    "bootstrap_block_days",
    "bootstrap_samples",
    "bootstrap_seed",
    "max_rows",
    "max_file_bytes",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
HARD_MAX_ROWS = 1_000_000
HARD_MAX_FILE_BYTES = 2 * 1024 * 1024 * 1024
HARD_MAX_BOOTSTRAP_SAMPLES = 10_000


@dataclass(frozen=True)
class D3InputError(ValueError):
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
        raise D3InputError("qe_p0_d3_json_not_canonical", str(exc)) from exc
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
        raise D3InputError(reason_code, f"{label} must be a JSON object")
    return value


def _require_text(value: Any, *, reason_code: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise D3InputError(reason_code, f"{label} must be a non-empty string")
    return value.strip()


def _require_sha(value: Any, *, reason_code: str, label: str) -> str:
    text = _require_text(value, reason_code=reason_code, label=label)
    if not SHA256_RE.fullmatch(text):
        raise D3InputError(reason_code, f"{label} must be a lowercase SHA256")
    return text


def _require_int(value: Any, *, label: str, minimum: int = 1, maximum: int | None = None) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        bound = f"[{minimum}, {maximum}]" if maximum is not None else f">= {minimum}"
        raise D3InputError("qe_p0_d3_config_invalid", f"{label} must be an integer in {bound}")
    return value


def _require_float(
    value: Any,
    *,
    label: str,
    minimum_exclusive: float = 0.0,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise D3InputError("qe_p0_d3_config_invalid", f"{label} must be numeric")
    result = float(value)
    if (
        not math.isfinite(result)
        or result <= minimum_exclusive
        or (maximum is not None and result > maximum)
    ):
        raise D3InputError(
            "qe_p0_d3_config_invalid",
            f"{label} must be finite, > {minimum_exclusive}, and <= {maximum}",
        )
    return result


def validate_input_manifest(payload: Any, *, panel_path: Path) -> ValidatedInput:
    manifest = _require_mapping(
        payload,
        reason_code="qe_p0_d3_manifest_invalid",
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
        raise D3InputError(
            "qe_p0_d3_manifest_fields_invalid",
            f"manifest must contain exactly {sorted(expected_fields)}",
        )
    if manifest.get("schema_version") != INPUT_SCHEMA:
        raise D3InputError(
            "qe_p0_d3_manifest_schema_invalid",
            f"schema_version must be {INPUT_SCHEMA}",
        )
    supplied_manifest_sha = _require_sha(
        manifest.get("manifest_sha256"),
        reason_code="qe_p0_d3_manifest_sha_invalid",
        label="manifest_sha256",
    )
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256")
    calculated_manifest_sha = canonical_sha256(unsigned)
    if supplied_manifest_sha != calculated_manifest_sha:
        raise D3InputError(
            "qe_p0_d3_manifest_sha_mismatch",
            f"supplied={supplied_manifest_sha} calculated={calculated_manifest_sha}",
        )
    if panel_path.is_symlink() or not panel_path.is_file():
        raise D3InputError(
            "qe_p0_d3_panel_file_invalid",
            f"panel must be an existing non-symlink file: {panel_path}",
        )
    if panel_path.stat().st_size > HARD_MAX_FILE_BYTES:
        raise D3InputError(
            "qe_p0_d3_resource_limit_exceeded",
            f"panel exceeds hard byte limit={HARD_MAX_FILE_BYTES}",
        )
    supplied_panel_sha = _require_sha(
        manifest.get("panel_sha256"),
        reason_code="qe_p0_d3_panel_sha_invalid",
        label="panel_sha256",
    )
    calculated_panel_sha = file_sha256(panel_path)
    if supplied_panel_sha != calculated_panel_sha:
        raise D3InputError(
            "qe_p0_d3_panel_sha_mismatch",
            f"supplied={supplied_panel_sha} calculated={calculated_panel_sha}",
        )
    identities = _require_mapping(
        manifest.get("identities"),
        reason_code="qe_p0_d3_identity_invalid",
        label="identities",
    )
    if set(identities) != set(IDENTITY_KEYS):
        raise D3InputError(
            "qe_p0_d3_identity_set_invalid",
            f"identities must contain exactly {list(IDENTITY_KEYS)}",
        )
    normalized_identities: dict[str, Mapping[str, str]] = {}
    for key in IDENTITY_KEYS:
        identity = _require_mapping(
            identities[key],
            reason_code="qe_p0_d3_identity_invalid",
            label=f"identities.{key}",
        )
        if set(identity) != {"identity", "sha256"}:
            raise D3InputError(
                "qe_p0_d3_identity_fields_invalid",
                f"identities.{key} must contain exactly identity/sha256",
            )
        normalized_identities[key] = {
            "identity": _require_text(
                identity.get("identity"),
                reason_code="qe_p0_d3_identity_invalid",
                label=f"identities.{key}.identity",
            ),
            "sha256": _require_sha(
                identity.get("sha256"),
                reason_code="qe_p0_d3_identity_sha_invalid",
                label=f"identities.{key}.sha256",
            ),
        }
    config = _require_mapping(
        manifest.get("config"),
        reason_code="qe_p0_d3_config_invalid",
        label="config",
    )
    if set(config) != set(CONFIG_KEYS):
        raise D3InputError(
            "qe_p0_d3_config_fields_invalid",
            f"config must contain exactly {list(CONFIG_KEYS)}",
        )
    normalized_config = {
        "annualization_days": _require_int(
            config["annualization_days"], label="config.annualization_days", maximum=366
        ),
        "weight_tolerance": _require_float(
            config["weight_tolerance"], label="config.weight_tolerance", maximum=1e-3
        ),
        "reconciliation_tolerance": _require_float(
            config["reconciliation_tolerance"],
            label="config.reconciliation_tolerance",
            maximum=1e-6,
        ),
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
        raise D3InputError(
            "qe_p0_d3_resource_limit_exceeded",
            f"panel bytes exceed max_file_bytes={normalized_config['max_file_bytes']}",
        )
    try:
        parquet_file = pq.ParquetFile(panel_path)
        metadata = parquet_file.metadata
    except Exception as exc:
        raise D3InputError("qe_p0_d3_panel_parquet_invalid", str(exc)) from exc
    parquet_columns = tuple(parquet_file.schema_arrow.names)
    if set(parquet_columns) != set(PANEL_COLUMNS) or len(parquet_columns) != len(PANEL_COLUMNS):
        raise D3InputError(
            "qe_p0_d3_panel_columns_invalid",
            f"Parquet columns must be exactly {list(PANEL_COLUMNS)}; observed={list(parquet_columns)}",
        )
    if metadata is None or metadata.num_rows > normalized_config["max_rows"]:
        rows = None if metadata is None else metadata.num_rows
        raise D3InputError(
            "qe_p0_d3_resource_limit_exceeded",
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
        raise D3InputError("qe_p0_d3_panel_columns_invalid", f"missing={missing} extra={extra}")
    frame = panel.loc[:, PANEL_COLUMNS].copy()
    try:
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="raise")
    except Exception as exc:
        raise D3InputError("qe_p0_d3_datetime_invalid", str(exc)) from exc
    if frame["datetime"].dt.tz is not None or not frame["datetime"].eq(frame["datetime"].dt.normalize()).all():
        raise D3InputError("qe_p0_d3_datetime_invalid", "datetime must be timezone-naive daily midnight")
    if frame[["datetime", "l2_code_id"]].duplicated().any():
        raise D3InputError("qe_p0_d3_panel_duplicate_key", "datetime/l2_code_id keys must be unique")
    numeric_columns = PANEL_COLUMNS[1:]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if not np.isfinite(frame.loc[:, numeric_columns].to_numpy(dtype=float)).all():
        raise D3InputError("qe_p0_d3_numeric_invalid", "all numeric fields must be finite")
    if not frame["l2_code_id"].mod(1).eq(0).all() or not frame["l2_code_id"].gt(0).all():
        raise D3InputError("qe_p0_d3_taxonomy_invalid", "l2_code_id must be a positive integer")
    frame["l2_code_id"] = frame["l2_code_id"].astype("int32")
    for weight_column in ("portfolio_weight", "benchmark_weight"):
        if not frame[weight_column].between(0.0, 1.0, inclusive="both").all():
            raise D3InputError(
                "qe_p0_d3_weight_invalid",
                f"{weight_column} must be in [0, 1]",
            )
    for return_column in ("portfolio_sector_return", "benchmark_sector_return"):
        if not frame[return_column].gt(-1.0).all():
            raise D3InputError(
                "qe_p0_d3_return_invalid",
                f"{return_column} must be greater than -1",
            )
    tolerance = float(config["weight_tolerance"])
    weight_sums = frame.groupby("datetime", sort=True)[
        ["portfolio_weight", "benchmark_weight"]
    ].sum()
    if not np.allclose(weight_sums.to_numpy(dtype=float), 1.0, atol=tolerance, rtol=0.0):
        raise D3InputError(
            "qe_p0_d3_weight_sum_mismatch",
            "portfolio and benchmark weights must each sum to one on every date",
        )
    sector_counts = frame.groupby("datetime", sort=True)["l2_code_id"].nunique()
    if sector_counts.empty or sector_counts.lt(2).any():
        raise D3InputError(
            "qe_p0_d3_sector_coverage_insufficient",
            "each date requires at least two explicit sector rows",
        )
    if len(frame) > int(config["max_rows"]):
        raise D3InputError("qe_p0_d3_resource_limit_exceeded", "loaded rows exceed max_rows")
    return frame.sort_values(["datetime", "l2_code_id"], kind="mergesort").reset_index(drop=True)


def _moving_block_ci(
    values: Sequence[float],
    *,
    block_days: int,
    samples: int,
    seed: int,
) -> Mapping[str, float]:
    array = np.asarray(values, dtype=float)
    if len(array) == 0 or not np.isfinite(array).all():
        raise D3InputError("qe_p0_d3_bootstrap_input_invalid", "bootstrap values must be finite")
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


def _daily_attribution(group: pd.DataFrame) -> Mapping[str, float]:
    portfolio_return = float((group["portfolio_weight"] * group["portfolio_sector_return"]).sum())
    benchmark_return = float((group["benchmark_weight"] * group["benchmark_sector_return"]).sum())
    active_return = portfolio_return - benchmark_return
    allocation = float(
        (
            (group["portfolio_weight"] - group["benchmark_weight"])
            * (group["benchmark_sector_return"] - benchmark_return)
        ).sum()
    )
    selection = float(
        (group["benchmark_weight"] * (group["portfolio_sector_return"] - group["benchmark_sector_return"])).sum()
    )
    interaction = float(
        (
            (group["portfolio_weight"] - group["benchmark_weight"])
            * (group["portfolio_sector_return"] - group["benchmark_sector_return"])
        ).sum()
    )
    residual = active_return - allocation - selection - interaction
    return {
        "portfolio_return": portfolio_return,
        "benchmark_return": benchmark_return,
        "active_return": active_return,
        "allocation": allocation,
        "selection": selection,
        "interaction": interaction,
        "reconciliation_residual": residual,
    }


def evaluate_panel(panel: pd.DataFrame, *, validated_input: ValidatedInput) -> Mapping[str, Any]:
    frame = validate_panel(panel, config=validated_input.config)
    daily_rows: list[Mapping[str, Any]] = []
    daily_sample_counts: dict[str, Mapping[str, int]] = {}
    for current_date, group in frame.groupby("datetime", sort=True):
        date_key = pd.Timestamp(current_date).date().isoformat()
        row = {"date": date_key, **_daily_attribution(group)}
        daily_rows.append(row)
        daily_sample_counts[date_key] = {
            "rows": int(len(group)),
            "sectors": int(group["l2_code_id"].nunique()),
        }
    tolerance = float(validated_input.config["reconciliation_tolerance"])
    max_residual = max(abs(float(row["reconciliation_residual"])) for row in daily_rows)
    if max_residual > tolerance:
        raise D3InputError(
            "qe_p0_d3_brinson_reconciliation_failed",
            f"max residual={max_residual} exceeds tolerance={tolerance}",
        )
    portfolio = np.asarray([row["portfolio_return"] for row in daily_rows], dtype=float)
    benchmark = np.asarray([row["benchmark_return"] for row in daily_rows], dtype=float)
    active = portfolio - benchmark
    if len(active) < 3:
        raise D3InputError("qe_p0_d3_date_coverage_insufficient", "at least three dates are required")
    benchmark_variance = float(np.var(benchmark, ddof=1))
    if benchmark_variance <= 0:
        raise D3InputError("qe_p0_d3_benchmark_variance_zero", "benchmark variance must be positive")
    beta = float(np.cov(portfolio, benchmark, ddof=1)[0, 1] / benchmark_variance)
    annualization_days = int(validated_input.config["annualization_days"])
    tracking_error = float(np.std(active, ddof=1) * math.sqrt(annualization_days))
    if tracking_error <= 0:
        raise D3InputError("qe_p0_d3_tracking_error_zero", "tracking error must be positive")
    information_ratio = float(np.mean(active) * annualization_days / tracking_error)
    bootstrap_kwargs = {
        "block_days": int(validated_input.config["bootstrap_block_days"]),
        "samples": int(validated_input.config["bootstrap_samples"]),
        "seed": int(validated_input.config["bootstrap_seed"]),
    }
    effects = {
        name: {
            "sum": float(sum(float(row[name]) for row in daily_rows)),
            "daily_mean_block_bootstrap": _moving_block_ci(
                [float(row[name]) for row in daily_rows],
                **bootstrap_kwargs,
            ),
        }
        for name in ("allocation", "selection", "interaction")
    }
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "outcome": OUTCOME_RECONCILED,
        "reason_codes": ["qe_p0_d3_absolute_active_brinson_reconciled"],
        "input_manifest_sha256": validated_input.manifest_sha256,
        "panel_sha256": validated_input.panel_sha256,
        "identities": validated_input.identities,
        "config": validated_input.config,
        "row_count": len(frame),
        "date_count": len(daily_rows),
        "daily_sample_counts": daily_sample_counts,
        "absolute_and_active": {
            "portfolio_cumulative_return": float(np.prod(1.0 + portfolio) - 1.0),
            "benchmark_cumulative_return": float(np.prod(1.0 + benchmark) - 1.0),
            "active_cumulative_return_difference": float(
                np.prod(1.0 + portfolio) - np.prod(1.0 + benchmark)
            ),
            "active_arithmetic_sum": float(active.sum()),
            "active_daily_mean_block_bootstrap": _moving_block_ci(active.tolist(), **bootstrap_kwargs),
            "beta": beta,
            "tracking_error_annualized": tracking_error,
            "information_ratio_annualized": information_ratio,
        },
        "brinson": {
            "effects": effects,
            "max_abs_daily_reconciliation_residual": max_residual,
            "reconciliation_tolerance": tolerance,
        },
        "daily_attribution_sha256": canonical_sha256(daily_rows),
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return receipt


def _load_manifest(path: Path) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise D3InputError(
            "qe_p0_d3_manifest_file_invalid",
            f"manifest must be an existing non-symlink file: {path}",
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise D3InputError("qe_p0_d3_manifest_file_invalid", str(exc)) from exc
    return _require_mapping(
        value,
        reason_code="qe_p0_d3_manifest_invalid",
        label="input manifest",
    )


def _not_computable_receipt(error: D3InputError) -> Mapping[str, Any]:
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "outcome": OUTCOME_NOT_COMPUTABLE,
        "reason_codes": [error.reason_code],
        "detail": error.detail,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return receipt


def _write_receipt(path: Path, receipt: Mapping[str, Any], *, protected_inputs: Sequence[Path]) -> None:
    resolved_output = path.resolve(strict=False)
    if any(resolved_output == item.resolve(strict=False) for item in protected_inputs):
        raise D3InputError("qe_p0_d3_output_overlaps_input", "output must not replace an input")
    if path.exists() and path.is_symlink():
        raise D3InputError("qe_p0_d3_output_symlink_forbidden", f"output is a symlink: {path}")
    if not path.parent.is_dir() or path.parent.is_symlink():
        raise D3InputError(
            "qe_p0_d3_output_parent_invalid",
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
    parser = argparse.ArgumentParser(description="Compute file-only QE P0-D3 benchmark and Brinson attribution.")
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
            raise D3InputError("qe_p0_d3_panel_parquet_invalid", str(exc)) from exc
        receipt = evaluate_panel(panel, validated_input=validated)
        exit_code = 0
    except D3InputError as exc:
        receipt = _not_computable_receipt(exc)
        exit_code = 2
    try:
        _write_receipt(args.output, receipt, protected_inputs=protected)
    except D3InputError as exc:
        print(f"outcome={OUTCOME_NOT_COMPUTABLE} reason_code={exc.reason_code} detail={exc.detail}")
        return 2
    print(
        f"outcome={receipt['outcome']} reason_code={receipt['reason_codes'][0]} "
        f"receipt_sha256={receipt['receipt_sha256']} output={args.output}"
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
