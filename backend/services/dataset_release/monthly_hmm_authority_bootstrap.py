"""Create a frozen monthly HMM derivation authority from an approved artifact.

The bootstrap is deliberately separate from the monthly worker.  It packages
an already approved HMM model and coefficient artifact into a repository-
external, create-exclusive authority.  It never selects a model from a
database and never trains or mutates the source artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
from typing import Any, Mapping, Sequence
from uuid import uuid4

from .canonical import canonical_json_bytes
from .monthly_production import MONTHLY_HMM_AUTHORITY_SCHEMA


BOOTSTRAP_RECEIPT_SCHEMA = "aistock_monthly_hmm_authority_bootstrap_receipt_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_AUTHORITY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,127}$")
_ASSET_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{1,95}$")
_PRESET_KEY = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class MonthlyHMMAuthorityBootstrapError(ValueError):
    """The approved HMM inputs cannot be packaged without ambiguity."""


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _plain_path(path: Path, *, label: str, kind: str) -> Path:
    if not path.is_absolute():
        raise MonthlyHMMAuthorityBootstrapError(f"{label} must be absolute")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if _is_link(current):
            raise MonthlyHMMAuthorityBootstrapError(
                f"{label} path chain must not be linked"
            )
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise MonthlyHMMAuthorityBootstrapError(f"{label} is unavailable") from exc
    if kind == "file" and not resolved.is_file():
        raise MonthlyHMMAuthorityBootstrapError(f"{label} must be a plain file")
    if kind == "dir" and not resolved.is_dir():
        raise MonthlyHMMAuthorityBootstrapError(f"{label} must be a directory")
    return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyHMMAuthorityBootstrapError(f"{label} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise MonthlyHMMAuthorityBootstrapError(f"{label} must be a JSON object")
    return value


def _finite_coefficients(raw: object) -> dict[str, float]:
    if not isinstance(raw, Mapping) or not raw:
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact has no preset coefficients"
        )
    result: dict[str, float] = {}
    for key, value in raw.items():
        name = str(key).strip()
        if (
            not name
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise MonthlyHMMAuthorityBootstrapError(
                "approved coefficient artifact has invalid preset coefficients"
            )
        result[name] = float(value)
    return result


def _validate_daily_grid(
    raw: object,
    *,
    test_start: date,
    backtest_end: date,
) -> tuple[int, int]:
    if not isinstance(raw, Mapping) or not raw:
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact has no daily coefficient grid"
        )
    parsed_dates: list[date] = []
    coefficient_rows = 0
    for raw_date, row in raw.items():
        try:
            trade_date = date.fromisoformat(str(raw_date))
        except ValueError as exc:
            raise MonthlyHMMAuthorityBootstrapError(
                "approved coefficient artifact has an invalid trade date"
            ) from exc
        if not isinstance(row, Mapping) or not row:
            raise MonthlyHMMAuthorityBootstrapError(
                "approved coefficient artifact has an empty daily row"
            )
        for value in row.values():
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise MonthlyHMMAuthorityBootstrapError(
                    "approved coefficient artifact has a non-finite daily coefficient"
                )
        parsed_dates.append(trade_date)
        coefficient_rows += len(row)
    if (
        parsed_dates != sorted(set(parsed_dates))
        or parsed_dates[0] != test_start
        or parsed_dates[-1] != backtest_end
    ):
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact daily window differs"
        )
    return len(parsed_dates), coefficient_rows


def _validate_static_model(
    model: Mapping[str, Any],
    *,
    preset_coefficients: Mapping[str, float],
) -> int:
    if not model:
        raise MonthlyHMMAuthorityBootstrapError("approved HMM model is empty")
    known_labels: set[str] = set()
    for sector, row in model.items():
        if not str(sector).strip() or not isinstance(row, Mapping):
            raise MonthlyHMMAuthorityBootstrapError(
                "approved HMM model has an invalid sector row"
            )
        labels = row.get("state_labels")
        if not isinstance(labels, Mapping) or not labels:
            raise MonthlyHMMAuthorityBootstrapError(
                "automatic empty config is allowed only for state-label HMM models"
            )
        known_labels.update(str(value).strip() for value in labels.values())
    if not known_labels or set(preset_coefficients) != known_labels:
        raise MonthlyHMMAuthorityBootstrapError(
            "preset coefficients differ from the frozen model state labels"
        )
    return len(model)


def _write_exclusive(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


@dataclass(frozen=True, slots=True)
class MonthlyHMMAuthorityBootstrapResult:
    root: Path
    authority_path: Path
    receipt_path: Path
    authority_sha256: str
    receipt_sha256: str


def bootstrap_monthly_hmm_authority(
    *,
    source_coefficients: Path,
    model_path: Path,
    producer_script: Path,
    output_root: Path,
    project_root: Path,
    authority_id: str,
    asset_id: str,
    expected_dataset_manifest_sha256: str,
    backtest_lag_trade_days: int = 1,
    config_path: Path | None = None,
) -> MonthlyHMMAuthorityBootstrapResult:
    """Freeze one approved product into a worker-compatible authority package."""

    if _AUTHORITY_ID.fullmatch(authority_id) is None:
        raise MonthlyHMMAuthorityBootstrapError("authority_id is invalid")
    if _ASSET_ID.fullmatch(asset_id) is None:
        raise MonthlyHMMAuthorityBootstrapError("asset_id is invalid")
    if _SHA256.fullmatch(expected_dataset_manifest_sha256) is None:
        raise MonthlyHMMAuthorityBootstrapError(
            "expected dataset manifest SHA256 is invalid"
        )
    if type(backtest_lag_trade_days) is not int or backtest_lag_trade_days < 1:
        raise MonthlyHMMAuthorityBootstrapError(
            "backtest lag must be a positive trading-day count"
        )

    source = _plain_path(source_coefficients, label="source coefficients", kind="file")
    model_source = _plain_path(model_path, label="frozen HMM model", kind="file")
    script = _plain_path(producer_script, label="HMM coefficient producer", kind="file")
    project = _plain_path(project_root, label="project root", kind="dir")
    if not output_root.is_absolute():
        raise MonthlyHMMAuthorityBootstrapError("output root must be absolute")
    parent = _plain_path(output_root.parent, label="output parent", kind="dir")
    target = parent / output_root.name
    if target.exists():
        raise MonthlyHMMAuthorityBootstrapError("output root already exists")
    if target.is_relative_to(project):
        raise MonthlyHMMAuthorityBootstrapError(
            "monthly HMM authority package must be repository-external"
        )

    coefficient_value = _read_object(source, label="source coefficients")
    model_value = _read_object(model_source, label="frozen HMM model")
    model_sha = _sha256(model_source)
    if coefficient_value.get("model_sha256") != model_sha:
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact model SHA256 differs"
        )
    identity = coefficient_value.get("dataset_identity")
    if (
        not isinstance(identity, Mapping)
        or identity.get("dataset_manifest_sha256")
        != expected_dataset_manifest_sha256
    ):
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact dataset manifest differs"
        )
    preset_key = str(coefficient_value.get("preset_key") or "")
    if _PRESET_KEY.fullmatch(preset_key) is None:
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact preset key is invalid"
        )
    preset_coefficients = _finite_coefficients(
        coefficient_value.get("preset_coeffs")
    )
    try:
        test_start = date.fromisoformat(str(coefficient_value["test_start"]))
        backtest_end = date.fromisoformat(str(coefficient_value["backtest_end"]))
    except (KeyError, ValueError) as exc:
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact window is invalid"
        ) from exc
    if test_start > backtest_end:
        raise MonthlyHMMAuthorityBootstrapError(
            "approved coefficient artifact window is inverted"
        )
    date_count, coefficient_row_count = _validate_daily_grid(
        coefficient_value.get("daily_coefficients"),
        test_start=test_start,
        backtest_end=backtest_end,
    )

    if config_path is None:
        if coefficient_value.get("dynamic_coefficients") is not False:
            raise MonthlyHMMAuthorityBootstrapError(
                "dynamic HMM authority requires an explicit frozen config"
            )
        sector_count = _validate_static_model(
            model_value,
            preset_coefficients=preset_coefficients,
        )
        config_bytes = canonical_json_bytes({}) + b"\n"
        config_mode = "static_state_labels_empty_config_v1"
    else:
        supplied_config = _plain_path(
            config_path,
            label="frozen HMM config",
            kind="file",
        )
        _read_object(supplied_config, label="frozen HMM config")
        config_bytes = supplied_config.read_bytes()
        sector_count = len(model_value)
        config_mode = "explicit_frozen_config_v1"

    staging = parent / f".{target.name}.partial.{uuid4().hex}"
    staging.mkdir(parents=False, exist_ok=False)
    try:
        model_target = staging / "models.json"
        source_target = staging / "approved_coefficients.json"
        config_target = staging / "config.json"
        _write_exclusive(model_target, model_source.read_bytes())
        _write_exclusive(source_target, source.read_bytes())
        _write_exclusive(config_target, config_bytes)

        final_model = target / model_target.name
        final_config = target / config_target.name
        authority_value: dict[str, Any] = {
            "schema_version": MONTHLY_HMM_AUTHORITY_SCHEMA,
            "authority_id": authority_id,
            "model": {
                "path": str(final_model),
                "sha256": model_sha,
                "size": model_target.stat().st_size,
            },
            "config": {
                "path": str(final_config),
                "sha256": _sha256(config_target),
                "size": config_target.stat().st_size,
            },
            "producer_script_sha256": _sha256(script),
            "products": [
                {
                    "asset_id": asset_id,
                    "preset_key": preset_key,
                    "preset_coefficients": preset_coefficients,
                    "test_start": test_start.isoformat(),
                    "backtest_lag_trade_days": backtest_lag_trade_days,
                }
            ],
        }
        authority_path = staging / "monthly_hmm_coefficient_authority.json"
        _write_exclusive(
            authority_path,
            canonical_json_bytes(authority_value) + b"\n",
        )
        receipt_value = {
            "schema_version": BOOTSTRAP_RECEIPT_SCHEMA,
            "authority_file": authority_path.name,
            "authority_file_sha256": _sha256(authority_path),
            "authority_id": authority_id,
            "asset_id": asset_id,
            "source_coefficient_file": source_target.name,
            "source_coefficient_sha256": _sha256(source_target),
            "source_dataset_identity": dict(identity),
            "expected_dataset_manifest_sha256": expected_dataset_manifest_sha256,
            "model_sha256": model_sha,
            "model_sector_count": sector_count,
            "config_sha256": _sha256(config_target),
            "config_mode": config_mode,
            "producer_script_sha256": _sha256(script),
            "preset_key": preset_key,
            "preset_coefficients": preset_coefficients,
            "test_start": test_start.isoformat(),
            "backtest_end": backtest_end.isoformat(),
            "backtest_lag_trade_days": backtest_lag_trade_days,
            "trade_date_count": date_count,
            "coefficient_row_count": coefficient_row_count,
            "database_read_performed": False,
            "database_write_performed": False,
            "candidate_write_performed": False,
            "profile_write_performed": False,
            "training_started": False,
            "experiment_started": False,
            "runtime_action_performed": False,
        }
        receipt_path = staging / "bootstrap_receipt.json"
        _write_exclusive(receipt_path, canonical_json_bytes(receipt_value) + b"\n")
        os.replace(staging, target)
    except BaseException:
        if staging.exists() and staging.parent.resolve(strict=True) == parent:
            shutil.rmtree(staging)
        raise

    final_authority = target / authority_path.name
    final_receipt = target / receipt_path.name
    return MonthlyHMMAuthorityBootstrapResult(
        root=target,
        authority_path=final_authority,
        receipt_path=final_receipt,
        authority_sha256=_sha256(final_authority),
        receipt_sha256=_sha256(final_receipt),
    )


__all__: Sequence[str] = (
    "BOOTSTRAP_RECEIPT_SCHEMA",
    "MonthlyHMMAuthorityBootstrapError",
    "MonthlyHMMAuthorityBootstrapResult",
    "bootstrap_monthly_hmm_authority",
)
