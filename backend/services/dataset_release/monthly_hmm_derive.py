"""Generate manifest-bound HMM coefficients from one sealed monthly release.

The executor is deliberately file-only.  It invokes the existing formal
``precompute_hmm_coefficients.py`` producer with hash-pinned candidate files,
a frozen model and a frozen config.  The producer output is validated and
canonicalised into the candidate's ``derived`` directory; it never fits a
model, opens a database, or resolves a runtime dataset fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
from typing import Any, Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_official_adapters import (
    DeriveExecution,
    DerivedAsset,
    StageWorkload,
)
from .monthly_worker import ProducerContext


SHARED_HMM_COEFFICIENT_SCHEMA = "aistock_shared_hmm_coefficients_v1"
FROZEN_HMM_INPUT_SCHEMA = "qe_hmm_frozen_input_v1"
MARKET_VOLUME_DEFINITION = "sum_market_sw_daily_vol_all_rows_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_REQUIRED_INPUTS = {
    "sector_data_h5": "factor_bundle/sector_data.h5",
    "index_daily_h5": "index_context/index_daily.h5",
    "sector_code_map_json": "components/sector_context_candidate_v1/sector_code_map.json",
    "market_context_parquet": "components/sector_context_candidate_v1/market_context.parquet",
    "sector_membership_spans_parquet": ("components/sector_context_candidate_v1/sector_membership_spans.parquet"),
    "sector_quote_availability_json": ("components/sector_context_candidate_v1/sector_quote_availability.json"),
}


class MonthlyHMMDeriveError(RuntimeError):
    """Frozen HMM derivation did not close its file or output identity."""


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _plain_file(path: Path, *, root: Path | None = None, label: str) -> Path:
    requested = path.absolute()
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise MonthlyHMMDeriveError(f"{label} is unavailable") from exc
    if _is_link(requested) or not resolved.is_file():
        raise MonthlyHMMDeriveError(f"{label} must be a regular non-link file")
    if root is not None:
        resolved_root = root.resolve(strict=True)
        try:
            relative = requested.relative_to(resolved_root)
        except ValueError as exc:
            raise MonthlyHMMDeriveError(f"{label} escapes the candidate") from exc
        current = resolved_root
        for part in relative.parts:
            current /= part
            if _is_link(current):
                raise MonthlyHMMDeriveError(f"{label} path chain contains a link")
        if not resolved.is_relative_to(resolved_root):
            raise MonthlyHMMDeriveError(f"{label} escapes the candidate")
    return resolved


def _read_json(path: Path, *, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyHMMDeriveError(f"{label} is not readable JSON") from exc
    if not isinstance(value, Mapping):
        raise MonthlyHMMDeriveError(f"{label} must be an object")
    return value


def _write_canonical_exclusive(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise MonthlyHMMDeriveError(f"HMM derived target already exists: {path.name}") from exc
    return path


def _write_canonical_or_identical(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        try:
            if path.read_bytes() == payload and not _is_link(path):
                return path
        except OSError:
            pass
        raise MonthlyHMMDeriveError(f"existing HMM derived target bytes differ: {path.name}") from exc
    return path


@dataclass(frozen=True, slots=True)
class HMMCoefficientProduct:
    asset_id: str
    preset_key: str
    preset_coefficients: Mapping[str, float]
    test_start: date
    backtest_lag_trade_days: int = 1

    def __post_init__(self) -> None:
        if re.fullmatch(r"[a-z0-9][a-z0-9_.-]{1,95}", self.asset_id) is None:
            raise ValueError("HMM coefficient asset_id is invalid")
        if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.preset_key) is None:
            raise ValueError("HMM coefficient preset_key is invalid")
        if not self.preset_coefficients or any(
            not str(key).strip()
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            for key, value in self.preset_coefficients.items()
        ):
            raise ValueError("HMM preset coefficients are invalid")
        if type(self.backtest_lag_trade_days) is not int or self.backtest_lag_trade_days < 1:
            raise ValueError("HMM backtest lag must be a positive trading-day count")


@dataclass(frozen=True, slots=True)
class FrozenHMMCoefficientAuthority:
    authority_id: str
    model_path: Path
    model_sha256: str
    config_path: Path
    config_sha256: str
    script_sha256: str
    products: tuple[HMMCoefficientProduct, ...]

    def __post_init__(self) -> None:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{1,127}", self.authority_id) is None:
            raise ValueError("HMM derivation authority_id is invalid")
        if not self.model_path.is_absolute() or not self.config_path.is_absolute():
            raise ValueError("HMM derivation authority paths must be absolute")
        ensure_sha256(self.model_sha256, field="model_sha256")
        ensure_sha256(self.config_sha256, field="config_sha256")
        ensure_sha256(self.script_sha256, field="script_sha256")
        if not self.products or len({item.asset_id for item in self.products}) != len(self.products):
            raise ValueError("HMM coefficient products are empty or duplicated")
        filenames = {(item.preset_key, item.test_start, item.backtest_lag_trade_days) for item in self.products}
        if len(filenames) != len(self.products):
            raise ValueError("HMM coefficient output filenames are ambiguous")

    @property
    def authority_sha256(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "authority_id": self.authority_id,
                    "model_sha256": self.model_sha256,
                    "config_sha256": self.config_sha256,
                    "script_sha256": self.script_sha256,
                    "products": [
                        {
                            "asset_id": item.asset_id,
                            "preset_key": item.preset_key,
                            "preset_coefficients": dict(item.preset_coefficients),
                            "test_start": item.test_start.isoformat(),
                            "backtest_lag_trade_days": item.backtest_lag_trade_days,
                        }
                        for item in self.products
                    ],
                }
            )
        ).hexdigest()


class FrozenHMMCoefficientProcess(Protocol):
    script_path: Path
    script_sha256: str

    def execution_path(self, path: Path) -> str: ...

    def run(self, payload: Mapping[str, Any]) -> bytes: ...


@dataclass(frozen=True, slots=True)
class LocalPythonHMMCoefficientProcess:
    python_executable: Path
    script_path: Path
    project_root: Path
    timeout_seconds: int = 900

    def __post_init__(self) -> None:
        if not all(path.is_absolute() for path in (self.python_executable, self.script_path, self.project_root)):
            raise ValueError("local HMM process paths must be absolute")
        _plain_file(self.python_executable, label="HMM Python executable")
        _plain_file(self.script_path, label="HMM coefficient producer")
        project = self.project_root.resolve(strict=True)
        if _is_link(self.project_root) or not project.is_dir():
            raise ValueError("HMM producer project root is unavailable")
        if type(self.timeout_seconds) is not int or self.timeout_seconds <= 0:
            raise ValueError("HMM producer timeout is invalid")

    @property
    def script_sha256(self) -> str:
        return _sha256(self.script_path)

    def execution_path(self, path: Path) -> str:
        return str(path.resolve(strict=True))

    def run(self, payload: Mapping[str, Any]) -> bytes:
        environment = {
            key: value for key, value in os.environ.items() if key in {"PATH", "SYSTEMROOT", "WINDIR", "PYTHONPATH"}
        }
        completed = subprocess.run(
            [str(self.python_executable), str(self.script_path)],
            cwd=self.project_root,
            env=environment,
            input=canonical_json_bytes(payload),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout_seconds,
            check=False,
        )
        if completed.returncode != 0:
            error = completed.stderr.decode("utf-8", errors="replace")[-4000:]
            raise MonthlyHMMDeriveError(f"formal HMM coefficient producer failed: {error}")
        return completed.stdout


@dataclass(frozen=True, slots=True)
class WSLPythonHMMCoefficientProcess:
    wsl_executable: Path
    distribution: str
    python_executable: str
    script_path: Path
    project_root: Path
    timeout_seconds: int = 900

    def __post_init__(self) -> None:
        if not all(path.is_absolute() for path in (self.wsl_executable, self.script_path, self.project_root)):
            raise ValueError("WSL HMM process paths must be absolute")
        _plain_file(self.wsl_executable, label="WSL executable")
        _plain_file(self.script_path, label="HMM coefficient producer")
        project = self.project_root.resolve(strict=True)
        if _is_link(self.project_root) or not project.is_dir():
            raise ValueError("HMM producer project root is unavailable")
        if re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", self.distribution) is None:
            raise ValueError("WSL distribution is invalid")
        executable = PurePosixPath(self.python_executable)
        if not executable.is_absolute() or ".." in executable.parts:
            raise ValueError("WSL HMM Python executable path is invalid")
        if type(self.timeout_seconds) is not int or self.timeout_seconds <= 0:
            raise ValueError("HMM producer timeout is invalid")

    @property
    def script_sha256(self) -> str:
        return _sha256(self.script_path)

    def execution_path(self, path: Path) -> str:
        resolved = path.resolve(strict=True)
        drive = resolved.drive.rstrip(":").lower()
        if len(drive) != 1 or not drive.isalpha():
            raise MonthlyHMMDeriveError("formal WSL HMM input is not on a mounted drive")
        tail = resolved.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    def run(self, payload: Mapping[str, Any]) -> bytes:
        command = [
            str(self.wsl_executable),
            "--distribution",
            self.distribution,
            "--exec",
            self.python_executable,
            self.execution_path(self.script_path),
        ]
        completed = subprocess.run(
            command,
            cwd=self.project_root,
            input=canonical_json_bytes(payload),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout_seconds,
            check=False,
        )
        if completed.returncode != 0:
            error = completed.stderr.decode("utf-8", errors="replace")[-4000:]
            raise MonthlyHMMDeriveError(f"formal WSL HMM coefficient producer failed: {error}")
        return completed.stdout


@dataclass(frozen=True, slots=True)
class MonthlyHMMCoefficientExecutor:
    authority: FrozenHMMCoefficientAuthority
    process: FrozenHMMCoefficientProcess
    artifact_root: Path

    def __post_init__(self) -> None:
        root = self.artifact_root.resolve(strict=True)
        if _is_link(self.artifact_root) or not root.is_dir():
            raise ValueError("HMM derivation artifact root is unavailable")
        if self.process.script_sha256 != self.authority.script_sha256:
            raise ValueError("HMM coefficient producer script identity differs")
        model = _plain_file(self.authority.model_path, label="frozen HMM model")
        config = _plain_file(self.authority.config_path, label="frozen HMM config")
        if _sha256(model) != self.authority.model_sha256:
            raise ValueError("frozen HMM model hash differs")
        if _sha256(config) != self.authority.config_sha256:
            raise ValueError("frozen HMM config hash differs")

    def contract_identity(self) -> Mapping[str, Any]:
        return {
            "schema_version": "aistock_monthly_hmm_derive_executor_v1",
            "authority_id": self.authority.authority_id,
            "authority_sha256": self.authority.authority_sha256,
            "model_sha256": self.authority.model_sha256,
            "config_sha256": self.authority.config_sha256,
            "producer_script_sha256": self.authority.script_sha256,
            "output_schema_version": SHARED_HMM_COEFFICIENT_SCHEMA,
        }

    def execute(
        self,
        context: ProducerContext,
        *,
        dataset_manifest_sha256: str,
    ) -> DeriveExecution:
        if context.stage != "DERIVE":
            raise MonthlyHMMDeriveError("HMM derivation executor received another stage")
        ensure_sha256(dataset_manifest_sha256, field="dataset_manifest_sha256")
        candidate = Path(str(context.plan.get("candidate_root") or ""))
        if not candidate.is_absolute():
            raise MonthlyHMMDeriveError("monthly HMM candidate root is invalid")
        candidate_root = candidate.resolve(strict=True)
        if _is_link(candidate) or not candidate_root.is_dir():
            raise MonthlyHMMDeriveError("monthly HMM candidate root is unavailable")
        manifest_path = _plain_file(
            candidate_root / "qe_dataset_manifest.json",
            root=candidate_root,
            label="monthly dataset manifest",
        )
        manifest = _read_json(manifest_path, label="monthly dataset manifest")
        if (
            manifest.get("dataset_manifest_sha256") != dataset_manifest_sha256
            or manifest.get("release_id") != context.plan.get("release_id")
            or manifest.get("revision") != context.plan.get("revision")
            or manifest.get("cutoff_trade_date") != context.plan.get("target_cutoff")
        ):
            raise MonthlyHMMDeriveError("monthly dataset manifest identity differs")
        unsigned_manifest = dict(manifest)
        unsigned_manifest.pop("dataset_manifest_sha256", None)
        if hashlib.sha256(canonical_json_bytes(unsigned_manifest)).hexdigest() != dataset_manifest_sha256:
            raise MonthlyHMMDeriveError("monthly dataset manifest canonical identity differs")
        components = manifest.get("components")
        if not isinstance(components, Mapping):
            raise MonthlyHMMDeriveError("monthly dataset manifest components are invalid")
        by_path = {str(raw.get("path") or ""): raw for raw in components.values() if isinstance(raw, Mapping)}
        if len(by_path) != len(components):
            raise MonthlyHMMDeriveError("monthly dataset manifest component paths are ambiguous")
        files: dict[str, dict[str, str]] = {}
        input_paths: list[Path] = []
        for key, relative in _REQUIRED_INPUTS.items():
            pin = by_path.get(relative)
            path = _plain_file(candidate_root / relative, root=candidate_root, label=key)
            actual = _sha256(path)
            if not isinstance(pin, Mapping) or pin.get("sha256") != actual or pin.get("size") != path.stat().st_size:
                raise MonthlyHMMDeriveError(f"monthly HMM input pin differs: {key}")
            files[key] = {"relative_path": relative, "sha256": actual}
            input_paths.append(path)

        calendar_path = _plain_file(
            candidate_root / "daily_bin/qlib/calendars/day.txt",
            root=candidate_root,
            label="monthly day calendar",
        )
        calendar_pin = by_path.get("daily_bin/qlib/calendars/day.txt")
        if (
            not isinstance(calendar_pin, Mapping)
            or calendar_pin.get("sha256") != _sha256(calendar_path)
            or calendar_pin.get("size") != calendar_path.stat().st_size
        ):
            raise MonthlyHMMDeriveError("monthly day calendar pin differs")
        try:
            calendar = [
                date.fromisoformat(line.strip())
                for line in calendar_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            cutoff = date.fromisoformat(str(context.plan.get("target_cutoff") or ""))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise MonthlyHMMDeriveError("monthly HMM calendar is invalid") from exc
        if not calendar or calendar != sorted(set(calendar)) or calendar[-1] != cutoff:
            raise MonthlyHMMDeriveError("monthly HMM calendar does not end at cutoff")

        bundle = {
            "schema_version": FROZEN_HMM_INPUT_SCHEMA,
            "dataset_root": self.process.execution_path(candidate_root),
            "dataset_identity": {
                "generation": context.plan.get("generation"),
                "release_id": context.plan.get("release_id"),
                "revision": context.plan.get("revision"),
                "cutoff": cutoff.isoformat(),
                "dataset_manifest_sha256": dataset_manifest_sha256,
                "dataset_manifest_file_sha256": _sha256(manifest_path),
            },
            "market_volume_definition": MARKET_VOLUME_DEFINITION,
            "files": files,
        }
        attempt_root = (
            self.artifact_root / "monthly" / context.operation_id / "derive-hmm" / f"attempt-{context.attempt}"
        )
        attempt_root.mkdir(parents=True, exist_ok=False)
        bundle_path = _write_canonical_exclusive(attempt_root / "frozen-input-bundle.json", bundle)
        derived_root = candidate_root / "derived"
        if derived_root.exists():
            if _is_link(derived_root) or not derived_root.is_dir():
                raise MonthlyHMMDeriveError("candidate derived root is invalid")
        else:
            derived_root.mkdir(parents=False, exist_ok=False)

        model = _plain_file(self.authority.model_path, label="frozen HMM model")
        config_path = _plain_file(self.authority.config_path, label="frozen HMM config")
        if (
            _sha256(model) != self.authority.model_sha256
            or _sha256(config_path) != self.authority.config_sha256
            or self.process.script_sha256 != self.authority.script_sha256
        ):
            raise MonthlyHMMDeriveError("frozen HMM derivation authority changed during execution")
        config = _read_json(config_path, label="frozen HMM config")
        assets: list[DerivedAsset] = []
        computed_rows = 0
        for product in self.authority.products:
            if len(calendar) <= product.backtest_lag_trade_days:
                raise MonthlyHMMDeriveError("monthly HMM calendar lacks backtest lag history")
            backtest_end = calendar[-1 - product.backtest_lag_trade_days]
            if product.test_start > backtest_end:
                raise MonthlyHMMDeriveError("monthly HMM product window is inverted")
            payload = {
                "model_path": self.process.execution_path(model),
                "model_sha256": self.authority.model_sha256,
                "test_start": product.test_start.isoformat(),
                "backtest_end": backtest_end.isoformat(),
                "preset_coeffs": dict(product.preset_coefficients),
                "preset_key": product.preset_key,
                "config_json": dict(config),
                "frozen_input_bundle": bundle,
            }
            raw = self.process.run(payload)
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MonthlyHMMDeriveError("formal HMM producer output is invalid JSON") from exc
            if not isinstance(value, dict):
                raise MonthlyHMMDeriveError("formal HMM producer output must be an object")
            if value.get("schema_version", SHARED_HMM_COEFFICIENT_SCHEMA) != (
                SHARED_HMM_COEFFICIENT_SCHEMA
            ) or value.get("dataset_manifest_sha256", dataset_manifest_sha256) != (dataset_manifest_sha256):
                raise MonthlyHMMDeriveError("formal HMM producer output contract differs")
            daily = value.get("daily_coefficients")
            if (
                value.get("model_sha256") != self.authority.model_sha256
                or value.get("dataset_identity") != bundle["dataset_identity"]
                or value.get("input_file_sha256") != {key: spec["sha256"] for key, spec in files.items()}
                or value.get("test_start") != product.test_start.isoformat()
                or value.get("backtest_end") != backtest_end.isoformat()
                or not isinstance(daily, dict)
                or not daily
            ):
                raise MonthlyHMMDeriveError("formal HMM producer output identity differs")
            expected_dates = {item.isoformat() for item in calendar if product.test_start <= item <= backtest_end}
            if set(daily) != expected_dates or any(
                not isinstance(row, Mapping)
                or not row
                or any(
                    isinstance(coefficient, bool) or not math.isfinite(float(coefficient))
                    for coefficient in row.values()
                )
                for row in daily.values()
            ):
                raise MonthlyHMMDeriveError("formal HMM coefficient grid is incomplete")
            value.update(
                {
                    "schema_version": SHARED_HMM_COEFFICIENT_SCHEMA,
                    "dataset_manifest_sha256": dataset_manifest_sha256,
                    "derivation_identity": {
                        "authority_id": self.authority.authority_id,
                        "authority_sha256": self.authority.authority_sha256,
                        "model_sha256": self.authority.model_sha256,
                        "config_sha256": self.authority.config_sha256,
                        "producer_script_sha256": self.authority.script_sha256,
                        "preset_key": product.preset_key,
                        "test_start": product.test_start.isoformat(),
                        "backtest_end": backtest_end.isoformat(),
                        "fit_performed": False,
                        "database_access": False,
                    },
                }
            )
            filename = (
                f"coefficients_{product.preset_key}_{product.test_start.isoformat()}_{backtest_end.isoformat()}.json"
            )
            output_path = _write_canonical_or_identical(derived_root / filename, value)
            assets.append(
                DerivedAsset(
                    product.asset_id,
                    output_path,
                    SHARED_HMM_COEFFICIENT_SCHEMA,
                )
            )
            computed_rows += sum(len(row) for row in daily.values())

        return DeriveExecution(
            assets=tuple(assets),
            input_artifacts=(
                manifest_path,
                calendar_path,
                *input_paths,
                model,
                config_path,
                self.process.script_path.resolve(strict=True),
                bundle_path,
            ),
            workload=StageWorkload(computed_rows=computed_rows),
        )


__all__: Sequence[str] = (
    "FROZEN_HMM_INPUT_SCHEMA",
    "FrozenHMMCoefficientAuthority",
    "FrozenHMMCoefficientProcess",
    "HMMCoefficientProduct",
    "LocalPythonHMMCoefficientProcess",
    "MARKET_VOLUME_DEFINITION",
    "MonthlyHMMCoefficientExecutor",
    "MonthlyHMMDeriveError",
    "SHARED_HMM_COEFFICIENT_SCHEMA",
    "WSLPythonHMMCoefficientProcess",
)
