"""Validate one hash-pinned direct-v2 QE dataset binding before qrun.

The validator is copied into an experiment workspace.  It reads only files
named by ``qe_direct_v2_dataset_binding.json`` and never opens a database or
discovers a fallback dataset.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


BINDING_FILE = "qe_direct_v2_dataset_binding.json"
BINDING_SCHEMA = "qe_direct_v2_dataset_binding_v2"
BINDING_FIELDS = {
    "schema_version",
    "release_id",
    "cutoff",
    "candidate_root",
    "provider_uri_day",
    "provider_uri_1min",
    "factor_data_dir",
    "index_context_path",
    "suspend_data_dir",
    "factor_meta",
    "factor_meta_sha256",
    "day_pins",
    "minute_pins",
    "selection_pins",
    "index_pins",
    "suspend_pins",
}
INDEX_CODES = (
    "000001.SH",
    "000016.SH",
    "000300.SH",
    "000688.SH",
    "000852.SH",
    "000905.SH",
    "000985.CSI",
    "399001.SZ",
    "399006.SZ",
    "399102.SZ",
    "399107.SZ",
    "932000.CSI",
)


class DirectV2DatasetValidationError(RuntimeError):
    """Raised when a selected direct-v2 component drifts from its binding."""


def _fail(code: str, detail: str) -> DirectV2DatasetValidationError:
    return DirectV2DatasetValidationError(f"reason_code={code}: {detail}")


def _load_json(path: Path, *, code: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _fail(code, f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise _fail(code, f"{path} is not a JSON object")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise _fail("qe_direct_v2_file_unreadable", f"cannot read {path}: {exc}") from exc
    return digest.hexdigest()


def _require_file(path: Path, expected_sha256: str) -> None:
    if path.is_symlink():
        raise _fail("qe_direct_v2_symlink_forbidden", f"required file is a symlink: {path}")
    if not path.is_file():
        raise _fail("qe_direct_v2_file_missing", f"required file is missing: {path}")
    actual = _sha256(path)
    if actual != expected_sha256:
        raise _fail(
            "qe_direct_v2_hash_mismatch",
            f"path={path} expected={expected_sha256} actual={actual}",
        )


def _require_meta(path: Path, expected: Mapping[str, Any]) -> dict[str, Any]:
    actual = _load_json(path, code="qe_direct_v2_metadata_unreadable")
    mismatches = {
        key: {"expected": value, "actual": actual.get(key)}
        for key, value in expected.items()
        if actual.get(key) != value
    }
    if mismatches:
        raise _fail(
            "qe_direct_v2_identity_mismatch",
            json.dumps(mismatches, ensure_ascii=True, sort_keys=True),
        )
    return actual


def _validate_qlib_component(root: Path, pins: Mapping[str, str], *, freq: str) -> None:
    calendar_name = "day.txt" if freq == "day" else "1min.txt"
    _require_file(root / "instruments" / "all.txt", pins["instruments_sha256"])
    _require_file(root / "calendars" / calendar_name, pins["calendar_sha256"])
    meta_path = root / "meta_export.json"
    _require_file(meta_path, pins["meta_export_sha256"])
    _require_meta(
        meta_path,
        {
            "snapshot_id": pins["snapshot_id"],
            "universe_key": pins["universe_key"],
            "rule_version": pins["rule_version"],
        },
    )


def _validate_selection_universe(
    root: Path,
    pins: Mapping[str, str],
    *,
    cutoff: str,
) -> None:
    required_fields = {
        "stock_pool",
        "instruments_sha256",
        "benchmark_code",
        "benchmark_instruments_sha256",
    }
    if not isinstance(pins, Mapping) or set(pins) != required_fields:
        raise _fail("qe_direct_v2_selection_contract_invalid", "selection fields differ")
    if pins.get("stock_pool") != "stock_universe" or pins.get("benchmark_code") != "000300.SH":
        raise _fail("qe_direct_v2_selection_contract_invalid", "selection identity differs")
    instruments = root / "instruments"
    all_path = instruments / "all.txt"
    stock_path = instruments / "stock_universe.txt"
    benchmark_path = instruments / "benchmark.txt"
    _require_file(stock_path, str(pins["instruments_sha256"]))
    _require_file(benchmark_path, str(pins["benchmark_instruments_sha256"]))
    try:
        all_lines = all_path.read_text(encoding="utf-8").splitlines()
        stock_lines = stock_path.read_text(encoding="utf-8").splitlines()
        benchmark_lines = benchmark_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise _fail("qe_direct_v2_file_unreadable", f"cannot read instruments: {exc}") from exc
    benchmark_line = f"000300.SH\t2018-08-01\t{cutoff}"
    if benchmark_lines != [benchmark_line]:
        raise _fail("qe_direct_v2_benchmark_universe_invalid", "benchmark.txt differs")
    if benchmark_line in stock_lines:
        raise _fail("qe_direct_v2_selection_contains_benchmark", "000300.SH is selection eligible")
    if all_lines.count(benchmark_line) != 1 or [line for line in all_lines if line != benchmark_line] != stock_lines:
        raise _fail(
            "qe_direct_v2_provider_selection_mismatch",
            "all.txt is not exactly stock_universe.txt plus benchmark.txt",
        )
    meta = _load_json(root / "meta_export.json", code="qe_direct_v2_metadata_unreadable")
    benchmark_meta = meta.get("benchmark_only")
    expected = {
        "schema_version": "qe_direct_daily_benchmark_v1",
        "code": "000300.SH",
        "start": "2018-08-01",
        "end": cutoff,
        "source": "components/index_context/index_daily.h5",
        "provider_catalog": "instruments/all.txt",
        "selection_universe": "instruments/stock_universe.txt",
        "benchmark_universe": "instruments/benchmark.txt",
        "selection_eligible": False,
    }
    if not isinstance(benchmark_meta, Mapping) or any(
        benchmark_meta.get(key) != value for key, value in expected.items()
    ):
        raise _fail("qe_direct_v2_benchmark_metadata_invalid", "benchmark_only metadata differs")


def _validate_index(path: Path, pins: Mapping[str, Any]) -> None:
    _require_file(path, str(pins["sha256"]))
    try:
        import pandas as pd

        frame = pd.read_hdf(path)
    except Exception as exc:
        raise _fail("qe_direct_v2_index_unreadable", f"cannot read {path}: {exc}") from exc
    if "ts_code" in frame.index.names:
        codes = tuple(sorted(frame.index.get_level_values("ts_code").astype(str).unique()))
    elif "ts_code" in frame.columns:
        codes = tuple(sorted(frame["ts_code"].astype(str).unique()))
    else:
        raise _fail("qe_direct_v2_index_schema_invalid", "ts_code is missing")
    if "trade_date" in frame.index.names:
        dates = pd.to_datetime(frame.index.get_level_values("trade_date"))
    elif "trade_date" in frame.columns:
        dates = pd.to_datetime(frame["trade_date"])
    else:
        raise _fail("qe_direct_v2_index_schema_invalid", "trade_date is missing")
    max_date = dates.max().date().isoformat()
    if codes != tuple(pins["codes"]) or codes != INDEX_CODES:
        raise _fail("qe_direct_v2_index_codes_mismatch", f"actual={codes!r}")
    if max_date != pins["max_date"]:
        raise _fail(
            "qe_direct_v2_index_cutoff_mismatch",
            f"expected={pins['max_date']} actual={max_date}",
        )


def validate_binding(path: Path = Path(BINDING_FILE)) -> dict[str, Any]:
    binding = _load_json(path, code="qe_direct_v2_binding_unreadable")
    if binding.get("schema_version") != BINDING_SCHEMA:
        raise _fail("qe_direct_v2_binding_schema_invalid", "schema_version differs")
    if set(binding) != BINDING_FIELDS:
        raise _fail("qe_direct_v2_binding_schema_invalid", "binding fields differ")

    root = str(binding["candidate_root"]).rstrip("/")
    if not root or "\\" in root or "/../" in f"/{root.strip('/')}/":
        raise _fail("qe_direct_v2_binding_path_invalid", "candidate_root is not canonical")
    expected_paths = {
        "provider_uri_day": f"{root}/components/daily_bin_candidate",
        "provider_uri_1min": f"{root}/components/minute_bin_candidate",
        "factor_data_dir": f"{root}/components/factor_h5_static_candidate_v2",
        "index_context_path": f"{root}/components/index_context/index_daily.h5",
        "suspend_data_dir": f"{root}/components/suspend_d_daily_candidate_v2",
    }
    mismatched_paths = {
        key: {"expected": expected, "actual": binding.get(key)}
        for key, expected in expected_paths.items()
        if binding.get(key) != expected
    }
    if mismatched_paths:
        raise _fail(
            "qe_direct_v2_binding_path_mismatch",
            json.dumps(mismatched_paths, ensure_ascii=True, sort_keys=True),
        )

    day = Path(binding["provider_uri_day"])
    minute = Path(binding["provider_uri_1min"])
    factor = Path(binding["factor_data_dir"])
    index = Path(binding["index_context_path"])
    suspend = Path(binding["suspend_data_dir"])
    for component in (day, minute, factor, index.parent, suspend):
        if component.is_symlink():
            raise _fail(
                "qe_direct_v2_symlink_forbidden",
                f"component path is a symlink: {component}",
            )
        if not component.is_dir():
            raise _fail(
                "qe_direct_v2_component_missing",
                f"component directory is missing: {component}",
            )

    _validate_qlib_component(day, binding["day_pins"], freq="day")
    _validate_qlib_component(minute, binding["minute_pins"], freq="1min")
    _validate_selection_universe(day, binding["selection_pins"], cutoff=binding["cutoff"])
    factor_meta_path = factor / "meta.json"
    _require_file(factor_meta_path, binding["factor_meta_sha256"])
    _require_meta(factor_meta_path, binding["factor_meta"])
    _validate_index(index, binding["index_pins"])

    suspend_pins = binding["suspend_pins"]
    suspend_meta_path = suspend / "meta.json"
    _require_file(suspend_meta_path, suspend_pins["metadata_sha256"])
    _require_file(suspend / "suspend_d.parquet", suspend_pins["parquet_sha256"])
    _require_meta(
        suspend_meta_path,
        {
            "schema_version": suspend_pins["schema_version"],
            "component": "suspend_d",
            "start": binding["factor_meta"]["start"],
            "end": binding["cutoff"],
            "universe_key": binding["factor_meta"]["universe_key"],
        },
    )
    return binding


def main() -> None:
    binding = validate_binding()
    print(
        "[INFO] QE direct-v2 dataset binding verified: "
        f"release_id={binding['release_id']} cutoff={binding['cutoff']}"
    )


if __name__ == "__main__":
    main()
