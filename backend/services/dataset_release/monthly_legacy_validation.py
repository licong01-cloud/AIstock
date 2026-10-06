"""Compare newly written Qlib month values to sealed canonical inputs.

The inherited calendar is metadata, not an invitation to re-audit historical
values. SHA256 pins for rewritten files come from the actual streaming writer;
only new-month CSV inputs and small calendar/catalog files are hashed here.
"""

from __future__ import annotations

import csv
from bisect import bisect_left
from datetime import date
import hashlib
import io
import math
from pathlib import Path
import re
import struct
from typing import Any, Callable, Mapping

import numpy as np

from .monthly_legacy_prefix import _plain_chain, _relative, _signature, is_monthly_provider_instrument
from .factor_materializer import FACTOR_H5_DATASETS
from .index_contract import DOMESTIC_INDEX_DEFINITIONS
from .qlib_bounded_update import _parse_datetime, read_calendar
from .stock_schema import QLIB_STOCK_FIELDS


class MonthlyLegacyValidationError(RuntimeError):
    """The actual month output differs from its sealed construction inputs."""


def validate_aggregate_month_receipt(
    *, root: Path, receipt: Mapping[str, Any], component: str,
    cutoff: date, predecessor_manifest_sha256: str, source_bundle_sha256: str,
    repair_inputs: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    """Retain genuine writer QA/pins, without repeating old H5/Parquet reads."""
    schemas = {"factor": "aistock_monthly_native_factor_materialization_v1", "index": "aistock_monthly_native_index_materialization_v1"}
    if (
        component not in schemas or receipt.get("schema_version") != schemas[component]
        or receipt.get("status") != "PASS" or receipt.get("cutoff") != cutoff.isoformat()
        or receipt.get("month_start") != cutoff.replace(day=1).isoformat()
        or receipt.get("predecessor_manifest_sha256") != predecessor_manifest_sha256
        or receipt.get("source_bundle_sha256") != source_bundle_sha256
        or receipt.get("historical_business_audit_performed") is not False
        or receipt.get("publication_allowed") is not False
        or component == "factor" and receipt.get("monthly_repair_inputs") != repair_inputs
    ):
        raise MonthlyLegacyValidationError("native aggregate month receipt identity differs")
    files = receipt.get("files")
    if not isinstance(files, list):
        raise MonthlyLegacyValidationError("native aggregate month file set is missing")
    if component == "factor":
        expected = {f"{name}.h5" for name in FACTOR_H5_DATASETS} | {"static_factors.parquet"}
        base = "factor_bundle"
    else:
        expected = {"index_daily.h5", "index_context.parquet"} | {f"index_csv/{item.daily_code}.csv" for item in DOMESTIC_INDEX_DEFINITIONS}
        base = "index_context"
        if receipt.get("index_count") != 12 or receipt.get("month_output_values_verified") is not True:
            raise MonthlyLegacyValidationError("native index month QA is incomplete")
    root = Path(root).absolute()
    pins: dict[str, Any] = {}
    month_ranges: dict[str, Any] = {}
    observed = set()
    for item in files:
        if not isinstance(item, Mapping):
            raise MonthlyLegacyValidationError("native aggregate month file pin is invalid")
        if component == "factor":
            name = item.get("dataset")
            relative = f"{name}.{'parquet' if name == 'static_factors' else 'h5'}"
            override = (repair_inputs or {}).get("factor_prefixes", {}).get(name)
            expected_physical = override["dataset_manifest_sha256"] if override else predecessor_manifest_sha256
            if (
                item.get("schema_version") != "aistock_monthly_legacy_factor_append_v1"
                or item.get("predecessor_manifest_sha256") != predecessor_manifest_sha256
                or item.get("physical_prefix_manifest_sha256", predecessor_manifest_sha256) != expected_physical
                or item.get("source_bundle_sha256") != source_bundle_sha256
                or item.get("cutoff") != cutoff.isoformat() or item.get("validation_scope") != "month_delta"
                or item.get("month_output_values_verified") is not True
                or item.get("historical_business_audit_performed") is not False
                or item.get("publication_allowed") is not False
            ):
                raise MonthlyLegacyValidationError("native factor month writer QA is incomplete")
            inherited, appended = item.get("inherited_rows"), item.get("month_rows")
            if type(inherited) is not int or inherited < 0 or type(appended) is not int or appended < 0:
                raise MonthlyLegacyValidationError("native factor physical month row boundary is invalid")
            month_ranges[name] = {"start_row": inherited, "row_count": appended}
        else:
            relative = item.get("relative_path")
        if relative not in expected or relative in observed:
            raise MonthlyLegacyValidationError("native aggregate month file population differs")
        observed.add(relative)
        path = root / base / _relative(relative)
        _plain_chain(path)
        before = _signature(path)
        if (
            list(before) != item.get("target_signature") or before[2] != item.get("size")
            or not isinstance(item.get("sha256"), str) or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
        ):
            raise MonthlyLegacyValidationError("native aggregate month output changed after verified writing")
        pins[path.relative_to(root).as_posix()] = {"sha256": item["sha256"], "size": before[2], "signature": list(before)}
    if observed != expected:
        raise MonthlyLegacyValidationError("native aggregate month file population is incomplete")
    if component == "factor":
        alias = receipt.get("alias_coverage")
        metadata = receipt.get("metadata_files")
        if (
            not isinstance(alias, Mapping) or alias.get("schema_version") != "qe_moneyflow_alias_coverage_receipt_v1"
            or alias.get("status") != "PASS" or alias.get("validation_scope") != "month_delta"
            or alias.get("month_start") != cutoff.replace(day=1).isoformat() or alias.get("cutoff") != cutoff.isoformat()
            or alias.get("source_bundle_sha256") != source_bundle_sha256
            or alias.get("predecessor_manifest_sha256") != predecessor_manifest_sha256
            or alias.get("moneyflow_sha256") != pins["factor_bundle/moneyflow.h5"]["sha256"]
            or alias.get("expected") != alias.get("resolved")
            or any(alias.get(key) != 0 for key in ("unknown", "nonfinite", "mismatched"))
            or not isinstance(metadata, list) or len(metadata) != 2
        ):
            raise MonthlyLegacyValidationError("native factor month alias authority is incomplete")
        names = set()
        for item in metadata:
            relative = item.get("relative_path")
            if relative not in {"security_source_identity.json", "moneyflow_alias_coverage_v1.json"} or relative in names:
                raise MonthlyLegacyValidationError("native factor metadata population differs")
            names.add(relative)
            path = root / base / relative
            _plain_chain(path)
            before = _signature(path)
            raw = path.read_bytes()
            if (
                list(before) != item.get("target_signature") or before[2] != item.get("size")
                or _signature(path) != before or hashlib.sha256(raw).hexdigest() != item.get("sha256")
            ):
                raise MonthlyLegacyValidationError("native factor metadata changed after sealing")
            pins[path.relative_to(root).as_posix()] = {"sha256": item["sha256"], "size": before[2], "signature": list(before)}
    return {"validation_scope": "month_delta", "status": "PASS", "verified_output_files": pins,
            "physical_month_ranges": month_ranges, "historical_values_read": 0}


def validate_qlib_month(
    *, root: Path, materialization: Mapping[str, Any], cas,
    cutoff: date, predecessor_manifest_sha256: str, source_bundle_sha256: str,
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any]:
    dataset = materialization.get("dataset")
    if (
        dataset not in {"daily_bin", "minute_bin"}
        or materialization.get("schema_version") != "aistock_monthly_native_qlib_materialization_v1"
        or materialization.get("status") != "PASS" or materialization.get("scope") != "month_delta"
        or materialization.get("cutoff") != cutoff.isoformat()
        or materialization.get("predecessor_manifest_sha256") != predecessor_manifest_sha256
        or materialization.get("source_bundle_sha256") != source_bundle_sha256
        or materialization.get("historical_business_audit_performed") is not False
    ):
        raise MonthlyLegacyValidationError("native month materialization identity differs")
    root = Path(root).absolute()
    provider = root / dataset / "qlib"
    native = materialization.get("native_writer")
    sealed = materialization.get("sealed_canonical_rows")
    if (
        not isinstance(native, Mapping) or not isinstance(sealed, Mapping)
        or native.get("schema_version") != "aistock_qlib_bounded_update_receipt_v1"
        or native.get("predecessor_manifest_sha256") != predecessor_manifest_sha256
        or Path(str(native.get("target_root", ""))).absolute() != provider
        or native.get("historical_content_revalidated") is not False
        or sealed.get("schema_version") != "aistock_monthly_native_qlib_prefix_and_delta_v1"
        or sealed.get("dataset") != dataset
        or sealed.get("predecessor_manifest_sha256") != predecessor_manifest_sha256
    ):
        raise MonthlyLegacyValidationError("native month writer binding differs")
    prepared = cas.get_json_bounded(sealed.get("month_preparation_ref"), max_bytes=16 * 1024 * 1024)
    if (
        prepared.get("schema_version") != "aistock_monthly_legacy_qlib_operation_v1"
        or prepared.get("dataset") != dataset or prepared.get("cutoff") != cutoff.isoformat()
        or prepared.get("predecessor_manifest_sha256") != predecessor_manifest_sha256
        or prepared.get("source_bundle_sha256") != source_bundle_sha256
    ):
        raise MonthlyLegacyValidationError("native month sealed preparation identity differs")
    frequency = "day" if dataset == "daily_bin" else "1min"
    pins: dict[str, Any] = {}

    def record(path: Path, *, sha256: object, size: object, signature: object) -> None:
        _plain_chain(path)
        actual = _signature(path)
        if (
            not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            or type(size) is not int or actual[2] != size
            or not isinstance(signature, list) or list(actual) != signature
        ):
            raise MonthlyLegacyValidationError("native month output changed after writing")
        pins[path.relative_to(root).as_posix()] = {"sha256": sha256, "size": size, "signature": signature}

    for relative, sha_key, stat_key in (
        (f"calendars/{frequency}.txt", "calendar_sha256", "calendar_signature"),
        ("instruments/all.txt", "instruments_sha256", "instruments_signature"),
    ):
        path = provider / relative
        _plain_chain(path)
        before = _signature(path)
        raw = path.read_bytes()
        record(path, sha256=native.get(sha_key), size=len(raw), signature=native.get(stat_key))
        if _signature(path) != before or hashlib.sha256(raw).hexdigest() != native.get(sha_key):
            raise MonthlyLegacyValidationError("native month calendar/catalog bytes differ")
        source_ref = prepared["calendar_ref" if relative.startswith("calendars/") else "instruments_ref"]
        if source_ref.get("sha256") != native.get(sha_key) or source_ref.get("size") != len(raw):
            raise MonthlyLegacyValidationError("native month calendar/catalog sealed binding differs")
    calendar = read_calendar(provider / f"calendars/{frequency}.txt")
    month_start = cutoff.replace(day=1).isoformat()
    if calendar[-1][:10] != cutoff.isoformat():
        raise MonthlyLegacyValidationError("native month calendar cutoff differs")
    calendar_positions = {stamp: i for i, stamp in enumerate(calendar)}
    features = native.get("feature_receipts")
    refs = prepared.get("csv_refs")
    if not isinstance(features, list) or not isinstance(refs, list) or not refs:
        raise MonthlyLegacyValidationError("native month feature population is missing")
    lookup = {}
    for item in features:
        key = (str(item.get("instrument", "")).casefold(), str(item.get("feature", "")))
        if key in lookup or key[1] not in QLIB_STOCK_FIELDS:
            raise MonthlyLegacyValidationError("native month feature population is ambiguous")
        lookup[key] = item
    expected_keys = set()
    validated_rows = comparisons = 0
    for ref in refs:
        checkpoint()
        csv_path = root / _relative(ref["id"])
        _plain_chain(csv_path)
        before = _signature(csv_path)
        # One stock/month, not an all-market or whole-history frame.
        raw = csv_path.read_bytes()
        if before != _signature(csv_path) or len(raw) != ref.get("size") or hashlib.sha256(raw).hexdigest() != ref.get("sha256"):
            raise MonthlyLegacyValidationError("native month frozen CSV bytes differ")
        code = csv_path.stem.casefold()
        if not is_monthly_provider_instrument(code, dataset=dataset):
            raise MonthlyLegacyValidationError("native month frozen CSV instrument is invalid")
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
        if reader.fieldnames is None or set(reader.fieldnames) != {"date", "symbol", *QLIB_STOCK_FIELDS}:
            raise MonthlyLegacyValidationError("native month frozen CSV schema differs")
        rows: dict[int, Mapping[str, str]] = {}
        previous = -1
        for row in reader:
            stamp = _parse_datetime(row["date"], frequency=frequency)
            position = calendar_positions.get(stamp)
            if (
                position is None or position <= previous or not month_start <= stamp[:10] <= cutoff.isoformat()
                or row["symbol"].casefold() != code or None in row
            ):
                raise MonthlyLegacyValidationError("native month frozen CSV date/symbol differs")
            rows[position] = row
            previous = position
        if not rows:
            raise MonthlyLegacyValidationError("native month frozen CSV is empty")
        for field in QLIB_STOCK_FIELDS:
            key = (code, field)
            if key in expected_keys or key not in lookup:
                raise MonthlyLegacyValidationError("native month feature population differs")
            expected_keys.add(key)
            item = lookup[key]
            path = provider / "features" / code / f"{field}.{frequency}.bin"
            record(path, sha256=item.get("target_sha256"), size=item.get("target_size"), signature=item.get("target_signature"))
            before = _signature(path)
            with path.open("rb") as stream:
                header = stream.read(4)
                if len(header) != 4 or before[2] < 8 or before[2] % 4:
                    raise MonthlyLegacyValidationError("native month feature dimensions differ")
                start = struct.unpack("<f", header)[0]
                if not math.isfinite(start) or start != int(start) or start < 0:
                    raise MonthlyLegacyValidationError("native month feature calendar offset differs")
                start = int(start)
                end = start + before[2] // 4 - 2
                first = max(start, bisect_left(calendar, month_start))
                last = max(rows)
                if start > min(rows) or end != last or end >= len(calendar) or item.get("start_index") != start or item.get("end_index") != end:
                    raise MonthlyLegacyValidationError("native month feature bounds differ")
                stream.seek(4 + (first - start) * 4)
                observed = np.frombuffer(stream.read((last - first + 1) * 4), dtype="<f4")
            if _signature(path) != before or len(observed) != last - first + 1:
                raise MonthlyLegacyValidationError("native month feature changed during validation")
            expected = np.full(last - first + 1, np.nan, dtype="<f4")
            with np.errstate(over="ignore", invalid="ignore"):
                for position, row in rows.items():
                    expected[position - first] = float(row[field])
            if np.isinf(expected).any() or not np.array_equal(observed, expected, equal_nan=True):
                raise MonthlyLegacyValidationError(f"native month {code}/{field} differs from frozen CSV")
            comparisons += len(rows)
        validated_rows += len(rows)
    if expected_keys != set(lookup) or validated_rows != materialization["csv"]["rows"] + materialization["indices"]["rows"]:
        raise MonthlyLegacyValidationError("native month row/feature population differs")
    return {
        "schema_version": "aistock_monthly_native_qlib_validation_v1", "dataset": dataset,
        "status": "PASS", "validation_scope": "month_delta", "cutoff": cutoff.isoformat(),
        "predecessor_manifest_sha256": predecessor_manifest_sha256, "source_bundle_sha256": source_bundle_sha256,
        "validated_rows": validated_rows, "value_comparison_count": comparisons,
        "historical_values_read": 0, "verified_output_files": pins,
        "publication_allowed": False, "database_read": False, "database_write": False,
    }
