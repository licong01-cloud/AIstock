"""Candidate-local daily/minute CSV preparation and supervised Qlib dumping.

Rows arrive as a single, ordered stream produced from frozen source partitions.
The module retains only one instrument's CSV writer at a time and invokes the
allowlisted Qlib dump tool exclusively through the Worker's supervised runner.
"""

from __future__ import annotations

import csv
import hashlib
import itertools
import json
import math
import os
import re
import shlex
import shutil
import stat
import tempfile
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from .canonical import digest_named_fields
from .canonical_lineage import (
    CANONICAL_LINEAGE_SCHEMA,
    active_segments as lineage_active_segments,
    instrument_summaries as lineage_instrument_summaries,
    is_lineage_v3,
    latest_event_inventory,
    legacy_active_segments,
    validate_lineage_descriptor,
    write_genesis,
)
from .errors import DatasetReleaseError
from .index_contract import DOMESTIC_INDEX_DEFINITIONS, INDEX_QLIB_FIELDS
from .pit import FrozenPitSnapshot
from .resource_supervisor import WslSupervisedOptions
from .stock_schema import QLIB_STOCK_FIELDS
from .streaming_artifacts import sha256_file


DAILY_MINUTE_SCHEMA_V1 = "dataset_release_daily_minute_materialization_v1"
DAILY_MINUTE_SCHEMA = "dataset_release_daily_minute_materialization_v2"
DAILY_MINUTE_SUPPORTED_SCHEMAS = frozenset({DAILY_MINUTE_SCHEMA_V1, DAILY_MINUTE_SCHEMA})
DAILY_MINUTE_CSV_PREPARATION_SCHEMA_V1 = "dataset_release_daily_minute_csv_preparation_v1"
DAILY_MINUTE_CSV_PREPARATION_SCHEMA = "dataset_release_daily_minute_csv_preparation_v2"
DAILY_MINUTE_CSV_PREPARATION_SUPPORTED_SCHEMAS = frozenset(
    {DAILY_MINUTE_CSV_PREPARATION_SCHEMA_V1, DAILY_MINUTE_CSV_PREPARATION_SCHEMA}
)
SEALED_QLIB_CSV_ROWS_SCHEMA = "dataset_release_sealed_qlib_csv_rows_v1"
SEALED_QLIB_CSV_COMPOSITE_SCHEMA = "dataset_release_sealed_qlib_csv_rows_composite_v1"
DAILY_FIELDS = QLIB_STOCK_FIELDS
MINUTE_FIELDS = QLIB_STOCK_FIELDS
_SAFE_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ)\Z")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class DailyMinuteMaterializationError(DatasetReleaseError):
    code = "DATASET_RELEASE_DAILY_MINUTE_MATERIALIZATION_INVALID"


class SupervisedDumpFailed(DailyMinuteMaterializationError):
    code = "BLOCKED_SUPERVISED_QLIB_DUMP_FAILED"


class SupervisedExecutor(Protocol):
    def run_supervised(
        self,
        command: Sequence[str],
        *,
        execution_id: str,
        cwd: Path,
        env: Mapping[str, str] | None = None,
        runtime: str = "windows",
        timeout_seconds: float | None = None,
        cooperative_grace_seconds: float = 30.0,
        wsl: WslSupervisedOptions | None = None,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class QlibDumpToolchain:
    distro: str
    conda_sh: str
    conda_env: str
    dump_script_wsl: str
    dump_script_windows: Path
    dump_script_sha256: str
    guardian_python: str
    guardian_script_wsl: str
    guardian_script_windows: Path
    guardian_script_sha256: str
    heartbeat_path_wsl: str
    runner_python_wsl: str
    runner_script_wsl: str
    runner_script_windows: Path
    runner_script_sha256: str

    def __post_init__(self) -> None:
        for field, value in asdict(self).items():
            text = str(value).strip()
            if not text or "\x00" in text:
                raise DailyMinuteMaterializationError(f"Qlib toolchain {field} is invalid")
        if not self.dump_script_wsl.startswith("/"):
            raise DailyMinuteMaterializationError("dump script must be an absolute WSL path")
        for field_name in (
            "dump_script_sha256",
            "guardian_script_sha256",
            "runner_script_sha256",
        ):
            if re.fullmatch(r"[0-9a-f]{64}", str(getattr(self, field_name))) is None:
                raise DailyMinuteMaterializationError(f"Qlib toolchain {field_name} is invalid")
        for field_name in (
            "dump_script_windows",
            "guardian_script_windows",
            "runner_script_windows",
        ):
            if not Path(getattr(self, field_name)).is_absolute():
                raise DailyMinuteMaterializationError(f"Qlib toolchain {field_name} must be absolute")

    @property
    def digest(self) -> str:
        return digest_named_fields(
            "dataset_release_qlib_toolchain_v1",
            {
                **asdict(self),
                "dump_script_windows": str(self.dump_script_windows),
                "guardian_script_windows": str(self.guardian_script_windows),
                "runner_script_windows": str(self.runner_script_windows),
            },
        )

    def verify_content(self) -> dict[str, Any]:
        files = {
            "dump_bin": (Path(self.dump_script_windows), self.dump_script_sha256),
            "wsl_resource_guardian": (
                Path(self.guardian_script_windows),
                self.guardian_script_sha256,
            ),
            "subprocess_runner": (
                Path(self.runner_script_windows),
                self.runner_script_sha256,
            ),
        }
        receipt: dict[str, Any] = {}
        for name, (path, expected) in files.items():
            try:
                resolved = path.resolve(strict=True)
            except OSError as exc:
                raise DailyMinuteMaterializationError(f"Qlib toolchain file is unavailable: {name}") from exc
            _assert_plain(resolved)
            actual = sha256_file(resolved)
            if actual != expected:
                raise DailyMinuteMaterializationError(f"Qlib toolchain content digest differs: {name}")
            receipt[name] = {
                "windows_path": str(resolved),
                "sha256": actual,
                "size_bytes": resolved.stat().st_size,
            }
        return {
            "schema_version": "dataset_release_qlib_toolchain_verification_v1",
            "toolchain_digest": self.digest,
            "files": receipt,
        }


@dataclass(frozen=True, slots=True)
class DailyMinuteMaterializationSpec:
    dataset: str
    staging_root: Path
    project_root: Path
    cutoff: date
    effective_start: date
    pit_snapshot: FrozenPitSnapshot
    dump_workers: int
    toolchain: QlibDumpToolchain
    index_csv_root: Path | None = None
    child_timeout_seconds: float = 14_400.0

    def __post_init__(self) -> None:
        if self.dataset not in {"daily_bin", "minute_bin"}:
            raise DailyMinuteMaterializationError("dataset must be daily_bin or minute_bin")
        if self.pit_snapshot.cutoff != self.cutoff:
            raise DailyMinuteMaterializationError("PIT cutoff differs from bin cutoff")
        if self.effective_start > self.cutoff:
            raise DailyMinuteMaterializationError("bin effective start exceeds cutoff")
        if type(self.dump_workers) is not int or not 0 < self.dump_workers <= 8:
            raise DailyMinuteMaterializationError("dump workers must be in [1,8]")
        if self.child_timeout_seconds <= 0:
            raise DailyMinuteMaterializationError("child timeout must be positive")
        if self.dataset == "daily_bin" and self.index_csv_root is None:
            raise DailyMinuteMaterializationError("daily bin requires the frozen domestic-index CSV authority")
        if self.dataset == "minute_bin" and self.index_csv_root is not None:
            raise DailyMinuteMaterializationError("minute bin cannot consume index CSV")

    @property
    def digest(self) -> str:
        return digest_named_fields(
            DAILY_MINUTE_SCHEMA,
            {
                "dataset": self.dataset,
                "cutoff": self.cutoff,
                "effective_start": self.effective_start,
                "pit_spans_sha256": self.pit_snapshot.spans_sha256,
                "dump_workers": self.dump_workers,
                "toolchain": asdict(self.toolchain),
                "child_timeout_seconds": self.child_timeout_seconds,
            },
        )

    @property
    def legacy_digest(self) -> str:
        """Identity accepted only when reading an already-sealed v1 receipt."""

        return digest_named_fields(
            DAILY_MINUTE_SCHEMA_V1,
            {
                "dataset": self.dataset,
                "cutoff": self.cutoff,
                "effective_start": self.effective_start,
                "pit_spans_sha256": self.pit_snapshot.spans_sha256,
                "dump_workers": self.dump_workers,
                "toolchain": asdict(self.toolchain),
                "child_timeout_seconds": self.child_timeout_seconds,
            },
        )


@dataclass(frozen=True, slots=True)
class DailyMinuteMaterializationReceipt:
    receipt: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DailyMinuteCsvPreparationReceipt:
    receipt: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DailyMinutePatchPreparationReceipt:
    receipt: Mapping[str, Any]


class DailyMinuteCsvPreparer:
    """Prepare canonical per-code CSVs in a supervised Windows stage."""

    def prepare(
        self,
        spec: DailyMinuteMaterializationSpec,
        *,
        rows: Iterable[Mapping[str, Any]],
        checkpoint: Callable[[], None] = lambda: None,
    ) -> DailyMinuteCsvPreparationReceipt:
        staging = _plain_root(spec.staging_root)
        root = staging / spec.dataset
        if root.exists():
            _assert_plain(root)
        else:
            root.mkdir()
        csv_root = root / "csv"
        csv_root.mkdir(exist_ok=True)
        preparation_path = root / "csv_preparation_receipt.json"
        csv_receipt = _write_ordered_stock_csvs(
            rows,
            csv_root=csv_root,
            dataset=spec.dataset,
            cutoff=spec.cutoff,
            checkpoint=checkpoint,
        )
        if spec.dataset == "daily_bin":
            assert spec.index_csv_root is not None
            index_receipt = _copy_index_csvs(spec.index_csv_root, csv_root)
        else:
            index_receipt = {"codes": [], "files": []}
        legacy_sealed = _sealed_canonical_rows(spec.dataset, csv_receipt)
        lineage = write_genesis(
            root,
            dataset=spec.dataset,
            ordered_fields=legacy_sealed["ordered_fields"],
            segments=legacy_active_segments(
                legacy_sealed,
                dataset=spec.dataset,
            ),
            cutoff=spec.cutoff.isoformat(),
            mutation_identity=spec.digest,
        )
        sealed = dict(lineage.descriptor)
        receipt = {
            "schema_version": DAILY_MINUTE_CSV_PREPARATION_SCHEMA,
            "spec_digest": spec.digest,
            "status": "PASS",
            "dataset": spec.dataset,
            "cutoff": spec.cutoff.isoformat(),
            "pit_spans_sha256": spec.pit_snapshot.spans_sha256,
            "csv": csv_receipt,
            "indices": index_receipt,
            "sealed_canonical_rows": sealed,
            "memory_contract": {
                "mode": "one_instrument_csv_writer_v1",
                "cross_instrument_frames_retained": 0,
                "canonical_lineage": "persistent_code_head_merkle_v3",
            },
            "safety": _zero_safety(),
        }
        if preparation_path.exists():
            existing = _load_json(preparation_path)
            if existing != receipt:
                raise DailyMinuteMaterializationError("daily/minute CSV preparation identity conflicts")
        else:
            _atomic_json(preparation_path, receipt)
        return DailyMinuteCsvPreparationReceipt(receipt)


class DailyMinutePatchCsvPreparer:
    """Prepare only bounded stock/index CSV inputs for a private Qlib writer."""

    def prepare(
        self,
        spec: DailyMinuteMaterializationSpec,
        *,
        rows: Iterable[Mapping[str, Any]],
        output_root: Path,
        index_codes: Sequence[str] = (),
        index_date_ranges: Sequence[tuple[date, date]] = (),
        checkpoint: Callable[[], None] = lambda: None,
    ) -> DailyMinutePatchPreparationReceipt:
        root = Path(output_root)
        if root.exists():
            raise DailyMinuteMaterializationError("private patch CSV root already exists")
        root.mkdir(parents=True, exist_ok=False)
        buffered = iter(rows)
        try:
            first = next(buffered)
        except StopIteration:
            csv_receipt: dict[str, Any] = {
                "rows": 0,
                "instruments": [],
                "files": [],
                "ranges": {},
            }
        else:
            csv_receipt = _write_ordered_stock_csvs(
                itertools.chain((first,), buffered),
                csv_root=root,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                checkpoint=checkpoint,
            )
        if spec.dataset == "daily_bin":
            if index_codes and spec.index_csv_root is None:
                raise DailyMinuteMaterializationError("daily index patch lacks frozen index CSV authority")
            index_receipt = _copy_index_csv_ranges(
                spec.index_csv_root,
                root,
                codes=index_codes,
                date_ranges=index_date_ranges,
            )
        elif index_codes or index_date_ranges:
            raise DailyMinuteMaterializationError("minute patch cannot contain index CSVs")
        else:
            index_receipt = {"codes": [], "files": [], "rows": 0}
        if int(csv_receipt["rows"]) <= 0 and not index_receipt["files"]:
            raise DailyMinuteMaterializationError("bounded Qlib patch is empty")
        return DailyMinutePatchPreparationReceipt(
            {
                "schema_version": "dataset_release_daily_minute_patch_csv_v1",
                "spec_digest": spec.digest,
                "status": "PASS",
                "dataset": spec.dataset,
                "cutoff": spec.cutoff.isoformat(),
                "csv": csv_receipt,
                "indices": index_receipt,
                "actual_work": {
                    "stock_rows_transformed": int(csv_receipt["rows"]),
                    "stock_instruments": len(csv_receipt["instruments"]),
                    "index_rows_copied": int(index_receipt["rows"]),
                    "index_instruments": len(index_receipt["codes"]),
                },
                "memory_contract": {
                    "mode": "bounded_patch_one_instrument_csv_writer_v1",
                    "cross_instrument_frames_retained": 0,
                },
                "safety": _zero_safety(),
            }
        )


class DailyMinuteIncrementalFinalizer:
    """Audit an adopted private-baseline dump_update/per-code patch."""

    def audit(
        self,
        spec: DailyMinuteMaterializationSpec,
        *,
        sealed_canonical_rows: Mapping[str, Any],
        supervised_child: Mapping[str, Any],
        patch_preparation: Mapping[str, Any],
        adoption: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        child = _child_receipt(supervised_child)
        if (
            int(child.get("returncode", -1)) != 0
            or int(child.get("active_processes", -1)) != 0
            or child.get("runtime") not in {None, "wsl"}
        ):
            raise SupervisedDumpFailed("incremental Qlib dump is not quiescent/successful")
        root = _plain_root(Path(spec.staging_root) / spec.dataset)
        csv_root = _plain_root(root / "csv")
        if is_lineage_v3(sealed_canonical_rows):
            _audit_lineage_canonical_rows(
                _plain_root(Path(spec.staging_root)),
                root,
                sealed_canonical_rows,
                latest_event_only=True,
            )
            csv_summary = _lineage_csv_summary(root, sealed_canonical_rows)
            stock_codes = set(csv_summary["instruments"])
        elif sealed_canonical_rows.get("schema_version") == SEALED_QLIB_CSV_COMPOSITE_SCHEMA:
            _audit_composite_canonical_rows(
                _plain_root(Path(spec.staging_root)),
                sealed_canonical_rows,
            )
            stock_codes = {str(item["instrument"]).upper() for item in sealed_canonical_rows["files"]}
            csv_summary = {
                "rows": int(sealed_canonical_rows["rows"]),
                "instruments": sorted(stock_codes),
                "files": list(sealed_canonical_rows["files"]),
                "ranges": {
                    str(item["instrument"]): [
                        str(item["start"])[:10],
                        str(item["end"])[:10],
                    ]
                    for item in sealed_canonical_rows["files"]
                },
            }
        else:
            _audit_prepared_csvs(csv_root, sealed_canonical_rows)
            stock_codes = {str(item["instrument"]).upper() for item in sealed_canonical_rows["files"]}
            csv_summary = {
                "rows": int(sealed_canonical_rows["rows"]),
                "instruments": sorted(stock_codes),
                "files": list(sealed_canonical_rows["files"]),
                "ranges": {
                    str(item["instrument"]): [
                        str(item["start"])[:10],
                        str(item["end"])[:10],
                    ]
                    for item in sealed_canonical_rows["files"]
                },
            }
        bin_audit = _audit_bin_root(
            _plain_root(root / "qlib"),
            dataset=spec.dataset,
            cutoff=spec.cutoff,
            expected_stock_codes=stock_codes,
        )
        return {
            "schema_version": DAILY_MINUTE_SCHEMA,
            "spec_digest": spec.digest,
            "status": "PASS",
            "dataset": spec.dataset,
            "cutoff": spec.cutoff.isoformat(),
            "pit_spans_sha256": spec.pit_snapshot.spans_sha256,
            "csv": csv_summary,
            "indices": dict(patch_preparation.get("indices") or {}),
            "sealed_canonical_rows": dict(sealed_canonical_rows),
            "bin": bin_audit,
            "supervised_child": child,
            "component_action": str(adoption.get("action", "")),
            "bounded_patch": dict(patch_preparation.get("actual_work") or {}),
            "adoption": dict(adoption),
            "memory_contract": {
                "mode": "private_baseline_bounded_patch_adoption_v1",
                "whole_market_frames_retained": 0,
            },
            "safety": _zero_safety(),
        }


def build_composite_canonical_rows(
    *,
    dataset: str,
    baseline: Mapping[str, Any],
    patch_preparation: Mapping[str, Any],
    delta_root_relative_path: str,
) -> Mapping[str, Any]:
    """Compose immutable baseline CSV segments with one bounded tail delta."""

    if dataset not in {"daily_bin", "minute_bin"}:
        raise DailyMinuteMaterializationError("composite CSV dataset is invalid")
    ordered_fields = [
        "date",
        "symbol",
        *(DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS),
    ]
    segments: list[dict[str, Any]] = []
    override_lineage: list[dict[str, Any]] = []
    if baseline.get("schema_version") == SEALED_QLIB_CSV_ROWS_SCHEMA:
        if baseline.get("dataset") != dataset or baseline.get("ordered_fields") != ordered_fields:
            raise DailyMinuteMaterializationError("baseline canonical CSV contract differs")
        for item in baseline.get("files") or ():
            segments.append(
                {
                    **dict(item),
                    "root_relative_path": str(baseline["root_relative_path"]),
                }
            )
    elif baseline.get("schema_version") == SEALED_QLIB_CSV_COMPOSITE_SCHEMA:
        if (
            baseline.get("dataset") != dataset
            or baseline.get("ordered_fields") != ordered_fields
            or not isinstance(baseline.get("segments"), list)
        ):
            raise DailyMinuteMaterializationError("baseline composite canonical CSV contract differs")
        segments.extend(dict(item) for item in baseline["segments"])
        raw_overrides = baseline.get("overrides") or []
        if not isinstance(raw_overrides, list) or not all(isinstance(item, Mapping) for item in raw_overrides):
            raise DailyMinuteMaterializationError("baseline override lineage is invalid")
        override_lineage.extend(dict(item) for item in raw_overrides)
    else:
        raise DailyMinuteMaterializationError("baseline canonical CSV schema is unsupported")
    patch = patch_preparation.get("csv")
    if not isinstance(patch, Mapping):
        raise DailyMinuteMaterializationError("patch canonical CSV receipt is missing")
    for item in patch.get("files") or ():
        segments.append(
            {
                "instrument": item["instrument"],
                "root_relative_path": delta_root_relative_path,
                "relative_path": f"{str(item['instrument']).casefold()}.csv",
                "rows": int(item["rows"]),
                "sha256": item["sha256"],
                "size_bytes": int(item["size_bytes"]),
                "start": item["start"],
                "end": item["end"],
            }
        )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for segment in segments:
        code = str(segment.get("instrument", "")).upper()
        if _SAFE_CODE.fullmatch(code) is None:
            raise DailyMinuteMaterializationError("composite canonical CSV instrument is invalid")
        grouped.setdefault(code, []).append(segment)
    files: list[dict[str, Any]] = []
    ordered_segments: list[dict[str, Any]] = []
    for code in sorted(grouped):
        values = sorted(grouped[code], key=lambda item: str(item["start"]))
        for previous, current in zip(values, values[1:]):
            if str(current["start"]) <= str(previous["end"]):
                raise DailyMinuteMaterializationError(f"composite canonical CSV segments overlap: {code}")
        ordered_segments.extend(values)
        files.append(
            {
                "instrument": code,
                "rows": sum(int(item["rows"]) for item in values),
                "segments": len(values),
                "start": values[0]["start"],
                "end": values[-1]["end"],
            }
        )
    result = {
        "schema_version": SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
        "dataset": dataset,
        "ordered_fields": ordered_fields,
        "rows": sum(int(item["rows"]) for item in ordered_segments),
        "files": files,
        "segments": ordered_segments,
        "merge_contract": (
            "instrument_active_segments_with_explicit_overrides_v1"
            if override_lineage
            else "instrument_datetime_strict_append_segments_v1"
        ),
    }
    if override_lineage:
        result["overrides"] = override_lineage
    return result


def build_selective_override_canonical_rows(
    *,
    dataset: str,
    baseline: Mapping[str, Any],
    patch_preparation: Mapping[str, Any],
    override_root_relative_path: str,
    invalidation_scopes: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    """Switch only affected instruments to one full-history active override."""

    active = build_composite_canonical_rows(
        dataset=dataset,
        baseline=baseline,
        patch_preparation={
            "csv": {"files": [], "rows": 0},
        },
        delta_root_relative_path="unused",
    )
    segments = [dict(item) for item in active["segments"]]
    patch = patch_preparation.get("csv")
    if not isinstance(patch, Mapping) or not patch.get("files"):
        raise DailyMinuteMaterializationError("selective CSV override is empty")
    overrides: list[dict[str, Any]] = [dict(item) for item in active.get("overrides") or ()]
    for item in patch["files"]:
        code = str(item["instrument"]).upper()
        superseded = [value for value in segments if str(value["instrument"]).upper() == code]
        if not superseded:
            raise DailyMinuteMaterializationError(f"selective CSV override lacks baseline segments: {code}")
        segments = [value for value in segments if str(value["instrument"]).upper() != code]
        replacement = {
            "instrument": code,
            "root_relative_path": override_root_relative_path,
            "relative_path": f"{code.casefold()}.csv",
            "rows": int(item["rows"]),
            "sha256": item["sha256"],
            "size_bytes": int(item["size_bytes"]),
            "start": item["start"],
            "end": item["end"],
        }
        segments.append(replacement)
        authorized_scopes = [dict(value) for value in invalidation_scopes if _scope_authorizes_override(value, code)]
        if not authorized_scopes:
            raise DailyMinuteMaterializationError(
                f"selective CSV override lacks code-local invalidation authority: {code}"
            )
        overrides.append(
            {
                **replacement,
                "superseded_segments": [
                    {
                        "root_relative_path": value["root_relative_path"],
                        "relative_path": value["relative_path"],
                        "sha256": value["sha256"],
                    }
                    for value in superseded
                ],
                "invalidation_scopes": authorized_scopes,
            }
        )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in segments:
        grouped.setdefault(str(item["instrument"]).upper(), []).append(item)
    ordered_segments: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    for code in sorted(grouped):
        values = sorted(grouped[code], key=lambda value: str(value["start"]))
        for previous, current in zip(values, values[1:]):
            if str(current["start"]) <= str(previous["end"]):
                raise DailyMinuteMaterializationError(f"selective active CSV segments overlap: {code}")
        ordered_segments.extend(values)
        files.append(
            {
                "instrument": code,
                "rows": sum(int(value["rows"]) for value in values),
                "segments": len(values),
                "start": values[0]["start"],
                "end": values[-1]["end"],
            }
        )
    return {
        "schema_version": SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
        "dataset": dataset,
        "ordered_fields": list(active["ordered_fields"]),
        "rows": sum(int(item["rows"]) for item in ordered_segments),
        "files": files,
        "segments": ordered_segments,
        "overrides": overrides,
        "merge_contract": "instrument_active_segments_with_explicit_overrides_v1",
    }


def _scope_authorizes_override(scope: Mapping[str, Any], code: str) -> bool:
    kind = str(scope.get("kind", ""))
    if kind in {
        "qfq_denominator_change",
        "qfq_historical_numerator_revision",
    }:
        instrument = scope.get("instrument")
        return instrument is not None and str(instrument).upper() == code
    if kind == "pit_span_change":
        return code in {str(value).upper() for value in scope.get("changed_instruments") or ()}
    if kind == "historical_source_revision":
        return code in {str(value).upper() for value in scope.get("affected_instruments") or ()}
    return False


def _audit_composite_canonical_rows(candidate_root: Path, sealed: Mapping[str, Any]) -> None:
    if (
        sealed.get("schema_version") != SEALED_QLIB_CSV_COMPOSITE_SCHEMA
        or not isinstance(sealed.get("segments"), list)
        or not isinstance(sealed.get("files"), list)
    ):
        raise DailyMinuteMaterializationError("composite canonical CSV receipt is invalid")
    observed = 0
    for item in sealed["segments"]:
        root = (candidate_root / str(item.get("root_relative_path", ""))).resolve(strict=True)
        path = (root / str(item.get("relative_path", ""))).resolve(strict=True)
        if candidate_root not in root.parents or root not in path.parents or not path.is_file():
            raise DailyMinuteMaterializationError("composite canonical CSV path escapes candidate")
        if int(item.get("size_bytes", -1)) != path.stat().st_size or sha256_file(path) != item.get("sha256"):
            raise DailyMinuteMaterializationError("composite canonical CSV segment bytes differ")
        rows = 0
        with path.open("rb") as handle:
            if not handle.readline(1024 * 1024 + 1):
                raise DailyMinuteMaterializationError("composite canonical CSV header is empty")
            while line := handle.readline(1024 * 1024 + 1):
                if len(line) > 1024 * 1024:
                    raise DailyMinuteMaterializationError("composite canonical CSV line exceeds memory bound")
                rows += 1
        if rows != int(item.get("rows", -1)):
            raise DailyMinuteMaterializationError("composite canonical CSV segment row count differs")
        observed += rows
    if observed != int(sealed.get("rows", -1)):
        raise DailyMinuteMaterializationError("composite canonical CSV total row count differs")


def _audit_lineage_canonical_rows(
    candidate_root: Path,
    component_root: Path,
    sealed: Mapping[str, Any],
    *,
    latest_event_only: bool,
) -> tuple[dict[str, Any], ...]:
    validated = validate_lineage_descriptor(component_root, sealed)
    rows = (
        latest_event_inventory(component_root, validated)
        if latest_event_only
        else lineage_active_segments(component_root, validated)
    )
    if latest_event_only and not rows:
        raise DailyMinuteMaterializationError("incremental lineage event contains no file inventory")
    observed = 0
    for item in rows:
        logical_root = candidate_root / str(item.get("root_relative_path", ""))
        logical_path = logical_root / str(item.get("relative_path", ""))
        root = logical_root.resolve(strict=True)
        path = logical_path.resolve(strict=True)
        if candidate_root not in root.parents or root not in path.parents or not path.is_file():
            raise DailyMinuteMaterializationError("lineage canonical CSV path escapes candidate")
        if int(item.get("size_bytes", -1)) != path.stat().st_size or sha256_file(path) != item.get("sha256"):
            raise DailyMinuteMaterializationError("lineage canonical CSV segment bytes differ")
        row_count = 0
        with path.open("rb") as handle:
            header = handle.readline(1024 * 1024 + 1)
            if not header or len(header) > 1024 * 1024:
                raise DailyMinuteMaterializationError("lineage canonical CSV header is invalid")
            while line := handle.readline(1024 * 1024 + 1):
                if len(line) > 1024 * 1024:
                    raise DailyMinuteMaterializationError("lineage canonical CSV line exceeds memory bound")
                row_count += 1
        if row_count != int(item.get("rows", -1)):
            raise DailyMinuteMaterializationError("lineage canonical CSV segment row count differs")
        if item.get("active", True) is True:
            observed += row_count
    if not latest_event_only and observed != int(validated["rows"]):
        raise DailyMinuteMaterializationError("lineage canonical CSV total row count differs")
    return rows


def _lineage_csv_summary(
    component_root: Path,
    sealed: Mapping[str, Any],
) -> dict[str, Any]:
    summaries = lineage_instrument_summaries(component_root, sealed)
    return {
        "rows": int(sealed["rows"]),
        "instruments": [str(item["instrument"]) for item in summaries],
        "ranges": {
            str(item["instrument"]): [
                str(item["start"])[:10],
                str(item["end"])[:10],
            ]
            for item in summaries
        },
        "instrument_summaries": [dict(item) for item in summaries],
        "canonical_schema_version": CANONICAL_LINEAGE_SCHEMA,
    }


class DailyMinuteBinFinalizer:
    """Adopt one quiescent supervised WSL dump in a Windows stage."""

    def finalize(
        self,
        spec: DailyMinuteMaterializationSpec,
        *,
        preparation: Mapping[str, Any],
        supervised_child: Mapping[str, Any],
        batched_dump: Mapping[str, Any] | None = None,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> DailyMinuteMaterializationReceipt:
        if (
            not _preparation_receipt_matches(preparation, spec)
            or preparation.get("status") != "PASS"
            or preparation.get("dataset") != spec.dataset
        ):
            raise DailyMinuteMaterializationError("daily/minute CSV preparation receipt differs")
        child_receipt = _child_receipt(supervised_child)
        if (
            int(child_receipt.get("returncode", -1)) != 0
            or int(child_receipt.get("active_processes", -1)) != 0
            or child_receipt.get("runtime") not in {None, "wsl"}
        ):
            raise SupervisedDumpFailed("Qlib dump is not quiescent/successful")
        staging = _plain_root(spec.staging_root)
        root = _plain_root(staging / spec.dataset)
        csv_root = _plain_root(root / "csv")
        sealed = preparation["sealed_canonical_rows"]
        if is_lineage_v3(sealed):
            _audit_lineage_canonical_rows(
                staging,
                root,
                sealed,
                latest_event_only=False,
            )
        else:
            _audit_prepared_csvs(csv_root, sealed)
        receipt_path = root / "materialization_receipt.json"
        bin_root = root / "qlib"
        if receipt_path.exists():
            existing = _load_json(receipt_path)
            if (
                not _materialization_receipt_matches(existing, spec)
                or existing.get("status") != "PASS"
                or (batched_dump is not None and existing.get("batched_dump") != dict(batched_dump))
            ):
                raise DailyMinuteMaterializationError("daily/minute receipt identity conflicts")
            _audit_bin_root(
                bin_root,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(preparation["csv"]["instruments"]),
            )
            return DailyMinuteMaterializationReceipt(existing)
        if bin_root.exists():
            bin_audit = _audit_bin_root(
                bin_root,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(preparation["csv"]["instruments"]),
            )
        else:
            working = root / ".qlib.working"
            if not working.is_dir():
                raise SupervisedDumpFailed("supervised Qlib working tree is missing")
            _write_instrument_authorities(
                working,
                spec.pit_snapshot,
                stock_ranges=preparation["csv"]["ranges"],
                dataset=spec.dataset,
                effective_start=spec.effective_start,
            )
            _audit_bin_root(
                working,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(preparation["csv"]["instruments"]),
            )
            os.rename(working, bin_root)
            bin_audit = _audit_bin_root(
                bin_root,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(preparation["csv"]["instruments"]),
            )
        checkpoint()
        receipt = {
            "schema_version": DAILY_MINUTE_SCHEMA,
            "spec_digest": spec.digest,
            "status": "PASS",
            "dataset": spec.dataset,
            "cutoff": spec.cutoff.isoformat(),
            "pit_spans_sha256": spec.pit_snapshot.spans_sha256,
            "csv": preparation["csv"],
            "indices": preparation["indices"],
            "sealed_canonical_rows": preparation["sealed_canonical_rows"],
            "bin": bin_audit,
            "supervised_child": child_receipt,
            **({"batched_dump": dict(batched_dump)} if batched_dump is not None else {}),
            "memory_contract": preparation["memory_contract"],
            "safety": _zero_safety(),
        }
        _atomic_json(receipt_path, receipt)
        return DailyMinuteMaterializationReceipt(receipt)


class DailyMinuteMaterializer:
    def materialize(
        self,
        spec: DailyMinuteMaterializationSpec,
        *,
        rows: Iterable[Mapping[str, Any]],
        executor: SupervisedExecutor,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> DailyMinuteMaterializationReceipt:
        project = _plain_root(spec.project_root)
        prepared = DailyMinuteCsvPreparer().prepare(spec, rows=rows, checkpoint=checkpoint).receipt
        root = _plain_root(Path(spec.staging_root) / spec.dataset)
        csv_root = _plain_root(root / "csv")
        receipt_path = root / "materialization_receipt.json"
        bin_root = root / "qlib"
        checkpoint()

        if receipt_path.exists():
            existing = _load_json(receipt_path)
            if not _materialization_receipt_matches(existing, spec) or existing.get("status") != "PASS":
                raise DailyMinuteMaterializationError("daily/minute receipt identity conflicts")
            _audit_bin_root(
                bin_root,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(prepared["csv"]["instruments"]),
            )
            return DailyMinuteMaterializationReceipt(existing)
        if bin_root.exists():
            # Adopt a complete crash-window output only after full local audit.
            bin_audit = _audit_bin_root(
                bin_root,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(prepared["csv"]["instruments"]),
            )
            child_receipt: Mapping[str, Any] = {
                "recovered_after_atomic_publish": True,
                "returncode": 0,
                "log_segments": [],
            }
        else:
            working = root / ".qlib.working"
            if working.exists():
                raise DailyMinuteMaterializationError(
                    "uncheckpointed Qlib working directory exists; a new fenced attempt is required"
                )
            command = _dump_command(spec, csv_root, working)
            child = executor.run_supervised(
                command,
                execution_id=f"{spec.dataset}-qlib-dump",
                cwd=project,
                runtime="wsl",
                timeout_seconds=spec.child_timeout_seconds,
                cooperative_grace_seconds=30.0,
                wsl=WslSupervisedOptions(
                    distro=spec.toolchain.distro,
                    guardian_python=spec.toolchain.guardian_python,
                    guardian_script_wsl=spec.toolchain.guardian_script_wsl,
                    heartbeat_path_wsl=spec.toolchain.heartbeat_path_wsl,
                    runner_python_wsl=spec.toolchain.runner_python_wsl,
                    runner_script_wsl=spec.toolchain.runner_script_wsl,
                    task_cwd_wsl=_windows_to_wsl(project),
                    execution_root_wsl=_windows_to_wsl(root / "supervised"),
                ),
            )
            child_receipt = _child_receipt(child)
            if int(child_receipt.get("returncode", -1)) != 0:
                raise SupervisedDumpFailed(f"supervised Qlib dump exited {child_receipt.get('returncode')}")
            if int(child_receipt.get("active_processes", 0)):
                raise SupervisedDumpFailed("Qlib dump retained active task children")
            _write_instrument_authorities(
                working,
                spec.pit_snapshot,
                stock_ranges=prepared["csv"]["ranges"],
                dataset=spec.dataset,
                effective_start=spec.effective_start,
            )
            bin_audit = _audit_bin_root(
                working,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(prepared["csv"]["instruments"]),
            )
            os.rename(working, bin_root)
            bin_audit = _audit_bin_root(
                bin_root,
                dataset=spec.dataset,
                cutoff=spec.cutoff,
                expected_stock_codes=set(prepared["csv"]["instruments"]),
            )

        receipt = {
            "schema_version": DAILY_MINUTE_SCHEMA,
            "spec_digest": spec.digest,
            "status": "PASS",
            "dataset": spec.dataset,
            "cutoff": spec.cutoff.isoformat(),
            "pit_spans_sha256": spec.pit_snapshot.spans_sha256,
            "csv": prepared["csv"],
            "sealed_canonical_rows": prepared["sealed_canonical_rows"],
            "indices": prepared["indices"],
            "bin": bin_audit,
            "supervised_child": child_receipt,
            "memory_contract": {
                **prepared["memory_contract"],
            },
            "safety": _zero_safety(),
        }
        _atomic_json(receipt_path, receipt)
        return DailyMinuteMaterializationReceipt(receipt)


def _write_ordered_stock_csvs(
    rows: Iterable[Mapping[str, Any]],
    *,
    csv_root: Path,
    dataset: str,
    cutoff: date,
    checkpoint: Callable[[], None],
) -> dict[str, Any]:
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    current_code: str | None = None
    current_temp: Path | None = None
    current_handle = None
    writer: csv.DictWriter | None = None
    previous_key: tuple[str, datetime] | None = None
    first_time: datetime | None = None
    last_time: datetime | None = None
    current_rows = 0
    total_rows = 0
    files: list[dict[str, Any]] = []
    ranges: dict[str, list[str]] = {}

    def close_current() -> None:
        nonlocal current_handle, current_temp, current_rows, first_time, last_time
        if current_code is None or current_handle is None or current_temp is None:
            return
        current_handle.flush()
        os.fsync(current_handle.fileno())
        current_handle.close()
        target = csv_root / f"{current_code}.csv"
        digest = sha256_file(current_temp)
        if target.exists():
            _assert_plain(target)
            if sha256_file(target) != digest:
                raise DailyMinuteMaterializationError(f"existing stock CSV conflicts: {current_code}")
        else:
            os.link(current_temp, target)
        current_temp.unlink(missing_ok=True)
        assert first_time is not None and last_time is not None
        files.append(
            {
                "instrument": current_code,
                "rows": current_rows,
                "sha256": digest,
                "size_bytes": int(target.stat().st_size),
                "start": first_time.isoformat(sep=" "),
                "end": last_time.isoformat(sep=" "),
            }
        )
        ranges[current_code] = [
            first_time.date().isoformat(),
            last_time.date().isoformat(),
        ]
        checkpoint()
        current_handle = None
        current_temp = None
        current_rows = 0
        first_time = None
        last_time = None

    try:
        for ordinal, raw in enumerate(rows):
            normalized = _normalize_row(raw, fields=fields, dataset=dataset)
            code = normalized["symbol"]
            timestamp = normalized.pop("_timestamp")
            key = (code, timestamp)
            if previous_key is not None and key <= previous_key:
                raise DailyMinuteMaterializationError(
                    "stock source rows must be globally ordered by instrument,datetime"
                )
            previous_key = key
            if timestamp.date() > cutoff:
                raise DailyMinuteMaterializationError("stock source row exceeds cutoff")
            if code != current_code:
                close_current()
                current_code = code
                descriptor, name = tempfile.mkstemp(prefix=f".{code}.", suffix=".partial.csv", dir=csv_root)
                current_temp = Path(name)
                current_handle = os.fdopen(descriptor, "w", encoding="utf-8", newline="")
                writer = csv.DictWriter(
                    current_handle,
                    fieldnames=["date", "symbol", *fields],
                    extrasaction="raise",
                )
                writer.writeheader()
            assert writer is not None
            writer.writerow(normalized)
            first_time = first_time or timestamp
            last_time = timestamp
            current_rows += 1
            total_rows += 1
        close_current()
    finally:
        if current_handle is not None and not current_handle.closed:
            current_handle.close()
        if current_temp is not None:
            current_temp.unlink(missing_ok=True)
    if total_rows <= 0:
        raise DailyMinuteMaterializationError("stock source stream is empty")
    return {
        "rows": total_rows,
        "instruments": [item["instrument"] for item in files],
        "files": files,
        "ranges": ranges,
    }


def _normalize_row(raw: Mapping[str, Any], *, fields: Sequence[str], dataset: str) -> dict[str, Any]:
    missing = [name for name in ("datetime", "instrument", *fields) if name not in raw]
    if missing:
        raise DailyMinuteMaterializationError(f"stock source row missing fields: {missing}")
    code = str(raw["instrument"]).strip().upper()
    if _SAFE_CODE.fullmatch(code) is None:
        raise DailyMinuteMaterializationError(f"invalid SH/SZ stock code: {code}")
    try:
        timestamp = datetime.fromisoformat(str(raw["datetime"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DailyMinuteMaterializationError("stock source datetime is invalid") from exc
    if timestamp.tzinfo is not None:
        timestamp = timestamp.replace(tzinfo=None)
    if dataset == "daily_bin" and timestamp.time() != datetime.min.time():
        raise DailyMinuteMaterializationError("daily source timestamp must be date-aligned")
    output: dict[str, Any] = {
        "date": (
            timestamp.date().isoformat() if dataset == "daily_bin" else timestamp.isoformat(sep=" ", timespec="seconds")
        ),
        "symbol": code,
        "_timestamp": timestamp,
    }
    for field in fields:
        try:
            value = float(raw[field])
        except (TypeError, ValueError) as exc:
            raise DailyMinuteMaterializationError(f"stock source {field} is not numeric") from exc
        if not math.isfinite(value):
            raise DailyMinuteMaterializationError(f"stock source {field} is NULL/non-finite")
        output[field] = format(value, ".12g")
    return output


def _copy_index_csvs(source_root: Path, target_root: Path) -> dict[str, Any]:
    source = _plain_root(source_root)
    files: list[dict[str, Any]] = []
    for definition in DOMESTIC_INDEX_DEFINITIONS:
        source_path = (source / f"{definition.daily_code}.csv").resolve(strict=True)
        if source not in source_path.parents or not source_path.is_file():
            raise DailyMinuteMaterializationError(f"frozen index CSV missing: {definition.daily_code}")
        _assert_plain(source_path)
        rows = 0
        first: str | None = None
        last: str | None = None
        previous: str | None = None
        with source_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != (
                "date",
                "symbol",
                *INDEX_QLIB_FIELDS,
            ):
                raise DailyMinuteMaterializationError(
                    f"frozen index CSV field contract differs: {definition.daily_code}"
                )
            for row in reader:
                observed = str(row.get("date", ""))
                if (
                    str(row.get("symbol", "")).upper() != definition.daily_code
                    or not observed
                    or (previous is not None and observed <= previous)
                ):
                    raise DailyMinuteMaterializationError(f"frozen index CSV rows differ: {definition.daily_code}")
                first = first or observed
                last = observed
                previous = observed
                rows += 1
        if rows <= 0 or first is None or last is None:
            raise DailyMinuteMaterializationError(f"frozen index CSV is empty: {definition.daily_code}")
        target = target_root / source_path.name
        expected = sha256_file(source_path)
        if target.exists():
            if sha256_file(target) != expected:
                raise DailyMinuteMaterializationError(f"index CSV target conflicts: {definition.daily_code}")
        else:
            descriptor, name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".partial", dir=target.parent)
            temporary = Path(name)
            try:
                with os.fdopen(descriptor, "wb") as output, source_path.open("rb") as input_:
                    shutil.copyfileobj(input_, output, length=1024 * 1024)
                    output.flush()
                    os.fsync(output.fileno())
                os.link(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        files.append(
            {
                "code": definition.daily_code,
                "sha256": expected,
                "size_bytes": int(target.stat().st_size),
                "rows": rows,
                "start": first,
                "end": last,
            }
        )
    return {"codes": [item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS], "files": files}


def _copy_index_csv_ranges(
    source_root: Path | None,
    target_root: Path,
    *,
    codes: Sequence[str],
    date_ranges: Sequence[tuple[date, date]],
) -> dict[str, Any]:
    requested = tuple(sorted({str(value).upper() for value in codes}))
    if not requested:
        return {"codes": [], "files": [], "rows": 0}
    if source_root is None:
        raise DailyMinuteMaterializationError("index patch source root is missing")
    definitions = {item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS}
    if not set(requested).issubset(definitions):
        raise DailyMinuteMaterializationError("index patch codes exceed authority")
    ranges = tuple(sorted(date_ranges))
    if not ranges or any(end < start for start, end in ranges):
        raise DailyMinuteMaterializationError("index patch date ranges are invalid")
    source = _plain_root(source_root)
    files: list[dict[str, Any]] = []
    total_rows = 0
    for code in requested:
        source_path = (source / f"{code}.csv").resolve(strict=True)
        if source not in source_path.parents or not source_path.is_file():
            raise DailyMinuteMaterializationError(f"index patch source missing: {code}")
        target = target_root / f"{code}.csv"
        descriptor, name = tempfile.mkstemp(prefix=f".{code}.", suffix=".partial", dir=target_root)
        temporary = Path(name)
        rows = 0
        first: str | None = None
        last: str | None = None
        try:
            with (
                source_path.open("r", encoding="utf-8", newline="") as input_,
                os.fdopen(descriptor, "w", encoding="utf-8", newline="") as output,
            ):
                reader = csv.DictReader(input_)
                if tuple(reader.fieldnames or ()) != (
                    "date",
                    "symbol",
                    *INDEX_QLIB_FIELDS,
                ):
                    raise DailyMinuteMaterializationError(f"index patch CSV field contract differs: {code}")
                writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
                writer.writeheader()
                for row in reader:
                    observed = date.fromisoformat(str(row["date"])[:10])
                    if not any(start <= observed <= end for start, end in ranges):
                        continue
                    writer.writerow(row)
                    text = str(row["date"])
                    first = first or text
                    last = text
                    rows += 1
                output.flush()
                os.fsync(output.fileno())
            if rows <= 0:
                raise DailyMinuteMaterializationError(f"index patch range contains no rows: {code}")
            os.link(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        digest = sha256_file(target)
        files.append(
            {
                "code": code,
                "rows": rows,
                "sha256": digest,
                "size_bytes": int(target.stat().st_size),
                "start": first,
                "end": last,
            }
        )
        total_rows += rows
    return {"codes": list(requested), "files": files, "rows": total_rows}


def _sealed_canonical_rows(dataset: str, csv_receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "dataset": dataset,
        "root_relative_path": f"{dataset}/csv",
        "ordered_fields": [
            "date",
            "symbol",
            *(DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS),
        ],
        "rows": int(csv_receipt["rows"]),
        "files": [
            {
                "instrument": item["instrument"],
                "relative_path": f"{item['instrument']}.csv",
                "rows": int(item["rows"]),
                "sha256": item["sha256"],
                "size_bytes": int(item["size_bytes"]),
                "start": item["start"],
                "end": item["end"],
            }
            for item in csv_receipt["files"]
        ],
    }


def _audit_prepared_csvs(root: Path, sealed: Mapping[str, Any]) -> None:
    if (
        sealed.get("schema_version") != SEALED_QLIB_CSV_ROWS_SCHEMA
        or not isinstance(sealed.get("files"), list)
        or int(sealed.get("rows", -1)) < 0
    ):
        raise DailyMinuteMaterializationError("sealed canonical CSV receipt is invalid")
    observed_rows = 0
    seen: set[str] = set()
    for item in sealed["files"]:
        if not isinstance(item, Mapping):
            raise DailyMinuteMaterializationError("sealed canonical CSV file receipt is invalid")
        instrument = str(item.get("instrument", "")).upper()
        relative = str(item.get("relative_path", ""))
        if _SAFE_CODE.fullmatch(instrument) is None or relative != f"{instrument}.csv" or instrument in seen:
            raise DailyMinuteMaterializationError("sealed canonical CSV identity differs")
        seen.add(instrument)
        path = (root / relative).resolve(strict=True)
        if path.parent != root or not path.is_file():
            raise DailyMinuteMaterializationError("sealed canonical CSV path escapes root")
        _assert_plain(path)
        if path.stat().st_size != int(item.get("size_bytes", -1)) or sha256_file(path) != item.get("sha256"):
            raise DailyMinuteMaterializationError("sealed canonical CSV bytes differ")
        rows = 0
        with path.open("rb") as handle:
            header = handle.readline(1024 * 1024 + 1)
            if not header or len(header) > 1024 * 1024:
                raise DailyMinuteMaterializationError("canonical CSV header is invalid")
            while line := handle.readline(1024 * 1024 + 1):
                if len(line) > 1024 * 1024:
                    raise DailyMinuteMaterializationError("canonical CSV line exceeds memory bound")
                rows += 1
        if rows != int(item.get("rows", -1)):
            raise DailyMinuteMaterializationError("sealed canonical CSV row count differs")
        observed_rows += rows
    if observed_rows != int(sealed["rows"]):
        raise DailyMinuteMaterializationError("sealed canonical CSV total rows differ")


def _dump_command(spec: DailyMinuteMaterializationSpec, csv_root: Path, working: Path) -> list[str]:
    return build_qlib_dump_command(
        dataset=spec.dataset,
        csv_root=csv_root,
        working_root=working,
        dump_workers=spec.dump_workers,
        toolchain=spec.toolchain,
    )


def build_qlib_dump_command(
    *,
    dataset: str,
    csv_root: Path,
    working_root: Path,
    dump_workers: int,
    toolchain: QlibDumpToolchain,
    mode: str = "dump_all",
) -> list[str]:
    """Build the allowlisted WSL dump command without launching it.

    The parent Worker uses this pure helper before passing the command to
    ``WorkerAttemptContext.run_supervised(runtime='wsl')``.  No materializer
    may invoke an unmanaged WSL process.
    """

    if dataset not in {"daily_bin", "minute_bin"}:
        raise DailyMinuteMaterializationError("dataset must be daily_bin or minute_bin")
    if type(dump_workers) is not int or not 0 < dump_workers <= 8:
        raise DailyMinuteMaterializationError("dump workers must be in [1,8]")
    if mode not in {"dump_all", "dump_update", "batched_patch", "batched_full"}:
        raise DailyMinuteMaterializationError("Qlib dump mode is not allowlisted")
    freq = "day" if dataset == "daily_bin" else "1min"
    tool = toolchain
    batch_manifest = csv_root / "batch_manifest.json"
    if mode in {"batched_patch", "batched_full"} and not batch_manifest.is_file():
        raise DailyMinuteMaterializationError("batched Qlib manifest is missing")
    if mode not in {"batched_patch", "batched_full"} and batch_manifest.exists():
        raise DailyMinuteMaterializationError("batch manifest requires a batched operation contract")
    if batch_manifest.exists():
        _assert_plain(batch_manifest)
        wrapper = (Path(__file__).resolve().parents[3] / "scripts" / "dataset_release_qlib_batched_dump.py").resolve(
            strict=True
        )
        _assert_plain(wrapper)
        child = (
            "python",
            _windows_to_wsl(wrapper),
            "--manifest",
            _windows_to_wsl(batch_manifest),
            "--dump-script",
            tool.dump_script_wsl,
            "--qlib-dir",
            _windows_to_wsl(working_root),
            "--freq",
            freq,
            "--max-workers",
            str(dump_workers),
        )
    else:
        child = (
            "python",
            tool.dump_script_wsl,
            mode,
            "--data_path",
            _windows_to_wsl(csv_root),
            "--qlib_dir",
            _windows_to_wsl(working_root),
            "--freq",
            freq,
            "--date_field_name",
            "date",
            "--symbol_field_name",
            "symbol",
            "--exclude_fields",
            "date,symbol",
            "--max_workers",
            str(dump_workers),
        )
    inner = " && ".join(
        (
            f"source {shlex.quote(tool.conda_sh)}",
            f"conda activate {shlex.quote(tool.conda_env)}",
            " ".join(shlex.quote(value) for value in child),
        )
    )
    return ["bash", "-lc", inner]


def _write_instrument_authorities(
    qlib_root: Path,
    snapshot: FrozenPitSnapshot,
    *,
    stock_ranges: Mapping[str, Sequence[str]],
    dataset: str,
    effective_start: date,
) -> None:
    instruments = qlib_root / "instruments"
    instruments.mkdir(parents=True, exist_ok=True)
    all_path = instruments / "all.txt"
    lines: list[str] = []
    for span in snapshot.spans:
        observed = stock_ranges.get(span.ts_code)
        start = max(span.eligible_start, effective_start)
        end = min(span.eligible_end, snapshot.cutoff)
        if start <= end:
            if observed is None:
                raise DailyMinuteMaterializationError(f"PIT instrument has no canonical rows: {span.ts_code}")
            observed_start = date.fromisoformat(str(observed[0])[:10])
            observed_end = date.fromisoformat(str(observed[1])[:10])
            if observed_start > start or observed_end < end:
                raise DailyMinuteMaterializationError(f"canonical rows do not cover exact PIT span: {span.ts_code}")
            lines.append(f"{span.ts_code}\t{start.isoformat()}\t{end.isoformat()}")
    if not lines:
        raise DailyMinuteMaterializationError("PIT all.txt would be empty")
    _atomic_bytes(all_path, ("\n".join(lines) + "\n").encode("utf-8"))
    if dataset == "daily_bin":
        lines = [
            f"{item.daily_code}\t{item.required_from.isoformat()}\t{snapshot.cutoff.isoformat()}"
            for item in DOMESTIC_INDEX_DEFINITIONS
        ]
        _atomic_bytes(
            instruments / "index.txt",
            ("\n".join(lines) + "\n").encode("utf-8"),
        )


def _audit_bin_root(
    root: Path,
    *,
    dataset: str,
    cutoff: date,
    expected_stock_codes: set[str],
) -> dict[str, Any]:
    root = _plain_root(root)
    frequency = "day" if dataset == "daily_bin" else "1min"
    calendar_path = root / "calendars" / f"{frequency}.txt"
    all_path = root / "instruments" / "all.txt"
    if not calendar_path.is_file() or not all_path.is_file():
        raise DailyMinuteMaterializationError("Qlib dump omits calendar/all.txt")
    calendar = [value.strip() for value in calendar_path.read_text(encoding="utf-8").splitlines() if value.strip()]
    if not calendar or calendar[-1][:10] != cutoff.isoformat():
        raise DailyMinuteMaterializationError("Qlib calendar cutoff differs")
    if calendar != sorted(set(calendar)):
        raise DailyMinuteMaterializationError("Qlib calendar is duplicate/unsorted")
    stock_lines = [
        value.split("\t", 1)[0].strip().upper()
        for value in all_path.read_text(encoding="utf-8").splitlines()
        if value.strip()
    ]
    stock_codes = set(stock_lines)
    if stock_codes != expected_stock_codes:
        raise DailyMinuteMaterializationError("Qlib all.txt differs from frozen stock stream")
    index_codes: list[str] = []
    if dataset == "daily_bin":
        index_path = root / "instruments" / "index.txt"
        if not index_path.is_file():
            raise DailyMinuteMaterializationError("daily Qlib index.txt is missing")
        index_rows = _instrument_rows(index_path)
        index_codes = list(index_rows)
        expected = {item.daily_code: [(item.required_from, cutoff)] for item in DOMESTIC_INDEX_DEFINITIONS}
        if index_rows != expected or stock_codes.intersection(index_codes):
            raise DailyMinuteMaterializationError("daily stock/index instruments are not isolated")
    feature_codes = stock_codes.union(index_codes)
    suffix = ".day.bin" if dataset == "daily_bin" else ".1min.bin"
    missing_features: list[str] = []
    for code in sorted(feature_codes):
        feature_root = root / "features" / code.lower()
        if not feature_root.is_dir() or not any(
            path.is_file() and path.name.endswith(suffix) for path in feature_root.iterdir()
        ):
            missing_features.append(code)
    if missing_features:
        raise DailyMinuteMaterializationError(f"Qlib feature files missing: {missing_features[:20]}")
    return {
        "root": str(root),
        "calendar_rows": len(calendar),
        "calendar_sha256": sha256_file(calendar_path),
        "cutoff": cutoff.isoformat(),
        "stock_instruments": len(stock_codes),
        "index_codes": index_codes,
        "feature_instruments": len(feature_codes),
        "all_txt_sha256": sha256_file(all_path),
    }


def _instrument_rows(path: Path) -> dict[str, list[tuple[date, date]]]:
    rows: dict[str, list[tuple[date, date]]] = {}
    for ordinal, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        fields = raw.split("\t")
        if len(fields) != 3:
            raise DailyMinuteMaterializationError(f"Qlib instrument row is invalid: {path.name}:{ordinal}")
        try:
            start = date.fromisoformat(fields[1])
            end = date.fromisoformat(fields[2])
        except ValueError as exc:
            raise DailyMinuteMaterializationError(f"Qlib instrument date is invalid: {path.name}:{ordinal}") from exc
        code = fields[0].strip().upper()
        values = rows.setdefault(code, [])
        if not code or start > end or (values and start <= values[-1][1]):
            raise DailyMinuteMaterializationError(f"Qlib instrument span is invalid: {path.name}:{ordinal}")
        values.append((start, end))
    return rows


def _child_receipt(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        payload = dict(value)
    elif is_dataclass(value):
        payload = asdict(value)
    elif hasattr(value, "as_dict"):
        payload = dict(value.as_dict())
    else:
        payload = {
            name: getattr(value, name)
            for name in (
                "returncode",
                "pid",
                "elapsed_seconds",
                "log_segments",
                "job_accounting",
                "wsl_readback",
                "cancelled",
                "timed_out",
            )
            if hasattr(value, name)
        }
    if "returncode" not in payload:
        raise SupervisedDumpFailed("supervised execution receipt omits returncode")
    segments = []
    for item in payload.get("log_segments") or []:
        if not isinstance(item, Mapping):
            raise SupervisedDumpFailed("supervised log segment receipt is invalid")
        segments.append(
            {
                field: item[field]
                for field in (
                    "stream",
                    "generation",
                    "size_bytes",
                    "sha256",
                    "cas_ref",
                )
                if field in item
            }
        )
    payload["log_segments"] = segments
    # result_path/log_root are attempt-local control paths and become stale
    # after deployment. Hash/size segment identities remain durable evidence.
    payload.pop("result_path", None)
    payload.pop("log_root", None)
    return payload


def _windows_to_wsl(path: Path) -> str:
    text = str(path.resolve(strict=False)).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    if not text.startswith("/"):
        raise DailyMinuteMaterializationError("cannot map candidate path into WSL")
    return text


def _plain_root(path: Path) -> Path:
    requested = Path(path).expanduser()
    if not requested.is_absolute():
        requested = requested.absolute()
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise DailyMinuteMaterializationError(f"path is not a directory: {resolved}")
    current = Path(resolved.anchor)
    if current.exists():
        _assert_plain(current)
    for part in resolved.parts[1:]:
        current = current / part
        _assert_plain(current)
    return resolved


def _assert_plain(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise DailyMinuteMaterializationError(f"path is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or (int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise DailyMinuteMaterializationError(f"path traverses symlink/reparse: {path}")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_bytes(
        path,
        (
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8"),
    )


def _atomic_bytes(path: Path, payload: bytes) -> None:
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".partial", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    if hashlib.sha256(path.read_bytes()).digest() != hashlib.sha256(payload).digest():
        raise DailyMinuteMaterializationError("atomic file readback differs")


def _preparation_receipt_matches(value: Mapping[str, Any], spec: DailyMinuteMaterializationSpec) -> bool:
    schema = value.get("schema_version")
    return schema in DAILY_MINUTE_CSV_PREPARATION_SUPPORTED_SCHEMAS and value.get("spec_digest") == (
        spec.digest if schema == DAILY_MINUTE_CSV_PREPARATION_SCHEMA else spec.legacy_digest
    )


def _materialization_receipt_matches(value: Mapping[str, Any], spec: DailyMinuteMaterializationSpec) -> bool:
    schema = value.get("schema_version")
    return schema in DAILY_MINUTE_SUPPORTED_SCHEMAS and value.get("spec_digest") == (
        spec.digest if schema == DAILY_MINUTE_SCHEMA else spec.legacy_digest
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DailyMinuteMaterializationError("materialization receipt is unreadable") from exc
    if not isinstance(value, dict):
        raise DailyMinuteMaterializationError("materialization receipt must be an object")
    return value


def _zero_safety() -> dict[str, int]:
    return {
        "database_writes": 0,
        "production_writes": 0,
        "production_deletes": 0,
        "production_pointer_changes": 0,
        "service_process_controls": 0,
    }


__all__ = [
    "DAILY_MINUTE_CSV_PREPARATION_SCHEMA",
    "DAILY_FIELDS",
    "MINUTE_FIELDS",
    "DailyMinuteMaterializationError",
    "DailyMinuteMaterializationReceipt",
    "DailyMinuteMaterializationSpec",
    "DailyMinuteMaterializer",
    "DailyMinuteBinFinalizer",
    "DailyMinuteCsvPreparationReceipt",
    "DailyMinuteCsvPreparer",
    "DailyMinuteIncrementalFinalizer",
    "DailyMinutePatchCsvPreparer",
    "DailyMinutePatchPreparationReceipt",
    "QlibDumpToolchain",
    "SEALED_QLIB_CSV_COMPOSITE_SCHEMA",
    "SEALED_QLIB_CSV_ROWS_SCHEMA",
    "SupervisedDumpFailed",
    "SupervisedExecutor",
    "build_qlib_dump_command",
    "build_composite_canonical_rows",
    "build_selective_override_canonical_rows",
]
