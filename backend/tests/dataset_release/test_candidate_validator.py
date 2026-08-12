from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.dataset_release import candidate_validator as candidate_validator_module
from backend.data_service.moneyflow_contract import (
    MONEYFLOW_FACTOR_COLUMNS,
    derive_moneyflow_factors,
)
from backend.services.dataset_release.candidate_validator import (
    CandidateComponentTransitionAuthority,
    CandidateValidationError,
    CandidateValidationSpec,
    CandidateValidator,
    _validate_composite_lineage_contract,
    _validate_moneyflow_derived_formula_parity,
    _stream_minute_csv_parity,
    _validate_bin,
)
from backend.services.dataset_release.canonical import (
    digest_named_fields,
    merkle_root_from_named_digests,
    normalize_root_relative_path,
)
from backend.services.dataset_release.component_artifact_manifest import (
    COMPONENT_ARTIFACT_FILE_SCHEMA,
    ArtifactFileEvidence,
    ArtifactPartitionEvidence,
    ComponentArtifactEvidence,
)
from backend.services.dataset_release.copy_on_write import tree_merkle
from backend.services.dataset_release.candidate_consumer_smoke import (
    CANDIDATE_CONSUMER_SMOKE_SCHEMA,
    HMM_INDEX_H5_READER_CONTRACT,
    QE_DAILY_FIELDS,
    QE_INDEX_FIELDS,
    QE_MINUTE_FIELDS,
    QE_QLIB_READER_CONTRACT,
)
from backend.services.dataset_release.daily_minute_materializer import (
    DAILY_FIELDS,
    MINUTE_FIELDS,
    SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
    SEALED_QLIB_CSV_ROWS_SCHEMA,
    build_composite_canonical_rows,
    build_selective_override_canonical_rows,
)
from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.decision import DECISION_SCHEMA_VERSION
from backend.services.dataset_release.factor_materializer import (
    FACTOR_H5_DATASETS,
    FACTOR_H5_DTYPES,
    FACTOR_H5_SCHEMAS,
    STATIC_DATASET,
    FactorBundleMaterializer,
    FactorMaterializationSpec,
    SealedFactorChunk,
)
from backend.services.dataset_release.index_materializer import IndexContextMaterializer
from backend.services.dataset_release.index_contract import (
    DOMESTIC_INDEX_DEFINITIONS,
    INDEX_QLIB_FIELDS,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.streaming_artifacts import (
    sha256_file,
    write_frame_parquet_atomic,
)
from backend.services.dataset_release.static_schema import (
    STATIC_MONEYFLOW_DERIVED_COLUMNS,
    STATIC_ORDERED_COLUMNS,
)


DATES = (date(2026, 7, 30), date(2026, 7, 31))
STOCKS = ("000001.SZ", "600000.SH")


def _pit():
    return freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": DATES[0],
                "eligible_end": DATES[-1],
                "entry_reason": None,
                "exit_reason": None,
            }
            for code in STOCKS
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=DATES[0],
        cutoff=DATES[-1],
        state_identity="fixture-pit",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )


def _index(day: date, instruments=STOCKS) -> pd.MultiIndex:
    return pd.MultiIndex.from_product([[pd.Timestamp(day)], instruments], names=["datetime", "instrument"])


def _static_columns() -> tuple[str, ...]:
    return STATIC_ORDERED_COLUMNS


def _factor_candidate(root: Path) -> dict:
    source = root.parent / "sealed-factor-source"
    source.mkdir()
    static_columns = _static_columns()
    fixture_moneyflow: list[pd.DataFrame] = []
    fixture_daily: list[pd.DataFrame] = []
    for ordinal, day in enumerate(DATES):
        index = _index(day)
        base = np.asarray([ordinal + 1.0, ordinal + 2.0], dtype=np.float32)
        fixture_moneyflow.append(
            pd.DataFrame(
                {
                    field: (base + MONEYFLOW_FACTOR_COLUMNS.index(field)).astype(np.float32)
                    for field in FACTOR_H5_SCHEMAS["moneyflow"]
                },
                index=index,
            )
        )
        fixture_daily.append(
            pd.DataFrame(
                {
                    field: (base + position).astype(np.float32)
                    for position, field in enumerate(FACTOR_H5_SCHEMAS["daily_pv"])
                },
                index=index,
            )
        )
    derived_moneyflow = derive_moneyflow_factors(pd.concat(fixture_moneyflow), pd.concat(fixture_daily))
    chunks: list[SealedFactorChunk] = []
    for dataset in (*FACTOR_H5_DATASETS, STATIC_DATASET):
        for ordinal, day in enumerate(DATES):
            index = _index(day)
            base = np.asarray([ordinal + 1.0, ordinal + 2.0], dtype=np.float32)
            if dataset == STATIC_DATASET:
                values = {
                    field: (
                        np.asarray([1, -1], dtype=np.int16)
                        if field == "l2_code_id"
                        else (
                            base + MONEYFLOW_FACTOR_COLUMNS.index(field)
                            if field in MONEYFLOW_FACTOR_COLUMNS
                            else (
                                derived_moneyflow.loc[index, field].to_numpy(dtype=np.float32)
                                if field in STATIC_MONEYFLOW_DERIVED_COLUMNS
                                else base + position
                            )
                        )
                    )
                    for position, field in enumerate(static_columns)
                }
                frame = pd.DataFrame(values, index=index)
                frame = frame.loc[:, list(static_columns)]
            else:
                frame = pd.DataFrame(
                    {
                        field: (
                            np.asarray([1, -1], dtype=np.int16)
                            if FACTOR_H5_DTYPES[dataset][field] == "int16"
                            else (
                                base + MONEYFLOW_FACTOR_COLUMNS.index(field)
                                if dataset == "moneyflow"
                                else base + position
                            ).astype(np.float32)
                        )
                        for position, field in enumerate(FACTOR_H5_SCHEMAS[dataset])
                    },
                    index=index,
                )
            relative = Path(dataset) / f"part-{ordinal}.parquet"
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            write_frame_parquet_atomic(frame, path, row_group_size=1)
            chunks.append(
                SealedFactorChunk(
                    dataset=dataset,
                    partition_key=f"part-{ordinal}",
                    relative_path=relative.as_posix(),
                    sha256=sha256_file(path),
                    rows=len(frame),
                    ordered_columns=tuple(str(value) for value in frame.columns),
                )
            )
    spec = FactorMaterializationSpec(
        source_root=source,
        staging_root=root,
        chunks=tuple(chunks),
        static_ordered_columns=static_columns,
        row_group_rows=2,
    )
    return dict(FactorBundleMaterializer().materialize(spec).receipt)


class _IndexSource:
    def trading_dates(self, _start, _end):
        return DATES

    def database_rows(self, definition, _start, _end):
        return [
            {
                "ts_code": definition.daily_code,
                "trade_date": day,
                "open": 100.0 + ordinal,
                "high": 101.0 + ordinal,
                "low": 99.0 + ordinal,
                "close": 100.5 + ordinal,
                "pre_close": 99.5 + ordinal,
                "pct_chg": 1.0,
                "vol": 10.0 + ordinal,
                "amount": 20.0 + ordinal,
            }
            for ordinal, day in enumerate(DATES)
        ]

    def provider_rows(self, _definition, _start, _end):
        raise AssertionError("provider fallback is not required by this fixture")


def _write_bin(path: Path, values: list[float], start: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    np.asarray([float(start), *values], dtype="<f4").tofile(path)


def _instrument_file(root: Path, *, indices=()) -> None:
    instruments = root / "instruments"
    instruments.mkdir(parents=True, exist_ok=True)
    (instruments / "all.txt").write_text(
        "".join(f"{code}\t{DATES[0].isoformat()}\t{DATES[-1].isoformat()}\n" for code in STOCKS),
        encoding="utf-8",
    )
    if indices:
        definitions = {item.daily_code: item for item in DOMESTIC_INDEX_DEFINITIONS}
        (instruments / "index.txt").write_text(
            "".join(
                f"{code}\t{definitions[code].required_from.isoformat()}\t{DATES[-1].isoformat()}\n" for code in indices
            ),
            encoding="utf-8",
        )


def _daily_bin(root: Path, factor_root: Path, index_root: Path, index_codes) -> None:
    (root / "calendars").mkdir(parents=True)
    (root / "calendars" / "day.txt").write_text("".join(f"{day.isoformat()}\n" for day in DATES), encoding="utf-8")
    _instrument_file(root, indices=index_codes)
    daily = pd.read_hdf(factor_root / "daily_pv.h5", key="data")
    csv_root = root.parent / "csv"
    csv_root.mkdir()
    sealed_files = []
    sealed_rows = 0
    for code, group in daily.groupby(level="instrument", sort=False):
        close_values = pd.to_numeric(group["close"]).astype(float).tolist()
        extras = {
            "up_limit_price": [value * 1.1 for value in close_values],
            "down_limit_price": [value * 0.9 for value in close_values],
            "prev_close": [close_values[0], close_values[0]],
            "limit_up": [0.0, 0.0],
            "limit_down": [0.0, 0.0],
        }
        for field in DAILY_FIELDS:
            _write_bin(
                root / "features" / str(code).lower() / f"{field}.day.bin",
                (pd.to_numeric(group[field]).astype(float).tolist() if field in group else extras[field]),
            )
        csv_path = csv_root / f"{code}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["date", "symbol", *DAILY_FIELDS])
            writer.writeheader()
            for ordinal, day in enumerate(DATES):
                writer.writerow(
                    {
                        "date": day.isoformat(),
                        "symbol": code,
                        **{
                            field: (
                                float(pd.to_numeric(group[field]).iloc[ordinal])
                                if field in group
                                else float(extras[field][ordinal])
                            )
                            for field in DAILY_FIELDS
                        },
                    }
                )
        sealed_files.append(
            {
                "instrument": str(code),
                "relative_path": f"{code}.csv",
                "rows": len(group),
                "sha256": sha256_file(csv_path),
                "size_bytes": csv_path.stat().st_size,
                "start": f"{DATES[0].isoformat()} 00:00:00",
                "end": f"{DATES[-1].isoformat()} 00:00:00",
            }
        )
        sealed_rows += len(group)
    index = pd.read_hdf(index_root / "index_daily.h5", key="data")
    mapping = {
        "open": "idx_open_point",
        "high": "idx_high_point",
        "low": "idx_low_point",
        "close": "idx_close_point",
        "volume": "idx_volume_share_equiv",
        "amount": "idx_amount_cny",
        "prev_close": "idx_pre_close_point",
    }
    for code, group in index.groupby(level="instrument", sort=False):
        for field, source in mapping.items():
            _write_bin(
                root / "features" / str(code).lower() / f"{field}.day.bin",
                pd.to_numeric(group[source]).astype(float).tolist(),
            )
        neutral = pd.to_numeric(group["idx_pre_close_point"]).astype(float).tolist()
        for field in ("up_limit_price", "down_limit_price"):
            _write_bin(
                root / "features" / str(code).lower() / f"{field}.day.bin",
                neutral,
            )
        for field in ("limit_up", "limit_down"):
            _write_bin(
                root / "features" / str(code).lower() / f"{field}.day.bin",
                [0.0] * len(group),
            )
        _write_bin(
            root / "features" / str(code).lower() / "factor.day.bin",
            [1.0] * len(group),
        )
    sealed = {
        "schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "dataset": "daily_bin",
        "root_relative_path": "daily_bin/csv",
        "ordered_fields": ["date", "symbol", *DAILY_FIELDS],
        "rows": sealed_rows,
        "files": sealed_files,
    }
    (root.parent / "materialization_receipt.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "dataset": "daily_bin",
                "sealed_canonical_rows": sealed,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _minute_calendar() -> list[str]:
    result: list[str] = []
    for day in DATES:
        morning = [datetime.combine(day, time(9, 31)) + timedelta(minutes=value) for value in range(120)]
        afternoon = [datetime.combine(day, time(13, 1)) + timedelta(minutes=value) for value in range(120)]
        result.extend(value.isoformat(sep=" ", timespec="seconds") for value in (*morning, *afternoon))
    return result


def _minute_bin(root: Path) -> dict:
    (root / "calendars").mkdir(parents=True)
    calendar = _minute_calendar()
    (root / "calendars" / "1min.txt").write_text("\n".join(calendar) + "\n", encoding="utf-8")
    _instrument_file(root)
    csv_root = root.parent / "csv"
    csv_root.mkdir()
    files = []
    for code in STOCKS:
        columns = {
            field: [float(value + position) for value in range(len(calendar))]
            for position, field in enumerate(MINUTE_FIELDS)
        }
        for field, values in columns.items():
            _write_bin(root / "features" / code.lower() / f"{field}.1min.bin", values)
        csv_path = csv_root / f"{code}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["date", "symbol", *MINUTE_FIELDS])
            writer.writeheader()
            for ordinal, timestamp in enumerate(calendar):
                writer.writerow(
                    {
                        "date": timestamp,
                        "symbol": code,
                        **{field: columns[field][ordinal] for field in MINUTE_FIELDS},
                    }
                )
        files.append(
            {
                "instrument": code,
                "relative_path": f"{code}.csv",
                "rows": len(calendar),
                "sha256": sha256_file(csv_path),
                "size_bytes": csv_path.stat().st_size,
                "start": calendar[0],
                "end": calendar[-1],
            }
        )
    return {
        "schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "dataset": "minute_bin",
        "root_relative_path": "minute_bin/csv",
        "ordered_fields": ["date", "symbol", *MINUTE_FIELDS],
        "rows": len(calendar) * len(STOCKS),
        "files": files,
    }


def _composite_source(root: Path, source: dict) -> dict:
    dataset = str(source["dataset"])
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    segments: list[dict] = []
    summaries: list[dict] = []
    delta_root = root / dataset / "csv_deltas" / "202607"
    delta_root.mkdir(parents=True, exist_ok=True)
    for item in source["files"]:
        instrument = str(item["instrument"])
        source_path = root / dataset / "csv" / str(item["relative_path"])
        with source_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
        split = len(rows) // 2
        values = (
            (root / dataset / "csv", rows[:split]),
            (delta_root, rows[split:]),
        )
        code_segments: list[dict] = []
        for segment_root, segment_rows in values:
            path = source_path if segment_root.name == "csv" else segment_root / f"{instrument.casefold()}.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["date", "symbol", *fields])
                writer.writeheader()
                writer.writerows(segment_rows)
            start = str(segment_rows[0]["date"])
            end = str(segment_rows[-1]["date"])
            if dataset == "daily_bin":
                start = f"{start} 00:00:00"
                end = f"{end} 00:00:00"
            segment = {
                "instrument": instrument,
                "root_relative_path": segment_root.relative_to(root).as_posix(),
                "relative_path": path.name,
                "rows": len(segment_rows),
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
                "start": start,
                "end": end,
            }
            segments.append(segment)
            code_segments.append(segment)
        summaries.append(
            {
                "instrument": instrument,
                "rows": len(rows),
                "segments": len(code_segments),
                "start": code_segments[0]["start"],
                "end": code_segments[-1]["end"],
            }
        )
    composite = {
        "schema_version": SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
        "dataset": dataset,
        "ordered_fields": ["date", "symbol", *fields],
        "rows": sum(int(item["rows"]) for item in summaries),
        "files": summaries,
        "segments": segments,
        "merge_contract": "instrument_datetime_strict_append_segments_v1",
    }
    _write_canonical_namespace_manifest(
        root,
        dataset=dataset,
        namespace="csv_deltas",
        key="202607",
        canonical=composite,
    )
    return composite


def _write_canonical_namespace_manifest(
    root: Path,
    *,
    dataset: str,
    namespace: str,
    key: str,
    canonical: dict,
) -> None:
    phase = "tail" if namespace == "csv_deltas" else "override"
    component_action = "INCREMENTAL" if phase == "tail" else "SELECTIVE_REBUILD"
    namespace_root = root / dataset / namespace / key
    path = namespace_root / "manifest.json"
    active_paths = {
        (
            str(item["root_relative_path"]).casefold(),
            str(item["relative_path"]).casefold(),
        )
        for item in canonical["segments"]
    }
    inventory = []
    for csv_path in sorted(namespace_root.glob("*.csv"), key=lambda value: value.name):
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        instrument = str(rows[0]["symbol"]).upper() if rows else csv_path.stem.upper()
        start = str(rows[0]["date"]) if rows else None
        end = str(rows[-1]["date"]) if rows else None
        if rows and dataset == "daily_bin":
            start = f"{start} 00:00:00"
            end = f"{end} 00:00:00"
        root_relative = namespace_root.relative_to(root).as_posix()
        inventory.append(
            {
                "instrument": instrument,
                "relative_path": csv_path.name,
                "rows": len(rows),
                "sha256": sha256_file(csv_path),
                "size_bytes": csv_path.stat().st_size,
                "start": start,
                "end": end,
                "active": ((root_relative.casefold(), csv_path.name.casefold()) in active_paths),
            }
        )
    path.write_text(
        json.dumps(
            {
                "schema_version": "dataset_release_csv_segment_manifest_v2",
                "dataset": dataset,
                "component_action": component_action,
                "phase": phase,
                "segment_key": key,
                "files": inventory,
                "canonical": canonical,
                "patch_actual_work": {"fixture": True},
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _override_source(root: Path, source: dict, *, instrument: str) -> dict:
    dataset = str(source["dataset"])
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    rows: list[dict[str, str]] = []
    superseded = [item for item in source["segments"] if str(item["instrument"]).upper() == instrument]
    for item in superseded:
        path = root / str(item["root_relative_path"]) / str(item["relative_path"])
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows.extend(csv.DictReader(handle))
    key = "0123456789abcdef"
    override_root = root / dataset / "csv_overrides" / key
    override_root.mkdir(parents=True, exist_ok=True)
    path = override_root / f"{instrument.casefold()}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "symbol", *fields])
        writer.writeheader()
        writer.writerows(rows)
    start = str(rows[0]["date"])
    end = str(rows[-1]["date"])
    if dataset == "daily_bin":
        start = f"{start} 00:00:00"
        end = f"{end} 00:00:00"
    result = dict(
        build_selective_override_canonical_rows(
            dataset=dataset,
            baseline=source,
            patch_preparation={
                "csv": {
                    "rows": len(rows),
                    "files": [
                        {
                            "instrument": instrument,
                            "rows": len(rows),
                            "sha256": sha256_file(path),
                            "size_bytes": path.stat().st_size,
                            "start": start,
                            "end": end,
                        }
                    ],
                }
            },
            override_root_relative_path=f"{dataset}/csv_overrides/{key}",
            invalidation_scopes=[
                {
                    "kind": "qfq_denominator_change",
                    "instrument": instrument,
                    "start": DATES[0].isoformat(),
                    "end": DATES[-1].isoformat(),
                }
            ],
        )
    )
    _write_canonical_namespace_manifest(
        root,
        dataset=dataset,
        namespace="csv_overrides",
        key=key,
        canonical=result,
    )
    return result


def _write_daily_segment(
    root: Path,
    *,
    root_relative: str,
    instrument: str,
    days: tuple[date, ...],
) -> dict:
    target_root = root / root_relative
    target_root.mkdir(parents=True, exist_ok=True)
    path = target_root / f"{instrument.casefold()}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "symbol", *DAILY_FIELDS])
        writer.writeheader()
        for ordinal, day in enumerate(days):
            writer.writerow(
                {
                    "date": day.isoformat(),
                    "symbol": instrument,
                    **{field: float(ordinal + position + 1) for position, field in enumerate(DAILY_FIELDS)},
                }
            )
    return {
        "instrument": instrument,
        "relative_path": path.name,
        "rows": len(days),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
        "start": f"{days[0].isoformat()} 00:00:00",
        "end": f"{days[-1].isoformat()} 00:00:00",
    }


def _write_baseline_materialization_receipt(component_root: Path, source: dict) -> None:
    (component_root / "materialization_receipt.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "dataset": "daily_bin",
                "sealed_canonical_rows": source,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _fixture_component_evidence(
    component_root: Path,
    *,
    component: Component = Component.DAILY_BIN,
) -> ComponentArtifactEvidence:
    files, merkle = tree_merkle(component_root)
    file_rows: list[ArtifactFileEvidence] = []
    for item in files:
        relative = normalize_root_relative_path(item.relative_path)
        identity = digest_named_fields(
            COMPONENT_ARTIFACT_FILE_SCHEMA,
            {
                "relative_path": relative,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "instrument": None,
            },
        )
        file_rows.append(
            ArtifactFileEvidence(
                relative_path=relative,
                size_bytes=item.size_bytes,
                sha256=item.sha256,
                instrument=None,
                file_identity=identity,
            )
        )
    file_rows.sort(key=lambda item: item.relative_path)
    aggregate = merkle_root_from_named_digests(
        "dataset_release_component_files_v1",
        ((item.relative_path, item.file_identity) for item in file_rows),
    )
    partition = ArtifactPartitionEvidence(
        partition_key="full-profile",
        source_partition_identities=("fixture-source",),
        dependency_edges=("fixture->daily_bin",),
        instruments=(STOCKS[0],),
        start=date(2026, 7, 31),
        end=date(2026, 9, 1),
        files=tuple(file_rows),
        partition_identity=digest_named_fields(
            "fixture_partition_v1", {"files": [item.as_dict() for item in file_rows]}
        ),
    )
    component_identity = digest_named_fields("fixture_component_v1", {"files": [item.as_dict() for item in file_rows]})
    return ComponentArtifactEvidence(
        component=component,
        status="COMPLETE",
        reason_code=None,
        component_root_relative_path={
            Component.DAILY_BIN: "daily_bin",
            Component.MINUTE_BIN: "minute_bin",
            Component.FACTOR_H5_STATIC: "factor_bundle",
            Component.DOMESTIC_INDEX_CONTEXT: "index_context",
        }[component],
        artifact_partitions=(partition,),
        component_identity=component_identity,
        file_identity=aggregate,
        component_manifest_root=digest_named_fields("fixture_component_manifest_v1", {"component": component_identity}),
        filesystem_tree_merkle=merkle,
    )


def _transition_context(
    baseline_root: Path,
    *,
    action: ComponentAction,
    create: list[str],
    scopes: list[dict],
):
    evidence = _fixture_component_evidence(baseline_root / "daily_bin")
    source = json.loads((baseline_root / "daily_bin" / "materialization_receipt.json").read_text(encoding="utf-8"))[
        "sealed_canonical_rows"
    ]
    return candidate_validator_module._CanonicalTransitionContext(
        component=Component.DAILY_BIN,
        action=action,
        baseline_root=baseline_root / "daily_bin",
        baseline_source=source,
        baseline_evidence=evidence,
        frozen_reuse={
            "create_new_targets": create,
            "invalidation_scopes": scopes,
        },
        authorized_create_paths=frozenset(create),
        invalidation_scopes=tuple(scopes),
    )


def _append_delta_transition(
    tmp_path: Path,
    *,
    baseline_root: Path,
    source: dict,
    day: date,
) -> tuple[Path, dict, object]:
    candidate = tmp_path / f"candidate-{day:%Y%m%d}"
    shutil.copytree(baseline_root, candidate)
    key = day.strftime("%Y%m")
    item = _write_daily_segment(
        candidate,
        root_relative=f"daily_bin/csv_deltas/{key}",
        instrument=STOCKS[0],
        days=(day,),
    )
    current = dict(
        build_composite_canonical_rows(
            dataset="daily_bin",
            baseline=source,
            patch_preparation={"csv": {"rows": 1, "files": [item]}},
            delta_root_relative_path=f"daily_bin/csv_deltas/{key}",
        )
    )
    _write_canonical_namespace_manifest(
        candidate,
        dataset="daily_bin",
        namespace="csv_deltas",
        key=key,
        canonical=current,
    )
    create = [
        f"csv_deltas/{key}/{STOCKS[0].casefold()}.csv",
        f"csv_deltas/{key}/manifest.json",
    ]
    context = _transition_context(
        baseline_root,
        action=ComponentAction.INCREMENTAL,
        create=create,
        scopes=[
            {
                "kind": "monthly_tail_extension",
                "source_partition": f"fixture:{key}",
                "extended_from": "fixture:prior",
                "new_months": [f"{day:%Y-%m}"],
            }
        ],
    )
    return candidate, current, context


def _override_transition(
    tmp_path: Path,
    *,
    baseline_root: Path,
    source: dict,
    days: tuple[date, ...],
    candidate_name: str = "candidate-override",
    scopes: list[dict] | None = None,
) -> tuple[Path, dict, object]:
    candidate = tmp_path / candidate_name
    shutil.copytree(baseline_root, candidate)
    scopes = scopes or [
        {
            "kind": "qfq_historical_numerator_revision",
            "instrument": STOCKS[0],
            "months": ["2026-07"],
            "downstream_observations": 19,
            "fallback_scope": "instrument_full_history",
        }
    ]
    key = digest_named_fields(
        "dataset_release_csv_selective_override_v1",
        {
            "component": Component.DAILY_BIN.value,
            "cutoff": days[-1],
            "codes": [STOCKS[0]],
            "scopes": scopes,
        },
    )[:16]
    item = _write_daily_segment(
        candidate,
        root_relative=f"daily_bin/csv_overrides/{key}",
        instrument=STOCKS[0],
        days=days,
    )
    current = dict(
        build_selective_override_canonical_rows(
            dataset="daily_bin",
            baseline=source,
            patch_preparation={"csv": {"rows": len(days), "files": [item]}},
            override_root_relative_path=f"daily_bin/csv_overrides/{key}",
            invalidation_scopes=scopes,
        )
    )
    _write_canonical_namespace_manifest(
        candidate,
        dataset="daily_bin",
        namespace="csv_overrides",
        key=key,
        canonical=current,
    )
    create = [
        f"csv_overrides/{key}/{STOCKS[0].casefold()}.csv",
        f"csv_overrides/{key}/manifest.json",
    ]
    context = _transition_context(
        baseline_root,
        action=ComponentAction.SELECTIVE_REBUILD,
        create=create,
        scopes=scopes,
    )
    return candidate, current, context


def _durable_override_fixture(tmp_path: Path):
    baseline_v1 = tmp_path / "baseline-v1"
    (baseline_v1 / "daily_bin" / "csv").mkdir(parents=True)
    base_item = _write_daily_segment(
        baseline_v1,
        root_relative="daily_bin/csv",
        instrument=STOCKS[0],
        days=(date(2026, 7, 31),),
    )
    source: dict = {
        "schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "dataset": "daily_bin",
        "root_relative_path": "daily_bin/csv",
        "ordered_fields": ["date", "symbol", *DAILY_FIELDS],
        "rows": 1,
        "files": [base_item],
    }
    _write_baseline_materialization_receipt(baseline_v1 / "daily_bin", source)
    candidate_aug, source_aug, context_aug = _append_delta_transition(
        tmp_path,
        baseline_root=baseline_v1,
        source=source,
        day=date(2026, 8, 3),
    )
    _write_baseline_materialization_receipt(candidate_aug / "daily_bin", source_aug)
    candidate_sep, source_sep, context_sep = _append_delta_transition(
        tmp_path,
        baseline_root=candidate_aug,
        source=source_aug,
        day=date(2026, 9, 1),
    )
    _write_baseline_materialization_receipt(candidate_sep / "daily_bin", source_sep)
    candidate_final, source_final, context_final = _override_transition(
        tmp_path,
        baseline_root=candidate_sep,
        source=source_sep,
        days=(date(2026, 7, 31), date(2026, 8, 3), date(2026, 9, 1)),
    )
    return (
        (candidate_aug, source_aug, context_aug, date(2026, 8, 3)),
        (candidate_sep, source_sep, context_sep, date(2026, 9, 1)),
        (candidate_final, source_final, context_final, date(2026, 9, 1)),
    )


def _three_month_override_lineage(root: Path, *, final_historical_override: bool) -> tuple[dict, dict[str, str]]:
    dataset = "daily_bin"
    instrument = STOCKS[0]
    days = (date(2026, 7, 31), date(2026, 8, 3), date(2026, 9, 1))
    base_item = _write_daily_segment(
        root,
        root_relative=f"{dataset}/csv",
        instrument=instrument,
        days=days[:1],
    )
    baseline = {
        "schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "dataset": dataset,
        "root_relative_path": f"{dataset}/csv",
        "ordered_fields": ["date", "symbol", *DAILY_FIELDS],
        "rows": 1,
        "files": [base_item],
    }
    first_key = "0123456789abcdef"
    first_override = _write_daily_segment(
        root,
        root_relative=f"{dataset}/csv_overrides/{first_key}",
        instrument=instrument,
        days=days[:1],
    )
    source = dict(
        build_selective_override_canonical_rows(
            dataset=dataset,
            baseline=baseline,
            patch_preparation={"csv": {"rows": 1, "files": [first_override]}},
            override_root_relative_path=(f"{dataset}/csv_overrides/{first_key}"),
            invalidation_scopes=[
                {
                    "kind": "qfq_denominator_change",
                    "instrument": instrument,
                    "start": days[0].isoformat(),
                    "end": days[0].isoformat(),
                }
            ],
        )
    )
    _write_canonical_namespace_manifest(
        root,
        dataset=dataset,
        namespace="csv_overrides",
        key=first_key,
        canonical=source,
    )
    for key, day in (("202608", days[1]), ("202609", days[2])):
        item = _write_daily_segment(
            root,
            root_relative=f"{dataset}/csv_deltas/{key}",
            instrument=instrument,
            days=(day,),
        )
        source = dict(
            build_composite_canonical_rows(
                dataset=dataset,
                baseline=source,
                patch_preparation={"csv": {"rows": 1, "files": [item]}},
                delta_root_relative_path=f"{dataset}/csv_deltas/{key}",
            )
        )
        _write_canonical_namespace_manifest(
            root,
            dataset=dataset,
            namespace="csv_deltas",
            key=key,
            canonical=source,
        )
    immutable_paths = {
        item.relative_to(root).as_posix(): sha256_file(item) for item in sorted((root / dataset).rglob("*.csv"))
    }
    if final_historical_override:
        final_key = "fedcba9876543210"
        final_item = _write_daily_segment(
            root,
            root_relative=f"{dataset}/csv_overrides/{final_key}",
            instrument=instrument,
            days=days,
        )
        source = dict(
            build_selective_override_canonical_rows(
                dataset=dataset,
                baseline=source,
                patch_preparation={"csv": {"rows": len(days), "files": [final_item]}},
                override_root_relative_path=(f"{dataset}/csv_overrides/{final_key}"),
                invalidation_scopes=[
                    {
                        "kind": "qfq_historical_numerator_revision",
                        "instrument": instrument,
                        "months": ["2026-07"],
                        "downstream_observations": 0,
                        "fallback_scope": "changed_months",
                    }
                ],
            )
        )
        _write_canonical_namespace_manifest(
            root,
            dataset=dataset,
            namespace="csv_overrides",
            key=final_key,
            canonical=source,
        )
    return source, immutable_paths


def _moneyflow_formula_bundle(tmp_path: Path, *, rows: int = 25) -> Path:
    root = tmp_path / "moneyflow-formula-bundle"
    root.mkdir()
    dates = pd.bdate_range("2026-01-05", periods=rows)
    index = pd.MultiIndex.from_product(
        [dates, ["000001.SZ", "600000.SH"]],
        names=["datetime", "instrument"],
    )
    ordinal = np.arange(len(index), dtype=np.float64)
    moneyflow = pd.DataFrame(
        {
            field: (ordinal * 100.0 + position + 1.0).astype(np.float32)
            for position, field in enumerate(FACTOR_H5_SCHEMAS["moneyflow"])
        },
        index=index,
    )
    daily = pd.DataFrame(
        {
            "open": (10.0 + ordinal).astype(np.float32),
            "high": (11.0 + ordinal).astype(np.float32),
            "low": (9.0 + ordinal).astype(np.float32),
            "close": (10.5 + ordinal).astype(np.float32),
            "volume": (1_000_000.0 + ordinal * 10.0).astype(np.float32),
            "amount": (100_000_000.0 + ordinal * 100.0).astype(np.float32),
            "factor": (1.0 + ordinal * 0.001).astype(np.float32),
        },
        index=index,
    )
    derived = (
        derive_moneyflow_factors(moneyflow, daily).loc[:, list(STATIC_MONEYFLOW_DERIVED_COLUMNS)].astype("float32")
    )
    moneyflow.to_hdf(root / "moneyflow.h5", key="data", format="table", data_columns=True)
    daily.to_hdf(root / "daily_pv.h5", key="data", format="table", data_columns=True)
    derived.to_parquet(root / "static_factors.parquet", row_group_size=3)
    return root


def _candidate(tmp_path: Path, dataset_profile):
    root = tmp_path / "candidate-staging"
    root.mkdir()
    factor_receipt = _factor_candidate(root)
    index = IndexContextMaterializer(_IndexSource()).materialize(
        root / "index_context", cutoff=DATES[-1], row_group_rows=2
    )
    _daily_bin(
        root / "daily_bin" / "qlib",
        root / "factor_bundle",
        root / "index_context",
        dataset_profile.index_codes,
    )
    minute_source = _minute_bin(root / "minute_bin" / "qlib")
    minute_materialization = {
        "status": "PASS",
        "dataset": "minute_bin",
        "sealed_canonical_rows": minute_source,
    }
    (root / "minute_bin" / "materialization_receipt.json").write_text(
        json.dumps(
            minute_materialization,
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    daily_materialization = json.loads(
        (root / "daily_bin" / "materialization_receipt.json").read_text(encoding="utf-8")
    )
    for dataset, materialization in (
        ("daily_bin", daily_materialization),
        ("minute_bin", minute_materialization),
    ):
        preparation = {
            "schema_version": "fixture_csv_preparation_v1",
            "status": "PASS",
            "dataset": dataset,
            "sealed_canonical_rows": materialization["sealed_canonical_rows"],
        }
        (root / dataset / "csv_preparation_receipt.json").write_text(
            json.dumps(preparation, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
    materialization = json.loads(
        (root / "index_context" / "index_materialization_receipt.json").read_text(encoding="utf-8")
    )
    index_receipt = {
        "schema_version": "dataset_release_index_materialization_v1",
        "status": "PASS",
        "rows": index.rows,
        "provider_fill_rows": index.provider_fill_rows,
        "contract_digest": index.contract_digest,
        "details": dict(index.details),
        "root_relative_path": "index_context",
        "h5_relative_path": "index_context/index_daily.h5",
        "parquet_relative_path": "index_context/index_context.parquet",
        "database_writes": 0,
        "production_writes": 0,
    }
    assert materialization["rows"] == index_receipt["rows"]
    return root, factor_receipt, index_receipt, minute_source


def _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile):
    cutoff = DATES[-1].isoformat()
    minute_receipt = {
        "status": "PASS",
        "dataset": "minute_bin",
        "sealed_canonical_rows": minute_source,
    }

    def feature(fields, instruments, timestamps):
        keys = sorted(f"{instrument}|{timestamp}" for instrument in instruments for timestamp in timestamps)
        return {
            "rows": len(keys),
            "fields": list(fields),
            "start": cutoff,
            "end": cutoff,
            "finite_values": len(keys) * len(fields),
            "max_abs_value": 1.0,
            "unique_keys": len(keys),
            "instruments": sorted(instruments),
            "first_timestamp": min(timestamps),
            "last_timestamp": max(timestamps),
            "key_digest": hashlib.sha256(json.dumps(keys, separators=(",", ":")).encode("utf-8")).hexdigest(),
        }

    minute_timestamps = _minute_calendar()[-240:]
    external_smoke = {
        "schema_version": CANDIDATE_CONSUMER_SMOKE_SCHEMA,
        "status": "PASS",
        "execution_kind": "fixture_contract_test",
        "profile": dataset_profile.profile,
        "cutoff": cutoff,
        "stage_timeout_seconds": dataset_profile.stage_timeouts_seconds["consumer"],
        "identity": {
            "run_id": "fixture-run",
            "attempt_id": "fixture-attempt",
            "attempt_fence": 1,
            "release_id": "fixture-release",
            "release_digest": "a" * 64,
            "staging_relative_path": ".staging/fixture-release",
        },
        "qe": {
            "status": "PASS",
            "reader_contract": QE_QLIB_READER_CONTRACT,
            "qlib_init_provider_frequencies": ["1min", "day"],
            "stock_instrument": STOCKS[0],
            "daily": feature(QE_DAILY_FIELDS, STOCKS[:1], (cutoff,)),
            "minute": feature(
                QE_MINUTE_FIELDS,
                STOCKS[:1],
                minute_timestamps,
            ),
            "indices": {
                **feature(QE_INDEX_FIELDS, dataset_profile.index_codes, (cutoff,)),
                "codes": list(dataset_profile.index_codes),
            },
            "benchmark": {
                **feature(("$close/Ref($close,1)-1",), ("000300.SH",), (cutoff,)),
                "code": "000300.SH",
            },
        },
        "hmm_index_contract": {
            "status": "PASS",
            "reader_contract": HMM_INDEX_H5_READER_CONTRACT,
            "schema_version": "qe_index_context_v1",
            "universe_version": "qe_hmm_domestic_core_v1",
            "benchmark": "000300.SH",
            "fields": ["idx_close_point", "idx_return_1d"],
            "rows": 2,
            "cutoff_rows": 1,
            "cutoff": cutoff,
            "existing_hmm_consumer_activation": "not_activated_not_switched",
        },
        "consumer_activation": {
            "qe_candidate": "validated_not_activated",
            "existing_hmm": "not_activated_not_switched",
        },
        "safety": {
            "database_writes": 0,
            "provider_database_writes": 0,
            "production_writes": 0,
            "production_deletes": 0,
            "production_pointer_changes": 0,
            "service_process_controls": 0,
        },
    }
    action_entries = {
        component.value: {
            "component": component.value,
            "partition_key": "all",
            "action": ComponentAction.FULL_REBUILD.value,
            "reason": "fixture full rebuild",
            "changed_fingerprints": [],
            "invalidation_edges": [],
            "estimated_work": {},
            "frozen_reuse": None,
        }
        for component in Component
    }
    transition_authority = {
        component.value: CandidateComponentTransitionAuthority(
            component=component,
            action=ComponentAction.FULL_REBUILD,
            action_entry=action_entries[component.value],
        )
        for component in Component
    }
    action_plan_digest = digest_named_fields(
        DECISION_SCHEMA_VERSION,
        {
            "actions": sorted(
                action_entries.values(),
                key=lambda value: (value["component"], value["partition_key"]),
            )
        },
    )
    return CandidateValidationSpec(
        candidate_root=root,
        profile=dataset_profile,
        cutoff=DATES[-1],
        trading_dates=DATES,
        pit_snapshot=_pit(),
        factor_receipt=factor_receipt,
        daily_receipt=json.loads((root / "daily_bin" / "materialization_receipt.json").read_text(encoding="utf-8")),
        minute_receipt=minute_receipt,
        daily_materialization_receipt_file=json.loads(
            (root / "daily_bin" / "materialization_receipt.json").read_text(encoding="utf-8")
        ),
        minute_materialization_receipt_file=minute_receipt,
        index_materialization_receipt_file=json.loads(
            (root / "index_context" / "index_materialization_receipt.json").read_text(encoding="utf-8")
        ),
        daily_preparation_receipt=json.loads(
            (root / "daily_bin" / "csv_preparation_receipt.json").read_text(encoding="utf-8")
        ),
        minute_preparation_receipt=json.loads(
            (root / "minute_bin" / "csv_preparation_receipt.json").read_text(encoding="utf-8")
        ),
        minute_canonical_source=minute_source,
        index_receipt=index_receipt,
        external_consumer_smoke=external_smoke,
        minute_overlay_summary={
            "source_policy": "tdx_then_tushare_missing_keys_conflict_fail_v1",
            "database_rows": 900,
            "overlay_rows": 60,
            "tdx_rows": 50,
            "tushare_rows": 10,
            "missing_keys": 0,
            "duplicate_keys": 0,
            "overlap_mismatch_cells": 0,
            "database_writes": 0,
            "production_writes": 0,
            "provider_concurrency": 1,
        },
        actions={item.value: ComponentAction.FULL_REBUILD.value for item in Component},
        component_fingerprints={item.value: "c" * 64 for item in Component},
        validation_fingerprint="d" * 64,
        action_plan_digest=action_plan_digest,
        transition_authority=transition_authority,
        require_production_consumer_smoke=False,
    )


def _with_daily_receipt_authority(
    root: Path,
    spec: CandidateValidationSpec,
    sealed_canonical_rows: dict,
) -> CandidateValidationSpec:
    receipt = {
        "status": "PASS",
        "dataset": "daily_bin",
        "sealed_canonical_rows": sealed_canonical_rows,
    }
    preparation = dict(spec.daily_preparation_receipt or {})
    preparation["sealed_canonical_rows"] = sealed_canonical_rows
    (root / "daily_bin" / "materialization_receipt.json").write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (root / "daily_bin" / "csv_preparation_receipt.json").write_text(
        json.dumps(preparation, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return replace(
        spec,
        daily_receipt=receipt,
        daily_materialization_receipt_file=receipt,
        daily_preparation_receipt=preparation,
    )


def _with_minute_receipt_authority(
    root: Path,
    spec: CandidateValidationSpec,
    sealed_canonical_rows: dict,
) -> CandidateValidationSpec:
    receipt = {
        "status": "PASS",
        "dataset": "minute_bin",
        "sealed_canonical_rows": sealed_canonical_rows,
    }
    preparation = dict(spec.minute_preparation_receipt or {})
    preparation["sealed_canonical_rows"] = sealed_canonical_rows
    (root / "minute_bin" / "materialization_receipt.json").write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (root / "minute_bin" / "csv_preparation_receipt.json").write_text(
        json.dumps(preparation, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return replace(
        spec,
        minute_receipt=receipt,
        minute_materialization_receipt_file=receipt,
        minute_preparation_receipt=preparation,
        minute_canonical_source=sealed_canonical_rows,
    )


def test_candidate_validator_produces_complete_machine_signoff_input(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)

    report = CandidateValidator().validate(_spec(root, factor_receipt, index_receipt, minute_source, dataset_profile))

    assert report.payload["status"] == "PASS"
    assert report.payload["hmm"] == {
        "benchmark": "000300.SH",
        "shared_index_contract": "validated",
        "existing_consumer_activation": "not_activated_not_switched",
    }
    assert len(report.components) == 4
    assert {item.partition_key for item in report.components} == {"all"}
    assert {str(item["partition_key"]) for item in report.payload["components"]} == {"all"}
    assert len(report.validations) == 13
    assert report.payload["evidence"]["moneyflow_static_parity"]["rows"] == 4
    derived = report.payload["evidence"]["moneyflow_derived_formula_parity"]
    assert derived["rows_checked"] == 4
    assert derived["values_checked"] == 4 * len(STATIC_MONEYFLOW_DERIVED_COLUMNS)
    assert derived["unit_provenance"] == {
        "candidate_formula_input": "canonical_moneyflow_h5_share_cny",
        "source_hand_10k_to_share_cny_independently_reproven_here": False,
        "source_conversion_authority": ("artifact_ready_and_factor_producer_receipt_hash_chain"),
    }
    assert report.payload["evidence"]["daily_source_bin_parity"] == {
        **report.payload["evidence"]["daily_source_bin_parity"],
        "rows_checked": 4,
        "values_checked": 4 * len(DAILY_FIELDS),
        "sample_policy": "full_required_rows_no_sampling",
    }
    assert report.payload["evidence"]["daily_h5_bin_parity"]["values_checked"] > 0
    assert report.payload["evidence"]["minute_source_bin_parity"] == {
        **report.payload["evidence"]["minute_source_bin_parity"],
        "rows_checked": 960,
        "values_checked": 960 * len(MINUTE_FIELDS),
        "sample_policy": "full_required_rows_no_sampling",
    }
    minute_memory = report.payload["evidence"]["minute_source_bin_parity"]["memory_contract"]
    assert minute_memory["mode"] == "vectorized_csv_chunk_vs_12_memmaps_v1"
    assert minute_memory["peak_chunk_rows"] <= (dataset_profile.resource_policy.validation_read_chunk_rows)
    assert minute_memory["whole_market_frames_retained"] == 0
    assert (
        report.payload["evidence"]["minute_source_bin_parity"]["expected_day_contract"]
        == "one_calendar_scan_then_per_code_bisect_v1"
    )
    smoke = report.payload["evidence"]["qe_hmm_consumer_smoke"]
    assert smoke["qe"]["status"] == "PASS"
    assert smoke["hmm_index_contract"]["status"] == "PASS"
    assert smoke["execution_kind"] == "fixture_contract_test"


def test_candidate_validator_rejects_unbound_candidate_root_file(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    (root / "unbound-root-ghost.bin").write_bytes(b"ghost")

    with pytest.raises(
        CandidateValidationError,
        match="candidate root namespace differs",
    ):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validator_rejects_candidate_mutation_during_validation(
    tmp_path: Path,
    dataset_profile,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    target = root / "factor_bundle" / "factor_checkpoint.json"
    original = candidate_validator_module._validate_external_consumer_smoke

    def mutate_after_last_semantic_check(*args, **kwargs):
        result = original(*args, **kwargs)
        target.write_bytes(target.read_bytes() + b"tampered")
        return result

    monkeypatch.setattr(
        candidate_validator_module,
        "_validate_external_consumer_smoke",
        mutate_after_last_semantic_check,
    )

    with pytest.raises(
        CandidateValidationError,
        match="snapshot identity changed during validation",
    ):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


@pytest.mark.parametrize(
    ("component_root", "message"),
    [
        ("factor_bundle", "factor bundle namespace differs"),
        ("index_context", "index context namespace differs"),
    ],
)
def test_candidate_validator_rejects_full_component_ghost_file(
    tmp_path: Path,
    dataset_profile,
    component_root: str,
    message: str,
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    (root / component_root / "unbound-component-ghost.bin").write_bytes(b"ghost")

    with pytest.raises(CandidateValidationError, match=message):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


@pytest.mark.parametrize("dataset", ["daily_bin", "minute_bin"])
@pytest.mark.parametrize(
    "receipt_name",
    ["materialization_receipt.json", "csv_preparation_receipt.json"],
)
@pytest.mark.parametrize("mutation", ["tamper", "remove"])
def test_candidate_validator_binds_bin_receipt_files_to_frozen_authority(
    tmp_path: Path,
    dataset_profile,
    dataset: str,
    receipt_name: str,
    mutation: str,
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    path = root / dataset / receipt_name
    if mutation == "tamper":
        path.write_text("{}", encoding="utf-8")
    else:
        path.unlink()

    with pytest.raises(CandidateValidationError):
        CandidateValidator().validate(spec)


@pytest.mark.parametrize(
    ("relative_path", "mutation"),
    [
        ("factor_bundle/factor_checkpoint.json", "tamper"),
        ("factor_bundle/factor_checkpoint.json", "remove"),
        ("index_context/index_materialization_receipt.json", "tamper"),
        ("index_context/index_materialization_receipt.json", "remove"),
    ],
)
def test_candidate_validator_binds_factor_index_receipt_files(
    tmp_path: Path,
    dataset_profile,
    relative_path: str,
    mutation: str,
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    path = root / relative_path
    if mutation == "tamper":
        path.write_text("{}", encoding="utf-8")
    else:
        path.unlink()

    with pytest.raises(CandidateValidationError):
        CandidateValidator().validate(spec)


@pytest.mark.parametrize("drift", ["sha256", "size_bytes", "candidate_path"])
def test_candidate_validator_rejects_self_consistent_factor_chunk_receipt_drift(
    tmp_path: Path,
    dataset_profile,
    drift: str,
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    factor_receipt = json.loads(json.dumps(factor_receipt))
    chunk = factor_receipt["chunks"][0]
    if drift == "sha256":
        chunk["sha256"] = "0" * 64
    elif drift == "size_bytes":
        chunk["size_bytes"] = int(chunk["size_bytes"]) + 1
    else:
        chunk["candidate_relative_path"] = "factor_bundle/partitions/unbound/ghost.parquet"
    (root / "factor_bundle" / "factor_checkpoint.json").write_text(
        json.dumps(factor_receipt, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    with pytest.raises(CandidateValidationError, match="factor chunk"):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validation_spec_rejects_action_plan_digest_rebinding(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)

    with pytest.raises(
        CandidateValidationError,
        match="transition authority/action-plan digest differs",
    ):
        replace(spec, action_plan_digest="0" * 64)


def _bind_fixture_component_transition(
    spec: CandidateValidationSpec,
    *,
    component: Component,
    baseline_root: Path,
    evidence: ComponentArtifactEvidence,
    action: ComponentAction,
    replace_targets: tuple[str, ...] = (),
    create_targets: tuple[str, ...] = (),
    scopes: tuple[dict, ...] = (),
) -> CandidateValidationSpec:
    frozen = {
        "source_release_id": "baseline-release",
        "source_release_digest": "1" * 64,
        "source_attestation_key": "2" * 64,
        "artifact_id": evidence.component_identity,
        "component_partition_key": "all",
        "manifest_root": evidence.filesystem_tree_merkle,
        "file_identity": evidence.file_identity,
        "reuse_mode": action.value.casefold(),
        "mutation_set": sorted((*replace_targets, *create_targets)),
        "compatibility_reason": "fixture identity-bound baseline",
        "replace_existing_targets": sorted(replace_targets),
        "create_new_targets": sorted(create_targets),
        "invalidation_scopes": [dict(value) for value in scopes],
        "component_root_relative_path": evidence.component_root_relative_path,
    }
    action_entry = {
        "component": component.value,
        "partition_key": "all",
        "action": action.value,
        "reason": "fixture component transition",
        "changed_fingerprints": [],
        "invalidation_edges": [],
        "estimated_work": {},
        "frozen_reuse": frozen,
    }
    authorities = dict(spec.transition_authority)
    authorities[component.value] = CandidateComponentTransitionAuthority(
        component=component,
        action=action,
        action_entry=action_entry,
        frozen_reuse=frozen,
        baseline_component_root=baseline_root,
        baseline_evidence=evidence,
    )
    entries = [dict(authorities[item.value].action_entry) for item in Component]
    digest = digest_named_fields(
        DECISION_SCHEMA_VERSION,
        {
            "actions": sorted(
                entries,
                key=lambda value: (value["component"], value["partition_key"]),
            )
        },
    )
    actions = dict(spec.actions)
    actions[component.value] = action.value
    return replace(
        spec,
        actions=actions,
        transition_authority=authorities,
        action_plan_digest=digest,
    )


def test_candidate_transition_authority_rejects_frozen_partition_key_drift(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    evidence = _fixture_component_evidence(root / "factor_bundle", component=Component.FACTOR_H5_STATIC)
    bound = _bind_fixture_component_transition(
        spec,
        component=Component.FACTOR_H5_STATIC,
        baseline_root=root / "factor_bundle",
        evidence=evidence,
        action=ComponentAction.REUSE,
    )
    authority = bound.transition_authority[Component.FACTOR_H5_STATIC.value]
    action_entry = dict(authority.action_entry)
    action_entry["partition_key"] = "drifted-partition"

    with pytest.raises(
        CandidateValidationError,
        match="frozen baseline partition key differs",
    ):
        CandidateComponentTransitionAuthority(
            component=authority.component,
            action=authority.action,
            action_entry=action_entry,
            frozen_reuse=authority.frozen_reuse,
            baseline_component_root=authority.baseline_component_root,
            baseline_evidence=authority.baseline_evidence,
        )


@pytest.mark.parametrize(
    ("component", "action", "authorized", "tampered"),
    [
        (
            Component.FACTOR_H5_STATIC,
            ComponentAction.REUSE,
            None,
            "daily_pv.h5",
        ),
        (
            Component.DOMESTIC_INDEX_CONTEXT,
            ComponentAction.REUSE,
            None,
            "index_context.parquet",
        ),
        (
            Component.FACTOR_H5_STATIC,
            ComponentAction.SELECTIVE_REBUILD,
            "daily_pv.h5",
            "moneyflow.h5",
        ),
    ],
)
def test_candidate_validator_rejects_factor_index_transition_scope_rebinding(
    tmp_path: Path,
    dataset_profile,
    component: Component,
    action: ComponentAction,
    authorized: str | None,
    tampered: str,
) -> None:
    (tmp_path / "current").mkdir()
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path / "current", dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    relative_root = {
        Component.FACTOR_H5_STATIC: "factor_bundle",
        Component.DOMESTIC_INDEX_CONTEXT: "index_context",
    }[component]
    baseline = tmp_path / "baseline" / relative_root
    shutil.copytree(root / relative_root, baseline)
    evidence = _fixture_component_evidence(baseline, component=component)
    spec = _bind_fixture_component_transition(
        spec,
        component=component,
        baseline_root=baseline,
        evidence=evidence,
        action=action,
        replace_targets=((authorized,) if authorized else ()),
        scopes=(
            (
                {
                    "kind": "historical_source_revision",
                    "source_partition": "fixture",
                    "months": ["2026-07"],
                },
            )
            if action is ComponentAction.SELECTIVE_REBUILD
            else ()
        ),
    )
    target = root / relative_root / tampered
    target.write_bytes(target.read_bytes() + b"self-consistent-current-tamper")

    with pytest.raises(
        CandidateValidationError,
        match="REUSE candidate differs|changed outside frozen replace scope",
    ):
        CandidateValidator().validate(spec)


def test_candidate_validator_rejects_nonfull_component_ghost_outside_frozen_create(
    tmp_path: Path, dataset_profile
) -> None:
    (tmp_path / "current").mkdir()
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path / "current", dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    baseline = tmp_path / "baseline" / "factor_bundle"
    shutil.copytree(root / "factor_bundle", baseline)
    evidence = _fixture_component_evidence(baseline, component=Component.FACTOR_H5_STATIC)
    spec = _bind_fixture_component_transition(
        spec,
        component=Component.FACTOR_H5_STATIC,
        baseline_root=baseline,
        evidence=evidence,
        action=ComponentAction.REUSE,
    )
    (root / "factor_bundle" / "unbound-nonfull-ghost.bin").write_bytes(b"ghost")

    with pytest.raises(
        CandidateValidationError,
        match="candidate namespace differs from baseline plus creates",
    ):
        CandidateValidator().validate(spec)


def test_candidate_validator_rejects_replace_authority_for_immutable_lineage(tmp_path: Path, dataset_profile) -> None:
    (tmp_path / "current").mkdir()
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path / "current", dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    baseline = tmp_path / "baseline" / "daily_bin"
    shutil.copytree(root / "daily_bin", baseline)
    evidence = _fixture_component_evidence(baseline, component=Component.DAILY_BIN)
    spec = _bind_fixture_component_transition(
        spec,
        component=Component.DAILY_BIN,
        baseline_root=baseline,
        evidence=evidence,
        action=ComponentAction.SELECTIVE_REBUILD,
        replace_targets=(f"csv/{STOCKS[0].casefold()}.csv",),
        scopes=(
            {
                "kind": "historical_source_revision",
                "affected_instruments": [STOCKS[0]],
                "months": ["2026-07"],
            },
        ),
    )

    with pytest.raises(
        CandidateValidationError,
        match="replace authority targets immutable baseline lineage",
    ):
        CandidateValidator().validate(spec)


def test_candidate_validator_accepts_bounded_daily_and_minute_composite_segments(
    tmp_path: Path, dataset_profile
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    daily_receipt = json.loads((root / "daily_bin" / "materialization_receipt.json").read_text(encoding="utf-8"))
    daily_composite = _composite_source(root, dict(daily_receipt["sealed_canonical_rows"]))
    minute_composite = _composite_source(root, minute_source)
    spec = _with_daily_receipt_authority(
        root,
        _with_minute_receipt_authority(
            root,
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_composite,
                dataset_profile,
            ),
            minute_composite,
        ),
        daily_composite,
    )

    report = CandidateValidator().validate(spec)

    daily = report.payload["evidence"]["daily_source_bin_parity"]
    minute = report.payload["evidence"]["minute_source_bin_parity"]
    assert daily["source_schema_version"] == SEALED_QLIB_CSV_COMPOSITE_SCHEMA
    assert daily["rows_checked"] == 4
    assert daily["values_checked"] == 4 * len(DAILY_FIELDS)
    assert minute["source_schema_version"] == SEALED_QLIB_CSV_COMPOSITE_SCHEMA
    assert minute["rows_checked"] == 960
    assert minute["values_checked"] == 960 * len(MINUTE_FIELDS)
    assert daily["memory_contract"]["whole_market_frames_retained"] == 0
    assert minute["memory_contract"]["whole_market_frames_retained"] == 0


@pytest.mark.parametrize("dataset", ["daily_bin", "minute_bin"])
def test_candidate_validator_rejects_full_v1_ghost_csv_namespace_entry(
    tmp_path: Path, dataset_profile, dataset: str
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    (root / dataset / "csv" / "ghost.csv").write_text("unsealed ghost", encoding="utf-8")

    with pytest.raises(
        CandidateValidationError,
        match=rf"{dataset.removesuffix('_bin')} v1 canonical CSV namespace",
    ):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validator_accepts_current_full_v1_daily_index_mirror(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    for code in dataset_profile.index_codes:
        shutil.copy2(
            root / "index_context" / "index_csv" / f"{code}.csv",
            root / "daily_bin" / "csv" / f"{code}.csv",
        )

    report = CandidateValidator().validate(
        _spec(
            root,
            factor_receipt,
            index_receipt,
            minute_source,
            dataset_profile,
        )
    )

    assert report.payload["status"] == "PASS"


def test_candidate_validator_rejects_partial_full_v1_daily_index_mirror(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    code = dataset_profile.index_codes[0]
    shutil.copy2(
        root / "index_context" / "index_csv" / f"{code}.csv",
        root / "daily_bin" / "csv" / f"{code}.csv",
    )

    with pytest.raises(
        CandidateValidationError,
        match="daily v1 canonical CSV namespace differs from both supported exact layouts",
    ):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validator_rejects_tampered_full_v1_daily_index_mirror(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    for code in dataset_profile.index_codes:
        shutil.copy2(
            root / "index_context" / "index_csv" / f"{code}.csv",
            root / "daily_bin" / "csv" / f"{code}.csv",
        )
    tampered = root / "daily_bin" / "csv" / f"{dataset_profile.index_codes[0]}.csv"
    tampered.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(
        CandidateValidationError,
        match="daily/index-context canonical CSV mirror differs",
    ):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validator_accepts_explicit_override_lineage_with_clean_full_parity(
    tmp_path: Path, dataset_profile
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    daily_receipt = json.loads((root / "daily_bin" / "materialization_receipt.json").read_text(encoding="utf-8"))
    daily_composite = _composite_source(root, dict(daily_receipt["sealed_canonical_rows"]))
    minute_composite = _composite_source(root, minute_source)
    daily_override = _override_source(root, daily_composite, instrument=STOCKS[0])
    minute_override = _override_source(root, minute_composite, instrument=STOCKS[0])
    immutable_before = {
        (
            str(item["root_relative_path"]),
            str(item["relative_path"]),
        ): sha256_file(root / str(item["root_relative_path"]) / str(item["relative_path"]))
        for source in (daily_override, minute_override)
        for override in source["overrides"]
        for item in override["superseded_segments"]
    }
    spec = _with_daily_receipt_authority(
        root,
        _with_minute_receipt_authority(
            root,
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_override,
                dataset_profile,
            ),
            minute_override,
        ),
        daily_override,
    )

    report = CandidateValidator().validate(spec)

    for dataset in ("daily_source_bin_parity", "minute_source_bin_parity"):
        lineage = report.payload["evidence"][dataset]["lineage_contract"]
        assert lineage["merge_contract"] == ("instrument_active_segments_with_explicit_overrides_v1")
        assert lineage["override_events"] == 1
        assert lineage["superseded_segments"] == 2
        assert lineage["namespace_manifests"] == 2
        assert lineage["authority"] == ("top_level_segments_only_unique_code_datetime_v1")
    assert immutable_before == {
        relative: sha256_file(root / relative[0] / relative[1]) for relative in immutable_before
    }


@pytest.mark.parametrize(
    ("final_historical_override", "active_segments", "override_events", "retired"),
    [(False, 3, 1, 1), (True, 1, 2, 4)],
)
def test_composite_lineage_accepts_second_delta_and_later_historical_revision(
    tmp_path: Path,
    final_historical_override: bool,
    active_segments: int,
    override_events: int,
    retired: int,
) -> None:
    root = tmp_path / "candidate"
    root.mkdir()
    source, immutable_before = _three_month_override_lineage(root, final_historical_override=final_historical_override)

    evidence = _validate_composite_lineage_contract(
        dataset="daily_bin",
        candidate_root=root,
        source=source,
        expected_instruments=set(STOCKS),
        max_chunk_rows=2,
    )

    assert evidence["active_segments"] == active_segments
    assert evidence["override_events"] == override_events
    assert evidence["superseded_segments"] == retired
    assert evidence["namespace_manifests"] == (4 if final_historical_override else 3)
    assert immutable_before == {relative: sha256_file(root / relative) for relative in immutable_before}


@pytest.mark.parametrize(
    "attack",
    [
        "omitted_superseded",
        "tampered_retired_bytes",
        "overlapping_active",
        "ghost_csv",
        "scope_code_mismatch",
        "replacement_range_drift",
        "manifest_hash_drift",
    ],
)
def test_composite_override_lineage_fails_closed_on_adversarial_mutation(tmp_path: Path, attack: str) -> None:
    root = tmp_path / "candidate"
    root.mkdir()
    source, _immutable = _three_month_override_lineage(root, final_historical_override=False)
    source = json.loads(json.dumps(source))
    if attack == "omitted_superseded":
        source["overrides"][0]["superseded_segments"] = []
    elif attack == "tampered_retired_bytes":
        retired = source["overrides"][0]["superseded_segments"][0]
        path = root / retired["root_relative_path"] / retired["relative_path"]
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    elif attack == "overlapping_active":
        source["segments"][2]["start"] = source["segments"][1]["end"]
    elif attack == "ghost_csv":
        path = root / "daily_bin" / "csv_deltas" / "202609" / "600000.sh.csv"
        path.write_text(
            (
                "date,symbol,"
                + ",".join(DAILY_FIELDS)
                + "\n2026-09-01,600000.SH,"
                + ",".join("1" for _ in DAILY_FIELDS)
                + "\n"
            ),
            encoding="utf-8",
        )
    elif attack == "scope_code_mismatch":
        source["overrides"][0]["invalidation_scopes"][0]["instrument"] = STOCKS[1]
    elif attack == "replacement_range_drift":
        source["overrides"][0]["end"] = "2026-08-03 00:00:00"
    else:
        path = root / "daily_bin" / "csv_deltas" / "202609" / "manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        local = next(
            item for item in manifest["canonical"]["segments"] if item["root_relative_path"].endswith("/202609")
        )
        local["sha256"] = "0" * 64
        path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(CandidateValidationError):
        _validate_composite_lineage_contract(
            dataset="daily_bin",
            candidate_root=root,
            source=source,
            expected_instruments={STOCKS[0]},
            max_chunk_rows=2,
        )


def test_composite_manifest_v2_seals_header_tombstone_and_inactive_index_csv(
    tmp_path: Path,
) -> None:
    root = tmp_path / "candidate"
    root.mkdir()
    source, _immutable = _three_month_override_lineage(root, final_historical_override=False)
    namespace_root = root / "daily_bin" / "csv_deltas" / "202609"
    tombstone = namespace_root / f"{STOCKS[1].casefold()}.csv"
    tombstone.write_text(
        "date,symbol," + ",".join(DAILY_FIELDS) + "\n",
        encoding="utf-8",
    )
    _write_daily_segment(
        root,
        root_relative="daily_bin/csv_deltas/202609",
        instrument="000300.SH",
        days=(date(2026, 9, 1),),
    )
    _write_canonical_namespace_manifest(
        root,
        dataset="daily_bin",
        namespace="csv_deltas",
        key="202609",
        canonical=source,
    )

    evidence = _validate_composite_lineage_contract(
        dataset="daily_bin",
        candidate_root=root,
        source=source,
        expected_instruments=set(STOCKS),
        max_chunk_rows=2,
    )

    assert evidence["header_only_tombstones"] == 1
    assert evidence["inactive_index_csv"] == 1


@pytest.mark.parametrize("attack", ["missing_inventory", "tombstone_tamper"])
def test_composite_manifest_v2_rejects_unsealed_namespace_file(tmp_path: Path, attack: str) -> None:
    root = tmp_path / "candidate"
    root.mkdir()
    source, _immutable = _three_month_override_lineage(root, final_historical_override=False)
    namespace_root = root / "daily_bin" / "csv_deltas" / "202609"
    tombstone = namespace_root / f"{STOCKS[1].casefold()}.csv"
    tombstone.write_text(
        "date,symbol," + ",".join(DAILY_FIELDS) + "\n",
        encoding="utf-8",
    )
    _write_canonical_namespace_manifest(
        root,
        dataset="daily_bin",
        namespace="csv_deltas",
        key="202609",
        canonical=source,
    )
    manifest_path = namespace_root / "manifest.json"
    if attack == "missing_inventory":
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"] = [item for item in manifest["files"] if item["relative_path"] != tombstone.name]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    else:
        tombstone.write_text(tombstone.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(CandidateValidationError):
        _validate_composite_lineage_contract(
            dataset="daily_bin",
            candidate_root=root,
            source=source,
            expected_instruments={STOCKS[0]},
            max_chunk_rows=2,
        )


def test_header_tombstone_rejects_oversize_before_reading_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "oversize.csv"
    with path.open("wb") as handle:
        handle.seek(1024 * 1024)
        handle.write(b"x")
    original_open = Path.open
    opened = 0

    def counted_open(self, *args, **kwargs):
        nonlocal opened
        if self == path:
            opened += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)

    assert not candidate_validator_module._is_header_only_canonical_csv(path, dataset="daily_bin")
    assert opened == 0


def test_json_evidence_rejects_oversize_before_reading_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "oversize.json"
    with path.open("wb") as handle:
        handle.seek(32 * 1024 * 1024)
        handle.write(b"x")
    original_open = Path.open
    opened = 0

    def counted_open(self, *args, **kwargs):
        nonlocal opened
        if self == path:
            opened += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)

    with pytest.raises(CandidateValidationError, match="bounded read limit"):
        candidate_validator_module._load_json(path)
    assert opened == 0


def _validate_durable_fixture_case(case, *, extra_calendar: tuple[date, ...] = ()) -> dict:
    candidate, source, context, cutoff = case
    calendar = tuple(
        day.isoformat()
        for day in sorted(
            {
                *extra_calendar,
                date(2026, 7, 31),
                date(2026, 8, 3),
                date(2026, 9, 1),
            }
        )
        if day <= cutoff
    )
    return _validate_composite_lineage_contract(
        dataset="daily_bin",
        candidate_root=candidate,
        source=source,
        expected_instruments={STOCKS[0]},
        expected_index_codes={"000300.SH"},
        calendar=calendar,
        transition=context,
        max_chunk_rows=2,
    )


def test_durable_lineage_replays_v1_delta1_delta2_override(
    tmp_path: Path,
) -> None:
    cases = _durable_override_fixture(tmp_path)

    evidence = [_validate_durable_fixture_case(case) for case in cases]

    assert [item["transition_authority"] for item in evidence] == [
        "durable_baseline_replay_v1",
        "durable_baseline_replay_v1",
        "durable_baseline_replay_v1",
    ]
    assert [item["override_events"] for item in evidence] == [0, 0, 1]
    assert evidence[-1]["superseded_segments"] == 3


def test_durable_lineage_parses_only_new_authorized_namespace_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, source, context, cutoff = _durable_override_fixture(tmp_path)[-1]
    files, _merkle = tree_merkle(candidate / "daily_bin")
    context = replace(
        context,
        verified_candidate_files={
            normalize_root_relative_path(item.relative_path): (
                item.size_bytes,
                item.sha256,
            )
            for item in files
        },
    )
    manifest_loads = 0
    original_load = candidate_validator_module._load_json

    def counted_load(path: Path):
        nonlocal manifest_loads
        if path.name == "manifest.json":
            manifest_loads += 1
        return original_load(path)

    monkeypatch.setattr(candidate_validator_module, "_load_json", counted_load)

    evidence = _validate_durable_fixture_case((candidate, source, context, cutoff))

    assert manifest_loads == 1
    assert evidence["namespace_manifests"] == 3
    assert evidence["historical_manifest_replays_skipped"] == 2


def test_durable_lineage_manifest_parse_count_is_constant_across_30_months(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = tmp_path / "baseline-30-months"
    (baseline / "daily_bin" / "csv").mkdir(parents=True)
    base_day = date(2023, 12, 28)
    base_item = _write_daily_segment(
        baseline,
        root_relative="daily_bin/csv",
        instrument=STOCKS[0],
        days=(base_day,),
    )
    source: dict = {
        "schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "dataset": "daily_bin",
        "root_relative_path": "daily_bin/csv",
        "ordered_fields": ["date", "symbol", *DAILY_FIELDS],
        "rows": 1,
        "files": [base_item],
    }
    month_days = tuple(date(2024 + ordinal // 12, ordinal % 12 + 1, 28) for ordinal in range(30))
    for day in month_days[:-1]:
        key = day.strftime("%Y%m")
        item = _write_daily_segment(
            baseline,
            root_relative=f"daily_bin/csv_deltas/{key}",
            instrument=STOCKS[0],
            days=(day,),
        )
        source = dict(
            build_composite_canonical_rows(
                dataset="daily_bin",
                baseline=source,
                patch_preparation={"csv": {"rows": 1, "files": [item]}},
                delta_root_relative_path=f"daily_bin/csv_deltas/{key}",
            )
        )
        _write_canonical_namespace_manifest(
            baseline,
            dataset="daily_bin",
            namespace="csv_deltas",
            key=key,
            canonical=source,
        )
    _write_baseline_materialization_receipt(baseline / "daily_bin", source)
    final_day = month_days[-1]
    candidate, current, context = _append_delta_transition(
        tmp_path,
        baseline_root=baseline,
        source=source,
        day=final_day,
    )
    files, _merkle = tree_merkle(candidate / "daily_bin")
    context = replace(
        context,
        verified_candidate_files={
            normalize_root_relative_path(item.relative_path): (
                item.size_bytes,
                item.sha256,
            )
            for item in files
        },
    )
    manifest_loads = 0
    original_load = candidate_validator_module._load_json

    def counted_load(path: Path):
        nonlocal manifest_loads
        if path.name == "manifest.json":
            manifest_loads += 1
        return original_load(path)

    monkeypatch.setattr(candidate_validator_module, "_load_json", counted_load)

    evidence = _validate_composite_lineage_contract(
        dataset="daily_bin",
        candidate_root=candidate,
        source=current,
        expected_instruments={STOCKS[0]},
        calendar=tuple(day.isoformat() for day in (base_day, *month_days)),
        transition=context,
        max_chunk_rows=2,
    )

    assert manifest_loads == 1
    assert evidence["namespace_manifests"] == 30
    assert evidence["historical_manifest_replays_skipped"] == 29


@pytest.mark.parametrize(
    "attack",
    [
        "updated_self_hash",
        "removed_event",
        "removed_base",
        "never_active_superseded",
        "scope_month_drift",
        "scope_date_drift",
        "historical_manifest_drift",
        "empty_namespace",
        "undeclared_tombstone",
    ],
)
def test_durable_lineage_rejects_adversarial_transition_attacks(tmp_path: Path, attack: str) -> None:
    case = list(_durable_override_fixture(tmp_path))[-1]
    if attack in {"removed_event", "never_active_superseded"}:
        prior_candidate, prior_source, _prior_context, _prior_cutoff = case
        _write_baseline_materialization_receipt(prior_candidate / "daily_bin", prior_source)
        second = _override_transition(
            tmp_path,
            baseline_root=prior_candidate,
            source=prior_source,
            days=(date(2026, 7, 31), date(2026, 8, 3), date(2026, 9, 1)),
            candidate_name="candidate-override-2",
            scopes=[
                {
                    "kind": "qfq_denominator_change",
                    "instrument": STOCKS[0],
                    "start": "2026-07-31",
                    "end": "2026-09-01",
                }
            ],
        )
        case = (*second, date(2026, 9, 1))
        assert _validate_durable_fixture_case(case)["override_events"] == 2
    candidate, source, context, cutoff = case
    source = json.loads(json.dumps(source))
    event = source["overrides"][-1]
    override_root = candidate / event["root_relative_path"]
    override_manifest = override_root / "manifest.json"
    if attack == "updated_self_hash":
        retired = event["superseded_segments"][0]
        path = candidate / retired["root_relative_path"] / retired["relative_path"]
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        retired["sha256"] = sha256_file(path)
        manifest = json.loads(override_manifest.read_text(encoding="utf-8"))
        manifest["canonical"] = source
        override_manifest.write_text(json.dumps(manifest), encoding="utf-8")
    elif attack == "removed_event":
        source["overrides"].pop(0)
    elif attack == "removed_base":
        (candidate / "daily_bin" / "csv" / f"{STOCKS[0].casefold()}.csv").unlink()
    elif attack == "never_active_superseded":
        event["superseded_segments"].append(dict(source["overrides"][0]["superseded_segments"][0]))
    elif attack == "scope_month_drift":
        event["invalidation_scopes"][0]["months"] = ["2026-08"]
    elif attack == "scope_date_drift":
        scope = event["invalidation_scopes"][0]
        scope["downstream_observations"] = 18
    elif attack == "historical_manifest_drift":
        path = candidate / "daily_bin" / "csv_deltas" / "202608" / "manifest.json"
        path.write_text(path.read_text(encoding="utf-8") + " ", encoding="utf-8")
    elif attack == "empty_namespace":
        for path in override_root.glob("*.csv"):
            path.unlink()
        manifest = json.loads(override_manifest.read_text(encoding="utf-8"))
        manifest["files"] = []
        override_manifest.write_text(json.dumps(manifest), encoding="utf-8")
    else:
        namespace = candidate / "daily_bin" / "csv_deltas" / "202609"
        tombstone = namespace / f"{STOCKS[1].casefold()}.csv"
        tombstone.write_text(
            "date,symbol," + ",".join(DAILY_FIELDS) + "\n",
            encoding="utf-8",
        )
        manifest_path = namespace / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"].append(
            {
                "instrument": STOCKS[1],
                "relative_path": tombstone.name,
                "rows": 0,
                "sha256": sha256_file(tombstone),
                "size_bytes": tombstone.stat().st_size,
                "start": None,
                "end": None,
                "active": False,
            }
        )
        manifest["files"].sort(key=lambda item: item["relative_path"])
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(CandidateValidationError):
        _validate_durable_fixture_case((candidate, source, context, cutoff))


@pytest.mark.parametrize(
    ("code", "day"),
    [
        ("999999.SH", date(2026, 8, 3)),
        ("000300.SH", date(2099, 1, 5)),
        ("000688.SH", date(2019, 12, 31)),
    ],
)
def test_durable_lineage_rejects_unbound_or_future_inactive_index_csv(tmp_path: Path, code: str, day: date) -> None:
    candidate, source, context, cutoff = _durable_override_fixture(tmp_path)[0]
    namespace = candidate / "daily_bin" / "csv_deltas" / "202608"
    item = _write_daily_segment(
        candidate,
        root_relative="daily_bin/csv_deltas/202608",
        instrument=code,
        days=(day,),
    )
    manifest_path = namespace / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"].append(
        {
            **item,
            "active": False,
        }
    )
    manifest["files"].sort(key=lambda value: value["relative_path"])
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    relative = f"csv_deltas/202608/{code.casefold()}.csv"
    context = replace(
        context,
        authorized_create_paths=frozenset({*context.authorized_create_paths, relative}),
    )

    with pytest.raises(CandidateValidationError):
        _validate_durable_fixture_case(
            (candidate, source, context, cutoff),
            extra_calendar=((day,) if code == "000688.SH" else ()),
        )


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink contract")
def test_plain_root_rejects_posix_logical_symlink(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "logical-link"
    try:
        os.symlink(target, link, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"host does not permit isolated symlink creation: {exc}")

    with pytest.raises(CandidateValidationError, match="symlink/reparse"):
        candidate_validator_module._plain_root(link)


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_plain_root_rejects_windows_junction(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    junction = tmp_path / "logical-junction"
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            (
                "& { param([string]$link,[string]$target) "
                "New-Item -ItemType Junction -Path $link -Target $target | Out-Null }"
            ),
            str(junction),
            str(target),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0 or not junction.exists():
        pytest.skip(f"host junction creation is unavailable: {result.stderr.strip()}")
    try:
        with pytest.raises(CandidateValidationError, match="symlink/reparse"):
            candidate_validator_module._plain_root(junction)
    finally:
        os.rmdir(junction)


def _v1_daily_source_for_link_test() -> dict:
    return {
        "schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "dataset": "daily_bin",
        "root_relative_path": "daily_bin/csv",
        "ordered_fields": ["date", "symbol", *DAILY_FIELDS],
        "rows": 1,
        "files": [
            {
                "instrument": STOCKS[0],
                "relative_path": f"{STOCKS[0]}.csv",
                "rows": 1,
                "sha256": "0" * 64,
                "size_bytes": 1,
                "start": "2026-07-31 00:00:00",
                "end": "2026-07-31 00:00:00",
            }
        ],
    }


def _assert_v1_daily_source_rejects_logical_link(candidate: Path) -> None:
    source = _v1_daily_source_for_link_test()
    with pytest.raises(CandidateValidationError, match="symlink/reparse"):
        candidate_validator_module._validate_daily_source_bin_parity(
            candidate_root=candidate.resolve(strict=True),
            daily_receipt={
                "status": "PASS",
                "dataset": "daily_bin",
                "sealed_canonical_rows": source,
            },
            bin_root=candidate / "unused-qlib",
            calendar=("2026-07-31",),
            expected_instruments={STOCKS[0]},
            expected_stock_spans={STOCKS[0]: ((date(2026, 7, 31), date(2026, 7, 31)),)},
            max_chunk_rows=2,
            transition=None,
            expected_index_codes=set(),
        )


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink contract")
def test_v1_daily_source_rejects_posix_logical_csv_root(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    (candidate / "daily_bin").mkdir(parents=True)
    external = tmp_path / "external-csv"
    external.mkdir()
    os.symlink(external, candidate / "daily_bin" / "csv", target_is_directory=True)

    _assert_v1_daily_source_rejects_logical_link(candidate)


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_v1_daily_source_rejects_windows_junction_csv_root(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    (candidate / "daily_bin").mkdir(parents=True)
    external = tmp_path / "external-csv"
    external.mkdir()
    junction = candidate / "daily_bin" / "csv"
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            (
                "& { param([string]$link,[string]$target) "
                "New-Item -ItemType Junction -Path $link -Target $target | Out-Null }"
            ),
            str(junction),
            str(external),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0 or not junction.exists():
        pytest.skip(f"host junction creation is unavailable: {result.stderr.strip()}")
    try:
        _assert_v1_daily_source_rejects_logical_link(candidate)
    finally:
        os.rmdir(junction)


def test_moneyflow_derived_formula_parity_keeps_rolling_state_across_chunks(
    tmp_path: Path,
) -> None:
    root = _moneyflow_formula_bundle(tmp_path)

    result = _validate_moneyflow_derived_formula_parity(root, max_rows=4)

    assert result["rows_checked"] == 50
    assert result["values_checked"] == 50 * len(STATIC_MONEYFLOW_DERIVED_COLUMNS)
    assert result["rolling_contract"] == ("per_instrument_5_20_observations_cross_chunk_state_v1")
    assert result["memory_contract"]["peak_chunk_rows"] <= 4
    assert result["memory_contract"]["peak_rolling_state_values"] <= 2 * 19 * 4


@pytest.mark.parametrize(
    ("field", "row"),
    [
        ("mf_main_net_amt", 0),
        ("mf_total_net_amt_5d", 8),
        ("mf_elg_net_amt_ratio_20d", 38),
    ],
)
def test_moneyflow_derived_formula_parity_rejects_single_field_drift(
    tmp_path: Path,
    field: str,
    row: int,
) -> None:
    root = _moneyflow_formula_bundle(tmp_path)
    path = root / "static_factors.parquet"
    static = pd.read_parquet(path)
    static.iloc[row, static.columns.get_loc(field)] += np.float32(7.0)
    path.unlink()
    static.to_parquet(path, row_group_size=3)

    with pytest.raises(
        CandidateValidationError,
        match=rf"moneyflow derived formula parity differs: {field}",
    ):
        _validate_moneyflow_derived_formula_parity(root, max_rows=3)


def test_moneyflow_derived_formula_parity_rejects_window_nan_boundary_drift(
    tmp_path: Path,
) -> None:
    root = _moneyflow_formula_bundle(tmp_path)
    path = root / "static_factors.parquet"
    static = pd.read_parquet(path)
    field = "mf_total_net_amt_5d"
    assert pd.isna(static.iloc[3][field])
    static.iloc[3, static.columns.get_loc(field)] = np.float32(0.0)
    path.unlink()
    static.to_parquet(path, row_group_size=3)

    with pytest.raises(
        CandidateValidationError,
        match=rf"moneyflow derived NaN mask differs: {field}",
    ):
        _validate_moneyflow_derived_formula_parity(root, max_rows=3)


def test_candidate_validator_blocks_moneyflow_derived_drift_with_current_file_receipt(
    tmp_path: Path,
    dataset_profile,
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    path = root / "factor_bundle" / "static_factors.parquet"
    static = pd.read_parquet(path)
    field = "mf_main_net_amt"
    static.iloc[-1, static.columns.get_loc(field)] += np.float32(7.0)
    path.unlink()
    static.to_parquet(path, row_group_size=2)
    output = factor_receipt["outputs"][STATIC_DATASET]
    output["sha256"] = sha256_file(path)
    output["size_bytes"] = path.stat().st_size
    (root / "factor_bundle" / "factor_checkpoint.json").write_text(
        json.dumps(factor_receipt, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    with pytest.raises(
        CandidateValidationError,
        match=rf"moneyflow derived formula parity differs: {field}",
    ):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


@pytest.mark.parametrize("dataset", ["daily_bin", "minute_bin"])
def test_candidate_validator_rejects_out_of_order_composite_segments(
    tmp_path: Path, dataset_profile, dataset: str
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    daily_receipt = json.loads((root / "daily_bin" / "materialization_receipt.json").read_text(encoding="utf-8"))
    daily_composite = _composite_source(root, dict(daily_receipt["sealed_canonical_rows"]))
    minute_composite = _composite_source(root, minute_source)
    target = daily_composite if dataset == "daily_bin" else minute_composite
    target["segments"][0], target["segments"][1] = (
        target["segments"][1],
        target["segments"][0],
    )
    spec = _with_daily_receipt_authority(
        root,
        _with_minute_receipt_authority(
            root,
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_composite,
                dataset_profile,
            ),
            minute_composite,
        ),
        daily_composite,
    )

    with pytest.raises(CandidateValidationError, match="segment manifest is not strictly ordered"):
        CandidateValidator().validate(spec)


def test_candidate_validator_rejects_index_h5_float64_dtype(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    path = root / "index_context" / "index_daily.h5"
    frame = pd.read_hdf(path, key="data").astype("float64")
    path.unlink()
    frame.to_hdf(path, key="data", format="table")

    with pytest.raises(CandidateValidationError, match="exact float32 dtypes"):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validator_rejects_index_h5_parquet_value_drift(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    path = root / "index_context" / "index_context.parquet"
    frame = pd.read_parquet(path)
    frame.iloc[0, frame.columns.get_loc("idx_close_point")] += np.float32(1.0)
    path.unlink()
    frame.to_parquet(path, row_group_size=2)

    with pytest.raises(CandidateValidationError, match="full value parity differs"):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validator_rejects_index_file_receipt_hash_drift(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    receipt_path = root / "index_context" / "index_materialization_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["h5"]["sha256"] = "0" * 64
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    with pytest.raises(CandidateValidationError, match="receipt/hash/schema differs"):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


@pytest.mark.parametrize("field", INDEX_QLIB_FIELDS)
def test_candidate_validator_checks_every_index_qlib_field_value(tmp_path: Path, dataset_profile, field: str) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    path = root / "daily_bin" / "qlib" / "features" / dataset_profile.index_codes[0].lower() / f"{field}.day.bin"
    values = np.fromfile(path, dtype="<f4")
    values[-1] += np.float32(7.0)
    values.tofile(path)

    with pytest.raises(CandidateValidationError, match="value parity differs"):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


@pytest.mark.parametrize("field", DAILY_FIELDS)
def test_candidate_validator_checks_every_sealed_daily_stock_value(tmp_path: Path, dataset_profile, field: str) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    path = root / "daily_bin" / "qlib" / "features" / STOCKS[0].lower() / f"{field}.day.bin"
    values = np.fromfile(path, dtype="<f4")
    values[-1] += np.float32(7.0)
    values.tofile(path)

    with pytest.raises(
        CandidateValidationError,
        match=rf"daily canonical/bin value parity differs: .*:{field}",
    ):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_validator_fails_closed_on_bin_value_or_minute_calendar_drift(
    tmp_path: Path, dataset_profile
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _with_minute_receipt_authority(
        root,
        _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile),
        minute_source,
    )
    close = root / "daily_bin" / "qlib" / "features" / "000001.sz" / "close.day.bin"
    values = np.fromfile(close, dtype="<f4")
    values[1] += 9
    values.tofile(close)

    with pytest.raises(CandidateValidationError, match="value parity differs"):
        CandidateValidator().validate(spec)

    # Repair only the exact fixture file, then independently prove minute
    # coverage drift also blocks signoff.
    daily = pd.read_hdf(root / "factor_bundle" / "daily_pv.h5", key="data")
    group = daily.xs("000001.SZ", level="instrument")
    _write_bin(close, group["close"].astype(float).tolist())
    minute_calendar = root / "minute_bin" / "qlib" / "calendars" / "1min.txt"
    lines = minute_calendar.read_text(encoding="utf-8").splitlines()
    minute_calendar.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
    with pytest.raises(CandidateValidationError, match="240 bars/day"):
        CandidateValidator().validate(spec)


def test_candidate_validator_rejects_shifted_historical_minute_session(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    original = f"{DATES[0].isoformat()} 09:31:00"
    shifted = f"{DATES[0].isoformat()} 09:30:00"
    minute_calendar = root / "minute_bin" / "qlib" / "calendars" / "1min.txt"
    minute_calendar.write_text(
        minute_calendar.read_text(encoding="utf-8").replace(original, shifted, 1),
        encoding="utf-8",
    )
    for item in minute_source["files"]:
        path = root / "minute_bin" / "csv" / item["relative_path"]
        path.write_text(
            path.read_text(encoding="utf-8").replace(original, shifted, 1),
            encoding="utf-8",
        )
        item["sha256"] = sha256_file(path)
        item["size_bytes"] = path.stat().st_size
    spec = _with_minute_receipt_authority(
        root,
        _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile),
        minute_source,
    )

    with pytest.raises(CandidateValidationError, match="canonical Shanghai sessions"):
        CandidateValidator().validate(spec)


def test_candidate_validator_checks_every_sealed_minute_value(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    close = root / "minute_bin" / "qlib" / "features" / "000001.sz" / "close.1min.bin"
    values = np.fromfile(close, dtype="<f4")
    values[-1] += 7.0
    values.tofile(close)

    with pytest.raises(CandidateValidationError, match="value parity differs"):
        CandidateValidator().validate(spec)


def test_candidate_validator_requires_240_canonical_rows_per_pit_stock_day(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    item = minute_source["files"][0]
    path = root / "minute_bin" / "csv" / item["relative_path"]
    lines = path.read_text(encoding="utf-8").splitlines()
    path.write_text("\n".join([*lines[:100], *lines[101:]]) + "\n", encoding="utf-8")
    item["rows"] -= 1
    item["size_bytes"] = path.stat().st_size
    item["sha256"] = sha256_file(path)
    minute_source["rows"] -= 1
    spec = _with_minute_receipt_authority(
        root,
        _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile),
        minute_source,
    )

    with pytest.raises(CandidateValidationError, match="240-bar contract"):
        CandidateValidator().validate(spec)


def test_candidate_validator_requires_exact_daily_and_minute_pit_spans(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    all_path = root / "minute_bin" / "qlib" / "instruments" / "all.txt"
    lines = all_path.read_text(encoding="utf-8").splitlines()
    fields = lines[0].split("\t")
    fields[1] = DATES[-1].isoformat()
    lines[0] = "\t".join(fields)
    all_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(CandidateValidationError, match="frozen PIT spans"):
        CandidateValidator().validate(spec)


@pytest.mark.parametrize("drift", ["reorder", "rename"])
def test_candidate_validator_rejects_static_order_or_name_drift(tmp_path: Path, dataset_profile, drift: str) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    path = root / "factor_bundle" / "static_factors.parquet"
    frame = pd.read_parquet(path)
    columns = list(frame.columns)
    if drift == "reorder":
        columns[0], columns[1] = columns[1], columns[0]
        frame = frame.loc[:, columns]
    else:
        frame = frame.rename(columns={columns[0]: f"{columns[0]}_renamed"})
    frame.to_parquet(path)

    with pytest.raises(CandidateValidationError, match="static 121-column"):
        CandidateValidator().validate(spec)


def test_candidate_validator_rejects_static_float64_dtype_drift(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    path = root / "factor_bundle" / "static_factors.parquet"
    frame = pd.read_parquet(path)
    frame[frame.columns[0]] = frame.iloc[:, 0].astype("float64")
    frame.to_parquet(path)

    with pytest.raises(CandidateValidationError, match="dtype contract"):
        CandidateValidator().validate(spec)


@pytest.mark.parametrize("drift", ["missing", "reorder", "float64"])
def test_candidate_validator_rejects_factor_h5_authority_drift(tmp_path: Path, dataset_profile, drift: str) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    path = root / "factor_bundle" / "daily_basic.h5"
    frame = pd.read_hdf(path, key="data")
    columns = list(frame.columns)
    if drift == "missing":
        frame = frame.drop(columns=[columns[-1]])
    elif drift == "reorder":
        columns[0], columns[1] = columns[1], columns[0]
        frame = frame.loc[:, columns]
    else:
        frame[columns[0]] = frame[columns[0]].astype("float64")
    frame.to_hdf(path, key="data", mode="w", format="table", data_columns=True)

    with pytest.raises(CandidateValidationError, match="H5 .*drifted"):
        CandidateValidator().validate(spec)


@pytest.mark.parametrize("artifact", ["daily_pv", "static_factors"])
def test_candidate_validator_rejects_missing_dense_pit_instrument_keys(
    tmp_path: Path, dataset_profile, artifact: str
) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    spec = _spec(root, factor_receipt, index_receipt, minute_source, dataset_profile)
    if artifact == "daily_pv":
        path = root / "factor_bundle" / "daily_pv.h5"
        frame = pd.read_hdf(path, key="data")
        frame = frame.loc[frame.index.get_level_values("instrument") != STOCKS[-1]]
        frame.to_hdf(path, key="data", mode="w", format="table", data_columns=True)
    else:
        path = root / "factor_bundle" / "static_factors.parquet"
        frame = pd.read_parquet(path)
        frame = frame.loc[frame.index.get_level_values("instrument") != STOCKS[-1]]
        frame.to_parquet(path)

    with pytest.raises(CandidateValidationError, match="exact PIT|omits required PIT"):
        CandidateValidator().validate(spec)


def _pit_span_bin_fixture(tmp_path: Path):
    root = tmp_path / "qlib"
    days = tuple(date(2026, 7, value) for value in range(27, 32))
    (root / "calendars").mkdir(parents=True)
    (root / "calendars" / "day.txt").write_text("".join(f"{day.isoformat()}\n" for day in days), encoding="utf-8")
    spans = {
        "000001.SZ": ((days[0], days[1]),),
        "000002.SZ": ((days[0], days[-1]),),
        "000003.SZ": ((days[0], days[1]), (days[3], days[-1])),
    }
    instruments = root / "instruments"
    instruments.mkdir()
    (instruments / "index.txt").write_text("", encoding="utf-8")
    (instruments / "all.txt").write_text(
        "".join(
            f"{code}\t{start.isoformat()}\t{end.isoformat()}\n"
            for code, ranges in spans.items()
            for start, end in ranges
        ),
        encoding="utf-8",
    )
    rows = {"000001.SZ": 2, "000002.SZ": 5, "000003.SZ": 5}
    for code, count in rows.items():
        for position, field in enumerate(DAILY_FIELDS):
            values = [float(10 + position + offset) for offset in range(count)]
            if code == "000003.SZ":
                values[2] = float("nan")
            _write_bin(root / "features" / code.lower() / f"{field}.day.bin", values)
    return root, days, spans


def test_bin_coverage_allows_ended_and_multispan_but_requires_active_cutoff(
    tmp_path: Path,
) -> None:
    root, days, spans = _pit_span_bin_fixture(tmp_path)
    result = _validate_bin(
        root,
        dataset="daily_bin",
        cutoff=days[-1],
        expected_dates=tuple(day.isoformat() for day in days),
        expected_index_codes=(),
        expected_stock_spans=spans,
    )
    assert result["stock_span_lines"] == 4

    active = root / "features" / "000002.sz" / "close.day.bin"
    _write_bin(active, [10.0, 11.0, 12.0, 13.0])
    with pytest.raises(CandidateValidationError, match="cover PIT boundaries"):
        _validate_bin(
            root,
            dataset="daily_bin",
            cutoff=days[-1],
            expected_dates=tuple(day.isoformat() for day in days),
            expected_index_codes=(),
            expected_stock_spans=spans,
        )


def test_bin_coverage_reads_ohlc_at_each_reentry_span_boundary(tmp_path: Path) -> None:
    root, days, spans = _pit_span_bin_fixture(tmp_path)
    close = root / "features" / "000003.sz" / "close.day.bin"
    _write_bin(close, [10.0, 11.0, float("nan"), float("nan"), 14.0])

    with pytest.raises(CandidateValidationError, match="OHLC boundary"):
        _validate_bin(
            root,
            dataset="daily_bin",
            cutoff=days[-1],
            expected_dates=tuple(day.isoformat() for day in days),
            expected_index_codes=(),
            expected_stock_spans=spans,
        )


def test_daily_index_bins_start_exactly_at_required_from_not_calendar_prefix(
    tmp_path: Path,
) -> None:
    root = tmp_path / "qlib"
    days = (
        date(2018, 8, 1),
        date(2019, 12, 30),
        date(2020, 1, 2),
    )
    (root / "calendars").mkdir(parents=True)
    (root / "calendars" / "day.txt").write_text("".join(f"{day.isoformat()}\n" for day in days), encoding="utf-8")
    (root / "instruments").mkdir()
    stock = STOCKS[0]
    (root / "instruments" / "all.txt").write_text(
        f"{stock}\t{days[0].isoformat()}\t{days[-1].isoformat()}\n",
        encoding="utf-8",
    )
    (root / "instruments" / "index.txt").write_text(
        "".join(
            f"{item.daily_code}\t{item.required_from.isoformat()}\t{days[-1].isoformat()}\n"
            for item in DOMESTIC_INDEX_DEFINITIONS
        ),
        encoding="utf-8",
    )
    for field in DAILY_FIELDS:
        _write_bin(
            root / "features" / stock.lower() / f"{field}.day.bin",
            [1.0] * len(days),
        )
    for definition in DOMESTIC_INDEX_DEFINITIONS:
        expected_start = next(ordinal for ordinal, day in enumerate(days) if day >= definition.required_from)
        # This is the exploit: STAR50 (000688.SH) carries finite prefix rows
        # before its required_from instead of starting at expected_start=2.
        actual_start = 0 if definition.daily_code == "000688.SH" else expected_start
        values = [1.0] * (len(days) - actual_start)
        for field in INDEX_QLIB_FIELDS:
            _write_bin(
                root / "features" / definition.daily_code.lower() / f"{field}.day.bin",
                values,
                start=actual_start,
            )

    with pytest.raises(
        CandidateValidationError,
        match="exact required-from span.*000688",
    ):
        _validate_bin(
            root,
            dataset="daily_bin",
            cutoff=days[-1],
            expected_dates=tuple(day.isoformat() for day in days),
            expected_index_codes=tuple(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS),
            expected_stock_spans={stock: ((days[0], days[-1]),)},
        )


def test_validate_bin_builds_one_calendar_index_then_bisects_per_stock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, days, spans = _pit_span_bin_fixture(tmp_path)
    build_calls = 0
    span_calls = 0
    original_build = candidate_validator_module._build_calendar_boundary_index
    original_spans = candidate_validator_module._span_boundary_positions

    def counted_build(calendar):
        nonlocal build_calls
        build_calls += 1
        return original_build(calendar)

    def counted_spans(boundary_index, *, spans, dataset):
        nonlocal span_calls
        span_calls += 1
        return original_spans(
            boundary_index,
            spans=spans,
            dataset=dataset,
        )

    monkeypatch.setattr(
        candidate_validator_module,
        "_build_calendar_boundary_index",
        counted_build,
    )
    monkeypatch.setattr(
        candidate_validator_module,
        "_span_boundary_positions",
        counted_spans,
    )

    _validate_bin(
        root,
        dataset="daily_bin",
        cutoff=days[-1],
        expected_dates=tuple(day.isoformat() for day in days),
        expected_index_codes=(),
        expected_stock_spans=spans,
    )

    assert build_calls == 1
    assert span_calls == len(spans)


def test_calendar_boundary_scan_cost_is_independent_of_6000_stock_lookups() -> None:
    days = tuple(date(2026, 1, 1) + timedelta(days=value) for value in range(250))
    calendar = tuple(
        f"{day.isoformat()} {minute // 60:02d}:{minute % 60:02d}:00" for day in days for minute in range(240)
    )
    row_visits = 0

    class CountingCalendar:
        def __iter__(self):
            nonlocal row_visits
            for value in calendar:
                row_visits += 1
                yield value

    boundary_index = candidate_validator_module._build_calendar_boundary_index(CountingCalendar())
    initial_visits = row_visits
    expected = (0, len(calendar) - 1)
    for _code_ordinal in range(6_000):
        positions = candidate_validator_module._span_boundary_positions(
            boundary_index,
            spans=((days[0], days[-1]),),
            dataset="minute_bin",
        )
        assert positions == expected

    assert initial_visits == len(calendar)
    assert row_visits == initial_visits


@pytest.mark.parametrize("instrument_kind", ["stock", "index"])
def test_candidate_rejects_contract_extra_qlib_feature(tmp_path: Path, dataset_profile, instrument_kind: str) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    code = STOCKS[0] if instrument_kind == "stock" else dataset_profile.index_codes[0]
    _write_bin(
        root / "daily_bin" / "qlib" / "features" / code.lower() / "extra.day.bin",
        [1.0] * len(DATES),
    )

    with pytest.raises(CandidateValidationError, match=r"extra=\['extra'\]"):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_candidate_rejects_code_only_index_instrument_rows(tmp_path: Path, dataset_profile) -> None:
    root, factor_receipt, index_receipt, minute_source = _candidate(tmp_path, dataset_profile)
    index_path = root / "daily_bin" / "qlib" / "instruments" / "index.txt"
    index_path.write_text(
        "".join(f"{code}\n" for code in dataset_profile.index_codes),
        encoding="utf-8",
    )

    with pytest.raises(CandidateValidationError, match="instrument row 1 is invalid"):
        CandidateValidator().validate(
            _spec(
                root,
                factor_receipt,
                index_receipt,
                minute_source,
                dataset_profile,
            )
        )


def test_minute_vectorized_parity_obeys_chunk_bound_and_checks_every_cell(
    tmp_path: Path,
) -> None:
    code = "000001.SZ"
    days = tuple(pd.bdate_range("2026-06-01", periods=20).date)
    timestamps: list[str] = []
    for day in days:
        morning = [datetime.combine(day, time(9, 31)) + timedelta(minutes=value) for value in range(120)]
        afternoon = [datetime.combine(day, time(13, 1)) + timedelta(minutes=value) for value in range(120)]
        timestamps.extend(value.isoformat(sep=" ", timespec="seconds") for value in (*morning, *afternoon))
    csv_path = tmp_path / f"{code}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "symbol", *MINUTE_FIELDS])
        writer.writeheader()
        for ordinal, timestamp in enumerate(timestamps):
            writer.writerow(
                {
                    "date": timestamp,
                    "symbol": code,
                    **{field: float(ordinal + position + 1) for position, field in enumerate(MINUTE_FIELDS)},
                }
            )
    features = {}
    for position, field in enumerate(MINUTE_FIELDS):
        path = tmp_path / f"{field}.1min.bin"
        _write_bin(
            path,
            [float(ordinal + position + 1) for ordinal in range(len(timestamps))],
        )
        features[field] = (
            0,
            np.memmap(path, dtype="<f4", mode="r"),
        )
    metrics = {"chunks": 0, "peak_chunk_rows": 0}

    rows, values, _delta, counts = _stream_minute_csv_parity(
        csv_path,
        instrument=code,
        expected_sha256=sha256_file(csv_path),
        expected_rows=len(timestamps),
        calendar_index=pd.Index(timestamps, dtype="object"),
        feature_values=features,
        max_chunk_rows=127,
        metrics=metrics,
    )

    assert rows == len(timestamps)
    assert values == rows * len(MINUTE_FIELDS)
    assert tuple(counts.values()) == (240,) * len(days)
    assert metrics == {
        "chunks": (len(timestamps) + 126) // 127,
        "peak_chunk_rows": 127,
    }
