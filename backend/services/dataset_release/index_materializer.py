from __future__ import annotations

import json
import os
import shutil
import stat
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

import pandas as pd

from .index_contract import (
    DOMESTIC_INDEX_DEFINITIONS,
    INDEX_H5_COLUMNS,
    INDEX_QLIB_FIELDS,
    INDEX_SCHEMA_VERSION,
    INDEX_SOURCE_VALUE_FIELDS,
    INDEX_UNIVERSE_VERSION,
    IndexDefinition,
    index_contract_digest,
    index_contract_payload,
    merge_index_rows_missing_only,
    validate_index_definitions,
)
from .errors import IndexContractError
from .streaming_artifacts import (
    finalize_h5_from_parquet_chunks,
    finalize_parquet_chunks,
    iter_parquet_frames,
    write_frame_parquet_atomic,
)


_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class IndexContextSource(Protocol):
    def trading_dates(self, start: date, end: date) -> Sequence[date]: ...

    def database_rows(self, definition: IndexDefinition, start: date, end: date) -> Iterable[Mapping[str, Any]]: ...

    def provider_rows(self, definition: IndexDefinition, start: date, end: date) -> Iterable[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class IndexMaterializationReceipt:
    root: Path
    h5_path: Path
    parquet_path: Path
    csv_root: Path
    rows: int
    provider_fill_rows: int
    contract_digest: str
    details: Mapping[str, Any]


def _assert_plain_existing_chain(path: Path) -> None:
    current = path
    while True:
        if current.exists():
            info = current.lstat()
            if current.is_symlink() or bool(getattr(info, "st_file_attributes", 0) & _REPARSE_POINT):
                raise IndexContractError("index output path contains a symlink or reparse point")
        if current.parent == current:
            return
        current = current.parent


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
    finally:
        if descriptor != -1:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _normalized_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        if "vol" not in row and "volume" in row:
            row["vol"] = row["volume"]
        missing = [field for field in INDEX_SOURCE_VALUE_FIELDS if field not in row]
        if missing:
            raise IndexContractError(f"index source row is missing fields: {missing}")
        normalized.append(row)
    return normalized


def _to_context_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows:
        volume_hand = float(row["vol"])
        records.append(
            {
                "datetime": pd.Timestamp(row["trade_date"]),
                "instrument": str(row["ts_code"]),
                "idx_open_point": float(row["open"]),
                "idx_high_point": float(row["high"]),
                "idx_low_point": float(row["low"]),
                "idx_close_point": float(row["close"]),
                "idx_pre_close_point": float(row["pre_close"]),
                "idx_return_1d": float(row["pct_chg"]) / 100.0,
                "idx_volume_hand_source": volume_hand,
                "idx_volume_share_equiv": volume_hand * 100.0,
                "idx_amount_cny": float(row["amount"]) * 1000.0,
            }
        )
    frame = pd.DataFrame.from_records(records)
    if frame.empty:
        raise IndexContractError("index context has no rows")
    frame = frame.set_index(["datetime", "instrument"]).sort_index()
    if frame.index.duplicated().any():
        raise IndexContractError("index context contains duplicate keys")
    frame = frame.loc[:, list(INDEX_H5_COLUMNS)]
    for column in frame.columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float32")
    return frame


class IndexContextMaterializer:
    def __init__(
        self,
        source: IndexContextSource,
        *,
        definitions: Sequence[IndexDefinition] = DOMESTIC_INDEX_DEFINITIONS,
    ) -> None:
        self.source = source
        self.definitions = validate_index_definitions(definitions)

    def materialize(
        self,
        output_root: Path,
        *,
        cutoff: date,
        row_group_rows: int = 100_000,
    ) -> IndexMaterializationReceipt:
        root = Path(output_root)
        _assert_plain_existing_chain(root.parent)
        if root.exists():
            raise FileExistsError(root)
        root.mkdir(parents=False)
        chunks = root / ".chunks"
        chunks.mkdir()
        csv_root = root / "index_csv"
        csv_root.mkdir()
        merged_all: list[dict[str, Any]] = []
        details: dict[str, Any] = {}
        provider_fill_rows = 0
        for definition in self.definitions:
            start = definition.required_from
            if cutoff < start:
                raise IndexContractError(f"cutoff precedes required index start: {definition.daily_code}")
            expected_dates = tuple(self.source.trading_dates(start, cutoff))
            if not expected_dates:
                raise IndexContractError(f"trading calendar is empty for index: {definition.daily_code}")
            database = _normalized_rows(self.source.database_rows(definition, start, cutoff))
            database_keys = {
                (str(row["ts_code"]).upper(), date.fromisoformat(str(row["trade_date"]))) for row in database
            }
            expected_keys = {(definition.daily_code, value) for value in expected_dates}
            provider = (
                _normalized_rows(self.source.provider_rows(definition, start, cutoff))
                if expected_keys.difference(database_keys)
                else []
            )
            merged, evidence = merge_index_rows_missing_only(database, provider)
            merged_keys = {(str(row["ts_code"]), row["trade_date"]) for row in merged}
            missing = sorted(expected_keys.difference(merged_keys))
            extras = sorted(merged_keys.difference(expected_keys))
            if missing or extras:
                raise IndexContractError(
                    f"index calendar coverage mismatch: {definition.daily_code}",
                    context={
                        "missing_count": len(missing),
                        "missing_sample": [f"{code}:{day}" for code, day in missing[:20]],
                        "extra_count": len(extras),
                    },
                )
            details[definition.daily_code] = {
                **evidence,
                "required_from": start.isoformat(),
                "cutoff": cutoff.isoformat(),
                "expected_rows": len(expected_keys),
            }
            provider_fill_rows += int(evidence["provider_fill_rows"])
            merged_all.extend(merged)

        frame = _to_context_frame(merged_all)
        parquet_chunk = chunks / "index_context.parquet"
        write_frame_parquet_atomic(frame, parquet_chunk, row_group_size=row_group_rows)
        h5_path = root / "index_daily.h5"
        h5_receipt = finalize_h5_from_parquet_chunks(
            [parquet_chunk],
            h5_path,
            expected_columns=INDEX_H5_COLUMNS,
            max_rows_in_memory=row_group_rows,
        )
        parquet_path = root / "index_context.parquet"
        parquet_receipt = finalize_parquet_chunks([parquet_chunk], parquet_path, max_rows_in_memory=row_group_rows)
        parquet_chunk.unlink()
        chunks.rmdir()

        for code, group in frame.reset_index().groupby("instrument", sort=True):
            source = group.copy()
            csv = pd.DataFrame(
                {
                    "date": source["datetime"].dt.strftime("%Y-%m-%d"),
                    "symbol": code,
                    "open": source["idx_open_point"],
                    "high": source["idx_high_point"],
                    "low": source["idx_low_point"],
                    "close": source["idx_close_point"],
                    "volume": source["idx_volume_share_equiv"],
                    "amount": source["idx_amount_cny"],
                    "factor": 1.0,
                    # Indices have no exchange price-limit regime.  The exact
                    # shared 12-field provider schema uses previous close as a
                    # finite neutral sentinel and keeps both hit flags false.
                    "up_limit_price": source["idx_pre_close_point"],
                    "down_limit_price": source["idx_pre_close_point"],
                    "prev_close": source["idx_pre_close_point"],
                    "limit_up": 0.0,
                    "limit_down": 0.0,
                }
            )
            expected_csv = ("date", "symbol", *INDEX_QLIB_FIELDS)
            if tuple(csv.columns) != expected_csv:
                raise IndexContractError("index Qlib 12-field contract drifted")
            target = csv_root / f"{code}.csv"
            temporary = target.with_name(f".{target.name}.partial")
            csv.to_csv(temporary, index=False)
            os.replace(temporary, target)

        receipt_payload = {
            "schema_version": "dataset_release_index_materialization_v1",
            "index_schema_version": INDEX_SCHEMA_VERSION,
            "index_universe_version": INDEX_UNIVERSE_VERSION,
            "contract_digest": index_contract_digest(),
            "contract": index_contract_payload(),
            "cutoff": cutoff.isoformat(),
            "rows": len(frame),
            "provider_fill_rows": provider_fill_rows,
            "details": details,
            "h5": h5_receipt,
            "parquet": parquet_receipt,
            "database_writes": 0,
            "production_writes": 0,
        }
        _atomic_json(root / "index_materialization_receipt.json", receipt_payload)
        return IndexMaterializationReceipt(
            root=root,
            h5_path=h5_path,
            parquet_path=parquet_path,
            csv_root=csv_root,
            rows=len(frame),
            provider_fill_rows=provider_fill_rows,
            contract_digest=index_contract_digest(),
            details=details,
        )


class IncrementalIndexContextMaterializer:
    """Append one proven calendar tail from a sealed baseline artifact."""

    def __init__(
        self,
        source: IndexContextSource,
        *,
        definitions: Sequence[IndexDefinition] = DOMESTIC_INDEX_DEFINITIONS,
    ) -> None:
        self.source = source
        self.definitions = validate_index_definitions(definitions)

    def materialize(
        self,
        baseline_root: Path,
        output_root: Path,
        *,
        baseline_cutoff: date,
        cutoff: date,
        row_group_rows: int = 100_000,
    ) -> IndexMaterializationReceipt:
        if baseline_cutoff >= cutoff:
            raise IndexContractError("incremental index cutoff is not a strict tail")
        baseline = Path(baseline_root).resolve(strict=True)
        _assert_plain_existing_chain(baseline)
        baseline_receipt_path = baseline / "index_materialization_receipt.json"
        try:
            baseline_receipt = json.loads(baseline_receipt_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IndexContractError("baseline index receipt is unreadable") from exc
        if (
            not isinstance(baseline_receipt, Mapping)
            or baseline_receipt.get("schema_version") != "dataset_release_index_materialization_v1"
            or baseline_receipt.get("contract_digest") != index_contract_digest()
            or baseline_receipt.get("cutoff") != baseline_cutoff.isoformat()
        ):
            raise IndexContractError("baseline index receipt identity differs")
        baseline_parquet = baseline / "index_context.parquet"
        baseline_h5 = baseline / "index_daily.h5"
        baseline_csv = baseline / "index_csv"
        if not baseline_parquet.is_file() or not baseline_h5.is_file() or not baseline_csv.is_dir():
            raise IndexContractError("baseline index artifact is incomplete")

        root = Path(output_root)
        _assert_plain_existing_chain(root.parent)
        if root.exists():
            raise FileExistsError(root)
        root.mkdir(parents=False)
        chunks = root / ".chunks"
        chunks.mkdir()
        csv_root = root / "index_csv"
        csv_root.mkdir()
        start = baseline_cutoff + timedelta(days=1)
        merged_all: list[dict[str, Any]] = []
        details: dict[str, Any] = {}
        provider_fill_rows = 0
        for definition in self.definitions:
            expected_dates = tuple(self.source.trading_dates(start, cutoff))
            if not expected_dates:
                raise IndexContractError(f"incremental trading calendar is empty: {definition.daily_code}")
            database = _normalized_rows(self.source.database_rows(definition, start, cutoff))
            database_keys = {
                (str(row["ts_code"]).upper(), date.fromisoformat(str(row["trade_date"]))) for row in database
            }
            expected_keys = {(definition.daily_code, value) for value in expected_dates}
            provider = (
                _normalized_rows(self.source.provider_rows(definition, start, cutoff))
                if expected_keys.difference(database_keys)
                else []
            )
            merged, evidence = merge_index_rows_missing_only(database, provider)
            merged_keys = {(str(row["ts_code"]), row["trade_date"]) for row in merged}
            if merged_keys != expected_keys:
                raise IndexContractError(f"incremental index calendar coverage mismatch: {definition.daily_code}")
            prior = dict((baseline_receipt.get("details") or {}).get(definition.daily_code) or {})
            prior_expected = int(prior.get("expected_rows", 0))
            if prior_expected <= 0 or prior.get("cutoff") != baseline_cutoff.isoformat():
                raise IndexContractError("baseline index detail coverage differs")
            details[definition.daily_code] = {
                "database_rows": int(prior.get("database_rows", 0)) + int(evidence["database_rows"]),
                "provider_rows": int(prior.get("provider_rows", 0)) + int(evidence["provider_rows"]),
                "overlap_rows_verified": int(prior.get("overlap_rows_verified", 0))
                + int(evidence["overlap_rows_verified"]),
                "provider_fill_rows": int(prior.get("provider_fill_rows", 0)) + int(evidence["provider_fill_rows"]),
                "overlap_mismatch_cells": 0,
                "source_precedence": "database_then_provider_missing_keys_conflict_fail_v1",
                "required_from": definition.required_from.isoformat(),
                "cutoff": cutoff.isoformat(),
                "expected_rows": prior_expected + len(expected_keys),
            }
            provider_fill_rows += int(evidence["provider_fill_rows"])
            merged_all.extend(merged)

        new_frame = _to_context_frame(merged_all)
        new_chunk = chunks / "index_context_tail.parquet"
        write_frame_parquet_atomic(new_frame, new_chunk, row_group_size=row_group_rows)
        h5_path = root / "index_daily.h5"
        h5_receipt = finalize_h5_from_parquet_chunks(
            [baseline_parquet, new_chunk],
            h5_path,
            expected_columns=INDEX_H5_COLUMNS,
            max_rows_in_memory=row_group_rows,
        )
        parquet_path = root / "index_context.parquet"
        parquet_receipt = finalize_parquet_chunks(
            [baseline_parquet, new_chunk],
            parquet_path,
            max_rows_in_memory=row_group_rows,
        )
        new_chunk.unlink()
        chunks.rmdir()

        tail_csv = _index_csv_frames(new_frame)
        for definition in self.definitions:
            code = definition.daily_code
            source_path = baseline_csv / f"{code}.csv"
            target = csv_root / f"{code}.csv"
            if not source_path.is_file() or code not in tail_csv:
                raise IndexContractError(f"incremental index CSV baseline/tail missing: {code}")
            shutil.copyfile(source_path, target)
            tail_csv[code].to_csv(target, mode="a", header=False, index=False)

        rows = int(baseline_receipt.get("rows", 0)) + len(new_frame)
        receipt_payload = {
            "schema_version": "dataset_release_index_materialization_v1",
            "index_schema_version": INDEX_SCHEMA_VERSION,
            "index_universe_version": INDEX_UNIVERSE_VERSION,
            "contract_digest": index_contract_digest(),
            "contract": index_contract_payload(),
            "cutoff": cutoff.isoformat(),
            "rows": rows,
            "provider_fill_rows": int(baseline_receipt.get("provider_fill_rows", 0)) + provider_fill_rows,
            "details": details,
            "h5": h5_receipt,
            "parquet": parquet_receipt,
            "incremental": {
                "baseline_cutoff": baseline_cutoff.isoformat(),
                "tail_start": start.isoformat(),
                "tail_rows": len(new_frame),
                "baseline_rows_retransformed": 0,
            },
            "database_writes": 0,
            "production_writes": 0,
        }
        _atomic_json(root / "index_materialization_receipt.json", receipt_payload)
        return IndexMaterializationReceipt(
            root=root,
            h5_path=h5_path,
            parquet_path=parquet_path,
            csv_root=csv_root,
            rows=rows,
            provider_fill_rows=int(receipt_payload["provider_fill_rows"]),
            contract_digest=index_contract_digest(),
            details=details,
        )


class SelectiveIndexContextMaterializer:
    """Patch bounded historical index dates over sealed baseline artifacts."""

    def __init__(
        self,
        source: IndexContextSource,
        *,
        definitions: Sequence[IndexDefinition] = DOMESTIC_INDEX_DEFINITIONS,
    ) -> None:
        self.source = source
        self.definitions = validate_index_definitions(definitions)

    def materialize(
        self,
        baseline_root: Path,
        output_root: Path,
        *,
        cutoff: date,
        date_ranges: Sequence[tuple[date, date]],
        row_group_rows: int = 100_000,
    ) -> IndexMaterializationReceipt:
        ranges = tuple(sorted(date_ranges))
        if (
            not ranges
            or any(start > end or end > cutoff for start, end in ranges)
            or any(current[0] <= previous[1] for previous, current in zip(ranges, ranges[1:]))
        ):
            raise IndexContractError("selective index date ranges are invalid")
        baseline = Path(baseline_root).resolve(strict=True)
        receipt_path = baseline / "index_materialization_receipt.json"
        try:
            baseline_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IndexContractError("baseline index receipt is unreadable") from exc
        if (
            not isinstance(baseline_receipt, Mapping)
            or baseline_receipt.get("contract_digest") != index_contract_digest()
            or baseline_receipt.get("cutoff") != cutoff.isoformat()
        ):
            raise IndexContractError("selective baseline index identity differs")
        baseline_parquet = baseline / "index_context.parquet"
        baseline_csv = baseline / "index_csv"
        if not baseline_parquet.is_file() or not baseline_csv.is_dir():
            raise IndexContractError("selective baseline index artifact is incomplete")

        patch_rows: list[dict[str, Any]] = []
        provider_fill_rows = 0
        for definition in self.definitions:
            for start, end in ranges:
                expected_dates = tuple(self.source.trading_dates(start, end))
                if not expected_dates:
                    continue
                database = _normalized_rows(self.source.database_rows(definition, start, end))
                keys = {(str(row["ts_code"]).upper(), date.fromisoformat(str(row["trade_date"]))) for row in database}
                expected = {(definition.daily_code, value) for value in expected_dates}
                provider = (
                    _normalized_rows(self.source.provider_rows(definition, start, end))
                    if expected.difference(keys)
                    else []
                )
                merged, evidence = merge_index_rows_missing_only(database, provider)
                if {(str(row["ts_code"]), row["trade_date"]) for row in merged} != expected:
                    raise IndexContractError(f"selective index coverage mismatch: {definition.daily_code}")
                patch_rows.extend(merged)
                provider_fill_rows += int(evidence["provider_fill_rows"])
        patch_frame = _to_context_frame(patch_rows)

        root = Path(output_root)
        _assert_plain_existing_chain(root.parent)
        if root.exists():
            raise FileExistsError(root)
        root.mkdir(parents=False)
        chunks = root / ".chunks"
        chunks.mkdir()
        chunk_paths: list[Path] = []
        seen: set[tuple[pd.Timestamp, str]] = set()
        for ordinal, frame in enumerate(iter_parquet_frames([baseline_parquet], max_rows=row_group_rows)):
            common = frame.index.intersection(patch_frame.index)
            if len(common):
                frame.loc[common, list(INDEX_H5_COLUMNS)] = patch_frame.loc[common, list(INDEX_H5_COLUMNS)].to_numpy()
                seen.update((pd.Timestamp(value[0]), str(value[1])) for value in common)
            chunk = chunks / f"index_context_{ordinal:06d}.parquet"
            write_frame_parquet_atomic(frame, chunk, row_group_size=row_group_rows)
            chunk_paths.append(chunk)
        expected_patch = {(pd.Timestamp(value[0]), str(value[1])) for value in patch_frame.index}
        if seen != expected_patch or not chunk_paths:
            raise IndexContractError("selective index keys are absent from baseline")
        h5_path = root / "index_daily.h5"
        h5_receipt = finalize_h5_from_parquet_chunks(
            chunk_paths,
            h5_path,
            expected_columns=INDEX_H5_COLUMNS,
            max_rows_in_memory=row_group_rows,
        )
        parquet_path = root / "index_context.parquet"
        parquet_receipt = finalize_parquet_chunks(chunk_paths, parquet_path, max_rows_in_memory=row_group_rows)
        for chunk in chunk_paths:
            chunk.unlink()
        chunks.rmdir()

        csv_root = root / "index_csv"
        shutil.copytree(baseline_csv, csv_root)
        for code, values in _index_csv_frames(patch_frame).items():
            target = csv_root / f"{code}.csv"
            existing = pd.read_csv(target, dtype={"symbol": "string"})
            if tuple(existing.columns) != ("date", "symbol", *INDEX_QLIB_FIELDS):
                raise IndexContractError("selective baseline index CSV schema differs")
            existing = existing.set_index("date", drop=False)
            replacement = values.set_index("date", drop=False)
            missing = replacement.index.difference(existing.index)
            if len(missing):
                raise IndexContractError("selective index CSV key is absent from baseline")
            existing.loc[replacement.index, list(replacement.columns)] = replacement.to_numpy()
            temporary = target.with_name(f".{target.name}.selective.partial")
            existing.reset_index(drop=True).to_csv(temporary, index=False)
            os.replace(temporary, target)

        rows = int(baseline_receipt.get("rows", 0))
        receipt_payload = {
            **dict(baseline_receipt),
            "h5": h5_receipt,
            "parquet": parquet_receipt,
            "selective": {
                "date_ranges": [{"start": start.isoformat(), "end": end.isoformat()} for start, end in ranges],
                "source_rows_transformed": len(patch_frame),
                "baseline_source_rows_retransformed": 0,
                "baseline_artifact_rows_streamed": rows,
            },
            "provider_fill_rows": int(baseline_receipt.get("provider_fill_rows", 0)) + provider_fill_rows,
            "database_writes": 0,
            "production_writes": 0,
        }
        _atomic_json(root / "index_materialization_receipt.json", receipt_payload)
        return IndexMaterializationReceipt(
            root=root,
            h5_path=h5_path,
            parquet_path=parquet_path,
            csv_root=csv_root,
            rows=rows,
            provider_fill_rows=int(receipt_payload["provider_fill_rows"]),
            contract_digest=index_contract_digest(),
            details=dict(baseline_receipt.get("details") or {}),
        )


def _index_csv_frames(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
    for code, group in frame.reset_index().groupby("instrument", sort=True):
        source = group.copy()
        output[str(code)] = pd.DataFrame(
            {
                "date": source["datetime"].dt.strftime("%Y-%m-%d"),
                "symbol": code,
                "open": source["idx_open_point"],
                "high": source["idx_high_point"],
                "low": source["idx_low_point"],
                "close": source["idx_close_point"],
                "volume": source["idx_volume_share_equiv"],
                "amount": source["idx_amount_cny"],
                "factor": 1.0,
                "up_limit_price": source["idx_pre_close_point"],
                "down_limit_price": source["idx_pre_close_point"],
                "prev_close": source["idx_pre_close_point"],
                "limit_up": 0.0,
                "limit_down": 0.0,
            }
        )
    if any(tuple(value.columns) != ("date", "symbol", *INDEX_QLIB_FIELDS) for value in output.values()):
        raise IndexContractError("incremental index CSV field contract drifted")
    return output
