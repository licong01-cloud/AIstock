from __future__ import annotations

import json
import shutil
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
    canonicalize,
)

from .errors import AdvisoryModelingError, REASON_FEATURE_CLOSURE_INCOMPLETE


class RerankerDatasetSpool:
    """Operation-owned bounded spool for verified base and feature-source rows."""

    _OWNED_FILES = (
        "dataset.sqlite3",
        "dataset.sqlite3-journal",
        "dataset.sqlite3-wal",
        "dataset.sqlite3-shm",
    )

    def __init__(
        self,
        *,
        output_root: Path,
        repository_root: Path,
        artifact_root: Path,
        operation_id: str,
    ) -> None:
        roots = {
            "output_root": output_root,
            "repository_root": repository_root,
            "artifact_root": artifact_root,
        }
        if any(not value.is_absolute() for value in roots.values()):
            raise ValueError("spool roots must be explicit absolute paths")
        repository = repository_root.resolve(strict=True)
        artifact = artifact_root.resolve(strict=True)
        output = output_root.resolve(strict=True)
        if not all(value.is_dir() for value in (repository, artifact, output)):
            raise ValueError("spool roots must be existing directories")
        if any(
            output == protected or output in protected.parents or protected in output.parents
            for protected in (repository, artifact)
        ):
            raise ValueError("spool output root must not overlap repository or artifact root")
        if not operation_id or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
            for character in operation_id
        ):
            raise ValueError("operation_id must be a safe non-empty identifier")
        temp_root = (output / ".advisory-modeling-tmp").resolve()
        operation_root = (temp_root / operation_id).resolve()
        if output not in temp_root.parents or temp_root not in operation_root.parents:
            raise ValueError("spool path escapes its explicit output root")
        operation_root.mkdir(parents=True, exist_ok=False)
        self._operation_root = operation_root
        self._path = operation_root / "dataset.sqlite3"
        self._conn: sqlite3.Connection | None = None
        self._closed = False
        self._failed = False
        try:
            self._conn = sqlite3.connect(self._path)
            self._conn.execute("PRAGMA journal_mode=DELETE")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(
                """
                CREATE TABLE rows (
                    source_kind TEXT NOT NULL,
                    source_identity TEXT NOT NULL,
                    logical_role TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    row_identity TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (source_kind, source_identity, logical_role, row_identity)
                );
                CREATE INDEX rows_by_role_date
                    ON rows(source_kind, source_identity, logical_role, trade_date, symbol);
                CREATE INDEX rows_by_symbol_date
                    ON rows(source_kind, source_identity, logical_role, symbol, trade_date);
                CREATE TABLE append_receipts (
                    source_kind TEXT NOT NULL,
                    source_identity TEXT NOT NULL,
                    logical_role TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    row_count INTEGER NOT NULL CHECK (row_count >= 0),
                    row_set_hash TEXT NOT NULL,
                    PRIMARY KEY (source_kind, source_identity, logical_role, partition_key)
                );
                """
            )
            self._conn.commit()
        except Exception:
            self._close()
            self._remove_owned_root()
            raise

    @property
    def path(self) -> Path:
        return self._path

    def _connection(self) -> sqlite3.Connection:
        if self._closed or self._failed or self._conn is None:
            raise RuntimeError("spool is closed or failed a prior integrity check")
        return self._conn

    def append_partition(
        self,
        *,
        source_kind: str,
        source_identity: str,
        logical_role: str,
        partition_key: str,
        rows: Iterable[Mapping[str, Any]],
        identity_fields: tuple[str, ...],
        trade_date_field: str | None,
        symbol_field: str | None,
    ) -> tuple[int, str]:
        if not identity_fields or len(identity_fields) != len(set(identity_fields)):
            raise ValueError("identity_fields must be non-empty and unique")
        for field_name, value in (
            ("source_kind", source_kind),
            ("source_identity", source_identity),
            ("logical_role", logical_role),
            ("partition_key", partition_key),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{field_name} must be non-empty without surrounding whitespace")
        conn = self._connection()
        row_entries: list[tuple[str, str]] = []
        try:
            with conn:
                for raw in rows:
                    payload = canonicalize(dict(raw))
                    missing = tuple(field for field in identity_fields if field not in payload)
                    if missing:
                        raise AdvisoryModelingError(
                            REASON_FEATURE_CLOSURE_INCOMPLETE,
                            "spool row lacks frozen identity fields",
                            context={"logical_role": logical_role, "missing_fields": missing},
                        )
                    identity_values = tuple(payload[field] for field in identity_fields)
                    if any(value is None or value == "" for value in identity_values):
                        raise AdvisoryModelingError(
                            REASON_FEATURE_CLOSURE_INCOMPLETE,
                            "spool row has empty frozen identity values",
                            context={"logical_role": logical_role},
                        )
                    row_identity = canonical_json_text(identity_values)
                    trade_date = "" if trade_date_field is None else str(payload[trade_date_field])
                    symbol = "" if symbol_field is None else str(payload[symbol_field])
                    payload_json = canonical_json_text(payload)
                    payload_hash = canonical_json_sha256(payload)
                    conn.execute(
                        "INSERT INTO rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            source_kind,
                            source_identity,
                            logical_role,
                            partition_key,
                            trade_date,
                            symbol,
                            row_identity,
                            payload_hash,
                            payload_json,
                        ),
                    )
                    row_entries.append((row_identity, payload_hash))
                row_set_hash = canonical_json_sha256(tuple(sorted(row_entries)))
                conn.execute(
                    "INSERT INTO append_receipts VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        source_kind,
                        source_identity,
                        logical_role,
                        partition_key,
                        len(row_entries),
                        row_set_hash,
                    ),
                )
        except (sqlite3.DatabaseError, KeyError, TypeError, ValueError) as exc:
            self._failed = True
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "spool partition append failed",
                context={"logical_role": logical_role, "error_type": type(exc).__name__},
            ) from exc
        except AdvisoryModelingError:
            self._failed = True
            raise
        readback = tuple(
            (str(row[0]), str(row[1]), str(row[2]))
            for row in conn.execute(
                """
                SELECT row_identity, payload_hash, payload_json
                FROM rows
                WHERE source_kind = ? AND source_identity = ?
                  AND logical_role = ? AND partition_key = ?
                ORDER BY row_identity
                """,
                (source_kind, source_identity, logical_role, partition_key),
            )
        )
        readback_entries = tuple(
            (identity, canonical_json_sha256(json.loads(payload_json)))
            for identity, _stored_hash, payload_json in readback
        )
        expected_hash = canonical_json_sha256(tuple(sorted(row_entries)))
        if (
            len(readback) != len(row_entries)
            or canonical_json_sha256(readback_entries) != expected_hash
            or any(stored != recomputed for (_, stored, _), (_, recomputed) in zip(readback, readback_entries, strict=True))
        ):
            self._failed = True
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "spool partition differs on exact readback",
                context={"logical_role": logical_role, "partition_key": partition_key},
            )
        return len(row_entries), expected_hash

    def iter_rows(
        self,
        *,
        source_kind: str,
        source_identity: str,
        logical_role: str,
        start_date: str | None = None,
        end_date: str | None = None,
        symbol: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        conn = self._connection()
        sql = (
            "SELECT payload_json FROM rows WHERE source_kind = ? AND source_identity = ? "
            "AND logical_role = ?"
        )
        params: list[str] = [source_kind, source_identity, logical_role]
        if start_date is not None:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        if end_date is not None:
            sql += " AND trade_date <= ?"
            params.append(end_date)
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(symbol)
        sql += " ORDER BY trade_date, symbol, row_identity"
        for (payload_json,) in conn.execute(sql, params):
            yield dict(json.loads(str(payload_json)))

    def partition_receipts(
        self,
        *,
        source_kind: str,
        source_identity: str,
    ) -> tuple[dict[str, Any], ...]:
        return tuple(
            {
                "logical_role": str(row[0]),
                "partition_key": str(row[1]),
                "row_count": int(row[2]),
                "row_set_hash": str(row[3]),
            }
            for row in self._connection().execute(
                """
                SELECT logical_role, partition_key, row_count, row_set_hash
                FROM append_receipts
                WHERE source_kind = ? AND source_identity = ?
                ORDER BY logical_role, partition_key
                """,
                (source_kind, source_identity),
            )
        )

    def _close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def close(self) -> None:
        if not self._closed:
            self._close()
            self._closed = True

    def _remove_owned_root(self) -> None:
        operation_root = self._operation_root.resolve()
        for name in self._OWNED_FILES:
            path = (operation_root / name).resolve()
            if operation_root not in path.parents:
                raise RuntimeError("spool cleanup target escapes operation root")
        if operation_root.exists():
            shutil.rmtree(operation_root)
        parent = operation_root.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()

    def cleanup(self) -> None:
        self.close()
        self._remove_owned_root()

    def __enter__(self) -> "RerankerDatasetSpool":
        return self

    def __exit__(self, *_args: object) -> None:
        self.cleanup()
