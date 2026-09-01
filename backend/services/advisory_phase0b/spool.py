from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonicalize,
)
from backend.services.advisory_historical_range.models import require_sha256

from .errors import Phase0BAuditError, REASON_RELATION_CLOSURE_INVALID


class Phase0BBoundedSpool:
    """Operation-owned SQLite spool bounded by one verified snapshot file."""

    _SQLITE_FILENAMES = (
        "audit.sqlite3",
        "audit.sqlite3-journal",
        "audit.sqlite3-wal",
        "audit.sqlite3-shm",
    )

    def __init__(
        self,
        *,
        output_root: Path,
        repository_root: Path,
        dataset_root: Path,
        operation_id: str,
    ) -> None:
        for label, path in (
            ("output root", output_root),
            ("repository root", repository_root),
            ("dataset root", dataset_root),
        ):
            if not path.is_absolute():
                raise ValueError(f"spool {label} must be an explicit absolute path")
        if not repository_root.is_dir() or not dataset_root.is_dir():
            raise ValueError("spool repository and dataset roots must be existing directories")
        if not operation_id.strip() or any(
            char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
            for char in operation_id
        ):
            raise ValueError("spool requires a safe non-empty operation id")
        root = output_root.resolve()
        repository = repository_root.resolve()
        dataset = dataset_root.resolve()
        for label, protected in (("repository", repository), ("dataset", dataset)):
            if (
                root == protected
                or protected in root.parents
                or root in protected.parents
            ):
                raise ValueError(f"spool output root must not overlap the {label} root")
        temp_root = (root / ".phase0b-tmp").resolve()
        if root not in temp_root.parents:
            raise ValueError("spool temp root escapes output root")
        operation_root = (temp_root / operation_id).resolve()
        if temp_root not in operation_root.parents:
            raise ValueError("spool operation root escapes temp root")
        operation_root.mkdir(parents=True, exist_ok=False)
        self._temp_root = temp_root
        self._operation_root = operation_root
        self._path = operation_root / "audit.sqlite3"
        self._conn: sqlite3.Connection | None = None
        self._closed = False
        self._poisoned = False
        try:
            self._conn = sqlite3.connect(self._path)
            self._conn.execute("PRAGMA journal_mode=DELETE")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(
                """
                CREATE TABLE rows (
                    snapshot_id TEXT NOT NULL,
                    logical_role TEXT NOT NULL,
                    source_file_sha256 TEXT NOT NULL,
                    decision_date TEXT NOT NULL,
                    identity_key TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (snapshot_id, logical_role, identity_key)
                );
                CREATE TABLE append_receipts (
                    snapshot_id TEXT NOT NULL,
                    logical_role TEXT NOT NULL,
                    source_file_sha256 TEXT NOT NULL,
                    row_count INTEGER NOT NULL CHECK (row_count >= 0),
                    rows_hash TEXT NOT NULL,
                    PRIMARY KEY (snapshot_id, logical_role, source_file_sha256)
                );
                CREATE INDEX rows_by_date
                    ON rows(snapshot_id, decision_date, logical_role, identity_key);
                CREATE INDEX rows_by_source
                    ON rows(snapshot_id, logical_role, source_file_sha256, identity_key);
                """
            )
            self._conn.commit()
        except Exception as error:
            self._close_connection()
            try:
                self._remove_owned_files_and_directories()
            except Exception as cleanup_error:
                raise RuntimeError("spool initialization and exact cleanup both failed") from cleanup_error
            raise error

    @property
    def path(self) -> Path:
        return self._path

    def _require_connection(self) -> sqlite3.Connection:
        if self._closed or self._poisoned or self._conn is None:
            raise RuntimeError("spool is closed or failed a prior integrity check")
        return self._conn

    @staticmethod
    def _validate_non_empty(value: str, *, field_name: str) -> str:
        if not value.strip() or value != value.strip():
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                f"spool {field_name} must be non-empty without surrounding whitespace",
                context={"field": field_name},
            )
        return value

    @staticmethod
    def _rows_hash(entries: Iterable[tuple[str, str]]) -> str:
        return canonical_json_sha256(tuple(sorted(entries)))

    def append_rows(
        self,
        *,
        snapshot_id: str,
        logical_role: str,
        source_file_sha256: str,
        rows: Iterable[Mapping[str, Any]],
        identity_fields: tuple[str, ...],
        decision_date_field: str | None,
    ) -> int:
        conn = self._require_connection()
        snapshot_id = self._validate_non_empty(snapshot_id, field_name="snapshot_id")
        logical_role = self._validate_non_empty(logical_role, field_name="logical_role")
        try:
            source_file_sha256 = require_sha256(
                source_file_sha256,
                field_name="source_file_sha256",
            )
        except ValueError as error:
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "spool source file hash is invalid",
                context={"logical_role": logical_role},
            ) from error
        if not identity_fields or len(identity_fields) != len(set(identity_fields)):
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "spool identity fields must be non-empty and unique",
                context={"logical_role": logical_role},
            )
        normalized_entries: list[tuple[str, str]] = []
        try:
            with conn:
                for raw in rows:
                    payload = canonicalize(dict(raw))
                    try:
                        identity_values = [payload[field] for field in identity_fields]
                    except KeyError as error:
                        raise Phase0BAuditError(
                            REASON_RELATION_CLOSURE_INVALID,
                            "spool row lacks a frozen identity field",
                            context={"logical_role": logical_role, "field": str(error)},
                        ) from error
                    if any(
                        value is None
                        or (isinstance(value, str) and (not value.strip() or value != value.strip()))
                        for value in identity_values
                    ):
                        raise Phase0BAuditError(
                            REASON_RELATION_CLOSURE_INVALID,
                            "spool row contains an empty frozen identity value",
                            context={"logical_role": logical_role},
                        )
                    if decision_date_field is None:
                        decision_date = ""
                    else:
                        if decision_date_field not in payload:
                            raise Phase0BAuditError(
                                REASON_RELATION_CLOSURE_INVALID,
                                "spool row lacks its decision date field",
                                context={
                                    "logical_role": logical_role,
                                    "field": decision_date_field,
                                },
                            )
                        decision_date = str(payload[decision_date_field])
                        try:
                            parsed_date = date.fromisoformat(decision_date)
                        except (TypeError, ValueError) as error:
                            raise Phase0BAuditError(
                                REASON_RELATION_CLOSURE_INVALID,
                                "spool row decision date is not an ISO calendar date",
                                context={
                                    "logical_role": logical_role,
                                    "field": decision_date_field,
                                    "value": decision_date,
                                },
                            ) from error
                        if parsed_date.isoformat() != decision_date:
                            raise Phase0BAuditError(
                                REASON_RELATION_CLOSURE_INVALID,
                                "spool row decision date is not canonical",
                                context={
                                    "logical_role": logical_role,
                                    "field": decision_date_field,
                                },
                            )
                    identity_key = json.dumps(
                        identity_values,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    payload_json = json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    payload_sha256 = canonical_json_sha256(payload)
                    conn.execute(
                        "INSERT INTO rows VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            snapshot_id,
                            logical_role,
                            source_file_sha256,
                            decision_date,
                            identity_key,
                            payload_sha256,
                            payload_json,
                        ),
                    )
                    normalized_entries.append((identity_key, payload_sha256))
                expected_rows_hash = self._rows_hash(normalized_entries)
                conn.execute(
                    "INSERT INTO append_receipts VALUES (?, ?, ?, ?, ?)",
                    (
                        snapshot_id,
                        logical_role,
                        source_file_sha256,
                        len(normalized_entries),
                        expected_rows_hash,
                    ),
                )
        except (sqlite3.IntegrityError, sqlite3.DatabaseError) as error:
            self._poisoned = True
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "spool rows or source receipts conflict with frozen identities",
                context={"snapshot_id": snapshot_id, "logical_role": logical_role},
            ) from error
        except Phase0BAuditError:
            self._poisoned = True
            raise
        except Exception as error:
            self._poisoned = True
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "spool row normalization failed",
                context={
                    "snapshot_id": snapshot_id,
                    "logical_role": logical_role,
                    "error_type": type(error).__name__,
                },
            ) from error
        try:
            receipt = conn.execute(
                """
                SELECT row_count, rows_hash
                FROM append_receipts
                WHERE snapshot_id = ? AND logical_role = ? AND source_file_sha256 = ?
                """,
                (snapshot_id, logical_role, source_file_sha256),
            ).fetchone()
            readback_rows = tuple(
                (str(row[0]), str(row[1]), str(row[2]))
                for row in conn.execute(
                    """
                    SELECT identity_key, payload_sha256, payload_json
                    FROM rows
                    WHERE snapshot_id = ? AND logical_role = ? AND source_file_sha256 = ?
                    ORDER BY identity_key
                    """,
                    (snapshot_id, logical_role, source_file_sha256),
                )
            )
        except sqlite3.DatabaseError as error:
            self._poisoned = True
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "spool transaction readback query failed",
                context={"snapshot_id": snapshot_id, "logical_role": logical_role},
            ) from error
        readback_entries = tuple(
            (identity_key, canonical_json_sha256(json.loads(payload_json)))
            for identity_key, _stored_hash, payload_json in readback_rows
        )
        expected_rows_hash = self._rows_hash(normalized_entries)
        if (
            receipt is None
            or int(receipt[0]) != len(normalized_entries)
            or str(receipt[1]) != expected_rows_hash
            or len(readback_entries) != len(normalized_entries)
            or self._rows_hash(readback_entries) != expected_rows_hash
            or any(
                stored_hash != recomputed_hash
                for (_, stored_hash, _), (_, recomputed_hash) in zip(
                    readback_rows,
                    readback_entries,
                    strict=True,
                )
            )
        ):
            self._poisoned = True
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "spool transaction readback differs from appended rows",
                context={
                    "snapshot_id": snapshot_id,
                    "logical_role": logical_role,
                    "source_file_sha256": source_file_sha256,
                },
            )
        return len(normalized_entries)

    def decision_dates(self, *, snapshot_id: str) -> tuple[str, ...]:
        conn = self._require_connection()
        cursor = conn.execute(
            "SELECT DISTINCT decision_date FROM rows WHERE snapshot_id = ? AND decision_date <> '' ORDER BY decision_date",
            (snapshot_id,),
        )
        return tuple(str(row[0]) for row in cursor)

    def close_relations(self, *, snapshot_id: str) -> None:
        """Close immutable role relations and propagate bounded date partitions."""

        conn = self._require_connection()
        direct_relations = (
            ("observation_versions", "canonical_signal_id", "canonical_signals", "canonical_signal_id"),
            ("selected_observations", "canonical_signal_id", "canonical_signals", "canonical_signal_id"),
            (
                "selected_observations",
                "terminal_observation_version_id",
                "observation_versions",
                "observation_version_id",
            ),
            ("lineage", "canonical_signal_id", "canonical_signals", "canonical_signal_id"),
            ("lineage", "observation_version_id", "observation_versions", "observation_version_id"),
            ("stage_summaries", "observation_version_id", "observation_versions", "observation_version_id"),
            ("stage_candidates", "stage_evidence_id", "stage_summaries", "stage_evidence_id"),
            ("selected_labels", "label_key_hash", "outcome_labels", "label_key_hash"),
            (
                "selected_labels",
                "terminal_label_version_id",
                "outcome_labels",
                "label_version_id",
            ),
        )
        try:
            with conn:
                for child_role, child_field, parent_role, parent_field in direct_relations:
                    invalid_count = int(
                        conn.execute(
                            """
                            SELECT COUNT(*)
                            FROM rows child
                            WHERE child.snapshot_id = ? AND child.logical_role = ?
                              AND (
                                SELECT COUNT(*) FROM rows parent
                                WHERE parent.snapshot_id = child.snapshot_id
                                  AND parent.logical_role = ?
                                  AND json_extract(parent.payload_json, ?) =
                                      json_extract(child.payload_json, ?)
                              ) <> 1
                            """,
                            (
                                snapshot_id,
                                child_role,
                                parent_role,
                                f"$.{parent_field}",
                                f"$.{child_field}",
                            ),
                        ).fetchone()[0]
                    )
                    if invalid_count:
                        raise Phase0BAuditError(
                            REASON_RELATION_CLOSURE_INVALID,
                            "spool relation does not have exactly one frozen parent",
                            context={
                                "snapshot_id": snapshot_id,
                                "child_role": child_role,
                                "parent_role": parent_role,
                                "invalid_count": invalid_count,
                            },
                        )
                    conn.execute(
                        """
                        UPDATE rows AS child
                        SET decision_date = (
                            SELECT parent.decision_date FROM rows parent
                            WHERE parent.snapshot_id = child.snapshot_id
                              AND parent.logical_role = ?
                              AND json_extract(parent.payload_json, ?) =
                                  json_extract(child.payload_json, ?)
                        )
                        WHERE child.snapshot_id = ? AND child.logical_role = ?
                        """,
                        (
                            parent_role,
                            f"$.{parent_field}",
                            f"$.{child_field}",
                            snapshot_id,
                            child_role,
                        ),
                    )
                self._assert_optional_parent(
                    conn=conn,
                    snapshot_id=snapshot_id,
                    child_role="outcome_labels",
                    child_field="observation_version_id",
                    parent_role="observation_versions",
                    parent_field="observation_version_id",
                )
                self._assert_optional_parent(
                    conn=conn,
                    snapshot_id=snapshot_id,
                    child_role="outcome_labels",
                    child_field="candidate_stage_evidence_id",
                    parent_role="stage_summaries",
                    parent_field="stage_evidence_id",
                )
                self._assert_snapshot_writer_relations(conn=conn, snapshot_id=snapshot_id)
                date_bound_roles = tuple(
                    child_role for child_role, *_rest in direct_relations
                ) + ("canonical_signals", "outcome_labels", "universe_outcomes", "gaps")
                missing_dates = int(
                    conn.execute(
                        f"""
                        SELECT COUNT(*) FROM rows
                        WHERE snapshot_id = ?
                          AND logical_role IN ({','.join('?' for _ in date_bound_roles)})
                          AND decision_date = ''
                        """,
                        (snapshot_id, *date_bound_roles),
                    ).fetchone()[0]
                )
                if missing_dates:
                    raise Phase0BAuditError(
                        REASON_RELATION_CLOSURE_INVALID,
                        "date-bound snapshot rows did not close to a decision date",
                        context={"snapshot_id": snapshot_id, "invalid_count": missing_dates},
                    )
        except Phase0BAuditError:
            self._poisoned = True
            raise
        except sqlite3.DatabaseError as error:
            self._poisoned = True
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "spool relation closure query failed",
                context={"snapshot_id": snapshot_id},
            ) from error

    @staticmethod
    def _assert_snapshot_writer_relations(
        *,
        conn: sqlite3.Connection,
        snapshot_id: str,
    ) -> None:
        checks = (
            (
                "canonical signal does not have exactly one selected observation",
                """
                SELECT COUNT(*) FROM rows signal
                WHERE signal.snapshot_id = ? AND signal.logical_role = 'canonical_signals'
                  AND (
                      SELECT COUNT(*) FROM rows selected
                      WHERE selected.snapshot_id = signal.snapshot_id
                        AND selected.logical_role = 'selected_observations'
                        AND json_extract(selected.payload_json, '$.canonical_signal_id') =
                            json_extract(signal.payload_json, '$.canonical_signal_id')
                  ) <> 1
                """,
            ),
            (
                "selected observation payload differs from its terminal version",
                """
                SELECT COUNT(*)
                FROM rows selected
                JOIN rows observation
                  ON observation.snapshot_id = selected.snapshot_id
                 AND observation.logical_role = 'observation_versions'
                 AND json_extract(observation.payload_json, '$.observation_version_id') =
                     json_extract(selected.payload_json, '$.terminal_observation_version_id')
                WHERE selected.snapshot_id = ?
                  AND selected.logical_role = 'selected_observations'
                  AND (
                      json_extract(observation.payload_json, '$.canonical_signal_id') <>
                          json_extract(selected.payload_json, '$.canonical_signal_id')
                      OR json_extract(observation.payload_json, '$.observation_content_hash') <>
                          json_extract(selected.payload_json, '$.terminal_observation_content_hash')
                      OR json_extract(observation.payload_json, '$.observation_revision_no') <>
                          json_extract(selected.payload_json, '$.terminal_revision_no')
                      OR json_extract(observation.payload_json, '$.observation_status') <> 'COMPLETE'
                  )
                """,
            ),
            (
                "selected observation does not have any lineage row",
                """
                SELECT COUNT(*) FROM rows selected
                WHERE selected.snapshot_id = ? AND selected.logical_role = 'selected_observations'
                  AND (
                      SELECT COUNT(*) FROM rows lineage
                      WHERE lineage.snapshot_id = selected.snapshot_id
                        AND lineage.logical_role = 'lineage'
                        AND json_extract(lineage.payload_json, '$.observation_version_id') =
                            json_extract(selected.payload_json, '$.terminal_observation_version_id')
                  ) < 1
                """,
            ),
            (
                "selected label payload differs from its terminal outcome",
                """
                SELECT COUNT(*)
                FROM rows selected
                JOIN rows outcome
                  ON outcome.snapshot_id = selected.snapshot_id
                 AND outcome.logical_role = 'outcome_labels'
                 AND json_extract(outcome.payload_json, '$.label_version_id') =
                     json_extract(selected.payload_json, '$.terminal_label_version_id')
                WHERE selected.snapshot_id = ?
                  AND selected.logical_role = 'selected_labels'
                  AND (
                      json_extract(selected.payload_json, '$.selection_status') <> 'SELECTED'
                      OR json_extract(outcome.payload_json, '$.label_key_hash') <>
                          json_extract(selected.payload_json, '$.label_key_hash')
                      OR json_extract(outcome.payload_json, '$.label_content_hash') <>
                          json_extract(selected.payload_json, '$.terminal_label_content_hash')
                      OR json_extract(outcome.payload_json, '$.label_revision_no') <>
                          json_extract(selected.payload_json, '$.terminal_label_revision_no')
                      OR json_extract(outcome.payload_json, '$.maturity_status') <>
                          json_extract(selected.payload_json, '$.terminal_maturity_status')
                      OR json_extract(outcome.payload_json, '$.outcome_event_status') <>
                          json_extract(selected.payload_json, '$.terminal_outcome_event_status')
                      OR json_extract(outcome.payload_json, '$.owner_type') <> 'CANDIDATE'
                  )
                """,
            ),
            (
                "outcome source evidence does not cover every label exactly once",
                """
                SELECT COUNT(*) FROM rows outcome
                WHERE outcome.snapshot_id = ? AND outcome.logical_role = 'outcome_labels'
                  AND (
                      SELECT COUNT(*) FROM rows evidence
                      WHERE evidence.snapshot_id = outcome.snapshot_id
                        AND evidence.logical_role = 'outcome_source_evidence'
                        AND json_extract(evidence.payload_json, '$.label_version_id') =
                            json_extract(outcome.payload_json, '$.label_version_id')
                  ) <> 1
                """,
            ),
            (
                "outcome source evidence contains a label outside outcome authority",
                """
                SELECT COUNT(*) FROM rows evidence
                WHERE evidence.snapshot_id = ?
                  AND evidence.logical_role = 'outcome_source_evidence'
                  AND (
                      SELECT COUNT(*) FROM rows outcome
                      WHERE outcome.snapshot_id = evidence.snapshot_id
                        AND outcome.logical_role = 'outcome_labels'
                        AND json_extract(outcome.payload_json, '$.label_version_id') =
                            json_extract(evidence.payload_json, '$.label_version_id')
                  ) <> 1
                """,
            ),
            (
                "outcome source evidence descriptor differs from label",
                """
                SELECT COUNT(*)
                FROM rows outcome
                JOIN rows evidence
                  ON evidence.snapshot_id = outcome.snapshot_id
                 AND evidence.logical_role = 'outcome_source_evidence'
                 AND json_extract(evidence.payload_json, '$.label_version_id') =
                     json_extract(outcome.payload_json, '$.label_version_id')
                WHERE outcome.snapshot_id = ? AND outcome.logical_role = 'outcome_labels'
                  AND (
                      json_extract(evidence.payload_json, '$.owner_type') <>
                          json_extract(outcome.payload_json, '$.owner_type')
                      OR json_extract(evidence.payload_json, '$.label_key_hash') <>
                          json_extract(outcome.payload_json, '$.label_key_hash')
                      OR json_extract(evidence.payload_json, '$.canonical_signal_id') IS NOT
                          json_extract(outcome.payload_json, '$.canonical_signal_id')
                      OR json_extract(evidence.payload_json, '$.symbol') <>
                          json_extract(outcome.payload_json, '$.symbol')
                      OR json_extract(evidence.payload_json, '$.horizon_trading_days') <>
                          json_extract(outcome.payload_json, '$.horizon_trading_days')
                      OR json_extract(evidence.payload_json, '$.projection') <>
                          json_extract(outcome.payload_json, '$.projection')
                      OR json_extract(evidence.payload_json, '$.calculation_evidence_sha256') <>
                          json_extract(outcome.payload_json, '$.calculation_evidence_sha256')
                      OR json_extract(evidence.payload_json, '$.calculation_evidence_size_bytes') <>
                          json_extract(outcome.payload_json, '$.calculation_evidence_size_bytes')
                      OR json_extract(evidence.payload_json, '$.calculation_evidence_store_backend_hash') <>
                          json_extract(outcome.payload_json, '$.calculation_evidence_store_backend_hash')
                  )
                """,
            ),
        )
        for message, query in checks:
            invalid_count = int(conn.execute(query, (snapshot_id,)).fetchone()[0])
            if invalid_count:
                raise Phase0BAuditError(
                    REASON_RELATION_CLOSURE_INVALID,
                    message,
                    context={"snapshot_id": snapshot_id, "invalid_count": invalid_count},
                )
        universe_difference = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT payload_json FROM (
                        SELECT payload_json FROM rows
                        WHERE snapshot_id = ? AND logical_role = 'outcome_labels'
                          AND json_extract(payload_json, '$.owner_type') = 'UNIVERSE'
                        EXCEPT
                        SELECT payload_json FROM rows
                        WHERE snapshot_id = ? AND logical_role = 'universe_outcomes'
                    ) missing_universe
                    UNION ALL
                    SELECT payload_json FROM (
                        SELECT payload_json FROM rows
                        WHERE snapshot_id = ? AND logical_role = 'universe_outcomes'
                        EXCEPT
                        SELECT payload_json FROM rows
                        WHERE snapshot_id = ? AND logical_role = 'outcome_labels'
                          AND json_extract(payload_json, '$.owner_type') = 'UNIVERSE'
                    ) extra_universe
                ) difference
                """,
                (snapshot_id, snapshot_id, snapshot_id, snapshot_id),
            ).fetchone()[0]
        )
        if universe_difference:
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "universe outcomes differ from outcome label authority",
                context={"snapshot_id": snapshot_id, "invalid_count": universe_difference},
            )

    @staticmethod
    def _assert_optional_parent(
        *,
        conn: sqlite3.Connection,
        snapshot_id: str,
        child_role: str,
        child_field: str,
        parent_role: str,
        parent_field: str,
    ) -> None:
        invalid_count = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM rows child
                WHERE child.snapshot_id = ? AND child.logical_role = ?
                  AND json_extract(child.payload_json, ?) IS NOT NULL
                  AND (
                      SELECT COUNT(*) FROM rows parent
                      WHERE parent.snapshot_id = child.snapshot_id
                        AND parent.logical_role = ?
                        AND json_extract(parent.payload_json, ?) =
                            json_extract(child.payload_json, ?)
                  ) <> 1
                """,
                (
                    snapshot_id,
                    child_role,
                    f"$.{child_field}",
                    parent_role,
                    f"$.{parent_field}",
                    f"$.{child_field}",
                ),
            ).fetchone()[0]
        )
        if invalid_count:
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "optional snapshot relation references an invalid frozen parent",
                context={
                    "snapshot_id": snapshot_id,
                    "child_role": child_role,
                    "parent_role": parent_role,
                    "invalid_count": invalid_count,
                },
            )

    def distinct_target_lineages(
        self,
        *,
        snapshot_id: str,
    ) -> tuple[tuple[str, str, str, str | None, str | None], ...]:
        """Return bounded package/manifest/mode plus formal/range Program identity."""

        conn = self._require_connection()
        rows = conn.execute(
            """
            SELECT DISTINCT
                   json_extract(signal.payload_json, '$.package_id'),
                   json_extract(signal.payload_json, '$.manifest_sha256'),
                   json_extract(signal.payload_json, '$.alpha_mode'),
                   json_extract(lineage.payload_json, '$.program_id'),
                   json_extract(lineage.payload_json, '$.historical_range_frozen_program_hash')
            FROM rows signal
            JOIN rows lineage
              ON lineage.snapshot_id = signal.snapshot_id
             AND lineage.logical_role = 'lineage'
             AND json_extract(lineage.payload_json, '$.canonical_signal_id') =
                 json_extract(signal.payload_json, '$.canonical_signal_id')
            WHERE signal.snapshot_id = ? AND signal.logical_role = 'canonical_signals'
            ORDER BY 1, 2, 3, 4, 5
            """,
            (snapshot_id,),
        )
        return tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]) if row[3] is not None else None,
                str(row[4]) if row[4] is not None else None,
            )
            for row in rows
        )

    def iter_rows(
        self,
        *,
        snapshot_id: str,
        logical_role: str,
        decision_date: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        conn = self._require_connection()
        sql = "SELECT payload_json FROM rows WHERE snapshot_id = ? AND logical_role = ?"
        params: list[str] = [snapshot_id, logical_role]
        if decision_date is not None:
            sql += " AND decision_date = ?"
            params.append(decision_date)
        sql += " ORDER BY identity_key"
        for (payload,) in conn.execute(sql, params):
            yield dict(json.loads(str(payload)))

    def iter_rows_where(
        self,
        *,
        snapshot_id: str,
        logical_role: str,
        field_values: Mapping[str, str | int],
        decision_date: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        conn = self._require_connection()
        sql = "SELECT payload_json FROM rows WHERE snapshot_id = ? AND logical_role = ?"
        params: list[str | int] = [snapshot_id, logical_role]
        for field_name, value in sorted(field_values.items()):
            if not field_name or any(
                char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
                for char in field_name
            ):
                raise ValueError("spool JSON field filter must be a safe identifier")
            sql += " AND json_extract(payload_json, ?) = ?"
            params.extend((f"$.{field_name}", value))
        if decision_date is not None:
            sql += " AND decision_date = ?"
            params.append(decision_date)
        sql += " ORDER BY identity_key"
        for (payload,) in conn.execute(sql, params):
            yield dict(json.loads(str(payload)))

    def iter_target_signals(
        self,
        *,
        snapshot_id: str,
        package_id: str,
        manifest_sha256: str,
        alpha_mode: str,
        program_id: str | None,
        range_program_hash: str | None,
        decision_date: str,
    ) -> Iterator[dict[str, Any]]:
        if (program_id is None) == (range_program_hash is None):
            raise ValueError("target signal query requires exactly one Program identity form")
        conn = self._require_connection()
        program_field = (
            "$.program_id" if program_id is not None else "$.historical_range_frozen_program_hash"
        )
        program_value = program_id if program_id is not None else range_program_hash
        query = """
            SELECT DISTINCT signal.payload_json
            FROM rows signal
            JOIN rows lineage
              ON lineage.snapshot_id = signal.snapshot_id
             AND lineage.logical_role = 'lineage'
             AND json_extract(lineage.payload_json, '$.canonical_signal_id') =
                 json_extract(signal.payload_json, '$.canonical_signal_id')
            WHERE signal.snapshot_id = ?
              AND signal.logical_role = 'canonical_signals'
              AND signal.decision_date = ?
              AND json_extract(signal.payload_json, '$.package_id') = ?
              AND json_extract(signal.payload_json, '$.manifest_sha256') = ?
              AND json_extract(signal.payload_json, '$.alpha_mode') = ?
              AND json_extract(lineage.payload_json, ?) = ?
            ORDER BY signal.identity_key
        """
        for (payload,) in conn.execute(
            query,
            (
                snapshot_id,
                decision_date,
                package_id,
                manifest_sha256,
                alpha_mode,
                program_field,
                program_value,
            ),
        ):
            yield dict(json.loads(str(payload)))

    def target_decision_dates(
        self,
        *,
        snapshot_id: str,
        package_id: str,
        manifest_sha256: str,
        alpha_mode: str,
        program_id: str | None,
        range_program_hash: str | None,
    ) -> tuple[str, ...]:
        if (program_id is None) == (range_program_hash is None):
            raise ValueError("target date query requires exactly one Program identity form")
        conn = self._require_connection()
        program_field = (
            "$.program_id" if program_id is not None else "$.historical_range_frozen_program_hash"
        )
        program_value = program_id if program_id is not None else range_program_hash
        rows = conn.execute(
            """
            SELECT DISTINCT signal.decision_date
            FROM rows signal
            JOIN rows lineage
              ON lineage.snapshot_id = signal.snapshot_id
             AND lineage.logical_role = 'lineage'
             AND json_extract(lineage.payload_json, '$.canonical_signal_id') =
                 json_extract(signal.payload_json, '$.canonical_signal_id')
            WHERE signal.snapshot_id = ?
              AND signal.logical_role = 'canonical_signals'
              AND json_extract(signal.payload_json, '$.package_id') = ?
              AND json_extract(signal.payload_json, '$.manifest_sha256') = ?
              AND json_extract(signal.payload_json, '$.alpha_mode') = ?
              AND json_extract(lineage.payload_json, ?) = ?
              AND signal.decision_date <> ''
            ORDER BY signal.decision_date
            """,
            (
                snapshot_id,
                package_id,
                manifest_sha256,
                alpha_mode,
                program_field,
                program_value,
            ),
        )
        return tuple(str(row[0]) for row in rows)

    def role_count(self, *, snapshot_id: str, logical_role: str) -> int:
        conn = self._require_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM rows WHERE snapshot_id = ? AND logical_role = ?",
            (snapshot_id, logical_role),
        ).fetchone()
        if row is None:
            raise RuntimeError("spool count query returned no result")
        return int(row[0])

    def _close_connection(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def close(self) -> None:
        if not self._closed:
            self._close_connection()
            self._closed = True

    def _remove_owned_files_and_directories(self) -> None:
        operation_root = self._operation_root.resolve()
        for filename in self._SQLITE_FILENAMES:
            candidate = (operation_root / filename).resolve()
            if operation_root not in candidate.parents:
                raise RuntimeError("spool cleanup path escapes operation root")
            if candidate.exists():
                candidate.unlink()
        if self._operation_root.exists():
            self._operation_root.rmdir()
        if self._temp_root.exists() and not any(self._temp_root.iterdir()):
            self._temp_root.rmdir()

    def cleanup(self) -> None:
        self.close()
        self._remove_owned_files_and_directories()

    def __enter__(self) -> "Phase0BBoundedSpool":
        return self

    def __exit__(self, *_args: object) -> None:
        self.cleanup()
