"""Caller-owned PostgreSQL primitives for immutable Phase 1 observations."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

import psycopg2
import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonicalize
from backend.services.advisory_phase1.capture_foundation import CapturePlan
from backend.services.advisory_phase1.observation_capture import (
    Phase1GObservationRowBundle,
    Phase1GObservationSemanticDraft,
    build_observation_semantic_draft,
    materialize_observation_row_bundle,
)
from backend.services.advisory_phase1.phase1g_contract import (
    REASON_G3_CHILD_ROW_CONFLICT,
    REASON_G3_OBSERVATION_CONFLICT,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.stage_trace import (
    StageTraceEnvelope,
    TraceCaptureBinding,
)


_HEADER_COLUMNS = """
canonical_signal_id, signal_schema_version, stable_signal_semantics_hash,
canonical_signal_scope_hash, decision_as_of_trade_date, selection_as_of_trade_date,
target_trade_date, decision_cutoff_ts, package_id, manifest_sha256, alpha_mode,
selection_runtime_semantics_hash, package_effective_config_hash, calendar_version,
calendar_hash
"""

_VERSION_COLUMNS = """
observation_version_id, canonical_signal_id, observation_schema_version,
observation_revision_no, supersedes_observation_version_id,
signal_source_revision_set_id, signal_source_revision_set_hash,
phase0a_signal_context_hash, evidence_bundle_hash, stage_evidence_bundle_hash,
selection_evidence_id, selection_evidence_hash, selection_run_id,
selection_run_content_hash, selection_score_artifact_id,
selection_score_artifact_hash, runtime_profile_version_id,
runtime_profile_version_hash, hmm_snapshot_id, hmm_snapshot_hash,
hmm_snapshot_status, risk_policy_hash, universe_policy_hash,
symbol_normalization_policy_hash, valid_no_candidate, observation_status,
evidence_available_at, observation_content_hash, reason_codes,
created_by_capture_batch_id
"""

_LINEAGE_IDENTITY_COLUMNS = """
lineage_id, decision_as_of_trade_date, observation_version_id, phase0a_audit_id,
admission_scope_id, program_id, binding_version_id, lineage_source_type,
source_run_id, lineage_content_hash
"""

_LINEAGE_PAYLOAD_COLUMNS = """
decision_as_of_trade_date, lineage_id, canonical_signal_id,
phase0a_audit_manifest_hash, handoff_readiness_hash, admission_scope_hash,
audit_target_id, target_scope_hash, capability, stable_signal_semantics_hash,
canonical_signal_scope_hash, phase0a_signal_context_hash, oos_interval_id,
oos_interval_hash, evidence_scope, signal_evidence_level, effective_cutoff_date,
review_run_id, list_version_id
"""

_STAGE_COLUMNS = """
stage_evidence_id, observation_version_id, stage, capability_status, input_count,
output_count, excluded_count, observed_max_rank, source_artifact_id,
source_artifact_hash, content_hash, semantic_hash, score_direction,
tie_break_policy_id, tie_break_policy_hash, reason_codes
"""

_CANDIDATE_IDENTITY_COLUMNS = """
stage_evidence_id, symbol, decision_as_of_trade_date
"""

_CANDIDATE_PAYLOAD_COLUMNS = """
decision_as_of_trade_date, stage_evidence_id, symbol, membership_status, rank,
score_decimal, input_rank, input_score_decimal, exclusion_reason_code,
component_capability, component_evidence_schema_version, component_evidence_json,
component_evidence_hash, component_reason_codes, candidate_content_hash
"""


class PostgresObservationCaptureRepository:
    """Exact immutable observation reads and writes on a caller cursor."""

    @staticmethod
    def lock_signal_in_transaction(cur: Any, canonical_signal_id: str) -> None:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", (canonical_signal_id,)
        )

    @staticmethod
    def find_header_in_transaction(
        cur: Any, canonical_signal_id: str, *, lock: bool = True
    ) -> dict[str, Any] | None:
        lock_clause = "FOR UPDATE" if lock else ""
        cur.execute(
            f"""
            SELECT {_HEADER_COLUMNS}
            FROM app.advisory_signal_observation
            WHERE canonical_signal_id = %s
            {lock_clause}
            """,
            (canonical_signal_id,),
        )
        row = cur.fetchone()
        return _normalized_row(row) if row is not None else None

    @classmethod
    def read_header_exact_in_transaction(
        cls, cur: Any, canonical_signal_id: str, *, lock: bool = True
    ) -> dict[str, Any]:
        row = cls.find_header_in_transaction(cur, canonical_signal_id, lock=lock)
        if row is None:
            raise SourceLedgerError(
                REASON_G3_OBSERVATION_CONFLICT, "canonical signal header does not exist"
            )
        return row

    @staticmethod
    def read_revision_chain_exact_in_transaction(
        cur: Any, canonical_signal_id: str
    ) -> tuple[dict[str, Any], ...]:
        cur.execute(
            f"""
            SELECT {_VERSION_COLUMNS}
            FROM app.advisory_signal_observation_version
            WHERE canonical_signal_id = %s
            ORDER BY observation_revision_no
            FOR UPDATE
            """,
            (canonical_signal_id,),
        )
        rows = tuple(_normalized_row(row) for row in cur.fetchall())
        for index, row in enumerate(rows, start=1):
            predecessor = rows[index - 2] if index > 1 else None
            if int(row["observation_revision_no"]) != index or row[
                "supersedes_observation_version_id"
            ] != (
                predecessor["observation_version_id"]
                if predecessor is not None
                else None
            ):
                raise SourceLedgerError(
                    REASON_G3_OBSERVATION_CONFLICT,
                    "persisted observation revision chain is invalid",
                )
        return rows

    @classmethod
    def read_semantic_draft_for_revision_in_transaction(
        cls,
        cur: Any,
        *,
        observation_version_id: str,
        plan: CapturePlan,
        envelope: StageTraceEnvelope,
        binding: TraceCaptureBinding,
    ) -> Phase1GObservationSemanticDraft:
        draft = build_observation_semantic_draft(
            plan=plan, envelope=envelope, binding=binding
        )
        cur.execute(
            f"""
            SELECT {_VERSION_COLUMNS}
            FROM app.advisory_signal_observation_version
            WHERE observation_version_id = %s
            FOR KEY SHARE
            """,
            (observation_version_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_G3_OBSERVATION_CONFLICT, "observation revision does not exist"
            )
        version = _normalized_row(row)
        expected = materialize_observation_row_bundle(
            draft=draft,
            observation_revision_no=int(version["observation_revision_no"]),
            supersedes_observation_version_id=version[
                "supersedes_observation_version_id"
            ],
            created_by_capture_batch_id=str(version["created_by_capture_batch_id"]),
        )
        if (
            expected.observation_version["observation_content_hash"]
            != version["observation_content_hash"]
        ):
            raise SourceLedgerError(
                REASON_G3_OBSERVATION_CONFLICT,
                "observation revision differs from semantic draft",
            )
        persisted = cls.read_observation_bundle_exact_in_transaction(
            cur,
            observation_version_id=observation_version_id,
            semantic_observation_key=draft.semantic_observation_key,
        )
        if _bundle_payload(persisted) != _bundle_payload(expected):
            raise SourceLedgerError(
                REASON_G3_CHILD_ROW_CONFLICT,
                "observation revision children differ from semantic draft",
            )
        return draft

    @classmethod
    def append_materialized_bundle_in_transaction(
        cls,
        cur: Any,
        *,
        row_bundle: Phase1GObservationRowBundle,
    ) -> Phase1GObservationRowBundle:
        header = dict(row_bundle.canonical_signal_header)
        existing_header = cls.find_header_in_transaction(
            cur, str(header["canonical_signal_id"])
        )
        if existing_header is None:
            cls._insert_header(cur, header)
        elif canonicalize(existing_header) != canonicalize(header):
            raise SourceLedgerError(
                REASON_G3_OBSERVATION_CONFLICT,
                "canonical signal header has conflicting content",
            )
        version = dict(row_bundle.observation_version)
        cur.execute(
            f"""
            SELECT {_VERSION_COLUMNS}
            FROM app.advisory_signal_observation_version
            WHERE observation_version_id = %s
            FOR UPDATE
            """,
            (version["observation_version_id"],),
        )
        if cur.fetchone() is None:
            try:
                cls._insert_version(cur, version)
                cls._insert_lineage(
                    cur,
                    dict(row_bundle.lineage_identity),
                    dict(row_bundle.lineage_payload),
                )
                for stage in row_bundle.stage_evidence_rows:
                    cls._insert_stage(cur, dict(stage))
                for identity, payload in zip(
                    row_bundle.candidate_identity_rows,
                    row_bundle.candidate_payload_rows,
                    strict=True,
                ):
                    cls._insert_candidate(cur, dict(identity), dict(payload))
            except (psycopg2.IntegrityError, psycopg2.errors.RaiseException) as exc:
                raise SourceLedgerError(
                    REASON_G3_CHILD_ROW_CONFLICT,
                    "database rejected immutable observation rows",
                    context={
                        "constraint_name": str(
                            getattr(exc.diag, "constraint_name", "") or ""
                        )
                        or None
                    },
                ) from exc
        persisted = cls.read_observation_bundle_exact_in_transaction(
            cur,
            observation_version_id=str(version["observation_version_id"]),
            semantic_observation_key=row_bundle.semantic_observation_key,
        )
        persisted_payload = _bundle_payload(persisted)
        requested_payload = _bundle_payload(row_bundle)
        if persisted_payload != requested_payload:
            mismatched_sections = tuple(
                sorted(
                    key
                    for key in set(persisted_payload) | set(requested_payload)
                    if persisted_payload.get(key) != requested_payload.get(key)
                )
            )
            raise SourceLedgerError(
                REASON_G3_CHILD_ROW_CONFLICT,
                "persisted observation bundle differs from requested rows; mismatched sections: "
                + ", ".join(mismatched_sections),
                context={"mismatched_sections": mismatched_sections},
            )
        return persisted

    @classmethod
    def read_observation_bundle_exact_in_transaction(
        cls,
        cur: Any,
        *,
        observation_version_id: str,
        semantic_observation_key: str,
        lock: bool = True,
    ) -> Phase1GObservationRowBundle:
        lock_clause = "FOR KEY SHARE" if lock else ""
        cur.execute(
            f"SELECT {_VERSION_COLUMNS} FROM app.advisory_signal_observation_version WHERE observation_version_id = %s {lock_clause}",
            (observation_version_id,),
        )
        version_row = cur.fetchone()
        if version_row is None:
            raise SourceLedgerError(
                REASON_G3_OBSERVATION_CONFLICT, "observation version does not exist"
            )
        version = _normalized_row(version_row)
        header = cls.read_header_exact_in_transaction(
            cur, str(version["canonical_signal_id"]), lock=lock
        )
        cur.execute(
            f"""
            SELECT {_LINEAGE_IDENTITY_COLUMNS}
            FROM app.advisory_signal_observation_lineage_identity
            WHERE observation_version_id = %s
            ORDER BY lineage_id
            {lock_clause}
            """,
            (observation_version_id,),
        )
        lineage_rows = list(cur.fetchall())
        if len(lineage_rows) != 1:
            raise SourceLedgerError(
                REASON_G3_CHILD_ROW_CONFLICT,
                "observation version requires one lineage identity",
            )
        lineage_identity = _normalized_row(lineage_rows[0])
        cur.execute(
            f"""
            SELECT {_LINEAGE_PAYLOAD_COLUMNS}
            FROM app.advisory_signal_observation_lineage_payload
            WHERE lineage_id = %s AND decision_as_of_trade_date = %s
            {lock_clause}
            """,
            (
                lineage_identity["lineage_id"],
                lineage_identity["decision_as_of_trade_date"],
            ),
        )
        lineage_payload_row = cur.fetchone()
        if lineage_payload_row is None:
            raise SourceLedgerError(
                REASON_G3_CHILD_ROW_CONFLICT, "lineage payload is missing"
            )
        lineage_payload = _normalized_row(lineage_payload_row)
        cur.execute(
            f"""
            SELECT {_STAGE_COLUMNS}
            FROM app.advisory_signal_stage_evidence
            WHERE observation_version_id = %s
            ORDER BY CASE stage
                WHEN 'alpha_raw' THEN 1
                WHEN 'hmm_adjusted' THEN 2
                WHEN 'risk_policy_adjusted' THEN 3
                WHEN 'selection_effective' THEN 4
                WHEN 'advisory_model' THEN 5
                ELSE 6
            END
            {lock_clause}
            """,
            (observation_version_id,),
        )
        stages = tuple(_normalized_row(row) for row in cur.fetchall())
        if len(stages) != 5:
            raise SourceLedgerError(
                REASON_G3_CHILD_ROW_CONFLICT,
                "observation version requires five stage rows",
            )
        stage_ids = tuple(str(row["stage_evidence_id"]) for row in stages)
        cur.execute(
            f"""
            SELECT {_CANDIDATE_IDENTITY_COLUMNS}
            FROM app.advisory_signal_stage_candidate_identity
            WHERE stage_evidence_id = ANY(%s)
            ORDER BY stage_evidence_id, symbol
            {lock_clause}
            """,
            (list(stage_ids),),
        )
        identities = tuple(_normalized_row(row) for row in cur.fetchall())
        payloads = []
        for identity in identities:
            cur.execute(
                f"""
                SELECT {_CANDIDATE_PAYLOAD_COLUMNS}
                FROM app.advisory_signal_stage_candidate_payload
                WHERE decision_as_of_trade_date = %s AND stage_evidence_id = %s AND symbol = %s
                {lock_clause}
                """,
                (
                    identity["decision_as_of_trade_date"],
                    identity["stage_evidence_id"],
                    identity["symbol"],
                ),
            )
            payload = cur.fetchone()
            if payload is None:
                raise SourceLedgerError(
                    REASON_G3_CHILD_ROW_CONFLICT, "candidate payload is missing"
                )
            payloads.append(_normalized_row(payload))
        return Phase1GObservationRowBundle(
            semantic_observation_key=semantic_observation_key,
            canonical_signal_header=header,
            observation_version=version,
            lineage_identity=lineage_identity,
            lineage_payload=lineage_payload,
            stage_evidence_rows=stages,
            candidate_identity_rows=identities,
            candidate_payload_rows=tuple(payloads),
            bundle_row_count=4 + len(stages) + 2 * len(identities),
        )

    @classmethod
    def read_observation_bundle_exact_readonly(
        cls,
        cur: Any,
        *,
        observation_version_id: str,
        semantic_observation_key: str,
    ) -> Phase1GObservationRowBundle:
        return cls.read_observation_bundle_exact_in_transaction(
            cur,
            observation_version_id=observation_version_id,
            semantic_observation_key=semantic_observation_key,
            lock=False,
        )

    @staticmethod
    def _insert_header(cur: Any, row: Mapping[str, Any]) -> None:
        cur.execute(
            f"""
            INSERT INTO app.advisory_signal_observation ({_HEADER_COLUMNS})
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING {_HEADER_COLUMNS}
            """,
            tuple(row[column.strip()] for column in _column_names(_HEADER_COLUMNS)),
        )
        if canonicalize(_normalized_row(cur.fetchone())) != canonicalize(dict(row)):
            raise SourceLedgerError(
                REASON_G3_OBSERVATION_CONFLICT,
                "canonical signal header readback failed",
            )

    @staticmethod
    def _insert_version(cur: Any, row: Mapping[str, Any]) -> None:
        columns = _column_names(_VERSION_COLUMNS)
        cur.execute(
            f"INSERT INTO app.advisory_signal_observation_version ({_VERSION_COLUMNS}) VALUES ({_placeholders(columns)}) RETURNING {_VERSION_COLUMNS}",
            _params(row, columns, json_columns={"reason_codes"}),
        )
        _assert_row_equal(
            cur.fetchone(), row, reason="observation version readback failed"
        )

    @staticmethod
    def _insert_lineage(
        cur: Any, identity: Mapping[str, Any], payload: Mapping[str, Any]
    ) -> None:
        identity_columns = _column_names(_LINEAGE_IDENTITY_COLUMNS)
        cur.execute(
            f"INSERT INTO app.advisory_signal_observation_lineage_identity ({_LINEAGE_IDENTITY_COLUMNS}) VALUES ({_placeholders(identity_columns)}) RETURNING {_LINEAGE_IDENTITY_COLUMNS}",
            _params(identity, identity_columns),
        )
        _assert_row_equal(
            cur.fetchone(), identity, reason="lineage identity readback failed"
        )
        payload_columns = _column_names(_LINEAGE_PAYLOAD_COLUMNS)
        cur.execute(
            f"INSERT INTO app.advisory_signal_observation_lineage_payload ({_LINEAGE_PAYLOAD_COLUMNS}) VALUES ({_placeholders(payload_columns)}) RETURNING {_LINEAGE_PAYLOAD_COLUMNS}",
            _params(payload, payload_columns),
        )
        _assert_row_equal(
            cur.fetchone(), payload, reason="lineage payload readback failed"
        )

    @staticmethod
    def _insert_stage(cur: Any, row: Mapping[str, Any]) -> None:
        columns = _column_names(_STAGE_COLUMNS)
        cur.execute(
            f"INSERT INTO app.advisory_signal_stage_evidence ({_STAGE_COLUMNS}) VALUES ({_placeholders(columns)}) RETURNING {_STAGE_COLUMNS}",
            _params(row, columns, json_columns={"reason_codes"}),
        )
        _assert_row_equal(cur.fetchone(), row, reason="stage evidence readback failed")

    @staticmethod
    def _insert_candidate(
        cur: Any, identity: Mapping[str, Any], payload: Mapping[str, Any]
    ) -> None:
        identity_columns = _column_names(_CANDIDATE_IDENTITY_COLUMNS)
        cur.execute(
            f"INSERT INTO app.advisory_signal_stage_candidate_identity ({_CANDIDATE_IDENTITY_COLUMNS}) VALUES ({_placeholders(identity_columns)}) RETURNING {_CANDIDATE_IDENTITY_COLUMNS}",
            _params(identity, identity_columns),
        )
        _assert_row_equal(
            cur.fetchone(), identity, reason="candidate identity readback failed"
        )
        payload_columns = _column_names(_CANDIDATE_PAYLOAD_COLUMNS)
        cur.execute(
            f"INSERT INTO app.advisory_signal_stage_candidate_payload ({_CANDIDATE_PAYLOAD_COLUMNS}) VALUES ({_placeholders(payload_columns)}) RETURNING {_CANDIDATE_PAYLOAD_COLUMNS}",
            _params(
                payload,
                payload_columns,
                json_columns={"component_evidence_json", "component_reason_codes"},
            ),
        )
        _assert_row_equal(
            cur.fetchone(), payload, reason="candidate payload readback failed"
        )


def _column_names(columns: str) -> tuple[str, ...]:
    return tuple(
        value.strip()
        for value in columns.replace("\n", " ").split(",")
        if value.strip()
    )


def _placeholders(columns: tuple[str, ...]) -> str:
    return ", ".join("%s" for _column in columns)


def _params(
    row: Mapping[str, Any],
    columns: tuple[str, ...],
    *,
    json_columns: set[str] | None = None,
) -> tuple[Any, ...]:
    json_columns = json_columns or set()
    return tuple(
        (
            psycopg2.extras.Json(canonicalize(row[column]))
            if column in json_columns and row[column] is not None
            else row[column]
        )
        for column in columns
    )


def _normalized_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {}
    for key, value in dict(row).items():
        if isinstance(value, Decimal):
            normalized[key] = format(value.normalize(), "f")
        else:
            normalized[key] = value
    return normalized


def _assert_row_equal(
    row: Mapping[str, Any], expected: Mapping[str, Any], *, reason: str
) -> None:
    actual_payload = canonicalize(_normalized_row(row))
    expected_payload = canonicalize(dict(expected))
    if actual_payload != expected_payload:
        mismatched_fields = tuple(
            sorted(
                key
                for key in set(actual_payload) | set(expected_payload)
                if actual_payload.get(key) != expected_payload.get(key)
            )
        )
        raise SourceLedgerError(
            REASON_G3_CHILD_ROW_CONFLICT,
            f"{reason}; mismatched fields: {', '.join(mismatched_fields)}",
            context={"mismatched_fields": mismatched_fields},
        )


def _bundle_payload(bundle: Phase1GObservationRowBundle) -> dict[str, Any]:
    return canonicalize(
        bundle.model_dump(mode="python", exclude={"bundle_content_hash"})
    )
