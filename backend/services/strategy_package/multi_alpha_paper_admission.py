"""Admission ledger for MULTI_ALPHA Paper v2 runtime validation."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.paper_trading_v2.models import BrokerBackendId

ConnFactory = Callable[[], Iterator[Any]]


def canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def admission_id_from_payload(payload: Any) -> str:
    return f"mapa_{canonical_json_sha256(payload)[:24]}"


class MultiAlphaPaperAdmissionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    admission_id: str
    package_id: str
    manifest_sha256: str
    broker_backend: BrokerBackendId
    runtime_variant: str
    eligible: bool = True
    dry_run_run_id: str
    artifact_shas: dict[str, Any] = Field(default_factory=dict)
    evidence_json: dict[str, Any] = Field(default_factory=dict)
    validated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    validated_by: str = "aistock_api"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class MultiAlphaPaperAdmissionRepository:
    """PostgreSQL repository for manifest-external MULTI_ALPHA paper admissions."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def get_eligible(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        broker_backend: BrokerBackendId,
        runtime_variant: str,
    ) -> MultiAlphaPaperAdmissionRecord | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT admission_id, package_id, manifest_sha256, broker_backend,
                           runtime_variant, eligible, dry_run_run_id, artifact_shas,
                           evidence_json, validated_at, validated_by, created_at
                    FROM strategy_pkg.multi_alpha_paper_admission
                    WHERE package_id = %s
                      AND manifest_sha256 = %s
                      AND broker_backend = %s
                      AND runtime_variant = %s
                      AND eligible = TRUE
                    ORDER BY validated_at DESC, admission_id ASC
                    LIMIT 1
                    """,
                    (package_id, manifest_sha256, broker_backend, runtime_variant),
                )
                row = cur.fetchone()
        if not row:
            return None
        return self._record_from_row(dict(row))

    def upsert_success(self, record: MultiAlphaPaperAdmissionRecord) -> MultiAlphaPaperAdmissionRecord:
        payload = record.model_dump(mode="json")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_paper_admission (
                        admission_id, package_id, manifest_sha256, broker_backend,
                        runtime_variant, eligible, dry_run_run_id, artifact_shas,
                        evidence_json, validated_at, validated_by, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (package_id, manifest_sha256, broker_backend, runtime_variant)
                    DO UPDATE SET
                        admission_id = EXCLUDED.admission_id,
                        eligible = EXCLUDED.eligible,
                        dry_run_run_id = EXCLUDED.dry_run_run_id,
                        artifact_shas = EXCLUDED.artifact_shas,
                        evidence_json = EXCLUDED.evidence_json,
                        validated_at = EXCLUDED.validated_at,
                        validated_by = EXCLUDED.validated_by,
                        created_at = EXCLUDED.created_at
                    RETURNING admission_id, package_id, manifest_sha256, broker_backend,
                              runtime_variant, eligible, dry_run_run_id, artifact_shas,
                              evidence_json, validated_at, validated_by, created_at
                    """,
                    (
                        record.admission_id,
                        record.package_id,
                        record.manifest_sha256,
                        record.broker_backend,
                        record.runtime_variant,
                        record.eligible,
                        record.dry_run_run_id,
                        psycopg2.extras.Json(payload["artifact_shas"]),
                        psycopg2.extras.Json(payload["evidence_json"]),
                        record.validated_at,
                        record.validated_by,
                        record.created_at,
                    ),
                )
                row = cur.fetchone()
        return self._record_from_row(dict(row))

    @staticmethod
    def _record_from_row(row: dict[str, Any]) -> MultiAlphaPaperAdmissionRecord:
        return MultiAlphaPaperAdmissionRecord(
            admission_id=str(row["admission_id"]),
            package_id=str(row["package_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            broker_backend=row["broker_backend"],
            runtime_variant=str(row["runtime_variant"]),
            eligible=bool(row["eligible"]),
            dry_run_run_id=str(row["dry_run_run_id"]),
            artifact_shas=dict(row.get("artifact_shas") or {}),
            evidence_json=dict(row.get("evidence_json") or {}),
            validated_at=row["validated_at"],
            validated_by=str(row["validated_by"]),
            created_at=row["created_at"],
        )


class InMemoryMultiAlphaPaperAdmissionRepository:
    """In-memory repository used by tests and dry-run unit wiring."""

    def __init__(self) -> None:
        self.records: dict[tuple[str, str, str, str], MultiAlphaPaperAdmissionRecord] = {}

    def get_eligible(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        broker_backend: BrokerBackendId,
        runtime_variant: str,
    ) -> MultiAlphaPaperAdmissionRecord | None:
        record = self.records.get((package_id, manifest_sha256, broker_backend, runtime_variant))
        if record is None or not record.eligible:
            return None
        return record

    def upsert_success(self, record: MultiAlphaPaperAdmissionRecord) -> MultiAlphaPaperAdmissionRecord:
        self.records[(record.package_id, record.manifest_sha256, record.broker_backend, record.runtime_variant)] = record
        return record
