"""Stateful L4 verification for prospective evidence on the local DEV database.

This test is deliberately opt-in at invocation time.  It refuses every target
except the local DEV database and runs all business writes in one transaction.
The final rollback is followed by readback assertions, so it cannot leave test
packages, score artifacts, or selection evidence behind.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import os
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import psycopg2
import pytest
from dotenv import load_dotenv

from backend.services.selection_center.prospective_evidence import canonical_evidence_json_sha256
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.strategy_package.manifest import StrategyPackageManifest, freeze_manifest
from backend.services.strategy_package.selection_artifact import (
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactRepository,
)
from backend.services.trading_core.errors import InvalidStateTransitionError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest
from backend.tests.strategy_package.test_prospective_selection_evidence import _prospective_capture_fixture


_ENV_FILE = Path("F:/Dev/AIstock/.env")
_MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "backend"
    / "db"
    / "migrations"
    / "add_selection_score_artifact_v2_evidence_20260712.sql"
)


def _dev_dsn() -> dict[str, Any]:
    """Resolve only the explicitly configured local DEV target."""

    if os.getenv("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("set AISTOCK_DEV_DB_E2E=1 to authorize the DEV-DB stateful L4 gate")
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)
    values = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": os.getenv("TDX_DB_DEV_PORT"),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.skip(f"DEV DB configuration is unavailable: {', '.join(missing)}")

    try:
        port = int(str(values["port"]))
    except ValueError as exc:
        raise AssertionError("TDX_DB_DEV_PORT must be an integer") from exc
    if values["host"] != "127.0.0.1" or port != 5433 or "dev" not in str(values["dbname"]).lower():
        raise AssertionError(
            "refusing L4 stateful verification outside local DEV DB "
            f"(host={values['host']!r}, port={port}, dbname={values['dbname']!r})"
        )
    return {**values, "port": port}


def _assert_v2_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'strategy_pkg'
              AND table_name = 'selection_score_artifact'
              AND column_name = ANY(%s)
            """,
            (
                [
                    "artifact_contract_version",
                    "artifact_payload_sha256",
                    "artifact_input_context_hash",
                    "source_revision_set_hash",
                    "asset_closure_hash",
                ],
            ),
        )
        columns = {row[0] for row in cur.fetchall()}
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'strategy_pkg'
              AND tablename = 'selection_score_artifact'
              AND indexname = 'ux_strategy_pkg_selection_artifact_v2_payload'
            """
        )
        index = cur.fetchone()

    expected_columns = {
        "artifact_contract_version",
        "artifact_payload_sha256",
        "artifact_input_context_hash",
        "source_revision_set_hash",
        "asset_closure_hash",
    }
    assert columns == expected_columns, f"v2 migration columns missing: {sorted(expected_columns - columns)}"
    assert index is not None and "WHERE (artifact_payload_sha256 IS NOT NULL)" in index[0], (
        "v2 partial unique index is missing or does not preserve legacy NULL rows"
    )


def _dynamic_fixture() -> tuple[Any, StrategyPackageManifest, SelectionScoreArtifact, Any, dict[str, Any], list[Any]]:
    """Create an isolated package identity while preserving a valid v2 contract."""

    context, _manifest, artifact, trace, runtime_config, selected = _prospective_capture_fixture()
    suffix = uuid4().hex
    manifest_payload = make_manifest().model_dump(mode="python")
    manifest_payload["package_name"] = f"phase0a2c_l4_{suffix}"
    manifest_payload["source"]["source_id"] = f"phase0a2c_l4_source_{suffix}"
    manifest = freeze_manifest(StrategyPackageManifest.model_validate(manifest_payload))

    asset_closure = [
        {
            "asset_role": "strategy_package_manifest",
            "asset_id": manifest.package_id,
            "asset_ref": None,
            "sha256": manifest.manifest_sha256,
            "first_observed_at": artifact.metadata["asset_closure"][0]["first_observed_at"],
            "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
        }
    ]
    metadata = {**artifact.metadata, "asset_closure": asset_closure}
    isolated = artifact.model_copy(
        update={
            "artifact_id": f"ssa_l4_{suffix}",
            "package_id": manifest.package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "metadata": metadata,
            "asset_closure_hash": canonical_evidence_json_sha256(
                [{key: value for key, value in asset_closure[0].items() if key != "first_observed_at"}]
            ),
            "artifact_payload_sha256": None,
        }
    )
    isolated = StrategyPackageSelectionArtifactRepository._with_digest(isolated)
    context = context.model_copy(update={"selection_run_id": f"sel_l4_{suffix}"})
    return context, manifest, isolated, trace, runtime_config, selected


def _insert_package(conn: psycopg2.extensions.connection, manifest: StrategyPackageManifest) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO strategy_pkg.package (
                package_id, package_name, package_version, source_type, source_id,
                package_status, manifest_json, manifest_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                manifest.package_id,
                manifest.package_name,
                manifest.manifest_version,
                manifest.source.source_type.value,
                manifest.source.source_id,
                manifest.package_status.value,
                psycopg2.extras.Json(manifest.model_dump(mode="json")),
                manifest.manifest_sha256,
            ),
        )


def _assert_rollback_clean(
    conn: psycopg2.extensions.connection,
    *,
    package_id: str,
    artifact_id: str,
    evidence_id: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM strategy_pkg.package WHERE package_id = %s", (package_id,))
        assert cur.fetchone() is None
        cur.execute("SELECT 1 FROM strategy_pkg.selection_score_artifact WHERE artifact_id = %s", (artifact_id,))
        assert cur.fetchone() is None
        cur.execute("SELECT 1 FROM selection.daily_selection_evidence WHERE evidence_id = %s", (evidence_id,))
        assert cur.fetchone() is None


def test_phase0a2c_l4_dev_db_v2_artifact_and_dse_are_transactional() -> None:
    """Apply-time schema, immutable repository, and rollback behavior are all real."""

    assert _MIGRATION.exists(), "committed additive migration is required before L4"
    conn = psycopg2.connect(**_dev_dsn(), connect_timeout=5)
    conn.autocommit = False
    try:
        _assert_v2_schema(conn)
        context, manifest, artifact, trace, runtime_config, selected = _dynamic_fixture()
        _insert_package(conn, manifest)

        @contextmanager
        def connection_provider() -> Iterator[psycopg2.extensions.connection]:
            yield conn

        artifact_repository = StrategyPackageSelectionArtifactRepository(conn_factory=connection_provider)
        first_artifact = artifact_repository.save(artifact)
        retry_artifact = artifact_repository.save(artifact)
        readback_artifact = artifact_repository.get(
            package_id=artifact.package_id,
            manifest_sha256=artifact.manifest_sha256,
            trade_date=artifact.trade_date,
            data_source=artifact.data_source,
            runtime_config_hash=artifact.runtime_config_hash,
        )
        assert retry_artifact.artifact_id == first_artifact.artifact_id == readback_artifact.artifact_id
        assert readback_artifact.artifact_sha256 == canonical_evidence_json_sha256(readback_artifact.scores_json)
        assert readback_artifact.artifact_payload_sha256 == canonical_evidence_json_sha256(
            readback_artifact.canonical_v2_header()
        )

        evidence = ProspectiveSelectionEvidenceAssembler().assemble(
            context=context,
            manifest=manifest,
            selection_run_id=context.selection_run_id,
            artifact=readback_artifact,
            stage_trace=trace,
            runtime_config=runtime_config,
            selected=selected,
            excluded=[],
            created_by="phase0a2c_l4_dev_db",
        )
        evidence_repository = SimulationRuntimeRepository(conn_factory=connection_provider)
        first_evidence = evidence_repository.save_daily_selection_evidence(evidence)
        retry_evidence = evidence_repository.save_daily_selection_evidence(evidence)
        readback_evidence = evidence_repository.get_daily_selection_evidence(evidence.evidence_id)
        assert retry_evidence.evidence_id == first_evidence.evidence_id == readback_evidence.evidence_id
        assert readback_evidence.artifact_hash == canonical_json_sha256(readback_evidence.evidence_payload_json)
        assert readback_evidence.evidence_payload_json["schema_version"] == "daily_selection_evidence_v2"
        assert readback_evidence.evidence_payload_json["evidence_contract"]["execution_prohibited"] is True

        before_conflict = readback_artifact.model_dump(mode="json")
        conflicting = artifact.model_copy(
            update={
                "scores_json": [{"symbol": "000001.SZ", "score": 2.0, "rank": 1}],
                "artifact_payload_sha256": None,
            }
        )
        conflicting = StrategyPackageSelectionArtifactRepository._with_digest(conflicting)
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            artifact_repository.save(conflicting)
        assert exc_info.value.context["reason_code"] == "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT"
        after_conflict = artifact_repository.get(
            package_id=artifact.package_id,
            manifest_sha256=artifact.manifest_sha256,
            trade_date=date.fromisoformat(artifact.trade_date.isoformat()),
            data_source=artifact.data_source,
            runtime_config_hash=artifact.runtime_config_hash,
        ).model_dump(mode="json")
        assert after_conflict == before_conflict

        conn.rollback()
        _assert_rollback_clean(
            conn,
            package_id=manifest.package_id,
            artifact_id=artifact.artifact_id,
            evidence_id=evidence.evidence_id,
        )
    finally:
        if not conn.closed:
            conn.rollback()
            conn.close()
