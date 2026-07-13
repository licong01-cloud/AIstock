"""Explicitly authorized rollback-only DEV-DB L4 for historical research."""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import psycopg2
import psycopg2.extras
import pytest
from dotenv import load_dotenv

from backend.services.advisory_phase0a.historical_research import (
    HistoricalAdvisoryResearchRunner,
    HistoricalResearchBatchRequest,
    HistoricalResearchRunStatus,
)
from backend.services.advisory_phase0a.historical_research_postgres import (
    PersistedHistoricalSelectionEvidenceAdapter,
    PostgresHistoricalResearchProgramResolver,
    PostgresHistoricalResearchRepository,
    PostgresHistoricalResearchTradingDateResolver,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactRepository
from backend.tests.strategy_package.test_prospective_selection_evidence_dev_db import _dynamic_fixture


_ENV_FILE = Path("F:/Dev/AIstock/.env")


def _dev_dsn() -> dict[str, Any]:
    if os.getenv("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("set AISTOCK_DEV_DB_E2E=1 to authorize the DEV-DB stateful L4 gate")
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)
    dsn = {
        "host": os.getenv("TDX_DB_DEV_HOST"),
        "port": int(os.getenv("TDX_DB_DEV_PORT", "0")),
        "dbname": os.getenv("TDX_DB_DEV_NAME"),
        "user": os.getenv("TDX_DB_DEV_USER"),
        "password": os.getenv("TDX_DB_DEV_PASSWORD"),
    }
    if dsn["host"] != "127.0.0.1" or dsn["port"] != 5433 or "dev" not in str(dsn["dbname"] or "").lower():
        raise AssertionError(f"refusing historical research L4 target {dsn['host']}:{dsn['port']}/{dsn['dbname']}")
    if not dsn["user"] or not dsn["password"]:
        pytest.skip("DEV DB credentials are unavailable")
    return dsn


def _insert_package(conn: psycopg2.extensions.connection, manifest: Any) -> None:
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


def _assert_l4_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'app'
              AND table_name = ANY(%s)
            """,
            (["advisory_research_batch", "advisory_research_program_run", "advisory_research_batch_receipt"],),
        )
        tables = {row[0] for row in cur.fetchall()}
    assert tables == {"advisory_research_batch", "advisory_research_program_run", "advisory_research_batch_receipt"}


def test_historical_research_runner_l4_dev_db_is_transactional_and_rolls_back() -> None:
    conn = psycopg2.connect(**_dev_dsn(), connect_timeout=5)
    conn.autocommit = False
    try:
        _assert_l4_schema(conn)
        context, manifest, artifact, trace, runtime_config, selected = _dynamic_fixture()
        program_id = f"program_l4_{uuid4().hex}"
        binding_id = f"bind_l4_{uuid4().hex}"
        binding_payload = {
            "binding_version_id": binding_id,
            "program_id": program_id,
            "program_version": 1,
            "package_mode": "single_package",
            "package_ids": [manifest.package_id],
            "package_weights": {manifest.package_id: 1.0},
            "runtime_config_json": runtime_config,
        }
        binding_hash = canonical_json_sha256(binding_payload)
        context = context.model_copy(
            update={
                "binding_ref": {"binding_id": binding_id, "binding_hash": binding_hash},
                "effective_config_seed": {
                    **context.effective_config_seed,
                    "binding_base_source_id": binding_id,
                    "binding_base_source_hash": binding_hash,
                },
            }
        )
        _insert_package(conn, manifest)

        # Inline binding insert uses the same immutable payload that the DSE records.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.advisory_program (
                    program_id, program_name, status, target_count, package_mode,
                    package_ids, package_weights, fusion_method, package_set_hash,
                    fusion_policy_sha256, review_policy, review_policy_sha256,
                    entry_price_basis, exit_price_basis, review_schedule, version,
                    program_payload_json
                ) VALUES (
                    %s, %s, 'ENABLED', 5, 'single_package', %s, %s, NULL, %s,
                    NULL, %s, %s, 'next_open_executable', 'next_open_executable', %s, 1, %s
                )
                """,
                (
                    program_id,
                    f"L4 {program_id}",
                    psycopg2.extras.Json([manifest.package_id]),
                    psycopg2.extras.Json({manifest.package_id: 1.0}),
                    "1" * 64,
                    psycopg2.extras.Json({}),
                    "2" * 64,
                    psycopg2.extras.Json({}),
                    psycopg2.extras.Json({"program_id": program_id}),
                ),
            )
            cur.execute(
                """
                INSERT INTO app.advisory_strategy_binding_version (
                    binding_version_id, program_id, program_version, package_mode, package_ids,
                    package_weights, fusion_method, package_set_hash, fusion_policy_sha256,
                    runtime_config_json, effective_from_trade_date, effective_to_trade_date,
                    activation_status, activation_reason, source_replay_run_id, created_by,
                    binding_payload_json
                ) VALUES (
                    %s, %s, 1, 'single_package', %s, %s, NULL, %s, NULL,
                    %s, %s, NULL, 'ACTIVE', 'DEV_DB_L4', NULL, 'dev_db_l4', %s
                )
                """,
                (
                    binding_id,
                    program_id,
                    psycopg2.extras.Json([manifest.package_id]),
                    psycopg2.extras.Json({manifest.package_id: 1.0}),
                    "1" * 64,
                    psycopg2.extras.Json(runtime_config),
                    artifact.trade_date,
                    psycopg2.extras.Json(binding_payload),
                ),
            )

        @contextmanager
        def connection_provider() -> Iterator[psycopg2.extensions.connection]:
            yield conn

        artifact_repository = StrategyPackageSelectionArtifactRepository(conn_factory=connection_provider)
        stored_artifact = artifact_repository.save(artifact)
        evidence = ProspectiveSelectionEvidenceAssembler().assemble(
            context=context,
            manifest=manifest,
            selection_run_id=context.selection_run_id,
            artifact=stored_artifact,
            stage_trace=trace,
            runtime_config=runtime_config,
            selected=selected,
            excluded=[],
            created_by="historical_research_dev_db_l4",
        )
        SimulationRuntimeRepository(conn_factory=connection_provider).save_daily_selection_evidence(evidence)

        runner = HistoricalAdvisoryResearchRunner(
            repository=PostgresHistoricalResearchRepository(conn_factory=connection_provider),
            trading_date_resolver=PostgresHistoricalResearchTradingDateResolver(conn_factory=connection_provider),
            program_resolver=PostgresHistoricalResearchProgramResolver(conn_factory=connection_provider),
            evidence_adapter=PersistedHistoricalSelectionEvidenceAdapter(conn_factory=connection_provider),
        )
        request = HistoricalResearchBatchRequest(
            decision_trade_date=artifact.trade_date,
            program_ids=[program_id],
            requested_at=artifact.created_at.replace(year=artifact.created_at.year + 1),
        )
        first = runner.run(request)
        retry = runner.run(request)

        assert first.status is HistoricalResearchRunStatus.COMPLETE
        assert retry.receipt_id == first.receipt_id
        assert retry.receipt_hash == first.receipt_hash
        assert retry.program_runs[0].program_run_id == first.program_runs[0].program_run_id
        assert first.program_runs[0].evidence_id == evidence.evidence_id
        assert first.program_runs[0].research_candidates[0].symbol == selected[0].symbol

        conn.rollback()
        with conn.cursor() as cur:
            for table, column, value in (
                ("app.advisory_research_batch", "batch_id", first.batch_id),
                ("app.advisory_research_program_run", "program_run_id", first.program_runs[0].program_run_id),
                ("app.advisory_research_batch_receipt", "receipt_id", first.receipt_id),
                ("app.advisory_program", "program_id", program_id),
                ("strategy_pkg.package", "package_id", manifest.package_id),
                ("selection.daily_selection_evidence", "evidence_id", evidence.evidence_id),
            ):
                cur.execute(f"SELECT 1 FROM {table} WHERE {column} = %s", (value,))
                assert cur.fetchone() is None, f"rollback left DEV-DB business row in {table}"
    finally:
        if not conn.closed:
            conn.rollback()
            conn.close()
