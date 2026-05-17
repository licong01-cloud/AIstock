"""DEV-DB E2E gate for QE candidate packages entering Paper v2.

This test intentionally uses a single guarded DEV-DB connection and rolls back
all writes. It proves the cross-module SQL/repository/service contract without
touching production DBs, production ports, or Claude-owned runtime engines.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest

from backend.db import init_qe_archive_schema, init_trading_core_v2_schema
from backend.routers.strategy_packages import _record_payload
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.models import SelectionCandidate, SelectionMode, SelectionRun
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.selection_center.service import SelectionCenterService
from backend.services.strategy_package.candidate import (
    CandidateStrategyPackageService,
    PostgresCandidateStrategyPackageRepository,
)
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime_config import (
    build_default_runtime_config_bundle,
    strip_historical_platform_keys,
)
from backend.services.strategy_package.selection_artifact import (
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactRepository,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn
from scripts.dev_db.seed_paper_v2_qe_candidate_flow import (
    PHASES,
    SCHEMA_VERSION,
    SYNTHETIC_ARCHIVE_RUN_ID,
    SYNTHETIC_EXPERIMENT_ID,
    build_devdb_candidate_snapshot,
    build_devdb_e2e_manifest,
    write_report,
)


REPORT_PATH = Path("tmp/validation/paper_v2_qe_candidate_devdb_e2e.json")


class _NoCommitConn:
    """Proxy a psycopg2 connection while turning repository commits into no-ops."""

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self._conn = conn

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        return self._conn.cursor(*args, **kwargs)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        self._conn.rollback()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _require_devdb_e2e_enabled() -> None:
    if os.environ.get("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("set AISTOCK_DEV_DB_E2E=1 to run DEV-DB E2E gate")


@contextmanager
def _conn_factory(conn: psycopg2.extensions.connection) -> Iterator[_NoCommitConn]:
    yield _NoCommitConn(conn)


def _apply_schema(cur: Any) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS market")
    for sql in init_trading_core_v2_schema.iter_ddl():
        cur.execute(sql)
    for sql in init_qe_archive_schema.iter_ddl():
        if "%s" in sql:
            cur.execute(sql, (init_qe_archive_schema.QE_ARCHIVE_SCHEMA_VERSION,))
        else:
            cur.execute(sql)


def _assert_table_exists(cur: Any, schema: str, table: str) -> None:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (schema, table),
    )
    assert cur.fetchone(), f"{schema}.{table} is missing"


def _foreign_keys_from_candidate_to_qe(cur: Any) -> list[str]:
    cur.execute(
        """
        SELECT conname
        FROM pg_constraint c
        JOIN pg_class tbl ON tbl.oid = c.conrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'strategy_pkg'
          AND tbl.relname = 'candidate_strategy_package'
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) ILIKE '%%qe%%'
        ORDER BY conname
        """
    )
    return [row[0] for row in cur.fetchall()]


class _ToggleResolver:
    def __init__(self, snapshot: dict[str, Any]) -> None:
        self.snapshot = snapshot
        self.fail = True

    def build_from_experiment(self, experiment_id: str, **_: Any) -> Any:
        if self.fail:
            raise StrategyPackageValidationError("legacy QE source is missing manifest snapshot")
        manifest = build_devdb_e2e_manifest()
        return manifest.model_copy(update={"source": manifest.source.model_copy(update={"source_id": experiment_id})})


def test_devdb_qe_candidate_to_paper_v2_flow() -> None:
    _require_devdb_e2e_enabled()
    dsn = _dev_dsn()
    assert dsn["host"] == "127.0.0.1"
    assert int(dsn["port"]) == 5433
    assert "dev" in str(dsn["dbname"]).lower()

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "phases": {phase: {"status": "PENDING"} for phase in PHASES},
        "production_touched": False,
        "services_touched": False,
    }
    conn = psycopg2.connect(**dsn, connect_timeout=3)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), inet_server_port()")
            dbname, server_port = cur.fetchone()
            assert dbname == dsn["dbname"]
            # Docker publishes the dev DB on host port 5433 while PostgreSQL
            # still reports its in-container server port as 5432.
            assert int(dsn["port"]) == 5433
        report["phases"]["phase_0_safety_gate"] = {
            "status": "PASSED",
            "host": dsn["host"],
            "port": dsn["port"],
            "server_port": server_port,
            "dbname": dsn["dbname"],
            "guard": "loopback port 5433 dev database only",
        }

        with conn.cursor() as cur:
            _apply_schema(cur)
            for schema, table in (
                ("strategy_pkg", "candidate_strategy_package"),
                ("strategy_pkg", "package"),
                ("strategy_pkg", "selection_score_artifact"),
                ("selection", "run"),
                ("selection", "paper_portfolio_link"),
                ("paper_v2", "portfolio"),
                ("qe_archive", "run"),
                ("qe_archive", "run_config"),
                ("qe_archive", "run_artifact"),
            ):
                _assert_table_exists(cur, schema, table)
            assert _foreign_keys_from_candidate_to_qe(cur) == []
        report["phases"]["phase_1_schema_gate"] = {
            "status": "PASSED",
            "candidate_has_no_qe_source_fk": True,
            "schemas": ["strategy_pkg", "selection", "paper_v2", "qe_archive"],
        }

        factory = lambda: _conn_factory(conn)
        manifest = build_devdb_e2e_manifest()
        snapshot = build_devdb_candidate_snapshot(manifest)
        resolver = _ToggleResolver(snapshot)
        candidate_repo = PostgresCandidateStrategyPackageRepository(conn_factory=factory)
        candidate_service = CandidateStrategyPackageService(repository=candidate_repo, resolver=resolver)
        candidate = candidate_service.create_from_qe_experiment(
            experiment_id=SYNTHETIC_EXPERIMENT_ID,
            created_by="devdb_e2e",
            archive_run_id=SYNTHETIC_ARCHIVE_RUN_ID,
            **snapshot,
        )
        assert candidate.completeness["strategy_package_manifest_available"] is False
        assert candidate.eligibility["selection_supported"] is True
        assert candidate.eligibility["paper_simulation_supported"] is True
        report["phases"]["phase_2_qe_candidate_seed"] = {
            "status": "PASSED",
            "candidate_id": candidate.candidate_id,
            "legacy_snapshot_started_incomplete": True,
            "selection_supported_without_seed": True,
            "paper_simulation_supported_without_seed": True,
            "live_approval_supported_without_seed": False,
        }

        resolver.fail = False
        refreshed = candidate_service.refresh_snapshot_from_source(
            candidate_id=candidate.candidate_id,
            refreshed_by="devdb_e2e",
        )
        assert refreshed.snapshot_config["strategy_package_manifest"]["source"]["source_id"] == SYNTHETIC_EXPERIMENT_ID
        assert refreshed.completeness["strategy_package_manifest_available"] is True

        package_repo = StrategyPackageRepository(conn_factory=factory)
        package_service = StrategyPackageService(
            repository=package_repo,
            candidate_service=candidate_service,
        )
        package_record = package_service.create_from_candidate(refreshed.candidate_id)
        package_record = package_service.enable_selection(package_record.package_id)
        payload = _record_payload(package_record)
        runtime_bundle = payload["runtime_config_contract"]
        assert runtime_bundle["equivalence"]["strategy_semantics_shared"] is True
        assert runtime_bundle["qe_backtest"]["adapter"]["kind"] == "qe_qlib_bin"
        assert runtime_bundle["paper_v2"]["adapter"]["kind"] == "paper_v2_db"

        artifact_repo = StrategyPackageSelectionArtifactRepository(conn_factory=factory)
        runtime_hash = selection_artifact_runtime_hash({"selection_artifact_config": {"mode": "devdb_e2e"}})
        artifact = artifact_repo.save(
            SelectionScoreArtifact(
                package_id=package_record.package_id,
                manifest_sha256=package_record.manifest_sha256,
                trade_date=date(2026, 5, 12),
                data_source="DEV_DB_E2E",
                runtime_config_hash=runtime_hash,
                scores_json=[
                    {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "target_weight": 0.5, "target_quantity": 100},
                    {"symbol": "000002.SZ", "score": 0.8, "rank": 2, "target_weight": 0.5, "target_quantity": 100},
                ],
                score_count=2,
                universe_count=2,
                top_score_symbol="000001.SZ",
                metadata={"authority_scope": "devdb_e2e_contract"},
            )
        )

        selection_repo = SelectionCenterRepository(conn_factory=factory)
        selection_run = selection_repo.create_run(
            SelectionRun(
                run_id="sel_devdb_candidate_e2e",
                mode=SelectionMode.SINGLE_PACKAGE,
                trade_date=date(2026, 5, 12),
                data_source="DEV_DB_E2E",
                package_ids=[package_record.package_id],
                runtime_config={"runtime_config_contract": runtime_bundle},
            )
        )
        completed_run = selection_repo.complete_run(
            selection_run.model_copy(
                update={
                    "package_results": {
                        package_record.package_id: [
                            SelectionCandidate(
                                symbol="000001.SZ",
                                score=0.9,
                                rank=1,
                                target_weight=0.5,
                                target_quantity=100,
                                reference_price=10.0,
                                component_scores={"source_package_ids": [package_record.package_id]},
                            )
                        ]
                    },
                    "aggregate_results": [
                        SelectionCandidate(
                            symbol="000001.SZ",
                            score=0.9,
                            rank=1,
                            target_weight=0.5,
                            target_quantity=100,
                            reference_price=10.0,
                            component_scores={"source_package_ids": [package_record.package_id]},
                        )
                    ],
                    "manifest_sha256_by_package": {
                        package_record.package_id: package_record.manifest_sha256,
                    },
                }
            )
        )

        paper_repo = PaperTradingV2Repository(conn_factory=factory)
        paper_service = PaperTradingV2PortfolioService(
            package_repository=package_repo,
            repository=paper_repo,
        )
        selection_service = SelectionCenterService(
            package_repository=package_repo,
            repository=selection_repo,
            paper_portfolio_service=paper_service,
        )
        paper_result = selection_service.create_paper_portfolio_from_run(
            run_id=completed_run.run_id,
            portfolio_name="devdb_candidate_e2e_portfolio",
            initial_cash=1_000_000,
            start_date=date(2026, 5, 13),
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
        portfolio = paper_result["portfolio"]
        assert portfolio.package_id == package_record.package_id
        assert paper_result["link"].portfolio_id == portfolio.portfolio_id
        report["phases"]["phase_3_cross_module_flow"] = {
            "status": "PASSED",
            "candidate_id": refreshed.candidate_id,
            "package_id": package_record.package_id,
            "selection_artifact_id": artifact.artifact_id,
            "selection_run_id": completed_run.run_id,
            "portfolio_id": portfolio.portfolio_id,
        }

        deleted_candidate = candidate_service.delete_candidate(
            candidate_id=refreshed.candidate_id,
            deleted_by="devdb_e2e",
            delete_reason="devdb e2e lifecycle cleanup rehearsal",
        )
        assert package_repo.get(package_record.package_id).source_id == refreshed.candidate_id
        assert deleted_candidate.status.value == "DELETED"

        strategy_config = package_record.current_manifest().strategy_config
        universe_policy = package_record.current_manifest().universe_policy.model_dump(mode="json")
        stripped_strategy_config = strip_historical_platform_keys(dict(strategy_config))
        stripped_universe_policy = strip_historical_platform_keys(dict(universe_policy))
        assert "_precomputed_hmm_coefficients_json" not in stripped_strategy_config
        assert "hmm_snapshot_id" not in stripped_strategy_config
        assert "st_pit_snapshot_id" not in stripped_universe_policy
        assert build_default_runtime_config_bundle(package_record.current_manifest())["config_sha256"] == runtime_bundle["config_sha256"]
        assert refreshed.artifact_refs["storage_policy"] == "uri_and_hash_only"

        report["phases"]["phase_4_invariants_and_report"] = {
            "status": "PASSED",
            "candidate_survives_qe_source_delete_by_text_reference": True,
            "package_survives_candidate_soft_delete": True,
            "qe_archive_refs_are_uri_only": True,
            "hmm_platform_capability_not_locked_asset": True,
            "st_pit_platform_capability_not_locked_asset": True,
            "runtime_config_strategy_hash_equal": True,
            "adapter_runtime_hash_different": runtime_bundle["equivalence"]["adapter_specific_runtime_hashes"],
        }
        write_report(REPORT_PATH, report)
    finally:
        conn.rollback()
        conn.close()
