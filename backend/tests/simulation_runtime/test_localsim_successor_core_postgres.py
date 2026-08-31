from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, date, datetime
import os
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import psycopg2
import psycopg2.extras
import pytest

from backend.db import init_trading_core_v2_schema
from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.localsim_replay import LocalSimHistoricalDayRunner, LocalSimReplayCoordinator
from backend.services.simulation_runtime.localsim_runtime_profile import (
    LocalSimRuntimeProfileConfigRequestV1,
    LocalSimRuntimeProfileConfigV1,
)
from backend.services.simulation_runtime.localsim_runtime_profile_repository import LocalSimRuntimeProfileRepository
from backend.services.simulation_runtime.localsim_runtime_profile_service import LocalSimRuntimeProfileService
from backend.services.simulation_runtime.models import SimulationReleaseBinding, canonical_json_sha256
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.simulation_runtime.successor_models import (
    LegacyLocalSimAccountInventoryV1,
    LocalSimReplayStatus,
    SimulationAccountStatus,
    SimulationLedgerScopeKind,
    SimulationLedgerScopeV1,
)
from backend.services.simulation_runtime.successor_repository import LocalSimSuccessorRepository
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot
from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/migrations/localsim_successor_core_20260831.sql"
PREFLIGHT = ROOT / "backend/migrations/localsim_successor_core_20260831.preflight.sql"
ACCOUNT_SLOTS = ROOT / "backend/migrations/add_simulation_runtime_account_slots_20260604.sql"
CUTOVER_PREFLIGHT = ROOT / "backend/migrations/localsim_product_cutover_bridge_20260831.preflight.sql"
CUTOVER_MIGRATION = ROOT / "backend/migrations/localsim_product_cutover_bridge_20260831.sql"


pytestmark = pytest.mark.skipif(
    os.getenv("AISTOCK_DEV_DB_E2E") != "1",
    reason="set AISTOCK_DEV_DB_E2E=1 to authorize the guarded DEV-DB successor-core gate",
)


class _RuntimeProfileAuthority:
    def require_package_identity(self, *, package_id: str, manifest_sha256: str) -> None:
        assert package_id and manifest_sha256

    def validate_and_materialize_config(
        self, *, package_id: str, manifest_sha256: str, config: LocalSimRuntimeProfileConfigRequestV1
    ) -> tuple[LocalSimRuntimeProfileConfigV1, dict[str, Any]]:
        self.require_package_identity(package_id=package_id, manifest_sha256=manifest_sha256)
        return config.materialize(runtime_variant_materialized_config=None), {
            "dev_postgres_reference_readback": True
        }


@contextmanager
def _transaction_factory(dsn: dict[str, Any]) -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(**dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _apply_sql(dsn: dict[str, Any], path: Path) -> None:
    conn = psycopg2.connect(**dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(path.read_text(encoding="utf-8"))
    finally:
        conn.close()


def _apply_bootstrap_successor_body(dsn: dict[str, Any]) -> None:
    conn = psycopg2.connect(**dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(init_trading_core_v2_schema.DDL[-1])
    finally:
        conn.close()


def _package_identity(dsn: dict[str, Any]) -> tuple[str, str]:
    with psycopg2.connect(**dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT package_id, manifest_sha256
                FROM strategy_pkg.package
                WHERE manifest_sha256 IS NOT NULL
                ORDER BY created_at DESC, package_id
                LIMIT 1
                """
            )
            row = cur.fetchone()
    if row is None:
        pytest.skip("DEV strategy_pkg.package has no package identity for the successor-core FK gate")
    return str(row[0]), str(row[1])


def _economic_scope_sha256(dsn: dict[str, Any], ledger_scope_id: str) -> str:
    counts: dict[str, int] = {}
    with psycopg2.connect(**dsn) as conn:
        with conn.cursor() as cur:
            for table in ("orders", "cash_ledger", "positions", "daily_snapshots"):
                cur.execute(f"SELECT count(*) FROM paper_v2.{table} WHERE portfolio_id = %s", (ledger_scope_id,))
                counts[table] = int(cur.fetchone()[0])
    return canonical_json_sha256(counts)


def _legacy_binding(
    binding: SimulationReleaseBinding,
    *,
    legacy_account_id: str,
) -> SimulationReleaseBinding:
    config = deepcopy(binding.binding_config_json)
    config["strategy_id"] = legacy_account_id
    config["broker_account_id"] = legacy_account_id
    config.pop("account_group_id", None)
    config.pop("strategy_slot_id", None)
    config["metadata"] = {"legacy_inventory": True}
    binding_hash = canonical_json_sha256(config)
    values = binding.model_dump()
    values.update(
        {
            "binding_id": f"simbind_{binding_hash[:16]}",
            "strategy_id": legacy_account_id,
            "broker_account_id": legacy_account_id,
            "account_group_id": None,
            "strategy_slot_id": None,
            "binding_config_json": config,
            "binding_hash": binding_hash,
            "effective_to": None,
        }
    )
    return SimulationReleaseBinding.model_validate(values)


def test_successor_schema_and_repository_are_idempotent_atomic_and_readable_on_dev_postgres() -> None:
    dsn = _dev_dsn()
    _apply_sql(dsn, ACCOUNT_SLOTS)
    _apply_sql(dsn, PREFLIGHT)
    _apply_sql(dsn, MIGRATION)
    _apply_sql(dsn, MIGRATION)
    _apply_sql(dsn, CUTOVER_PREFLIGHT)
    _apply_sql(dsn, CUTOVER_MIGRATION)
    _apply_sql(dsn, CUTOVER_MIGRATION)
    _apply_bootstrap_successor_body(dsn)
    package_id, manifest_sha256 = _package_identity(dsn)
    marker = uuid4().hex
    now = datetime(2026, 8, 31, 2, 0, tzinfo=UTC)
    repository = LocalSimSuccessorRepository(conn_factory=lambda: _transaction_factory(dsn))
    service = LocalSimControlPlaneService(repository=repository, clock=lambda: now)
    profile_repository = LocalSimRuntimeProfileRepository(conn_factory=lambda: _transaction_factory(dsn))
    profile_service = LocalSimRuntimeProfileService(
        repository=profile_repository,
        authority=_RuntimeProfileAuthority(),
        clock=lambda: now,
    )
    policy = local_sim_twap_only_policy_snapshot()
    profile_id: str | None = None
    profile_version_id: str | None = None
    account_id: str | None = None
    release_id: str | None = None
    binding_id: str | None = None
    replay_job_id: str | None = None
    live_release_id: str | None = None
    live_binding_id: str | None = None
    lineage_id: str | None = None
    lineage_account_id: str | None = None
    legacy_binding_id: str | None = None
    legacy_scope_id: str | None = None

    try:
        profile = profile_service.create_profile(
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            profile_name=f"test_int_localsim_profile_{marker}",
            created_by=f"test_int_{marker}",
        )
        profile_id = profile.profile_id
        profile, profile_version = profile_service.create_version(
            profile_id=profile.profile_id,
            expected_profile_version=profile.version,
            config_json={
                "schema_version": "localsim_runtime_profile_config_request_v1",
                "daily_strategy": {
                    "strategy_id": "test_int_daily_strategy",
                    "strategy_version": "v1",
                    "top_k": 20,
                    "industry_filters": [],
                    "sector_filters": [],
                    "parameters": {},
                },
                "hmm": {
                    "enabled": False,
                    "snapshot_id": None,
                    "model_version": None,
                    "preset": None,
                    "state_mapping": {},
                },
                "risk_policy": {"max_position_weight": 0.1},
                "fee_policy": {"commission_bps": 3},
                "runtime_variant_id": None,
                "runtime_variant_hash": None,
                "notes": None,
                "metadata": {},
            },
            created_by=f"test_int_{marker}",
        )
        profile_version_id = profile_version.profile_version_id
        account, ledger_scope, release, binding = service.create_account(
            account_name=f"test_int_localsim_successor_{marker}",
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            admission_receipt_id=f"test_int_admission_{marker}",
            initial_capital=1_000_000.0,
            runtime_profile_id=profile.profile_id,
            runtime_profile_version_id=profile_version.profile_version_id,
            runtime_profile_sha256=profile_version.config_sha256,
            daily_strategy_profile_version_id=profile_version.daily_strategy_profile_version_id,
            execution_policy_version_id=policy["policy_version_id"],
            execution_policy_sha256=policy["policy_sha256"],
            execution_policy_json=policy["policy_json"],
            tail_policy_version_id=f"test_int_tail_{marker}",
            tail_policy_sha256="b" * 64,
            effective_to=date(2026, 8, 28),
            created_by=f"test_int_{marker}",
        )
        account_id, release_id, binding_id = account.account_id, release.release_id, binding.binding_id
        assert ledger_scope.ledger_scope_id == account_id
        duplicate = service.create_account(
            account_name=f"test_int_localsim_successor_{marker}",
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            admission_receipt_id=f"test_int_admission_{marker}",
            initial_capital=1_000_000.0,
            runtime_profile_id=profile.profile_id,
            runtime_profile_version_id=profile_version.profile_version_id,
            runtime_profile_sha256=profile_version.config_sha256,
            daily_strategy_profile_version_id=profile_version.daily_strategy_profile_version_id,
            execution_policy_version_id=policy["policy_version_id"],
            execution_policy_sha256=policy["policy_sha256"],
            execution_policy_json=policy["policy_json"],
            tail_policy_version_id=f"test_int_tail_{marker}",
            tail_policy_sha256="b" * 64,
            effective_to=date(2026, 8, 28),
            created_by=f"test_int_{marker}",
        )
        assert duplicate[0].account_id == account_id
        assert duplicate[1].ledger_scope_id == account_id
        assert duplicate[2].release_id == release_id
        assert duplicate[3].binding_id == binding_id

        paused = service.pause_account(account_id=account_id, expected_version=account.version)
        assert paused.version == 2

        days = [date(2026, 8, 27), date(2026, 8, 28)]
        coordinator = LocalSimReplayCoordinator(
            repository=repository,
            historical_day_runner=LocalSimHistoricalDayRunner(
                historical_source_id="market.kline_minute_raw.completed_days",
                historical_source_sha256="c" * 64,
                run_day=lambda _job, _day: None,
            ),
            clock=lambda: now,
        )
        job = coordinator.create_job(
            simulation_account_id=account_id,
            release_id=release_id,
            binding_id=binding_id,
            start_trade_date=days[0],
            end_trade_date=days[-1],
            historical_source_id="market.kline_minute_raw.completed_days",
            historical_source_sha256="c" * 64,
            trading_days=days,
            created_by=f"test_int_{marker}",
        )
        replay_job_id = job.replay_job_id
        caught_up = coordinator.run_next_batch(
            replay_job_id=job.replay_job_id,
            expected_version=job.version,
            trading_days=days,
            current_trading_date=date(2026, 8, 31),
            max_days=2,
        )
        assert caught_up.status is LocalSimReplayStatus.CAUGHT_UP
        ready = repository.transition_replay_job(
            replay_job_id=job.replay_job_id,
            expected_version=caught_up.version,
            update={"status": LocalSimReplayStatus.READY_FOR_LIVE},
            updated_at=now,
        )
        live_account, _base_binding, live_release, live_binding = service.build_successor_release_bundle(
            account_id=account_id,
            base_release_id=release_id,
            base_binding_id=binding_id,
            runtime_profile_id=profile.profile_id,
            runtime_profile_version_id=profile_version.profile_version_id,
            runtime_profile_sha256=profile_version.config_sha256,
            daily_strategy_profile_version_id=profile_version.daily_strategy_profile_version_id,
            execution_policy_version_id=policy["policy_version_id"],
            execution_policy_sha256=policy["policy_sha256"],
            execution_policy_json=policy["policy_json"],
            tail_policy_version_id=f"test_int_live_tail_{marker}",
            tail_policy_sha256="d" * 64,
            effective_from=date(2026, 8, 31),
            created_by=f"test_int_{marker}",
        )
        persisted_release, persisted_binding, live_job = repository.create_and_activate_replay_live_successor(
            replay_job_id=job.replay_job_id,
            expected_version=ready.version,
            account=live_account,
            release=live_release,
            binding=live_binding,
            activation_trade_date=date(2026, 8, 31),
            updated_at=now,
        )
        live_release_id = persisted_release.release_id
        live_binding_id = persisted_binding.binding_id
        assert live_job.status is LocalSimReplayStatus.LIVE_ACTIVE

        legacy_account_id = f"test_int_legacy_{marker}"
        legacy_binding = _legacy_binding(binding, legacy_account_id=legacy_account_id)
        SimulationRuntimeRepository(conn_factory=lambda: _transaction_factory(dsn)).save_simulation_release_binding(
            legacy_binding
        )
        legacy_binding_id = legacy_binding.binding_id
        legacy_scope_id = legacy_account_id
        scope_identity = {
            "schema_version": "simulation_ledger_scope_v1",
            "ledger_scope_id": legacy_account_id,
            "scope_kind": "LEGACY_PORTFOLIO",
            "source_identity": legacy_account_id,
            "native_account_id": None,
        }
        repository.save_ledger_scope(
            SimulationLedgerScopeV1(
                ledger_scope_id=legacy_account_id,
                ledger_scope_hash=canonical_json_sha256(scope_identity),
                scope_kind=SimulationLedgerScopeKind.LEGACY_PORTFOLIO,
                source_identity=legacy_account_id,
                created_by=f"test_int_{marker}",
                created_at=now,
            )
        )
        economic_before = _economic_scope_sha256(dsn, legacy_account_id)
        lineage_account, lineage = service.prepare_legacy_lineage(
            LegacyLocalSimAccountInventoryV1(
                legacy_account_id=legacy_account_id,
                account_name=f"test_int_retained_{marker}",
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                admission_receipt_id=f"test_int_admission_{marker}",
                initial_capital=1_000_000.0,
                release_id=release_id,
                release_hash=release.release_hash or "",
                binding_id=legacy_binding.binding_id,
                binding_hash=legacy_binding.binding_hash or "",
                ledger_scope_id=legacy_account_id,
                economic_facts_sha256=economic_before,
                current_status=SimulationAccountStatus.ACTIVE,
                runtime_owned=True,
                retained_by_user=True,
                in_flight_economic_transactions=0,
            ),
            created_by=f"test_int_{marker}",
        )
        lineage_id = lineage.lineage_id
        lineage_account_id = lineage_account.account_id
        assert _economic_scope_sha256(dsn, legacy_account_id) == economic_before

        with psycopg2.connect(**dsn) as read_conn:
            with read_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT account_id, status, version
                    FROM paper_v2.simulation_account_v1
                    WHERE account_id = %s
                    """,
                    (account_id,),
                )
                assert dict(cur.fetchone()) == {"account_id": account_id, "status": "PAUSED", "version": 2}
                cur.execute(
                    "SELECT status, completed_trade_date FROM paper_v2.localsim_replay_job_v1 WHERE replay_job_id = %s",
                    (replay_job_id,),
                )
                assert dict(cur.fetchone()) == {"status": "LIVE_ACTIVE", "completed_trade_date": days[-1]}
    finally:
        with psycopg2.connect(**dsn) as cleanup_conn:
            with cleanup_conn.cursor() as cur:
                if replay_job_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.localsim_replay_job_v1 WHERE replay_job_id = %s", (replay_job_id,)
                    )
                if lineage_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.legacy_localsim_account_lineage_v1 WHERE lineage_id = %s",
                        (lineage_id,),
                    )
                if legacy_binding_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                        (legacy_binding_id,),
                    )
                if live_binding_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                        (live_binding_id,),
                    )
                if binding_id is not None:
                    cur.execute("DELETE FROM paper_v2.simulation_release_binding WHERE binding_id = %s", (binding_id,))
                if live_release_id is not None:
                    cur.execute(
                        "DELETE FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
                        (live_release_id,),
                    )
                if release_id is not None:
                    cur.execute(
                        "DELETE FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s", (release_id,)
                    )
                if profile_version_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.localsim_runtime_profile_version_v1 WHERE profile_version_id = %s",
                        (profile_version_id,),
                    )
                if profile_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.localsim_runtime_profile_v1 WHERE profile_id = %s",
                        (profile_id,),
                    )
                if legacy_scope_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.simulation_ledger_scope_v1 WHERE ledger_scope_id = %s",
                        (legacy_scope_id,),
                    )
                if account_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.simulation_ledger_scope_v1 WHERE ledger_scope_id = %s",
                        (account_id,),
                    )
                if account_id is not None:
                    cur.execute("DELETE FROM paper_v2.simulation_account_v1 WHERE account_id = %s", (account_id,))
                if lineage_account_id is not None:
                    cur.execute(
                        "DELETE FROM paper_v2.simulation_account_v1 WHERE account_id = %s",
                        (lineage_account_id,),
                    )
            cleanup_conn.commit()

    with psycopg2.connect(**dsn) as read_conn:
        with read_conn.cursor() as cur:
            for table, identity_column, identity_value in (
                ("paper_v2.localsim_replay_job_v1", "replay_job_id", replay_job_id),
                ("paper_v2.legacy_localsim_account_lineage_v1", "lineage_id", lineage_id),
                ("paper_v2.simulation_release_binding", "binding_id", legacy_binding_id),
                ("paper_v2.simulation_release_binding", "binding_id", live_binding_id),
                ("paper_v2.simulation_release_binding", "binding_id", binding_id),
                ("strategy_pkg.strategy_runtime_release", "release_id", live_release_id),
                ("strategy_pkg.strategy_runtime_release", "release_id", release_id),
                ("paper_v2.localsim_runtime_profile_version_v1", "profile_version_id", profile_version_id),
                ("paper_v2.localsim_runtime_profile_v1", "profile_id", profile_id),
                ("paper_v2.simulation_ledger_scope_v1", "ledger_scope_id", legacy_scope_id),
                ("paper_v2.simulation_ledger_scope_v1", "ledger_scope_id", account_id),
                ("paper_v2.simulation_account_v1", "account_id", account_id),
            ):
                if identity_value is None:
                    continue
                cur.execute(f"SELECT count(*) FROM {table} WHERE {identity_column} = %s", (identity_value,))
                assert cur.fetchone()[0] == 0
