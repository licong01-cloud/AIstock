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
from types import SimpleNamespace

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
from backend.services.trading_core.errors import InvalidStateTransitionError
from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn


@pytest.mark.parametrize("retire_old_handles", [False, True])
def test_terminal_replay_recovery_is_atomic_cas_locked_and_keeps_prefix_on_existing_dev(retire_old_handles):
    from backend.services.simulation_runtime.localsim_replay_recovery import (
        RECOVERY_KEY, RETIREMENT_KEY, LocalSimReplayRecoverRequestV1, LocalSimReplayRecoveryService,
    )
    from backend.services.simulation_runtime.models import (
        ExecutionPlan, ExecutionPlanIntent, SimulationDailyRun, SimulationDailyRunStatus, TradingRuleDecision,
    )
    from backend.services.simulation_signal.contracts import DailySelectionEvidence
    from backend.services.simulation_data.contracts import MinuteDataSource

    dsn = _dev_dsn()
    package_id, manifest = _package_identity(dsn)
    conn = psycopg2.connect(**dsn)
    marker = "test_int_recovery_" + uuid4().hex
    now = datetime(2026, 10, 15, tzinfo=UTC)
    days = (date(2026, 1, 20), date(2026, 1, 21))

    @contextmanager
    def factory():
        savepoint = "recovery_" + uuid4().hex
        with conn.cursor() as cur:
            cur.execute(f"SAVEPOINT {savepoint}")
        try:
            yield conn
        except Exception:
            with conn.cursor() as cur:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            raise
        finally:
            with conn.cursor() as cur:
                cur.execute(f"RELEASE SAVEPOINT {savepoint}")

    try:
        repository = LocalSimSuccessorRepository(conn_factory=factory)
        runtime = SimulationRuntimeRepository(conn_factory=factory)
        control = LocalSimControlPlaneService(repository=repository, clock=lambda: now)
        profiles = LocalSimRuntimeProfileService(
            repository=LocalSimRuntimeProfileRepository(conn_factory=factory),
            authority=_RuntimeProfileAuthority(), clock=lambda: now,
        )
        profile = profiles.create_profile(package_id=package_id, manifest_sha256=manifest,
                                          profile_name=marker, created_by=marker)
        profile, version = profiles.create_version(
            profile_id=profile.profile_id, expected_profile_version=profile.version,
            config_json={"schema_version": "localsim_runtime_profile_config_request_v1",
                         "daily_strategy": {"strategy_id": "test_int_daily_strategy", "strategy_version": "v1", "top_k": 50},
                         "hmm": {"enabled": False}, "risk_policy": {"enabled": False}, "fee_policy": {}}, created_by=marker,
        )
        policy = local_sim_twap_only_policy_snapshot()
        account, scope, release, binding = control.create_account(
            account_name=marker, package_id=package_id, manifest_sha256=manifest,
            admission_receipt_id=marker, initial_capital=5_000_000, effective_from=days[0], effective_to=days[-1],
            runtime_profile_id=profile.profile_id, runtime_profile_version_id=version.profile_version_id,
            runtime_profile_sha256=version.config_sha256,
            daily_strategy_profile_version_id=version.daily_strategy_profile_version_id,
            execution_policy_version_id=policy["policy_version_id"], execution_policy_sha256=policy["policy_sha256"],
            execution_policy_json=policy["policy_json"], tail_policy_version_id="test_int_tail",
            tail_policy_sha256="d" * 64, created_by=marker,
        )
        coordinator = LocalSimReplayCoordinator(repository=repository, clock=lambda: now,
            historical_day_runner=LocalSimHistoricalDayRunner(historical_source_id="market.kline_minute_raw.v1",
                historical_source_sha256="c" * 64, run_day=lambda *_: None))
        job = coordinator.create_job(simulation_account_id=account.account_id, release_id=release.release_id,
            binding_id=binding.binding_id, start_trade_date=days[0], end_trade_date=days[-1], trading_days=days,
            historical_source_id="market.kline_minute_raw.v1", historical_source_sha256="c" * 64, created_by=marker)
        job = repository.transition_replay_job(replay_job_id=job.replay_job_id, expected_version=job.version,
            update={"status": LocalSimReplayStatus.FAILED_TERMINAL, "completed_trade_date": days[0],
                    "next_trade_date": days[1], "failure_code": "BrokerConnectivityError",
                    "failure_context": {"trade_date": days[1].isoformat()}}, updated_at=now)
        evidence_payload = {"test_int_marker": marker}
        evidence_hash = canonical_json_sha256(evidence_payload)
        evidence = runtime.save_daily_selection_evidence(DailySelectionEvidence(
            evidence_id="dse_" + evidence_hash[:16], target_trade_date=days[1], package_id=package_id,
            manifest_sha256=manifest, release_id=release.release_id, release_hash=release.release_hash,
            runtime_profile_version_id=version.profile_version_id, runtime_profile_hash=version.config_sha256,
            source_type="test_int_recovery", data_source="DB_HISTORICAL", candidate_count=0, excluded_count=0,
            artifact_hash=evidence_hash, evidence_payload_json=evidence_payload))
        decision_payload = {
            "schema_version": "trading_rule_decision_v1", "symbol": "001205.SZ", "market_board": "SZ_MAIN",
            "side": "SELL", "requested_quantity": 100, "legal_quantity": 100, "lot_rule": {},
            "price_limit_rule": {}, "tplus1_available_quantity": 100, "decision": "EMIT",
            "reason_code": marker, "source_version": "test_int_recovery_v1",
        }
        decision_hash = canonical_json_sha256(decision_payload)
        decision = TradingRuleDecision(
            **{k: v for k, v in decision_payload.items() if k != "schema_version"},
            decision_id="trd_" + decision_hash[:16], decision_hash=decision_hash,
        )
        plan_payload = {"test_int_marker": marker, "trading_rule_decisions": [{
            **decision_payload, "decision_id": decision.decision_id, "decision_hash": decision_hash,
        }]}
        intent_id = "intent_" + marker
        if retire_old_handles:
            plan_payload.update(
                strategy_id=account.account_id, portfolio_id=scope.ledger_scope_id, package_id=package_id,
                release_id=release.release_id, release_hash=release.release_hash,
                binding_id=binding.binding_id, binding_hash=binding.binding_hash, target_trade_date=days[1].isoformat(),
                intents=[{"intent_id": intent_id, "symbol": "001205.SZ", "side": "SELL", "target_quantity": 0,
                          "delta_quantity": -100, "order_quantity": 100, "current_quantity": 100,
                          "current_available_quantity": 100, "rebalance_reason": marker,
                          "trading_rule_decision_id": decision.decision_id, "schedule_window": {}, "price_policy": {}}],
            )
        plan_hash = canonical_json_sha256(plan_payload)
        intents = ([ExecutionPlanIntent(
            **plan_payload["intents"][0], plan_id="plan_" + plan_hash[:16], strategy_id=account.account_id,
            portfolio_id=scope.ledger_scope_id, package_id=package_id, release_id=release.release_id,
            release_hash=release.release_hash, binding_id=binding.binding_id, binding_hash=binding.binding_hash,
        )] if retire_old_handles else [])
        plan = runtime.save_execution_plan(ExecutionPlan(
            plan_id="plan_" + plan_hash[:16], strategy_id=account.account_id, portfolio_id=scope.ledger_scope_id,
            package_id=package_id, release_id=release.release_id, release_hash=release.release_hash,
            binding_id=binding.binding_id, binding_hash=binding.binding_hash,
            selection_evidence_id=evidence.evidence_id, selection_evidence_hash=evidence.artifact_hash,
            target_trade_date=days[1], execution_policy_version_id=policy["policy_version_id"],
            execution_policy_sha256=policy["policy_sha256"], tail_policy_version_id="test_int_tail",
            tail_policy_sha256="d" * 64, intents=intents, trading_rule_decisions=[decision],
            plan_payload_json=plan_payload, plan_hash=plan_hash))
        run_payload = {"submit_failure": {
            "type": "BrokerConnectivityError", "stage": "LOCAL_SIM_SUBMIT_FAILED", "context": {
                "source": "DB_HISTORICAL", "symbol": "001205.SZ", "trade_date": days[1].isoformat(),
                "cause": "market.stk_limit pre_close is required and cannot be derived"}}}
        if retire_old_handles:
            run_payload.update(broker_called=False, submitted_intents=0, failed_intents=1,
                broker_order_handles=[{"handle_id": "lsh_" + marker, "intent_id": intent_id,
                                       "backend_id": "local_sim", "submitted_at": "2026-10-14T00:00:00Z"}],
                submit_failure={"type": "DataUnavailableError", "stage": "LOCAL_SIM_PERSISTENCE_FAILED",
                    "context": {"source": "market.stk_limit", "symbol": "001205.SZ",
                        "trade_date": days[1].isoformat(), "reason_code": "STK_LIMIT_PRE_CLOSE_INVALID",
                        "economic_commit_staged": False, "broker_called": False,
                        "submitted_intents": 0, "failed_intents": 1}})
        run = runtime.save_simulation_daily_run(SimulationDailyRun(
            run_id=marker, trade_date=days[1], strategy_id=account.account_id, broker_backend="local_sim",
            package_id=package_id, manifest_sha256=manifest, release_id=release.release_id, release_hash=release.release_hash,
            binding_id=binding.binding_id, binding_hash=binding.binding_hash, selection_evidence_id=evidence.evidence_id,
            selection_artifact_hash=evidence.artifact_hash, execution_plan_id=plan.plan_id, execution_plan_hash=plan.plan_hash,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE, run_payload_json=run_payload))
        initial_payload = deepcopy(runtime.get_simulation_daily_run(run.run_id).run_payload_json)
        source = SimpleNamespace(historical_source_id=job.historical_source_id,
            historical_source_sha256=job.historical_source_sha256, trading_days=days, current_trading_date=now.date())
        def recovery(repo):
            return LocalSimReplayRecoveryService(repository=repo,
                source_authority=SimpleNamespace(resolve=lambda **_: source), clock=lambda: now,
                process_started_at=datetime(2026, 10, 14, 12, tzinfo=UTC),
                market_provider=SimpleNamespace(load_symbol_input=lambda **kw: SimpleNamespace(
                    symbol=kw["symbol"], trade_date=days[1], source=MinuteDataSource.DB_HISTORICAL,
                    market_context={"prev_close": 15.3}, minute_bars=[object()])))
        service = recovery(repository)
        check = service.inspect(job.replay_job_id)
        assert check.ready and set(check.economic_counts.values()) == {0}
        assert service.inspect(job.replay_job_id).snapshot_sha256 == check.snapshot_sha256
        snapshot = repository.get_replay_recovery_snapshot(job.replay_job_id)
        assert snapshot["plan"]["plan_payload_json"]["trading_rule_decisions"][0]["decision_hash"] == decision_hash
        request = LocalSimReplayRecoverRequestV1(expected_version=job.version, snapshot_sha256=check.snapshot_sha256)

        # Activity that appears after inspection must still refuse the old request
        # inside the locked recovery transaction. All fixture changes roll back.
        with conn.cursor() as cur:
            cur.execute("UPDATE paper_v2.simulation_daily_run SET status = 'INTRADAY_RUNNING' WHERE run_id = %s",
                        (run.run_id,))
        busy = service.inspect(job.replay_job_id)
        assert not busy.ready and busy.reason_codes == ["LOCALSIM_REPLAY_RECOVERY_ACCOUNT_RUN_IN_FLIGHT"]
        with pytest.raises(InvalidStateTransitionError, match="evidence is blocked"):
            service.recover(job.replay_job_id, request)
        assert repository.get_replay_job(job.replay_job_id) == job
        assert runtime.get_simulation_daily_run(run.run_id).run_payload_json == initial_payload
        with conn.cursor() as cur:
            cur.execute("UPDATE paper_v2.simulation_daily_run SET status = 'FAILED_RETRYABLE' WHERE run_id = %s",
                        (run.run_id,))
        assert service.inspect(job.replay_job_id).snapshot_sha256 == check.snapshot_sha256

        class BrokenReadback(LocalSimSuccessorRepository):
            def _select_replay(self, cur, replay_job_id, **kwargs):
                result = super()._select_replay(cur, replay_job_id, **kwargs)
                if result.status == LocalSimReplayStatus.FAILED_RETRYABLE:
                    raise RuntimeError("injected recovery readback failure")
                return result

        with pytest.raises(RuntimeError, match="injected recovery"):
            recovery(BrokenReadback(conn_factory=factory)).recover(job.replay_job_id, request)
        assert repository.get_replay_job(job.replay_job_id) == job
        assert RECOVERY_KEY not in runtime.get_simulation_daily_run(run.run_id).run_payload_json
        assert runtime.get_simulation_daily_run(run.run_id).run_payload_json == initial_payload
        with psycopg2.connect(**dsn) as other:
            with other.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(2403, hashtext(%s))", (run.run_id,))
                try:
                    with pytest.raises(InvalidStateTransitionError, match="writer is active"):
                        service.recover(job.replay_job_id, request)
                    with pytest.raises(InvalidStateTransitionError, match="writer is active"):
                        runtime.require_localsim_side_effect_free_rebuild(run)
                finally:
                    cur.execute("SELECT pg_advisory_unlock(2403, hashtext(%s))", (run.run_id,))
        result = service.recover(job.replay_job_id, request)
        assert result.status == LocalSimReplayStatus.FAILED_RETRYABLE and result.version == job.version + 1
        assert result.completed_trade_date == days[0] and result.next_trade_date == days[1]
        assert result.replay_hash == job.replay_hash and result.failure_context == job.failure_context
        persisted = runtime.get_simulation_daily_run(run.run_id)
        assert persisted.execution_plan_id == run.execution_plan_id
        assert persisted.run_payload_json[RECOVERY_KEY]["evidence_hash"] == evidence_hash
        if retire_old_handles:
            retirement = persisted.run_payload_json[RETIREMENT_KEY]
            assert retirement["original_payload"] == initial_payload
            assert retirement["plan_hash"] == plan_hash
            assert persisted.run_payload_json[RECOVERY_KEY]["retirement_sha256"] == retirement["receipt_sha256"]
            assert not persisted.run_payload_json.get("broker_order_handles")
        runtime.require_localsim_side_effect_free_rebuild(persisted)
        assert repository.get_account(account.account_id) == account
        with pytest.raises(InvalidStateTransitionError, match="CAS"):
            service.recover(job.replay_job_id, request)
    finally:
        conn.rollback()
        conn.close()


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
        return config.materialize(runtime_variant_materialized_config=None), {"dev_postgres_reference_readback": True}


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


def test_catchup_successor_on_existing_dev_db_is_atomic_idempotent_and_keeps_native_account() -> None:
    """Use only the existing DEV schema, and roll back every fixture and attempted mutation."""
    dsn = _dev_dsn()
    package_id, manifest_sha256 = _package_identity(dsn)
    conn = psycopg2.connect(**dsn)
    marker = uuid4().hex
    now = datetime(2026, 10, 13, 0, 30, tzinfo=UTC)

    @contextmanager
    def factory():
        savepoint = "catchup_" + uuid4().hex
        with conn.cursor() as cur:
            cur.execute(f"SAVEPOINT {savepoint}")
        try:
            yield conn
        except Exception:
            with conn.cursor() as cur:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            raise
        finally:
            with conn.cursor() as cur:
                cur.execute(f"RELEASE SAVEPOINT {savepoint}")

    class BrokenRepository(LocalSimSuccessorRepository):
        @staticmethod
        def _insert_replay(_cur, _job):
            raise RuntimeError("injected SQL catchup job insert failure")

    account_id = None
    try:
        repository = LocalSimSuccessorRepository(conn_factory=factory)
        service = LocalSimControlPlaneService(repository=repository, clock=lambda: now)
        profiles = LocalSimRuntimeProfileService(
            repository=LocalSimRuntimeProfileRepository(conn_factory=factory),
            authority=_RuntimeProfileAuthority(), clock=lambda: now,
        )
        profile = profiles.create_profile(
            package_id=package_id, manifest_sha256=manifest_sha256,
            profile_name=f"test_int_catchup_{marker}", created_by=marker,
        )
        profile, version = profiles.create_version(
            profile_id=profile.profile_id, expected_profile_version=profile.version,
            config_json={
                "schema_version": "localsim_runtime_profile_config_request_v1",
                "daily_strategy": {"strategy_id": "test_int_daily_strategy", "strategy_version": "v1", "top_k": 50},
                "hmm": {"enabled": False}, "risk_policy": {"enabled": False}, "fee_policy": {},
            }, created_by=marker,
        )
        policy = local_sim_twap_only_policy_snapshot()
        product = {
            "runtime_profile_id": profile.profile_id, "runtime_profile_version_id": version.profile_version_id,
            "runtime_profile_sha256": version.config_sha256,
            "daily_strategy_profile_version_id": version.daily_strategy_profile_version_id,
            "execution_policy_version_id": policy["policy_version_id"],
            "execution_policy_sha256": policy["policy_sha256"], "execution_policy_json": policy["policy_json"],
            "tail_policy_version_id": "tail_catchup_test", "tail_policy_sha256": "d" * 64,
        }
        days = (date(2026, 1, 5), date(2026, 1, 6))
        account, scope, release, binding = service.create_account(
            account_name=f"test_int_catchup_{marker}", package_id=package_id,
            manifest_sha256=manifest_sha256, admission_receipt_id=marker, initial_capital=5_000_000,
            effective_from=days[0], effective_to=days[-1], created_by=marker, **product,
        )
        account_id = account.account_id
        coordinator = LocalSimReplayCoordinator(
            repository=repository, clock=lambda: now,
            historical_day_runner=LocalSimHistoricalDayRunner(
                historical_source_id="market.kline_minute_raw.v1", historical_source_sha256="c" * 64,
                run_day=lambda *_args: None,
            ),
        )
        root = coordinator.create_job(
            simulation_account_id=account_id, release_id=release.release_id, binding_id=binding.binding_id,
            start_trade_date=days[0], end_trade_date=days[-1], trading_days=days,
            historical_source_id="market.kline_minute_raw.v1", historical_source_sha256="c" * 64,
            created_by=marker,
        )
        parent = coordinator.run_next_batch(
            replay_job_id=root.replay_job_id, expected_version=root.version,
            trading_days=days, current_trading_date=date(2026, 10, 13), max_days=2,
        )

        def build(end):
            _, _, new_release, new_binding = service.build_successor_release_bundle(
                account_id=account_id, base_release_id=release.release_id, base_binding_id=binding.binding_id,
                effective_from=date(2026, 1, 7), effective_to=end, created_by=marker,
                release_metadata={"localsim_replay_predecessor_id": root.replay_job_id}, **product,
            )
            new_days = (date(2026, 1, 7),) if end == date(2026, 1, 7) else (date(2026, 1, 7), end)
            child = coordinator.build_job(
                account=account, release=new_release, binding=new_binding,
                start_trade_date=new_days[0], end_trade_date=new_days[-1], trading_days=new_days,
                historical_source_id=root.historical_source_id, historical_source_sha256=root.historical_source_sha256,
                created_by=marker,
            )
            return new_release, new_binding, child

        child_release, child_binding, child = build(date(2026, 1, 7))
        kwargs = {
            "replay_job_id": root.replay_job_id, "expected_version": parent.version,
            "release": child_release, "binding": child_binding, "successor_job": child, "updated_at": now,
        }
        with pytest.raises(RuntimeError, match="injected SQL"):
            BrokenRepository(conn_factory=factory).create_replay_catchup_successor(**kwargs)
        assert repository.get_replay_job(root.replay_job_id).version == parent.version
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
                        (child_release.release_id,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                        (child_binding.binding_id,))
            assert cur.fetchone()[0] == 0
        persisted = repository.create_replay_catchup_successor(**kwargs)
        assert repository.create_replay_catchup_successor(**kwargs).replay_job_id == persisted.replay_job_id
        alternate_release, alternate_binding, alternate = build(date(2026, 1, 8))
        same = repository.create_replay_catchup_successor(
            **{**kwargs, "expected_version": repository.get_replay_job(root.replay_job_id).version,
               "release": alternate_release, "binding": alternate_binding, "successor_job": alternate},
        )
        assert same.replay_job_id == persisted.replay_job_id
        assert repository.get_account(account_id).model_dump() == account.model_dump()
        assert repository.get_ledger_scope(account_id).model_dump() == scope.model_dump()
        assert float(repository.get_account(account_id).initial_capital) == 5_000_000
        retained = repository.get_replay_job(root.replay_job_id)
        assert retained.replay_hash == root.replay_hash and retained.end_trade_date == root.end_trade_date
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM paper_v2.localsim_replay_job_v1 WHERE simulation_account_id = %s",
                        (account_id,))
            assert cur.fetchone()[0] == 2
            cur.execute("SELECT count(*) FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                        (alternate_binding.binding_id,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM paper_v2.localsim_replay_job_v1 WHERE replay_job_id = %s",
                        (alternate.replay_job_id,))
            assert cur.fetchone()[0] == 0
        assert repository.get_release(child_release.release_id).effective_to == date(2026, 1, 7)
    finally:
        conn.rollback()
        conn.close()
    with psycopg2.connect(**dsn) as check:
        with check.cursor() as cur:
            cur.execute("SELECT count(*) FROM paper_v2.simulation_account_v1 WHERE account_id = %s", (account_id,))
            assert cur.fetchone()[0] == 0


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
        with pytest.raises(InvalidStateTransitionError, match="identity is not repairable"):
            repository.delete_prepared_lineage_bundle(
                legacy_account_id=legacy_account_id,
                expected_lineage_id=lineage.lineage_id,
                expected_lineage_hash="0" * 64,
                expected_account_id=lineage_account.account_id,
                expected_account_hash=lineage_account.account_hash,
                expected_economic_facts_sha256=economic_before,
                expected_created_by=f"test_int_{marker}",
            )
        assert repository.get_lineage_by_legacy_account(legacy_account_id) is not None
        repository.delete_prepared_lineage_bundle(
            legacy_account_id=legacy_account_id,
            expected_lineage_id=lineage.lineage_id,
            expected_lineage_hash=lineage.lineage_hash,
            expected_account_id=lineage_account.account_id,
            expected_account_hash=lineage_account.account_hash,
            expected_economic_facts_sha256=economic_before,
            expected_created_by=f"test_int_{marker}",
        )
        assert repository.get_lineage_by_legacy_account(legacy_account_id) is None
        with psycopg2.connect(**dsn) as repair_read_conn:
            with repair_read_conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM paper_v2.simulation_account_v1 WHERE account_id = %s",
                    (lineage_account_id,),
                )
                assert cur.fetchone()[0] == 0
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
