from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import os

import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV1,
    DependentBuyDecisionV1,
    DependentBuyLedgerObservationV1,
    DependentBuyReleaseDecisionV1,
    DependentBuySellDependencyV1,
    DependentBuyDependencyStatusV1,
    DependentBuyTriggerEventRefV1,
    DependentBuyTriggerTypeV1,
    ProductCommandAuthorityItemV2,
    ProductCommandAuthoritySetV2,
    ProductCommandDispositionV2,
    ProductRouteCutoverReceiptV1,
    ProductRouteOwnerKindV1,
    ProductRouteOwnerV1,
)
from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositoryCommitUnknown,
    KernelRepositoryConflict,
    KernelRepositorySchemaError,
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.kernel_product_repository import (
    K6B_CATALOG_SHA256,
    K6C0_CATALOG_SHA256_K6B,
    K6_CATALOG_SHA256_K6B,
    K6_TABLES,
    KernelProductRepositoryMixin,
    _assert_route_successor_authority_v1,
    migration_readback_sha256_v1,
    product_authority_schema_sha256_v3,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_k6_migration_postgres import (
    K6C_FORWARD,
    _apply_k2_and_k6,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    _dev_dsn,
    _fixture_schema,
    _insert_valid_k2_constraint_graph,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _conn_factory
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _commit_unknown_factory
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _forbidden_conn_factory
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _SchemaConnection
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import _SchemaCursor


NOW = datetime(2026, 7, 25, 1, 30, tzinfo=timezone.utc)


def _sha(char: str) -> str:
    return char * 64


def _dependency() -> DependentBuySellDependencyV1:
    return DependentBuySellDependencyV1.create(
        runtime_id="runtime_constraints",
        strategy_id="strategy_k6",
        sell_parent_intent_id="intent_sell",
        sell_algo_instance_id="algo_sell",
        latest_order_fact_ref=_sha("b"),
        settled_trade_fact_refs=(),
        settled_cash_ledger_refs=(),
        dependency_status=DependentBuyDependencyStatusV1.OPEN,
    )


def _coordination(
    *,
    status: DependentBuyCoordinationStatusV1 = DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
    decision_sequence: int = 0,
    last_decision_sha256: str | None = None,
    row_version: int = 1,
    leased: bool = False,
    lease_epoch: int = 1,
) -> DependentBuyCoordinationV1:
    return DependentBuyCoordinationV1.create(
        runtime_id="runtime_constraints",
        binding_id="binding_k6",
        trade_date="2026-07-25",
        strategy_id="strategy_k6",
        buy_algo_instance_id="algo_constraints",
        buy_parent_intent_id="intent_constraints",
        required_cash="800",
        release_command_payload_sha256=_sha("f"),
        ordered_sell_dependencies=(_dependency(),),
        status=status,
        decision_sequence=decision_sequence,
        last_decision_sha256=last_decision_sha256,
        released_command_id=None,
        released_outbox_id=None,
        row_version=row_version,
        lease_worker_id="worker_k6" if leased else None,
        lease_process_incarnation_id="process_k6" if leased else None,
        lease_epoch=lease_epoch if leased else 0,
        lease_expires_at_utc=NOW + timedelta(minutes=1) if leased else None,
        created_at_utc=NOW,
        updated_at_utc=NOW + timedelta(seconds=row_version - 1),
    )


def _ledger(
    *,
    runtime_id: str = "runtime_constraints",
    required_cash: str = "800",
    ledger_as_of_utc: datetime = NOW,
    trade_refs: tuple[str, ...] = (),
    cash_refs: tuple[str, ...] = (),
) -> DependentBuyLedgerObservationV1:
    return DependentBuyLedgerObservationV1.create(
        runtime_id=runtime_id,
        strategy_id="strategy_k6",
        trade_date="2026-07-25",
        virtual_account_id="account_k6",
        ledger_row_version=1,
        ledger_as_of_utc=ledger_as_of_utc,
        available_cash="100",
        required_cash=required_cash,
        ordered_settled_trade_refs=trade_refs,
        ordered_cash_ledger_refs=cash_refs,
        freshness_session_authority_sha256=_sha("e"),
    )


def _trigger(
    *, runtime_id: str = "runtime_constraints", observed_at_utc: datetime = NOW
) -> DependentBuyTriggerEventRefV1:
    return DependentBuyTriggerEventRefV1.create(
        runtime_id=runtime_id,
        event_id="event_constraints",
        event_type=DependentBuyTriggerTypeV1.ACCOUNT_REFRESHED,
        event_sequence=1,
        source_fact_type="qmt_strategy_ledger.virtual_account",
        source_fact_id="account_k6",
        source_fact_sha256=_sha("4"),
        observed_at_utc=observed_at_utc,
    )


def _zero_authority() -> ProductCommandAuthoritySetV2:
    return ProductCommandAuthoritySetV2.create(
        runtime_id="runtime_constraints",
        algo_instance_id="algo_constraints",
        event_id="event_constraints",
        delivery_id="delivery_constraints",
        transition_id="transition_constraints",
        catalog_sha256=_sha("b"),
        creation_binding_sha256=_sha("c"),
        facade_conformance_set_sha256=_sha("d"),
        execution_projection_set_sha256=_sha("a"),
        transition_receipt_sha256=_sha("a"),
        ordered_items=(),
    )


def _mixed_authority() -> ProductCommandAuthoritySetV2:
    common = {
        "runtime_id": "runtime_constraints",
        "algo_instance_id": "algo_constraints",
        "event_id": "event_constraints",
        "delivery_id": "delivery_constraints",
        "transition_id": "transition_constraints",
        "command_type": "SUBMIT_LIMIT",
        "command_payload_sha256": _sha("a"),
        "plugin_effect_sha256": _sha("b"),
        "execution_projection_set_sha256": _sha("a"),
        "oms_preflight_receipt_sha256": _sha("c"),
        "risk_decision_receipt_sha256": _sha("d"),
        "route_compatibility_receipt_sha256": _sha("e"),
        "market_data_projection_sha256": _sha("f"),
        "account_projection_sha256": _sha("1"),
        "contract_projection_sha256": _sha("2"),
    }
    materialized = ProductCommandAuthorityItemV2.create(
        **common,
        effect_ordinal=0,
        command_id="command_constraints",
        disposition=ProductCommandDispositionV2.MATERIALIZE,
        mapping_id="mapping_constraints",
        outbox_id="command_constraints",
        child_order_id="child_constraints",
    )
    rejected = ProductCommandAuthorityItemV2.create(
        **common,
        effect_ordinal=1,
        command_id="command_rejected_constraints",
        disposition=ProductCommandDispositionV2.REJECT_SYNCHRONOUS,
        reject_reason_code="MINIQMT_K6_OMS_PREFLIGHT_REJECTED",
        reject_context_sha256=_sha("3"),
    )
    return ProductCommandAuthoritySetV2.create(
        runtime_id="runtime_constraints",
        algo_instance_id="algo_constraints",
        event_id="event_constraints",
        delivery_id="delivery_constraints",
        transition_id="transition_constraints",
        catalog_sha256=_sha("b"),
        creation_binding_sha256=_sha("c"),
        facade_conformance_set_sha256=_sha("d"),
        execution_projection_set_sha256=_sha("a"),
        transition_receipt_sha256=_sha("a"),
        ordered_items=(materialized, rejected),
    )


def _route_receipt(*, epoch: int, owner: ProductRouteOwnerKindV1, previous: str | None) -> ProductRouteCutoverReceiptV1:
    return ProductRouteCutoverReceiptV1.create(
        runtime_id="runtime_constraints",
        binding_id="binding_k6",
        trade_date="2026-07-25",
        route_epoch=epoch,
        route_owner=owner,
        effective_new_instance_sequence=10 + epoch,
        legacy_active_instance_count=1 if owner is ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY else 0,
        kernel_active_instance_count=0,
        catalog_sha256=_sha("a"),
        gateway_capability_catalog_sha256=_sha("b"),
        exchange_session_authority_sha256=_sha("c"),
        migration_readback_sha256=_sha("d"),
        product_authority_schema_sha256=_sha("e"),
        previous_receipt_sha256=previous,
        created_at_utc=NOW + timedelta(seconds=epoch),
    )


@pytest.mark.parametrize(
    "field_name",
    (
        "exchange_session_authority_sha256",
        "migration_readback_sha256",
        "product_authority_schema_sha256",
    ),
)
def test_product_route_successor_rejects_non_catalog_authority_drift(field_name: str) -> None:
    """A successor may refresh catalog facts, never session or schema facts."""

    predecessor = _route_receipt(epoch=1, owner=ProductRouteOwnerKindV1.KERNEL_V2, previous=None)
    successor = {
        "catalog_sha256": predecessor.catalog_sha256,
        "gateway_capability_catalog_sha256": predecessor.gateway_capability_catalog_sha256,
        "exchange_session_authority_sha256": predecessor.exchange_session_authority_sha256,
        "migration_readback_sha256": predecessor.migration_readback_sha256,
        "product_authority_schema_sha256": predecessor.product_authority_schema_sha256,
    }
    successor[field_name] = _sha("f")

    with pytest.raises(KernelRepositoryConflict, match="MINIQMT_K6_ROUTE_AUTHORITY_DRIFT"):
        _assert_route_successor_authority_v1(predecessor=predecessor, **successor)


def test_product_route_successor_allows_only_exact_retry_or_catalog_gateway_renewal() -> None:
    predecessor = _route_receipt(epoch=1, owner=ProductRouteOwnerKindV1.KERNEL_V2, previous=None)
    exact = {
        "catalog_sha256": predecessor.catalog_sha256,
        "gateway_capability_catalog_sha256": predecessor.gateway_capability_catalog_sha256,
        "exchange_session_authority_sha256": predecessor.exchange_session_authority_sha256,
        "migration_readback_sha256": predecessor.migration_readback_sha256,
        "product_authority_schema_sha256": predecessor.product_authority_schema_sha256,
    }
    assert _assert_route_successor_authority_v1(predecessor=predecessor, **exact) is True
    assert (
        _assert_route_successor_authority_v1(
            predecessor=predecessor,
            **(exact | {"catalog_sha256": _sha("f"), "gateway_capability_catalog_sha256": _sha("0")}),
        )
        is False
    )


def _seed_schema(cur: object, schema: str) -> None:
    _apply_k2_and_k6(cur, schema)
    _insert_valid_k2_constraint_graph(cur, schema)
    sha = _sha("a")
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_algo_instance(
            algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,
            remaining_quantity,algo_code,status,kernel_contract_version,traded_quantity,plugin_id,
            plugin_version,plugin_manifest_sha256,plugin_config_json,plugin_config_sha256,
            compatibility_receipt_sha256,state_schema_version,state_json,state_sha256,
            transition_sequence,last_applied_delivery_sequence,last_closed_delivery_sequence,
            active_child_closure_status,active_child_count,row_version,kernel_carrier_json
        ) VALUES ('algo_sell','runtime_constraints','intent_sell','slot_sell','600001.SH','SELL',100,100,
                  'TWAP','ACTIVE','KERNEL_V2',0,'aistock.twap','1.0.0',%s,'{{}}'::jsonb,%s,%s,
                  'twap_state_v1','{{}}'::jsonb,%s,0,0,0,'NOT_APPLICABLE',0,1,'{{}}'::jsonb)
        """,
        (sha, sha, sha, sha),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"INSERT INTO {schema}.execution_kernel_worker_epoch(worker_id,process_role,incarnation_sequence) "
        "VALUES ('worker_k6','PRODUCT_COORDINATOR',1)"
    )
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_kernel_worker_incarnation(
            worker_id,process_role,incarnation_sequence,source_revision,process_incarnation_id,
            started_at_utc,startup_transaction_commit_identity,receipt_sha256,startup_receipt_json
        ) VALUES ('worker_k6','PRODUCT_COORDINATOR',1,'test','process_k6',now(),'tx_worker',%s,'{{}}'::jsonb)
        """,
        (sha,),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO {schema}.execution_algo_command_outbox(
            command_id,transition_id,ordinal,runtime_id,algo_instance_id,parent_intent_id,
            mapping_id,command_type,local_vt_orderid,payload_json,payload_sha256,status,
            attempt_count,lease_epoch,deterministic_client_order_ref,row_version,
            created_at_utc,updated_at_utc,carrier_json,outbox_row_sha256
        ) VALUES (
            'command_constraints','transition_constraints',0,'runtime_constraints','algo_constraints',
            'intent_constraints','mapping_constraints','SUBMIT_LIMIT','local_constraints','{{}}'::jsonb,
            %s,'PENDING',0,0,'client_constraints',1,now(),now(),'{{}}'::jsonb,%s
        )
        """,
        (sha, sha),
    )


class _ProductAuthoritySchemaCursor(_SchemaCursor):
    def execute(self, query: object, parameters: object = None) -> object:
        rewritten = query
        if isinstance(rewritten, str):
            rewritten = (
                rewritten.replace("strategy_pkg.strategy_runtime_release", f"{self._schema}.strategy_runtime_release")
                .replace("paper_v2.", f"{self._schema}.")
                .replace("qmt_strategy.", f"{self._schema}.")
            )
        return self._cursor.execute(rewritten, parameters)  # type: ignore[attr-defined]


class _ProductAuthoritySchemaConnection(_SchemaConnection):
    def cursor(self, *args: object, **kwargs: object) -> _ProductAuthoritySchemaCursor:
        return _ProductAuthoritySchemaCursor(  # type: ignore[attr-defined]
            self._connection.cursor(*args, **kwargs), self._schema
        )


def _product_authority_conn_factory(schema: str):
    @contextmanager
    def factory(*, autocommit: bool = False, manage_transaction: bool = False):
        connection = psycopg2.connect(**_dev_dsn())
        connection.autocommit = autocommit
        proxy = _ProductAuthoritySchemaConnection(connection, schema)
        try:
            yield proxy
            if manage_transaction and not autocommit:
                connection.commit()
        except Exception:
            if not autocommit:
                connection.rollback()
            raise
        finally:
            connection.close()

    return factory


def _seed_k6d_runtime_authority(cur: object, schema: str) -> tuple[str, str, str]:
    _apply_k2_and_k6(cur, schema)
    release_hash = _sha("b")
    manifest_sha256 = _sha("c")
    binding_hash = _sha("d")
    cur.execute(  # type: ignore[attr-defined]
        f"""
        ALTER TABLE {schema}.execution_runtime
            ADD COLUMN account_group_id TEXT,
            ADD COLUMN mode TEXT,
            ADD COLUMN event_loop_state TEXT,
            ADD COLUMN gateway_state TEXT,
            ADD COLUMN oms_state TEXT,
            ADD COLUMN runtime_config_hash TEXT,
            ADD COLUMN metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb;
        CREATE TABLE {schema}.strategy_runtime_release(
            release_id TEXT PRIMARY KEY, release_hash TEXT NOT NULL, package_id TEXT NOT NULL,
            manifest_sha256 TEXT NOT NULL
        );
        CREATE TABLE {schema}.simulation_release_binding(
            binding_id TEXT PRIMARY KEY, release_id TEXT NOT NULL, release_hash TEXT NOT NULL,
            package_id TEXT NOT NULL, manifest_sha256 TEXT NOT NULL, broker_backend TEXT NOT NULL,
            broker_account_id TEXT NOT NULL, account_group_id TEXT NOT NULL, effective_from DATE,
            effective_to DATE, binding_hash TEXT NOT NULL
        );
        CREATE TABLE {schema}.execution_plan(
            plan_id TEXT PRIMARY KEY, plan_hash TEXT NOT NULL, binding_id TEXT NOT NULL,
            binding_hash TEXT NOT NULL, release_id TEXT NOT NULL, release_hash TEXT NOT NULL,
            package_id TEXT NOT NULL, target_trade_date DATE NOT NULL,
            execution_policy_sha256 TEXT NOT NULL, tail_policy_sha256 TEXT NOT NULL
        );
        CREATE TABLE {schema}.execution_parent_benchmark(
            parent_intent_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL,
            execution_plan_id TEXT NOT NULL, execution_plan_hash TEXT NOT NULL,
            binding_id TEXT NOT NULL, binding_hash TEXT NOT NULL, release_id TEXT NOT NULL,
            package_id TEXT NOT NULL, trade_date DATE NOT NULL
        );
        """
    )
    cur.execute(  # type: ignore[attr-defined]
        f"INSERT INTO {schema}.strategy_runtime_release VALUES (%s,%s,%s,%s)",
        ("release_k6d", release_hash, "package_k6d", manifest_sha256),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"INSERT INTO {schema}.simulation_release_binding VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            "binding_k6d",
            "release_k6d",
            release_hash,
            "package_k6d",
            manifest_sha256,
            "minqmt_sim",
            "broker_k6d",
            "account_group_k6d",
            date(2026, 8, 6),
            None,
            binding_hash,
        ),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"INSERT INTO {schema}.execution_plan VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            "plan_k6d",
            _sha("e"),
            "binding_k6d",
            binding_hash,
            "release_k6d",
            release_hash,
            "package_k6d",
            date(2026, 8, 6),
            _sha("f"),
            _sha("0"),
        ),
    )
    cur.execute(  # type: ignore[attr-defined]
        f"INSERT INTO {schema}.execution_parent_benchmark VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            "intent_k6d",
            "runtime_k6d",
            "plan_k6d",
            _sha("e"),
            "binding_k6d",
            binding_hash,
            "release_k6d",
            "package_k6d",
            date(2026, 8, 6),
        ),
    )
    return "runtime_k6d", "binding_k6d", "plan_k6d"


def test_k6d_runtime_uses_declared_parent_schema_and_release_join_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6d_authority_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            runtime_id, binding_id, plan_id = _seed_k6d_runtime_authority(cur, schema)
        repository = PostgresMiniQMTKernelRepository(conn_factory=_product_authority_conn_factory(schema))

        readback = repository.ensure_product_runtime_v1(
            runtime_id=runtime_id,
            binding_id=binding_id,
            execution_plan_id=plan_id,
        )
        assert readback["runtime_id"] == runtime_id
        assert readback["metadata"]["route"] == "KERNEL_V2"

        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema=%s "
                "AND table_name='execution_parent_benchmark' ORDER BY ordinal_position",
                (schema,),
            )
            assert "release_hash" not in {row[0] for row in cur.fetchall()}
        drift_cases = (
            ("execution_plan_id", "plan_conflict", plan_id),
            ("execution_plan_hash", _sha("1"), _sha("e")),
            ("binding_id", "binding_conflict", binding_id),
            ("binding_hash", _sha("2"), _sha("d")),
            ("release_id", "release_conflict", "release_k6d"),
            ("package_id", "package_conflict", "package_k6d"),
            ("trade_date", date(2026, 8, 5), date(2026, 8, 6)),
        )
        for field_name, conflicting, restored in drift_cases:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {schema}.execution_runtime WHERE runtime_id=%s", (runtime_id,))
                cur.execute(
                    f"UPDATE {schema}.execution_parent_benchmark SET {field_name}=%s "
                    "WHERE parent_intent_id='intent_k6d'",
                    (conflicting,),
                )
            with pytest.raises(KernelRepositoryConflict, match="frozen parent benchmark"):
                repository.ensure_product_runtime_v1(
                    runtime_id=runtime_id,
                    binding_id=binding_id,
                    execution_plan_id=plan_id,
                )
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {schema}.execution_parent_benchmark SET {field_name}=%s "
                    "WHERE parent_intent_id='intent_k6d'",
                    (restored,),
                )
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k6_repository_public_surface_is_complete() -> None:
    expected = {
        "preflight_k6_schema",
        "preflight_k6c_schema",
        "write_dependent_buy_coordination_v1",
        "read_dependent_buy_coordination_v1",
        "append_dependent_buy_decision_v1",
        "read_dependent_buy_decision_v1",
        "read_dependent_buy_decision_evidence_v1",
        "write_product_command_authority_set_v2",
        "read_product_command_authority_set_v2",
        "activate_kernel_v2_route_v1",
        "write_product_route_cutover_v1",
        "read_product_route_owner_v1",
    }
    assert expected <= set(dir(PostgresMiniQMTKernelRepository))


def test_k6d_route_commit_unknown_uses_one_readback_without_reentering_writer() -> None:
    receipt = _route_receipt(epoch=1, owner=ProductRouteOwnerKindV1.KERNEL_V2, previous=None)
    expected = ProductRouteOwnerV1.create(receipt=receipt, row_version=1)

    class CommitUnknownRouteRepository:
        activate_kernel_v2_route_v1 = KernelProductRepositoryMixin.activate_kernel_v2_route_v1

        def __init__(self) -> None:
            self.write_attempts = 0
            self.read_attempts = 0

        def _activate_kernel_v2_route_transaction_v1(self, **_: object) -> ProductRouteOwnerV1:
            self.write_attempts += 1
            raise KernelRepositoryCommitUnknown("commit return was not observed")

        def _read_route_after_commit_unknown_v1(self, **_: object) -> ProductRouteOwnerV1:
            self.read_attempts += 1
            return expected

    repository = CommitUnknownRouteRepository()
    assert (
        repository.activate_kernel_v2_route_v1(
            runtime_id=receipt.runtime_id,
            binding_id=receipt.binding_id,
            trade_date=receipt.trade_date,
            worker_incarnation_id="process_k6",
        )
        == expected
    )
    assert (repository.write_attempts, repository.read_attempts) == (1, 1)


def test_k6d_route_commit_unknown_fails_loud_when_independent_readback_does_not_close() -> None:
    class MissingCommitReadbackRepository:
        activate_kernel_v2_route_v1 = KernelProductRepositoryMixin.activate_kernel_v2_route_v1

        def _activate_kernel_v2_route_transaction_v1(self, **_: object) -> ProductRouteOwnerV1:
            raise KernelRepositoryCommitUnknown("commit return was not observed")

        def _read_route_after_commit_unknown_v1(self, **_: object) -> ProductRouteOwnerV1:
            raise KernelRepositoryConflict("owner missing")

    with pytest.raises(KernelRepositoryCommitUnknown, match="independent authority readback did not close") as exc:
        MissingCommitReadbackRepository().activate_kernel_v2_route_v1(
            runtime_id="runtime_constraints",
            binding_id="binding_k6",
            trade_date=date(2026, 7, 25),
            worker_incarnation_id="process_k6",
        )
    assert isinstance(exc.value.__cause__, KernelRepositoryCommitUnknown)


@pytest.mark.parametrize("bad", [None, True, 1, [], {}, " ", "g" * 64])
def test_k6_repository_rejects_malformed_read_identity_before_database(bad: object) -> None:
    repository = PostgresMiniQMTKernelRepository(conn_factory=_forbidden_conn_factory)
    with pytest.raises(ValueError):
        repository.read_dependent_buy_coordination_v1(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_date", [None, True, 1, "2026-1-1", "20260725", "not-a-date"])
def test_k6_repository_rejects_malformed_trade_date_before_database(bad_date: object) -> None:
    repository = PostgresMiniQMTKernelRepository(conn_factory=_forbidden_conn_factory)
    with pytest.raises(ValueError):
        repository.read_product_route_owner_v1(
            runtime_id="runtime_k6",
            binding_id="binding_k6",
            trade_date=bad_date,  # type: ignore[arg-type]
        )


def test_k6_repository_rejects_non_carrier_writes_before_database() -> None:
    repository = PostgresMiniQMTKernelRepository(conn_factory=_forbidden_conn_factory)
    with pytest.raises(TypeError, match="coordination"):
        repository.write_dependent_buy_coordination_v1({})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="strict K6 carriers"):
        repository.append_dependent_buy_decision_v1(  # type: ignore[arg-type]
            coordination={},
            decision={},
            trigger_ref={},
            ledger_observation={},
        )
    with pytest.raises(TypeError, match="authority"):
        repository.write_product_command_authority_set_v2({})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="strict K6 route carriers"):
        repository.write_product_route_cutover_v1(receipt={}, owner={})  # type: ignore[arg-type]
    with pytest.raises(KernelRepositoryConflict, match="strict product gateway authority"):
        repository.activate_kernel_v2_route_v1(
            runtime_id="runtime_constraints",
            binding_id="binding_k6",
            trade_date=date(2026, 7, 25),
            worker_incarnation_id="product_process_k6",
        )


def test_k6d_route_schema_and_migration_identity_reject_partial_preflight() -> None:
    complete = {
        **{name: True for name in K6_TABLES},
        "k6c0_schema_catalog_fingerprint": True,
        "k6b_schema_catalog_fingerprint": True,
    }
    hashes = {
        "k6_catalog_sha256": K6_CATALOG_SHA256_K6B,
        "k6c_catalog_sha256": K6C0_CATALOG_SHA256_K6B,
        "k6b_catalog_sha256": K6B_CATALOG_SHA256,
    }
    assert product_authority_schema_sha256_v3() == product_authority_schema_sha256_v3()
    assert migration_readback_sha256_v1(dict(complete), **hashes) == migration_readback_sha256_v1(
        dict(complete), **hashes
    )
    for invalid in (
        {key: value for key, value in complete.items() if key != "k6b_schema_catalog_fingerprint"},
        {**complete, "k6b_schema_catalog_fingerprint": False},
        {**complete, "unexpected": True},
    ):
        with pytest.raises(KernelRepositorySchemaError, match="complete exact all-true"):
            migration_readback_sha256_v1(invalid, **hashes)
    for field_name in hashes:
        with pytest.raises(KernelRepositorySchemaError, match=field_name):
            migration_readback_sha256_v1(complete, **(hashes | {field_name: _sha("f")}))


def test_k6_repository_writer_readback_cas_and_drift_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6repo_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _seed_schema(cur, schema)
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        assert repository.preflight_k6_schema()["execution_product_route_owner"] is True

        initial = _coordination()
        assert repository.write_dependent_buy_coordination_v1(initial) == initial
        assert repository.write_dependent_buy_coordination_v1(initial) == initial
        assert repository.read_dependent_buy_coordination_v1(initial.coordination_id) == initial

        ledger = _ledger()
        trigger = _trigger()
        decision = DependentBuyReleaseDecisionV1.create(
            coordination_id=initial.coordination_id,
            decision_sequence=1,
            previous_decision_sha256=None,
            trigger_ref_sha256=trigger.trigger_ref_sha256,
            decision=DependentBuyDecisionV1.WAIT,
            reason_code="MINIQMT_K6_COORDINATION_CASH_STILL_INSUFFICIENT",
            ledger_observation_sha256=ledger.observation_sha256,
            ordered_dependency_sha256s=(_dependency().dependency_sha256,),
            decided_at_utc=NOW,
            worker_id="worker_k6",
            process_incarnation_id="process_k6",
            lease_epoch=1,
        )
        successor = _coordination(
            decision_sequence=1,
            last_decision_sha256=decision.decision_sha256,
            row_version=2,
            leased=True,
        )
        result = repository.append_dependent_buy_decision_v1(
            coordination=successor,
            decision=decision,
            trigger_ref=trigger,
            ledger_observation=ledger,
        )
        assert result["coordination"] == successor
        assert result["decision"] == decision
        assert result["trigger_ref"] == trigger
        assert result["ledger_observation"] == ledger
        assert (
            repository.append_dependent_buy_decision_v1(
                coordination=successor,
                decision=decision,
                trigger_ref=trigger,
                ledger_observation=ledger,
            )
            == result
        )

        authority = _mixed_authority()
        assert repository.write_product_command_authority_set_v2(authority) == authority
        assert repository.write_product_command_authority_set_v2(authority) == authority
        assert repository.read_product_command_authority_set_v2(authority.authority_set_sha256) == authority

        receipt1 = _route_receipt(epoch=1, owner=ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY, previous=None)
        owner1 = ProductRouteOwnerV1.create(receipt=receipt1, row_version=1)
        assert repository.write_product_route_cutover_v1(receipt=receipt1, owner=owner1) == owner1
        receipt2 = _route_receipt(epoch=2, owner=ProductRouteOwnerKindV1.KERNEL_V2, previous=receipt1.receipt_sha256)
        owner2 = ProductRouteOwnerV1.create(receipt=receipt2, row_version=2)
        assert repository.write_product_route_cutover_v1(receipt=receipt2, owner=owner2) == owner2
        assert repository.write_product_route_cutover_v1(receipt=receipt2, owner=owner2) == owner2
        assert (
            repository.read_product_route_owner_v1(
                runtime_id="runtime_constraints", binding_id="binding_k6", trade_date="2026-07-25"
            )
            == owner2
        )
        receipt3 = _route_receipt(
            epoch=3,
            owner=ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY,
            previous=receipt2.receipt_sha256,
        )
        owner3 = ProductRouteOwnerV1.create(receipt=receipt3, row_version=3)
        with pytest.raises(KernelRepositoryConflict, match="cannot revert"):
            repository.write_product_route_cutover_v1(receipt=receipt3, owner=owner3)
        assert (
            repository.read_product_route_owner_v1(
                runtime_id="runtime_constraints", binding_id="binding_k6", trade_date="2026-07-25"
            )
            == owner2
        )

        with conn.cursor() as cur:
            with pytest.raises(psycopg2.errors.RaiseException, match="append-only"):
                cur.execute(
                    f"UPDATE {schema}.execution_dependent_buy_decision SET reason_code='forged' WHERE decision_id=%s",
                    (decision.decision_id,),
                )
            cur.execute("ROLLBACK")
            with pytest.raises(psycopg2.errors.RaiseException, match="append-only"):
                cur.execute(
                    f"DELETE FROM {schema}.execution_product_route_cutover WHERE receipt_sha256=%s",
                    (receipt1.receipt_sha256,),
                )
            cur.execute("ROLLBACK")

        with conn.cursor() as cur:
            with pytest.raises(psycopg2.errors.RaiseException, match="route owner"):
                cur.execute(
                    f"UPDATE {schema}.execution_product_route_owner SET effective_new_instance_sequence=999 "
                    "WHERE runtime_id='runtime_constraints' AND binding_id='binding_k6'"
                )
            cur.execute("ROLLBACK")
            cur.execute(f"ALTER TABLE {schema}.execution_product_route_owner DISABLE TRIGGER USER")
            cur.execute(
                f"UPDATE {schema}.execution_product_route_owner SET effective_new_instance_sequence=999 "
                "WHERE runtime_id='runtime_constraints' AND binding_id='binding_k6'"
            )
            cur.execute(f"ALTER TABLE {schema}.execution_product_route_owner ENABLE TRIGGER USER")
        with pytest.raises(KernelRepositoryConflict, match="route owner"):
            repository.read_product_route_owner_v1(
                runtime_id="runtime_constraints", binding_id="binding_k6", trade_date="2026-07-25"
            )
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k6_repository_rejects_decision_status_drift_and_forged_lease_epoch_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6fence_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _seed_schema(cur, schema)
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        initial = _coordination()
        repository.write_dependent_buy_coordination_v1(initial)
        trigger = _trigger()
        ledger = _ledger()

        blocked_decision = DependentBuyReleaseDecisionV1.create(
            coordination_id=initial.coordination_id,
            decision_sequence=1,
            previous_decision_sha256=None,
            trigger_ref_sha256=trigger.trigger_ref_sha256,
            decision=DependentBuyDecisionV1.BLOCK,
            reason_code="MINIQMT_K6_COORDINATION_PROCEEDS_UNAVAILABLE",
            ledger_observation_sha256=ledger.observation_sha256,
            ordered_dependency_sha256s=(_dependency().dependency_sha256,),
            decided_at_utc=NOW,
            worker_id="worker_k6",
            process_incarnation_id="process_k6",
            lease_epoch=1,
        )
        waiting_successor = _coordination(
            decision_sequence=1,
            last_decision_sha256=blocked_decision.decision_sha256,
            row_version=2,
            leased=True,
        )
        with pytest.raises(KernelRepositoryConflict, match="decision kind"):
            repository.append_dependent_buy_decision_v1(
                coordination=waiting_successor,
                decision=blocked_decision,
                trigger_ref=trigger,
                ledger_observation=ledger,
            )

        forged_epoch_decision = DependentBuyReleaseDecisionV1.create(
            coordination_id=initial.coordination_id,
            decision_sequence=1,
            previous_decision_sha256=None,
            trigger_ref_sha256=trigger.trigger_ref_sha256,
            decision=DependentBuyDecisionV1.WAIT,
            reason_code="MINIQMT_K6_COORDINATION_CASH_STILL_INSUFFICIENT",
            ledger_observation_sha256=ledger.observation_sha256,
            ordered_dependency_sha256s=(_dependency().dependency_sha256,),
            decided_at_utc=NOW,
            worker_id="worker_k6",
            process_incarnation_id="process_k6",
            lease_epoch=7,
        )
        forged_successor = _coordination(
            decision_sequence=1,
            last_decision_sha256=forged_epoch_decision.decision_sha256,
            row_version=2,
            leased=True,
            lease_epoch=7,
        )
        with pytest.raises(KernelRepositoryConflict, match="first coordination lease epoch"):
            repository.append_dependent_buy_decision_v1(
                coordination=forged_successor,
                decision=forged_epoch_decision,
                trigger_ref=trigger,
                ledger_observation=ledger,
            )
        assert repository.read_dependent_buy_coordination_v1(initial.coordination_id) == initial
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k6_repository_rejects_incomplete_trigger_and_ledger_evidence_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6evidence_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _seed_schema(cur, schema)
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        initial = _coordination()
        repository.write_dependent_buy_coordination_v1(initial)
        valid_trigger = _trigger()
        valid_ledger = _ledger()

        def decision_for(
            trigger_ref: DependentBuyTriggerEventRefV1,
            ledger_observation: DependentBuyLedgerObservationV1,
            *,
            trigger_sha256: str | None = None,
            ledger_sha256: str | None = None,
        ) -> DependentBuyReleaseDecisionV1:
            return DependentBuyReleaseDecisionV1.create(
                coordination_id=initial.coordination_id,
                decision_sequence=1,
                previous_decision_sha256=None,
                trigger_ref_sha256=trigger_sha256 or trigger_ref.trigger_ref_sha256,
                decision=DependentBuyDecisionV1.WAIT,
                reason_code="MINIQMT_K6_COORDINATION_CASH_STILL_INSUFFICIENT",
                ledger_observation_sha256=ledger_sha256 or ledger_observation.observation_sha256,
                ordered_dependency_sha256s=(_dependency().dependency_sha256,),
                decided_at_utc=NOW,
                worker_id="worker_k6",
                process_incarnation_id="process_k6",
                lease_epoch=1,
            )

        wrong_runtime_trigger = _trigger(runtime_id="runtime_other")
        wrong_runtime_ledger = _ledger(runtime_id="runtime_other")
        stale_ledger = _ledger(ledger_as_of_utc=NOW - timedelta(seconds=1))
        wrong_refs_ledger = _ledger(trade_refs=(_sha("5"),), cash_refs=(_sha("6"),))
        cases = (
            (
                decision_for(valid_trigger, valid_ledger, trigger_sha256=_sha("7")),
                valid_trigger,
                valid_ledger,
                "trigger reference",
            ),
            (
                decision_for(valid_trigger, valid_ledger, ledger_sha256=_sha("8")),
                valid_trigger,
                valid_ledger,
                "ledger hash",
            ),
            (
                decision_for(wrong_runtime_trigger, valid_ledger),
                wrong_runtime_trigger,
                valid_ledger,
                "trigger runtime",
            ),
            (
                decision_for(valid_trigger, wrong_runtime_ledger),
                valid_trigger,
                wrong_runtime_ledger,
                "ledger owner",
            ),
            (
                decision_for(valid_trigger, stale_ledger),
                valid_trigger,
                stale_ledger,
                "predates trigger",
            ),
            (
                decision_for(valid_trigger, wrong_refs_ledger),
                valid_trigger,
                wrong_refs_ledger,
                "settled facts",
            ),
        )
        for decision, trigger_ref, ledger_observation, message in cases:
            successor = _coordination(
                decision_sequence=1,
                last_decision_sha256=decision.decision_sha256,
                row_version=2,
                leased=True,
            )
            with pytest.raises(KernelRepositoryConflict, match=message):
                repository.append_dependent_buy_decision_v1(
                    coordination=successor,
                    decision=decision,
                    trigger_ref=trigger_ref,
                    ledger_observation=ledger_observation,
                )
        assert repository.read_dependent_buy_coordination_v1(initial.coordination_id) == initial
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k6_repository_commit_unknown_is_not_reported_as_success_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6unknown_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _seed_schema(cur, schema)
        coordination = _coordination()
        uncertain = PostgresMiniQMTKernelRepository(conn_factory=_commit_unknown_factory(schema))
        with pytest.raises(KernelRepositoryCommitUnknown, match="not observed"):
            uncertain.write_dependent_buy_coordination_v1(coordination)
        authoritative = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        assert authoritative.read_dependent_buy_coordination_v1(coordination.coordination_id) == coordination
        assert authoritative.write_dependent_buy_coordination_v1(coordination) == coordination
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k6_repository_rejects_forged_catalog_function_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6catalog_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _seed_schema(cur, schema)
            cur.execute(
                f"CREATE OR REPLACE FUNCTION {schema}.miniqmt_k6_catalog_fingerprint() "
                "RETURNS TEXT LANGUAGE sql STABLE AS $$ "
                "SELECT 'f9985b5c93aae9655d78179cf39e9ffd840ba095d1a91a6a34d0186beafbf198'::TEXT $$"
            )
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        with pytest.raises(KernelRepositorySchemaError, match="function definition drift"):
            repository.preflight_k6_schema()
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()


def test_k6c0_repository_preflight_requires_exact_successor_catalog_on_dev_postgres() -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6-C0 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6c0repo_", 1)
    conn = psycopg2.connect(**_dev_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _seed_schema(cur, schema)
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        assert repository.preflight_k6_schema()["execution_product_route_owner"] is True
        with pytest.raises(KernelRepositorySchemaError, match="K6-C0"):
            repository.preflight_k6c_schema()
        with conn.cursor() as cur:
            cur.execute(K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
        assert repository.preflight_schema()["schema_catalog_fingerprint"] is True
        assert repository.preflight_k6_schema()["execution_product_route_owner"] is True
        assert repository.preflight_k6c_schema()["k6c0_schema_catalog_fingerprint"] is True
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE OR REPLACE FUNCTION {schema}.miniqmt_k6c_catalog_fingerprint() "
                "RETURNS TEXT LANGUAGE sql STABLE AS $$ "
                "SELECT 'f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d'::TEXT $$"
            )
        with pytest.raises(KernelRepositorySchemaError, match="function definition drift"):
            repository.preflight_k6c_schema()
    finally:
        with conn.cursor() as cur:
            cur.execute("ROLLBACK")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()
