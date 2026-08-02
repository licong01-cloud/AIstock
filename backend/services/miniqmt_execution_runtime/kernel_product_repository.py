"""K6-A durable product-authority repository mixin.

This module owns persistence only.  It does not coordinate dependent BUYs,
materialize commands, select a product route, or call a broker.
"""

from __future__ import annotations

from datetime import date, datetime
import hashlib
from typing import Any

import psycopg2.extras

from .kernel_product_contracts import (
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV1,
    DependentBuyDecisionV1,
    DependentBuyLedgerObservationV1,
    DependentBuyReleaseDecisionV1,
    DependentBuyTriggerEventRefV1,
    ProductCommandAuthorityItemV2,
    ProductCommandAuthoritySetV2,
    ProductRouteCutoverReceiptV1,
    ProductRouteOwnerKindV1,
    ProductRouteOwnerV1,
    validate_kernel_product_payload_v1,
)
from .kernel_repository_common import (
    KernelRepositoryConflict,
    KernelRepositorySchemaError,
    _json,
    _row_json,
)
from .kernel_repository_projection import _assert_scalar_columns


K6_TABLES = (
    "execution_dependent_buy_coordination",
    "execution_dependent_buy_dependency",
    "execution_dependent_buy_decision",
    "execution_product_command_authority",
    "execution_product_command_authority_item",
    "execution_product_route_cutover",
    "execution_product_route_owner",
)
K6_CATALOG_SHA256 = "546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad"
K6_CATALOG_SHA256_K6C0 = "6e33248ad909c59db11059f723adbe39c4c8a151c902e9af0fe0fd3637adacc9"
K6_CATALOG_SHA256S = frozenset({K6_CATALOG_SHA256, K6_CATALOG_SHA256_K6C0})
K6_CATALOG_FUNCTION_BODY_SHA256 = "bcb0b57b1cb425f4eb3d34b2ce5ca24c9f430986665871384482dfc056f5628a"
K6C0_CATALOG_SHA256 = "f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d"
K6C0_CATALOG_FUNCTION_BODY_SHA256 = "0b9889bc7c4bdfa980e3deddfc87edfd7379047778f837a2c4d6b1eecee272f4"


def _strict_identity(value: Any, *, field_name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty canonical strict string")
    return value


def _strict_sha256(value: Any, *, field_name: str) -> str:
    identity = _strict_identity(value, field_name=field_name)
    if len(identity) != 64 or any(character not in "0123456789abcdef" for character in identity):
        raise ValueError(f"{field_name} must be a lowercase SHA-256")
    return identity


def _strict_trade_date(value: Any) -> date:
    if type(value) is date:
        return value
    if type(value) is not str:
        raise ValueError("trade_date must be a strict date or canonical YYYY-MM-DD string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("trade_date must be canonical YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("trade_date must be canonical YYYY-MM-DD")
    return parsed


def _read_k6_model(model_type: Any, value: dict[str, Any], *, stage: str) -> Any:
    return validate_kernel_product_payload_v1(model_type, value, stage=stage)


_K6_CATALOG_PAYLOAD_SQL = """
WITH target_tables(relname) AS (
    VALUES
        ('execution_dependent_buy_coordination'),
        ('execution_dependent_buy_dependency'),
        ('execution_dependent_buy_decision'),
        ('execution_product_command_authority'),
        ('execution_product_command_authority_item'),
        ('execution_product_route_cutover'),
        ('execution_product_route_owner')
), catalog_items(sort_key,item) AS (
    SELECT format('column:%s:%05s',c.relname,a.attnum),
           jsonb_build_array('column',c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
                             coalesce(pg_get_expr(d.adbin,d.adrelid),''))
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('constraint:%s:%s',c.relname,k.conname),
           jsonb_build_array('constraint',c.relname,k.conname,k.contype,k.condeferrable,k.condeferred,k.convalidated,
                             replace(pg_get_constraintdef(k.oid,true),n.nspname||'.','<schema>.'))
    FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('index:%s:%s',c.relname,i.relname),
           jsonb_build_array('index',c.relname,i.relname,x.indisunique,x.indisprimary,x.indisvalid,x.indisready,
                             replace(pg_get_indexdef(x.indexrelid,0,true),n.nspname||'.','<schema>.'),
                             coalesce(replace(pg_get_expr(x.indpred,x.indrelid,true),n.nspname||'.','<schema>.'),''))
    FROM pg_index x JOIN pg_class c ON c.oid=x.indrelid JOIN pg_class i ON i.oid=x.indexrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('table-comment:%s',c.relname),jsonb_build_array('table-comment',c.relname,coalesce(obj_description(c.oid),'') )
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('column-comment:%s:%05s',c.relname,a.attnum),
           jsonb_build_array('column-comment',c.relname,a.attname,coalesce(col_description(c.oid,a.attnum),''))
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('trigger:%s:%s',c.relname,t.tgname),
           jsonb_build_array('trigger',c.relname,t.tgname,t.tgenabled,
                             replace(pg_get_triggerdef(t.oid,true),n.nspname||'.','<schema>.'))
    FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables) AND NOT t.tgisinternal
    UNION ALL
    SELECT format('function:%s',p.proname),
           jsonb_build_array('function',p.proname,l.lanname,p.provolatile,
                             replace(p.prosrc,n.nspname||'.','<schema>.'))
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
    WHERE n.nspname='qmt_strategy' AND p.proname IN (
        'miniqmt_k6_reject_immutable_mutation','miniqmt_k6_validate_route_owner',
        'miniqmt_k6_validate_coordination_update','miniqmt_k6_validate_decision_closure'
    )
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key),'[]'::jsonb)::TEXT AS payload FROM catalog_items
)
SELECT payload FROM canonical_catalog
"""

_K6C0_CATALOG_PAYLOAD_SQL = """
WITH target_columns(relname,attname) AS (
    VALUES
        ('execution_child_order','mapping_status'),
        ('execution_dependent_buy_coordination','release_command_id'),
        ('execution_dependent_buy_coordination','release_transition_id'),
        ('execution_dependent_buy_coordination','release_command_authority_item_sha256'),
        ('execution_dependent_buy_dependency','latest_order_fact_id'),
        ('execution_dependent_buy_dependency','latest_order_fact_sha256'),
        ('execution_dependent_buy_dependency','ordered_settled_proceeds_refs'),
        ('execution_dependent_buy_decision','ledger_row_version'),
        ('execution_dependent_buy_decision','ledger_virtual_account_updated_at_utc'),
        ('execution_dependent_buy_decision','ledger_latest_cash_sequence'),
        ('execution_dependent_buy_decision','ledger_revision_sha256'),
        ('execution_product_command_authority','defer_count'),
        ('execution_product_command_authority_item','command_json'),
        ('execution_product_command_authority_item','evaluation_evidence_json'),
        ('execution_product_command_authority_item','evaluation_evidence_sha256'),
        ('execution_product_command_authority_item','coordination_id')
), target_constraints(relname,conname) AS (
    VALUES
        ('execution_child_order','ck_miniqmt_k2_child_mapping_contract'),
        ('execution_child_order','ck_miniqmt_k2_child_mapping_initial'),
        ('execution_child_order','ck_miniqmt_k6_product_mapping_state'),
        ('execution_dependent_buy_coordination','ck_miniqmt_k6_coordination_v2_release'),
        ('execution_dependent_buy_coordination','fk_miniqmt_k6_coordination_release_item'),
        ('execution_dependent_buy_dependency','ck_miniqmt_k6_dependency_v2_json'),
        ('execution_dependent_buy_decision','ck_miniqmt_k6_decision_v2_ledger'),
        ('execution_product_command_authority','ck_miniqmt_k6_authority_counts'),
        ('execution_product_command_authority','ck_miniqmt_k6_authority_disposition'),
        ('execution_product_command_authority_item','ck_miniqmt_k6_authority_item_disposition'),
        ('execution_product_command_authority_item','ck_miniqmt_k6_authority_item_presence'),
        ('execution_product_command_authority_item','uq_miniqmt_k6_authority_item_sha256'),
        ('execution_product_command_authority_item','fk_miniqmt_k6_authority_item_coordination')
), catalog_items(sort_key,item) AS (
    SELECT format('column:%s:%s',c.relname,a.attname),
           jsonb_build_array('column',c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
                             coalesce(pg_get_expr(d.adbin,d.adrelid),''),coalesce(col_description(c.oid,a.attnum),''))
    FROM target_columns tc JOIN pg_class c ON c.relname=tc.relname
    JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='qmt_strategy'
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=tc.attname AND a.attnum>0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
    UNION ALL
    SELECT format('constraint:%s:%s',c.relname,k.conname),
           jsonb_build_array('constraint',c.relname,k.conname,k.contype,k.condeferrable,k.condeferred,k.convalidated,
                             replace(pg_get_constraintdef(k.oid,true),n.nspname||'.','<schema>.'))
    FROM target_constraints tc JOIN pg_class c ON c.relname=tc.relname
    JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='qmt_strategy'
    JOIN pg_constraint k ON k.conrelid=c.oid AND k.conname=tc.conname
    UNION ALL
    SELECT 'function:miniqmt_k6_validate_coordination_update',
           jsonb_build_array('function',p.proname,l.lanname,p.provolatile,
                             replace(p.prosrc,n.nspname||'.','<schema>.'))
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
    WHERE n.nspname='qmt_strategy' AND p.proname='miniqmt_k6_validate_coordination_update'
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key),'[]'::jsonb)::TEXT AS payload FROM catalog_items
)
SELECT payload FROM canonical_catalog
"""


def _coordination_projection(value: DependentBuyCoordinationV1) -> dict[str, Any]:
    return {
        "coordination_id": value.coordination_id,
        "runtime_id": value.runtime_id,
        "binding_id": value.binding_id,
        "trade_date": value.trade_date,
        "strategy_id": value.strategy_id,
        "buy_algo_instance_id": value.buy_algo_instance_id,
        "buy_parent_intent_id": value.buy_parent_intent_id,
        "required_cash": value.required_cash,
        "release_command_payload_sha256": value.release_command_payload_sha256,
        "status": value.status.value,
        "decision_sequence": value.decision_sequence,
        "last_decision_sha256": value.last_decision_sha256,
        "released_command_id": value.released_command_id,
        "released_outbox_id": value.released_outbox_id,
        "row_version": value.row_version,
        "lease_worker_id": value.lease_worker_id,
        "lease_process_incarnation_id": value.lease_process_incarnation_id,
        "lease_epoch": value.lease_epoch,
        "lease_expires_at_utc": value.lease_expires_at_utc,
        "created_at_utc": value.created_at_utc,
        "updated_at_utc": value.updated_at_utc,
        "coordination_sha256": value.coordination_sha256,
    }


def _dependency_projection(value: Any) -> dict[str, Any]:
    return {
        "runtime_id": value.runtime_id,
        "strategy_id": value.strategy_id,
        "sell_parent_intent_id": value.sell_parent_intent_id,
        "sell_algo_instance_id": value.sell_algo_instance_id,
        "latest_order_fact_ref": value.latest_order_fact_ref,
        "settled_trade_fact_refs": list(value.settled_trade_fact_refs),
        "settled_cash_ledger_refs": list(value.settled_cash_ledger_refs),
        "dependency_status": value.dependency_status.value,
        "dependency_sha256": value.dependency_sha256,
    }


def _decision_projection(value: DependentBuyReleaseDecisionV1) -> dict[str, Any]:
    return {
        "decision_id": value.decision_id,
        "coordination_id": value.coordination_id,
        "decision_sequence": value.decision_sequence,
        "previous_decision_sha256": value.previous_decision_sha256,
        "trigger_ref_sha256": value.trigger_ref_sha256,
        "decision": value.decision.value,
        "reason_code": value.reason_code,
        "ledger_observation_sha256": value.ledger_observation_sha256,
        "ordered_dependency_sha256s": list(value.ordered_dependency_sha256s),
        "release_event_id": value.release_event_id,
        "release_transition_id": value.release_transition_id,
        "release_command_authority_set_sha256": value.release_command_authority_set_sha256,
        "decided_at_utc": value.decided_at_utc,
        "worker_id": value.worker_id,
        "process_incarnation_id": value.process_incarnation_id,
        "lease_epoch": value.lease_epoch,
        "decision_sha256": value.decision_sha256,
    }


def _decision_evidence_projection(
    trigger_ref: DependentBuyTriggerEventRefV1,
    ledger_observation: DependentBuyLedgerObservationV1,
) -> dict[str, Any]:
    return {
        "trigger_event_id": trigger_ref.event_id,
        "ledger_virtual_account_id": ledger_observation.virtual_account_id,
        "ledger_row_version": ledger_observation.ledger_row_version,
    }


def _authority_projection(value: ProductCommandAuthoritySetV2) -> dict[str, Any]:
    return {
        "authority_set_sha256": value.authority_set_sha256,
        "transition_id": value.transition_id,
        "runtime_id": value.runtime_id,
        "algo_instance_id": value.algo_instance_id,
        "event_id": value.event_id,
        "delivery_id": value.delivery_id,
        "catalog_sha256": value.catalog_sha256,
        "creation_binding_sha256": value.creation_binding_sha256,
        "facade_conformance_set_sha256": value.facade_conformance_set_sha256,
        "execution_projection_set_sha256": value.execution_projection_set_sha256,
        "transition_receipt_sha256": value.transition_receipt_sha256,
        "materialize_count": value.materialize_count,
        "reject_count": value.reject_count,
        "total_count": value.total_count,
        "aggregate_disposition": value.aggregate_disposition.value,
    }


def _authority_item_projection(value: ProductCommandAuthorityItemV2) -> dict[str, Any]:
    return {
        "transition_id": value.transition_id,
        "effect_ordinal": value.effect_ordinal,
        "command_id": value.command_id,
        "disposition": value.disposition.value,
        "mapping_id": value.mapping_id,
        "outbox_id": value.outbox_id,
        "child_order_id": value.child_order_id,
        "reject_reason_code": value.reject_reason_code,
        "reject_context_sha256": value.reject_context_sha256,
        "item_sha256": value.item_sha256,
    }


def _route_projection(value: ProductRouteCutoverReceiptV1) -> dict[str, Any]:
    return {
        "runtime_id": value.runtime_id,
        "binding_id": value.binding_id,
        "trade_date": value.trade_date,
        "route_epoch": value.route_epoch,
        "route_owner": value.route_owner.value,
        "effective_new_instance_sequence": value.effective_new_instance_sequence,
        "legacy_active_instance_count": value.legacy_active_instance_count,
        "kernel_active_instance_count": value.kernel_active_instance_count,
        "catalog_sha256": value.catalog_sha256,
        "gateway_capability_catalog_sha256": value.gateway_capability_catalog_sha256,
        "exchange_session_authority_sha256": value.exchange_session_authority_sha256,
        "migration_readback_sha256": value.migration_readback_sha256,
        "product_authority_schema_sha256": value.product_authority_schema_sha256,
        "previous_receipt_sha256": value.previous_receipt_sha256,
        "created_at_utc": value.created_at_utc,
        "receipt_sha256": value.receipt_sha256,
    }


def _owner_projection(value: ProductRouteOwnerV1) -> dict[str, Any]:
    return {
        "runtime_id": value.runtime_id,
        "binding_id": value.binding_id,
        "trade_date": value.trade_date,
        "current_route_epoch": value.current_route_epoch,
        "current_receipt_sha256": value.current_receipt_sha256,
        "route_owner": value.route_owner.value,
        "effective_new_instance_sequence": value.effective_new_instance_sequence,
        "row_version": value.row_version,
        "owner_sha256": value.owner_sha256,
    }


class KernelProductRepositoryMixin:
    """Strict K6-A writer/readback authority over the shared connection owner."""

    def preflight_k6_schema(self) -> dict[str, bool]:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname='qmt_strategy' AND c.relkind='r' AND c.relname=ANY(%s)",
                    (list(K6_TABLES),),
                )
                found = {str(row["relname"]) for row in cur.fetchall()}
                if found != set(K6_TABLES):
                    raise KernelRepositorySchemaError(
                        f"K6 schema incomplete: missing={sorted(set(K6_TABLES) - found)} extra={sorted(found - set(K6_TABLES))}"
                    )
                cur.execute(_K6_CATALOG_PAYLOAD_SQL)
                row = cur.fetchone()
                if row is None or type(row["payload"]) is not str:
                    raise KernelRepositorySchemaError("K6 independent catalog payload is unavailable")
                independent = hashlib.sha256(row["payload"].encode("utf-8")).hexdigest()
                cur.execute("SELECT qmt_strategy.miniqmt_k6_catalog_fingerprint() AS catalog_sha256")
                function_row = cur.fetchone()
                cur.execute(
                    "SELECT n.nspname,l.lanname,p.provolatile,p.prokind,"
                    "pg_get_function_identity_arguments(p.oid) AS identity_arguments,p.prosrc "
                    "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                    "JOIN pg_language l ON l.oid=p.prolang "
                    "WHERE n.nspname='qmt_strategy' AND p.proname='miniqmt_k6_catalog_fingerprint'"
                )
                definition_row = cur.fetchone()
        function_value = None if function_row is None else function_row["catalog_sha256"]
        if definition_row is None:
            raise KernelRepositorySchemaError("K6 catalog fingerprint function definition is missing")
        normalized_body = (
            str(definition_row["prosrc"])
            .replace(str(definition_row["nspname"]), "<schema>")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
        )
        body_sha256 = hashlib.sha256(normalized_body.encode("utf-8")).hexdigest()
        metadata = (
            definition_row["lanname"],
            definition_row["provolatile"],
            definition_row["prokind"],
            definition_row["identity_arguments"],
        )
        if metadata != ("sql", "s", "f", "") or body_sha256 != K6_CATALOG_FUNCTION_BODY_SHA256:
            raise KernelRepositorySchemaError(
                "K6 catalog fingerprint function definition drift: "
                f"metadata={metadata} expected_body={K6_CATALOG_FUNCTION_BODY_SHA256} actual_body={body_sha256}"
            )
        if independent not in K6_CATALOG_SHA256S or function_value != independent:
            raise KernelRepositorySchemaError(
                "K6 catalog fingerprint mismatch: "
                f"expected_one_of={sorted(K6_CATALOG_SHA256S)} independent={independent} function={function_value}"
            )
        return {table: True for table in K6_TABLES}

    def preflight_k6c_schema(self) -> dict[str, bool]:
        """Require the exact K6-C0 successor catalog without changing the K6-A preflight contract."""

        result = self.preflight_k6_schema()
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_K6_CATALOG_PAYLOAD_SQL)
                k6_row = cur.fetchone()
                cur.execute(_K6C0_CATALOG_PAYLOAD_SQL)
                k6c0_row = cur.fetchone()
                cur.execute(
                    "SELECT n.nspname,l.lanname,p.provolatile,p.prokind,"
                    "pg_get_function_identity_arguments(p.oid) AS identity_arguments,p.prosrc "
                    "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                    "JOIN pg_language l ON l.oid=p.prolang "
                    "WHERE n.nspname='qmt_strategy' AND p.proname='miniqmt_k6c_catalog_fingerprint'"
                )
                definition_row = cur.fetchone()
                function_row = None
                if definition_row is not None:
                    cur.execute("SELECT qmt_strategy.miniqmt_k6c_catalog_fingerprint() AS catalog_sha256")
                    function_row = cur.fetchone()
        if k6_row is None or type(k6_row["payload"]) is not str:
            raise KernelRepositorySchemaError("K6-C0 base catalog payload is unavailable")
        if k6c0_row is None or type(k6c0_row["payload"]) is not str:
            raise KernelRepositorySchemaError("K6-C0 independent catalog payload is unavailable")
        if function_row is None or definition_row is None:
            raise KernelRepositorySchemaError("K6-C0 catalog fingerprint function definition is missing")
        k6_catalog_sha256 = hashlib.sha256(k6_row["payload"].encode("utf-8")).hexdigest()
        independent = hashlib.sha256(k6c0_row["payload"].encode("utf-8")).hexdigest()
        function_value = function_row["catalog_sha256"]
        normalized_body = (
            str(definition_row["prosrc"])
            .replace(str(definition_row["nspname"]), "<schema>")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
        )
        body_sha256 = hashlib.sha256(normalized_body.encode("utf-8")).hexdigest()
        metadata = (
            definition_row["lanname"],
            definition_row["provolatile"],
            definition_row["prokind"],
            definition_row["identity_arguments"],
        )
        if metadata != ("sql", "s", "f", "") or body_sha256 != K6C0_CATALOG_FUNCTION_BODY_SHA256:
            raise KernelRepositorySchemaError(
                "K6-C0 catalog fingerprint function definition drift: "
                f"metadata={metadata} expected_body={K6C0_CATALOG_FUNCTION_BODY_SHA256} actual_body={body_sha256}"
            )
        if k6_catalog_sha256 != K6_CATALOG_SHA256_K6C0:
            raise KernelRepositorySchemaError(
                "K6-C0 requires the exact successor K6 catalog: "
                f"expected={K6_CATALOG_SHA256_K6C0} actual={k6_catalog_sha256}"
            )
        if independent != K6C0_CATALOG_SHA256 or function_value != independent:
            raise KernelRepositorySchemaError(
                "K6-C0 catalog fingerprint mismatch: "
                f"expected={K6C0_CATALOG_SHA256} independent={independent} function={function_value}"
            )
        result["k6c0_schema_catalog_fingerprint"] = True
        return result

    def read_dependent_buy_coordination_v1(self, coordination_id: str) -> DependentBuyCoordinationV1:
        coordination_id = _strict_sha256(coordination_id, field_name="coordination_id")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_dependent_buy_coordination WHERE coordination_id=%s",
                    (coordination_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(coordination_id)
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_dependent_buy_dependency WHERE coordination_id=%s "
                    "ORDER BY sell_parent_intent_id",
                    (coordination_id,),
                )
                dependency_rows = cur.fetchall()
        coordination = _read_k6_model(
            DependentBuyCoordinationV1,
            _row_json(row, "carrier_json"),
            stage="DEPENDENT_BUY_COORDINATION_READBACK",
        )
        _assert_scalar_columns(row, _coordination_projection(coordination), carrier_name="dependent_buy_coordination")
        dependencies = tuple(
            _read_k6_model(
                type(coordination.ordered_sell_dependencies[0]),
                _row_json(item, "carrier_json"),
                stage="DEPENDENT_BUY_DEPENDENCY_READBACK",
            )
            for item in dependency_rows
        )
        for item, dependency in zip(dependency_rows, dependencies, strict=True):
            _assert_scalar_columns(item, _dependency_projection(dependency), carrier_name="dependent_buy_dependency")
        if dependencies != coordination.ordered_sell_dependencies:
            raise KernelRepositoryConflict("dependent-BUY coordination dependency readback differs")
        return coordination

    def write_dependent_buy_coordination_v1(
        self, coordination: DependentBuyCoordinationV1
    ) -> DependentBuyCoordinationV1:
        if not isinstance(coordination, DependentBuyCoordinationV1):
            raise TypeError("coordination must be DependentBuyCoordinationV1")
        coordination.validate_initial_v1()
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_dependent_buy_coordination "
                    "WHERE coordination_id=%s FOR SHARE",
                    (coordination.coordination_id,),
                )
                existing_coordination_row = cur.fetchone()
                if existing_coordination_row is not None:
                    existing_coordination = _read_k6_model(
                        DependentBuyCoordinationV1,
                        _row_json(existing_coordination_row, "carrier_json"),
                        stage="DEPENDENT_BUY_COORDINATION_IDEMPOTENT_READBACK",
                    )
                    cur.execute(
                        "SELECT carrier_json FROM qmt_strategy.execution_dependent_buy_dependency "
                        "WHERE coordination_id=%s ORDER BY sell_parent_intent_id",
                        (coordination.coordination_id,),
                    )
                    dependency_model = type(coordination.ordered_sell_dependencies[0])
                    existing_dependencies = tuple(
                        _read_k6_model(
                            dependency_model,
                            _row_json(row, "carrier_json"),
                            stage="DEPENDENT_BUY_DEPENDENCY_IDEMPOTENT_READBACK",
                        )
                        for row in cur.fetchall()
                    )
                    if (
                        existing_coordination != coordination
                        or existing_dependencies != coordination.ordered_sell_dependencies
                    ):
                        raise KernelRepositoryConflict(
                            "idempotent coordination retry differs from complete durable closure"
                        )
                    is_retry = True
                else:
                    is_retry = False
                cur.execute(
                    "SELECT runtime_id,parent_intent_id,side FROM qmt_strategy.execution_algo_instance "
                    "WHERE algo_instance_id=%s FOR SHARE",
                    (coordination.buy_algo_instance_id,),
                )
                buy = cur.fetchone()
                if buy is None or (buy["runtime_id"], buy["parent_intent_id"], buy["side"]) != (
                    coordination.runtime_id,
                    coordination.buy_parent_intent_id,
                    "BUY",
                ):
                    raise KernelRepositoryConflict("dependent-BUY owner does not close to a durable BUY algo")
                for dependency in coordination.ordered_sell_dependencies:
                    cur.execute(
                        "SELECT runtime_id,parent_intent_id,side FROM qmt_strategy.execution_algo_instance "
                        "WHERE algo_instance_id=%s FOR SHARE",
                        (dependency.sell_algo_instance_id,),
                    )
                    sell = cur.fetchone()
                    if sell is None or (sell["runtime_id"], sell["parent_intent_id"], sell["side"]) != (
                        coordination.runtime_id,
                        dependency.sell_parent_intent_id,
                        "SELL",
                    ):
                        raise KernelRepositoryConflict("dependent-BUY dependency does not close to a durable SELL algo")
                if not is_retry:
                    self._write_dependent_buy_coordination_with_cursor(cur, coordination)
        readback = self.read_dependent_buy_coordination_v1(coordination.coordination_id)
        if readback != coordination:
            raise KernelRepositoryConflict("dependent-BUY coordination post-commit readback differs")
        return readback

    @staticmethod
    def _write_dependent_buy_coordination_with_cursor(
        cur: Any,
        coordination: DependentBuyCoordinationV1,
    ) -> None:
        projection = _coordination_projection(coordination)
        columns = tuple(projection)
        cur.execute(
            f"INSERT INTO qmt_strategy.execution_dependent_buy_coordination({','.join(columns)},carrier_json) "
            f"VALUES ({','.join(['%s'] * (len(columns) + 1))})",
            (*projection.values(), _json(coordination.model_dump(mode="json"))),
        )
        for dependency in coordination.ordered_sell_dependencies:
            item = _dependency_projection(dependency)
            columns = ("coordination_id", *item)
            cur.execute(
                f"INSERT INTO qmt_strategy.execution_dependent_buy_dependency({','.join(columns)},carrier_json) "
                f"VALUES ({','.join(['%s'] * (len(columns) + 1))})",
                (
                    coordination.coordination_id,
                    *(
                        _json(value) if key in {"settled_trade_fact_refs", "settled_cash_ledger_refs"} else value
                        for key, value in item.items()
                    ),
                    _json(dependency.model_dump(mode="json")),
                ),
            )

    def read_dependent_buy_decision_evidence_v1(self, decision_id: str) -> dict[str, Any]:
        decision_id = _strict_sha256(decision_id, field_name="decision_id")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_dependent_buy_decision WHERE decision_id=%s", (decision_id,)
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(decision_id)
        decision = _read_k6_model(
            DependentBuyReleaseDecisionV1,
            _row_json(row, "carrier_json"),
            stage="DEPENDENT_BUY_DECISION_READBACK",
        )
        trigger_ref = _read_k6_model(
            DependentBuyTriggerEventRefV1,
            _row_json(row, "trigger_ref_json"),
            stage="DEPENDENT_BUY_TRIGGER_READBACK",
        )
        ledger_observation = _read_k6_model(
            DependentBuyLedgerObservationV1,
            _row_json(row, "ledger_observation_json"),
            stage="DEPENDENT_BUY_LEDGER_OBSERVATION_READBACK",
        )
        _assert_scalar_columns(row, _decision_projection(decision), carrier_name="dependent_buy_decision")
        _assert_scalar_columns(
            row,
            _decision_evidence_projection(trigger_ref, ledger_observation),
            carrier_name="dependent_buy_decision_evidence",
        )
        coordination = self.read_dependent_buy_coordination_v1(decision.coordination_id)
        self._validate_dependent_buy_decision_evidence_v1(
            coordination=coordination,
            decision=decision,
            trigger_ref=trigger_ref,
            ledger_observation=ledger_observation,
        )
        return {
            "decision": decision,
            "trigger_ref": trigger_ref,
            "ledger_observation": ledger_observation,
        }

    def read_dependent_buy_decision_v1(self, decision_id: str) -> DependentBuyReleaseDecisionV1:
        return self.read_dependent_buy_decision_evidence_v1(decision_id)["decision"]

    def append_dependent_buy_decision_v1(
        self,
        *,
        coordination: DependentBuyCoordinationV1,
        decision: DependentBuyReleaseDecisionV1,
        trigger_ref: DependentBuyTriggerEventRefV1,
        ledger_observation: DependentBuyLedgerObservationV1,
    ) -> dict[str, Any]:
        if (
            not isinstance(coordination, DependentBuyCoordinationV1)
            or not isinstance(decision, DependentBuyReleaseDecisionV1)
            or not isinstance(trigger_ref, DependentBuyTriggerEventRefV1)
            or not isinstance(ledger_observation, DependentBuyLedgerObservationV1)
        ):
            raise TypeError("coordination, decision, trigger and ledger observation must use strict K6 carriers")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_dependent_buy_coordination "
                    "WHERE coordination_id=%s FOR UPDATE",
                    (coordination.coordination_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(coordination.coordination_id)
                previous = _read_k6_model(
                    DependentBuyCoordinationV1,
                    _row_json(row, "carrier_json"),
                    stage="DEPENDENT_BUY_COORDINATION_PREDECESSOR_READBACK",
                )
                if previous == coordination:
                    cur.execute(
                        "SELECT carrier_json,trigger_ref_json,ledger_observation_json "
                        "FROM qmt_strategy.execution_dependent_buy_decision WHERE decision_id=%s",
                        (decision.decision_id,),
                    )
                    existing_decision_row = cur.fetchone()
                    existing_decision = (
                        None
                        if existing_decision_row is None
                        else _read_k6_model(
                            DependentBuyReleaseDecisionV1,
                            _row_json(existing_decision_row, "carrier_json"),
                            stage="DEPENDENT_BUY_DECISION_IDEMPOTENT_READBACK",
                        )
                    )
                    if existing_decision != decision:
                        raise KernelRepositoryConflict("idempotent decision retry differs from durable decision")
                    if existing_decision_row is None:
                        raise KernelRepositoryConflict("idempotent decision retry lacks durable evidence")
                    existing_trigger = _read_k6_model(
                        DependentBuyTriggerEventRefV1,
                        _row_json(existing_decision_row, "trigger_ref_json"),
                        stage="DEPENDENT_BUY_TRIGGER_IDEMPOTENT_READBACK",
                    )
                    existing_ledger = _read_k6_model(
                        DependentBuyLedgerObservationV1,
                        _row_json(existing_decision_row, "ledger_observation_json"),
                        stage="DEPENDENT_BUY_LEDGER_IDEMPOTENT_READBACK",
                    )
                    if existing_trigger != trigger_ref or existing_ledger != ledger_observation:
                        raise KernelRepositoryConflict("idempotent decision retry differs from durable evidence")
                    is_retry = True
                else:
                    is_retry = False
                if not is_retry:
                    coordination.validate_successor_v1(previous)
                    self._append_dependent_buy_decision_with_cursor(
                        cur,
                        previous,
                        coordination,
                        decision,
                        trigger_ref,
                        ledger_observation,
                    )
        evidence_readback = self.read_dependent_buy_decision_evidence_v1(decision.decision_id)
        decision_readback = evidence_readback["decision"]
        coordination_readback = self.read_dependent_buy_coordination_v1(coordination.coordination_id)
        if (
            decision_readback != decision
            or evidence_readback["trigger_ref"] != trigger_ref
            or evidence_readback["ledger_observation"] != ledger_observation
            or coordination_readback != coordination
        ):
            raise KernelRepositoryConflict("dependent-BUY decision transaction post-commit readback differs")
        return {
            "coordination": coordination_readback,
            "decision": decision_readback,
            "trigger_ref": evidence_readback["trigger_ref"],
            "ledger_observation": evidence_readback["ledger_observation"],
        }

    def _append_dependent_buy_decision_with_cursor(
        self,
        cur: Any,
        previous: DependentBuyCoordinationV1,
        coordination: DependentBuyCoordinationV1,
        decision: DependentBuyReleaseDecisionV1,
        trigger_ref: DependentBuyTriggerEventRefV1,
        ledger_observation: DependentBuyLedgerObservationV1,
    ) -> None:
        if decision.coordination_id != coordination.coordination_id:
            raise KernelRepositoryConflict("decision owner differs from coordination")
        if decision.decision_sequence != previous.decision_sequence + 1:
            raise KernelRepositoryConflict("decision sequence is not the exact durable successor")
        if decision.previous_decision_sha256 != previous.last_decision_sha256:
            raise KernelRepositoryConflict("decision predecessor differs from durable coordination")
        dependency_hashes = tuple(sorted(item.dependency_sha256 for item in coordination.ordered_sell_dependencies))
        if dependency_hashes != decision.ordered_dependency_sha256s:
            raise KernelRepositoryConflict("decision dependency closure differs from coordination")
        if (
            coordination.decision_sequence != decision.decision_sequence
            or coordination.last_decision_sha256 != decision.decision_sha256
        ):
            raise KernelRepositoryConflict("coordination successor does not close to decision")
        expected_status = {
            DependentBuyDecisionV1.WAIT: DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
            DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX: DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX,
            DependentBuyDecisionV1.BLOCK: DependentBuyCoordinationStatusV1.BLOCKED_SELL_PROCEEDS_UNAVAILABLE,
            DependentBuyDecisionV1.EOD_RESIDUAL: DependentBuyCoordinationStatusV1.EOD_RESIDUAL,
        }[decision.decision]
        if coordination.status is not expected_status:
            raise KernelRepositoryConflict("decision kind and coordination durable status differ")
        if (coordination.lease_worker_id, coordination.lease_process_incarnation_id, coordination.lease_epoch) != (
            decision.worker_id,
            decision.process_incarnation_id,
            decision.lease_epoch,
        ):
            raise KernelRepositoryConflict("decision fencing differs from coordination lease")
        if previous.lease_epoch == 0:
            if coordination.lease_epoch != 1:
                raise KernelRepositoryConflict("first coordination lease epoch must be one")
        elif coordination.lease_epoch not in {previous.lease_epoch, previous.lease_epoch + 1}:
            raise KernelRepositoryConflict("coordination lease epoch is not current or exact successor")
        elif coordination.lease_epoch == previous.lease_epoch and (
            coordination.lease_worker_id,
            coordination.lease_process_incarnation_id,
        ) != (previous.lease_worker_id, previous.lease_process_incarnation_id):
            raise KernelRepositoryConflict("same coordination lease epoch cannot change owner")
        self._validate_dependent_buy_decision_evidence_v1(
            coordination=coordination,
            decision=decision,
            trigger_ref=trigger_ref,
            ledger_observation=ledger_observation,
        )
        cur.execute(
            "SELECT runtime_id,sequence AS event_sequence FROM qmt_strategy.execution_runtime_event "
            "WHERE event_id=%s FOR SHARE",
            (trigger_ref.event_id,),
        )
        trigger_event = cur.fetchone()
        if trigger_event is None or (
            trigger_event["runtime_id"],
            int(trigger_event["event_sequence"]),
        ) != (trigger_ref.runtime_id, trigger_ref.event_sequence):
            raise KernelRepositoryConflict("dependent-BUY trigger does not close to exact K2 runtime event")
        self._verify_k6_worker_cursor(cur, decision.worker_id, decision.process_incarnation_id, decision.lease_epoch)
        projection = _decision_projection(decision) | _decision_evidence_projection(trigger_ref, ledger_observation)
        columns = (*projection, "trigger_ref_json", "ledger_observation_json", "carrier_json")
        cur.execute(
            f"INSERT INTO qmt_strategy.execution_dependent_buy_decision({','.join(columns)}) "
            f"VALUES ({','.join(['%s'] * len(columns))}) ON CONFLICT (decision_id) DO NOTHING",
            (
                *(_json(value) if key == "ordered_dependency_sha256s" else value for key, value in projection.items()),
                _json(trigger_ref.model_dump(mode="json")),
                _json(ledger_observation.model_dump(mode="json")),
                _json(decision.model_dump(mode="json")),
            ),
        )
        if cur.rowcount != 1:
            cur.execute(
                "SELECT carrier_json FROM qmt_strategy.execution_dependent_buy_decision WHERE decision_id=%s",
                (decision.decision_id,),
            )
            conflict_row = cur.fetchone()
            conflict = (
                None
                if conflict_row is None
                else _read_k6_model(
                    DependentBuyReleaseDecisionV1,
                    _row_json(conflict_row, "carrier_json"),
                    stage="DEPENDENT_BUY_DECISION_CONFLICT_READBACK",
                )
            )
            if conflict != decision:
                raise KernelRepositoryConflict("decision identity exists with different durable payload")
        successor = _coordination_projection(coordination)
        assignments = ",".join(f"{key}=%s" for key in successor if key != "coordination_id") + ",carrier_json=%s"
        cur.execute(
            f"UPDATE qmt_strategy.execution_dependent_buy_coordination SET {assignments} "
            "WHERE coordination_id=%s AND row_version=%s",
            (
                *[value for key, value in successor.items() if key != "coordination_id"],
                _json(coordination.model_dump(mode="json")),
                coordination.coordination_id,
                previous.row_version,
            ),
        )
        if cur.rowcount != 1:
            raise KernelRepositoryConflict("dependent-BUY coordination CAS failed")

    @staticmethod
    def _verify_k6_worker_cursor(cur: Any, worker_id: str, process_id: str, lease_epoch: int) -> None:
        cur.execute(
            "SELECT incarnation.incarnation_sequence FROM qmt_strategy.execution_kernel_worker_incarnation incarnation "
            "JOIN qmt_strategy.execution_kernel_worker_epoch epoch "
            "ON epoch.worker_id=incarnation.worker_id AND epoch.process_role=incarnation.process_role "
            "AND epoch.incarnation_sequence=incarnation.incarnation_sequence "
            "WHERE incarnation.worker_id=%s AND incarnation.process_incarnation_id=%s "
            "AND incarnation.process_role='PRODUCT_COORDINATOR' FOR SHARE OF incarnation,epoch",
            (worker_id, process_id),
        )
        row = cur.fetchone()
        if row is None:
            raise KernelRepositoryConflict("dependent-BUY decision uses stale worker fencing")
        if type(lease_epoch) is not int or lease_epoch <= 0:
            raise KernelRepositoryConflict("dependent-BUY decision lease epoch is invalid")

    @staticmethod
    def _validate_dependent_buy_decision_evidence_v1(
        *,
        coordination: DependentBuyCoordinationV1,
        decision: DependentBuyReleaseDecisionV1,
        trigger_ref: DependentBuyTriggerEventRefV1,
        ledger_observation: DependentBuyLedgerObservationV1,
    ) -> None:
        if decision.trigger_ref_sha256 != trigger_ref.trigger_ref_sha256:
            raise KernelRepositoryConflict("decision trigger reference differs from durable trigger evidence")
        if decision.ledger_observation_sha256 != ledger_observation.observation_sha256:
            raise KernelRepositoryConflict("decision ledger hash differs from durable ledger evidence")
        if trigger_ref.runtime_id != coordination.runtime_id:
            raise KernelRepositoryConflict("decision trigger runtime differs from coordination")
        if (
            ledger_observation.runtime_id,
            ledger_observation.strategy_id,
            ledger_observation.trade_date,
            ledger_observation.required_cash,
        ) != (
            coordination.runtime_id,
            coordination.strategy_id,
            coordination.trade_date,
            coordination.required_cash,
        ):
            raise KernelRepositoryConflict("decision ledger owner or required cash differs from coordination")
        trigger_time = datetime.fromisoformat(str(trigger_ref.observed_at_utc).replace("Z", "+00:00"))
        ledger_time = datetime.fromisoformat(str(ledger_observation.ledger_as_of_utc).replace("Z", "+00:00"))
        if ledger_time < trigger_time:
            raise KernelRepositoryConflict("decision ledger observation predates trigger evidence")
        settled_trade_refs = tuple(
            sorted(
                ref
                for dependency in coordination.ordered_sell_dependencies
                for ref in dependency.settled_trade_fact_refs
            )
        )
        settled_cash_refs = tuple(
            sorted(
                ref
                for dependency in coordination.ordered_sell_dependencies
                for ref in dependency.settled_cash_ledger_refs
            )
        )
        if (
            ledger_observation.ordered_settled_trade_refs != settled_trade_refs
            or ledger_observation.ordered_cash_ledger_refs != settled_cash_refs
        ):
            raise KernelRepositoryConflict("decision ledger evidence differs from dependency settled facts")

    def read_product_command_authority_set_v2(self, authority_set_sha256: str) -> ProductCommandAuthoritySetV2:
        authority_set_sha256 = _strict_sha256(
            authority_set_sha256,
            field_name="authority_set_sha256",
        )
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_product_command_authority WHERE authority_set_sha256=%s",
                    (authority_set_sha256,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(authority_set_sha256)
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_product_command_authority_item WHERE authority_set_sha256=%s "
                    "ORDER BY effect_ordinal,command_id",
                    (authority_set_sha256,),
                )
                item_rows = cur.fetchall()
        authority = _read_k6_model(
            ProductCommandAuthoritySetV2,
            _row_json(row, "carrier_json"),
            stage="PRODUCT_COMMAND_AUTHORITY_READBACK",
        )
        _assert_scalar_columns(row, _authority_projection(authority), carrier_name="product_command_authority")
        items = tuple(
            _read_k6_model(
                ProductCommandAuthorityItemV2,
                _row_json(item, "carrier_json"),
                stage="PRODUCT_COMMAND_AUTHORITY_ITEM_READBACK",
            )
            for item in item_rows
        )
        for item_row, item in zip(item_rows, items, strict=True):
            _assert_scalar_columns(
                item_row, _authority_item_projection(item), carrier_name="product_command_authority_item"
            )
        if items != authority.ordered_items:
            raise KernelRepositoryConflict("product command authority item readback differs")
        return authority

    def write_product_command_authority_set_v2(
        self, authority: ProductCommandAuthoritySetV2
    ) -> ProductCommandAuthoritySetV2:
        if not isinstance(authority, ProductCommandAuthoritySetV2):
            raise TypeError("authority must be ProductCommandAuthoritySetV2")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_product_command_authority "
                    "WHERE authority_set_sha256=%s FOR SHARE",
                    (authority.authority_set_sha256,),
                )
                existing_row = cur.fetchone()
                if existing_row is not None:
                    existing = _read_k6_model(
                        ProductCommandAuthoritySetV2,
                        _row_json(existing_row, "carrier_json"),
                        stage="PRODUCT_COMMAND_AUTHORITY_IDEMPOTENT_READBACK",
                    )
                    cur.execute(
                        "SELECT carrier_json FROM qmt_strategy.execution_product_command_authority_item "
                        "WHERE authority_set_sha256=%s ORDER BY effect_ordinal,command_id",
                        (authority.authority_set_sha256,),
                    )
                    existing_items = tuple(
                        _read_k6_model(
                            ProductCommandAuthorityItemV2,
                            _row_json(row, "carrier_json"),
                            stage="PRODUCT_COMMAND_AUTHORITY_ITEM_IDEMPOTENT_READBACK",
                        )
                        for row in cur.fetchall()
                    )
                    if existing != authority or existing_items != authority.ordered_items:
                        raise KernelRepositoryConflict(
                            "idempotent product authority retry differs from durable closure"
                        )
                    is_retry = True
                else:
                    is_retry = False
                cur.execute(
                    "SELECT runtime_id,algo_instance_id,event_id,delivery_id,receipt_sha256,execution_projection_set_sha256 "
                    "FROM qmt_strategy.execution_algo_transition WHERE transition_id=%s FOR SHARE",
                    (authority.transition_id,),
                )
                transition = cur.fetchone()
                if transition is None or (
                    transition["runtime_id"],
                    transition["algo_instance_id"],
                    transition["event_id"],
                    transition["delivery_id"],
                ) != (authority.runtime_id, authority.algo_instance_id, authority.event_id, authority.delivery_id):
                    raise KernelRepositoryConflict("product authority owner differs from durable transition")
                if transition["receipt_sha256"] != authority.transition_receipt_sha256:
                    raise KernelRepositoryConflict("product authority transition receipt differs")
                if transition["execution_projection_set_sha256"] != authority.execution_projection_set_sha256:
                    raise KernelRepositoryConflict("product authority projection set differs from durable transition")
                for item in authority.ordered_items:
                    if item.disposition.value != "MATERIALIZE":
                        continue
                    cur.execute(
                        "SELECT child.runtime_id,child.algo_instance_id,child.mapping_id,child.command_id,"
                        "child.child_order_id,child.created_transition_id,outbox.runtime_id AS outbox_runtime_id,"
                        "outbox.algo_instance_id AS outbox_algo_instance_id,outbox.transition_id AS outbox_transition_id,"
                        "outbox.mapping_id AS outbox_mapping_id "
                        "FROM qmt_strategy.execution_child_order child "
                        "JOIN qmt_strategy.execution_algo_command_outbox outbox ON outbox.command_id=%s "
                        "WHERE child.child_order_id=%s FOR SHARE OF child,outbox",
                        (item.outbox_id, item.child_order_id),
                    )
                    closure = cur.fetchone()
                    if closure is None or (
                        closure["runtime_id"],
                        closure["algo_instance_id"],
                        closure["mapping_id"],
                        closure["command_id"],
                        closure["child_order_id"],
                        closure["created_transition_id"],
                        closure["outbox_runtime_id"],
                        closure["outbox_algo_instance_id"],
                        closure["outbox_transition_id"],
                        closure["outbox_mapping_id"],
                    ) != (
                        item.runtime_id,
                        item.algo_instance_id,
                        item.mapping_id,
                        item.command_id,
                        item.child_order_id,
                        item.transition_id,
                        item.runtime_id,
                        item.algo_instance_id,
                        item.transition_id,
                        item.mapping_id,
                    ):
                        raise KernelRepositoryConflict(
                            "materialized product command does not close to one K2 mapping/outbox/child chain"
                        )
                if not is_retry:
                    self._write_product_authority_with_cursor(cur, authority)
        readback = self.read_product_command_authority_set_v2(authority.authority_set_sha256)
        if readback != authority:
            raise KernelRepositoryConflict("product command authority post-commit readback differs")
        return readback

    @staticmethod
    def _write_product_authority_with_cursor(cur: Any, authority: ProductCommandAuthoritySetV2) -> None:
        projection = _authority_projection(authority)
        columns = tuple(projection)
        cur.execute(
            f"INSERT INTO qmt_strategy.execution_product_command_authority({','.join(columns)},carrier_json) "
            f"VALUES ({','.join(['%s'] * (len(columns) + 1))}) ON CONFLICT (authority_set_sha256) DO NOTHING",
            (*projection.values(), _json(authority.model_dump(mode="json"))),
        )
        for item in authority.ordered_items:
            projection_item = _authority_item_projection(item)
            columns = ("authority_set_sha256", *projection_item)
            cur.execute(
                f"INSERT INTO qmt_strategy.execution_product_command_authority_item({','.join(columns)},carrier_json) "
                f"VALUES ({','.join(['%s'] * (len(columns) + 1))}) ON CONFLICT (authority_set_sha256,effect_ordinal,command_id) DO NOTHING",
                (authority.authority_set_sha256, *projection_item.values(), _json(item.model_dump(mode="json"))),
            )

    def _read_product_route_receipt_v1(self, receipt_sha256: str) -> ProductRouteCutoverReceiptV1:
        receipt_sha256 = _strict_sha256(receipt_sha256, field_name="receipt_sha256")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_product_route_cutover WHERE receipt_sha256=%s",
                    (receipt_sha256,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(receipt_sha256)
        receipt = _read_k6_model(
            ProductRouteCutoverReceiptV1,
            _row_json(row, "carrier_json"),
            stage="PRODUCT_ROUTE_RECEIPT_READBACK",
        )
        _assert_scalar_columns(row, _route_projection(receipt), carrier_name="product_route_receipt")
        return receipt

    def read_product_route_owner_v1(
        self, *, runtime_id: str, binding_id: str, trade_date: date | str
    ) -> ProductRouteOwnerV1:
        runtime_id = _strict_identity(runtime_id, field_name="runtime_id")
        binding_id = _strict_identity(binding_id, field_name="binding_id")
        trade_date = _strict_trade_date(trade_date)
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_product_route_owner WHERE runtime_id=%s AND binding_id=%s AND trade_date=%s",
                    (runtime_id, binding_id, trade_date),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError((runtime_id, binding_id, trade_date))
        owner = _read_k6_model(
            ProductRouteOwnerV1,
            _row_json(row, "carrier_json"),
            stage="PRODUCT_ROUTE_OWNER_READBACK",
        )
        _assert_scalar_columns(row, _owner_projection(owner), carrier_name="product route owner")
        receipt = self._read_product_route_receipt_v1(owner.current_receipt_sha256)
        owner.validate_receipt_v1(receipt)
        return owner

    def write_product_route_cutover_v1(
        self, *, receipt: ProductRouteCutoverReceiptV1, owner: ProductRouteOwnerV1
    ) -> ProductRouteOwnerV1:
        if not isinstance(receipt, ProductRouteCutoverReceiptV1) or not isinstance(owner, ProductRouteOwnerV1):
            raise TypeError("receipt and owner must use strict K6 route carriers")
        owner.validate_receipt_v1(receipt)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_product_route_owner "
                    "WHERE runtime_id=%s AND binding_id=%s AND trade_date=%s FOR UPDATE",
                    (owner.runtime_id, owner.binding_id, owner.trade_date),
                )
                previous_row = cur.fetchone()
                previous_owner = (
                    None
                    if previous_row is None
                    else _read_k6_model(
                        ProductRouteOwnerV1,
                        _row_json(previous_row, "carrier_json"),
                        stage="PRODUCT_ROUTE_OWNER_PREDECESSOR_READBACK",
                    )
                )
                is_retry = previous_owner == owner
                if previous_owner is None:
                    if (
                        receipt.route_epoch != 1
                        or owner.row_version != 1
                        or receipt.previous_receipt_sha256 is not None
                    ):
                        raise KernelRepositoryConflict("first product route write requires epoch/version one")
                elif not is_retry:
                    if (
                        owner.row_version != previous_owner.row_version + 1
                        or receipt.route_epoch != previous_owner.current_route_epoch + 1
                    ):
                        raise KernelRepositoryConflict("product route successor is not the exact CAS successor")
                    if receipt.previous_receipt_sha256 != previous_owner.current_receipt_sha256:
                        raise KernelRepositoryConflict("product route predecessor receipt differs")
                    if (
                        previous_owner.route_owner is ProductRouteOwnerKindV1.KERNEL_V2
                        and owner.route_owner is not ProductRouteOwnerKindV1.KERNEL_V2
                    ):
                        raise KernelRepositoryConflict("product route cannot revert from KERNEL_V2")
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_product_route_cutover WHERE receipt_sha256=%s FOR SHARE",
                    (receipt.receipt_sha256,),
                )
                existing_receipt_row = cur.fetchone()
                if existing_receipt_row is not None:
                    existing_receipt = _read_k6_model(
                        ProductRouteCutoverReceiptV1,
                        _row_json(existing_receipt_row, "carrier_json"),
                        stage="PRODUCT_ROUTE_RECEIPT_IDEMPOTENT_READBACK",
                    )
                    _assert_scalar_columns(
                        existing_receipt_row,
                        _route_projection(existing_receipt),
                        carrier_name="product route receipt",
                    )
                    if existing_receipt != receipt:
                        raise KernelRepositoryConflict("product route receipt identity conflicts")
                elif is_retry:
                    raise KernelRepositoryConflict("route owner exists without its immutable receipt")
                route_projection = _route_projection(receipt)
                columns = tuple(route_projection)
                if existing_receipt_row is None:
                    cur.execute(
                        f"INSERT INTO qmt_strategy.execution_product_route_cutover({','.join(columns)},carrier_json) "
                        f"VALUES ({','.join(['%s'] * (len(columns) + 1))})",
                        (*route_projection.values(), _json(receipt.model_dump(mode="json"))),
                    )
                owner_projection = _owner_projection(owner)
                columns = tuple(owner_projection)
                if not is_retry:
                    if previous_owner is None:
                        cur.execute(
                            f"INSERT INTO qmt_strategy.execution_product_route_owner({','.join(columns)},carrier_json) "
                            f"VALUES ({','.join(['%s'] * (len(columns) + 1))})",
                            (*owner_projection.values(), _json(owner.model_dump(mode="json"))),
                        )
                    else:
                        mutable_columns = tuple(
                            key for key in columns if key not in {"runtime_id", "binding_id", "trade_date"}
                        )
                        cur.execute(
                            "UPDATE qmt_strategy.execution_product_route_owner SET "
                            + ",".join(f"{key}=%s" for key in mutable_columns)
                            + ",carrier_json=%s WHERE runtime_id=%s AND binding_id=%s AND trade_date=%s AND row_version=%s",
                            (
                                *(owner_projection[key] for key in mutable_columns),
                                _json(owner.model_dump(mode="json")),
                                owner.runtime_id,
                                owner.binding_id,
                                owner.trade_date,
                                previous_owner.row_version,
                            ),
                        )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("product route owner CAS failed")
        readback = self.read_product_route_owner_v1(
            runtime_id=owner.runtime_id, binding_id=owner.binding_id, trade_date=owner.trade_date
        )
        if readback != owner:
            raise KernelRepositoryConflict("product route owner post-commit readback differs")
        return readback


__all__ = ["KernelProductRepositoryMixin"]
