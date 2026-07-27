"""Code-owned K2 catalog authority and independent PostgreSQL preflight."""

from __future__ import annotations

import psycopg2
import psycopg2.extras

from .kernel_repository_common import KernelRepositorySchemaError


_K2_SCHEMA_CATALOG_SHA256 = "4c613f119a828c7ce3d1a9bac92113b803c93455802fb5b71b8a7ca2ac2743a5"

_K2_CATALOG_QUERY = """
WITH target_tables(relname) AS (
    VALUES
        ('execution_kernel_worker_epoch'),
        ('execution_kernel_worker_incarnation'),
        ('execution_algo_event_delivery'),
        ('execution_algo_transition'),
        ('execution_algo_command_outbox'),
        ('execution_algo_command_dispatch_attempt'),
        ('execution_algo_timer_schedule'),
        ('execution_algo_timer_occurrence'),
        ('execution_exchange_session_authority'),
        ('execution_algo_diagnostic_observation')
), additive_columns(relname,attname) AS (
    VALUES
        ('execution_runtime','runtime_id'),
        ('execution_runtime','trade_date'),
        ('execution_runtime_event','event_contract_version'),
        ('execution_runtime_event','event_schema_version'),
        ('execution_runtime_event','payload_schema_version'),
        ('execution_runtime_event','event_key_sha256'),
        ('execution_runtime_event','payload_sha256'),
        ('execution_runtime_event','observed_at_utc'),
        ('execution_runtime_event','logical_at_utc'),
        ('execution_runtime_event','source_identity_json'),
        ('execution_runtime_event','correlation_json'),
        ('execution_runtime_event','ingress_receipt_json'),
        ('execution_runtime_event','ingress_receipt_sha256'),
        ('execution_runtime_event','routing_rule_version'),
        ('execution_runtime_event','transaction_commit_identity'),
        ('execution_algo_instance','kernel_contract_version'),
        ('execution_algo_instance','traded_quantity'),
        ('execution_algo_instance','plugin_id'),
        ('execution_algo_instance','plugin_version'),
        ('execution_algo_instance','plugin_manifest_sha256'),
        ('execution_algo_instance','plugin_config_json'),
        ('execution_algo_instance','plugin_config_sha256'),
        ('execution_algo_instance','compatibility_receipt_sha256'),
        ('execution_algo_instance','state_schema_version'),
        ('execution_algo_instance','state_json'),
        ('execution_algo_instance','state_sha256'),
        ('execution_algo_instance','transition_sequence'),
        ('execution_algo_instance','last_applied_delivery_sequence'),
        ('execution_algo_instance','last_applied_delivery_id'),
        ('execution_algo_instance','last_closed_delivery_sequence'),
        ('execution_algo_instance','terminal_delivery_sequence'),
        ('execution_algo_instance','failure_receipt_id'),
        ('execution_algo_instance','active_child_closure_status'),
        ('execution_algo_instance','active_child_count'),
        ('execution_algo_instance','row_version'),
        ('execution_algo_instance','terminal_at_utc'),
        ('execution_algo_instance','kernel_carrier_json'),
        ('execution_child_order','kernel_contract_version'),
        ('execution_child_order','mapping_id'),
        ('execution_child_order','command_id'),
        ('execution_child_order','local_vt_orderid'),
        ('execution_child_order','deterministic_client_order_ref'),
        ('execution_child_order','order_remark'),
        ('execution_child_order','mapping_status'),
        ('execution_child_order','mapping_version'),
        ('execution_child_order','mapping_payload_sha256'),
        ('execution_child_order','mapping_receipt_sha256'),
        ('execution_child_order','broker_identity_source_event_id'),
        ('execution_child_order','last_order_event_id'),
        ('execution_child_order','last_trade_event_id'),
        ('execution_child_order','created_transition_id'),
        ('execution_child_order','updated_by_event_id'),
        ('execution_child_order','mapping_created_at_utc'),
        ('execution_child_order','mapping_updated_at_utc'),
        ('execution_child_order','mapping_json')
), catalog_items(sort_key,item) AS (
    SELECT
        format('column:%s:%05s', table_class.relname, attribute.attnum),
        jsonb_build_array(
            'column', table_class.relname, attribute.attname,
            format_type(attribute.atttypid, attribute.atttypmod),
            attribute.attnotnull,
            coalesce(pg_get_expr(attribute_default.adbin, attribute_default.adrelid), '')
        )
    FROM pg_class AS table_class
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    JOIN pg_attribute AS attribute
      ON attribute.attrelid=table_class.oid AND attribute.attnum > 0 AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef AS attribute_default
      ON attribute_default.adrelid=table_class.oid AND attribute_default.adnum=attribute.attnum
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR (table_class.relname,attribute.attname) IN (SELECT relname,attname FROM additive_columns)
      )

    UNION ALL

    SELECT
        format('constraint:%s:%s', table_class.relname, constraint_record.conname),
        jsonb_build_array(
            'constraint', table_class.relname, constraint_record.conname,
            constraint_record.contype, constraint_record.condeferrable,
            constraint_record.condeferred, constraint_record.convalidated,
            replace(
                pg_get_constraintdef(constraint_record.oid, true),
                table_schema.nspname || '.', '<schema>.'
            )
        )
    FROM pg_constraint AS constraint_record
    JOIN pg_class AS table_class ON table_class.oid=constraint_record.conrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR constraint_record.conname LIKE '%miniqmt_k2%'
      )

    UNION ALL

    SELECT
        format('index:%s:%s', table_class.relname, index_class.relname),
        jsonb_build_array(
            'index', table_class.relname, index_class.relname,
            index_record.indisunique, index_record.indisprimary,
            index_record.indisvalid, index_record.indisready,
            replace(
                pg_get_indexdef(index_record.indexrelid, 0, true),
                table_schema.nspname || '.', '<schema>.'
            ),
            coalesce(
                replace(
                    pg_get_expr(index_record.indpred, index_record.indrelid, true),
                    table_schema.nspname || '.', '<schema>.'
                ),
                ''
            )
        )
    FROM pg_index AS index_record
    JOIN pg_class AS table_class ON table_class.oid=index_record.indrelid
    JOIN pg_class AS index_class ON index_class.oid=index_record.indexrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR index_class.relname LIKE '%miniqmt_k2%'
      )
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key), '[]'::jsonb)::TEXT AS payload
    FROM catalog_items
)
SELECT encode(sha256(convert_to(payload, 'UTF8')), 'hex')
FROM canonical_catalog
""".strip()


class KernelRepositorySchemaMixin:
    """Validate helper definition and independently recompute the catalog fingerprint."""

    def preflight_schema(self) -> dict[str, bool]:
        required = (
            "execution_algo_event_delivery",
            "execution_algo_transition",
            "execution_algo_command_outbox",
            "execution_algo_command_dispatch_attempt",
            "execution_algo_timer_schedule",
            "execution_algo_timer_occurrence",
            "execution_kernel_worker_epoch",
            "execution_kernel_worker_incarnation",
            "execution_exchange_session_authority",
            "execution_algo_diagnostic_observation",
        )
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT relname, to_regclass('qmt_strategy.' || relname) IS NOT NULL AS present
                    FROM unnest(%s::text[]) AS relname
                    ORDER BY relname
                    """,
                    (list(required),),
                )
                result = {str(row["relname"]): bool(row["present"]) for row in cur.fetchall()}
        if not all(result.values()):
            raise KernelRepositorySchemaError(f"K2 schema is incomplete: {result}")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                try:
                    cur.execute(
                        """
                        SELECT schema_name,language_name,volatility,arguments,result_type,function_body
                        FROM (
                            SELECT namespace.nspname AS schema_name,language.lanname AS language_name,
                                   function_record.provolatile AS volatility,
                                   pg_get_function_arguments(function_record.oid) AS arguments,
                                   pg_get_function_result(function_record.oid) AS result_type,
                                   function_record.prosrc AS function_body
                            FROM pg_proc AS function_record
                            JOIN pg_namespace AS namespace ON namespace.oid=function_record.pronamespace
                            JOIN pg_language AS language ON language.oid=function_record.prolang
                            WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()')
                        ) AS function_authority
                        """
                    )
                    function_row = cur.fetchone()
                    if function_row is None:
                        raise KernelRepositorySchemaError("K2 schema fingerprint authority is unavailable")
                    normalized_body = (
                        str(function_row["function_body"])
                        .replace(str(function_row["schema_name"]), "qmt_strategy")
                        .strip()
                        .rstrip(";")
                    )
                    if (
                        function_row["language_name"] != "sql"
                        or function_row["volatility"] != "s"
                        or function_row["arguments"] != ""
                        or function_row["result_type"] != "text"
                        or normalized_body != _K2_CATALOG_QUERY.strip().rstrip(";")
                    ):
                        raise KernelRepositorySchemaError("K2 catalog function drift")
                    cur.execute(f"SELECT * FROM ({_K2_CATALOG_QUERY}) AS catalog(catalog_sha256)")
                    catalog_sha256 = str(cur.fetchone()["catalog_sha256"])
                except KernelRepositorySchemaError:
                    raise
                except psycopg2.Error as exc:
                    raise KernelRepositorySchemaError("K2 schema fingerprint authority is unavailable") from exc
        if catalog_sha256 != _K2_SCHEMA_CATALOG_SHA256:
            raise KernelRepositorySchemaError(
                f"K2 schema catalog drift: expected {_K2_SCHEMA_CATALOG_SHA256}, got {catalog_sha256}"
            )
        result["schema_catalog_fingerprint"] = True
        return result
