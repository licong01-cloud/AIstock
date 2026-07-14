"""Read-only PostgreSQL catalog projection for Advisory Phase 1F.

The verifier owns no migration loader and cannot execute DDL.  All queries in
this module target only PostgreSQL catalog relations and the database identity
functions mandated by the release contract; it never scans Advisory or market
business rows.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

from backend.services.advisory_phase1.release_schema_contract import (
    CATALOG_FINGERPRINT_KINDS,
    CatalogDifference,
    CatalogFingerprintEvidence,
    DatabaseIdentity,
    ManagedSchemaStatus,
    MonthPartition,
    NORMALIZER_VERSION,
    PrerequisiteRelationSpec,
    PrerequisiteStatus,
    ReleaseSchemaContract,
    TargetLabel,
    canonical_json_sha256,
    normalize_sql,
    REASON_CONTRACT_INVALID,
    REASON_PREREQUISITE_SCHEMA_DRIFTED,
    REASON_PREREQUISITE_SCHEMA_MISSING,
)


LOGGER = logging.getLogger(__name__)

REASON_ENV_CONFIG_MISSING = "PHASE1F_ENV_CONFIG_MISSING"
REASON_DATABASE_CONNECTION_FAILED = "PHASE1F_DATABASE_CONNECTION_FAILED"
REASON_POSTGRES_VERSION_UNSUPPORTED = "PHASE1F_POSTGRES_VERSION_UNSUPPORTED"
REASON_MANAGED_SCHEMA_MISSING = "PHASE1F_MANAGED_SCHEMA_MISSING"
REASON_MANAGED_SCHEMA_DRIFTED = "PHASE1F_MANAGED_SCHEMA_DRIFTED"
REASON_PARTITION_BOUND_MISMATCH = "PHASE1F_PARTITION_BOUND_MISMATCH"
REASON_READONLY_ASSERTION_FAILED = "PHASE1F_READONLY_ASSERTION_FAILED"


class ReleaseSchemaVerificationError(RuntimeError):
    """Structured verifier failure without connection secrets."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


@dataclass(frozen=True)
class DatabaseConnectionConfig:
    target_label: TargetLabel
    host: str
    port: int
    database: str
    user: str
    password: str
    environment_contract_hash: str

    def connect_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }


def _required_env_keys(target_label: TargetLabel) -> tuple[str, ...]:
    if target_label is TargetLabel.DEV:
        return (
            "TDX_DB_DEV_HOST",
            "TDX_DB_DEV_PORT",
            "TDX_DB_DEV_NAME",
            "TDX_DB_DEV_USER",
            "TDX_DB_DEV_PASSWORD",
        )
    return (
        "TDX_DB_HOST",
        "TDX_DB_PORT",
        "TDX_DB_NAME",
        "TDX_DB_USER",
        "TDX_DB_PASSWORD",
    )


def resolve_database_connection(*, target_label: TargetLabel, env_file: Path) -> DatabaseConnectionConfig:
    """Resolve one target from an explicit .env file with no shell fallback."""

    resolved = env_file.expanduser().resolve()
    if not resolved.is_file():
        raise ReleaseSchemaVerificationError(REASON_ENV_CONFIG_MISSING, "explicit env file is unavailable")
    values = dotenv_values(resolved)
    keys = _required_env_keys(target_label)
    missing = [key for key in keys if not str(values.get(key) or "").strip()]
    if missing:
        raise ReleaseSchemaVerificationError(
            REASON_ENV_CONFIG_MISSING,
            f"missing exact {target_label.value} env keys: {','.join(missing)}",
        )
    port_text = str(values[keys[1]]).strip()
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ReleaseSchemaVerificationError(REASON_ENV_CONFIG_MISSING, f"invalid {keys[1]}") from exc
    if port < 1 or port > 65_535:
        raise ReleaseSchemaVerificationError(REASON_ENV_CONFIG_MISSING, f"invalid {keys[1]}")
    environment_contract_hash = canonical_json_sha256(
        {
            "target_label": target_label.value,
            "host": str(values[keys[0]]).strip(),
            "port": port,
            "database": str(values[keys[2]]).strip(),
            "user": str(values[keys[3]]).strip(),
        }
    )
    return DatabaseConnectionConfig(
        target_label=target_label,
        host=str(values[keys[0]]).strip(),
        port=port,
        database=str(values[keys[2]]).strip(),
        user=str(values[keys[3]]).strip(),
        password=str(values[keys[4]]),
        environment_contract_hash=environment_contract_hash,
    )


@contextmanager
def readonly_catalog_connection(config: DatabaseConnectionConfig) -> Iterator[Any]:
    """Open an isolated repeatable-read catalog-only session."""

    try:
        connection = psycopg2.connect(**config.connect_kwargs())
    except Exception as exc:  # pragma: no cover - driver/environment dependent.
        raise ReleaseSchemaVerificationError(REASON_DATABASE_CONNECTION_FAILED, type(exc).__name__) from exc
    try:
        connection.set_session(readonly=True, autocommit=False, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            row = cursor.fetchone()
        if row is None or str(row[0]).lower() not in {"on", "true"}:
            raise ReleaseSchemaVerificationError(REASON_READONLY_ASSERTION_FAILED, "transaction_read_only is not on")
        yield connection
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


def _rows(cursor: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in cursor.fetchall()]


def _object_id(schema: str, relation: str, name: str, kind: str) -> str:
    return f"{kind}:{schema}.{relation}.{name}"


def _hash_text(value: str | None) -> str:
    if value is None:
        raise ValueError("expected comment/function definition text cannot be null")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalized_function_body(definition: str) -> str:
    """Extract a PL/pgSQL body without erasing expressions or statements."""

    match = re.search(r"\bAS\s+\$(?P<tag>[A-Za-z0-9_]*)\$(?P<body>.*?)\$(?P=tag)\$", definition, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        normalized = normalize_sql(definition)
    else:
        normalized = normalize_sql(match.group("body"))
    if normalized is None:
        raise ReleaseSchemaVerificationError(REASON_CONTRACT_INVALID, "empty function definition")
    return normalized


def _tuple_key(item: Mapping[str, Any], *fields: str) -> tuple[Any, ...]:
    return tuple(item[field] for field in fields)


def _sort_specs(values: Sequence[dict[str, Any]], *fields: str) -> tuple[dict[str, Any], ...]:
    return tuple(
        sorted(
            values,
            key=lambda item: tuple("" if item[field] is None else item[field] for field in fields),
        )
    )


def _is_generated_check_name(relation: str, name: str) -> bool:
    return re.fullmatch(rf"{re.escape(relation)}_check\d*", name) is not None


def _canonicalize_generated_check_names(
    constraints: Sequence[dict[str, Any]], contract: ReleaseSchemaContract
) -> tuple[dict[str, Any], ...]:
    expected_by_semantics = {
        (item.schema, item.relation, item.constraint_type, item.definition): item.name
        for item in contract.required_constraints
        if item.constraint_type == "c" and _is_generated_check_name(item.relation, item.name)
    }
    canonical: list[dict[str, Any]] = []
    claimed_names: set[tuple[str, str, str]] = set()
    for item in constraints:
        value = dict(item)
        key = (value["schema"], value["relation"], value["constraint_type"], value["definition"])
        expected_name = expected_by_semantics.get(key)
        identity = (value["schema"], value["relation"], str(expected_name))
        if expected_name is not None and identity not in claimed_names:
            value["name"] = expected_name
            claimed_names.add(identity)
        canonical.append(value)
    return _sort_specs(canonical, "schema", "relation", "name")


@dataclass(frozen=True)
class CatalogProjection:
    database_identity: DatabaseIdentity
    relations: tuple[dict[str, Any], ...]
    columns: tuple[dict[str, Any], ...]
    constraints: tuple[dict[str, Any], ...]
    indexes: tuple[dict[str, Any], ...]
    functions: tuple[dict[str, Any], ...]
    triggers: tuple[dict[str, Any], ...]
    comments: tuple[dict[str, Any], ...]
    partitions: tuple[dict[str, Any], ...]

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "database_identity": self.database_identity.canonical_payload(),
            "relations": self.relations,
            "columns": self.columns,
            "constraints": self.constraints,
            "indexes": self.indexes,
            "functions": self.functions,
            "triggers": self.triggers,
            "comments": self.comments,
            "partitions": self.partitions,
        }

    @property
    def fingerprint(self) -> str:
        return canonical_json_sha256(self.canonical_payload())


def _identity(cursor: Any, *, config: DatabaseConnectionConfig) -> DatabaseIdentity:
    cursor.execute(
        """
        SELECT current_database() AS current_database,
               host(inet_server_addr()) AS server_address,
               inet_server_port() AS server_port,
               current_setting('server_version_num')::integer AS server_version_num,
               current_user AS current_user
        """
    )
    row = dict(cursor.fetchone())
    return DatabaseIdentity(
        target_label=config.target_label,
        current_database=str(row["current_database"]),
        server_address=str(row["server_address"]) if row["server_address"] is not None else None,
        server_port=int(row["server_port"]),
        server_version_num=int(row["server_version_num"]),
        current_user_hash=hashlib.sha256(str(row["current_user"]).encode("utf-8")).hexdigest(),
        environment_contract_hash=config.environment_contract_hash,
    )


def _relation_names(contract: ReleaseSchemaContract, prerequisite_relations: Sequence[PrerequisiteRelationSpec]) -> tuple[tuple[str, str], ...]:
    values = {(item.schema, item.name) for item in contract.required_relations}
    values.update((item.schema, item.name) for item in prerequisite_relations)
    return tuple(sorted(values))


def project_catalog(
    *,
    connection: Any,
    config: DatabaseConnectionConfig,
    contract: ReleaseSchemaContract,
    expected_partitions: Sequence[MonthPartition],
    prerequisite_relations: Sequence[PrerequisiteRelationSpec] | None = None,
) -> CatalogProjection:
    """Project only contract-addressed catalog objects from an open read-only session."""

    prereqs = tuple(prerequisite_relations or ())
    relation_pairs = _relation_names(contract, prereqs)
    schemas = sorted({schema for schema, _ in relation_pairs})
    names = sorted({name for _, name in relation_pairs})
    function_pairs = tuple(sorted({(item.schema, item.name, item.identity_arguments) for item in contract.required_functions}))
    partition_parent = contract.partition_contract
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        identity = _identity(cursor, config=config)
        cursor.execute(
            """
            SELECT n.nspname AS schema, c.relname AS name, c.relkind, c.relpersistence AS persistence,
                   CASE WHEN pt.partrelid IS NULL THEN NULL ELSE pt.partstrat::text END AS partition_strategy,
                   pg_get_partkeydef(c.oid) AS partition_key,
                   c.relispartition AS is_partition,
                   pg_get_expr(c.relpartbound, c.oid, true) AS partition_bound
              FROM pg_catalog.pg_class c
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              LEFT JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = c.oid
             WHERE n.nspname = ANY(%s)
               AND c.relname = ANY(%s)
            """,
            (schemas, names),
        )
        relations = []
        for row in _rows(cursor):
            relations.append(
                {
                    "schema": row["schema"],
                    "name": row["name"],
                    "relkind": row["relkind"],
                    "persistence": row["persistence"],
                    "partition_strategy": row["partition_strategy"],
                    "partition_key": normalize_sql(row["partition_key"]),
                    "is_partition": bool(row["is_partition"]),
                    "partition_bound": normalize_sql(row["partition_bound"]),
                }
            )
        cursor.execute(
            """
            SELECT n.nspname AS schema, c.relname AS relation, a.attnum AS ordinal, a.attname AS name,
                   format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull AS not_null,
                   pg_get_expr(ad.adbin, ad.adrelid, true) AS default,
                   a.attidentity AS identity, a.attgenerated AS generated
              FROM pg_catalog.pg_attribute a
              JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
             WHERE n.nspname = ANY(%s)
               AND c.relname = ANY(%s)
               AND a.attnum > 0
               AND NOT a.attisdropped
            """,
            (schemas, names),
        )
        columns = [
            {
                "schema": row["schema"],
                "relation": row["relation"],
                "ordinal": int(row["ordinal"]),
                "name": row["name"],
                "data_type": row["data_type"],
                "nullable": not bool(row["not_null"]),
                "default": normalize_sql(row["default"]),
                "identity": row["identity"] or "",
                "generated": row["generated"] or "",
            }
            for row in _rows(cursor)
        ]
        cursor.execute(
            """
            SELECT n.nspname AS schema, c.relname AS relation, con.conname AS name,
                   con.contype AS constraint_type, con.condeferrable AS deferrable,
                   con.condeferred AS initially_deferred, con.convalidated AS validated,
                   pg_get_constraintdef(con.oid, true) AS definition
              FROM pg_catalog.pg_constraint con
              JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = ANY(%s)
               AND c.relname = ANY(%s)
            """,
            (schemas, names),
        )
        constraints = [
            {
                "schema": row["schema"],
                "relation": row["relation"],
                "name": row["name"],
                "constraint_type": row["constraint_type"],
                "deferrable": bool(row["deferrable"]),
                "initially_deferred": bool(row["initially_deferred"]),
                "validated": bool(row["validated"]),
                "definition": normalize_sql(row["definition"]),
            }
            for row in _rows(cursor)
        ]
        cursor.execute(
            """
            SELECT n.nspname AS schema, c.relname AS relation, ic.relname AS name,
                   idx.indisunique AS unique, idx.indisvalid AS valid, idx.indisready AS ready,
                   am.amname AS access_method, pg_get_indexdef(idx.indexrelid) AS definition
              FROM pg_catalog.pg_index idx
              JOIN pg_catalog.pg_class c ON c.oid = idx.indrelid
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_catalog.pg_class ic ON ic.oid = idx.indexrelid
              JOIN pg_catalog.pg_am am ON am.oid = ic.relam
             WHERE n.nspname = ANY(%s)
               AND c.relname = ANY(%s)
            """,
            (schemas, names),
        )
        indexes = [
            {
                "schema": row["schema"],
                "relation": row["relation"],
                "name": row["name"],
                "unique": bool(row["unique"]),
                "valid": bool(row["valid"]),
                "ready": bool(row["ready"]),
                "access_method": row["access_method"],
                "definition": normalize_sql(row["definition"]),
            }
            for row in _rows(cursor)
        ]
        if function_pairs:
            function_schemas = sorted({item[0] for item in function_pairs})
            function_names = sorted({item[1] for item in function_pairs})
            cursor.execute(
                """
                SELECT n.nspname AS schema, p.proname AS name,
                       pg_get_function_identity_arguments(p.oid) AS identity_arguments,
                       format_type(p.prorettype, NULL) AS return_type,
                       l.lanname AS language, p.provolatile::text AS volatility,
                       p.prosecdef AS security_definer, pg_get_functiondef(p.oid) AS definition
                  FROM pg_catalog.pg_proc p
                  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                  JOIN pg_catalog.pg_language l ON l.oid = p.prolang
                 WHERE n.nspname = ANY(%s)
                   AND p.proname = ANY(%s)
                """,
                (function_schemas, function_names),
            )
            functions = []
            allowed_functions = set(function_pairs)
            for row in _rows(cursor):
                identity_arguments = normalize_sql(row["identity_arguments"]) or ""
                function_identity = (row["schema"], row["name"], identity_arguments)
                if function_identity not in allowed_functions:
                    continue
                functions.append(
                    {
                        "schema": row["schema"],
                        "name": row["name"],
                        "identity_arguments": identity_arguments,
                        "return_type": row["return_type"],
                        "language": row["language"],
                        "volatility": row["volatility"],
                        "security_definer": bool(row["security_definer"]),
                        "body_sha256": _hash_text(_normalized_function_body(str(row["definition"]))),
                    }
                )
        else:
            functions = []
        cursor.execute(
            """
            SELECT n.nspname AS schema, c.relname AS relation, t.tgname AS name,
                   t.tgenabled::text AS enabled, t.tgtype::integer AS trigger_type,
                   t.tgconstraint <> 0 AS is_constraint,
                   t.tgdeferrable AS deferrable, t.tginitdeferred AS initially_deferred,
                   fnn.nspname AS function_schema, fn.proname AS function_name,
                   pg_get_function_identity_arguments(fn.oid) AS function_identity_arguments,
                   pg_get_triggerdef(t.oid, true) AS definition
              FROM pg_catalog.pg_trigger t
              JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_catalog.pg_proc fn ON fn.oid = t.tgfoid
              JOIN pg_catalog.pg_namespace fnn ON fnn.oid = fn.pronamespace
             WHERE n.nspname = ANY(%s)
               AND c.relname = ANY(%s)
               AND NOT t.tgisinternal
            """,
            (schemas, names),
        )
        triggers = [
            {
                "schema": row["schema"],
                "relation": row["relation"],
                "name": row["name"],
                "enabled": row["enabled"],
                "trigger_type": int(row["trigger_type"]),
                "is_constraint": bool(row["is_constraint"]),
                "deferrable": bool(row["deferrable"]),
                "initially_deferred": bool(row["initially_deferred"]),
                "function_schema": row["function_schema"],
                "function_name": row["function_name"],
                "function_identity_arguments": normalize_sql(row["function_identity_arguments"]) or "",
                "definition": normalize_sql(row["definition"]) or "",
            }
            for row in _rows(cursor)
        ]
        cursor.execute(
            """
            SELECT n.nspname AS schema, c.relname AS relation,
                   CASE WHEN d.objsubid = 0 THEN NULL ELSE a.attname END AS column,
                   d.description AS text
              FROM pg_catalog.pg_description d
              JOIN pg_catalog.pg_class c ON c.oid = d.objoid
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
             WHERE n.nspname = ANY(%s)
               AND c.relname = ANY(%s)
               AND d.description IS NOT NULL
            """,
            (schemas, names),
        )
        comments = [
            {
                "schema": row["schema"],
                "relation": row["relation"],
                "column": row["column"],
                "text_sha256": _hash_text(row["text"]),
            }
            for row in _rows(cursor)
        ]
        cursor.execute(
            """
            SELECT pn.nspname AS parent_schema, parent.relname AS parent_relation,
                   cn.nspname AS schema, child.relname AS name,
                   pg_get_expr(child.relpartbound, child.oid, true) AS partition_bound
              FROM pg_catalog.pg_inherits inh
              JOIN pg_catalog.pg_class parent ON parent.oid = inh.inhparent
              JOIN pg_catalog.pg_namespace pn ON pn.oid = parent.relnamespace
              JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
              JOIN pg_catalog.pg_namespace cn ON cn.oid = child.relnamespace
             WHERE pn.nspname = %s
               AND parent.relname = %s
            """,
            (partition_parent.schema, partition_parent.parent_relation),
        )
        partitions = [
            {
                "parent_schema": row["parent_schema"],
                "parent_relation": row["parent_relation"],
                "schema": row["schema"],
                "name": row["name"],
                "partition_bound": normalize_sql(row["partition_bound"]),
            }
            for row in _rows(cursor)
        ]
    return CatalogProjection(
        database_identity=identity,
        relations=_sort_specs(relations, "schema", "name"),
        columns=_sort_specs(columns, "schema", "relation", "ordinal", "name"),
        constraints=_sort_specs(constraints, "schema", "relation", "name"),
        indexes=_sort_specs(indexes, "schema", "relation", "name"),
        functions=_sort_specs(functions, "schema", "name", "identity_arguments"),
        triggers=_sort_specs(triggers, "schema", "relation", "name"),
        comments=_sort_specs(comments, "schema", "relation", "column"),
        partitions=_sort_specs(partitions, "schema", "name"),
    )


@dataclass(frozen=True)
class CatalogVerification:
    projection: CatalogProjection
    managed_schema_status: ManagedSchemaStatus
    prerequisite_status: PrerequisiteStatus
    managed_differences: tuple[CatalogDifference, ...]
    prerequisite_differences: tuple[CatalogDifference, ...]

    @property
    def downstream_ready(self) -> bool:
        return self.managed_schema_status is ManagedSchemaStatus.COMPATIBLE and self.prerequisite_status is PrerequisiteStatus.COMPATIBLE


def _expected_partition_payload(partition: MonthPartition) -> dict[str, Any]:
    return {
        "parent_schema": partition.schema,
        "parent_relation": partition.parent_relation,
        "schema": partition.schema,
        "name": partition.name,
        "partition_bound": normalize_sql(
            f"FOR VALUES FROM ('{partition.lower_bound.isoformat()}') TO ('{partition.upper_bound.isoformat()}')"
        ),
    }


def _catalog_fingerprint_evidence(payload: Mapping[str, Sequence[Mapping[str, Any]]]) -> CatalogFingerprintEvidence:
    per_kind_counts = {kind: len(payload[kind]) for kind in CATALOG_FINGERPRINT_KINDS}
    per_kind_hashes = {kind: canonical_json_sha256(payload[kind]) for kind in CATALOG_FINGERPRINT_KINDS}
    return CatalogFingerprintEvidence(
        normalizer_version=NORMALIZER_VERSION,
        total_sha256=canonical_json_sha256(payload),
        object_count=sum(per_kind_counts.values()),
        per_kind_counts=per_kind_counts,
        per_kind_hashes=per_kind_hashes,
    )


def expected_managed_catalog_evidence(
    *, contract: ReleaseSchemaContract, expected_partitions: Sequence[MonthPartition]
) -> CatalogFingerprintEvidence:
    """Describe the desired managed catalog; target identity is separate."""

    relations = []
    for item in contract.required_relations:
        value = _expected_relation_payload(item)
        value.update({"is_partition": False, "partition_bound": None})
        relations.append(value)
    payload = {
        "relations": _sort_specs(relations, "schema", "name"),
        "columns": _sort_specs(
            [_without_fields(_spec_payload(item), "ordinal") for item in contract.required_columns],
            "schema",
            "relation",
            "name",
        ),
        "constraints": _sort_specs([_spec_payload(item) for item in contract.required_constraints], "schema", "relation", "name"),
        "indexes": _sort_specs([_spec_payload(item) for item in contract.required_indexes], "schema", "relation", "name"),
        "functions": _sort_specs([_spec_payload(item) for item in contract.required_functions], "schema", "name", "identity_arguments"),
        "triggers": _sort_specs([_spec_payload(item) for item in contract.required_triggers], "schema", "relation", "name"),
        "comments": _sort_specs([_spec_payload(item) for item in contract.required_comments], "schema", "relation", "column"),
        "partitions": _sort_specs([_expected_partition_payload(item) for item in expected_partitions], "schema", "name"),
    }
    return _catalog_fingerprint_evidence(payload)


def expected_managed_catalog_fingerprint(
    *, contract: ReleaseSchemaContract, expected_partitions: Sequence[MonthPartition]
) -> str:
    return expected_managed_catalog_evidence(contract=contract, expected_partitions=expected_partitions).total_sha256


def observed_managed_catalog_evidence(
    *, projection: CatalogProjection, contract: ReleaseSchemaContract, expected_partitions: Sequence[MonthPartition]
) -> CatalogFingerprintEvidence:
    """Describe the actual managed projection, including semantic extras as drift evidence."""

    managed_relations = {(item.schema, item.name) for item in contract.required_relations}
    expected_partition_names = {item.name for item in expected_partitions}
    payload = {
        "relations": _sort_specs(
            [item for item in projection.relations if (item["schema"], item["name"]) in managed_relations], "schema", "name"
        ),
        "columns": _sort_specs(
            [
                _without_fields(item, "ordinal")
                for item in projection.columns
                if (item["schema"], item["relation"]) in managed_relations
            ],
            "schema",
            "relation",
            "name",
        ),
        "constraints": _sort_specs(
            [item for item in projection.constraints if (item["schema"], item["relation"]) in managed_relations], "schema", "relation", "name"
        ),
        "indexes": _sort_specs(
            [item for item in projection.indexes if (item["schema"], item["relation"]) in managed_relations], "schema", "relation", "name"
        ),
        "functions": _sort_specs(list(projection.functions), "schema", "name", "identity_arguments"),
        "triggers": _sort_specs(
            [item for item in projection.triggers if (item["schema"], item["relation"]) in managed_relations], "schema", "relation", "name"
        ),
        "comments": _sort_specs(
            [item for item in projection.comments if (item["schema"], item["relation"]) in managed_relations], "schema", "relation", "column"
        ),
        "partitions": _sort_specs(
            [
                item
                for item in projection.partitions
                if item["name"] in expected_partition_names or str(item.get("partition_bound") or "").upper().startswith("DEFAULT")
            ],
            "schema",
            "name",
        ),
    }
    return _catalog_fingerprint_evidence(payload)


def observed_managed_catalog_fingerprint(
    *, projection: CatalogProjection, contract: ReleaseSchemaContract, expected_partitions: Sequence[MonthPartition]
) -> str:
    return observed_managed_catalog_evidence(
        projection=projection,
        contract=contract,
        expected_partitions=expected_partitions,
    ).total_sha256


def subset_catalog_fingerprint(
    *, verification: CatalogVerification, object_ids: Sequence[str]
) -> str:
    """Hash only differences relevant to one frozen migration subset."""

    selected = set(object_ids)
    projection = verification.projection
    values = [
        item.model_dump(mode="python")
        for item in verification.managed_differences
        if item.object_id in selected
    ]
    payload = {
        "relations": [
            item for item in projection.relations if f"relation:{item['schema']}.{item['name']}" in selected
        ],
        "columns": [
            _without_fields(item, "ordinal")
            for item in projection.columns
            if f"column:{item['schema']}.{item['relation']}.{item['name']}" in selected
        ],
        "constraints": [
            item for item in projection.constraints if f"constraint:{item['schema']}.{item['relation']}.{item['name']}" in selected
        ],
        "indexes": [
            item for item in projection.indexes if f"index:{item['schema']}.{item['relation']}.{item['name']}" in selected
        ],
        "functions": [
            item
            for item in projection.functions
            if f"function:{item['schema']}.{item['name']}({item['identity_arguments']})" in selected
        ],
        "triggers": [
            item for item in projection.triggers if f"trigger:{item['schema']}.{item['relation']}.{item['name']}" in selected
        ],
        "comments": [
            item
            for item in projection.comments
            if f"comment:{item['schema']}.{item['relation']}.{item['column'] if item['column'] is not None else '__table__'}" in selected
        ],
        "differences": sorted(values, key=lambda item: item["object_id"]),
    }
    return canonical_json_sha256(payload)


def _spec_payload(value: Any) -> dict[str, Any]:
    return value.model_dump(mode="python", exclude={"repairable_by_orders"})


def _without_fields(value: Mapping[str, Any], *fields: str) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key not in fields}


def _index_actual(values: Sequence[Mapping[str, Any]], *fields: str) -> dict[tuple[Any, ...], dict[str, Any]]:
    return {_tuple_key(item, *fields): dict(item) for item in values}


def _compare_specs(
    *,
    expected: Sequence[Any],
    actual: Sequence[Mapping[str, Any]],
    fields: tuple[str, ...],
    kind: str,
    reason_missing: str,
    reason_drifted: str,
    include_unexpected: bool,
    ignored_payload_fields: tuple[str, ...] = (),
    repairable_drift_variants: Mapping[tuple[str, str], tuple[int, ...]] | None = None,
) -> list[CatalogDifference]:
    actual_by_key = _index_actual(actual, *fields)
    expected_by_key = {_tuple_key(_spec_payload(item), *fields): item for item in expected}
    differences: list[CatalogDifference] = []
    for key, expected_item in expected_by_key.items():
        expected_payload = _without_fields(_spec_payload(expected_item), *ignored_payload_fields)
        observed = actual_by_key.get(key)
        if observed is None:
            differences.append(
                CatalogDifference(
                    object_id=expected_item.object_id,
                    category="MISSING",
                    expected=expected_payload,
                    actual=None,
                    reason_code=reason_missing,
                    repairable_by_orders=expected_item.repairable_by_orders,
                )
            )
        elif {name: observed.get(name) for name in expected_payload} != expected_payload:
            actual_payload = {name: observed.get(name) for name in expected_payload}
            variant_orders = (repairable_drift_variants or {}).get(
                (expected_item.object_id, canonical_json_sha256(actual_payload)),
                (),
            )
            differences.append(
                CatalogDifference(
                    object_id=expected_item.object_id,
                    category="DRIFTED",
                    expected=expected_payload,
                    actual=actual_payload,
                    reason_code=reason_drifted,
                    repairable_by_orders=variant_orders,
                )
            )
    if include_unexpected:
        for key, observed in actual_by_key.items():
            if key not in expected_by_key:
                identity = ".".join(str(item) for item in key)
                differences.append(
                    CatalogDifference(
                        object_id=f"{kind}:{identity}",
                        category="UNEXPECTED",
                        expected=None,
                        actual=dict(observed),
                        reason_code=reason_drifted,
                    )
                )
    return differences


def _expected_relation_payload(spec: Any) -> dict[str, Any]:
    payload = {
        "schema": spec.schema,
        "name": spec.name,
        "relkind": spec.relkind,
    }
    if hasattr(spec, "persistence"):
        payload.update(
            {
                "persistence": spec.persistence,
                "partition_strategy": spec.partition_strategy,
                "partition_key": spec.partition_key,
            }
        )
    return payload


def _compare_relations(
    *, expected: Sequence[Any], actual: Sequence[Mapping[str, Any]], reason_missing: str, reason_drifted: str
) -> list[CatalogDifference]:
    actual_by_key = _index_actual(actual, "schema", "name")
    differences: list[CatalogDifference] = []
    for item in expected:
        expected_payload = _expected_relation_payload(item)
        observed = actual_by_key.get((item.schema, item.name))
        if observed is None:
            differences.append(
                CatalogDifference(
                    object_id=item.object_id,
                    category="MISSING",
                    expected=expected_payload,
                    reason_code=reason_missing,
                    repairable_by_orders=getattr(item, "repairable_by_orders", ()),
                )
            )
        else:
            actual_payload = {name: observed.get(name) for name in expected_payload}
            if actual_payload != expected_payload:
                differences.append(
                    CatalogDifference(
                        object_id=item.object_id,
                        category="DRIFTED",
                        expected=expected_payload,
                        actual=actual_payload,
                        reason_code=reason_drifted,
                    )
                )
    return differences


def _compare_partitions(
    *, contract: ReleaseSchemaContract, expected_partitions: Sequence[MonthPartition], actual: Sequence[Mapping[str, Any]]
) -> list[CatalogDifference]:
    expected_by_name = {item.name: item for item in expected_partitions}
    actual_by_name = {str(item["name"]): dict(item) for item in actual}
    differences: list[CatalogDifference] = []
    for name, item in expected_by_name.items():
        expected_bound = normalize_sql(
            f"FOR VALUES FROM ('{item.lower_bound.isoformat()}') TO ('{item.upper_bound.isoformat()}')"
        )
        expected_payload = {
            "parent_schema": item.schema,
            "parent_relation": item.parent_relation,
            "schema": item.schema,
            "name": name,
            "partition_bound": expected_bound,
        }
        observed = actual_by_name.get(name)
        if observed is None:
            differences.append(
                CatalogDifference(
                    object_id=item.object_id,
                    category="MISSING",
                    expected=expected_payload,
                    reason_code=REASON_MANAGED_SCHEMA_MISSING,
                    repairable_by_orders=contract.partition_contract.repairable_by_orders,
                )
            )
        elif {key: observed.get(key) for key in expected_payload} != expected_payload:
            differences.append(
                CatalogDifference(
                    object_id=item.object_id,
                    category="DRIFTED",
                    expected=expected_payload,
                    actual={key: observed.get(key) for key in expected_payload},
                    reason_code=REASON_PARTITION_BOUND_MISMATCH,
                )
            )
    for observed in actual:
        bound = str(observed.get("partition_bound") or "")
        if bound.upper().startswith("DEFAULT"):
            differences.append(
                CatalogDifference(
                    object_id=f"partition:{observed['schema']}.{observed['name']}",
                    category="DRIFTED",
                    actual=dict(observed),
                    reason_code=REASON_MANAGED_SCHEMA_DRIFTED,
                )
            )
    return differences


def _prerequisite_differences(
    *, contract: ReleaseSchemaContract, projection: CatalogProjection
) -> list[CatalogDifference]:
    expected_relations = tuple(contract.phase0a_prerequisite_relations) + tuple(contract.external_readonly_prerequisite_relations)
    differences = _compare_relations(
        expected=expected_relations,
        actual=projection.relations,
        reason_missing=REASON_PREREQUISITE_SCHEMA_MISSING,
        reason_drifted=REASON_PREREQUISITE_SCHEMA_DRIFTED,
    )
    columns = tuple(column for relation in expected_relations for column in relation.columns)
    constraints = tuple(constraint for relation in expected_relations for constraint in relation.constraints)
    differences.extend(
        _compare_specs(
            expected=columns,
            actual=projection.columns,
            fields=("schema", "relation", "name"),
            kind="column",
            reason_missing=REASON_PREREQUISITE_SCHEMA_MISSING,
            reason_drifted=REASON_PREREQUISITE_SCHEMA_DRIFTED,
            include_unexpected=False,
            ignored_payload_fields=("ordinal",),
        )
    )
    differences.extend(
        _compare_specs(
            expected=constraints,
            actual=projection.constraints,
            fields=("schema", "relation", "name"),
            kind="constraint",
            reason_missing=REASON_PREREQUISITE_SCHEMA_MISSING,
            reason_drifted=REASON_PREREQUISITE_SCHEMA_DRIFTED,
            include_unexpected=False,
        )
    )
    return differences


def verify_catalog(
    *,
    connection: Any,
    config: DatabaseConnectionConfig,
    contract: ReleaseSchemaContract,
    expected_partitions: Sequence[MonthPartition],
) -> CatalogVerification:
    """Perform complete catalog validation without reads from business tables."""

    projection = project_catalog(
        connection=connection,
        config=config,
        contract=contract,
        expected_partitions=expected_partitions,
        prerequisite_relations=tuple(contract.phase0a_prerequisite_relations) + tuple(contract.external_readonly_prerequisite_relations),
    )
    projection = CatalogProjection(
        database_identity=projection.database_identity,
        relations=projection.relations,
        columns=projection.columns,
        constraints=_canonicalize_generated_check_names(projection.constraints, contract),
        indexes=projection.indexes,
        functions=projection.functions,
        triggers=projection.triggers,
        comments=projection.comments,
        partitions=projection.partitions,
    )
    repairable_drift_variants = {
        (item.object_id, item.actual_payload_sha256): item.repairable_by_orders
        for item in contract.repairable_drift_variants
    }
    major = projection.database_identity.server_version_num // 10_000
    if major not in contract.supported_postgres_major_versions:
        unsupported = CatalogDifference(
            object_id="postgres:server_version",
            category="UNSUPPORTED",
            expected={"supported_postgres_major_versions": list(contract.supported_postgres_major_versions)},
            actual={"server_version_num": projection.database_identity.server_version_num},
            reason_code=REASON_POSTGRES_VERSION_UNSUPPORTED,
        )
        return CatalogVerification(
            projection=projection,
            managed_schema_status=ManagedSchemaStatus.UNSUPPORTED,
            prerequisite_status=PrerequisiteStatus.UNSUPPORTED,
            managed_differences=(unsupported,),
            prerequisite_differences=(unsupported,),
        )
    managed: list[CatalogDifference] = []
    managed_relations = {(item.schema, item.name) for item in contract.required_relations}
    managed_columns = tuple(
        item for item in projection.columns if (item["schema"], item["relation"]) in managed_relations
    )
    managed_constraints = tuple(
        item for item in projection.constraints if (item["schema"], item["relation"]) in managed_relations
    )
    managed_indexes = tuple(
        item for item in projection.indexes if (item["schema"], item["relation"]) in managed_relations
    )
    managed_triggers = tuple(
        item for item in projection.triggers if (item["schema"], item["relation"]) in managed_relations
    )
    managed_comments = tuple(
        item for item in projection.comments if (item["schema"], item["relation"]) in managed_relations
    )
    managed.extend(
        _compare_relations(
            expected=contract.required_relations,
            actual=projection.relations,
            reason_missing=REASON_MANAGED_SCHEMA_MISSING,
            reason_drifted=REASON_MANAGED_SCHEMA_DRIFTED,
        )
    )
    managed.extend(
        _compare_specs(
            expected=contract.required_columns,
            actual=managed_columns,
            fields=("schema", "relation", "name"),
            kind="column",
            reason_missing=REASON_MANAGED_SCHEMA_MISSING,
            reason_drifted=REASON_MANAGED_SCHEMA_DRIFTED,
            include_unexpected=True,
            ignored_payload_fields=("ordinal",),
            repairable_drift_variants=repairable_drift_variants,
        )
    )
    managed.extend(
        _compare_specs(
            expected=contract.required_constraints,
            actual=managed_constraints,
            fields=("schema", "relation", "name"),
            kind="constraint",
            reason_missing=REASON_MANAGED_SCHEMA_MISSING,
            reason_drifted=REASON_MANAGED_SCHEMA_DRIFTED,
            include_unexpected=True,
            repairable_drift_variants=repairable_drift_variants,
        )
    )
    managed.extend(
        _compare_specs(
            expected=contract.required_indexes,
            actual=managed_indexes,
            fields=("schema", "relation", "name"),
            kind="index",
            reason_missing=REASON_MANAGED_SCHEMA_MISSING,
            reason_drifted=REASON_MANAGED_SCHEMA_DRIFTED,
            include_unexpected=True,
            repairable_drift_variants=repairable_drift_variants,
        )
    )
    managed.extend(
        _compare_specs(
            expected=contract.required_functions,
            actual=projection.functions,
            fields=("schema", "name", "identity_arguments"),
            kind="function",
            reason_missing=REASON_MANAGED_SCHEMA_MISSING,
            reason_drifted=REASON_MANAGED_SCHEMA_DRIFTED,
            include_unexpected=False,
            repairable_drift_variants=repairable_drift_variants,
        )
    )
    managed.extend(
        _compare_specs(
            expected=contract.required_triggers,
            actual=managed_triggers,
            fields=("schema", "relation", "name"),
            kind="trigger",
            reason_missing=REASON_MANAGED_SCHEMA_MISSING,
            reason_drifted=REASON_MANAGED_SCHEMA_DRIFTED,
            include_unexpected=True,
            repairable_drift_variants=repairable_drift_variants,
        )
    )
    managed.extend(
        _compare_specs(
            expected=contract.required_comments,
            actual=managed_comments,
            fields=("schema", "relation", "column"),
            kind="comment",
            reason_missing=REASON_MANAGED_SCHEMA_MISSING,
            reason_drifted=REASON_MANAGED_SCHEMA_DRIFTED,
            include_unexpected=False,
            repairable_drift_variants=repairable_drift_variants,
        )
    )
    managed.extend(_compare_partitions(contract=contract, expected_partitions=expected_partitions, actual=projection.partitions))
    missing_relation_orders = {
        item.object_id.removeprefix("relation:"): item.repairable_by_orders
        for item in managed
        if item.category == "MISSING" and item.object_id.startswith("relation:")
    }
    repaired_children: list[CatalogDifference] = []
    for item in managed:
        relation_key: str | None = None
        parts = item.object_id.split(":", 1)
        if len(parts) == 2 and parts[0] in {"column", "constraint", "index", "trigger", "comment"}:
            relation_parts = parts[1].split(".")
            if len(relation_parts) >= 2:
                relation_key = ".".join(relation_parts[:2])
        if item.category == "MISSING" and relation_key in missing_relation_orders:
            repaired_children.append(item.model_copy(update={"repairable_by_orders": missing_relation_orders[relation_key]}))
        else:
            repaired_children.append(item)
    managed = repaired_children
    managed = sorted(managed, key=lambda item: (item.category, item.object_id))
    existing_managed_relations = {
        (item["schema"], item["name"])
        for item in projection.relations
        if (item["schema"], item["name"]) in {(spec.schema, spec.name) for spec in contract.required_relations}
    }
    if not managed:
        managed_status = ManagedSchemaStatus.COMPATIBLE
    elif not existing_managed_relations:
        managed_status = ManagedSchemaStatus.ABSENT
    elif all(item.category in {"MISSING", "DRIFTED"} and item.repairable_by_orders for item in managed):
        managed_status = ManagedSchemaStatus.PARTIAL_ADDITIVE
    else:
        managed_status = ManagedSchemaStatus.DRIFTED
    prereq = sorted(_prerequisite_differences(contract=contract, projection=projection), key=lambda item: (item.category, item.object_id))
    if not prereq:
        prerequisite_status = PrerequisiteStatus.COMPATIBLE
    elif any(item.category == "DRIFTED" for item in prereq):
        prerequisite_status = PrerequisiteStatus.DRIFTED
    else:
        prerequisite_status = PrerequisiteStatus.MISSING
    return CatalogVerification(
        projection=projection,
        managed_schema_status=managed_status,
        prerequisite_status=prerequisite_status,
        managed_differences=tuple(managed),
        prerequisite_differences=tuple(prereq),
    )


def verify_database_catalog(
    *, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract, expected_partitions: Sequence[MonthPartition]
) -> CatalogVerification:
    with readonly_catalog_connection(config) as connection:
        return verify_catalog(connection=connection, config=config, contract=contract, expected_partitions=expected_partitions)
