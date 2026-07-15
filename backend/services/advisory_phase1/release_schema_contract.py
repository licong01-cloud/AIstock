"""Typed, repository-owned contract for the Advisory Phase 1F release schema.

This module is deliberately pure.  It does not import PostgreSQL drivers,
migration files, Selection, Paper, simulation, or recommendation services.
The read-only verifier and the DDL executor consume this contract from their
own modules so later Advisory workers cannot accidentally acquire DDL access
through an import.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, time, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


CONTRACT_SCHEMA_VERSION = "advisory_phase1f_release_schema_contract_v1"
CONTRACT_SCHEMA_VERSION_V2 = "advisory_phase1f_release_schema_contract_v2"
PLAN_REQUEST_SCHEMA_VERSION = "advisory_phase1f_release_plan_request_v1"
PLAN_SCHEMA_VERSION = "advisory_phase1f_release_plan_v1"
PLAN_SCHEMA_VERSION_V2 = "advisory_phase1f_release_plan_v2"
RECEIPT_SCHEMA_VERSION = "advisory_phase1f_release_receipt_v1"
RECEIPT_SCHEMA_VERSION_V2 = "advisory_phase1f_release_receipt_v2"
NORMALIZER_VERSION = "advisory_phase1f_catalog_normalizer_v1"

REASON_CONTRACT_INVALID = "PHASE1F_CONTRACT_INVALID"
REASON_CONTRACT_HASH_MISMATCH = "PHASE1F_CONTRACT_HASH_MISMATCH"
REASON_REQUEST_INVALID = "PHASE1F_REQUEST_INVALID"
REASON_DATABASE_IDENTITY_MISMATCH = "PHASE1F_DATABASE_IDENTITY_MISMATCH"
REASON_PARTITION_RANGE_INVALID = "PHASE1F_PARTITION_RANGE_INVALID"
REASON_PREREQUISITE_SCHEMA_MISSING = "PHASE1F_PREREQUISITE_SCHEMA_MISSING"
REASON_PREREQUISITE_SCHEMA_DRIFTED = "PHASE1F_PREREQUISITE_SCHEMA_DRIFTED"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class ReleaseSchemaContractError(ValueError):
    """Raised when a frozen release contract or typed request is invalid."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


def canonicalize(value: Any, *, key: str | None = None) -> Any:
    """Convert supported values into a deterministic JSON-safe payload."""

    del key  # The Phase 1F catalog contract has no field-specific rounding.
    if isinstance(value, BaseModel):
        return canonicalize(value.model_dump(mode="python", exclude_none=False))
    if isinstance(value, Enum):
        return canonicalize(value.value)
    if isinstance(value, (datetime,)):
        normalized = value.astimezone(timezone.utc) if value.tzinfo is not None else value
        return normalized.isoformat(timespec="microseconds")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(item_key): canonicalize(item_value)
            for item_key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (tuple, list)):
        return [canonicalize(item) for item in value]
    if isinstance(value, set):
        return sorted((canonicalize(item) for item in value), key=canonical_json_text)
    if value is None or isinstance(value, (str, int, bool, float)):
        return value
    raise ReleaseSchemaContractError(REASON_CONTRACT_INVALID, f"unsupported canonical value {type(value).__name__}")


def canonical_json_text(payload: Any) -> str:
    return json.dumps(canonicalize(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def canonical_json_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json_text(payload).encode("utf-8")).hexdigest()


def normalize_sql(value: str | None) -> str | None:
    """Collapse display whitespace without changing quoted SQL content."""

    if value is None:
        return None
    source = value.strip()
    output: list[str] = []
    index = 0
    pending_space = False
    quote: str | None = None
    dollar_tag: str | None = None
    while index < len(source):
        char = source[index]
        if dollar_tag is not None:
            if source.startswith(dollar_tag, index):
                output.append(dollar_tag)
                index += len(dollar_tag)
                dollar_tag = None
            else:
                output.append(char)
                index += 1
            continue
        if quote is not None:
            output.append(char)
            index += 1
            if char == "\\" and quote == "'" and index < len(source):
                output.append(source[index])
                index += 1
            elif char == quote:
                if index < len(source) and source[index] == quote:
                    output.append(source[index])
                    index += 1
                else:
                    quote = None
            continue
        if char.isspace():
            pending_space = bool(output)
            index += 1
            continue
        if pending_space:
            output.append(" ")
            pending_space = False
        if char in {"'", '"'}:
            quote = char
            output.append(char)
            index += 1
            continue
        if char == "$":
            match = re.match(r"\$[A-Za-z0-9_]*\$", source[index:])
            if match is not None:
                dollar_tag = match.group(0)
                output.append(dollar_tag)
                index += len(dollar_tag)
                continue
        output.append(char)
        index += 1
    return "".join(output)


def _sha256(value: str, *, field_name: str) -> str:
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _identifier(value: str, *, field_name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase PostgreSQL identifier")
    return value


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True, serialize_by_alias=True)


class SchemaScopedModel(StrictModel):
    """Use ``schema`` in JSON without shadowing Pydantic's legacy method."""

    schema_name: str = Field(alias="schema")

    @property
    def schema(self) -> str:
        return self.schema_name


class DdlSessionPolicy(StrictModel):
    lock_timeout_ms: int = Field(ge=1)
    statement_timeout_ms: int = Field(ge=1)
    automatic_retry: Literal[False]

    @model_validator(mode="after")
    def _verify_v1_policy(self) -> "DdlSessionPolicy":
        if self.lock_timeout_ms != 10_000 or self.statement_timeout_ms != 900_000:
            raise ValueError("Phase 1F v1 DDL policy is frozen at 10000ms/900000ms")
        return self

    @property
    def policy_hash(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="python"))


class TransactionMode(str, Enum):
    FILE_WRAPPED = "FILE_WRAPPED"
    EXECUTOR_MANAGED = "EXECUTOR_MANAGED"


class ExecutorAction(str, Enum):
    """Typed executor behavior bound to one frozen migration entry."""

    SQL_FILE = "SQL_FILE"
    CREATE_PARTITIONS = "CREATE_PARTITIONS"
    CUTOVER = "CUTOVER"


class ManagedMigration(StrictModel):
    order: int = Field(ge=1)
    relative_path: str | None = None
    file_sha256: str | None = None
    depends_on_orders: tuple[int, ...] = ()
    transaction_group: str = Field(min_length=1)
    transaction_mode: TransactionMode
    declared_object_ids: tuple[str, ...] = Field(min_length=1)
    executor_action: ExecutorAction = ExecutorAction.SQL_FILE
    partition_parent_relations: tuple[str, ...] = ()

    @field_validator("file_sha256")
    @classmethod
    def _validate_file_hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="file_sha256") if value is not None else None

    @field_validator("relative_path")
    @classmethod
    def _validate_relative_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = Path(value)
        if path.is_absolute() or ".." in path.parts or path.suffix != ".sql":
            raise ValueError("relative_path must be a repository-relative .sql path")
        return value.replace("\\", "/")

    @model_validator(mode="after")
    def _validate_dependencies(self) -> "ManagedMigration":
        if tuple(sorted(set(self.depends_on_orders))) != self.depends_on_orders:
            raise ValueError("depends_on_orders must be sorted and duplicate-free")
        if any(value >= self.order for value in self.depends_on_orders):
            raise ValueError("migration dependencies must precede the migration order")
        if tuple(sorted(set(self.declared_object_ids))) != self.declared_object_ids:
            raise ValueError("declared_object_ids must be sorted and duplicate-free")
        if tuple(sorted(set(self.partition_parent_relations))) != self.partition_parent_relations:
            raise ValueError("partition_parent_relations must be sorted and duplicate-free")
        if (
            self.transaction_mode is TransactionMode.FILE_WRAPPED
            and self.executor_action is not ExecutorAction.SQL_FILE
        ):
            raise ValueError("file-wrapped migration must use SQL_FILE executor action")
        if self.executor_action is ExecutorAction.CREATE_PARTITIONS:
            if self.relative_path is not None or self.file_sha256 is not None:
                raise ValueError("CREATE_PARTITIONS migration cannot carry a SQL source file")
        elif self.relative_path is None or self.file_sha256 is None:
            raise ValueError("SQL-backed migration requires a frozen source path and SHA-256")
        if self.executor_action is ExecutorAction.CREATE_PARTITIONS and not self.partition_parent_relations:
            raise ValueError("CREATE_PARTITIONS migration must declare partition parents")
        if self.executor_action is not ExecutorAction.CREATE_PARTITIONS and self.partition_parent_relations:
            raise ValueError("only CREATE_PARTITIONS migration may declare partition parents")
        return self


class RelationSpec(SchemaScopedModel):
    name: str
    relkind: str = Field(min_length=1, max_length=1)
    persistence: str = Field(min_length=1, max_length=1)
    partition_strategy: str | None = None
    partition_key: str | None = None
    definition_sha256: str | None = None
    repairable_by_orders: tuple[int, ...] = ()

    @field_validator("schema_name", "name")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("definition_sha256")
    @classmethod
    def _validate_definition_hash(cls, value: str | None) -> str | None:
        return _sha256(value, field_name="definition_sha256") if value is not None else None

    @property
    def object_id(self) -> str:
        return f"relation:{self.schema}.{self.name}"


class ColumnSpec(SchemaScopedModel):
    relation: str
    ordinal: int = Field(ge=1)
    name: str
    data_type: str = Field(min_length=1)
    nullable: bool
    default: str | None = None
    identity: str = ""
    generated: str = ""
    repairable_by_orders: tuple[int, ...] = ()

    @field_validator("schema_name", "relation", "name")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("default")
    @classmethod
    def _normalize_default(cls, value: str | None) -> str | None:
        return normalize_sql(value)

    @property
    def object_id(self) -> str:
        return f"column:{self.schema}.{self.relation}.{self.name}"


class ConstraintSpec(SchemaScopedModel):
    relation: str
    name: str
    constraint_type: str = Field(min_length=1, max_length=1)
    deferrable: bool
    initially_deferred: bool
    validated: bool
    definition: str = Field(min_length=1)
    repairable_by_orders: tuple[int, ...] = ()

    @field_validator("schema_name", "relation", "name")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("definition")
    @classmethod
    def _normalize_definition(cls, value: str) -> str:
        normalized = normalize_sql(value)
        assert normalized is not None
        return normalized

    @property
    def object_id(self) -> str:
        return f"constraint:{self.schema}.{self.relation}.{self.name}"


class IndexSpec(SchemaScopedModel):
    relation: str
    name: str
    unique: bool
    valid: bool
    ready: bool
    access_method: str
    definition: str = Field(min_length=1)
    repairable_by_orders: tuple[int, ...] = ()

    @field_validator("schema_name", "relation", "name", "access_method")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("definition")
    @classmethod
    def _normalize_definition(cls, value: str) -> str:
        normalized = normalize_sql(value)
        assert normalized is not None
        return normalized

    @property
    def object_id(self) -> str:
        return f"index:{self.schema}.{self.relation}.{self.name}"


class FunctionSpec(SchemaScopedModel):
    name: str
    identity_arguments: str = ""
    return_type: str = Field(min_length=1)
    language: str = Field(min_length=1)
    volatility: str = Field(min_length=1, max_length=1)
    security_definer: bool
    body_sha256: str
    repairable_by_orders: tuple[int, ...] = ()

    @field_validator("schema_name", "name", "language")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("identity_arguments")
    @classmethod
    def _normalize_arguments(cls, value: str) -> str:
        return normalize_sql(value) or ""

    @field_validator("body_sha256")
    @classmethod
    def _validate_body_hash(cls, value: str) -> str:
        return _sha256(value, field_name="body_sha256")

    @property
    def object_id(self) -> str:
        return f"function:{self.schema}.{self.name}({self.identity_arguments})"


class TriggerSpec(SchemaScopedModel):
    relation: str
    name: str
    enabled: str = Field(min_length=1, max_length=1)
    trigger_type: int = Field(ge=0)
    is_constraint: bool
    deferrable: bool
    initially_deferred: bool
    function_schema: str
    function_name: str
    function_identity_arguments: str = ""
    definition: str = Field(min_length=1)
    repairable_by_orders: tuple[int, ...] = ()

    @field_validator("schema_name", "relation", "name", "function_schema", "function_name")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("function_identity_arguments", "definition")
    @classmethod
    def _normalize_sql(cls, value: str) -> str:
        return normalize_sql(value) or ""

    @property
    def object_id(self) -> str:
        return f"trigger:{self.schema}.{self.relation}.{self.name}"


class CommentSpec(SchemaScopedModel):
    relation: str
    column: str | None = None
    text_sha256: str
    repairable_by_orders: tuple[int, ...] = ()

    @field_validator("schema_name", "relation")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("column")
    @classmethod
    def _validate_optional_identifier(cls, value: str | None) -> str | None:
        return _identifier(value, field_name="column") if value is not None else None

    @field_validator("text_sha256")
    @classmethod
    def _validate_text_hash(cls, value: str) -> str:
        return _sha256(value, field_name="text_sha256")

    @property
    def object_id(self) -> str:
        suffix = self.column if self.column is not None else "__table__"
        return f"comment:{self.schema}.{self.relation}.{suffix}"


class PrerequisiteRelationSpec(SchemaScopedModel):
    name: str
    relkind: str = Field(min_length=1, max_length=1)
    columns: tuple[ColumnSpec, ...] = Field(min_length=1)
    constraints: tuple[ConstraintSpec, ...] = ()

    @field_validator("schema_name", "name")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_members(self) -> "PrerequisiteRelationSpec":
        if any(column.schema != self.schema or column.relation != self.name for column in self.columns):
            raise ValueError("prerequisite columns must belong to the prerequisite relation")
        if any(item.schema != self.schema or item.relation != self.name for item in self.constraints):
            raise ValueError("prerequisite constraints must belong to the prerequisite relation")
        return self

    @property
    def object_id(self) -> str:
        return f"prerequisite_relation:{self.schema}.{self.name}"


class PartitionContract(SchemaScopedModel):
    parent_relation: str
    partition_strategy: Literal["r"]
    partition_key: str = Field(min_length=1)
    child_prefix: str = Field(min_length=1)
    no_default_partition: Literal[True]
    repairable_by_orders: tuple[int, ...] = (60,)

    @field_validator("schema_name", "parent_relation", "child_prefix")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_repair_orders(self) -> "PartitionContract":
        if tuple(sorted(set(self.repairable_by_orders))) != self.repairable_by_orders:
            raise ValueError("partition repair orders must be sorted and duplicate-free")
        return self


class RepairableDriftVariant(StrictModel):
    object_id: str = Field(min_length=1)
    actual_payload_sha256: str
    repairable_by_orders: tuple[int, ...] = Field(min_length=1)

    @field_validator("actual_payload_sha256")
    @classmethod
    def _validate_payload_hash(cls, value: str) -> str:
        return _sha256(value, field_name="actual_payload_sha256")

    @model_validator(mode="after")
    def _validate_variant(self) -> "RepairableDriftVariant":
        if tuple(sorted(set(self.repairable_by_orders))) != self.repairable_by_orders:
            raise ValueError("repairable drift orders must be sorted and duplicate-free")
        return self


class RepairableUnexpectedObject(StrictModel):
    """A frozen predecessor-only catalog object removed by one forward migration."""

    object_id: str = Field(min_length=1)
    repairable_by_orders: tuple[int, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_orders(self) -> "RepairableUnexpectedObject":
        if tuple(sorted(set(self.repairable_by_orders))) != self.repairable_by_orders:
            raise ValueError("unexpected-object repair orders must be sorted and duplicate-free")
        return self


class PredecessorContractSpec(StrictModel):
    """Frozen catalog contract used only to recognize an exact forward predecessor."""

    relative_path: str = Field(min_length=1)
    contract_content_hash: str
    exact_relations: tuple[str, ...] = Field(min_length=1)

    @field_validator("contract_content_hash")
    @classmethod
    def _validate_contract_hash(cls, value: str) -> str:
        return _sha256(value, field_name="predecessor contract_content_hash")

    @model_validator(mode="after")
    def _validate_spec(self) -> "PredecessorContractSpec":
        path = Path(self.relative_path)
        if path.name != self.relative_path or path.suffix != ".json":
            raise ValueError("predecessor contract path must be one registry JSON filename")
        if tuple(sorted(set(self.exact_relations))) != self.exact_relations:
            raise ValueError("predecessor exact relations must be sorted and duplicate-free")
        for relation in self.exact_relations:
            parts = relation.split(".")
            if len(parts) != 2:
                raise ValueError("predecessor exact relation must be schema-qualified")
            _identifier(parts[0], field_name="predecessor relation schema")
            _identifier(parts[1], field_name="predecessor relation name")
        return self


class ReleaseSchemaContract(StrictModel):
    schema_version: Literal[CONTRACT_SCHEMA_VERSION, CONTRACT_SCHEMA_VERSION_V2]
    release_schema_version: str = Field(min_length=1)
    normalizer_version: Literal[NORMALIZER_VERSION]
    supported_postgres_major_versions: tuple[int, ...] = Field(min_length=1)
    ddl_session_policy: DdlSessionPolicy
    managed_migrations: tuple[ManagedMigration, ...] = Field(min_length=1)
    repairable_drift_variants: tuple[RepairableDriftVariant, ...] = ()
    repairable_unexpected_objects: tuple[RepairableUnexpectedObject, ...] = ()
    predecessor_contract: PredecessorContractSpec | None = None
    phase0a_prerequisite_relations: tuple[PrerequisiteRelationSpec, ...] = ()
    external_readonly_prerequisite_relations: tuple[PrerequisiteRelationSpec, ...] = Field(min_length=1)
    required_relations: tuple[RelationSpec, ...] = Field(min_length=1)
    required_columns: tuple[ColumnSpec, ...] = Field(min_length=1)
    required_constraints: tuple[ConstraintSpec, ...] = ()
    required_indexes: tuple[IndexSpec, ...] = ()
    required_functions: tuple[FunctionSpec, ...] = ()
    required_triggers: tuple[TriggerSpec, ...] = ()
    required_comments: tuple[CommentSpec, ...] = ()
    partition_contract: PartitionContract
    additional_partition_contracts: tuple[PartitionContract, ...] = ()
    contract_content_hash: str

    @field_validator("contract_content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, field_name="contract_content_hash")

    @model_validator(mode="after")
    def _validate_contract(self) -> "ReleaseSchemaContract":
        migrations = tuple(sorted(self.managed_migrations, key=lambda item: item.order))
        if migrations != self.managed_migrations or len({item.order for item in migrations}) != len(migrations):
            raise ValueError("managed migrations must be strictly ordered and unique")
        known_orders = {item.order for item in migrations}
        if any(not set(item.depends_on_orders).issubset(known_orders) for item in migrations):
            raise ValueError("migration dependency references an unknown order")
        if tuple(sorted(set(self.supported_postgres_major_versions))) != self.supported_postgres_major_versions:
            raise ValueError("supported_postgres_major_versions must be sorted and duplicate-free")
        variant_keys = tuple((item.object_id, item.actual_payload_sha256) for item in self.repairable_drift_variants)
        if tuple(sorted(set(variant_keys))) != variant_keys:
            raise ValueError("repairable_drift_variants must be sorted and duplicate-free")
        unexpected_ids = tuple(item.object_id for item in self.repairable_unexpected_objects)
        if tuple(sorted(set(unexpected_ids))) != unexpected_ids:
            raise ValueError("repairable unexpected objects must be sorted and duplicate-free")
        relation_ids = {item.object_id for item in self.required_relations}
        if len(relation_ids) != len(self.required_relations):
            raise ValueError("required_relations must have unique identities")
        allowed_relations = {(item.schema, item.name) for item in self.required_relations}
        if any((item.schema, item.relation) not in allowed_relations for item in self.required_columns):
            raise ValueError("required column refers to an unmanaged relation")
        if any((item.schema, item.relation) not in allowed_relations for item in self.required_constraints):
            raise ValueError("required constraint refers to an unmanaged relation")
        if any((item.schema, item.relation) not in allowed_relations for item in self.required_indexes):
            raise ValueError("required index refers to an unmanaged relation")
        if any((item.schema, item.relation) not in allowed_relations for item in self.required_triggers):
            raise ValueError("required trigger refers to an unmanaged relation")
        if any((item.schema, item.relation) not in allowed_relations for item in self.required_comments):
            raise ValueError("required comment refers to an unmanaged relation")
        partition_parents = tuple((item.schema, item.parent_relation) for item in self.partition_contracts)
        if len(set(partition_parents)) != len(partition_parents):
            raise ValueError("partition parents must be unique")
        child_prefixes = tuple(item.child_prefix for item in self.partition_contracts)
        if len(set(child_prefixes)) != len(child_prefixes):
            raise ValueError("partition child prefixes must be unique")
        for item in self.partition_contracts:
            parent = (item.schema, item.parent_relation)
            if parent not in allowed_relations:
                raise ValueError("partition parent must be a required relation")
            relation = next(value for value in self.required_relations if (value.schema, value.name) == parent)
            if relation.partition_strategy != item.partition_strategy or relation.partition_key != item.partition_key:
                raise ValueError("partition contract must match its required parent relation")
        if self.schema_version == CONTRACT_SCHEMA_VERSION:
            if self.additional_partition_contracts:
                raise ValueError("v1 contract cannot declare additional partition contracts")
            if self.repairable_unexpected_objects:
                raise ValueError("v1 contract cannot declare repairable unexpected objects")
            if self.predecessor_contract is not None:
                raise ValueError("v1 contract cannot declare a predecessor contract")
            if any(
                item.executor_action is not ExecutorAction.SQL_FILE or item.partition_parent_relations
                for item in migrations
            ):
                raise ValueError("v1 contract cannot declare typed executor actions")
            if any(item.definition_sha256 is not None for item in self.required_relations):
                raise ValueError("v1 contract cannot declare relation definition hashes")
        elif self.predecessor_contract is None:
            raise ValueError("v2 contract must freeze its exact predecessor contract")
        if not {item.object_id for item in self.repairable_drift_variants}.issubset(self.object_ids()):
            raise ValueError("repairable drift references an unmanaged object")
        if any(not set(item.repairable_by_orders).issubset(known_orders) for item in self.repairable_drift_variants):
            raise ValueError("repairable drift references an unknown migration order")
        if any(
            not set(item.repairable_by_orders).issubset(known_orders) for item in self.repairable_unexpected_objects
        ):
            raise ValueError("repairable unexpected object references an unknown migration order")
        if self.contract_content_hash != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("contract_content_hash does not match the canonical contract payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="python", exclude={"contract_content_hash"})
        if self.schema_version == CONTRACT_SCHEMA_VERSION:
            payload.pop("additional_partition_contracts", None)
            payload.pop("repairable_unexpected_objects", None)
            payload.pop("predecessor_contract", None)
            for migration in payload["managed_migrations"]:
                migration.pop("executor_action", None)
                migration.pop("partition_parent_relations", None)
            for relation in payload["required_relations"]:
                relation.pop("definition_sha256", None)
        return payload

    @property
    def partition_contracts(self) -> tuple[PartitionContract, ...]:
        return (self.partition_contract, *self.additional_partition_contracts)

    def partition_contract_for(self, partition: "MonthPartition") -> PartitionContract:
        for item in self.partition_contracts:
            if item.schema == partition.schema and item.parent_relation == partition.parent_relation:
                return item
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_INVALID,
            f"partition {partition.schema}.{partition.name} is not owned by a contract parent",
        )

    @property
    def ddl_session_policy_hash(self) -> str:
        return self.ddl_session_policy.policy_hash

    def object_ids(self) -> set[str]:
        values: set[str] = set()
        for group in (
            self.required_relations,
            self.required_columns,
            self.required_constraints,
            self.required_indexes,
            self.required_functions,
            self.required_triggers,
            self.required_comments,
        ):
            values.update(item.object_id for item in group)
        return values


class TargetLabel(str, Enum):
    DEV = "DEV"
    PRODUCTION = "PRODUCTION"


class RequestedOperation(str, Enum):
    PLAN = "PLAN"
    VERIFY = "VERIFY"
    APPLY = "APPLY"


class ManagedSchemaStatus(str, Enum):
    COMPATIBLE = "COMPATIBLE"
    ABSENT = "ABSENT"
    PARTIAL_ADDITIVE = "PARTIAL_ADDITIVE"
    DRIFTED = "DRIFTED"
    UNSUPPORTED = "UNSUPPORTED"


class PrerequisiteStatus(str, Enum):
    COMPATIBLE = "COMPATIBLE"
    MISSING = "MISSING"
    DRIFTED = "DRIFTED"
    UNSUPPORTED = "UNSUPPORTED"


class OperationStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class PendingDdlOperation(StrictModel):
    kind: Literal["MIGRATION", "PARTITION"]
    migration_order: int | None = Field(default=None, ge=1)
    partition_name: str | None = None
    lower_bound: date | None = None
    upper_bound: date | None = None

    @model_validator(mode="after")
    def _validate_operation(self) -> "PendingDdlOperation":
        if self.kind == "MIGRATION":
            if self.migration_order is None or any(
                value is not None for value in (self.partition_name, self.lower_bound, self.upper_bound)
            ):
                raise ValueError("migration operation must contain only migration_order")
        elif (
            self.migration_order is not None
            or self.partition_name is None
            or self.lower_bound is None
            or self.upper_bound is None
        ):
            raise ValueError("partition operation must contain a complete partition bound")
        elif self.lower_bound >= self.upper_bound:
            raise ValueError("partition lower bound must precede upper bound")
        return self


class CatalogDifference(StrictModel):
    object_id: str = Field(min_length=1)
    category: Literal["MISSING", "DRIFTED", "UNEXPECTED", "UNSUPPORTED"]
    expected: dict[str, Any] | None = None
    actual: dict[str, Any] | None = None
    reason_code: str = Field(min_length=1)
    repairable_by_orders: tuple[int, ...] = ()


CATALOG_FINGERPRINT_KINDS = (
    "relations",
    "columns",
    "constraints",
    "indexes",
    "functions",
    "triggers",
    "comments",
    "partitions",
)


class CatalogFingerprintEvidence(StrictModel):
    normalizer_version: Literal[NORMALIZER_VERSION]
    total_sha256: str
    object_count: int = Field(ge=0)
    per_kind_counts: dict[str, int]
    per_kind_hashes: dict[str, str]

    @field_validator("total_sha256")
    @classmethod
    def _validate_total_hash(cls, value: str) -> str:
        return _sha256(value, field_name="total_sha256")

    @model_validator(mode="after")
    def _validate_evidence(self) -> "CatalogFingerprintEvidence":
        expected = set(CATALOG_FINGERPRINT_KINDS)
        if set(self.per_kind_counts) != expected or set(self.per_kind_hashes) != expected:
            raise ValueError("catalog fingerprint evidence must contain every frozen object kind")
        if any(value < 0 for value in self.per_kind_counts.values()):
            raise ValueError("catalog fingerprint counts must be non-negative")
        if self.object_count != sum(self.per_kind_counts.values()):
            raise ValueError("catalog fingerprint object_count does not match per-kind counts")
        for value in self.per_kind_hashes.values():
            _sha256(value, field_name="per_kind_hashes")
        return self


class DatabaseIdentity(StrictModel):
    target_label: TargetLabel
    current_database: str = Field(min_length=1)
    server_address: str | None = None
    server_port: int = Field(ge=1, le=65535)
    server_version_num: int = Field(ge=1)
    current_user_hash: str
    environment_contract_hash: str

    @field_validator("current_user_hash", "environment_contract_hash")
    @classmethod
    def _validate_identity_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, field_name=info.field_name)

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="python")


class ReleaseSchemaPlanRequest(StrictModel):
    schema_version: Literal[PLAN_REQUEST_SCHEMA_VERSION]
    release_schema_version: str = Field(min_length=1)
    contract_content_hash: str
    target_label: TargetLabel
    ddl_session_policy_hash: str
    history_start_trade_date: date
    history_end_trade_date: date
    capacity_request_hash: str
    capacity_receipt_hash: str | None = None
    phase1e_plan_hashes: tuple[str, ...] = ()
    requested_operation: RequestedOperation
    request_content_hash: str

    @field_validator(
        "contract_content_hash", "ddl_session_policy_hash", "capacity_request_hash", "capacity_receipt_hash"
    )
    @classmethod
    def _validate_optional_hash(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("phase1e_plan_hashes")
    @classmethod
    def _validate_phase1e_hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if tuple(sorted(set(values))) != values:
            raise ValueError("phase1e_plan_hashes must be sorted and duplicate-free")
        for value in values:
            _sha256(value, field_name="phase1e_plan_hashes")
        return values

    @field_validator("request_content_hash")
    @classmethod
    def _validate_request_hash(cls, value: str) -> str:
        return _sha256(value, field_name="request_content_hash")

    @model_validator(mode="after")
    def _validate_request(self) -> "ReleaseSchemaPlanRequest":
        if self.history_start_trade_date > self.history_end_trade_date:
            raise ValueError("history_start_trade_date must not be after history_end_trade_date")
        if self.request_content_hash != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("request_content_hash does not match canonical request payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="python", exclude={"request_content_hash"})


class Phase1F1LegacyMonthInventory(StrictModel):
    """Frozen, read-only predecessor evidence used by the v2 cutover transaction."""

    schema_version: Literal["advisory_phase1f1_legacy_month_inventory_v1"]
    predecessor_layout: Literal["ABSENT", "V1_TABLES", "V2_VIEWS"]
    lineage_row_count: int = Field(ge=0)
    candidate_row_count: int = Field(ge=0)
    legacy_months: tuple[date, ...] = ()
    target_months: tuple[date, ...] = Field(min_length=1)
    legacy_months_hash: str
    target_months_hash: str
    legacy_inventory_hash: str

    @field_validator("legacy_months", "target_months")
    @classmethod
    def _validate_months(cls, values: tuple[date, ...], info: Any) -> tuple[date, ...]:
        if any(value.day != 1 for value in values):
            raise ValueError(f"{info.field_name} values must be calendar-month starts")
        if tuple(sorted(set(values))) != values:
            raise ValueError(f"{info.field_name} must be sorted and duplicate-free")
        return values

    @field_validator("legacy_months_hash", "target_months_hash", "legacy_inventory_hash")
    @classmethod
    def _validate_hashes(cls, value: str, info: Any) -> str:
        return _sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_inventory(self) -> "Phase1F1LegacyMonthInventory":
        if self.predecessor_layout != "V1_TABLES" and (
            self.lineage_row_count or self.candidate_row_count or self.legacy_months
        ):
            raise ValueError("only a v1-table predecessor may carry legacy rows or months")
        if not set(self.legacy_months).issubset(self.target_months):
            raise ValueError("target months must include every legacy month")
        if self.legacy_months_hash != canonical_json_sha256(self.legacy_months):
            raise ValueError("legacy_months_hash does not match legacy months")
        if self.target_months_hash != canonical_json_sha256(self.target_months):
            raise ValueError("target_months_hash does not match target months")
        if self.legacy_inventory_hash != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("legacy_inventory_hash does not match frozen inventory payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="python", exclude={"legacy_inventory_hash"})


class ReleaseSchemaPlan(StrictModel):
    schema_version: Literal[PLAN_SCHEMA_VERSION, PLAN_SCHEMA_VERSION_V2]
    request: ReleaseSchemaPlanRequest
    database_identity: DatabaseIdentity
    release_schema_version: str
    contract_content_hash: str
    ddl_session_policy: DdlSessionPolicy
    ddl_session_policy_hash: str
    ordered_migrations: tuple[ManagedMigration, ...]
    pre_catalog_fingerprint: str
    pre_catalog_evidence: CatalogFingerprintEvidence
    expected_final_catalog_fingerprint: str
    expected_final_catalog_evidence: CatalogFingerprintEvidence
    managed_schema_status: ManagedSchemaStatus
    prerequisite_status: PrerequisiteStatus
    downstream_ready: bool
    managed_differences: tuple[CatalogDifference, ...] = ()
    prerequisite_differences: tuple[CatalogDifference, ...] = ()
    expected_partitions: tuple["MonthPartition", ...]
    legacy_inventory: Phase1F1LegacyMonthInventory | None = None
    pending_ddl_operations: tuple[PendingDdlOperation, ...] = ()
    plan_content_hash: str

    @field_validator(
        "contract_content_hash",
        "ddl_session_policy_hash",
        "pre_catalog_fingerprint",
        "expected_final_catalog_fingerprint",
        "plan_content_hash",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_plan(self) -> "ReleaseSchemaPlan":
        if self.ddl_session_policy_hash != self.ddl_session_policy.policy_hash:
            raise ValueError("ddl_session_policy_hash does not match policy")
        if self.pre_catalog_fingerprint != self.pre_catalog_evidence.total_sha256:
            raise ValueError("pre_catalog_fingerprint does not match evidence")
        if self.expected_final_catalog_fingerprint != self.expected_final_catalog_evidence.total_sha256:
            raise ValueError("expected_final_catalog_fingerprint does not match evidence")
        expected_downstream = (
            self.managed_schema_status is ManagedSchemaStatus.COMPATIBLE
            and self.prerequisite_status is PrerequisiteStatus.COMPATIBLE
        )
        if self.downstream_ready != expected_downstream:
            raise ValueError("downstream_ready must be derived from both schema axes")
        if self.schema_version == PLAN_SCHEMA_VERSION_V2 and self.legacy_inventory is None:
            raise ValueError("v2 plan requires frozen legacy inventory")
        if self.schema_version == PLAN_SCHEMA_VERSION and self.legacy_inventory is not None:
            raise ValueError("v1 plan cannot contain v2 legacy inventory")
        if self.plan_content_hash != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("plan_content_hash does not match canonical plan payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="python", exclude={"plan_content_hash"})
        if self.schema_version == PLAN_SCHEMA_VERSION:
            payload.pop("legacy_inventory", None)
        return payload


class MigrationExecutionStatus(str, Enum):
    NOT_NEEDED = "NOT_NEEDED"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class MigrationExecutionResult(StrictModel):
    order: int = Field(ge=1)
    transaction_mode: TransactionMode
    status: MigrationExecutionStatus
    pre_subset_fingerprint: str | None = None
    post_subset_fingerprint: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_code: str | None = None
    error_type: str | None = None

    @field_validator("pre_subset_fingerprint", "post_subset_fingerprint")
    @classmethod
    def _validate_optional_hash(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None


class ReleaseSchemaReceipt(StrictModel):
    schema_version: Literal[RECEIPT_SCHEMA_VERSION, RECEIPT_SCHEMA_VERSION_V2]
    operation: RequestedOperation
    requested_operation: RequestedOperation
    database_identity: DatabaseIdentity
    request_content_hash: str
    plan_content_hash: str | None = None
    contract_content_hash: str
    pre_catalog_fingerprint: str | None = None
    pre_catalog_evidence: CatalogFingerprintEvidence | None = None
    executed_migration_hashes: tuple[str, ...] = ()
    per_migration_results: tuple[MigrationExecutionResult, ...] = ()
    executed_partitions: tuple["MonthPartition", ...] = ()
    post_catalog_fingerprint: str | None = None
    post_catalog_evidence: CatalogFingerprintEvidence | None = None
    operation_status: OperationStatus
    managed_schema_status: ManagedSchemaStatus
    prerequisite_status: PrerequisiteStatus
    downstream_ready: bool
    managed_differences: tuple[CatalogDifference, ...] = ()
    prerequisite_differences: tuple[CatalogDifference, ...] = ()
    legacy_inventory: Phase1F1LegacyMonthInventory | None = None
    diagnostics: tuple[dict[str, Any], ...] = ()
    errors: tuple[dict[str, Any], ...] = ()
    started_at: datetime
    finished_at: datetime
    ddl_executed: bool
    dml_executed: Literal[False]
    runtime_activated: Literal[False]
    receipt_content_hash: str

    @field_validator(
        "request_content_hash",
        "plan_content_hash",
        "contract_content_hash",
        "pre_catalog_fingerprint",
        "post_catalog_fingerprint",
        "receipt_content_hash",
    )
    @classmethod
    def _validate_optional_hash(cls, value: str | None, info: Any) -> str | None:
        return _sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("executed_migration_hashes")
    @classmethod
    def _validate_migration_hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        for value in values:
            _sha256(value, field_name="executed_migration_hashes")
        return values

    @model_validator(mode="after")
    def _validate_receipt(self) -> "ReleaseSchemaReceipt":
        expected_downstream = (
            self.managed_schema_status is ManagedSchemaStatus.COMPATIBLE
            and self.prerequisite_status is PrerequisiteStatus.COMPATIBLE
        )
        if self.downstream_ready != expected_downstream:
            raise ValueError("downstream_ready must be derived from final statuses")
        if self.operation_status is OperationStatus.SUCCESS and self.errors:
            raise ValueError("successful receipt cannot contain errors")
        if self.operation_status is OperationStatus.FAILED and not self.errors:
            raise ValueError("failed receipt must contain errors")
        if self.operation is not RequestedOperation.APPLY and self.ddl_executed:
            raise ValueError("only apply may execute DDL")
        if self.operation is not RequestedOperation.PLAN and self.operation is not self.requested_operation:
            raise ValueError("non-plan receipt operation must match requested_operation")
        if self.schema_version == RECEIPT_SCHEMA_VERSION_V2 and self.legacy_inventory is None:
            raise ValueError("v2 receipt requires frozen legacy inventory")
        if self.schema_version == RECEIPT_SCHEMA_VERSION and self.legacy_inventory is not None:
            raise ValueError("v1 receipt cannot contain v2 legacy inventory")
        if (self.pre_catalog_fingerprint is None) != (self.pre_catalog_evidence is None):
            raise ValueError("pre catalog fingerprint and evidence must be present together")
        if (
            self.pre_catalog_evidence is not None
            and self.pre_catalog_fingerprint != self.pre_catalog_evidence.total_sha256
        ):
            raise ValueError("pre catalog fingerprint does not match evidence")
        if (self.post_catalog_fingerprint is None) != (self.post_catalog_evidence is None):
            raise ValueError("post catalog fingerprint and evidence must be present together")
        if (
            self.post_catalog_evidence is not None
            and self.post_catalog_fingerprint != self.post_catalog_evidence.total_sha256
        ):
            raise ValueError("post catalog fingerprint does not match evidence")
        if self.receipt_content_hash != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("receipt_content_hash does not match canonical receipt payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="python", exclude={"receipt_content_hash"})
        if self.schema_version == RECEIPT_SCHEMA_VERSION:
            payload.pop("legacy_inventory", None)
        return payload


class MonthPartition(SchemaScopedModel):
    parent_relation: str
    name: str
    lower_bound: date
    upper_bound: date

    @model_validator(mode="after")
    def _validate_bounds(self) -> "MonthPartition":
        if self.lower_bound >= self.upper_bound:
            raise ValueError("partition lower bound must precede upper bound")
        if self.lower_bound.day != 1 or self.upper_bound.day != 1:
            raise ValueError("month partition bounds must start on the first day")
        return self

    @property
    def object_id(self) -> str:
        return f"partition:{self.schema}.{self.name}"


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def plan_month_partitions(
    *,
    partition_contract: PartitionContract,
    history_start_trade_date: date,
    history_end_trade_date: date,
) -> tuple[MonthPartition, ...]:
    """Return every inclusive calendar month required by an explicit capacity range."""

    if history_start_trade_date > history_end_trade_date:
        raise ReleaseSchemaContractError(REASON_PARTITION_RANGE_INVALID, "history start date is after end date")
    current = _month_start(history_start_trade_date)
    final = _month_start(history_end_trade_date)
    partitions: list[MonthPartition] = []
    while current <= final:
        upper = _next_month(current)
        partitions.append(
            MonthPartition(
                schema=partition_contract.schema,
                parent_relation=partition_contract.parent_relation,
                name=f"{partition_contract.child_prefix}_{current.year:04d}{current.month:02d}",
                lower_bound=current,
                upper_bound=upper,
            )
        )
        current = upper
    return tuple(partitions)


def plan_month_partitions_for_contracts(
    *,
    partition_contracts: Iterable[PartitionContract],
    target_months: Iterable[date],
) -> tuple[MonthPartition, ...]:
    """Expand a frozen set of month starts for every declared partition parent."""

    months = tuple(sorted(set(target_months)))
    if not months:
        raise ReleaseSchemaContractError(REASON_PARTITION_RANGE_INVALID, "at least one target month is required")
    if any(value.day != 1 for value in months):
        raise ReleaseSchemaContractError(REASON_PARTITION_RANGE_INVALID, "target months must be calendar-month starts")
    contracts = tuple(partition_contracts)
    if not contracts:
        raise ReleaseSchemaContractError(REASON_CONTRACT_INVALID, "at least one partition contract is required")
    partitions: list[MonthPartition] = []
    for contract in contracts:
        for lower in months:
            partitions.append(
                MonthPartition(
                    schema=contract.schema,
                    parent_relation=contract.parent_relation,
                    name=f"{contract.child_prefix}_{lower.year:04d}{lower.month:02d}",
                    lower_bound=lower,
                    upper_bound=_next_month(lower),
                )
            )
    return tuple(sorted(partitions, key=lambda item: (item.schema, item.parent_relation, item.lower_bound, item.name)))


def make_release_plan_request(
    *,
    contract: ReleaseSchemaContract,
    target_label: TargetLabel,
    history_start_trade_date: date,
    history_end_trade_date: date,
    capacity_request_hash: str,
    capacity_receipt_hash: str | None,
    phase1e_plan_hashes: Iterable[str],
    requested_operation: RequestedOperation,
) -> ReleaseSchemaPlanRequest:
    payload: dict[str, Any] = {
        "schema_version": PLAN_REQUEST_SCHEMA_VERSION,
        "release_schema_version": contract.release_schema_version,
        "contract_content_hash": contract.contract_content_hash,
        "target_label": target_label,
        "ddl_session_policy_hash": contract.ddl_session_policy_hash,
        "history_start_trade_date": history_start_trade_date,
        "history_end_trade_date": history_end_trade_date,
        "capacity_request_hash": capacity_request_hash,
        "capacity_receipt_hash": capacity_receipt_hash,
        "phase1e_plan_hashes": tuple(sorted(set(phase1e_plan_hashes))),
        "requested_operation": requested_operation,
    }
    payload["request_content_hash"] = canonical_json_sha256(payload)
    return ReleaseSchemaPlanRequest.model_validate(payload)


RELEASE_SCHEMA_REGISTRY_ROOT = Path(__file__).resolve().parent / "release_schema_registry"
DEFAULT_RELEASE_SCHEMA_REGISTRY = RELEASE_SCHEMA_REGISTRY_ROOT / "advisory_phase1_dataset_foundation_v3.json"


def load_release_schema_contract(path: Path | None = None) -> ReleaseSchemaContract:
    """Load the immutable repository contract without consulting a database."""

    registry_path = (path or DEFAULT_RELEASE_SCHEMA_REGISTRY).resolve()
    try:
        registry_path.relative_to(RELEASE_SCHEMA_REGISTRY_ROOT.resolve())
    except ValueError as exc:
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_INVALID, "registry path escapes the repository registry root"
        ) from exc
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_INVALID, f"unable to load release schema registry: {type(exc).__name__}"
        ) from exc
    if not isinstance(payload, dict):
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_INVALID, "release schema registry must contain one JSON object"
        )
    supplied_hash = payload.get("contract_content_hash")
    hash_payload = dict(payload)
    hash_payload.pop("contract_content_hash", None)
    if supplied_hash != canonical_json_sha256(hash_payload):
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_HASH_MISMATCH, "release schema registry hash does not match content"
        )
    try:
        return ReleaseSchemaContract.model_validate(payload)
    except Exception as exc:
        raise ReleaseSchemaContractError(REASON_CONTRACT_INVALID, str(exc)) from exc


def load_predecessor_release_schema_contract(contract: ReleaseSchemaContract) -> ReleaseSchemaContract | None:
    """Load and verify the repository-frozen predecessor named by a forward contract."""

    spec = contract.predecessor_contract
    if spec is None:
        return None
    predecessor = load_release_schema_contract(RELEASE_SCHEMA_REGISTRY_ROOT / spec.relative_path)
    if predecessor.contract_content_hash != spec.contract_content_hash:
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_HASH_MISMATCH,
            "predecessor registry content hash differs from the forward contract",
        )
    if predecessor.release_schema_version == contract.release_schema_version:
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_INVALID,
            "predecessor release schema version must differ from the current contract",
        )
    predecessor_relations = {f"{item.schema}.{item.name}" for item in predecessor.required_relations}
    if not set(spec.exact_relations).issubset(predecessor_relations):
        raise ReleaseSchemaContractError(
            REASON_CONTRACT_INVALID,
            "predecessor exact relation scope is not present in the predecessor registry",
        )
    return predecessor
