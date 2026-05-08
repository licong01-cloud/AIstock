from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterator, Protocol
from uuid import uuid4

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError

ConnFactory = Callable[[], Iterator[Any]]


class TemplateLifecycleStatus(str, Enum):
    ACTIVE = "active"
    EXPERIMENTAL = "experimental"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class SpecLifecycleStatus(str, Enum):
    TEMPLATE = "template"
    RESEARCH_CANDIDATE = "research_candidate"
    RDAGENT_CANDIDATE = "rdagent_candidate"
    VALIDATED_SPEC = "validated_spec"
    PROMOTED_ARTIFACT = "promoted_artifact"
    PAPER_CANDIDATE = "paper_candidate"
    PAPER_ENABLED = "paper_enabled"
    QUARANTINED = "quarantined"
    TRAINING_FAILED = "training_failed"
    RETIRED = "retired"


class TrialStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    INVALID = "invalid"


class ArtifactStatus(str, Enum):
    PRESENT = "present"
    MISSING = "missing"
    CORRUPTED = "corrupted"
    EXPIRED = "expired"


class ModelObjectType(str, Enum):
    TEMPLATE = "template"
    SPEC = "spec"
    TRIAL = "trial"
    ARTIFACT = "artifact"


class ModelTemplateRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    template_id: str
    family: str
    model_type: str
    display_name: str
    description: str | None = None
    task_type: str = "rank"
    supported_freq: list[str] = Field(default_factory=lambda: ["day"])
    supported_input_shape: str = "tabular"
    train_backend: str = "qlib"
    default_search_space: dict[str, Any] = Field(default_factory=dict)
    default_train_budget: dict[str, Any] = Field(default_factory=dict)
    seed_capability: str = "unset_legacy"
    deterministic_support: str = "partial"
    gpu_required: bool = False
    lifecycle_status: TemplateLifecycleStatus = TemplateLifecycleStatus.ACTIVE
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ModelSpecRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spec_id: str
    template_id: str
    spec_version: str = "v1"
    model_name: str
    model_type: str
    code_ref: str | None = None
    code_text: str | None = None
    code_sha256: str | None = None
    architecture_config: dict[str, Any] = Field(default_factory=dict)
    architecture_sha256: str | None = None
    hyperparam_schema: dict[str, Any] = Field(default_factory=dict)
    default_hyperparams: dict[str, Any] = Field(default_factory=dict)
    search_space_json: dict[str, Any] = Field(default_factory=dict)
    input_contract_json: dict[str, Any] = Field(default_factory=dict)
    output_contract_json: dict[str, Any] = Field(default_factory=dict)
    feature_schema_requirements: dict[str, Any] = Field(default_factory=dict)
    label_requirements: dict[str, Any] = Field(default_factory=dict)
    dependency_versions: dict[str, Any] = Field(default_factory=dict)
    source_type: str = "builtin"
    source_task_id: str | None = None
    source_loop_id: str | None = None
    lifecycle_status: SpecLifecycleStatus = SpecLifecycleStatus.RESEARCH_CANDIDATE
    qe_selectable: bool = True
    qe_selectability_reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ModelTrialRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trial_id: str
    spec_id: str
    qe_run_id: str | None = None
    qe_experiment_id: str | None = None
    qe_task_id: str | None = None
    qe_loop_id: str | None = None
    factor_set_hash: str | None = None
    factor_list_ordered: list[str] = Field(default_factory=list)
    feature_schema_hash: str | None = None
    data_context_id: str | None = None
    dataset_version: str | None = None
    label_config_hash: str | None = None
    train_start: date | None = None
    train_end: date | None = None
    valid_start: date | None = None
    valid_end: date | None = None
    test_start: date | None = None
    test_end: date | None = None
    train_config_json: dict[str, Any] = Field(default_factory=dict)
    hyperparams_json: dict[str, Any] = Field(default_factory=dict)
    seed_policy: str = "unset_legacy"
    random_seed: int | None = None
    seed_sequence: dict[str, Any] = Field(default_factory=dict)
    deterministic_flags_json: dict[str, Any] = Field(default_factory=dict)
    status: TrialStatus = TrialStatus.SUCCEEDED
    failure_reason: str | None = None
    best_epoch: int | None = None
    total_epochs: int | None = None
    train_loss_final: float | None = None
    val_loss_final: float | None = None
    training_curves: dict[str, Any] = Field(default_factory=dict)
    ic: float | None = None
    rank_ic: float | None = None
    icir: float | None = None
    annualized_return: float | None = None
    sharpe: float | None = None
    max_drawdown: float | None = None
    turnover: float | None = None
    cost_drag: float | None = None
    score_total: float | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None


class ModelArtifactRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_id: str
    trial_id: str
    artifact_type: str
    artifact_uri: str
    artifact_sha256: str | None = None
    artifact_size_bytes: int | None = None
    feature_schema_hash: str | None = None
    feature_order_hash: str | None = None
    preprocessor_hash: str | None = None
    model_format: str | None = None
    retention_class: str = "archived"
    protected_asset: bool = False
    artifact_status: ArtifactStatus = ArtifactStatus.PRESENT
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    validated_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class ModelLifecycleEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    object_type: ModelObjectType
    object_id: str
    from_status: str | None = None
    to_status: str
    reason: str
    operator: str
    context_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ModelRegistryRepository(Protocol):
    def upsert_template(self, record: ModelTemplateRecord) -> ModelTemplateRecord: ...

    def upsert_spec(self, record: ModelSpecRecord) -> ModelSpecRecord: ...

    def upsert_trial(self, record: ModelTrialRecord) -> ModelTrialRecord: ...

    def upsert_artifact(self, record: ModelArtifactRecord) -> ModelArtifactRecord: ...

    def list_qe_selectable_specs(self) -> list[ModelSpecRecord]: ...

    def transition_status(
        self,
        *,
        object_type: ModelObjectType,
        object_id: str,
        to_status: str,
        reason: str,
        operator: str,
        context_json: dict[str, Any] | None = None,
    ) -> ModelLifecycleEvent: ...


_HIDDEN_QE_SPEC_STATUSES = {
    SpecLifecycleStatus.QUARANTINED,
    SpecLifecycleStatus.TRAINING_FAILED,
    SpecLifecycleStatus.RETIRED,
}


class InMemoryModelRegistryRepository:
    """Unit-test model registry repository; it mirrors no-delete lifecycle semantics."""

    def __init__(self) -> None:
        self.templates: dict[str, ModelTemplateRecord] = {}
        self.specs: dict[str, ModelSpecRecord] = {}
        self.trials: dict[str, ModelTrialRecord] = {}
        self.artifacts: dict[str, ModelArtifactRecord] = {}
        self.events: list[ModelLifecycleEvent] = []

    def upsert_template(self, record: ModelTemplateRecord) -> ModelTemplateRecord:
        self.templates[record.template_id] = record
        return record

    def upsert_spec(self, record: ModelSpecRecord) -> ModelSpecRecord:
        if record.template_id not in self.templates:
            raise DataUnavailableError("model template is required before registering a spec", context={"template_id": record.template_id})
        self.specs[record.spec_id] = record
        return record

    def upsert_trial(self, record: ModelTrialRecord) -> ModelTrialRecord:
        if record.spec_id not in self.specs:
            raise DataUnavailableError("model spec is required before registering a trial", context={"spec_id": record.spec_id})
        self.trials[record.trial_id] = record
        return record

    def upsert_artifact(self, record: ModelArtifactRecord) -> ModelArtifactRecord:
        if record.trial_id not in self.trials:
            raise DataUnavailableError("model trial is required before registering an artifact", context={"trial_id": record.trial_id})
        if record.protected_asset and record.retention_class not in {"promoted", "protected"}:
            raise StrategyPackageValidationError("protected model artifacts must use promoted or protected retention")
        self.artifacts[record.artifact_id] = record
        return record

    def list_qe_selectable_specs(self) -> list[ModelSpecRecord]:
        result: list[ModelSpecRecord] = []
        for spec in self.specs.values():
            template = self.templates.get(spec.template_id)
            if template is None:
                raise DataUnavailableError("model spec references a missing template", context={"spec_id": spec.spec_id})
            if not spec.qe_selectable:
                continue
            if spec.lifecycle_status in _HIDDEN_QE_SPEC_STATUSES:
                continue
            if template.lifecycle_status in {TemplateLifecycleStatus.DEPRECATED, TemplateLifecycleStatus.RETIRED}:
                continue
            result.append(spec)
        return sorted(result, key=lambda item: item.spec_id)

    def transition_status(
        self,
        *,
        object_type: ModelObjectType,
        object_id: str,
        to_status: str,
        reason: str,
        operator: str,
        context_json: dict[str, Any] | None = None,
    ) -> ModelLifecycleEvent:
        _validate_transition_request(object_type=object_type, to_status=to_status, reason=reason, operator=operator)
        from_status: str | None
        if object_type == ModelObjectType.TEMPLATE:
            record = self.templates.get(object_id)
            if record is None:
                raise DataUnavailableError("model template not found", context={"template_id": object_id})
            from_status = record.lifecycle_status.value
            _validate_terminal_transition(from_status=from_status, to_status=to_status, object_type=object_type)
            self.templates[object_id] = record.model_copy(update={"lifecycle_status": TemplateLifecycleStatus(to_status)})
        elif object_type == ModelObjectType.SPEC:
            record = self.specs.get(object_id)
            if record is None:
                raise DataUnavailableError("model spec not found", context={"spec_id": object_id})
            from_status = record.lifecycle_status.value
            _validate_terminal_transition(from_status=from_status, to_status=to_status, object_type=object_type)
            self.specs[object_id] = record.model_copy(update={"lifecycle_status": SpecLifecycleStatus(to_status)})
        elif object_type == ModelObjectType.TRIAL:
            record = self.trials.get(object_id)
            if record is None:
                raise DataUnavailableError("model trial not found", context={"trial_id": object_id})
            from_status = record.status.value
            self.trials[object_id] = record.model_copy(update={"status": TrialStatus(to_status)})
        elif object_type == ModelObjectType.ARTIFACT:
            record = self.artifacts.get(object_id)
            if record is None:
                raise DataUnavailableError("model artifact not found", context={"artifact_id": object_id})
            from_status = record.artifact_status.value
            self.artifacts[object_id] = record.model_copy(update={"artifact_status": ArtifactStatus(to_status)})
        else:
            raise StrategyPackageValidationError("unsupported model registry object type")

        event = ModelLifecycleEvent(
            event_id=f"mle_{uuid4().hex}",
            object_type=object_type,
            object_id=object_id,
            from_status=from_status,
            to_status=to_status,
            reason=reason,
            operator=operator,
            context_json=context_json or {},
        )
        self.events.append(event)
        return event


class PostgresModelRegistryRepository:
    """PostgreSQL model registry repository; it performs data writes only, never DDL."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def upsert_template(self, record: ModelTemplateRecord) -> ModelTemplateRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO model_registry.model_template (
                        template_id, family, model_type, display_name, description, task_type,
                        supported_freq, supported_input_shape, train_backend, default_search_space,
                        default_train_budget, seed_capability, deterministic_support, gpu_required,
                        lifecycle_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (template_id) DO UPDATE SET
                        family = EXCLUDED.family,
                        model_type = EXCLUDED.model_type,
                        display_name = EXCLUDED.display_name,
                        description = EXCLUDED.description,
                        task_type = EXCLUDED.task_type,
                        supported_freq = EXCLUDED.supported_freq,
                        supported_input_shape = EXCLUDED.supported_input_shape,
                        train_backend = EXCLUDED.train_backend,
                        default_search_space = EXCLUDED.default_search_space,
                        default_train_budget = EXCLUDED.default_train_budget,
                        seed_capability = EXCLUDED.seed_capability,
                        deterministic_support = EXCLUDED.deterministic_support,
                        gpu_required = EXCLUDED.gpu_required,
                        lifecycle_status = EXCLUDED.lifecycle_status,
                        updated_at = NOW()
                    RETURNING *
                    """,
                    (
                        record.template_id,
                        record.family,
                        record.model_type,
                        record.display_name,
                        record.description,
                        record.task_type,
                        record.supported_freq,
                        record.supported_input_shape,
                        record.train_backend,
                        psycopg2.extras.Json(record.default_search_space),
                        psycopg2.extras.Json(record.default_train_budget),
                        record.seed_capability,
                        record.deterministic_support,
                        record.gpu_required,
                        record.lifecycle_status.value,
                    ),
                )
                return self._template_from_row(dict(cur.fetchone()))

    def upsert_spec(self, record: ModelSpecRecord) -> ModelSpecRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO model_registry.model_spec (
                        spec_id, template_id, spec_version, model_name, model_type, code_ref,
                        code_text, code_sha256, architecture_config, architecture_sha256,
                        hyperparam_schema, default_hyperparams, search_space_json,
                        input_contract_json, output_contract_json, feature_schema_requirements,
                        label_requirements, dependency_versions, source_type, source_task_id,
                        source_loop_id, lifecycle_status, qe_selectable, qe_selectability_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (spec_id) DO UPDATE SET
                        template_id = EXCLUDED.template_id,
                        spec_version = EXCLUDED.spec_version,
                        model_name = EXCLUDED.model_name,
                        model_type = EXCLUDED.model_type,
                        code_ref = EXCLUDED.code_ref,
                        code_text = EXCLUDED.code_text,
                        code_sha256 = EXCLUDED.code_sha256,
                        architecture_config = EXCLUDED.architecture_config,
                        architecture_sha256 = EXCLUDED.architecture_sha256,
                        hyperparam_schema = EXCLUDED.hyperparam_schema,
                        default_hyperparams = EXCLUDED.default_hyperparams,
                        search_space_json = EXCLUDED.search_space_json,
                        input_contract_json = EXCLUDED.input_contract_json,
                        output_contract_json = EXCLUDED.output_contract_json,
                        feature_schema_requirements = EXCLUDED.feature_schema_requirements,
                        label_requirements = EXCLUDED.label_requirements,
                        dependency_versions = EXCLUDED.dependency_versions,
                        source_type = EXCLUDED.source_type,
                        source_task_id = EXCLUDED.source_task_id,
                        source_loop_id = EXCLUDED.source_loop_id,
                        lifecycle_status = EXCLUDED.lifecycle_status,
                        qe_selectable = EXCLUDED.qe_selectable,
                        qe_selectability_reason = EXCLUDED.qe_selectability_reason,
                        updated_at = NOW()
                    RETURNING *
                    """,
                    (
                        record.spec_id,
                        record.template_id,
                        record.spec_version,
                        record.model_name,
                        record.model_type,
                        record.code_ref,
                        record.code_text,
                        record.code_sha256,
                        psycopg2.extras.Json(record.architecture_config),
                        record.architecture_sha256,
                        psycopg2.extras.Json(record.hyperparam_schema),
                        psycopg2.extras.Json(record.default_hyperparams),
                        psycopg2.extras.Json(record.search_space_json),
                        psycopg2.extras.Json(record.input_contract_json),
                        psycopg2.extras.Json(record.output_contract_json),
                        psycopg2.extras.Json(record.feature_schema_requirements),
                        psycopg2.extras.Json(record.label_requirements),
                        psycopg2.extras.Json(record.dependency_versions),
                        record.source_type,
                        record.source_task_id,
                        record.source_loop_id,
                        record.lifecycle_status.value,
                        record.qe_selectable,
                        record.qe_selectability_reason,
                    ),
                )
                return self._spec_from_row(dict(cur.fetchone()))

    def upsert_trial(self, record: ModelTrialRecord) -> ModelTrialRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO model_registry.model_trial (
                        trial_id, spec_id, qe_run_id, qe_experiment_id, qe_task_id, qe_loop_id,
                        factor_set_hash, factor_list_ordered, feature_schema_hash, data_context_id,
                        dataset_version, label_config_hash, train_start, train_end, valid_start,
                        valid_end, test_start, test_end, train_config_json, hyperparams_json,
                        seed_policy, random_seed, seed_sequence, deterministic_flags_json, status,
                        failure_reason, best_epoch, total_epochs, train_loss_final, val_loss_final,
                        training_curves, ic, rank_ic, icir, annualized_return, sharpe,
                        max_drawdown, turnover, cost_drag, score_total, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trial_id) DO UPDATE SET
                        spec_id = EXCLUDED.spec_id,
                        qe_run_id = EXCLUDED.qe_run_id,
                        qe_experiment_id = EXCLUDED.qe_experiment_id,
                        qe_task_id = EXCLUDED.qe_task_id,
                        qe_loop_id = EXCLUDED.qe_loop_id,
                        factor_set_hash = EXCLUDED.factor_set_hash,
                        factor_list_ordered = EXCLUDED.factor_list_ordered,
                        feature_schema_hash = EXCLUDED.feature_schema_hash,
                        data_context_id = EXCLUDED.data_context_id,
                        dataset_version = EXCLUDED.dataset_version,
                        label_config_hash = EXCLUDED.label_config_hash,
                        train_start = EXCLUDED.train_start,
                        train_end = EXCLUDED.train_end,
                        valid_start = EXCLUDED.valid_start,
                        valid_end = EXCLUDED.valid_end,
                        test_start = EXCLUDED.test_start,
                        test_end = EXCLUDED.test_end,
                        train_config_json = EXCLUDED.train_config_json,
                        hyperparams_json = EXCLUDED.hyperparams_json,
                        seed_policy = EXCLUDED.seed_policy,
                        random_seed = EXCLUDED.random_seed,
                        seed_sequence = EXCLUDED.seed_sequence,
                        deterministic_flags_json = EXCLUDED.deterministic_flags_json,
                        status = EXCLUDED.status,
                        failure_reason = EXCLUDED.failure_reason,
                        best_epoch = EXCLUDED.best_epoch,
                        total_epochs = EXCLUDED.total_epochs,
                        train_loss_final = EXCLUDED.train_loss_final,
                        val_loss_final = EXCLUDED.val_loss_final,
                        training_curves = EXCLUDED.training_curves,
                        ic = EXCLUDED.ic,
                        rank_ic = EXCLUDED.rank_ic,
                        icir = EXCLUDED.icir,
                        annualized_return = EXCLUDED.annualized_return,
                        sharpe = EXCLUDED.sharpe,
                        max_drawdown = EXCLUDED.max_drawdown,
                        turnover = EXCLUDED.turnover,
                        cost_drag = EXCLUDED.cost_drag,
                        score_total = EXCLUDED.score_total,
                        completed_at = EXCLUDED.completed_at
                    RETURNING *
                    """,
                    _trial_params(record),
                )
                return self._trial_from_row(dict(cur.fetchone()))

    def upsert_artifact(self, record: ModelArtifactRecord) -> ModelArtifactRecord:
        if record.protected_asset and record.retention_class not in {"promoted", "protected"}:
            raise StrategyPackageValidationError("protected model artifacts must use promoted or protected retention")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO model_registry.model_artifact (
                        artifact_id, trial_id, artifact_type, artifact_uri, artifact_sha256,
                        artifact_size_bytes, feature_schema_hash, feature_order_hash,
                        preprocessor_hash, model_format, retention_class, protected_asset,
                        artifact_status, validated_at, metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (artifact_id) DO UPDATE SET
                        trial_id = EXCLUDED.trial_id,
                        artifact_type = EXCLUDED.artifact_type,
                        artifact_uri = EXCLUDED.artifact_uri,
                        artifact_sha256 = EXCLUDED.artifact_sha256,
                        artifact_size_bytes = EXCLUDED.artifact_size_bytes,
                        feature_schema_hash = EXCLUDED.feature_schema_hash,
                        feature_order_hash = EXCLUDED.feature_order_hash,
                        preprocessor_hash = EXCLUDED.preprocessor_hash,
                        model_format = EXCLUDED.model_format,
                        retention_class = EXCLUDED.retention_class,
                        protected_asset = EXCLUDED.protected_asset,
                        artifact_status = EXCLUDED.artifact_status,
                        validated_at = EXCLUDED.validated_at,
                        metadata_json = EXCLUDED.metadata_json
                    RETURNING *
                    """,
                    (
                        record.artifact_id,
                        record.trial_id,
                        record.artifact_type,
                        record.artifact_uri,
                        record.artifact_sha256,
                        record.artifact_size_bytes,
                        record.feature_schema_hash,
                        record.feature_order_hash,
                        record.preprocessor_hash,
                        record.model_format,
                        record.retention_class,
                        record.protected_asset,
                        record.artifact_status.value,
                        record.validated_at,
                        psycopg2.extras.Json(record.metadata_json),
                    ),
                )
                return self._artifact_from_row(dict(cur.fetchone()))

    def list_qe_selectable_specs(self) -> list[ModelSpecRecord]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM model_registry.v_qe_selectable_model_spec
                    ORDER BY model_name ASC, spec_id ASC
                    """
                )
                return [self._spec_from_row(dict(row)) for row in cur.fetchall()]

    def transition_status(
        self,
        *,
        object_type: ModelObjectType,
        object_id: str,
        to_status: str,
        reason: str,
        operator: str,
        context_json: dict[str, Any] | None = None,
    ) -> ModelLifecycleEvent:
        _validate_transition_request(object_type=object_type, to_status=to_status, reason=reason, operator=operator)
        table_name, id_column, status_column = self._transition_target(object_type)
        event_id = f"mle_{uuid4().hex}"
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"SELECT {status_column} AS status FROM {table_name} WHERE {id_column} = %s FOR UPDATE", (object_id,))
                row = cur.fetchone()
                if row is None:
                    raise DataUnavailableError("model registry object not found", context={"object_type": object_type.value, "object_id": object_id})
                from_status = row["status"]
                _validate_terminal_transition(from_status=from_status, to_status=to_status, object_type=object_type)
                cur.execute(f"UPDATE {table_name} SET {status_column} = %s WHERE {id_column} = %s", (to_status, object_id))
                cur.execute(
                    """
                    INSERT INTO model_registry.model_lifecycle_event (
                        event_id, object_type, object_id, from_status, to_status, reason, operator, context_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        event_id,
                        object_type.value,
                        object_id,
                        from_status,
                        to_status,
                        reason,
                        operator,
                        psycopg2.extras.Json(context_json or {}),
                    ),
                )
                return self._event_from_row(dict(cur.fetchone()))

    @staticmethod
    def _transition_target(object_type: ModelObjectType) -> tuple[str, str, str]:
        if object_type == ModelObjectType.TEMPLATE:
            return "model_registry.model_template", "template_id", "lifecycle_status"
        if object_type == ModelObjectType.SPEC:
            return "model_registry.model_spec", "spec_id", "lifecycle_status"
        if object_type == ModelObjectType.TRIAL:
            return "model_registry.model_trial", "trial_id", "status"
        if object_type == ModelObjectType.ARTIFACT:
            return "model_registry.model_artifact", "artifact_id", "artifact_status"
        raise StrategyPackageValidationError("unsupported model registry object type")

    @staticmethod
    def _template_from_row(row: dict[str, Any]) -> ModelTemplateRecord:
        return ModelTemplateRecord(
            template_id=row["template_id"],
            family=row["family"],
            model_type=row["model_type"],
            display_name=row["display_name"],
            description=row.get("description"),
            task_type=row.get("task_type") or "rank",
            supported_freq=list(row.get("supported_freq") or ["day"]),
            supported_input_shape=row.get("supported_input_shape") or "tabular",
            train_backend=row.get("train_backend") or "qlib",
            default_search_space=row.get("default_search_space") or {},
            default_train_budget=row.get("default_train_budget") or {},
            seed_capability=row.get("seed_capability") or "unset_legacy",
            deterministic_support=row.get("deterministic_support") or "partial",
            gpu_required=bool(row.get("gpu_required")),
            lifecycle_status=TemplateLifecycleStatus(row.get("lifecycle_status") or "active"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _spec_from_row(row: dict[str, Any]) -> ModelSpecRecord:
        return ModelSpecRecord(
            spec_id=row["spec_id"],
            template_id=row["template_id"],
            spec_version=row.get("spec_version") or "v1",
            model_name=row["model_name"],
            model_type=row["model_type"],
            code_ref=row.get("code_ref"),
            code_text=row.get("code_text"),
            code_sha256=row.get("code_sha256"),
            architecture_config=row.get("architecture_config") or {},
            architecture_sha256=row.get("architecture_sha256"),
            hyperparam_schema=row.get("hyperparam_schema") or {},
            default_hyperparams=row.get("default_hyperparams") or {},
            search_space_json=row.get("search_space_json") or {},
            input_contract_json=row.get("input_contract_json") or {},
            output_contract_json=row.get("output_contract_json") or {},
            feature_schema_requirements=row.get("feature_schema_requirements") or {},
            label_requirements=row.get("label_requirements") or {},
            dependency_versions=row.get("dependency_versions") or {},
            source_type=row.get("source_type") or "builtin",
            source_task_id=row.get("source_task_id"),
            source_loop_id=row.get("source_loop_id"),
            lifecycle_status=SpecLifecycleStatus(row.get("lifecycle_status") or "research_candidate"),
            qe_selectable=bool(row.get("qe_selectable", True)),
            qe_selectability_reason=row.get("qe_selectability_reason"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _trial_from_row(row: dict[str, Any]) -> ModelTrialRecord:
        return ModelTrialRecord(
            trial_id=row["trial_id"],
            spec_id=row["spec_id"],
            qe_run_id=row.get("qe_run_id"),
            qe_experiment_id=row.get("qe_experiment_id"),
            qe_task_id=row.get("qe_task_id"),
            qe_loop_id=row.get("qe_loop_id"),
            factor_set_hash=row.get("factor_set_hash"),
            factor_list_ordered=row.get("factor_list_ordered") or [],
            feature_schema_hash=row.get("feature_schema_hash"),
            data_context_id=row.get("data_context_id"),
            dataset_version=row.get("dataset_version"),
            label_config_hash=row.get("label_config_hash"),
            train_start=row.get("train_start"),
            train_end=row.get("train_end"),
            valid_start=row.get("valid_start"),
            valid_end=row.get("valid_end"),
            test_start=row.get("test_start"),
            test_end=row.get("test_end"),
            train_config_json=row.get("train_config_json") or {},
            hyperparams_json=row.get("hyperparams_json") or {},
            seed_policy=row.get("seed_policy") or "unset_legacy",
            random_seed=row.get("random_seed"),
            seed_sequence=row.get("seed_sequence") or {},
            deterministic_flags_json=row.get("deterministic_flags_json") or {},
            status=TrialStatus(row.get("status") or "succeeded"),
            failure_reason=row.get("failure_reason"),
            best_epoch=row.get("best_epoch"),
            total_epochs=row.get("total_epochs"),
            train_loss_final=row.get("train_loss_final"),
            val_loss_final=row.get("val_loss_final"),
            training_curves=row.get("training_curves") or {},
            ic=row.get("ic"),
            rank_ic=row.get("rank_ic"),
            icir=row.get("icir"),
            annualized_return=row.get("annualized_return"),
            sharpe=row.get("sharpe"),
            max_drawdown=row.get("max_drawdown"),
            turnover=row.get("turnover"),
            cost_drag=row.get("cost_drag"),
            score_total=row.get("score_total"),
            created_at=row["created_at"],
            completed_at=row.get("completed_at"),
        )

    @staticmethod
    def _artifact_from_row(row: dict[str, Any]) -> ModelArtifactRecord:
        return ModelArtifactRecord(
            artifact_id=row["artifact_id"],
            trial_id=row["trial_id"],
            artifact_type=row["artifact_type"],
            artifact_uri=row["artifact_uri"],
            artifact_sha256=row.get("artifact_sha256"),
            artifact_size_bytes=row.get("artifact_size_bytes"),
            feature_schema_hash=row.get("feature_schema_hash"),
            feature_order_hash=row.get("feature_order_hash"),
            preprocessor_hash=row.get("preprocessor_hash"),
            model_format=row.get("model_format"),
            retention_class=row.get("retention_class") or "archived",
            protected_asset=bool(row.get("protected_asset")),
            artifact_status=ArtifactStatus(row.get("artifact_status") or "present"),
            created_at=row["created_at"],
            validated_at=row.get("validated_at"),
            metadata_json=row.get("metadata_json") or {},
        )

    @staticmethod
    def _event_from_row(row: dict[str, Any]) -> ModelLifecycleEvent:
        return ModelLifecycleEvent(
            event_id=row["event_id"],
            object_type=ModelObjectType(row["object_type"]),
            object_id=row["object_id"],
            from_status=row.get("from_status"),
            to_status=row["to_status"],
            reason=row["reason"],
            operator=row["operator"],
            context_json=row.get("context_json") or {},
            created_at=row["created_at"],
        )


class ModelRegistryService:
    def __init__(self, repository: ModelRegistryRepository | None = None) -> None:
        self.repository = repository or PostgresModelRegistryRepository()

    def register_template(self, record: ModelTemplateRecord) -> ModelTemplateRecord:
        self._require_id(record.template_id, "template_id")
        if not record.display_name.strip():
            raise StrategyPackageValidationError("display_name is required for model template")
        return self.repository.upsert_template(record)

    def register_spec(self, record: ModelSpecRecord) -> ModelSpecRecord:
        self._require_id(record.spec_id, "spec_id")
        self._require_id(record.template_id, "template_id")
        if record.lifecycle_status in _HIDDEN_QE_SPEC_STATUSES and record.qe_selectable:
            record = record.model_copy(update={"qe_selectable": False, "qe_selectability_reason": record.qe_selectability_reason or "hidden by lifecycle status"})
        return self.repository.upsert_spec(record)

    def register_trial(self, record: ModelTrialRecord) -> ModelTrialRecord:
        self._require_id(record.trial_id, "trial_id")
        self._require_id(record.spec_id, "spec_id")
        return self.repository.upsert_trial(record)

    def register_artifact(self, record: ModelArtifactRecord) -> ModelArtifactRecord:
        self._require_id(record.artifact_id, "artifact_id")
        self._require_id(record.trial_id, "trial_id")
        if not record.artifact_uri.strip():
            raise StrategyPackageValidationError("artifact_uri is required for model artifact")
        return self.repository.upsert_artifact(record)

    def list_qe_selectable_specs(self) -> list[ModelSpecRecord]:
        return self.repository.list_qe_selectable_specs()

    def transition_status(
        self,
        *,
        object_type: ModelObjectType,
        object_id: str,
        to_status: str,
        reason: str,
        operator: str,
        context_json: dict[str, Any] | None = None,
    ) -> ModelLifecycleEvent:
        self._require_id(object_id, "object_id")
        _validate_transition_request(object_type=object_type, to_status=to_status, reason=reason, operator=operator)
        return self.repository.transition_status(
            object_type=object_type,
            object_id=object_id,
            to_status=to_status,
            reason=reason,
            operator=operator,
            context_json=context_json,
        )

    @staticmethod
    def _require_id(value: str, field_name: str) -> None:
        if not value or not value.strip():
            raise StrategyPackageValidationError(f"{field_name} is required")


def _trial_params(record: ModelTrialRecord) -> tuple[Any, ...]:
    return (
        record.trial_id,
        record.spec_id,
        record.qe_run_id,
        record.qe_experiment_id,
        record.qe_task_id,
        record.qe_loop_id,
        record.factor_set_hash,
        psycopg2.extras.Json(record.factor_list_ordered),
        record.feature_schema_hash,
        record.data_context_id,
        record.dataset_version,
        record.label_config_hash,
        record.train_start,
        record.train_end,
        record.valid_start,
        record.valid_end,
        record.test_start,
        record.test_end,
        psycopg2.extras.Json(record.train_config_json),
        psycopg2.extras.Json(record.hyperparams_json),
        record.seed_policy,
        record.random_seed,
        psycopg2.extras.Json(record.seed_sequence),
        psycopg2.extras.Json(record.deterministic_flags_json),
        record.status.value,
        record.failure_reason,
        record.best_epoch,
        record.total_epochs,
        record.train_loss_final,
        record.val_loss_final,
        psycopg2.extras.Json(record.training_curves),
        record.ic,
        record.rank_ic,
        record.icir,
        record.annualized_return,
        record.sharpe,
        record.max_drawdown,
        record.turnover,
        record.cost_drag,
        record.score_total,
        record.completed_at,
    )


def _validate_transition_request(*, object_type: ModelObjectType, to_status: str, reason: str, operator: str) -> None:
    if not reason.strip() or not operator.strip():
        raise StrategyPackageValidationError("reason and operator are required for model lifecycle transitions")
    if not to_status.strip():
        raise StrategyPackageValidationError("to_status is required for model lifecycle transition")
    try:
        if object_type == ModelObjectType.TEMPLATE:
            TemplateLifecycleStatus(to_status)
        elif object_type == ModelObjectType.SPEC:
            SpecLifecycleStatus(to_status)
        elif object_type == ModelObjectType.TRIAL:
            TrialStatus(to_status)
        elif object_type == ModelObjectType.ARTIFACT:
            ArtifactStatus(to_status)
        else:
            raise StrategyPackageValidationError("unsupported model registry object type")
    except ValueError as exc:
        raise StrategyPackageValidationError(
            "invalid model registry target status",
            context={"object_type": object_type.value, "to_status": to_status},
        ) from exc


def _validate_terminal_transition(*, from_status: str | None, to_status: str, object_type: ModelObjectType) -> None:
    if object_type in {ModelObjectType.TEMPLATE, ModelObjectType.SPEC} and from_status == "retired" and to_status != "retired":
        raise InvalidStateTransitionError(
            "retired model registry objects are terminal",
            context={"object_type": object_type.value, "from_status": from_status, "to_status": to_status},
        )
