from __future__ import annotations

import inspect
import re
from pathlib import Path

from fastapi import FastAPI
from fastapi import HTTPException
import pytest
from fastapi.testclient import TestClient

import backend.routers.model_registry as model_registry_router
from backend.services.model_registry import (
    InMemoryModelRegistryRepository,
    LegacyModelCatalogBridgeRecord,
    ModelCatalogCompatRecord,
    ModelArtifactRecord,
    ModelRegistryService,
    ModelSpecRecord,
    ModelTemplateRecord,
    ModelTrialRecord,
)
from backend.services.model_registry.registry import (
    ModelObjectType,
    PostgresModelRegistryRepository,
    SpecLifecycleStatus,
    TemplateLifecycleStatus,
    TrialStatus,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError

REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend" / "migrations" / "model_registry_phase5_20260509.sql"


def _migration_sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _table_columns(sql: str, table: str) -> list[str]:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS model_registry\.{table} \((.*?)\n\);",
        sql,
        flags=re.DOTALL,
    )
    assert match, f"missing table {table}"
    columns: list[str] = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("CONSTRAINT"):
            continue
        columns.append(line.split()[0])
    return columns


def test_phase5_migration_creates_only_model_registry_schema_tables_with_comments() -> None:
    sql = _migration_sql()
    assert "CREATE SCHEMA IF NOT EXISTS model_registry" in sql
    assert "CREATE TABLE IF NOT EXISTS public." not in sql
    expected_tables = [
        "model_template",
        "model_spec",
        "model_trial",
        "model_artifact",
        "model_lifecycle_event",
    ]
    for table in expected_tables:
        assert f"CREATE TABLE IF NOT EXISTS model_registry.{table}" in sql
        assert f"COMMENT ON TABLE model_registry.{table}" in sql
        for column in _table_columns(sql, table):
            assert f"COMMENT ON COLUMN model_registry.{table}.{column}" in sql, f"missing comment for {table}.{column}"


def test_phase5_migration_defines_qe_selector_and_legacy_bridge_views() -> None:
    sql = _migration_sql()
    assert "CREATE OR REPLACE VIEW model_registry.v_qe_selectable_model_spec" in sql
    assert "s.qe_selectable = TRUE" in sql
    assert "s.lifecycle_status NOT IN ('quarantined', 'training_failed', 'retired')" in sql
    assert "t.lifecycle_status NOT IN ('deprecated', 'retired')" in sql
    assert "CREATE OR REPLACE VIEW model_registry.v_model_catalog_compat" in sql
    assert "FALSE::BOOLEAN AS paper_selectable" in sql
    assert "CREATE OR REPLACE VIEW model_registry.v_legacy_aistock_model_catalog_bridge" in sql
    assert "FROM public.aistock_model_catalog" in sql
    assert "THEN FALSE ELSE TRUE END AS qe_selectable" in sql
    for field in (
        "s.code_text",
        "s.code_sha256",
        "s.architecture_config",
        "s.hyperparam_schema",
        "s.feature_schema_requirements",
        "s.label_requirements",
        "s.dependency_versions",
        "s.source_type",
        "s.source_task_id",
        "s.source_loop_id",
    ):
        assert field in sql


def test_model_spec_record_accepts_complete_phase5_contract_payload() -> None:
    record = ModelSpecRecord(
        spec_id="spec_complete",
        template_id="tpl",
        model_name="complete",
        model_type="LGBModel",
        code_text="class Model: pass",
        architecture_config={"layers": 2},
        hyperparam_schema={"type": "object"},
        input_contract_json={"features": ["alpha_1"]},
        output_contract_json={"prediction": "score"},
        feature_schema_requirements={"feature_order_required": True},
        label_requirements={"horizon": "1D"},
        dependency_versions={"qlib": "0.9"},
    )

    assert record.code_text == "class Model: pass"
    assert record.architecture_config["layers"] == 2
    assert record.input_contract_json["features"] == ["alpha_1"]


def test_model_registry_default_qe_selector_hides_failed_quarantined_retired_and_deprecated() -> None:
    repo = InMemoryModelRegistryRepository()
    service = ModelRegistryService(repo)
    service.register_template(ModelTemplateRecord(template_id="tpl_active", family="boosting", model_type="LGBModel", display_name="LGB"))
    service.register_template(
        ModelTemplateRecord(
            template_id="tpl_deprecated",
            family="boosting",
            model_type="CatBoost",
            display_name="Old CatBoost",
            lifecycle_status=TemplateLifecycleStatus.DEPRECATED,
        )
    )
    service.register_spec(ModelSpecRecord(spec_id="spec_ok", template_id="tpl_active", model_name="ok", model_type="LGBModel"))
    service.register_spec(
        ModelSpecRecord(
            spec_id="spec_quarantined",
            template_id="tpl_active",
            model_name="bad",
            model_type="LGBModel",
            lifecycle_status=SpecLifecycleStatus.QUARANTINED,
        )
    )
    service.register_spec(
        ModelSpecRecord(
            spec_id="spec_retired",
            template_id="tpl_active",
            model_name="old",
            model_type="LGBModel",
            lifecycle_status=SpecLifecycleStatus.RETIRED,
        )
    )
    service.register_spec(
        ModelSpecRecord(
            spec_id="spec_training_failed",
            template_id="tpl_active",
            model_name="failed",
            model_type="LGBModel",
            lifecycle_status=SpecLifecycleStatus.TRAINING_FAILED,
        )
    )
    service.register_spec(
        ModelSpecRecord(
            spec_id="spec_deprecated_template",
            template_id="tpl_deprecated",
            model_name="deprecated template",
            model_type="CatBoost",
        )
    )

    assert [item.spec_id for item in service.list_qe_selectable_specs()] == ["spec_ok"]
    assert repo.specs["spec_quarantined"].qe_selectable is False
    assert repo.specs["spec_training_failed"].qe_selectable is False


def test_model_registry_lifecycle_transition_is_append_only_audit_event() -> None:
    repo = InMemoryModelRegistryRepository()
    service = ModelRegistryService(repo)
    service.register_template(ModelTemplateRecord(template_id="tpl", family="boosting", model_type="LGBModel", display_name="LGB"))
    service.register_spec(ModelSpecRecord(spec_id="spec", template_id="tpl", model_name="candidate", model_type="LGBModel"))

    event = service.transition_status(
        object_type=ModelObjectType.SPEC,
        object_id="spec",
        to_status="quarantined",
        reason="bad feature schema hash",
        operator="unit_test",
        context_json={"source": "phase5"},
    )

    assert event.from_status == "research_candidate"
    assert event.to_status == "quarantined"
    assert repo.specs["spec"].lifecycle_status == SpecLifecycleStatus.QUARANTINED
    assert repo.events == [event]


def test_model_registry_registers_trial_and_artifact_layers() -> None:
    repo = InMemoryModelRegistryRepository()
    service = ModelRegistryService(repo)
    service.register_template(ModelTemplateRecord(template_id="tpl", family="boosting", model_type="LGBModel", display_name="LGB"))
    service.register_spec(ModelSpecRecord(spec_id="spec", template_id="tpl", model_name="candidate", model_type="LGBModel"))

    trial = service.register_trial(
        ModelTrialRecord(
            trial_id="trial_1",
            spec_id="spec",
            qe_task_id="task_1",
            qe_loop_id="Loop1",
            status=TrialStatus.SUCCEEDED,
            factor_list_ordered=["alpha_1", "alpha_2"],
            seed_policy="fixed",
            random_seed=42,
        )
    )
    artifact = service.register_artifact(
        ModelArtifactRecord(
            artifact_id="artifact_1",
            trial_id="trial_1",
            artifact_type="weights",
            artifact_uri="model_registry://artifact_1/weights.pkl",
            retention_class="protected",
            protected_asset=True,
        )
    )

    assert repo.trials["trial_1"] == trial
    assert repo.artifacts["artifact_1"] == artifact


def test_model_registry_lists_catalog_compat_and_legacy_bridge_read_models() -> None:
    repo = InMemoryModelRegistryRepository()
    repo.catalog_compat.append(
        ModelCatalogCompatRecord(
            model_id="spec_ok",
            model_name="ok",
            model_type="LGBModel",
            lifecycle_status="research_candidate",
            qe_selectable=True,
            paper_selectable=False,
        )
    )
    repo.legacy_catalog_bridge.extend(
        [
            LegacyModelCatalogBridgeRecord(
                legacy_model_id="legacy_ok",
                model_type="LGBModel",
                lifecycle_status="research_candidate",
                qe_selectable=True,
                paper_selectable=False,
            ),
            LegacyModelCatalogBridgeRecord(
                legacy_model_id="legacy_failed",
                model_type="LGBModel",
                lifecycle_status="training_failed",
                qe_selectable=False,
                paper_selectable=False,
            ),
        ]
    )
    service = ModelRegistryService(repo)

    compat = service.list_model_catalog_compat(qe_selectable=True)
    legacy = service.list_legacy_catalog_bridge(include_training_failed=False)

    assert [item.model_id for item in compat] == ["spec_ok"]
    assert [item.legacy_model_id for item in legacy] == ["legacy_ok"]
    assert compat[0].paper_selectable is False
    assert legacy[0].paper_selectable is False


def test_model_registry_list_pagination_fails_fast() -> None:
    service = ModelRegistryService(InMemoryModelRegistryRepository())

    with pytest.raises(StrategyPackageValidationError, match="limit"):
        service.list_model_catalog_compat(limit=0)
    with pytest.raises(StrategyPackageValidationError, match="offset"):
        service.list_legacy_catalog_bridge(offset=-1)


def test_model_registry_lifecycle_transition_fails_fast_for_missing_objects_and_empty_reason() -> None:
    repo = InMemoryModelRegistryRepository()
    service = ModelRegistryService(repo)
    with pytest.raises(DataUnavailableError):
        service.transition_status(
            object_type=ModelObjectType.SPEC,
            object_id="missing",
            to_status="retired",
            reason="not used",
            operator="unit_test",
        )
    service.register_template(ModelTemplateRecord(template_id="tpl", family="boosting", model_type="LGBModel", display_name="LGB"))
    with pytest.raises(StrategyPackageValidationError):
        service.transition_status(
            object_type=ModelObjectType.TEMPLATE,
            object_id="tpl",
            to_status="retired",
            reason=" ",
            operator="unit_test",
        )


def test_model_registry_lifecycle_transition_rejects_invalid_status_as_domain_error() -> None:
    repo = InMemoryModelRegistryRepository()
    service = ModelRegistryService(repo)
    service.register_template(ModelTemplateRecord(template_id="tpl", family="boosting", model_type="LGBModel", display_name="LGB"))
    service.register_spec(ModelSpecRecord(spec_id="spec", template_id="tpl", model_name="candidate", model_type="LGBModel"))
    with pytest.raises(StrategyPackageValidationError, match="invalid model registry target status"):
        service.transition_status(
            object_type=ModelObjectType.SPEC,
            object_id="spec",
            to_status="not_a_status",
            reason="bad request",
            operator="unit_test",
        )


def test_model_registry_lifecycle_retired_status_is_terminal() -> None:
    repo = InMemoryModelRegistryRepository()
    service = ModelRegistryService(repo)
    service.register_template(ModelTemplateRecord(template_id="tpl", family="boosting", model_type="LGBModel", display_name="LGB"))
    service.register_spec(ModelSpecRecord(spec_id="spec", template_id="tpl", model_name="candidate", model_type="LGBModel"))

    service.transition_status(
        object_type=ModelObjectType.SPEC,
        object_id="spec",
        to_status="retired",
        reason="retire old candidate",
        operator="unit_test",
    )
    with pytest.raises(InvalidStateTransitionError, match="terminal"):
        service.transition_status(
            object_type=ModelObjectType.SPEC,
            object_id="spec",
            to_status="research_candidate",
            reason="should fail",
            operator="unit_test",
        )


def test_model_registry_write_api_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED", raising=False)
    with pytest.raises(HTTPException) as exc:
        model_registry_router._assert_write_api_enabled()

    assert exc.value.status_code == 403
    assert exc.value.detail["error_code"] == "MODEL_REGISTRY_WRITE_API_DISABLED"


def test_model_registry_post_routes_return_403_before_db_access(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED", raising=False)
    app = FastAPI()
    app.include_router(model_registry_router.router)
    client = TestClient(app)

    requests = [
        ("/templates", {"template_id": "tpl", "family": "boosting", "model_type": "LGBModel", "display_name": "LGB"}),
        ("/specs", {"spec_id": "spec", "template_id": "tpl", "model_name": "spec", "model_type": "LGBModel"}),
        ("/trials", {"trial_id": "trial", "spec_id": "spec"}),
        ("/artifacts", {"artifact_id": "artifact", "trial_id": "trial", "artifact_type": "weights", "artifact_uri": "memory://weights"}),
        ("/lifecycle-events", {"object_type": "spec", "object_id": "spec", "to_status": "retired", "reason": "guard"}),
    ]
    for path, payload in requests:
        response = client.post(f"/model-registry{path}", json=payload)
        assert response.status_code == 403
        assert response.json()["detail"]["error_code"] == "MODEL_REGISTRY_WRITE_API_DISABLED"


def test_model_registry_read_routes_do_not_require_write_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeService:
        def list_qe_selectable_specs(self) -> list[ModelSpecRecord]:
            return []

        def list_model_catalog_compat(
            self,
            *,
            limit: int,
            offset: int,
            qe_selectable: bool | None = None,
        ) -> list[ModelCatalogCompatRecord]:
            assert (limit, offset, qe_selectable) == (2, 1, True)
            return [
                ModelCatalogCompatRecord(
                    model_id="spec_ok",
                    model_name="ok",
                    lifecycle_status="research_candidate",
                    qe_selectable=True,
                    paper_selectable=False,
                )
            ]

        def list_legacy_catalog_bridge(
            self,
            *,
            limit: int,
            offset: int,
            qe_selectable: bool | None = None,
            include_training_failed: bool = True,
        ) -> list[LegacyModelCatalogBridgeRecord]:
            assert (limit, offset, qe_selectable, include_training_failed) == (3, 0, False, False)
            return [
                LegacyModelCatalogBridgeRecord(
                    legacy_model_id="legacy_failed",
                    lifecycle_status="training_failed",
                    qe_selectable=False,
                    paper_selectable=False,
                )
            ]

    monkeypatch.delenv("AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED", raising=False)
    monkeypatch.setattr(model_registry_router, "_service", lambda: FakeService())
    app = FastAPI()
    app.include_router(model_registry_router.router)
    client = TestClient(app)

    compat = client.get("/model-registry/catalog-compat?limit=2&offset=1&qe_selectable=true")
    legacy = client.get("/model-registry/legacy-catalog-bridge?limit=3&qe_selectable=false&include_training_failed=false")

    assert compat.status_code == 200
    assert compat.json()["items"][0]["model_id"] == "spec_ok"
    assert compat.json()["items"][0]["paper_selectable"] is False
    assert legacy.status_code == 200
    assert legacy.json()["items"][0]["legacy_model_id"] == "legacy_failed"
    assert legacy.json()["items"][0]["paper_selectable"] is False


def test_model_registry_router_exposes_write_guard_and_trial_artifact_endpoints() -> None:
    source = inspect.getsource(model_registry_router)
    assert "AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED" in source
    assert "@router.post(\"/trials\"" in source
    assert "@router.post(\"/artifacts\"" in source
    assert "@router.get(\"/catalog-compat\"" in source
    assert "@router.get(\"/legacy-catalog-bridge\"" in source
    assert source.count("_assert_write_api_enabled()") >= 5


def test_postgres_repository_uses_lifecycle_update_and_event_not_delete() -> None:
    source = inspect.getsource(PostgresModelRegistryRepository.transition_status)
    assert "UPDATE {table_name}" in source
    assert "model_registry.model_lifecycle_event" in source
    assert "DELETE" not in source.upper()


def test_postgres_repository_uses_read_only_model_registry_views_for_catalog_bridge() -> None:
    compat_source = inspect.getsource(PostgresModelRegistryRepository.list_model_catalog_compat)
    legacy_source = inspect.getsource(PostgresModelRegistryRepository.list_legacy_catalog_bridge)

    assert "model_registry.v_model_catalog_compat" in compat_source
    assert "model_registry.v_legacy_aistock_model_catalog_bridge" in legacy_source
    combined = f"{compat_source}\n{legacy_source}".upper()
    assert "INSERT" not in combined
    assert "UPDATE" not in combined
    assert "DELETE" not in combined
