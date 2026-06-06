from __future__ import annotations

import asyncio
import pytest

from backend.services.qe_templates.models import QETemplateRecord
from backend.services.qe_templates.materializer import QETemplateMaterializer
from backend.services.qe_templates.validator import validate_template_payload


def test_single_template_rejects_multi_alpha() -> None:
    result = validate_template_payload("single_experiment", {"alpha_mode": "multi", "factor_names": ["f"], "model_id": "m"})
    assert result["valid"] is False
    assert any("multi-alpha" in error for error in result["errors"])


def test_custom_evo_requires_loops() -> None:
    result = validate_template_payload("custom_evo", {"loops": []})
    assert result["valid"] is False
    assert "custom_evo config requires non-empty loops" in result["errors"]


def test_custom_evo_warns_when_runtime_metadata_is_nested_in_strategy_params() -> None:
    result = validate_template_payload(
        "custom_evo",
        {
            "loops": [
                {
                    "factor_keys": ["Alpha001||alpha158"],
                    "model_id": "model_lgbm_v1",
                    "strategy_params": {"topk": 20, "archive_policy": "SKIP", "random_seed": 42},
                }
            ]
        },
    )

    assert result["valid"] is True
    assert any("runtime metadata" in warning for warning in result["warnings"])


def test_custom_evo_rejects_future_stock_pool_for_default_historical_window() -> None:
    result = validate_template_payload(
        "custom_evo",
        {
            "loops": [
                {
                    "factor_keys": ["Alpha001||alpha158"],
                    "model_id": "model_lgbm_v1",
                    "stock_pool": "filtered_pool_20260519",
                    "runtime_flags": {"random_seed": 20260529},
                }
            ]
        },
    )

    assert result["valid"] is False
    assert any("QE_STOCK_POOL_DATE_OUT_OF_WINDOW" in error for error in result["errors"])


def test_custom_evo_accepts_pit_stock_pool_at_test_end() -> None:
    result = validate_template_payload(
        "custom_evo",
        {
            "data_split": {
                "train_start": "2018-08-01",
                "train_end": "2022-12-31",
                "valid_start": "2023-01-01",
                "valid_end": "2024-06-30",
                "test_start": "2024-07-01",
                "test_end": "2026-04-28",
                "backtest_end": "2026-04-27",
            },
            "loops": [
                {
                    "factor_keys": ["Alpha001||alpha158"],
                    "model_id": "model_lgbm_v1",
                    "strategy_params": {"topk": 20, "stock_pool": "filtered_pool_20260428"},
                    "runtime_flags": {"random_seed": 20260529},
                }
            ],
        },
    )

    assert result["valid"] is True
    assert result["errors"] == []


def test_template_record_hashes_config_and_normalizes_archive_policy() -> None:
    record = QETemplateRecord(
        template_kind="single_experiment",
        title="smoke",
        config_json={"factor_names": ["f1"], "model_id": "lgb"},
        archive_policy="skip",
    )
    assert record.config_sha256
    assert record.archive_policy == "SKIP"


def test_single_template_materializer_reuses_quantevolver_generate_config(monkeypatch) -> None:
    calls = {}

    def fake_generate_existing_api(payload):  # type: ignore[no-untyped-def]
        calls["payload"] = payload
        return {"ok": True, "experiment_id": "qe_template_1"}

    class FakeRepository:
        def get(self, template_id):  # type: ignore[no-untyped-def]
            return {
                "template_id": template_id,
                "template_kind": "single_experiment",
                "title": "template smoke",
                "status": "approved",
                "archive_policy": "SKIP",
                "archive_reason": "unit skip",
                "config_json": {
                    "factor_names": ["f1"],
                    "model_id": "lgb",
                    "strategy_id": "topk",
                    "custom_params": {"label_horizon": 3, "random_seed": 42},
                    "unfilled_handler": "TAIL_SUBSTITUTE",
                    "unfilled_handler_params": {"backup_depth": 15},
                },
            }

        def mark_materialized(self, template_id, **kwargs):  # type: ignore[no-untyped-def]
            calls["mark"] = {"template_id": template_id, **kwargs}
            return {"template_id": template_id, "status": "materialized"}

    monkeypatch.setattr(
        "backend.services.qe_templates.materializer._generate_single_experiment_through_existing_api",
        fake_generate_existing_api,
    )

    result = QETemplateMaterializer(repository=FakeRepository())._materialize_single(FakeRepository().get("qet_1"))  # type: ignore[arg-type]

    assert result["materialized"]["experiment_id"] == "qe_template_1"
    assert calls["payload"]["factor_names"] == ["f1"]
    assert calls["payload"]["custom_params"]["archive_policy"] == "SKIP"
    assert calls["payload"]["custom_params"]["archive_reason"] == "unit skip"
    assert calls["payload"]["custom_params"]["label_horizon"] == 3
    assert calls["payload"]["unfilled_handler"] == "TAIL_SUBSTITUTE"
    assert calls["mark"]["experiment_id"] == "qe_template_1"


def test_single_template_materializer_surfaces_existing_generate_config_errors(monkeypatch) -> None:
    from fastapi import HTTPException

    def fake_generate_existing_api(payload):  # type: ignore[no-untyped-def]
        raise HTTPException(status_code=400, detail="model_id='bad' 在模型目录中不存在")

    monkeypatch.setattr(
        "backend.services.qe_templates.materializer._generate_single_experiment_through_existing_api",
        fake_generate_existing_api,
    )

    with pytest.raises(ValueError, match="模型目录"):
        QETemplateMaterializer(repository=None)._materialize_single(  # type: ignore[arg-type]
            {
                "template_id": "qet_bad",
                "template_kind": "single_experiment",
                "title": "bad",
                "archive_policy": "AUTO",
                "config_json": {"factor_names": ["f1"], "model_id": "bad", "custom_params": {"random_seed": 42}},
            }
        )


def test_custom_evo_materializer_keeps_archive_policy_out_of_strategy_params(monkeypatch) -> None:
    calls = {}

    async def fake_prepare(loop_models, *, request_node_id, node_parallelism_payload):  # type: ignore[no-untyped-def]
        loop = loop_models[0].model_dump() if hasattr(loop_models[0], "model_dump") else loop_models[0].dict()
        loop["strategy_params"] = {"topk": 20}
        loop["runtime_flags"] = {"random_seed": 42}
        calls["prepare"] = {
            "request_node_id": request_node_id,
            "node_parallelism_payload": node_parallelism_payload,
        }
        return [loop], "local", {}

    class FakeScheduler:
        async def create_custom_evo_task(self, **kwargs):  # type: ignore[no-untyped-def]
            calls["task_kwargs"] = kwargs
            return "qe_template_task"

    class FakeRepository:
        def get(self, template_id):  # type: ignore[no-untyped-def]
            return {
                "template_id": template_id,
                "template_kind": "custom_evo",
                "title": "custom template smoke",
                "description": "unit",
                "status": "approved",
                "archive_policy": "MANUAL_ONLY",
                "archive_reason": "unit manual",
                "config_json": {
                    "task_name": "custom task",
                    "loops": [
                        {
                            "factor_keys": ["Alpha001||alpha158"],
                            "model_id": "model_lgbm_v1",
                            "strategy_params": {"topk": 20},
                            "runtime_flags": {"random_seed": 42},
                        }
                    ],
                },
            }

        def mark_materialized(self, template_id, **kwargs):  # type: ignore[no-untyped-def]
            calls["mark"] = {"template_id": template_id, **kwargs}
            return {"template_id": template_id, "status": "materialized"}

    monkeypatch.setattr("backend.routers.quantevolver_evolution._prepare_custom_evo_loop_configs", fake_prepare)
    monkeypatch.setattr("backend.routers.quantevolver_evolution.scheduler", FakeScheduler())

    result = asyncio.run(QETemplateMaterializer(repository=FakeRepository()).materialize("qet_custom"))  # type: ignore[arg-type]

    loop = calls["task_kwargs"]["loops_config"][0]
    assert result["materialized"]["task_id"] == "qe_template_task"
    assert loop["strategy_params"] == {"topk": 20}
    assert loop["runtime_flags"]["random_seed"] == 42
    assert loop["runtime_flags"]["archive_policy"] == "MANUAL_ONLY"
    assert loop["runtime_flags"]["archive_reason"] == "unit manual"
    assert "archive_policy" not in loop["strategy_params"]


def test_template_update_resets_review_when_config_changes() -> None:
    from backend.routers.qe_templates import QETemplateUpdateRequest, _template_update_payload

    updates = _template_update_payload(
        {
            "template_id": "qet_1",
            "status": "approved",
            "submitted_experiment_id": "qe_old",
            "submitted_task_id": None,
        },
        QETemplateUpdateRequest(config_json={"factor_names": ["f2"], "model_id": "catboost"}),
    )

    assert updates["status"] == "draft"
    assert updates["validation_json"] == {}
    assert updates["approval_json"] == {}
    assert updates["submitted_experiment_id"] is None
    assert updates["runtime_diff_json"] == {}


def test_template_update_rejects_executed_or_materialized_rows() -> None:
    from backend.routers.qe_templates import QETemplateUpdateRequest, _template_update_payload

    with pytest.raises(ValueError, match="does not allow editing"):
        _template_update_payload(
            {"template_id": "qet_1", "status": "materialized"},
            QETemplateUpdateRequest(config_json={"factor_names": ["f2"], "model_id": "catboost"}),
        )


def test_template_update_rejects_direct_status_mutation() -> None:
    from backend.routers.qe_templates import QETemplateUpdateRequest, _template_update_payload

    with pytest.raises(ValueError, match="status must be changed"):
        _template_update_payload(
            {"template_id": "qet_1", "status": "draft"},
            QETemplateUpdateRequest(status="approved"),
        )


def test_materializer_requires_manual_approval_before_materialize() -> None:
    class FakeRepository:
        def get(self, template_id):  # type: ignore[no-untyped-def]
            return {
                "template_id": template_id,
                "template_kind": "single_experiment",
                "title": "draft smoke",
                "status": "ready_for_review",
                "archive_policy": "AUTO",
                "config_json": {"factor_names": ["f1"], "model_id": "lgb"},
            }

    with pytest.raises(ValueError, match="before approval"):
        asyncio.run(QETemplateMaterializer(repository=FakeRepository()).materialize("qet_1"))  # type: ignore[arg-type]


def test_template_repository_delete_pending_allows_only_unmaterialized_rows() -> None:
    from backend.services.qe_templates.repository import hard_delete_blocker

    assert hard_delete_blocker(
        "qet_draft",
        {"template_id": "qet_draft", "status": "draft", "submitted_experiment_id": None, "submitted_task_id": None, "runtime_config_sha256": None},
    ) is None
    assert hard_delete_blocker("qet_missing", None) == "template not found: qet_missing"
    assert "only allowed before materialization" in (
        hard_delete_blocker(
            "qet_materialized",
            {"template_id": "qet_materialized", "status": "materialized", "submitted_experiment_id": None, "submitted_task_id": None, "runtime_config_sha256": None},
        ) or ""
    )
    assert "runtime materialization history" in (
        hard_delete_blocker(
            "qet_runtime",
            {"template_id": "qet_runtime", "status": "approved", "submitted_experiment_id": "qe_1", "submitted_task_id": None, "runtime_config_sha256": None},
        ) or ""
    )


def test_template_delete_endpoint_requires_confirmation() -> None:
    from fastapi import HTTPException
    from backend.routers.qe_templates import QETemplateDeleteRequest, delete_pending_qe_template

    with pytest.raises(HTTPException) as exc_info:
        delete_pending_qe_template("qet_1", QETemplateDeleteRequest(confirm_delete="WRONG"))

    assert exc_info.value.status_code == 400
    assert "QE_TEMPLATE_DELETE" in str(exc_info.value.detail)
