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
                    "custom_params": {"label_horizon": 3},
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
                "config_json": {"factor_names": ["f1"], "model_id": "bad"},
            }
        )


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
