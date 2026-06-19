from __future__ import annotations

import hashlib
from datetime import date

import pytest

from backend.services.strategy_package.components import StrategyPackageComponentService
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaMode,
    SourceType,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _single_manifest(name: str, *, run_id: str | None = None):
    manifest = make_manifest()
    source = manifest.source.model_copy(update={"source_id": name, "run_id": run_id})
    component = manifest.alpha_components[0].model_copy(update={"alpha_id": f"alpha_{name}", "alpha_name": name})
    return manifest.model_copy(
        update={
            "package_name": f"单A·核心域·{name}·20260619",
            "source": source,
            "alpha_components": [component],
            "alpha_combination_policy": AlphaCombinationPolicy(method="identity", weights={component.alpha_id: 1.0}),
            "manifest_sha256": None,
        }
    )


def _multi_manifest(name: str, child_a: AlphaComponent, child_b: AlphaComponent):
    comp_a = child_a.model_copy(update={"component_weight": 0.6})
    comp_b = child_b.model_copy(update={"component_weight": 0.4})
    base = make_manifest()
    return base.model_copy(
        update={
            "package_name": f"组合×2·核心多Alpha·{name}·20260619",
            "source": base.source.model_copy(update={"source_type": SourceType.CANDIDATE_STRATEGY_PACKAGE, "source_id": f"combo_{name}"}),
            "alpha_mode": AlphaMode.MULTI_ALPHA,
            "alpha_components": [comp_a, comp_b],
            "alpha_combination_policy": AlphaCombinationPolicy(
                method="weighted_sum",
                weights={comp_a.alpha_id: 0.6, comp_b.alpha_id: 0.4},
            ),
            "manifest_sha256": None,
        }
    )


def _seed_two_children(repo: InMemoryStrategyPackageRepository):
    child_a_manifest = freeze_manifest(_single_manifest("fm"))
    child_b_manifest = freeze_manifest(_single_manifest("margin"))
    child_a = repo.save_manifest(child_a_manifest)
    child_b = repo.save_manifest(child_b_manifest)
    return child_a, child_b


def test_create_multi_alpha_package_persists_depth_one_component_edges() -> None:
    repo = InMemoryStrategyPackageRepository()
    child_a, child_b = _seed_two_children(repo)
    service = StrategyPackageComponentService(repository=repo)

    parent, edges = service.create_multi_alpha_package(
        manifest=_multi_manifest("core", child_a.manifest.alpha_components[0], child_b.manifest.alpha_components[0]),
        components=[
            {"child_package_id": child_a.package_id, "component_weight": 0.6, "score_normalization": "rank", "position": 1},
            {"child_package_id": child_b.package_id, "component_weight": 0.4, "score_normalization": "zscore", "position": 2},
        ],
    )

    assert parent.alpha_mode == AlphaMode.MULTI_ALPHA
    assert [edge.child_package_id for edge in edges] == [child_a.package_id, child_b.package_id]
    assert edges[0].child_manifest_sha256 == child_a.manifest_sha256
    payload = service.get_components(parent.package_id)
    assert payload["component_count"] == 2
    assert payload["components"][0]["score_normalization"] == "rank"


def test_multi_alpha_components_reject_non_single_child_before_parent_save() -> None:
    repo = InMemoryStrategyPackageRepository()
    child_a, child_b = _seed_two_children(repo)
    service = StrategyPackageComponentService(repository=repo)
    multi_child, _ = service.create_multi_alpha_package(
        manifest=_multi_manifest("nested_child", child_a.manifest.alpha_components[0], child_b.manifest.alpha_components[0]),
        components=[
            {"child_package_id": child_a.package_id, "component_weight": 0.5, "position": 1},
            {"child_package_id": child_b.package_id, "component_weight": 0.5, "position": 2},
        ],
    )
    before = set(repo.records)

    with pytest.raises(StrategyPackageValidationError, match="child must have alpha_mode=single_alpha") as excinfo:
        service.create_multi_alpha_package(
            manifest=_multi_manifest("bad_parent", child_a.manifest.alpha_components[0], child_b.manifest.alpha_components[0]),
            components=[
                {"child_package_id": child_a.package_id, "component_weight": 0.5, "position": 1},
                {"child_package_id": multi_child.package_id, "component_weight": 0.5, "position": 2},
            ],
        )

    assert excinfo.value.context["child_package_id"] == multi_child.package_id
    assert set(repo.records) == before


def test_referenced_single_alpha_child_cannot_be_retired() -> None:
    repo = InMemoryStrategyPackageRepository()
    child_a, child_b = _seed_two_children(repo)
    component_service = StrategyPackageComponentService(repository=repo)
    component_service.create_multi_alpha_package(
        manifest=_multi_manifest("retire_guard", child_a.manifest.alpha_components[0], child_b.manifest.alpha_components[0]),
        components=[
            {"child_package_id": child_a.package_id, "component_weight": 0.5, "position": 1},
            {"child_package_id": child_b.package_id, "component_weight": 0.5, "position": 2},
        ],
    )
    package_service = StrategyPackageService(repository=repo)

    with pytest.raises(InvalidStateTransitionError, match="cannot be retired") as excinfo:
        package_service.retire(child_a.package_id)

    assert child_a.package_id == excinfo.value.context["package_id"]
    assert excinfo.value.context["active_parent_package_ids"]


def test_prediction_ref_binding_extracts_prediction_and_model_sha() -> None:
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest(freeze_manifest(_single_manifest("bind", run_id="qear_run_bind")))
    prediction_sha = "a" * 64
    params_sha = "b" * 64

    class FakeModelStore:
        def get_pointer(self, *, run_id: str):
            assert run_id == "qear_run_bind"
            return {
                "pointer_status": "available",
                "prediction_store_manifest": {
                    "uri": "aistock-prediction-store://runs/qear_run_bind",
                    "artifacts": [
                        {"artifact_type": "prediction", "uri": "aistock-prediction-store://runs/qear_run_bind/prediction", "sha256": prediction_sha},
                        {"artifact_type": "model_params", "uri": "aistock-prediction-store://runs/qear_run_bind/model_params", "sha256": params_sha},
                    ],
                },
            }

    service = StrategyPackageComponentService(repository=repo, model_store=FakeModelStore())
    updated = service.bind_prediction_ref_from_run(package_id=record.package_id)

    assert updated.prediction_ref_uri == "aistock-prediction-store://runs/qear_run_bind/prediction"
    assert updated.prediction_ref_sha256 == prediction_sha
    assert updated.model_artifact_sha256 == params_sha
    pointer = service.get_prediction_ref(record.package_id)
    assert pointer["has_prediction_ref"] is True


def test_verify_on_use_fails_loud_on_sha_mismatch() -> None:
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest(freeze_manifest(_single_manifest("verify")))
    expected = hashlib.sha256(b"expected-bytes").hexdigest()
    repo.update_artifact_refs(
        record.package_id,
        prediction_ref_uri="aistock-prediction-store://runs/qear_verify/prediction",
        prediction_ref_sha256=expected,
    )
    service = StrategyPackageComponentService(repository=repo)

    ok = service.verify_artifact_on_use(
        package_id=record.package_id,
        artifact_kind="prediction",
        resolver=lambda _uri: b"expected-bytes",
    )
    assert ok["verified"] is True

    with pytest.raises(StrategyPackageValidationError, match="sha256 mismatch") as excinfo:
        service.verify_artifact_on_use(
            package_id=record.package_id,
            artifact_kind="prediction",
            resolver=lambda _uri: b"wrong-bytes",
        )

    assert excinfo.value.context["package_id"] == record.package_id
    assert excinfo.value.context["expected_sha256"] == expected
    assert excinfo.value.context["actual_sha256"] == hashlib.sha256(b"wrong-bytes").hexdigest()


def test_display_name_rejects_bare_ids_and_builds_blueprint_name() -> None:
    service = StrategyPackageComponentService(repository=InMemoryStrategyPackageRepository())
    assert service.build_display_name(
        alpha_mode=AlphaMode.MULTI_ALPHA,
        signal_domain="核心多Alpha",
        custom_name="低回撤",
        data_vintage=date(2026, 6, 19),
        component_count=3,
    ) == "组合×3·核心多Alpha·低回撤·20260619"

    with pytest.raises(StrategyPackageValidationError, match="human-readable"):
        service.build_display_name(
            alpha_mode=AlphaMode.SINGLE_ALPHA,
            signal_domain="pkg_deadbeef",
            custom_name="模型",
            data_vintage=date(2026, 6, 19),
        )


def test_missing_prediction_ref_is_explicit() -> None:
    repo = InMemoryStrategyPackageRepository()
    record = repo.save_manifest(freeze_manifest(_single_manifest("missing")))
    service = StrategyPackageComponentService(repository=repo)

    with pytest.raises(DataUnavailableError, match="artifact pointer is missing"):
        service.verify_artifact_on_use(package_id=record.package_id, artifact_kind="prediction", resolver=lambda _uri: b"")
