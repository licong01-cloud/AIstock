"""Preflight proves installed preparation wiring without executing producers."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.monthly_runtime import MonthlyRuntimeConfigurationError
from backend.services.dataset_release.monthly_worker_runtime import MonthlyWorkerRuntime


def _never_run(*_args, **_kwargs):
    raise AssertionError("preflight must not invoke a producer or open a scope")


def _runtime(version="6", *, installed=True, shared_scope=True):
    # A callable object, not a producer invocation.
    class Preparation:
        execution_scope_factory = staticmethod(_never_run)

        def __call__(self, *_args, **_kwargs):
            _never_run()

    private = Preparation() if installed else None
    adapter = SimpleNamespace(
        adapter_id="aistock.monthly.postgres_source",
        adapter_version=version,
        preparation_executor=private,
    )
    runner = SimpleNamespace(
        execution_scope_factory=_never_run if shared_scope else lambda: None,
    )
    registry = SimpleNamespace(
        source=SimpleNamespace(adapter=adapter),
        build=SimpleNamespace(adapter=SimpleNamespace(executor=SimpleNamespace(runner=runner))),
        contract=lambda: {"source": {"version": version}},
    )
    return MonthlyWorkerRuntime(
        settings=None,
        nodes=None,
        production=SimpleNamespace(profile_path=Path("profile"), hmm_authority_path=Path("authority")),
        registry=registry,
        worker=None,
    )


def test_preflight_proves_source6_shared_execution_scope_without_running():
    receipt = _runtime().preflight_receipt()
    assert receipt["status"] == "PASS"
    assert receipt["source_preparation"] == {
        "adapter_id": "aistock.monthly.postgres_source",
        "adapter_version": "6",
        "mode": "INDEPENDENT_UNPUBLISHED_PREPARATION",
        "shared_build_execution_scope": True,
        "publication_allowed": False,
    }
    assert not any(receipt["safety"].values())


def test_source5_remains_explicit_full_source_only():
    receipt = _runtime("5", installed=False).preflight_receipt()
    assert receipt["source_preparation"]["mode"] == "FULL_SOURCE_ONLY"
    assert receipt["source_preparation"]["shared_build_execution_scope"] is False


@pytest.mark.parametrize("version,installed,shared", [
    ("6", False, True), ("6", True, False), ("5", True, True),
    ("7", False, True), (None, False, True), (6, True, True),
])
def test_incomplete_or_unrecognized_source_contract_fails_closed(version, installed, shared):
    with pytest.raises(MonthlyRuntimeConfigurationError):
        _runtime(version, installed=installed, shared_scope=shared).preflight_receipt()


@pytest.mark.parametrize("field,value", [
    ("adapter_id", "foreign.source"), ("preparation_executor", object()),
])
def test_foreign_or_non_callable_preparation_is_rejected(field, value):
    runtime = _runtime()
    setattr(runtime.registry.source.adapter, field, value)
    with pytest.raises(MonthlyRuntimeConfigurationError):
        runtime.preflight_receipt()


def test_missing_build_scope_cannot_pass_by_none_identity():
    runtime = _runtime()
    runtime.registry.source.adapter.preparation_executor.execution_scope_factory = None
    runtime.registry.build.adapter.executor.runner.execution_scope_factory = None
    with pytest.raises(MonthlyRuntimeConfigurationError):
        runtime.preflight_receipt()


@pytest.mark.parametrize("stage", ["source", "build"])
def test_missing_registry_stage_is_a_typed_configuration_failure(stage):
    runtime = _runtime()
    delattr(runtime.registry, stage)
    with pytest.raises(MonthlyRuntimeConfigurationError):
        runtime.preflight_receipt()
