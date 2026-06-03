from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from backend.mcp.modules import (
    execution_policy,
    external_research,
    factor_correlation,
    factor_library,
    factor_metrics,
    model_registry,
    strategy_governance,
)
from backend.mcp.registry import ModuleRegistry


class FakeMCP:
    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(self, name: str | None = None, **_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[name or func.__name__] = func
            return func

        return decorator


def _decode_json_body(request: httpx.Request) -> dict[str, Any]:
    if not request.content:
        return {}
    return json.loads(request.content.decode("utf-8"))


def _registry_with_capture(module: Any) -> tuple[ModuleRegistry, FakeMCP, list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call = {
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.url.params),
            "body": _decode_json_body(request),
        }
        calls.append(call)
        return httpx.Response(200, json={"ok": True, "path": call["path"], "summary_first": True})

    mcp = FakeMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url="http://127.0.0.1:8011/api/v1",
        env_name="test",
        transport=httpx.MockTransport(handler),
    )
    module.register(registry)
    return registry, mcp, calls


@pytest.mark.parametrize(
    ("module", "module_name", "prefix", "count"),
    [
        (factor_library, "factor_library", "/api/v1/factor-library/", 10),
        (factor_metrics, "factor_metrics", "/api/v1/factor-metrics/", 7),
        (factor_correlation, "factor_correlation", "/api/v1/factor-correlation/", 8),
        (model_registry, "model_registry", "/api/v1/model-registry/", 9),
        (strategy_governance, "strategy_governance", "/api/v1/strategy-governance/", 9),
        (execution_policy, "execution_policy", "/api/v1/execution-policy/", 7),
        (external_research, "external_research", "/api/v1/external-research/", 4),
    ],
)
def test_domain_modules_register_exact_tool_catalog(module: Any, module_name: str, prefix: str, count: int) -> None:
    registry, mcp, _calls = _registry_with_capture(module)

    assert module.TOOL_COUNT == count
    assert registry.tool_count(module_name) == count
    assert registry.total_tool_count() == count
    assert set(mcp.tools) == set(module.TOOL_NAMES)
    assert all(callable(func) for func in mcp.tools.values())


def test_factor_library_tools_call_only_facade_and_default_limit_20() -> None:
    _registry, mcp, calls = _registry_with_capture(factor_library)
    tools = mcp.tools

    tools["factor_library_list"]()
    tools["factor_library_search"]("momentum")
    tools["factor_library_get"]("alpha_001")
    tools["factor_library_get_coverage"]("alpha_001")
    tools["factor_library_get_metric_summary"]("alpha_001")
    tools["factor_library_get_usage_summary"]("alpha_001")
    tools["factor_library_plan_register"]({"factor_name": "alpha_002"})
    tools["factor_library_register_confirmed"]({"factor_name": "alpha_002"}, confirm=factor_library.REGISTER_FACTOR_CONFIRM)
    tools["factor_library_plan_deprecate"]({"factor_name": "alpha_002", "reason": "bad"})
    tools["factor_library_deprecate_confirmed"]({"factor_name": "alpha_002", "reason": "bad"}, confirm=factor_library.DEPRECATE_FACTOR_CONFIRM)

    assert len(calls) == factor_library.TOOL_COUNT
    assert all(call["path"].startswith("/api/v1/factor-library/") for call in calls)
    assert calls[0]["query"]["limit"] == "20"
    assert calls[1]["query"]["limit"] == "20"
    assert calls[7]["body"]["confirm"] == factor_library.REGISTER_FACTOR_CONFIRM


def test_factor_metrics_tools_call_only_facade_and_confirm_before_http() -> None:
    _registry, mcp, calls = _registry_with_capture(factor_metrics)
    with pytest.raises(ValueError, match=factor_metrics.SUBMIT_FACTOR_METRICS_CONFIRM):
        mcp.tools["factor_metrics_submit_confirmed"]({"factor_names": ["x"]}, confirm="WRONG")
    assert calls == []

    mcp.tools["factor_metrics_plan"]({"factor_names": ["x"]})
    mcp.tools["factor_metrics_validate_inputs"]({"factor_names": ["x"]})
    mcp.tools["factor_metrics_submit_confirmed"]({"factor_names": ["x"]}, confirm=factor_metrics.SUBMIT_FACTOR_METRICS_CONFIRM)
    mcp.tools["factor_metrics_get_job"]("job_1")
    mcp.tools["factor_metrics_get_result"]()
    mcp.tools["factor_metrics_compare_versions"]("x")
    mcp.tools["factor_metrics_export_result_ref"](factor_name="x")

    assert len(calls) == factor_metrics.TOOL_COUNT
    assert all(call["path"].startswith("/api/v1/factor-metrics/") for call in calls)
    assert calls[4]["query"]["limit"] == "20"


def test_factor_correlation_tools_call_only_facade_and_confirm_before_http() -> None:
    _registry, mcp, calls = _registry_with_capture(factor_correlation)
    with pytest.raises(ValueError, match=factor_correlation.SUBMIT_FACTOR_CORRELATION_CONFIRM):
        mcp.tools["factor_corr_submit_confirmed"]({}, confirm="WRONG")
    assert calls == []

    mcp.tools["factor_corr_plan"]({"factor_names": ["a", "b"]})
    mcp.tools["factor_corr_validate_inputs"]({"factor_names": ["a", "b"]})
    mcp.tools["factor_corr_submit_confirmed"]({"factor_names": ["a", "b"]}, confirm=factor_correlation.SUBMIT_FACTOR_CORRELATION_CONFIRM)
    mcp.tools["factor_corr_get_job"]("job_1")
    mcp.tools["factor_corr_get_top_pairs"]()
    mcp.tools["factor_corr_get_clusters"]()
    mcp.tools["factor_corr_suggest_replacements"]("a")
    mcp.tools["factor_corr_get_matrix_ref"]()

    assert len(calls) == factor_correlation.TOOL_COUNT
    assert all(call["path"].startswith("/api/v1/factor-correlation/") for call in calls)
    assert calls[4]["query"]["limit"] == "20"
    assert calls[-1]["path"].endswith("/matrix-ref")


def test_model_strategy_execution_modules_call_facades_and_confirm_before_http() -> None:
    for module, confirm_tool, expected in [
        (model_registry, "model_registry_register_confirmed", model_registry.REGISTER_MODEL_CONFIRM),
        (strategy_governance, "strategy_governance_promote_confirmed", strategy_governance.PROMOTE_STRATEGY_CONFIRM),
        (execution_policy, "execution_policy_bind_confirmed", execution_policy.BIND_EXECUTION_POLICY_CONFIRM),
    ]:
        _registry, mcp, calls = _registry_with_capture(module)
        with pytest.raises(ValueError, match=expected):
            if module is strategy_governance:
                mcp.tools[confirm_tool]("pkg_1", {}, confirm="WRONG")
            else:
                mcp.tools[confirm_tool]({}, confirm="WRONG")
        assert calls == []

    _registry, mcp, calls = _registry_with_capture(model_registry)
    mcp.tools["model_registry_list"]()
    mcp.tools["model_registry_get"]("model_1")
    mcp.tools["model_registry_compare_trials"]("model_1")
    mcp.tools["model_registry_get_seed_stability"]("model_1")
    mcp.tools["model_registry_get_hyperparam_history"]("model_1")
    mcp.tools["model_registry_get_artifacts"]("model_1")
    mcp.tools["model_registry_plan_register"]({"object_type": "spec"})
    mcp.tools["model_registry_register_confirmed"]({"object_type": "spec", "payload": {}}, confirm=model_registry.REGISTER_MODEL_CONFIRM)
    mcp.tools["model_registry_deprecate_confirmed"]({"object_id": "model_1"}, confirm=model_registry.DEPRECATE_MODEL_CONFIRM)
    assert len(calls) == model_registry.TOOL_COUNT
    assert all(call["path"].startswith("/api/v1/model-registry/") for call in calls)
    assert calls[0]["query"]["limit"] == "20"

    _registry, mcp, calls = _registry_with_capture(strategy_governance)
    mcp.tools["strategy_governance_list_packages"]()
    mcp.tools["strategy_governance_get_package"]("pkg_1")
    mcp.tools["strategy_governance_get_health"]("pkg_1")
    mcp.tools["strategy_governance_get_selection_readiness"]("pkg_1")
    mcp.tools["strategy_governance_get_paper_readiness"]("pkg_1")
    mcp.tools["strategy_governance_plan_promotion"]("pkg_1", {})
    mcp.tools["strategy_governance_plan_retirement"]("pkg_1", {})
    mcp.tools["strategy_governance_promote_confirmed"]("pkg_1", {}, confirm=strategy_governance.PROMOTE_STRATEGY_CONFIRM)
    mcp.tools["strategy_governance_retire_confirmed"]("pkg_1", {}, confirm=strategy_governance.RETIRE_STRATEGY_CONFIRM)
    assert len(calls) == strategy_governance.TOOL_COUNT
    assert all(call["path"].startswith("/api/v1/strategy-governance/") for call in calls)
    assert calls[0]["query"]["limit"] == "20"

    _registry, mcp, calls = _registry_with_capture(execution_policy)
    mcp.tools["execution_policy_list_algos"]()
    mcp.tools["execution_policy_get_algo"]("TWAP")
    mcp.tools["execution_policy_validate_for_strategy"]({"package_id": "pkg_1", "policy_json": {"algo_code": "TWAP"}})
    mcp.tools["execution_policy_get_market_state_constraints"]()
    mcp.tools["execution_policy_plan_binding"]({"package_id": "pkg_1"})
    mcp.tools["execution_policy_bind_confirmed"]({"package_id": "pkg_1"}, confirm=execution_policy.BIND_EXECUTION_POLICY_CONFIRM)
    mcp.tools["execution_policy_retire_confirmed"]({"package_id": "pkg_1", "policy_id": "pol_1"}, confirm=execution_policy.RETIRE_EXECUTION_POLICY_CONFIRM)
    assert len(calls) == execution_policy.TOOL_COUNT
    assert all(call["path"].startswith("/api/v1/execution-policy/") for call in calls)


@pytest.mark.parametrize("module", [factor_library, factor_metrics, factor_correlation, model_registry, strategy_governance, execution_policy, external_research])
def test_new_mcp_modules_are_thin_gateway_wrappers(module: Any) -> None:
    source = inspect.getsource(module)
    forbidden = ["backend.routers", "backend.services", "get_conn", "psycopg", "sqlalchemy", "subprocess"]
    assert not [token for token in forbidden if token in source]
