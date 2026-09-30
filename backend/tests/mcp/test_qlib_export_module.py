"""Contract tests for the Qlib H5/Bin export MCP module."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from backend.mcp.modules import qlib_export
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


def _registry_with_capture() -> tuple[ModuleRegistry, FakeMCP, list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call = {
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.url.params),
            "body": _decode_json_body(request),
        }
        if "X-Dataset-Release-Operator-Token" in request.headers:
            call["operator_token"] = request.headers["X-Dataset-Release-Operator-Token"]
        if "Idempotency-Key" in request.headers:
            call["idempotency_key"] = request.headers["Idempotency-Key"]
        calls.append(call)
        payload: dict[str, Any] = {
            "success": True,
            "snapshot_id": call["body"].get("snapshot_id", "qlib_test"),
            "start": call["body"].get("start"),
            "end": call["body"].get("end", "2026-05-29"),
            "rows": 123,
            "ts_codes": [f"{i:06d}.SZ" for i in range(30)],
            "steps": [
                {"dataset": "stock_daily", "ok": True, "rows": 10, "stdout": "large log"},
                {"dataset": "stock_minute", "ok": True, "rows": 20, "stderr": "large log"},
            ],
            "items": [{"snapshot_id": "bin_1", "verbose": "x" * 100}],
            "snapshots": [{"snapshot_id": "qlib_test", "meta": {"large": "x" * 100}}],
        }
        return httpx.Response(200, json=payload)

    mcp = FakeMCP()
    registry = ModuleRegistry(
        mcp=mcp,
        base_url="http://127.0.0.1:8011/api/v1",
        env_name="test",
        transport=httpx.MockTransport(handler),
    )
    qlib_export.register(registry)
    return registry, mcp, calls


def test_qlib_export_module_registers_design_tool_catalog() -> None:
    registry, mcp, _calls = _registry_with_capture()

    assert qlib_export.TOOL_COUNT == 23
    assert registry.tool_count("qlib_export") == qlib_export.TOOL_COUNT
    assert registry.total_tool_count() == qlib_export.TOOL_COUNT
    assert set(mcp.tools) == set(qlib_export.TOOL_NAMES)
    assert not {name for name in mcp.tools if "promote" in name or "production" in name}


def test_qlib_export_tools_call_only_qlib_facade_and_compact_responses() -> None:
    _registry, mcp, calls = _registry_with_capture()
    tools = mcp.tools

    tools["qlib_export_get_config"]()
    tools["qlib_export_list_snapshots"]()
    tools["qlib_export_list_bin_exports"]()
    tools["qlib_export_get_snapshot_quality"]("qlib_test", data_type="daily")
    tools["qlib_export_validate_snapshot"]("qlib_test", data_type="minute")
    tools["qlib_export_data_check"]({"start": "2026-05-01", "end": "2026-05-29", "sample_size": 2})
    preview = tools["qlib_export_data_preview"]("000001.SZ", "2026-05-01", "2026-05-29", limit=5)
    plan = tools["qlib_export_plan_dataset_update"]({"target_end": "2026-05-29", "snapshot_id": "qlib_test"})
    tools["qlib_export_run_h5_dataset_full_confirmed"](
        "daily",
        {"snapshot_id": "qlib_test", "start": "2026-05-01", "end": "2026-05-29"},
        confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM,
    )
    tools["qlib_export_run_h5_dataset_incremental_confirmed"](
        "minute",
        {"snapshot_id": "qlib_test", "end": "2026-05-29"},
        confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM,
    )
    incremental_all = tools["qlib_export_run_h5_daily_aux_incremental_all_confirmed"](
        "qlib_test",
        {"snapshot_id": "qlib_test", "end": "2026-05-29"},
        confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM,
    )
    tools["qlib_export_build_static_factors_confirmed"]("qlib_test", confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM)
    tools["qlib_export_export_field_map_confirmed"](
        {"snapshot_id": "qlib_test", "write_to_h5": True},
        confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM,
    )
    bin_result = tools["qlib_export_run_bin_unified_v2_confirmed"](
        {
            "snapshot_id": "bin_20260529",
            "mode": "full",
            "start": "2018-08-01",
            "end": "2026-05-29",
            "datasets": ["stock_daily"],
        },
        confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM,
    )
    candidate = tools["qlib_export_generate_backtest_candidate_confirmed"](
        {
            "snapshot_id": "qlib_test",
            "end": "2026-05-29",
            "include_minute_h5": True,
            "bin_payload": {
                "snapshot_id": "bin_20260529",
                "mode": "full",
                "start": "2018-08-01",
                "end": "2026-05-29",
                "datasets": ["stock_daily"],
            },
        },
        confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM,
    )

    assert len(calls) == 18
    assert all(call["path"].startswith("/api/v1/qlib/") for call in calls)
    assert calls[:14] == [
        {"method": "GET", "path": "/api/v1/qlib/config", "query": {}, "body": {}},
        {"method": "GET", "path": "/api/v1/qlib/snapshots", "query": {}, "body": {}},
        {"method": "GET", "path": "/api/v1/qlib/bin/exports", "query": {}, "body": {}},
        {
            "method": "GET",
            "path": "/api/v1/qlib/snapshots/qlib_test/quality",
            "query": {"data_type": "daily", "detect_anomalies": "true"},
            "body": {},
        },
        {
            "method": "GET",
            "path": "/api/v1/qlib/snapshots/qlib_test/validate",
            "query": {"data_type": "minute"},
            "body": {},
        },
        {
            "method": "POST",
            "path": "/api/v1/qlib/data/check",
            "query": {},
            "body": {"start": "2026-05-01", "end": "2026-05-29", "sample_size": 2},
        },
        {
            "method": "GET",
            "path": "/api/v1/qlib/data/preview",
            "query": {"ts_code": "000001.SZ", "start": "2026-05-01", "end": "2026-05-29", "limit": "5"},
            "body": {},
        },
        {
            "method": "POST",
            "path": "/api/v1/qlib/snapshots/daily",
            "query": {},
            "body": {"snapshot_id": "qlib_test", "start": "2026-05-01", "end": "2026-05-29"},
        },
        {
            "method": "POST",
            "path": "/api/v1/qlib/snapshots/minute/incremental",
            "query": {},
            "body": {"snapshot_id": "qlib_test", "end": "2026-05-29"},
        },
        {
            "method": "POST",
            "path": "/api/v1/qlib/snapshots/qlib_test/incremental_all",
            "query": {},
            "body": {"snapshot_id": "qlib_test", "end": "2026-05-29"},
        },
        {"method": "POST", "path": "/api/v1/qlib/snapshots/qlib_test/static_factors", "query": {}, "body": {}},
        {
            "method": "POST",
            "path": "/api/v1/qlib/field_map/export",
            "query": {},
            "body": {"snapshot_id": "qlib_test", "write_to_h5": True},
        },
        {
            "method": "POST",
            "path": "/api/v1/qlib/bin/unified_export_v2",
            "query": {},
            "body": {
                "snapshot_id": "bin_20260529",
                "mode": "full",
                "start": "2018-08-01",
                "end": "2026-05-29",
                "datasets": ["stock_daily"],
            },
        },
        {
            "method": "POST",
            "path": "/api/v1/qlib/snapshots/qlib_test/incremental_all",
            "query": {},
            "body": {
                "snapshot_id": "qlib_test",
                "end": "2026-05-29",
                "stock_universe_mode": "pit_spans",
                "universe_key": "shsz_st_pit_active_v1",
            },
        },
    ]
    assert preview["ts_codes"][:2] == ["000000.SZ", "000001.SZ"]
    assert preview["ts_code_count"] == 30
    assert plan["status"] == "plan_only"
    assert plan["production_promotion_supported"] is False
    assert incremental_all["minute_h5_included"] is False
    assert bin_result["steps"] == [
        {"dataset": "stock_daily", "ok": True, "rows": 10},
        {"dataset": "stock_minute", "ok": True, "rows": 20},
    ]
    assert candidate["candidate_only"] is True
    assert candidate["production_promotion_supported"] is False


def test_monthly_release_tools_delegate_to_single_api_with_secret_headers(tmp_path, monkeypatch) -> None:
    token = "t" * 40
    token_file = tmp_path / "operator-token.txt"
    token_file.write_text(token, encoding="utf-8")
    monkeypatch.setenv(qlib_export.MONTHLY_RELEASE_TOKEN_FILE_ENV, str(token_file))
    _registry, mcp, calls = _registry_with_capture()
    tools = mcp.tools
    payload = {"target_cutoff": "2026-09-30", "activation_mode": "prepare_only"}
    operation_id = "dmr_" + "a" * 32
    authorization_ref = "dsauth_" + "b" * 32

    tools["qlib_monthly_release_plan"](payload, "monthly-202609-plan")
    tools["qlib_monthly_release_submit_confirmed"](
        payload,
        "monthly-202609-submit",
        confirm=qlib_export.MONTHLY_RELEASE_RUN_CONFIRM,
    )
    tools["qlib_monthly_release_status"](operation_id)
    tools["qlib_monthly_release_receipts"](operation_id)
    tools["qlib_monthly_release_resume_confirmed"](operation_id, confirm=qlib_export.MONTHLY_RELEASE_RUN_CONFIRM)
    tools["qlib_monthly_release_cancel_confirmed"](operation_id, confirm=qlib_export.MONTHLY_RELEASE_RUN_CONFIRM)
    tools["qlib_monthly_release_activate_confirmed"](
        operation_id,
        authorization_ref,
        confirm=qlib_export.MONTHLY_RELEASE_ACTIVATE_CONFIRM,
    )
    tools["qlib_monthly_release_rollback_confirmed"](
        operation_id,
        authorization_ref,
        confirm=qlib_export.MONTHLY_RELEASE_ACTIVATE_CONFIRM,
    )

    monthly_calls = calls[-8:]
    assert [item["path"] for item in monthly_calls] == [
        "/api/v1/qlib/monthly-releases/plan",
        "/api/v1/qlib/monthly-releases",
        f"/api/v1/qlib/monthly-releases/{operation_id}",
        f"/api/v1/qlib/monthly-releases/{operation_id}/receipts",
        f"/api/v1/qlib/monthly-releases/{operation_id}/resume",
        f"/api/v1/qlib/monthly-releases/{operation_id}/cancel",
        f"/api/v1/qlib/monthly-releases/{operation_id}/activate",
        f"/api/v1/qlib/monthly-releases/{operation_id}/rollback",
    ]
    assert all(item["operator_token"] == token for item in monthly_calls)
    assert monthly_calls[0]["idempotency_key"] == "monthly-202609-plan"
    assert monthly_calls[1]["idempotency_key"] == "monthly-202609-submit"
    assert "operator_token" not in str(monthly_calls[0]["body"])


def test_monthly_release_tools_fail_closed_before_http(tmp_path, monkeypatch) -> None:
    token_file = tmp_path / "operator-token.txt"
    token_file.write_text("t" * 40, encoding="utf-8")
    monkeypatch.setenv(qlib_export.MONTHLY_RELEASE_TOKEN_FILE_ENV, str(token_file))
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match="canonical monthly release id"):
        mcp.tools["qlib_monthly_release_status"]("../release")
    with pytest.raises(ValueError, match=qlib_export.MONTHLY_RELEASE_RUN_CONFIRM):
        mcp.tools["qlib_monthly_release_submit_confirmed"]({"target_cutoff": "2026-09-30"}, "key")
    with pytest.raises(ValueError, match="authorization_ref"):
        mcp.tools["qlib_monthly_release_activate_confirmed"](
            "dmr_" + "a" * 32,
            "bad-ref",
            confirm=qlib_export.MONTHLY_RELEASE_ACTIVATE_CONFIRM,
        )
    assert calls == []


def test_monthly_release_tools_reject_reparse_token_file_before_http(tmp_path, monkeypatch) -> None:
    token_file = tmp_path / "operator-token.txt"
    token_file.write_text("t" * 40, encoding="utf-8")
    monkeypatch.setenv(qlib_export.MONTHLY_RELEASE_TOKEN_FILE_ENV, str(token_file))
    monkeypatch.setattr(
        qlib_export,
        "_is_reparse_or_symlink",
        lambda path: path == token_file,
    )
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(RuntimeError, match="token file is invalid"):
        mcp.tools["qlib_monthly_release_status"]("dmr_" + "a" * 32)

    assert calls == []


@pytest.mark.parametrize(
    ("tool_name", "args"),
    [
        (
            "qlib_export_run_h5_dataset_full_confirmed",
            ("daily", {"snapshot_id": "qlib_test", "start": "2026-05-01", "end": "2026-05-29"}),
        ),
        (
            "qlib_export_run_h5_dataset_incremental_confirmed",
            ("daily", {"snapshot_id": "qlib_test", "end": "2026-05-29"}),
        ),
        (
            "qlib_export_run_h5_daily_aux_incremental_all_confirmed",
            ("qlib_test", {"snapshot_id": "qlib_test", "end": "2026-05-29"}),
        ),
        ("qlib_export_build_static_factors_confirmed", ("qlib_test",)),
        ("qlib_export_export_field_map_confirmed", ({"snapshot_id": "qlib_test"},)),
        (
            "qlib_export_run_bin_unified_v2_confirmed",
            ({"snapshot_id": "bin_1", "end": "2026-05-29", "datasets": ["stock_daily"]},),
        ),
        ("qlib_export_generate_backtest_candidate_confirmed", ({"snapshot_id": "qlib_test", "end": "2026-05-29"},)),
    ],
)
def test_confirmed_qlib_export_tools_reject_before_http(tool_name: str, args: tuple[Any, ...]) -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match=qlib_export.QLIB_EXPORT_RUN_CONFIRM):
        mcp.tools[tool_name](*args, confirm="WRONG")

    assert calls == []


@pytest.mark.parametrize(
    ("tool_name", "args", "kwargs"),
    [
        ("qlib_export_get_snapshot_quality", ("../qlib_test",), {}),
        ("qlib_export_validate_snapshot", ("qlib/test",), {}),
        ("qlib_export_data_preview", ("000001/SZ", "2026-05-01", "2026-05-29"), {}),
        ("qlib_export_plan_dataset_update", ({"target_end": "2026-05-29", "snapshot_id": "../qlib_test"},), {}),
        (
            "qlib_export_run_h5_dataset_full_confirmed",
            ("daily", {"snapshot_id": "../qlib_test", "start": "2026-05-01", "end": "2026-05-29"}),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_run_h5_dataset_incremental_confirmed",
            ("minute", {"snapshot_id": "qlib/test", "end": "2026-05-29"}),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_run_h5_daily_aux_incremental_all_confirmed",
            ("qlib/test", {"snapshot_id": "qlib_test", "end": "2026-05-29"}),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_run_h5_daily_aux_incremental_all_confirmed",
            ("qlib_test", {"snapshot_id": "qlib/test", "end": "2026-05-29"}),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_build_static_factors_confirmed",
            ("qlib/test",),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_export_field_map_confirmed",
            ({"snapshot_id": "qlib/test"},),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_run_bin_unified_v2_confirmed",
            ({"snapshot_id": "../qlib_bin", "end": "2026-05-29", "datasets": ["stock_daily"]},),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_generate_backtest_candidate_confirmed",
            ({"snapshot_id": "qlib_test", "end": "2026-05-29", "bin_payload": {"snapshot_id": "../qlib_bin"}},),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
    ],
)
def test_qlib_export_rejects_path_injection_before_http(
    tool_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError):
        mcp.tools[tool_name](*args, **kwargs)

    assert calls == []


@pytest.mark.parametrize(
    ("tool_name", "args", "kwargs"),
    [
        ("qlib_export_plan_dataset_update", ({"target_end": "2026-05-29", "bin_snapshot_id": "qlib_bin"},), {}),
        (
            "qlib_export_run_h5_dataset_full_confirmed",
            ("daily", {"snapshot_id": "factor_data", "start": "2026-05-01", "end": "2026-05-29"}),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_run_h5_daily_aux_incremental_all_confirmed",
            ("qlib_minute_bin", {"snapshot_id": "qlib_minute_bin", "end": "2026-05-29"}),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_run_bin_unified_v2_confirmed",
            ({"snapshot_id": "qlib_bin", "end": "2026-05-29", "datasets": ["stock_daily"]},),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
        (
            "qlib_export_generate_backtest_candidate_confirmed",
            ({"snapshot_id": "qlib_test", "end": "2026-05-29", "bin_payload": {"snapshot_id": "qlib_bin"}},),
            {"confirm": qlib_export.QLIB_EXPORT_RUN_CONFIRM},
        ),
    ],
)
def test_qlib_export_rejects_production_target_names_before_http(
    tool_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match="candidate export"):
        mcp.tools[tool_name](*args, **kwargs)

    assert calls == []


def test_qlib_export_rejects_unsupported_dataset_before_http() -> None:
    _registry, mcp, calls = _registry_with_capture()

    with pytest.raises(ValueError, match="dataset must be one of"):
        mcp.tools["qlib_export_run_h5_dataset_incremental_confirmed"](
            "unknown",
            {"snapshot_id": "qlib_test", "end": "2026-05-29"},
            confirm=qlib_export.QLIB_EXPORT_RUN_CONFIRM,
        )

    assert calls == []
