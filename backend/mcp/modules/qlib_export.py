"""Qlib H5/Bin dataset export MCP wrappers.

This module is a thin gateway layer over ``/api/v1/qlib/*``. It validates
path fragments and confirmation text, then delegates to the same FastAPI
surface used by the Qlib UI. It must not import exporter services, database
readers, or file-copy/promotion helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.mcp.registry import ModuleRegistry


QLIB_EXPORT_RUN_CONFIRM = "RUN_QLIB_EXPORT"
PRODUCTION_TARGET_NAMES = frozenset({"qlib_bin", "qlib_minute_bin", "factor_data"})

H5_FULL_DATASET_ENDPOINTS = {
    "daily": "/snapshots/daily",
    "minute": "/snapshots/minute",
    "daily_basic": "/snapshots/daily_basic",
    "moneyflow": "/snapshots/moneyflow",
    "bak_basic": "/snapshots/bak_basic",
    "margin_detail": "/snapshots/margin_detail",
    "cyq_perf": "/snapshots/cyq_perf",
    "sector_data": "/snapshots/sector_data",
}
H5_INCREMENTAL_DATASET_ENDPOINTS = {
    "daily": "/snapshots/daily/incremental",
    "minute": "/snapshots/minute/incremental",
    "daily_basic": "/snapshots/daily_basic/incremental",
    "moneyflow": "/snapshots/moneyflow/incremental",
    "bak_basic": "/snapshots/bak_basic/incremental",
    "margin_detail": "/snapshots/margin_detail/incremental",
    "cyq_perf": "/snapshots/cyq_perf/incremental",
    "sector_data": "/snapshots/sector_data/incremental",
}
DAILY_AUX_INCREMENTAL_DATASETS = (
    "daily",
    "moneyflow",
    "daily_basic",
    "bak_basic",
    "margin_detail",
    "cyq_perf",
    "sector_data",
)

TOOL_NAMES = (
    "qlib_export_get_config",
    "qlib_export_list_snapshots",
    "qlib_export_list_bin_exports",
    "qlib_export_get_snapshot_quality",
    "qlib_export_validate_snapshot",
    "qlib_export_data_check",
    "qlib_export_data_preview",
    "qlib_export_plan_dataset_update",
    "qlib_export_run_h5_dataset_full_confirmed",
    "qlib_export_run_h5_dataset_incremental_confirmed",
    "qlib_export_run_h5_daily_aux_incremental_all_confirmed",
    "qlib_export_build_static_factors_confirmed",
    "qlib_export_export_field_map_confirmed",
    "qlib_export_run_bin_unified_v2_confirmed",
    "qlib_export_generate_backtest_candidate_confirmed",
)
TOOL_COUNT = len(TOOL_NAMES)


def _fragment(registry: "ModuleRegistry", value: Any, name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a string or integer path fragment; got {value!r}")
    raw = str(value) if isinstance(value, int) else value
    return registry.sanitize(raw, name)


def _write_fragment(registry: "ModuleRegistry", value: Any, name: str) -> str:
    safe = _fragment(registry, value, name)
    if safe.lower() in PRODUCTION_TARGET_NAMES:
        raise ValueError(f"{name} must identify a candidate export, not production target {safe!r}")
    return safe


def _body(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return dict(payload or {})


def _sanitize_payload_identifiers(
    registry: "ModuleRegistry",
    payload: dict[str, Any] | None,
    *,
    field_names: tuple[str, ...] = ("snapshot_id", "bin_snapshot_id"),
) -> dict[str, Any]:
    body = _body(payload)
    for field_name in field_names:
        if field_name in body and body[field_name] is not None:
            body[field_name] = _write_fragment(registry, body[field_name], field_name)
    return body


def _confirmed_body(
    registry: "ModuleRegistry",
    *,
    confirm: str | None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry.confirm(confirm, QLIB_EXPORT_RUN_CONFIRM, "confirm")
    return _sanitize_payload_identifiers(registry, payload)


def _dataset_endpoint(dataset: str, mapping: dict[str, str]) -> str:
    key = str(dataset or "").strip()
    if key not in mapping:
        allowed = ", ".join(sorted(mapping))
        raise ValueError(f"dataset must be one of {allowed}; got {dataset!r}")
    return mapping[key]


def _compact(value: Any) -> Any:
    """Return a token-bounded summary for export responses."""

    if not isinstance(value, dict):
        return value
    keep_keys = {
        "success",
        "ok",
        "status",
        "summary",
        "snapshot_root",
        "default_snapshot_id",
        "snapshot_id",
        "ts_code",
        "mode",
        "freq",
        "start",
        "end",
        "rows",
        "ts_codes",
        "csv_path",
        "csv_path_win",
        "csv_path_wsl",
        "report_path",
        "quality_score",
        "check_ok",
        "is_valid",
        "issues",
        "total_rows",
        "total_instruments",
        "date_range",
        "trading_days",
        "coverage_rate",
        "data_coverage",
        "adj_factor_coverage",
        "factor_range",
    }
    compact: dict[str, Any] = {key: value[key] for key in keep_keys if key in value}
    if "ts_codes" in compact and isinstance(compact["ts_codes"], list):
        compact["ts_code_count"] = len(compact["ts_codes"])
        compact["ts_codes"] = compact["ts_codes"][:20]
    if "steps" in value and isinstance(value["steps"], list):
        compact["steps"] = [
            {
                key: step.get(key)
                for key in ("dataset", "ok", "rows", "error", "mode_used")
                if isinstance(step, dict) and key in step
            }
            for step in value["steps"]
        ]
    if "items" in value and isinstance(value["items"], list):
        compact["total"] = value.get("total", len(value["items"]))
        compact["items"] = value["items"][:20]
    if "snapshots" in value and isinstance(value["snapshots"], list):
        compact["total"] = value.get("total", len(value["snapshots"]))
        compact["snapshots"] = value["snapshots"][:20]
    if "sample_data" in value and isinstance(value["sample_data"], list):
        compact["sample_data"] = value["sample_data"][:5]
    if "data" in value and isinstance(value["data"], list):
        compact["data"] = value["data"][:5]
    if "columns" in value and isinstance(value["columns"], list):
        compact["columns"] = value["columns"][:30]
        compact["column_count"] = len(value["columns"])
    if "validation_report" in value and isinstance(value["validation_report"], dict):
        report = value["validation_report"]
        compact["validation_report"] = {
            key: report.get(key)
            for key in (
                "total_rows",
                "duplicate_count",
                "date_range",
                "trading_days",
                "instrument_count",
            )
            if key in report
        }
    return compact or {"status": "ok", "omitted_large_payload": True}


def _require_date(value: Any, name: str) -> str:
    raw = str(value or "").strip()
    if len(raw) != 10 or raw[4] != "-" or raw[7] != "-":
        raise ValueError(f"{name} must be YYYY-MM-DD; got {value!r}")
    return raw


def _plan_dataset_update(registry: "ModuleRegistry", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = _sanitize_payload_identifiers(registry, payload)
    target_end = _require_date(body.get("target_end"), "target_end")
    snapshot_id = _write_fragment(registry, body.get("snapshot_id") or "qlib_test", "snapshot_id")
    bin_snapshot_id = _write_fragment(
        registry,
        body.get("bin_snapshot_id") or f"qlib_bin_st_pit_active_candidate_to_{target_end.replace('-', '')}",
        "bin_snapshot_id",
    )
    include_minute_h5 = bool(body.get("include_minute_h5", True))
    include_bin = bool(body.get("include_bin", True))
    bin_mode = str(body.get("bin_mode") or "full")
    if bin_mode not in {"full", "incremental"}:
        raise ValueError(f"bin_mode must be 'full' or 'incremental'; got {bin_mode!r}")
    bin_datasets = body.get("bin_datasets") or ["stock_daily", "stock_minute"]
    if not isinstance(bin_datasets, list) or not all(isinstance(item, str) for item in bin_datasets):
        raise ValueError("bin_datasets must be a list of dataset strings")

    steps: list[dict[str, Any]] = [
        {
            "step": "check_source_readiness",
            "tool": "local_data_health_overview",
            "risk": "read_only",
            "note": "Confirm source datasets cover the target trading date before exporting.",
        },
        {
            "step": "h5_daily_aux_incremental",
            "tool": "qlib_export_run_h5_daily_aux_incremental_all_confirmed",
            "requires_confirmation": QLIB_EXPORT_RUN_CONFIRM,
            "payload": {"snapshot_id": snapshot_id, "end": target_end},
            "note": "This updates daily/aux H5 files only; minute_1min.h5 is not included.",
        },
    ]
    if include_minute_h5:
        steps.append(
            {
                "step": "h5_minute_incremental",
                "tool": "qlib_export_run_h5_dataset_incremental_confirmed",
                "requires_confirmation": QLIB_EXPORT_RUN_CONFIRM,
                "payload": {"dataset": "minute", "snapshot_id": snapshot_id, "end": target_end},
            }
        )
    steps.extend(
        [
            {
                "step": "static_factors",
                "tool": "qlib_export_build_static_factors_confirmed",
                "requires_confirmation": QLIB_EXPORT_RUN_CONFIRM,
                "payload": {"snapshot_id": snapshot_id},
            },
            {
                "step": "field_map",
                "tool": "qlib_export_export_field_map_confirmed",
                "requires_confirmation": QLIB_EXPORT_RUN_CONFIRM,
                "payload": {"snapshot_id": snapshot_id, "write_to_h5": True},
            },
        ]
    )
    if include_bin:
        steps.append(
            {
                "step": "bin_candidate",
                "tool": "qlib_export_run_bin_unified_v2_confirmed",
                "requires_confirmation": QLIB_EXPORT_RUN_CONFIRM,
                "payload": {
                    "snapshot_id": bin_snapshot_id,
                    "mode": bin_mode,
                    "start": body.get("bin_start") or ("2018-08-01" if bin_mode == "full" else None),
                    "end": target_end,
                    "datasets": bin_datasets,
                    "stock_universe_mode": body.get("stock_universe_mode", "pit_spans"),
                    "universe_key": body.get("universe_key", "shsz_st_pit_active_v1"),
                    "run_health_check": True,
                },
                "note": "Use full mode when extending stock qfq basis_end; incremental may fail fast by design.",
            }
        )

    return {
        "status": "plan_only",
        "writes_data": False,
        "target_end": target_end,
        "snapshot_id": snapshot_id,
        "bin_snapshot_id": bin_snapshot_id if include_bin else None,
        "candidate_only": True,
        "production_promotion_supported": False,
        "required_confirmation": QLIB_EXPORT_RUN_CONFIRM,
        "steps": steps,
        "known_limits": [
            "incremental_all does not update minute_1min.h5.",
            "MCP phase 1 does not replace /home/lc999/data/qlib_bin, /home/lc999/data/qlib_minute_bin, or /home/lc999/data/factor_data.",
            "Qlib Bin incremental can fail when qfq basis_end must be extended; use full rebuild for authoritative candidates.",
        ],
    }


def register(registry: "ModuleRegistry") -> None:
    """Register Qlib H5/Bin export tools on the shared MCP gateway."""

    client = registry.client("qlib")

    @registry.mcp.tool(name="qlib_export_get_config")
    def qlib_export_get_config() -> Any:
        """Return Qlib export root config and field mapping metadata."""

        return _compact(client.get("/config"))

    @registry.mcp.tool(name="qlib_export_list_snapshots")
    def qlib_export_list_snapshots() -> Any:
        """List existing H5 snapshots using a compact response."""

        return _compact(client.get("/snapshots"))

    @registry.mcp.tool(name="qlib_export_list_bin_exports")
    def qlib_export_list_bin_exports() -> Any:
        """List existing Qlib Bin exports using a compact response."""

        return _compact(client.get("/bin/exports"))

    @registry.mcp.tool(name="qlib_export_get_snapshot_quality")
    def qlib_export_get_snapshot_quality(
        snapshot_id: str,
        data_type: str = "daily",
        detect_anomalies: bool = True,
    ) -> Any:
        """Return a compact quality report for one snapshot H5 file."""

        safe_snapshot_id = _fragment(registry, snapshot_id, "snapshot_id")
        return _compact(
            client.get(
                f"/snapshots/{safe_snapshot_id}/quality",
                params={"data_type": data_type, "detect_anomalies": detect_anomalies},
            )
        )

    @registry.mcp.tool(name="qlib_export_validate_snapshot")
    def qlib_export_validate_snapshot(snapshot_id: str, data_type: str = "daily") -> Any:
        """Validate one snapshot H5 file and return a compact issue summary."""

        safe_snapshot_id = _fragment(registry, snapshot_id, "snapshot_id")
        return _compact(client.get(f"/snapshots/{safe_snapshot_id}/validate", params={"data_type": data_type}))

    @registry.mcp.tool(name="qlib_export_data_check")
    def qlib_export_data_check(payload: dict[str, Any]) -> Any:
        """Check DB source data readiness for a bounded date/code sample."""

        return _compact(client.post("/data/check", _body(payload)))

    @registry.mcp.tool(name="qlib_export_data_preview")
    def qlib_export_data_preview(ts_code: str, start: str, end: str, limit: int = 20) -> Any:
        """Preview a single instrument in Qlib format with a capped row count."""

        if limit < 1 or limit > 50:
            raise ValueError(f"limit must be between 1 and 50; got {limit}")
        safe_ts_code = _fragment(registry, ts_code, "ts_code")
        return _compact(client.get("/data/preview", params={"ts_code": safe_ts_code, "start": start, "end": end, "limit": limit}))

    @registry.mcp.tool(name="qlib_export_plan_dataset_update")
    def qlib_export_plan_dataset_update(payload: dict[str, Any] | None = None) -> Any:
        """Build a dry-run plan for H5/Bin candidate update without writing data."""

        return _plan_dataset_update(registry, payload)

    @registry.mcp.tool(name="qlib_export_run_h5_dataset_full_confirmed")
    def qlib_export_run_h5_dataset_full_confirmed(
        dataset: str,
        payload: dict[str, Any],
        confirm: str | None = None,
    ) -> Any:
        """Run one H5 full export after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, payload=payload)
        return _compact(client.post(_dataset_endpoint(dataset, H5_FULL_DATASET_ENDPOINTS), body))

    @registry.mcp.tool(name="qlib_export_run_h5_dataset_incremental_confirmed")
    def qlib_export_run_h5_dataset_incremental_confirmed(
        dataset: str,
        payload: dict[str, Any],
        confirm: str | None = None,
    ) -> Any:
        """Run one H5 incremental export after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, payload=payload)
        return _compact(client.post(_dataset_endpoint(dataset, H5_INCREMENTAL_DATASET_ENDPOINTS), body))

    @registry.mcp.tool(name="qlib_export_run_h5_daily_aux_incremental_all_confirmed")
    def qlib_export_run_h5_daily_aux_incremental_all_confirmed(
        snapshot_id: str,
        payload: dict[str, Any],
        confirm: str | None = None,
    ) -> Any:
        """Run daily/aux H5 incremental_all; minute_1min.h5 is intentionally excluded."""

        safe_snapshot_id = _write_fragment(registry, snapshot_id, "snapshot_id")
        body = _confirmed_body(registry, confirm=confirm, payload=payload)
        result = _compact(client.post(f"/snapshots/{safe_snapshot_id}/incremental_all", body))
        if isinstance(result, dict):
            result["minute_h5_included"] = False
            result["included_dataset_scope"] = list(DAILY_AUX_INCREMENTAL_DATASETS)
        return result

    @registry.mcp.tool(name="qlib_export_build_static_factors_confirmed")
    def qlib_export_build_static_factors_confirmed(snapshot_id: str, confirm: str | None = None) -> Any:
        """Build static_factors.parquet for an H5 snapshot after confirmation."""

        registry.confirm(confirm, QLIB_EXPORT_RUN_CONFIRM, "confirm")
        safe_snapshot_id = _write_fragment(registry, snapshot_id, "snapshot_id")
        return _compact(client.post(f"/snapshots/{safe_snapshot_id}/static_factors", {}))

    @registry.mcp.tool(name="qlib_export_export_field_map_confirmed")
    def qlib_export_export_field_map_confirmed(
        payload: dict[str, Any],
        confirm: str | None = None,
    ) -> Any:
        """Export field map CSV/H5 metadata after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, payload=payload)
        return _compact(client.post("/field_map/export", body))

    @registry.mcp.tool(name="qlib_export_run_bin_unified_v2_confirmed")
    def qlib_export_run_bin_unified_v2_confirmed(
        payload: dict[str, Any],
        confirm: str | None = None,
    ) -> Any:
        """Run Qlib Bin unified_export_v2 after explicit confirmation."""

        body = _confirmed_body(registry, confirm=confirm, payload=payload)
        return _compact(client.post("/bin/unified_export_v2", body))

    @registry.mcp.tool(name="qlib_export_generate_backtest_candidate_confirmed")
    def qlib_export_generate_backtest_candidate_confirmed(
        payload: dict[str, Any],
        confirm: str | None = None,
    ) -> Any:
        """Generate H5/Bin candidate datasets without production promotion."""

        registry.confirm(confirm, QLIB_EXPORT_RUN_CONFIRM, "confirm")
        body = _body(payload)
        snapshot_id = _write_fragment(registry, body.get("snapshot_id"), "snapshot_id")
        end = _require_date(body.get("end"), "end")
        bin_payload = body.get("bin_payload")
        if bin_payload is not None:
            if not isinstance(bin_payload, dict):
                raise ValueError("bin_payload must be an object when provided")
            bin_payload = _sanitize_payload_identifiers(registry, bin_payload)
        base_payload = {
            "snapshot_id": snapshot_id,
            "end": end,
            "stock_universe_mode": body.get("stock_universe_mode", "pit_spans"),
            "universe_key": body.get("universe_key", "shsz_st_pit_active_v1"),
        }
        steps: list[dict[str, Any]] = []

        h5_daily_aux = _compact(client.post(f"/snapshots/{snapshot_id}/incremental_all", base_payload))
        steps.append({"step": "h5_daily_aux_incremental_all", "result": h5_daily_aux, "minute_h5_included": False})

        if body.get("include_minute_h5", True):
            h5_minute = _compact(client.post("/snapshots/minute/incremental", base_payload))
            steps.append({"step": "h5_minute_incremental", "result": h5_minute})

        if body.get("build_static_factors", True):
            static_factors = _compact(client.post(f"/snapshots/{snapshot_id}/static_factors", {}))
            steps.append({"step": "static_factors", "result": static_factors})

        if body.get("export_field_map", True):
            field_map = _compact(client.post("/field_map/export", {"snapshot_id": snapshot_id, "write_to_h5": True}))
            steps.append({"step": "field_map", "result": field_map})

        if bin_payload:
            bin_result = _compact(client.post("/bin/unified_export_v2", bin_payload))
            steps.append({"step": "bin_unified_export_v2", "result": bin_result})

        return {
            "status": "candidate_generated",
            "candidate_only": True,
            "production_promotion_supported": False,
            "snapshot_id": snapshot_id,
            "end": end,
            "steps": steps,
            "next_actions": [
                "Run qlib_export_validate_snapshot for daily and minute datasets.",
                "Run Data Doctor before any production replacement decision.",
                "Do not treat this MCP result as production promotion.",
            ],
        }

    registry.register_tool_count("qlib_export", TOOL_COUNT)
