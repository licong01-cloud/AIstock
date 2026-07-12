from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd
import pytest

from backend.mcp.modules import factor_library as factor_library_mcp
from backend.mcp.modules import factor_metrics as factor_metrics_mcp
from backend.routers import factor_correlation, factor_library, factor_metrics
from backend.services import rdagent_factor_metrics_sync
from backend.services.factor_metrics_contract import (
    H20_CONTRACT_PRESENT_PARAM,
    H20_METRIC_FIELDS,
    with_h20_metric_defaults,
)
from backend.services.quantevolver import factor_official_evaluation_service as official_service
from backend.services.quantevolver import factor_compute_preflight
from backend.services.quantevolver import qe_eval_v2_metric_engine as metric_engine


ROOT = Path(__file__).resolve().parents[2]
H20_FIELD_TYPES = {
    "h20_return_horizon": "TEXT",
    "h20_ic_mean": "DOUBLE PRECISION",
    "h20_ic_std": "DOUBLE PRECISION",
    "h20_rank_ic_mean": "DOUBLE PRECISION",
    "h20_rank_ic_std": "DOUBLE PRECISION",
    "h20_icir": "DOUBLE PRECISION",
    "h20_rank_icir": "DOUBLE PRECISION",
    "h20_icir_hac": "DOUBLE PRECISION",
    "h20_rank_icir_hac": "DOUBLE PRECISION",
    "h20_ic_positive_ratio": "DOUBLE PRECISION",
    "h20_n_obs": "INTEGER",
    "h20_hac_lag": "INTEGER",
}
H20_FIELDS = H20_METRIC_FIELDS


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self._rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        if "SELECT factor_name, code_text" in sql:
            self._rows = [("alpha_h20", "def alpha_h20(df):\n    return df")]
        else:
            self._rows = []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = _FakeCursor()
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self, *args, **kwargs) -> _FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _StaticRowsCursor(_FakeCursor):
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        super().__init__()
        self._rows = rows

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))


class _StaticRowsConnection(_FakeConnection):
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        super().__init__()
        self.cursor_instance = _StaticRowsCursor(rows)


class _StubMCP:
    def __init__(self) -> None:
        self.tools: dict[str, Any] = {}

    def tool(self, name: str | None = None, **kwargs: Any):
        def decorator(func):
            self.tools[name or func.__name__] = func
            return func

        return decorator


class _EchoClient:
    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "path": path,
            "params": params or {},
            "items": [{field: None for field in H20_FIELDS}],
        }

    def post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        return {"path": path, "body": body}


class _StubRegistry:
    def __init__(self) -> None:
        self.mcp = _StubMCP()

    def client(self, name: str) -> _EchoClient:
        return _EchoClient()

    def sanitize(self, value: str, name: str) -> str:
        return value

    def confirm(self, actual: str | None, expected: str, name: str) -> None:
        if actual != expected:
            raise ValueError(name)

    def register_tool_count(self, module_name: str, count: int) -> None:
        return None


def _metric_record(**extra: Any) -> dict[str, Any]:
    return {
        "factor_name": "alpha_h20",
        "eval_window": "full",
        "data_start": "2024-01-02",
        "data_end": "2025-12-31",
        **extra,
    }


def test_h20_schema_sources_and_migration_are_additive_and_nullable() -> None:
    migration = (
        ROOT / "backend/migrations/factor_metrics_h20_companion_fields_20260711.sql"
    ).read_text(encoding="utf-8")
    init_quant = (ROOT / "backend/db/init_quant_schema.py").read_text(encoding="utf-8")
    init_catalog = (ROOT / "backend/init_catalog_db.py").read_text(encoding="utf-8")

    for field, sql_type in H20_FIELD_TYPES.items():
        assert f"ADD COLUMN IF NOT EXISTS {field}" in migration
        assert sql_type in migration.split(f"ADD COLUMN IF NOT EXISTS {field}", 1)[1].split(",", 1)[0]
        assert field in init_quant
        assert field in init_catalog
        assert f"COMMENT ON COLUMN aistock_factor_metrics.{field}" in migration

    assert "NOT NULL" not in migration.split("ALTER TABLE aistock_factor_metrics", 1)[1].split(";", 1)[0]


@pytest.mark.parametrize(
    ("h20_payload", "expected_horizon", "expected_contract_present"),
    [
        ({}, None, False),
        (
            {
                "h20_return_horizon": "T21T1",
                "h20_ic_mean": 0.021,
                "h20_ic_std": 0.08,
                "h20_rank_ic_mean": 0.024,
                "h20_rank_ic_std": 0.075,
                "h20_icir": 0.2625,
                "h20_rank_icir": 0.32,
                "h20_icir_hac": 0.19,
                "h20_rank_icir_hac": 0.22,
                "h20_ic_positive_ratio": 0.56,
                "h20_n_obs": 412,
                "h20_hac_lag": 19,
            },
            "T21T1",
            True,
        ),
    ],
)
def test_official_metric_upsert_accepts_new_and_legacy_payloads(
    monkeypatch: pytest.MonkeyPatch,
    h20_payload: dict[str, Any],
    expected_horizon: str | None,
    expected_contract_present: bool,
) -> None:
    conn = _FakeConnection()
    monkeypatch.setattr(official_service, "get_conn", lambda: conn)
    monkeypatch.setattr(
        official_service,
        "_emit_factor_recompute_event",
        lambda **kwargs: "evt-h20",
    )
    service = object.__new__(official_service.FactorOfficialEvaluationService)

    result = service._save_metrics(
        {"calc_batch_id": "batch-h20", "metrics": [_metric_record(**h20_payload)]},
        snapshot_date="2025-12-31",
        factor_ids={"alpha_h20": 7},
    )

    metric_writes = [
        params
        for sql, params in conn.cursor_instance.executed
        if "INSERT INTO aistock_factor_metrics" in sql
    ]
    assert result["inserted"] == 1
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert len(metric_writes) == 1
    assert metric_writes[0]["h20_return_horizon"] == expected_horizon
    assert metric_writes[0][H20_CONTRACT_PRESENT_PARAM] is expected_contract_present
    for field in H20_FIELDS:
        assert metric_writes[0][field] == h20_payload.get(field)
    # Exercise Python's named-placeholder lookup because the fake cursor does
    # not run psycopg's binding layer.
    assert "INSERT INTO aistock_factor_metrics" in official_service._UPSERT_SQL % metric_writes[0]


def test_all_metric_upsert_contracts_include_h20_companion_fields() -> None:
    for sql in (official_service._UPSERT_SQL, rdagent_factor_metrics_sync._UPSERT_SQL):
        for field in H20_FIELDS:
            assert field in sql
            assert f"%({field})s" in sql
            assert f"EXCLUDED.{field}" in sql
            assert f"aistock_factor_metrics.{field}" in sql
        assert f"%({H20_CONTRACT_PRESENT_PARAM})s" in sql


def test_legacy_rd_payload_is_normalized_before_h20_named_binding() -> None:
    placeholder_names = set(re.findall(r"%\(([^)]+)\)s", rdagent_factor_metrics_sync._UPSERT_SQL))
    legacy_params = {
        name: None
        for name in placeholder_names
        if name not in H20_FIELDS
    }

    normalized = rdagent_factor_metrics_sync._normalize_upsert_params(legacy_params)

    assert all(field in normalized and normalized[field] is None for field in H20_FIELDS)
    assert normalized[H20_CONTRACT_PRESENT_PARAM] is False
    assert "INSERT INTO aistock_factor_metrics" in rdagent_factor_metrics_sync._UPSERT_SQL % normalized


def test_explicit_nullable_h20_contract_is_distinct_from_legacy_absence() -> None:
    normalized = with_h20_metric_defaults({"h20_return_horizon": None})

    assert normalized[H20_CONTRACT_PRESENT_PARAM] is True
    assert all(field in normalized for field in H20_FIELDS)


def test_official_summary_positional_mapping_includes_h20_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h20_values = (
        "T21T1",
        0.021,
        0.08,
        0.024,
        0.075,
        0.2625,
        0.32,
        0.19,
        0.22,
        0.56,
        412,
        19,
    )
    row = (
        "alpha_h20",
        0.01,
        1.2,
        0.18,
        0.012,
        0.1,
        1.58,
        1.7,
        1,
        20,
        0.01,
        "2026-01-02T00:00:00+00:00",
        "2025-12-31",
        "batch-h20",
        *h20_values,
    )
    conn = _StaticRowsConnection([row])
    monkeypatch.setattr(official_service, "get_conn", lambda: conn)
    service = object.__new__(official_service.FactorOfficialEvaluationService)

    payload = service.get_summary()
    summary = payload["summary"]["alpha_h20"]

    assert tuple(summary[field] for field in H20_FIELDS) == h20_values
    select_sql = conn.cursor_instance.executed[0][0]
    assert all(field in select_sql for field in H20_FIELDS)


def test_factor_metric_facades_expose_h20_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []

    def _rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        captured_sql.append(sql)
        return [{field: None for field in H20_FIELDS}]

    monkeypatch.setattr(factor_metrics, "_rows", _rows)
    monkeypatch.setattr(factor_metrics, "_one", lambda *args, **kwargs: {"total": 1})
    monkeypatch.setattr(factor_library, "_rows", _rows)

    metrics_payload = factor_metrics.get_result(factor_name="alpha_h20")
    compare_payload = factor_metrics.compare_versions("alpha_h20")
    library_payload = factor_library.get_metric_summary("alpha_h20")

    assert metrics_payload["items"][0].keys() >= set(H20_FIELDS)
    assert compare_payload["items"][0].keys() >= set(H20_FIELDS)
    assert library_payload["items"][0].keys() >= set(H20_FIELDS)
    for field in H20_FIELDS:
        assert sum(field in sql for sql in captured_sql) == 3
        assert field in factor_library.METRIC_FIELDS


def test_mcp_metric_tools_advertise_and_pass_through_h20_companion_output() -> None:
    library_registry = _StubRegistry()
    metrics_registry = _StubRegistry()
    factor_library_mcp.register(library_registry)
    factor_metrics_mcp.register(metrics_registry)

    library_tool = library_registry.mcp.tools["factor_library_get_metric_summary"]
    result_tool = metrics_registry.mcp.tools["factor_metrics_get_result"]
    compare_tool = metrics_registry.mcp.tools["factor_metrics_compare_versions"]

    assert "nullable h20/HAC companion fields" in (library_tool.__doc__ or "")
    assert "nullable h20/HAC companion fields" in (result_tool.__doc__ or "")
    assert "nullable h20/HAC companion fields" in (compare_tool.__doc__ or "")
    for payload in (library_tool("alpha_h20"), result_tool("alpha_h20"), compare_tool("alpha_h20")):
        assert payload["items"][0].keys() >= set(H20_FIELDS)


def test_qe_eval_v2_computes_h20_hac_companion_without_changing_1d_contract() -> None:
    rng = np.random.default_rng(621)
    n_dates = 140
    n_instruments = 12
    dates = pd.bdate_range("2024-07-01", periods=n_dates)
    instruments = [f"{index:06d}.SZ" for index in range(n_instruments)]
    factor_values = rng.normal(size=(n_dates, n_instruments))
    one_day_returns = rng.normal(scale=0.02, size=factor_values.shape)
    time_loading = 0.15 + 0.10 * np.sin(np.arange(n_dates) / 7.0)
    h20_returns = (
        factor_values * time_loading[:, None]
        + rng.normal(scale=0.8, size=factor_values.shape)
    )
    close = pd.DataFrame(
        100.0 + np.cumsum(rng.normal(scale=0.5, size=factor_values.shape), axis=0),
        index=dates,
        columns=instruments,
    )

    metrics, reports = metric_engine._compute_factor_metrics_impl(
        fname="alpha_h20",
        f_arr_full=factor_values,
        dates=dates,
        fwd_arr=one_day_returns,
        fwd_arrs={
            "1d": one_day_returns,
            "5d": rng.normal(size=factor_values.shape),
            "10d": rng.normal(size=factor_values.shape),
            "20d": h20_returns,
        },
        close_unstacked=close,
        data_start=str(dates[0].date()),
        data_end=str(dates[-1].date()),
        calc_batch_id="batch-621",
        suspended_mask=np.zeros_like(factor_values, dtype=bool),
        eligible_mask=np.ones_like(factor_values, dtype=bool),
    )

    full = next(row for row in metrics if row["eval_window"] == "full")
    assert full["return_horizon"] == "1d"
    assert full["h20_return_horizon"] == "T21T1"
    assert full["h20_hac_lag"] == 19
    assert full["h20_n_obs"] == n_dates
    assert np.isfinite(full["h20_ic_mean"])
    assert np.isfinite(full["h20_rank_ic_mean"])
    assert np.isfinite(full["h20_icir_hac"])
    assert np.isfinite(full["h20_rank_icir_hac"])
    assert all(row["status"] == "ok" for row in reports)


def test_h20_hac_is_nullable_for_insufficient_or_degenerate_ic_series() -> None:
    assert metric_engine._hac_icir(np.asarray([0.1] * 19), lag=19) is None
    assert metric_engine._hac_icir(np.asarray([0.1] * 25), lag=19) is None
    values = np.asarray([0.01 + (index % 5) * 0.002 for index in range(40)])
    assert np.isfinite(metric_engine._hac_icir(values, lag=19))


def test_factor_mcp_plans_report_full_count_and_cache_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    preview = [
        {"factor_name": f"factor_{index}", "source": "manual", "is_available": True}
        for index in range(20)
    ]
    stale_cache = {
        "ok": False,
        "blockers": ["official_cache_snapshot_mismatch"],
    }
    for module in (factor_metrics, factor_correlation):
        monkeypatch.setattr(module, "_rows", lambda *args, **kwargs: preview)
        monkeypatch.setattr(module, "_one", lambda *args, **kwargs: {"total": 585})
        monkeypatch.setattr(
            module,
            "build_official_cache_preflight",
            lambda **kwargs: {**stale_cache, **kwargs},
        )

    metrics_plan = factor_metrics.plan_metrics(
        factor_metrics.FactorMetricsPlanRequest(options={"end_date": "2026-06-30"})
    )
    assert metrics_plan["requested_factor_count"] == "all_available"
    assert metrics_plan["eligible_factor_count"] == 585
    assert metrics_plan["eligible_preview_count"] == 20
    assert metrics_plan["cache_rebuild_required"] is True
    metrics_validation = factor_metrics.validate_inputs(
        factor_metrics.FactorMetricsPlanRequest(options={"end_date": "2026-06-30"})
    )
    assert metrics_validation["ok"] is True
    assert metrics_validation["warnings"] == ["official_cache_snapshot_mismatch"]

    correlation_plan = factor_correlation.plan_correlation(
        factor_correlation.FactorCorrelationPlanRequest(
            options={"as_of_date": "2026-06-30"}
        )
    )
    assert correlation_plan["eligible_factor_count"] == 585
    assert correlation_plan["eligible_factor_count_preview"] == 20
    assert correlation_plan["estimated_pair_count"] == 170820
    correlation_validation = factor_correlation.validate_inputs(
        factor_correlation.FactorCorrelationPlanRequest(
            options={"as_of_date": "2026-06-30"}
        )
    )
    assert correlation_validation["ok"] is False
    assert correlation_validation["blockers"] == [
        "official_cache_snapshot_mismatch"
    ]


def test_official_cache_preflight_reports_compact_full_run_blockers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        factor_compute_preflight,
        "get_correlation_factor_cache_status",
        lambda: {
            "cached_count": 580,
            "as_of_date": "2026-04-28",
            "window_backtest_end": "2026-04-28",
            "generated_at": "2026-07-11T08:40:07Z",
            "cache_source": "offline_research_backtest_factor_values",
            "cache_root": "/mnt/f/Dev/AIstock/rdagent_assets/factor_values",
            "integrity_ok": False,
            "moneyflow_unit_contract_version": None,
            "integrity": {
                "as_of_date_distribution": {
                    "2026-04-30": 575,
                    "2026-04-28": 5,
                }
            },
        },
    )

    result = factor_compute_preflight.build_official_cache_preflight(
        target_end="2026-06-30",
        eligible_factor_count=585,
    )

    assert result["ok"] is False
    assert result["cached_factor_count"] == 580
    assert result["as_of_date_distribution"] == {
        "2026-04-30": 575,
        "2026-04-28": 5,
    }
    assert result["blockers"] == [
        "official_cache_integrity_failed",
        "official_cache_snapshot_mismatch",
        "official_cache_moneyflow_contract_mismatch",
        "official_cache_factor_coverage_incomplete",
    ]
