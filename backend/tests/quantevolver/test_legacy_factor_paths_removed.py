from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from backend.services.quantevolver.factor_value_loader import FactorValueLoader


class _FactorPipelineConn:
    def __init__(self, rows):
        self.cur = MagicMock()
        self.cur.description = [("factor_name",), ("source",), ("factor_type",)]
        self.cur.fetchall.return_value = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        cm = MagicMock()
        cm.__enter__.return_value = self.cur
        cm.__exit__.return_value = False
        return cm


def test_factor_value_loader_requires_explicit_source() -> None:
    with pytest.raises(ValueError, match="source must be explicit"):
        FactorValueLoader()


def test_factor_analyst_uses_official_single_loader(monkeypatch) -> None:
    from backend.services.quantevolver import factor_analyst as analyst_mod

    captured: dict[str, str] = {}

    class FakeLoader:
        def __init__(self, *, source: str, pipeline_dir: str):
            captured["source"] = source
            captured["pipeline_dir"] = pipeline_dir

    monkeypatch.setattr(analyst_mod, "FactorValueLoader", FakeLoader)

    loader = analyst_mod._official_factor_value_loader()

    assert isinstance(loader, FakeLoader)
    assert captured["source"] == "single"
    assert captured["pipeline_dir"].replace("\\", "/").endswith("rdagent_assets/factor_values")


def test_factor_value_pipeline_computable_factors_use_official_catalog_code(monkeypatch, tmp_path) -> None:
    from backend.services.quantevolver import factor_value_pipeline as pipeline_mod

    conn = _FactorPipelineConn([("factor_a", "rdagent_task_sync", "alpha")])
    monkeypatch.setattr(pipeline_mod, "get_conn", lambda: conn)

    result = pipeline_mod.FactorValuePipeline(output_dir=str(tmp_path)).get_computable_factors(
        limit=10,
        factor_types=["alpha"],
    )

    sql, params = conn.cur.execute.call_args.args
    assert "is_available = true" in sql
    assert "code_text IS NOT NULL" in sql
    assert "length(trim(code_text)) > 0" in sql
    assert "factor_type IN (%s)" in sql
    assert "transformation_status = 'SUCCESS'" not in sql
    assert "qe_code_path" not in sql
    assert "last_transformation_at" not in sql
    assert "LIMIT 10" in sql
    assert params == ["alpha"]
    assert result == [
        {
            "factor_name": "factor_a",
            "source": "rdagent_task_sync",
            "factor_type": "alpha",
            "code_source": "code_text",
            "code_exists": True,
            "cache_source": "official_offline_backtest_factor_data",
        }
    ]


def test_factor_analyst_no_legacy_correlation_writer_or_naked_loader() -> None:
    source = Path("backend/services/quantevolver/factor_analyst.py").read_text(encoding="utf-8")

    assert "FactorValueLoader()" not in source
    assert "def _estimate_by_category" not in source
    assert "category_estimation" not in source
    assert "def _upsert_correlation" not in source
    assert "INSERT INTO qe_factor_correlations" not in source
    assert "save_hdf5=False" in source


def test_correlation_router_loader_defaults_to_official_single() -> None:
    source = Path("backend/routers/quantevolver_evolution.py").read_text(encoding="utf-8")

    assert 'def _get_loader(source: str = "single")' in source
    assert 'def _get_loader(source: str = "auto")' not in source
    assert "SELECT factor_name, source, is_sota_factor, is_available,\n                               code_text, asset_path" in source
    assert "realtime_code_text, asset_path, qe_code_path" not in source
    assert 'for path_key in ["qe_code_path", "asset_path"]' not in source


def test_correlation_llm_agent_uses_official_source_only() -> None:
    source = Path("backend/services/quantevolver/correlation_llm_agent.py").read_text(encoding="utf-8")

    assert "SELECT code_text, asset_path" in source
    assert "realtime_code_text" not in source
    assert "qe_code_path" not in source


def test_correlation_service_error_text_does_not_require_transformed_code() -> None:
    source = Path("backend/services/quantevolver/correlation_compute_service.py").read_text(encoding="utf-8")

    assert "transformation_status=SUCCESS 且 qe_code_path" not in source


def test_factor_code_transformer_is_live_only() -> None:
    from backend.services.quantevolver.factor_code_transformer import (
        FactorCodeTransformer,
        NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT,
    )

    with pytest.raises(ValueError, match="live-only"):
        FactorCodeTransformer(usage_context="official_factor_cache")

    transformer = FactorCodeTransformer(usage_context=NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT)
    assert transformer.usage_context == NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT


def test_backfill_factor_cache_main_is_retired() -> None:
    from scripts import backfill_factor_cache

    with pytest.raises(SystemExit, match="official_factor_full_compute"):
        backfill_factor_cache.main()


def test_factor_transformation_frontend_does_not_expose_realtime_dto_names() -> None:
    root = Path("frontend/src/app/quantevolver/factor-transformation")
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in root.rglob("*")
        if path.suffix in {".ts", ".tsx"}
    )

    assert "has_realtime_code" not in source
    assert "realtime_code_text" not in source
    assert 'tab: "original" | "realtime"' not in source
    assert '"realtime"' not in source
    assert "has_non_official_code" in source
    assert "non_official_code_path" in source
    assert "transformed_code_text" in source



def test_deterministic_v2_backfill_entry_is_removed() -> None:
    source = Path("backend/services/quantevolver/factor_analyst.py").read_text(encoding="utf-8")

    assert not Path("scripts/_backfill_v2_deterministic.py").exists()
    assert "backfill_deterministic_v2" not in source
    assert "batch_backfill_deterministic_v2" not in source
