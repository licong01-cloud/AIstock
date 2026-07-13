import json

import pytest

from backend.routers import quantevolver as qe_router
from backend.services.quantevolver.config_composer import (
    ConfigComposer,
    QE_DEFAULT_SIGNAL_END,
    RDAGENT_DEFAULT_DATA_SPLIT,
)
from scripts import backfill_factor_cache


def test_qe_wsl_command_forces_backtest_cache_dir_over_inherited_env() -> None:
    composer = ConfigComposer()
    env_lines, _ = composer._build_auto_wsl_command_parts(
        "/mnt/f/Dev/RD-Agent-main/qe_workspace/demo",
        has_custom_factors=True,
        use_custom_model=False,
        backtest_freq="1min",
    )

    factor_cache_exports = [line for line in env_lines if "FACTOR_CACHE_DIR" in line]

    assert len(factor_cache_exports) == 1
    assert "factor_values_shadow" not in factor_cache_exports[0]
    assert "FACTOR_CACHE_DIR:-" not in factor_cache_exports[0]
    assert factor_cache_exports[0].startswith('export FACTOR_CACHE_DIR="')


def test_qe_wsl_command_rejects_non_official_factor_cache_dir() -> None:
    composer = ConfigComposer()

    with pytest.raises(ValueError, match="official factor_values cache"):
        composer._build_auto_wsl_command_parts(
            "/mnt/f/Dev/RD-Agent-main/qe_workspace/demo",
            has_custom_factors=True,
            use_custom_model=False,
            backtest_freq="1min",
            factor_cache_dir="/mnt/f/Dev/AIstock/rdagent_assets/factor_values_shadow/single",
        )


def _write_cache_meta(root, factors) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "single").mkdir(parents=True, exist_ok=True)
    (root / "_meta.json").write_text(
        json.dumps({"factors": factors}, ensure_ascii=False),
        encoding="utf-8",
    )


def _cache_source_specs(tmp_path):
    backtest_root = tmp_path / "factor_values"
    shadow_root = tmp_path / "factor_values_shadow"
    return (
        {
            "key": "backtest",
            "label": "回测缓存",
            "single_dir": backtest_root / "single",
            "meta_path": backtest_root / "_meta.json",
        },
        {
            "key": "shadow_cache",
            "label": "官方快照",
            "single_dir": shadow_root / "single",
            "meta_path": shadow_root / "_meta.json",
        },
    )


def test_factor_cache_rejects_backtest_spec_pointing_to_non_official_root(tmp_path) -> None:
    qe_router._invalidate_cache_meta()
    shadow_root = tmp_path / "factor_values_shadow"
    _write_cache_meta(
        shadow_root,
        {
            "LeakFactor": {
                "status": "ok",
                "date_range": "2018-08-01~2026-04-28",
                "rows": 10,
            }
        },
    )
    (shadow_root / "single" / "LeakFactor.parquet").write_bytes(b"PAR1")
    specs = (
        {
            "key": "backtest",
            "label": "QE backtest",
            "single_dir": shadow_root / "single",
            "meta_path": shadow_root / "_meta.json",
        },
    )

    candidates = qe_router._collect_factor_cache_candidates("LeakFactor", source_specs=specs)

    assert candidates == []


def test_factor_cache_ignores_non_official_cache_even_when_backtest_error(tmp_path) -> None:
    qe_router._invalidate_cache_meta()
    specs = _cache_source_specs(tmp_path)
    backtest_root = tmp_path / "factor_values"
    shadow_root = tmp_path / "factor_values_shadow"
    _write_cache_meta(
        backtest_root,
        {
            "RESI5": {
                "status": "error",
                "computed_at": "2026-05-01T00:22:26",
                "error": "factor.py did not produce output",
            }
        },
    )
    _write_cache_meta(
        shadow_root,
        {
            "RESI5": {
                "status": "ok",
                "computed_at": "2026-05-01T08:21:46",
                "date_range": "2018-08-07~2026-04-10",
                "as_of_date": "2026-04-10",
                "data_source_mode": "snapshot",
                "rows": 7247352,
            }
        },
    )
    (shadow_root / "single" / "RESI5.parquet").write_bytes(b"PAR1")

    selected = qe_router._choose_best_factor_cache_candidate(
        qe_router._collect_factor_cache_candidates("RESI5", source_specs=specs),
        "2018-08-01",
        "2026-04-10",
    )

    assert selected is not None
    assert selected["valid_cache"] is False
    assert selected["source_key"] == "backtest"
    assert selected["cache_status"] == "error"


def test_factor_cache_covers_recorded_window_even_with_long_warmup(tmp_path) -> None:
    qe_router._invalidate_cache_meta()
    specs = _cache_source_specs(tmp_path)
    backtest_root = tmp_path / "factor_values"
    _write_cache_meta(
        backtest_root,
        {
            "LongWarmupFactor": {
                "status": "ok",
                "computed_at": "2026-05-01T08:21:46",
                "date_range": "2018-12-31~2026-04-28",
                "as_of_date": "2026-04-28",
                "window_train_start": "2018-08-01",
                "window_backtest_end": "2026-04-28",
                "rows": 100,
            }
        },
    )
    (backtest_root / "single" / "LongWarmupFactor.parquet").write_bytes(b"PAR1")

    selected = qe_router._choose_best_factor_cache_candidate(
        qe_router._collect_factor_cache_candidates("LongWarmupFactor", source_specs=specs),
        "2018-08-01",
        "2026-04-28",
    )

    assert selected is not None
    assert qe_router._factor_cache_candidate_covers(selected, "2018-08-01", "2026-04-28")


def test_factor_cache_backfill_skips_recorded_warmup_window(monkeypatch, tmp_path) -> None:
    single_dir = tmp_path / "single"
    single_dir.mkdir()
    monkeypatch.setattr(backfill_factor_cache, "SINGLE_DIR", single_dir)
    (single_dir / "LongWarmupFactor.parquet").write_bytes(b"PAR1")

    plan = backfill_factor_cache.plan_factor_action(
        "LongWarmupFactor",
        "2018-08-01",
        "2026-04-28",
        {
            "factors": {
                "LongWarmupFactor": {
                    "date_range": "2018-12-31~2026-04-28",
                    "window_train_start": "2018-08-01",
                    "window_backtest_end": "2026-04-28",
                }
            }
        },
        incremental=True,
        force=False,
    )

    assert plan["action"] == "skip"
    assert plan["reason"] == "covered_warmup_window"


def test_factor_cache_backfill_extends_forward_after_warmup(monkeypatch, tmp_path) -> None:
    single_dir = tmp_path / "single"
    single_dir.mkdir()
    monkeypatch.setattr(backfill_factor_cache, "SINGLE_DIR", single_dir)
    (single_dir / "LongWarmupFactor.parquet").write_bytes(b"PAR1")

    plan = backfill_factor_cache.plan_factor_action(
        "LongWarmupFactor",
        "2018-08-01",
        "2026-04-28",
        {
            "factors": {
                "LongWarmupFactor": {
                    "date_range": "2018-12-31~2026-04-10",
                    "window_train_start": "2018-08-01",
                    "window_backtest_end": "2026-04-10",
                }
            }
        },
        incremental=True,
        force=False,
    )

    assert plan["action"] == "extend_forward"


def test_factor_cache_backfill_rebuilds_on_universe_mismatch(monkeypatch, tmp_path) -> None:
    single_dir = tmp_path / "single"
    single_dir.mkdir()
    monkeypatch.setattr(backfill_factor_cache, "SINGLE_DIR", single_dir)
    (single_dir / "PitFactor.parquet").write_bytes(b"PAR1")

    plan = backfill_factor_cache.plan_factor_action(
        "PitFactor",
        "2018-08-01",
        "2026-04-28",
        {
            "factors": {
                "PitFactor": {
                    "date_range": "2018-08-01~2026-04-28",
                    "window_train_start": "2018-08-01",
                    "window_backtest_end": "2026-04-28",
                    "universe_key": "legacy_all",
                    "universe_fingerprint_sha256": "old",
                    "index_policy": "legacy_index",
                }
            }
        },
        incremental=True,
        force=False,
        expected_universe_metadata={
            "universe_key": "shsz_st_pit_active_v1",
            "universe_fingerprint_sha256": "new",
            "index_policy": "st_pit_buy_eligible_reindexed_v1",
        },
    )

    assert plan["action"] == "full_rebuild"
    assert plan["reason"] == "universe_mismatch"


def test_factor_cache_backfill_rebuilds_on_hash_mismatch(monkeypatch, tmp_path) -> None:
    single_dir = tmp_path / "single"
    single_dir.mkdir()
    monkeypatch.setattr(backfill_factor_cache, "SINGLE_DIR", single_dir)
    (single_dir / "HashFactor.parquet").write_bytes(b"PAR1")

    plan = backfill_factor_cache.plan_factor_action(
        "HashFactor",
        "2018-08-01",
        "2026-04-28",
        {
            "factors": {
                "HashFactor": {
                    "date_range": "2018-08-01~2026-04-28",
                    "window_train_start": "2018-08-01",
                    "window_backtest_end": "2026-04-28",
                    "source_hash_raw": "oldhash",
                }
            }
        },
        incremental=True,
        force=False,
        expected_source_hash_raw="newhash",
    )

    assert plan["action"] == "full_rebuild"
    assert plan["reason"] == "hash_mismatch"


def test_qe_prepare_factors_default_window_uses_current_signal_end_and_records_cache_window(monkeypatch) -> None:
    monkeypatch.setattr(
        ConfigComposer,
        "_resolve_factor_cache_universe_metadata",
        lambda self, *, start_date, end_date: {
            "universe_key": "shsz_st_pit_active_v1",
            "universe_rule_version": "rule_v1",
            "universe_fingerprint_sha256": "fp-test",
            "index_policy": "st_pit_buy_eligible_reindexed_v1",
            "coverage_semantics": "st_pit_buy_eligible_suspend_excluded_non_warmup_v1",
        },
    )
    code = (
        "def calculate_DemoFactor(instruments, start_date, end_date):\n"
        "    import pandas as pd\n"
        "    return pd.DataFrame(index=pd.MultiIndex.from_arrays([[], []], names=['datetime', 'instrument']), columns=['DemoFactor'])\n"
    )
    script = ConfigComposer()._compose_prepare_factors(
        [{"factor_name": "DemoFactor", "code_text": code}],
        factor_data_dir="/tmp/factor_data",
        data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
    )

    assert script is not None
    assert f"TEST_END = '{QE_DEFAULT_SIGNAL_END}'" in script
    assert "TEST_END = '2026-03-10'" not in script
    assert "'window_train_start': TRAIN_START" in script
    assert "'window_backtest_end': TEST_END" in script
    assert "entry.get('window_train_start')" in script
    assert "open(FACTOR_CACHE_META, 'r', encoding='utf-8')" in script
    assert "os.makedirs(FACTOR_CACHE_SINGLE_DIR, exist_ok=True)" in script
    assert "os.fdopen(tmp_fd, 'w', encoding='utf-8')" in script
    assert "FACTOR_CACHE_EXPECTED_UNIVERSE_META" in script
    assert "def _is_forbidden_factor_cache_path" not in script
    assert "def _is_official_factor_cache_path_shape" in script
    assert "factor_values_shadow" not in script
    assert "'data_source_mode': 'backtest_factor_data_dir'" in script
    assert "'universe_fingerprint_sha256': 'fp-test'" in script
    assert "_cache_universe_mismatch" in script
    assert "cache universe mismatch" in script
    assert "factors[factor_name].update(universe_meta)" in script


def test_qe_prepare_factors_keeps_cache_hits_when_expected_fingerprint_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        ConfigComposer,
        "_resolve_factor_cache_universe_metadata",
        lambda self, *, start_date, end_date: {
            "data_freshness_profile": "qe_backtest_coverage",
            "universe_key": "shsz_st_pit_active_v1",
            "universe_rule_version": "rule_v1",
            "universe_fingerprint_sha256": "",
            "index_policy": "st_pit_buy_eligible_reindexed_v1",
            "coverage_semantics": "st_pit_buy_eligible_suspend_excluded_non_warmup_v1",
        },
    )
    script = ConfigComposer()._compose_prepare_factors(
        [
            {
                "factor_name": "DemoFactor",
                "code_text": "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n",
            }
        ],
        factor_data_dir="/tmp/factor_data",
        data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
    )

    assert script is not None
    assert "'data_freshness_profile': 'qe_backtest_coverage'" in script
    assert "universe_fingerprint_missing_expected" not in script
    assert "expected_fp = expected.get('universe_fingerprint_sha256')" in script
    assert "return 'universe_fingerprint_sha256'" in script


def test_factor_cache_uses_error_only_when_no_valid_cache_exists(tmp_path) -> None:
    qe_router._invalidate_cache_meta()
    specs = _cache_source_specs(tmp_path)
    backtest_root = tmp_path / "factor_values"
    shadow_root = tmp_path / "factor_values_shadow"
    _write_cache_meta(
        backtest_root,
        {
            "KLEN": {
                "status": "error",
                "computed_at": "2026-05-01T00:22:26",
                "error": "runtime failed",
            }
        },
    )
    _write_cache_meta(shadow_root, {})

    selected = qe_router._choose_best_factor_cache_candidate(
        qe_router._collect_factor_cache_candidates("KLEN", source_specs=specs)
    )

    assert selected is not None
    assert selected["valid_cache"] is False
    assert selected["cache_status"] == "error"
    assert selected["source_key"] == "backtest"



def test_factor_cache_stats_is_lightweight_and_counts_disk_meta_gap(monkeypatch, tmp_path) -> None:
    cache_root = tmp_path / "factor_values"
    single_dir = cache_root / "single"
    single_dir.mkdir(parents=True)
    (single_dir / "FactorA.parquet").write_bytes(b"PAR1" * 10)
    (single_dir / "FactorB.parquet").write_bytes(b"PAR1" * 20)
    (single_dir / "_merged_panel.parquet").write_bytes(b"PAR1" * 30)
    (cache_root / "_meta.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-01T00:00:00",
                "factors": {
                    "FactorA": {
                        "status": "ok",
                        "date_range": "2018-08-01~2026-04-30",
                        "as_of_date": "2026-04-30",
                        "window_train_start": "2018-08-01",
                        "window_backtest_end": "2026-04-28",
                        "data_source_mode": "backtest_factor_data_dir",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return [("FactorA", True), ("FactorB", True), ("DisabledC", False)]

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cur()

    def fail_if_heavy_parquet(path):
        raise AssertionError(f"factor-cache stats must not infer parquet windows by default: {path}")

    monkeypatch.setattr(qe_router, "FACTOR_CACHE_ROOT", cache_root)
    monkeypatch.setattr(qe_router, "FACTOR_CACHE_SINGLE_DIR", single_dir)
    monkeypatch.setattr(qe_router, "FACTOR_CACHE_META_PATH", cache_root / "_meta.json")
    monkeypatch.setattr(qe_router, "get_conn", lambda: _Conn())
    monkeypatch.setattr(qe_router, "_infer_factor_cache_window_from_parquet", fail_if_heavy_parquet)
    monkeypatch.setattr(qe_router, "_get_current_factor_code_hashes", lambda names: (_ for _ in ()).throw(AssertionError("hash check skipped by default")))
    qe_router._invalidate_cache_meta()

    result = qe_router.factor_cache_stats()

    assert result["ok"] is True
    assert result["stats_mode"] == "lightweight_inventory"
    assert result["hash_check_enabled"] is False
    assert result["db_hash_check_skipped"] is True
    assert result["total_cached"] == 2
    assert result["total_code_factors"] == 2
    assert result["coverage_pct"] == 100.0
    assert result["meta_valid_cached"] == 1
    assert result["reconcile_required"] == 1
    assert result["reconcile_required_sample"] == ["FactorB"]
    assert result["disk_factor_count"] == 2
    assert result["factor_parquet_count"] == 2
    assert result["all_parquet_count"] == 3
    assert result["merged_panel_present"] is True
    assert result["meta_factor_count"] == 1
    assert result["orphan_parquet_count"] == 1
    assert result["no_cache"] == 0
    assert result["date_range_distribution"]["metadata_pending"] == 1
