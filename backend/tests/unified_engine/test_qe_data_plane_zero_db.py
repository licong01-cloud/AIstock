"""BUG-989 anti-regression gates: the QE / multi-alpha computation data plane
must access ZERO database.

Invariant under test: QE and multi-alpha train / predict / backtest / combine
inputs come exclusively from frozen, versioned dataset files (qlib bin / H5 /
Parquet / sidecar).  The database is only allowed on the control plane
(experiment tasks, run status, result records).  Missing offline data fails
closed — no DB fallback, no online backfill, no silent degradation.

Gates:
1. The industry provider aligns per-trading-day PIT ids from H5/Parquet files.
2. Missing l2_code_id / insufficient coverage / missing frozen files fail
   closed with stable reason codes.
3. A poisoned database layer (any psycopg2/sqlalchemy import or composer
   get_conn call raises) does not affect the file provider or the frozen
   build-spec composer path.
4. The Composer artifact-generation module contains no market.* SQL and its
   frozen spec builder runs without any DB connection.
5. Every script copied into the QE workspace payload (runners, helpers,
   strategies, model package) contains no psycopg2/sqlalchemy/get_conn, no DB
   credential environment variables and no market.* SQL.
6. The frozen build spec and the rebuilt risk policy artifact carry
   path+version+hash traceability (provider_uri, snapshot identity, sha256
   pins) and the rebuild is deterministic.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MODELS_ROOT = PROJECT_ROOT / "aistock_models"
if str(MODELS_ROOT) not in sys.path:
    sys.path.insert(0, str(MODELS_ROOT))
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import qe_build_frozen_risk_policy as frozen_builder  # noqa: E402
from aistock_models import gats_industry_provider as provider_mod  # noqa: E402
from backend.services.quantevolver import config_composer as composer_module  # noqa: E402
from backend.services.quantevolver.config_composer import ConfigComposer  # noqa: E402
from backend.services.quantevolver.qe_dataset_contract import (  # noqa: E402
    QE_DATASET_CONTRACT_ID,
    QE_FROZEN_BIN_SNAPSHOT_ID,
    QE_FROZEN_BIN_UNIVERSE_KEY,
    QE_FROZEN_INSTRUMENTS_SHA256,
    QE_ST_PIT_UNIVERSE_KEY,
)

DATA_SPLIT = {
    "train_start": "2021-01-01",
    "train_end": "2021-06-30",
    "valid_start": "2021-07-01",
    "valid_end": "2021-09-30",
    "test_start": "2021-10-01",
    "test_end": "2021-12-31",
    "backtest_end": "2021-12-31",
}

FORBIDDEN_PATTERNS = ("psycopg2", "sqlalchemy", "get_conn", "POSTGRES_", "PG_PASSWORD", "TDX_DB")
MARKET_SQL_RE = re.compile(r"\b(FROM|JOIN|UPDATE|INTO)\s+market\.", re.IGNORECASE)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_frozen_bin_tree(root: Path, *, spans, calendar_dates) -> dict:
    """Create a minimal frozen qlib bin tree and return its sha256 pins."""
    instruments = root / "instruments"
    calendars = root / "calendars"
    instruments.mkdir(parents=True)
    calendars.mkdir(parents=True)
    instruments_file = instruments / "all.txt"
    instruments_file.write_text(
        "".join(f"{code}\t{start}\t{end}\n" for code, start, end in spans),
        encoding="utf-8",
    )
    calendar_file = calendars / "day.txt"
    calendar_file.write_text("".join(f"{day}\n" for day in calendar_dates), encoding="utf-8")
    meta_file = root / "meta_export.json"
    meta_file.write_text(
        json.dumps(
            {
                "snapshot_id": QE_FROZEN_BIN_SNAPSHOT_ID,
                "universe_key": QE_FROZEN_BIN_UNIVERSE_KEY,
                "generated_at": "2026-07-07T06:09:13",
            }
        ),
        encoding="utf-8",
    )
    return {
        "snapshot_id": QE_FROZEN_BIN_SNAPSHOT_ID,
        "universe_key": QE_FROZEN_BIN_UNIVERSE_KEY,
        "instruments_sha256": _sha256(instruments_file),
        "calendar_sha256": _sha256(calendar_file),
        "meta_export_sha256": _sha256(meta_file),
    }


def _write_spec(workspace: Path, provider_dir: Path, pins: dict, *, start="2021-01-04", end="2021-01-08") -> Path:
    spec = {
        "schema_version": frozen_builder.SPEC_SCHEMA_VERSION,
        "kind": frozen_builder.SPEC_KIND,
        "provider_uri_day": str(provider_dir),
        "start_date": start,
        "end_date": end,
        "profile": {
            "contract": "stock_event_risk_policy_v1",
            "providers": ["st_pit"],
            "hard_actions": ["block_buy", "force_exit"],
            "visible_time_mode": "asof",
            "strict_data_ready": True,
        },
        "dataset": {
            "contract_id": QE_DATASET_CONTRACT_ID,
            "st_universe_key": QE_ST_PIT_UNIVERSE_KEY,
            "rule_version": "frozen_qlib_bin_universe_v1",
        },
        "pins": pins,
    }
    spec_path = workspace / frozen_builder.SPEC_FILE
    spec_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
    return spec_path


def _write_sector_parquet(tmp_path: Path, rows, *, name="sector_data_fixture.parquet") -> Path:
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp(day), instrument) for day, instrument, _code in rows],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"l2_code_id": [code for _d, _i, code in rows]}, index=index)
    path = tmp_path / name
    frame.to_parquet(path)
    return path


# ---------------------------------------------------------------------------
# Gate 1: industry provider PIT alignment on frozen H5/Parquet samples
# ---------------------------------------------------------------------------


def test_gate1_provider_pit_alignment_on_h5_sample(tmp_path):
    pytest.importorskip("tables")
    rows = [
        ("2021-01-04", "000001.SZ", 10),
        ("2021-01-05", "000001.SZ", 20),
        ("2021-01-06", "000002.SZ", 30),
    ]
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp(day), instrument) for day, instrument, _code in rows],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"l2_code_id": [code for _d, _i, code in rows]}, index=index)
    h5_path = tmp_path / "sector_data_sample.h5"
    frame.to_hdf(h5_path, key="data", mode="w")

    provider = provider_mod.SectorDataIndustryIdProvider(source_path=h5_path, min_coverage=0.5)
    target = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2021-01-04"), "000001.SZ"),
            (pd.Timestamp("2021-01-05"), "000001.SZ"),
            (pd.Timestamp("2021-01-05"), "000002.SZ"),  # only known from 01-06 -> no future leak
        ],
        names=["datetime", "instrument"],
    )
    values = provider(target)

    assert values.loc[(pd.Timestamp("2021-01-04"), "000001.SZ")] == 10
    assert values.loc[(pd.Timestamp("2021-01-05"), "000001.SZ")] == 20
    assert pd.isna(values.loc[(pd.Timestamp("2021-01-05"), "000002.SZ")])
    assert provider.last_coverage["source"] == str(h5_path)


def test_gate1_provider_pit_alignment_on_parquet_sample(tmp_path):
    source = _write_sector_parquet(
        tmp_path,
        [("2021-01-04", "000001.SZ", 10), ("2021-01-05", "000001.SZ", 20)],
    )
    provider = provider_mod.SectorDataIndustryIdProvider(source_path=source, min_coverage=1.0)
    target = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2021-01-04"), "SZ000001"),  # qlib-style code normalisation
            (pd.Timestamp("2021-01-05"), "000001.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    values = provider(target)
    assert list(values) == [10, 20]
    assert provider.last_coverage["coverage"] == 1.0


# ---------------------------------------------------------------------------
# Gate 2: fail closed on missing fields / coverage / frozen files
# ---------------------------------------------------------------------------


def test_gate2_provider_missing_explicit_l2_code_id_fails_closed(tmp_path):
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2021-01-04"), "000001.SZ")],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"industry_name": ["bank"]}, index=index)
    source = tmp_path / "sector_no_l2_code_id.parquet"
    frame.to_parquet(source)

    provider = provider_mod.SectorDataIndustryIdProvider(source_path=source, min_coverage=0.9)
    with pytest.raises(provider_mod.GatsIndustryProviderError, match="qe_gats_industry_source_schema_invalid"):
        provider(index)


def test_gate2_frozen_builder_pin_mismatch_fails_closed(tmp_path):
    provider_dir = tmp_path / "bin"
    pins = _write_frozen_bin_tree(
        provider_dir,
        spans=[("000001.SZ", "2018-08-01", "2026-06-30")],
        calendar_dates=["2021-01-04", "2021-01-05"],
    )
    pins["instruments_sha256"] = "0" * 64
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _write_spec(workspace, provider_dir, pins)

    with pytest.raises(frozen_builder.FrozenRiskPolicyBuildError, match="qe_frozen_universe_pin_mismatch"):
        frozen_builder.ensure_frozen_risk_policy_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)
    assert not (workspace / frozen_builder.ARTIFACT_FILE).exists()


def test_gate2_frozen_builder_missing_snapshot_dir_fails_closed(tmp_path):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _write_spec(workspace, tmp_path / "missing_bin", {"snapshot_id": "x", "universe_key": "y"})

    with pytest.raises(frozen_builder.FrozenRiskPolicyBuildError, match="qe_frozen_universe_dir_missing"):
        frozen_builder.ensure_frozen_risk_policy_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_gate2_frozen_builder_empty_window_and_bad_schema_fail_closed(tmp_path):
    provider_dir = tmp_path / "bin"
    pins = _write_frozen_bin_tree(
        provider_dir,
        spans=[("000001.SZ", "2018-08-01", "2026-06-30")],
        calendar_dates=["2021-01-04"],
    )
    workspace = tmp_path / "ws"
    workspace.mkdir()
    spec_path = _write_spec(workspace, provider_dir, pins, start="2030-01-01", end="2030-01-31")
    with pytest.raises(frozen_builder.FrozenRiskPolicyBuildError, match="qe_frozen_calendar_window_empty"):
        frozen_builder.ensure_frozen_risk_policy_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)

    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["schema_version"] = "bogus"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    with pytest.raises(frozen_builder.FrozenRiskPolicyBuildError, match="qe_frozen_build_spec_invalid"):
        frozen_builder.ensure_frozen_risk_policy_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_gate2_frozen_builder_no_spec_is_noop(tmp_path):
    assert frozen_builder.ensure_frozen_risk_policy_artifact(cwd=tmp_path, print_fn=lambda *_a, **_k: None) is None
    assert not (tmp_path / frozen_builder.ARTIFACT_FILE).exists()


# ---------------------------------------------------------------------------
# Gate 3: poisoned DB layer does not affect the computation data plane
# ---------------------------------------------------------------------------


def test_gate3_file_provider_completes_with_poisoned_db_imports(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    monkeypatch.setitem(sys.modules, "sqlalchemy", None)
    source = _write_sector_parquet(tmp_path, [("2021-01-04", "000001.SZ", 10)])
    provider = provider_mod.SectorDataIndustryIdProvider(source_path=source, min_coverage=1.0)
    target = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2021-01-04"), "000001.SZ")],
        names=["datetime", "instrument"],
    )
    assert provider(target).iloc[0] == 10


def test_gate3_composer_frozen_spec_completes_with_db_rigged_to_throw(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise AssertionError("data-plane composer path must not open DB connections")

    monkeypatch.setattr(composer_module, "get_conn", _boom)
    spec = json.loads(
        ConfigComposer()._build_qe_frozen_risk_policy_spec(
            DATA_SPLIT,
            # Mirrors the ensure_qe_risk_policy-enforced payload shape.
            {"risk_policy": {"enabled": True, "providers": ["st_pit"], "policy_version": "stock_event_risk_policy_v1"}},
            qlib_data_path="/frozen/bin",
        )
    )
    assert spec["provider_uri_day"] == "/frozen/bin"
    assert spec["pins"]["instruments_sha256"] == QE_FROZEN_INSTRUMENTS_SHA256


def test_gate3_factor_cache_universe_metadata_uses_frozen_pin_without_db(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise AssertionError("factor cache metadata must come from frozen pins, not the DB")

    monkeypatch.setattr(composer_module, "get_conn", _boom)
    meta = ConfigComposer()._resolve_factor_cache_universe_metadata(
        start_date=DATA_SPLIT["train_start"],
        end_date=DATA_SPLIT["backtest_end"],
    )
    assert meta["universe_key"] == QE_ST_PIT_UNIVERSE_KEY
    assert meta["universe_fingerprint_sha256"] == QE_FROZEN_INSTRUMENTS_SHA256


# ---------------------------------------------------------------------------
# Gate 4: composer artifact-generation paths contain no market.* SQL
# ---------------------------------------------------------------------------


def test_gate4_composer_has_no_market_sql_or_dataset_freshness_audit():
    source = (PROJECT_ROOT / "backend" / "services" / "quantevolver" / "config_composer.py").read_text(
        encoding="utf-8"
    )
    assert not MARKET_SQL_RE.search(source), "composer must not execute market.* SQL"
    assert "dataset_date_refresh_audit" not in source
    assert "_build_suspend_filter_artifact" not in source
    assert "StockUniversePitService" not in source
    assert "FactorUniverseMaskService" not in source


def test_gate4_suspend_filter_wires_frozen_artifact_without_querying_db(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise AssertionError("suspend filter assembly must not open DB connections")

    monkeypatch.setattr(composer_module, "get_conn", _boom)
    custom_params, artifact = ConfigComposer()._prepare_suspend_filter_runtime(
        custom_params={
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
            }
        },
        data_split=DATA_SPLIT,
        strategy_info=None,
        execution_algo=None,
    )
    # The suspend artifact is rebuilt on the compute node from the frozen
    # suspend_d candidate dataset pinned in qe_frozen_build_spec.json; the
    # composer wires the strict runtime contract without touching the DB.
    assert artifact is None
    assert custom_params["suspend_filter_file"] == "qe_suspend_filter.json"
    assert custom_params["suspend_filter_strict"] is True


# ---------------------------------------------------------------------------
# Gate 5: workspace payload static scan — no DB drivers / credentials / SQL
# ---------------------------------------------------------------------------


_LOCAL_IMPORT_RE = re.compile(r"^(?:from|import)\s+([a-zA-Z_][\w]*)", re.MULTILINE)


def _workspace_payload_files() -> list[Path]:
    """Files actually copied into a QE workspace, plus their workspace-local
    import closure (e.g. score_weighted_strategy.py pulled in by
    score_weighted_strategy_v2.py).  Backend-only modules such as
    backend/rebalance_strategies/topk_dropout_rc.py (live rebalance engine,
    never imported by QE strategies) are deliberately NOT part of this set.
    """
    seeds: list[Path] = []
    for name in composer_module.QE_MINUTE_RUNTIME_HELPER_FILES:
        seeds.append(composer_module.AUTHORITATIVE_QE_HELPER_ASSETS.get(name, SCRIPTS_DIR / name))
    # ConfigComposer copies both strategies explicitly instead of discovering
    # them through the helper import graph.  V25 used to pull tail_twap in as
    # an accidental transitive dependency, which hid these missing roots.
    for name in ("tail_twap_strategy.py", "tail_twap_v24_strategy.py"):
        seeds.append(SCRIPTS_DIR / name)
    for name in ("qrun_limit.py", "qrun_limit_minute.py", "qe_prediction_store_client.py", "qe_runtime_resource.py"):
        seeds.append(SCRIPTS_DIR / name)
    seeds.append(PROJECT_ROOT / "backend" / "services" / "quantevolver" / "qe_custom_loaders.py")

    files: list[Path] = []
    seen: set[Path] = set()
    stack = list(seeds)
    while stack:
        path = stack.pop()
        path = path.resolve()
        if path in seen or not path.is_file():
            continue
        seen.add(path)
        files.append(path)
        text = path.read_text(encoding="utf-8")
        for module_name in _LOCAL_IMPORT_RE.findall(text):
            candidate = path.parent / f"{module_name}.py"
            if candidate.is_file() and candidate.resolve() not in seen:
                stack.append(candidate)
            candidate_scripts = SCRIPTS_DIR / f"{module_name}.py"
            if candidate_scripts.is_file() and candidate_scripts.resolve() not in seen:
                stack.append(candidate_scripts)

    files.extend(sorted((MODELS_ROOT / "aistock_models").glob("*.py")))
    return files


def test_gate5_workspace_payload_has_no_db_driver_credentials_or_market_sql():
    files = _workspace_payload_files()
    assert files, "workspace payload file set must not be empty"
    missing = [str(path) for path in files if not path.is_file()]
    assert not missing, f"payload files missing from repo: {missing}"
    names = {path.name for path in files}
    # The import closure must cover the workspace-local strategy dependencies;
    # otherwise the scan would silently pass on an incomplete payload set.
    assert "score_weighted_strategy.py" in names
    assert "tail_twap_strategy.py" in names
    assert "tail_twap_v24_strategy.py" in names
    assert "qe_build_frozen_risk_policy.py" in names
    assert "qe_build_frozen_suspend_filter.py" in names

    offenders = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_PATTERNS:
            if pattern in text:
                offenders.append(f"{path.name}: {pattern}")
        if MARKET_SQL_RE.search(text):
            offenders.append(f"{path.name}: market.* SQL")
    assert not offenders, "workspace payload carries DB-backed entries: " + "; ".join(offenders)


def test_gate5_both_qrun_runners_rebuild_frozen_risk_policy_before_qlib_init():
    for name in ("qrun_limit.py", "qrun_limit_minute.py"):
        text = (SCRIPTS_DIR / name).read_text(encoding="utf-8")
        assert "ensure_frozen_risk_policy_artifact" in text, f"{name} lost the frozen rebuild hook"
        assert "qe_frozen_build_spec.json" in text
        assert "qe_build_frozen_risk_policy.py" in text
        hook_at = text.index("ensure_frozen_risk_policy_artifact(cwd=")
        qlib_init_at = text.index("qlib.init(")
        assert hook_at < qlib_init_at, f"{name} must rebuild the artifact before qlib init"
        assert "ensure_frozen_suspend_filter_artifact" in text, f"{name} lost the suspend rebuild hook"
        assert "qe_build_frozen_suspend_filter.py" in text
        suspend_hook_at = text.index("ensure_frozen_suspend_filter_artifact(cwd=")
        assert suspend_hook_at < qlib_init_at, f"{name} must rebuild the suspend artifact before qlib init"


def test_gate5_helper_manifest_includes_frozen_builder():
    assert "qe_build_frozen_risk_policy.py" in composer_module.QE_STRATEGY_RUNTIME_HELPER_FILES
    assert "qe_build_frozen_suspend_filter.py" in composer_module.QE_STRATEGY_RUNTIME_HELPER_FILES
    assert composer_module.FROZEN_BUILD_SPEC_FILE == frozen_builder.SPEC_FILE


# ---------------------------------------------------------------------------
# Gate 6: traceability — path + version + hash on data-plane inputs
# ---------------------------------------------------------------------------


def test_gate6_frozen_artifact_is_deterministic_and_traceable(tmp_path):
    provider_dir = tmp_path / "bin"
    pins = _write_frozen_bin_tree(
        provider_dir,
        spans=[
            ("000001.SZ", "2018-08-01", "2026-06-30"),
            ("600000.SH", "2018-08-01", "2026-06-30"),
        ],
        calendar_dates=["2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08"],
    )
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _write_spec(workspace, provider_dir, pins)

    first = frozen_builder.ensure_frozen_risk_policy_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)
    first_bytes = first.read_bytes()
    second = frozen_builder.ensure_frozen_risk_policy_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)
    assert first_bytes == second.read_bytes(), "frozen rebuild must be deterministic"

    payload = json.loads(first_bytes.decode("utf-8"))
    # path traceability
    assert payload["source"] == "frozen:qlib_bin/instruments/all.txt"
    # version traceability
    assert payload["dataset_contract_id"] == QE_DATASET_CONTRACT_ID
    assert payload["st_universe_key"] == QE_ST_PIT_UNIVERSE_KEY
    assert payload["state"]["universe_key"] == QE_FROZEN_BIN_UNIVERSE_KEY
    assert payload["state"]["status"] == "frozen"
    # hash traceability
    assert payload["state"]["source_fingerprint_sha256"] == pins["instruments_sha256"]
    # semantic content
    assert payload["enabled"] is True
    assert payload["contract"] == "stock_event_risk_policy_v1"
    assert payload["span_count"] == 2
    assert payload["trade_date_count"] == 5
    spans = {(row["ts_code"], row["eligible_start"], row["eligible_end"]) for row in payload["active_spans"]}
    assert spans == {
        ("000001.SZ", "2018-08-01", "2026-06-30"),
        ("600000.SH", "2018-08-01", "2026-06-30"),
    }


def test_gate6_composer_spec_pins_full_traceability_triplet():
    spec = json.loads(
        ConfigComposer()._build_qe_frozen_risk_policy_spec(
            DATA_SPLIT,
            {"risk_policy": {"enabled": True, "providers": ["st_pit"], "policy_version": "stock_event_risk_policy_v1"}},
            qlib_data_path="/frozen/bin",
        )
    )
    assert spec["profile"]["contract"] == "stock_event_risk_policy_v1"
    assert spec["provider_uri_day"] == "/frozen/bin"  # path
    assert spec["dataset"]["contract_id"] == QE_DATASET_CONTRACT_ID  # version
    assert spec["pins"]["snapshot_id"] == QE_FROZEN_BIN_SNAPSHOT_ID
    assert spec["pins"]["universe_key"] == QE_FROZEN_BIN_UNIVERSE_KEY
    for key in ("instruments_sha256", "calendar_sha256", "meta_export_sha256"):  # hash
        assert re.fullmatch(r"[0-9a-f]{64}", spec["pins"][key]), key
    # Suspend pins (BUG-989 continuation): the frozen suspend_d candidate is a
    # versioned sibling of the frozen bin directory and carries the same
    # path+version+hash traceability triplet.
    suspend = spec["suspend"]
    assert suspend["provider_uri"] == "/frozen/suspend_d_daily_candidate_20180801_20260630"
    assert suspend["dataset_id"] == "suspend_d_daily_candidate_20180801_20260630"
    assert suspend["universe_key"] == QE_FROZEN_BIN_UNIVERSE_KEY
    assert suspend["source_contract"] == "tushare_suspend_d_shsz_S_v1"
    for key in ("parquet_sha256", "manifest_sha256"):
        assert re.fullmatch(r"[0-9a-f]{64}", suspend[key]), key


def test_gate6_runtime_risk_policy_validators_accept_frozen_artifact(tmp_path):
    """The runtime validator (scripts/qe_event_risk_policy.py) must accept the
    frozen artifact the builder emits — same contract end to end."""
    provider_dir = tmp_path / "bin"
    pins = _write_frozen_bin_tree(
        provider_dir,
        spans=[("000001.SZ", "2018-08-01", "2026-06-30")],
        calendar_dates=["2021-01-04", "2021-01-05"],
    )
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _write_spec(workspace, provider_dir, pins, start="2021-01-04", end="2021-01-05")
    artifact = frozen_builder.ensure_frozen_risk_policy_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)

    import qe_event_risk_policy as runtime_policy

    policy = runtime_policy.QEEventRiskPolicy(enabled=True, risk_policy_file=str(artifact), strict=True)
    assert policy.is_buy_allowed("000001.SZ", pd.Timestamp("2021-01-04")) is True
