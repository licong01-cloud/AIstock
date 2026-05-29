import importlib.util
import sys
from pathlib import Path

import pytest


_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "aistock_data_quality_smoke.py"
_SPEC = importlib.util.spec_from_file_location("aistock_data_quality_smoke", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
smoke = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = smoke
_SPEC.loader.exec_module(smoke)


class _FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))

    def fetchone(self):
        return self._rows.pop(0)


def _smoke_without_db(**overrides):
    instance = smoke.DataQualitySmoke.__new__(smoke.DataQualitySmoke)
    instance.max_recent_runs = overrides.get("max_recent_runs", 80)
    instance.since_hours = overrides.get("since_hours")
    instance.portfolio_name_prefix = overrides.get("portfolio_name_prefix")
    instance.portfolio_ids = overrides.get("portfolio_ids", [])
    instance.strict_history = overrides.get("strict_history", False)
    instance.audit_schema_only = False
    instance.results = []
    return instance


def test_explicit_env_file_is_loaded_and_overrides_empty_env(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("TDX_DB_HOST=127.0.0.1\nTDX_DB_PASSWORD=secret-for-test\n", encoding="utf-8")
    monkeypatch.setenv("TDX_DB_HOST", "")
    monkeypatch.delenv("TDX_DB_PASSWORD", raising=False)

    loaded = smoke._load_dotenv(env_file)

    assert loaded == env_file
    assert smoke.os.environ["TDX_DB_HOST"] == "127.0.0.1"
    assert smoke.os.environ["TDX_DB_PASSWORD"] == "secret-for-test"


def test_missing_explicit_env_file_fails_fast(tmp_path):
    with pytest.raises(smoke.SmokeFailure, match="explicit env file does not exist"):
        smoke._discover_env_file(tmp_path / "missing.env")


def test_use_dev_db_maps_only_safe_local_dev_target(monkeypatch):
    for key, value in {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5433",
        "TDX_DB_DEV_NAME": "aistock_dev",
        "TDX_DB_DEV_USER": "postgres",
        "TDX_DB_DEV_PASSWORD": "dev-password-for-test",
    }.items():
        monkeypatch.setenv(key, value)
    for runtime_key in smoke.DEV_DB_KEYS.values():
        monkeypatch.delenv(runtime_key, raising=False)

    target = smoke._apply_dev_db_env()

    assert target["source"] == "tdx_db_dev"
    assert target["host"] == "127.0.0.1"
    assert target["port"] == "5433"
    assert target["dbname"] == "aistock_dev"
    assert target["password_configured"] is True
    assert smoke.os.environ["TDX_DB_PASSWORD"] == "dev-password-for-test"


def test_use_dev_db_refuses_non_dev_target(monkeypatch):
    for key, value in {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5432",
        "TDX_DB_DEV_NAME": "aistock",
        "TDX_DB_DEV_USER": "postgres",
        "TDX_DB_DEV_PASSWORD": "dev-password-for-test",
    }.items():
        monkeypatch.setenv(key, value)

    with pytest.raises(smoke.SmokeFailure, match="not the local dev DB"):
        smoke._apply_dev_db_env()


def test_paper_v2_run_traceability_accepts_minqmt_success_events():
    runner = _smoke_without_db(portfolio_ids=["paper_minqmt"])
    cur = _FakeCursor(rows=[(3, 0, 0)])

    runner._check_paper_v2_runs(cur)

    sql, params = cur.calls[0]
    success_events = params[-1]
    assert "ev.event_type = ANY(%s)" in sql
    assert success_events == list(smoke.PAPER_V2_SUCCESS_RUN_EVENTS)
    assert "MINIQMT_RUN_RECONCILED" in success_events
    assert "MINIQMT_NATIVE_RUN_RECONCILED" in success_events
    assert runner.results[0].status == "PASS"


def test_paper_v2_ledger_consistency_uses_broker_authority_invariants():
    runner = _smoke_without_db(portfolio_ids=["paper_minqmt"])
    cur = _FakeCursor(rows=[(0,), (0,), (0,), (0,)])

    runner._check_paper_v2_ledger_consistency(cur)

    cash_sql, cash_params = cur.calls[1]
    snapshot_sql, snapshot_params = cur.calls[2]
    assert "COALESCE(p.broker_backend, 'local_sim') AS broker_backend" in cash_sql
    assert "r.broker_backend <> ALL(%s)" in cash_sql
    assert cash_params[-1] == list(smoke.BROKER_AUTHORITY_BACKENDS)
    assert "r.broker_backend <> ALL(%s)" in snapshot_sql
    assert "abs(ds.nav - ds.cash - ds.market_value) > 0.01" in snapshot_sql
    assert snapshot_params[-1] == list(smoke.BROKER_AUTHORITY_BACKENDS)
    assert runner.results[0].status == "PASS"
